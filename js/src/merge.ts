/*
  PRISMERGE - a tool for merging SQLite databases together using their shared Prisma schema.

  Cameron C. Dutro
  January 2025

  Preamble

  Prismerge is a tool written specifically for merging SQLite databases together for an
  internal tool at GitHub that allows engineers working on the Primer design system
  (https://primer.style) to easily identify React and Rails component usages across
  various repositories and package versions.

  NOTE: Prismerge may or may not work for your use-case. Please read this entire comment
  carefully to understand how it works and what it assumes about your schema and data
  model.


  Introduction

  Prismerge is capable of merging n distinct SQLite databases into a single database,
  where each database has the same schema defined in a Prisma schema file. Prisma is a
  JavaScript ORM akin to ActiveRecord in Rails, Ecto in Phoenix, or Hibernate in Java.
  The schema file enumerates the columns present in each table as well as the
  relationships between tables. The information in the schema file is more accurate
  than what could be gathered by dumping the database's native schema, and also easier
  to parse. Prismerge copies data from each input database into a final merged database,
  ensuring foreign key data integrity along the way.

  The idea is fairly straightforward: each time a row is inserted into the database,
  record its old primary key (eg. its "ID") and its new primary key in a separate mapping
  table. When inserting rows that reference this table via a foreign key, translate the
  old ID to the new ID before insertion. This way, all copied rows will correctly
  reference their parent rows in the new database.


  Assumptions

  Prismerge assumes several major things about your schema and data model:

  1. All tables have a primary key.

  2. All primary keys are UUID strings. When inserting new rows, prismerge generates new
      UUIDs and inserts them into both the merged table and the mapping table. Prismerge
      is not designed to accommodate non-string, non-UUID primary keys.

  3. Primary keys are strictly IDs and not data. For example, a table cannot use a git
      SHA as a primary key because the merging process involves generating new primary
      keys for inserted rows. This limitation could perhaps be relaxed in the future.

  3. Tables have unique indices to prevent duplicate rows. Prismerge detects the
      presence of unique indices defined in the Prisma schema and uses them to prevent
      inserting duplicate rows. For each row in each of the input databases, Prismerge
      checks the merged database for a row that contains the same data as the current one.
      If such a row exists, prismerge skips inserting a new row and instead only inserts
      a mapping table row where the old ID is the ID of the original row and the new ID
      is the ID of the already inserted row.

  4. No cycles. Prismerge assumes no cycles in the graph of relationships between tables,
      i.e. A depends on B which depends on A again.


  Maintaining foreign key integrity

  Connections between tables in relational databases can be thought of as a series of
  parent-child relationships. Foreign keys in child tables point to rows in parent tables.
  This means rows in parent tables must exist before child rows can reference them, and
  rows in grandparent tables must exist before parent rows can reference _them_, and so on
  and so forth all the way up the family tree.

  To ensure data is inserted in the correct order, Prismerge uses the relationships
  defined in the Prisma schema to populate tables from the top of the family tree to the
  bottom. This ensures that parent rows exist before child rows need to reference them.
  To achieve the correct ordering, prismerge uses a topological sorting algorithm.


  Primary and secondary databases

  It is much more efficient to insert data into the merged database without checking if
  the row exists already. To maximize efficiency and on a per-table basis, prismerge
  counts the rows for the table in each of the input databases. The database with the
  most records in the given table is called the primary, and all the rest are secondaries.
  Prismerge skips the existence check when copying data from the primary, which is much
  faster. Unfortunately, existence checking must be performed for all the secondary
  databases.
*/

import sqlite3 from 'sqlite3'
import { open } from 'sqlite';
import { getSchema } from './prisma.js';
import type { Column, Model, Schema } from './data.js';
import { Connection } from './connection.js';
import { InsertManager } from './insert_manager.js';
import { type Option, None, Some } from './option.js';
import * as crypto from "node:crypto";
import { ProgressIndicator } from './progress.js';

const DEFAULT_MIN_INSERTS = 1000;

export async function merge(inputPaths: string[], outputPath: string, minInserts: number = DEFAULT_MIN_INSERTS, keepIdMaps: boolean = false) {
  const schema = getSchema();

  // Open input and output databases.
  const connections = await Promise.all(
    inputPaths.map(async (path) => {
      return new Connection(await open({filename: path, driver: sqlite3.Database}))
    })
  );

  const merged = new Connection(
    await open({filename: outputPath, driver: sqlite3.Database})
  );

  await prismerge(schema, connections, merged, minInserts, true);

  // Make sure there are no foreign key integrity problems. If there are,
  // print out warnings so the user knows what's up.
  for (const [_, currentModel] of schema.models) {
    const integrity = await currentModel.verifyIntegrity(merged);

    if (integrity.isErr()) {
      console.log(`Table ${currentModel.name} has ${integrity.value} foreign key integrity problems`)
    }
  }

  // Clean up after ourselves by dropping all the map tables.
  if (!keepIdMaps) {
    for (const [_, currentModel] of schema.models) {
      await currentModel.mapTable.dropFrom(merged);
    }
  }

  // Reclaim space from deleted tables, etc.
  vacuum(merged);

  await merged.close();
}

export async function prismerge(schema: Schema, connections: Connection[], merged: Connection, minInserts: number, showProgress: boolean): Promise<void> {
  // Get a list of Model objects, sorted topologically so parent records are
  // created before children.
  let order = schema.sortedModels();

  // Turn off a lot of important stuff so inserting is fast.
  await merged.execOrError(`
    PRAGMA synchronous = OFF;
    PRAGMA journal_mode = OFF;
    PRAGMA temp_store = MEMORY;
    PRAGMA cache_size = -16000;
    PRAGMA foreign_keys = OFF;
  `);

  // Set up the merged database by copying over the schema. Each row here is a
  // CREATE TABLE or CREATE INDEX statement that we can execute directly on the
  // merged database connection.
  await connections[0]!.each<{sql: string | null}>("SELECT sql FROM sqlite_master;", async (result) => {
    const row = result.unwrap();

    if (row.sql) {
      await merged.execOrError(row.sql);
    }
  });

  // Merge each model.
  for (const currentModel of order) {
    await mergeModel(currentModel, schema, connections, merged, minInserts, showProgress);
  }

  // Turn important things back on to ensure integrity, etc.
  await merged.execOrError(`
    PRAGMA synchronous = ON;
    PRAGMA journal_mode = DELETE;
    PRAGMA foreign_keys = ON;
  `);
}

// Runs the SQLite VACUUM command which reclaims space from deleted tables, indices, etc.
async function vacuum(conn: Connection) {
  await conn.execOrError("VACUUM;");
}

// This is where most of the magic happens. This function merges the records for the
// given Model, copying records from the databases in `connections` into the database
// in `merged`. The min_inserts argument specifies how many INSERTs to batch up before
// inserting in bulk.
async function mergeModel(model: Model, schema: Schema, connections: Connection[], merged: Connection, minInserts: number, showProgress: boolean) {
  await model.mapTable.createInto(merged);

  const inserter = new InsertManager(merged, minInserts);
  const primaryKey = model.primaryKey.unwrap();
  const colsToCopy: Column[] = [];

  // Enumerate columns that will be copied wholesale, i.e. without any translation.
  // In other words, all columns that aren't foreign keys.
  for (const column of model.columns) {
    if (column.isRegular(schema)) {
      colsToCopy.push(column);
    }
  }

  const countQuery = `SELECT COUNT(${primaryKey.name}) AS count FROM \"${model.name}\" WHERE 1`;

  // This is the query that will be used to iterate over all the rows in each of the
  // input databases. We select two versions of the primary key, one quoted and one
  // unquoted. It's important to select both because they are used in different contexts
  // during the merge process.
  //
  // We also select quoted versions of all the other columns as well so they can be
  // directly interpolated into INSERT statements without having to know what data type
  // they are. It would be quite tedious to quote things or not depending on the type, so
  // we let SQLite do the work for us.
  const quotedColumns = colsToCopy
    .map(col => `${col.quoted(model.name)} AS ${col.name}`)
    .join(", ");

  const selectQuery = `
    SELECT \"${primaryKey.name}\" AS unquotedPk,
      quote(\"${primaryKey.name}\") AS \"${primaryKey.name}\",
      ${quotedColumns}
    FROM \"${model.name}\"
    WHERE 1;
  `;

  let checkSqlTemplate: Option<string> = None();

  // If the model has a unique index, we want to use it to query for existing records.
  // We enumerate all of its columns here and build up a SELECT query. This query not
  // only has to check existing "regular" columns (i.e. columns that are not foreign
  // keys), but also foreign keys that will have been translated into new keys via one
  // of the mapping tables. The resulting query includes a a JOIN clause for each of the
  // foreign keys, as well as a WHERE clause containing normal comparisons for the
  // regular columns and comparisons to the mapped old ID for all foreign keys.
  if (model.unique.isSome()) {
    const unique = model.unique.unwrap();
    const checkWheres: string[] = [];
    const checkJoins: string [] = [];

    for (let idx = 0; idx < unique.columnNames.length; idx ++) {
      const name = unique.columnNames[idx]!;
      const col = model.getCol(name).unwrap();

      // Check if the current column holds a foreign key by attempting to find the
      // @relation associated with it. The Column struct returned by the
      // `get_related_column()` method will return the column with the @relation
      // annotation, which isn't an actual database column. That column's type points
      // at the associated table, which allows us to construct the right JOIN clause.
      col.getRelatedColumn(model).and((relatedColumn) => {
        const key = `\"${model.name}\".\"${col.name}\"`;
        const foreignKey = `${relatedColumn.ty.name}_id_map.new_id`;

        checkJoins.push(
          `JOIN ${relatedColumn.ty.name}_id_map ON ${key} = ${foreignKey}`
        );

        checkWheres.push(
          `${relatedColumn.ty.name}_id_map.old_id = ?${idx + 1}`
        );
      }).or(() => {
        // Regular columns only need to have their values compared.
        checkWheres.push(
          `${name} = ?${idx + 1}`
        );
      });
    }

    checkSqlTemplate = Some(
      `
        SELECT quote(${primaryKey.name}) AS ${primaryKey.name} FROM "${model.name}"
        ${checkJoins.join("\n")}
        WHERE ${checkWheres.join(" AND ")}
        LIMIT 1;
      `
    );
  }

  let totalRows = 0;

  // As described earlier, the "primary" connection is the one that contains the
  // largest number of rows for the given model. Every other connection is called
  // a "secondary."
  let primary = connections[0]!;
  let primaryCount = 0;

  for (const conn of connections) {
    const count = (await conn.get<{count: number}>(countQuery)).unwrap().unwrap().count;
    totalRows += count;

    if (count > primaryCount) {
      primaryCount = count;
      primary = conn;
    }
  }

  // Insert the primary connection first so it's processed first. Copying from the
  // primary connection first enables us to skip checking for existing records for
  // the connection with the largest number of rows, which can significantly increase
  // performance.
  const sortedConnections: Connection[] = [primary];

  // Append all secondary connections.
  for (const conn of connections) {
    if (conn !== primary) {
      sortedConnections.push(conn);
    }
  }

  const progress = showProgress ?
    ProgressIndicator.create(model.name, totalRows) :
    ProgressIndicator.null();

  // Iterate over each connection and copy all rows to the merged database.
  for (const conn of sortedConnections) {
    const isPrimary = conn === primary;
    const isSecondary = !isPrimary;

    // Execute a query for iterating over all existing rows in the current input database.
    await conn.each<{[key: string]: any}>(selectQuery, async (rowResult) => {
      if (rowResult.isErr()) {
        return;
      }

      const row = rowResult.unwrap();
      const oldPk = row.unquotedPk;
      let existingPk: Option<string> = None();

      // If we're copying rows from a secondary database, check
      // if the current row already exists using the existing
      // unique index, if any.
      if (isSecondary) {
        if (checkSqlTemplate.isSome()) {
          let checkSql = checkSqlTemplate.unwrap();

          // Rather than use rusqlite's mechanism for binding
          // values to a query string, we perform a dumb string
          // replacement here. Rusqlite expects placeholders of
          // the form ?<n>, where <n> is an unsigned integer.
          // Since all the columns we're copying have already
          // been quoted by SQLite, we want to avoid any extra
          // escaping or munging that rusqlite might do, so we
          // simply swap in the quoted value and call it a day.
          const columnNames = model.unique.unwrap().columnNames;

          for (let idx = 0; idx < columnNames.length; idx ++) {
            const col = columnNames[idx]!;
            const value = row[col];
            checkSql = checkSql.replace(`?${idx + 1}`, value);
          }

          (await merged.get<{[key: string]: any}>(checkSql)).and((existingRow) => {
            existingRow.and((existingRow) => {
              // Found a result, so record the existing primary key for use later.
              existingPk = Some(existingRow[primaryKey.name]);
            });
          });
        }
      }

      // An existing row was found, so only insert a map table entry.
      if (existingPk.isSome()) {
        const existingId = existingPk.unwrap();
        const idMapInsert = `INSERT INTO \"${model.mapTable.name}\" (old_id, new_id) VALUES ('${oldPk}', ${existingId})`;

        // Even though this is an INSERT into the ID map table, it
        // represents an actual row. We're skipping because it already
        // exists, so we call insert() instead of insertSupporting()
        // to count it towards merge progress.
        progress.inc(await inserter.insert(idMapInsert));

        return;
      }

      // In the case of the primary, we can use the old primary key. In
      // the case of a secondary, we mint a new primary key (mostly to
      // avoid confusion when debugging lol).
      const newPk = isPrimary ? oldPk : crypto.randomUUID();

      // Just as we did with the check_sql_template above, the INSERT
      // statement must not only copy over values from the original input
      // row, but also translate foreign keys via mapping tables. To
      // achieve this, a JOIN statement is included in the INSERT statement
      // for each foreign key.
      const selectValues: string[] = [`'${newPk}'`];
      const selectColumns: string[] = [primaryKey.name];
      const joinStatements: string[] = [];

      for (const column of model.columns) {
        column.getRelatedColumn(model).and((relatedColumn) => {
          let oldId: String = row[column.name];

          selectValues.push(
            `${relatedColumn.ty.name}_id_map.new_id`
          );

          selectColumns.push(column.name);
          joinStatements.push(
            `
              LEFT JOIN ${relatedColumn.ty.name}_id_map
              ON ${relatedColumn.ty.name}_id_map.old_id = ${oldId}
            `
          );
        }).or(() => {
          if (column.isRegular(schema)) {
            const value = row[column.name];
            selectValues.push(value);
            selectColumns.push(column.name);
          }
        });
      }

      // Construct the actual INSERT statement.
      const insert_sql =
        `
          INSERT INTO "${model.name}" (${selectColumns.join(", ")})
          SELECT ${selectValues.join(", ")}
          FROM (SELECT 1) AS dummy
          ${joinStatements.join("\n")}
          LIMIT 1
        `;

      progress.inc(await inserter.insert(insert_sql));

      // Construct the INSERT statement for the map table.
      const idMapInsert = `
        INSERT INTO \"${model.mapTable.name}\" (old_id, new_id)
        VALUES ('${oldPk}', '${newPk}')
      `;

      progress.inc(await inserter.insertSupporting(idMapInsert));
    });

    // Insert any lingering records.
    progress.inc(await inserter.flush());
  }

  progress.inc(await inserter.flush());

  // Create several indices on the mapping table. We do this after we're entirely
  // finished inserting because it's much faster to do it at the end rather than
  // refresh the index on each individual INSERT.
  await model.mapTable.createIndices(merged);

  progress.finish();
}
