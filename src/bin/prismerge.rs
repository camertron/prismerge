/*
    PRISMERGE - a tool for merging SQLite databases together using their shared Prisma schema.

    Cameron C. Dutro
    November 2024

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

use prismerge::prisma_parser::{self, Column, Model, Schema};
use std::{fs, time::SystemTime};
use std::io::{self, IsTerminal};
use rusqlite::{Connection, Result};
use uuid::Uuid;
use indicatif::{ProgressBar, ProgressStyle};
use clap::{ArgAction, Parser};

#[derive(Parser, Debug)]
#[command(
    name="prismerge",
    author="Cameron C. Dutro",
    version="1.0.0",
    about="Merge SQLite databases together using their shared Prisma schema."
)]
struct CLI {
    #[arg(
        long,
        short,
        value_name="PATH",
        help="The path to the Prisma schema file."
    )]
    schema_path: String,

    #[arg(
        long,
        short,
        value_name="PATH",
        default_value="./merged.db",
        help="The path of the merged database file."
    )]
    output_path: String,

    #[arg(
        long,
        short,
        action=ArgAction::SetTrue,
        help="After merging is complete, don't drop the temporary tables prismerge creates to keep track of old -> new foreign key mappings."
    )]
    keep_id_maps: bool,

    #[arg(
        long,
        short,
        value_name="NUMBER",
        default_value="1000",
        help="The minimum number of rows to insert at a time."
    )]
    min_inserts: u64,

    #[arg(
        value_name="INPUT PATHS",
        num_args=1..,
        required=true,
        help="Paths to the SQLite database files to merge."
    )]
    input_paths: Vec<String>,
}

/* The InsertManager is a convenient way to insert records in bulk. Every time
 * a record is inserted, the manager adds it to an internal list. When the length
 * of the list exceeds the given threshold, all the records are inserted at once,
 * in bulk.
 *
 * There are two kinds of records managed by the InsertManager - regular reecords,
 * and so-called "supporting" records. Supporting records are records that do not
 * contribute to overall merge progress. For prismerge, supporting records are
 * records inserted into ID mapping tables. Other records, i.e. records from
 * input databases, are regular records.
 *
 * Insert regular records using the `insert()` method, `and insert_supporting()`
 * to insert supporting records. The value of the `count` attribute will be
 * incremented for regular records, but not for supporting records. Each insert
 * method returns either 0 or this count value, indicating how many regular
 * records were actually inserted.
 *
 * Call the `flush()` method to force the InsertManager to insert all pending
 * records, regular and otherwise.
 */
struct InsertManager<'a> {
    connection: &'a Connection,
    threshold: u64,
    statements: Vec<String>,
    count: usize
}

impl<'a> InsertManager<'a> {
    fn new(connection: &'a Connection, threshold: u64) -> Self {
        InsertManager { connection, threshold, statements: vec![], count: 0 }
    }

    fn insert(self: &mut Self, statement: String) -> u64 {
        self.statements.push(statement);
        self.count += 1;
        self.maybe_flush()
    }

    fn insert_supporting(self: &mut Self, statement: String) -> u64 {
        self.statements.push(statement);
        self.maybe_flush()
    }

    fn maybe_flush(self: &mut Self) -> u64 {
        if self.statements.len() as u64 >= self.threshold {
            return self.flush();
        }

        0
    }

    fn flush(self: &mut Self) -> u64 {
        let batch = format!("BEGIN TRANSACTION; {}; COMMIT;", self.statements.join("; "));
        self.connection.execute_batch(batch.as_str()).unwrap();
        self.statements.clear();
        let count = self.count as u64;
        self.count = 0;
        count
    }
}

enum ProgressType {
    Bar(ProgressBar),
    Console
}

/* A wrapper around the ProgressBar crate that falls back to regular 'ol console logging
 * if STDIN isn't a terminal. Progress bars don't work super well in GitHub Actions and
 * end up writing nothing, making it difficult to track merge progress.
 */
struct ProgressIndicator {
    progress_type: ProgressType,
    model_name: String,
    total_rows: u64,
    count: u64
}

impl ProgressIndicator {
    fn new(model_name: &str, total_rows: u64) -> Self {
        if io::stdin().is_terminal() {
            let pb = ProgressBar::new(total_rows);

            pb.set_style(
                ProgressStyle::with_template(
                    format!("{{spinner:.green}} {} [{{elapsed_precise}}] [{{wide_bar:.cyan/blue}}] {{pos}}/{{len}}", model_name).as_str()
                )
                .unwrap()
                .progress_chars("#>-"));

            ProgressIndicator {
                progress_type: ProgressType::Bar(pb),
                model_name: model_name.to_string(),
                total_rows: total_rows,
                count: 0
            }
        } else {
            ProgressIndicator {
                progress_type: ProgressType::Console,
                model_name: model_name.to_string(),
                total_rows: total_rows,
                count: 0
            }
        }
    }

    fn inc(self: &mut Self, delta: u64) {
        match &self.progress_type {
            ProgressType::Bar(pb) => pb.inc(delta),
            ProgressType::Console => {
                self.count += delta;

                if delta != 0 {
                    println!("{}: Processed {}/{} records", self.model_name, self.count, self.total_rows);
                }
            }
        }
    }

    fn finish(self: &mut Self) {
        match &self.progress_type {
            ProgressType::Bar(pb) => pb.finish(),
            ProgressType::Console => {
                self.count = self.total_rows;
                println!("{}: Processed {}/{} records", self.model_name, self.count, self.total_rows);
            }
        }
    }
}

fn main() -> Result<(), String> {
    let start_time = SystemTime::now();
    let options = CLI::parse();

    // Load and parse the Prisma schema.
    let source_code_str = fs::read_to_string(options.schema_path).unwrap();
    let source_code = source_code_str.as_str();
    let schema = prisma_parser::parse(source_code).unwrap();

    // Get a list of Model objects, sorted topologically so parent records are
    // created before children.
    let order = schema.sorted();

    // Open all input databases.
    let connections: Vec<Connection> = options.input_paths[1..]
        .iter()
        .map(|path| Connection::open(path).unwrap())
        .collect();

    // Open output database.
    let merged = Connection::open(options.output_path).unwrap();

    // Turn off a lot of important stuff so inserting is fast.
    merged.execute_batch(r#"
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -16000;
        PRAGMA foreign_keys = OFF;
    "#).unwrap();

    // Set up the merged database by copying over the schema. Each row here is a
    // CREATE TABLE or CREATE INDEX statement that we can execute directly on the
    // merged database connection.
    let mut schema_query = connections[0].prepare("SELECT sql FROM sqlite_master;").unwrap();
    let mut schema_rows = schema_query.query(()).unwrap();

    loop {
        match schema_rows.next() {
            Ok(Some(row)) => {
                let stmt = row.get::<usize, String>(0);

                match stmt {
                    Ok(stmt) => {
                        merged.execute(stmt.as_str(), ()).unwrap();
                    }

                    Err(_) => ()
                }
            }

            Ok(None) => break,
            Err(_) => break
        }
    }

    // Merge each model.
    for current_model in &order {
        merge_model(current_model, &schema, &connections, &merged, options.min_inserts);
    }

    // Turn important things back on to ensure integrity, etc.
    merged.execute_batch(r#"
        PRAGMA synchronous = ON;
        PRAGMA journal_mode = DELETE;
        PRAGMA foreign_keys = ON;
    "#).unwrap();

    // Make sure there are no foreign key integrity problems. If there are,
    // print out warnings so the user knows what's up.
    for current_model in &order {
        match current_model.verify_integrity(&merged) {
            Err(count) => println!("Table {} has {} foreign key integrity problems", current_model.name, count),
            _ => ()
        }
    }

    // Clean up after ourselves by dropping all the map tables.
    if !options.keep_id_maps {
        for current_model in order {
            current_model.map_table.drop_from(&merged);
        }
    }

    // Reclaim space from deleted tables, etc.
    vacuum(&merged);

    // Report how long the whole merge process took.
    match start_time.elapsed() {
        Ok(elapsed) => {
            let total_secs = elapsed.as_secs();
            let secs = total_secs % 60;
            let mins = total_secs / 60;
            let hrs = total_secs / 60 / 60;

            if hrs > 0 {
                println!("Finished in {}h{:02}m{:02}s", hrs, mins, secs);
            } else {
                println!("Finished in {}m{:02}s", mins, secs);
            }
        }

        Err(_) => {}
    }

    Ok(())
}

// Runs the SQLite VACUUM command which reclaims space from deleted tables, indices, etc.
fn vacuum(conn: &Connection) {
    conn.execute("VACUUM;", ()).unwrap();
}

// This is where most of the magic happens. This function merges the records for the
// given Model, copying records from the databases in `connections` into the database
// in `merged`. The min_inserts argument specifies how many INSERTs to batch up before
// inserting in bulk.
fn merge_model(model: &Model, schema: &Schema, connections: &Vec<Connection>, merged: &Connection, min_inserts: u64) {
    model.map_table.create_into(&merged);

    let mut inserter = InsertManager::new(merged, min_inserts);
    let primary_key = model.primary_key().unwrap();
    let mut cols_to_copy: Vec<&Column> = vec![];

    // Enumerate columns that will be copied wholesale, i.e. without any translation.
    // In other words, all columns that aren't foreign keys.
    for column in model.columns.iter() {
        if column.is_regular(schema) {
            cols_to_copy.push(column);
        }
    }

    let count_query = format!(
        "SELECT COUNT({primary_key}) FROM \"{table}\" WHERE 1",
        primary_key = primary_key.name,
        table = model.name
    );

    // This is the query that will be used to iterate over all the rows in each of the
    // input databases. We select two versions of the primary key, one quoted and one
    // unquoted. It's important to select both because they are used in different contexts
    // during the merge process.
    //
    // We also select quoted versions of all the other columns as well so they can be
    // directly interpolated into INSERT statements without having to know what data type
    // they are. It would be quite tedious to quote things or not depending on the type, so
    // we let SQLite do the work for us.
    let select_query = format!(
        "SELECT \"{primary_key}\" AS unquoted_pk, quote(\"{primary_key}\") AS \"{primary_key}\", {quoted_columns} FROM \"{table}\" WHERE 1;",
        quoted_columns = cols_to_copy
            .iter()
            .map(|col| format!("{} AS {}", col.quoted(&model.name), col.name))
            .collect::<Vec<String>>()
            .join(", "),
        primary_key = primary_key.name,
        table = model.name
    );

    let mut check_sql_template: Option<String> = None;

    // If the model has a unique index, we want to use it to query for existing records.
    // We enumerate all of its columns here and build up a SELECT query. This query not
    // only has to check existing "regular" columns (i.e. columns that are not foreign
    // keys), but also foreign keys that will have been translated into new keys via one
    // of the mapping tables. The resulting query includes a a JOIN clause for each of the
    // foreign keys, as well as a WHERE clause containing normal comparisons for the
    // regular columns and comparisons to the mapped old ID for all foreign keys.
    if let Some(unique) = &model.unique {
        let mut check_wheres: Vec<String> = vec![];
        let mut check_joins: Vec<String> = vec![];

        for (idx, name) in unique.column_names.iter().enumerate() {
            let col = model.get_col(name).unwrap();

            // Check if the current column holds a foreign key by attempting to find the
            // @relation associated with it. The Column struct returned by the
            // `get_related_column()` method will return the column with the @relation
            // annotation, which isn't an actual database column. That column's type points
            // at the associated table, which allows us to construct the right JOIN clause.
            if let Some(related_column) = col.get_related_column(&model) {
                check_joins.push(
                    format!(
                        "JOIN {table}_id_map ON {key} = {foreign_key}",
                        table = related_column.ty.name,
                        key = format!("\"{}\".\"{}\"", model.name, col.name),
                        foreign_key = format!("{}_id_map.new_id", related_column.ty.name)
                    )
                );

                check_wheres.push(
                    format!(
                        "{table}_id_map.old_id = ?{idx}",
                        table = related_column.ty.name,
                        idx = idx + 1
                    )
                );
            } else {
                // Regular columns only need to have their values compared.
                check_wheres.push(
                    format!(
                        "{col} = ?{idx}",
                        col = name,
                        idx = idx + 1
                    )
                )
            }
        }

        check_sql_template = Some(
            format!(
            r#"
                SELECT quote({primary_key}) FROM "{table}"
                {check_joins}
                WHERE {where_stmts}
                LIMIT 1;
            "#,
            primary_key = primary_key.name,
            table = model.name,
            check_joins = check_joins.join("\n"),
            where_stmts = check_wheres.join(" AND ")
        ));
    }

    let mut total_rows: u64 = 0;

    // As described earlier, the "primary" connection is the one that contains the
    // largest number of rows for the given model. Every other connection is called
    // a "secondary."
    let mut primary = &connections[0];
    let mut primary_count: u64 = 0;

    for conn in connections {
        let mut count_stmt = conn.prepare(count_query.as_str()).unwrap();
        let mut count_rows = count_stmt.query(()).unwrap();
        let count: u64 = count_rows.next().unwrap().unwrap().get(0).unwrap();
        total_rows += count;

        if count > primary_count {
            primary_count = count;
            primary = conn;
        }
    }

    // Insert the primary connection first so it's processed first. Copying from the
    // primary connection first enables us to skip checking for existing records for
    // the connection with the largest number of rows, which can significantly increase
    // performance.
    let mut sorted_connections: Vec<&Connection> = vec![primary];

    // Append all secondary connections.
    for conn in connections {
        if !core::ptr::eq(conn, primary) {
            sorted_connections.push(conn);
        }
    }

    let mut progress = ProgressIndicator::new(model.name.as_str(), total_rows);

    // Iterate over each connection and copy all rows to the merged database.
    for conn in sorted_connections {
        let is_primary = core::ptr::eq(conn, primary);
        let is_secondary = !is_primary;

        // Execute a query for iterating over all existing rows in the current input database.
        let mut stmt = conn.prepare(select_query.as_str()).unwrap();
        let mut rows = stmt.query(()).unwrap();

        loop {
            match rows.next() {
                // Successfully fetched the next row
                Ok(Some(row)) => {
                    let old_pk: String = row.get(0).unwrap();
                    let mut existing_pk: Option<String> = None;

                    // If we're copying rows from a secondary database, check
                    // if the current row already exists using the existing
                    // unique index, if any.
                    if is_secondary {
                        if let Some(check_sql_orig) = &check_sql_template {
                            let mut check_sql = check_sql_orig.clone();

                            // Rather than use rusqlite's mechanism for binding
                            // values to a query string, we perform a dumb string
                            // replacement here. Rusqlite expects placeholders of
                            // the form ?<n>, where <n> is an unsigned integer.
                            // Since all the columns we're copying have already
                            // been quoted by SQLite, we want to avoid any extra
                            // escaping or munging that rusqlite might do, so we
                            // simply swap in the quoted value and call it a day.
                            for (idx, col) in model.unique.as_ref().unwrap().column_names.iter().enumerate() {
                                let value = row.get::<_, String>(col.as_str()).unwrap();
                                check_sql = check_sql.replace(&format!("?{}", idx + 1), &value);
                            }

                            match merged.query_row(check_sql.as_str(), (), |row| {
                                // Found a result, so record the existing primary key for use later.
                                existing_pk = Some(row.get::<_, String>(0).unwrap());
                                Ok(())
                            }) {
                                Ok(_) => (),
                                Err(_) => ()
                            }
                        }
                    }

                    // An existing row was found, so only insert a map table entry.
                    if let Some(existing_id) = existing_pk {
                        let id_map_insert = format!(
                            "INSERT INTO \"{table}\" (old_id, new_id) VALUES ('{old_pk}', {existing_id})",
                            table = model.map_table.name,
                            old_pk = old_pk,
                            existing_id = existing_id
                        );

                        // Even though this is an INSERT into the ID map table, it
                        // represents an actual row. We're skipping because it already
                        // exists, so we call insert() instead of insert_supporting()
                        // to count it towards merge progress.
                        progress.inc(inserter.insert(id_map_insert));

                        continue;
                    }

                    // In the case of the primary, we can use the old primary key. In
                    // the case of a secondary, we mint a new primary key (mostly to
                    // avoid confusion when debugging lol).
                    let new_pk = if is_primary {
                        old_pk.clone()
                    } else {
                        Uuid::new_v4().to_string()
                    };

                    // Just as we did with the check_sql_template above, the INSERT
                    // statement must not only copy over values from the original input
                    // row, but also translate foreign keys via mapping tables. To
                    // achieve this, a JOIN statement is included in the INSERT statement
                    // for each foreign key.
                    let mut select_values: Vec<String> = vec![format!("'{}'", new_pk)];
                    let mut select_columns: Vec<&str> = vec![primary_key.name.as_str()];
                    let mut join_statements: Vec<String> = vec![];
                    let mut field_index = 2;

                    for column in model.columns.iter() {
                        if let Some(related_column) = column.get_related_column(&model) {
                            let old_id: String = row.get(field_index).unwrap();
                            field_index += 1;

                            select_values.push(format!(
                                "{}_id_map.new_id",
                                related_column.ty.name
                            ));

                            select_columns.push(column.name.as_str());
                            join_statements.push(
                                format!(
                                    "LEFT JOIN {table}_id_map ON {table}_id_map.old_id = {old_id}",
                                    table = related_column.ty.name,
                                    old_id = old_id
                                )
                            )
                        } else if column.is_regular(&schema) {
                            let value: String = row.get(field_index).unwrap();
                            field_index += 1;
                            select_values.push(value);
                            select_columns.push(column.name.as_str());
                        }
                    }

                    // Construct the actual INSERT statement.
                    let insert_sql = format!(
                        r#"
                            INSERT INTO "{table}" ({column_names})
                            SELECT {select_values}
                            FROM (SELECT 1) AS dummy
                            {join_statements}
                            LIMIT 1
                        "#,
                        table = model.name,
                        column_names = select_columns.join(", "),
                        select_values = select_values.join(", "),
                        join_statements = join_statements.join("\n")
                    );

                    progress.inc(inserter.insert(insert_sql));

                    // Construct the INSERT statement for the map table.
                    let id_map_insert = format!(
                        "INSERT INTO \"{table}\" (old_id, new_id) VALUES ('{old_id}', '{new_id}')",
                        table = model.map_table.name,
                        old_id = old_pk,
                        new_id = new_pk
                    );

                    progress.inc(inserter.insert_supporting(id_map_insert));
                }

                // Occurs when there are no more rows in the result set.
                Ok(None) => break,

                // Some SQLite error occurred.
                Err(_) => continue
            }
        }

        // Insert any lingering records.
        progress.inc(inserter.flush());
    }

    progress.inc(inserter.flush());

    // Create several indices on the mapping table. We do this after we're entirely
    // finished inserting because it's much faster to do it at the end rather than
    // refresh the index on each individual INSERT.
    model.map_table.create_indices(&merged);

    progress.finish();
}
