use prismerge::prisma_parser::{self, Column, Model, Schema};
use std::{fs, time::SystemTime};
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
    #[arg(long, short, value_name="PATH", help="The path to the Prisma schema file.")]
    schema_path: String,

    #[arg(long, short, value_name="PATH", default_value="./merged.db", help="The path of the merged database file.")]
    output_path: String,

    #[arg(long, short, action=ArgAction::SetFalse, help="After merging is complete, don't drop the temporary tables prismerge creates to keep track of old -> new foreign key mappings.")]
    keep_id_maps: bool,

    #[arg(long, short, value_name="NUMBER", default_value="1000", help="The minimum number of rows to insert at a time.")]
    min_inserts: u64,

    #[arg(value_name="INPUT PATHS", num_args=1.., required=true, help="Paths to the SQLite database files to merge.")]
    input_paths: Vec<String>,
}

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

fn main() -> Result<(), String> {
    let start_time = SystemTime::now();
    let options = CLI::parse();

    let source_code_str = fs::read_to_string(options.schema_path).unwrap();
    let source_code = source_code_str.as_str();
    let schema = prisma_parser::parse(source_code).unwrap();
    let order = schema.sorted();

    // open connections to all databases
    let connections: Vec<Connection> = options.input_paths[1..].iter().map(|path| Connection::open(path).unwrap()).collect();
    let merged = Connection::open(options.output_path).unwrap();

    merged.execute_batch(r#"
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -16000;
        PRAGMA foreign_keys = OFF;
    "#).unwrap();

    // set up merged database by copying over the schema
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

    for current_model in &order {
        merge_model(current_model, &schema, &connections, &merged, options.min_inserts);
    }

    merged.execute_batch(r#"
        PRAGMA synchronous = ON;
        PRAGMA journal_mode = DELETE;
        PRAGMA foreign_keys = ON;
    "#).unwrap();

    for current_model in &order {
        match verify_integrity(current_model, &merged) {
            Err(count) => println!("Table {} has {} foreign key integrity problems", current_model.name, count),
            _ => ()
        }
    }

    for current_model in order {
        drop_map_table(current_model, &merged);
    }

    vacuum(&merged);

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

fn vacuum(conn: &Connection) {
    conn.execute("VACUUM;", ()).unwrap();
}

fn drop_map_table(model: &Model, conn: &Connection) {
    conn.execute_batch(
        format!(r#"
                DROP INDEX IF EXISTS "{map_table_name}_old_id";
                DROP INDEX IF EXISTS "{map_table_name}_new_id";
                DROP INDEX IF EXISTS "{map_table_name}_new_id_old_id";
                DROP TABLE IF EXISTS "{map_table_name}";
            "#,
            map_table_name = model.map_table_name()
        ).as_str()
    ).unwrap();
}

fn verify_integrity(model: &Model, conn: &Connection) -> Result<(), usize> {
    let mut result: Result<(), usize> = Ok(());

    conn.query_row(format!("SELECT COUNT(*) FROM pragma_foreign_key_check('{}');", model.name).as_str(), (), |row| {
        let count = row.get::<_, usize>(0).unwrap();

        if count > 0 {
            result = Err(count);
        }

        Ok(())
    }).unwrap();

    result
}

fn merge_model(model: &Model, schema: &Schema, connections: &Vec<Connection>, merged: &Connection, min_inserts: u64) {
    let mut inserter = InsertManager::new(merged, min_inserts);

    let map_table_name = model.map_table_name();
    let create_map_table = format!(
        r#"
            CREATE TABLE {table} (
                old_id TEXT NOT NULL,
                new_id TEXT NOT NULL
            )
        "#,
        table = map_table_name
    );

    merged.execute(create_map_table.as_str(), ()).unwrap();

    let primary_key = model.primary_key().unwrap();
    let mut cols_to_copy: Vec<&Column> = vec![];

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

    // println!("Select query: {}", select_query);

    let mut check_sql_template: Option<String> = None;

    if let Some(unique) = &model.unique {
        let mut check_wheres: Vec<String> = vec![];
        let mut check_joins: Vec<String> = vec![];

        for (idx, name) in unique.column_names.iter().enumerate() {
            let col = model.get_col(name).unwrap();

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

    let mut sorted_connections: Vec<&Connection> = vec![primary];

    for conn in connections {
        if !core::ptr::eq(conn, primary) {
            sorted_connections.push(conn);
        }
    }

    let pb = ProgressBar::new(total_rows);

    pb.set_style(
        ProgressStyle::with_template(
            format!("{{spinner:.green}} {} [{{elapsed_precise}}] [{{wide_bar:.cyan/blue}}] {{pos}}/{{len}}", model.name).as_str()
        )
        .unwrap()
        .progress_chars("#>-"));

    for conn in sorted_connections {
        let is_primary = core::ptr::eq(conn, primary);
        let is_secondary = !is_primary;
        let mut stmt = conn.prepare(select_query.as_str()).unwrap();
        let mut rows = stmt.query(()).unwrap();

        loop {
            match rows.next() {
                Ok(Some(row)) => {
                    let old_pk: String = row.get(0).unwrap();
                    let mut existing_pk: Option<String> = None;

                    if is_secondary {
                        if let Some(check_sql_orig) = &check_sql_template {
                            let mut check_sql = check_sql_orig.clone();

                            for (idx, col) in model.unique.as_ref().unwrap().column_names.iter().enumerate() {
                                let value = row.get::<_, String>(col.as_str()).unwrap();
                                check_sql = check_sql.replace(&format!("?{}", idx + 1), &value);
                            }

                            match merged.query_row(check_sql.as_str(), (), |row| {
                                existing_pk = Some(row.get::<_, String>(0).unwrap());
                                Ok(())
                            }) {
                                Ok(_) => (),
                                Err(_) => ()
                            }
                        }
                    }

                    if let Some(existing_id) = existing_pk {
                        let id_map_insert = format!(
                            "INSERT INTO \"{table}\" (old_id, new_id) VALUES ('{old_pk}', {existing_id})",
                            table = map_table_name,
                            old_pk = old_pk,
                            existing_id = existing_id
                        );

                        // Even though this is an INSERT into the ID map table, it represents an actual row.
                        // We're skipping because it already exists, so we call insert() instead of insert_supporting()
                        // to count it towards merge progress.
                        pb.inc(inserter.insert(id_map_insert));

                        continue;
                    }

                    // Re-use old pk so we don't have to update the map table
                    let new_pk = if is_primary {
                        old_pk.clone()
                    } else {
                        Uuid::new_v4().to_string()
                    };

                    let mut select_values: Vec<String> = vec![format!("'{}'", new_pk)];
                    let mut select_columns: Vec<&str> = vec![primary_key.name.as_str()];
                    let mut join_statements: Vec<String> = vec![];
                    let mut field_index = 2;

                    for column in model.columns.iter() {
                        if let Some(related_column) = column.get_related_column(&model) {
                            let old_id: String = row.get(field_index).unwrap();
                            field_index += 1;
                            select_values.push(format!(
                                "COALESCE({}_id_map.new_id, {})",
                                related_column.ty.name,
                                old_id
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

                    pb.inc(inserter.insert(insert_sql));

                    let id_map_insert = format!(
                        "INSERT INTO \"{table}\" (old_id, new_id) VALUES ('{old_id}', '{new_id}')",
                        table = map_table_name,
                        old_id = old_pk,
                        new_id = new_pk
                    );

                    pb.inc(inserter.insert_supporting(id_map_insert));
                }

                Ok(None) => break,
                Err(_) => continue
            }
        }

        pb.inc(inserter.flush());
    }

    pb.inc(inserter.flush());

    let map_table_indices = [
        format!("CREATE INDEX \"{table}_id_map_old_id\" ON \"{table}\"(\"old_id\");", table = model.name),
        format!("CREATE INDEX \"{table}_id_map_new_id\" ON \"{table}\"(\"new_id\");", table = model.name),
        format!("CREATE INDEX \"{table}_id_map_new_id_old_id\" ON \"{table}\"(\"new_id\", \"old_id\");", table = model.name)
    ];

    for map_table_index in map_table_indices {
        merged.execute(&map_table_index, ()).unwrap();
    }

    pb.finish();
}
