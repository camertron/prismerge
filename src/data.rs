use rusqlite::Connection;
use tap::prelude::*;
use topological_sort::TopologicalSort;
use std::{collections::HashMap, hash::Hash};

#[derive(Debug)]
pub struct Relation {
    pub fields: Vec<String>,
    pub references: Vec<String>
}

#[derive(Debug)]
pub struct ColumnType {
    pub name: String,
    pub collection: bool,
    pub nullable: bool
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub relation: Option<Relation>,
    pub unique: bool,
    pub primary_key: bool
}

impl Column {
    pub fn has_relation(self: &Self) -> bool {
        self.relation.is_some()
    }

    pub fn get_related_column<'a>(self: &Self, model: &'a Model) -> Option<&'a Column> {
        for column in model.columns.iter() {
            if let Some(relation) = &column.relation {
                if relation.fields.contains(&self.name) {
                    return Some(column);
                }
            }
        }

        None
    }

    pub fn quoted(self: &Self, model_name: &String) -> String {
        format!("quote(\"{}\".\"{}\")", model_name, self.name)
    }

    pub fn is_regular(self: &Self, schema: &Schema) -> bool {
        !self.primary_key &&
            !self.ty.collection &&
            !self.has_relation() &&
            !schema.models.contains_key(&self.ty.name)
    }
}

#[derive(Debug)]
pub struct Unique {
    pub column_names: Vec<String>
}

#[derive(Debug)]
pub struct Model {
    pub name: String,
    pub columns: Vec<Column>,
    pub unique: Option<Unique>,
    pub map_table: MapTable,
    pub primary_key_index: Option<usize>
}

impl Model {
    pub fn new(name: String, columns: Vec<Column>, unique: Option<Unique>) -> Self {
        let mut primary_key_index: Option<usize> = None;

        for (idx, column) in columns.iter().enumerate() {
            if column.primary_key {
                primary_key_index = Some(idx)
            }
        }

        Model {
            name: name.clone(),
            columns,
            unique,
            map_table: MapTable::new(name),
            primary_key_index: primary_key_index
        }
    }

    pub fn primary_key(self: &Self) -> Option<&Column> {
        if let Some(idx) = self.primary_key_index {
            return Some(&self.columns[idx]);
        }

        None
    }

    // Return the column with the given name.
    pub fn get_col(self: &Self, name: &str) -> Option<&Column> {
        for column in self.columns.iter() {
            if column.name == name {
                return Some(column);
            }
        }

        None
    }

    // Check that all foreign keys point to existing records. Returns the count of
    // rows that have bad/missing foreign keys.
    pub fn verify_integrity(self: &Self, conn: &Connection) -> Result<(), usize> {
        let mut result: Result<(), usize> = Ok(());

        conn.query_row(format!("SELECT COUNT(*) FROM pragma_foreign_key_check('{}');", self.name).as_str(), (), |row| {
            let count = row.get::<_, usize>(0).unwrap();

            if count > 0 {
                result = Err(count);
            }

            Ok(())
        }).unwrap();

        result
    }
}

impl Hash for Model {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Model {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Model {
}

#[derive(Debug)]
pub struct MapTable {
    pub name: String
}

impl MapTable {
    fn new(model_name: String) -> Self {
        MapTable { name: format!("{}_id_map", model_name) }
    }

    pub fn create_into(self: &Self, connection: &Connection) {
        let create_map_table_sql = format!(
            r#"
                CREATE TABLE {table} (
                    old_id TEXT NOT NULL,
                    new_id TEXT NOT NULL
                )
            "#,
            table = self.name
        );

        connection.execute(create_map_table_sql.as_str(), ()).unwrap();
    }

    pub fn drop_from(self: &Self, connection: &Connection) {
        connection.execute_batch(
            format!(r#"
                    DROP INDEX IF EXISTS "{table}_old_id";
                    DROP INDEX IF EXISTS "{table}_new_id";
                    DROP INDEX IF EXISTS "{table}_new_id_old_id";
                    DROP TABLE IF EXISTS "{table}";
                "#,
                table = self.name
            ).as_str()
        ).unwrap();
    }

    pub fn create_indices(self: &Self, connection: &Connection) {
        let query = format!(
            r#"
                CREATE INDEX "{table}_old_id" ON "{table}"("old_id");
                CREATE INDEX "{table}_new_id" ON "{table}"("new_id");
                CREATE INDEX "{table}_new_id_old_id" ON "{table}"("new_id", "old_id");
            "#,
            table = self.name
        );

        connection.execute_batch(query.as_str()).unwrap();
    }
}

#[derive(Debug)]
pub struct Schema {
    pub models: HashMap<String, Model>
}

impl Schema {
    pub fn new() -> Self {
        Schema { models: HashMap::new() }
    }

    pub fn sorted(self: &Self) -> Vec<&Model> {
        let mut ts = TopologicalSort::<&Model>::new();

        for (_name, model) in self.models.iter() {
            ts.insert(model);

            for column in &model.columns {
                if column.relation.is_some() {
                    if let Some(child_model) = self.models.get(&column.ty.name) {
                        ts.add_dependency(model, child_model);
                    }
                }
            }
        }

        ts
            .collect::<Vec<&Model>>()
            .tap_mut(|order| order.reverse())
    }
}
