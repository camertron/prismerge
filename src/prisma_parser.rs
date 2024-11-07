use rusqlite::Connection;
use tree_sitter_prisma_io;
use tree_sitter::{Node, Parser};
use std::{collections::HashMap, hash::Hash};
use topological_sort::TopologicalSort;
use tap::prelude::*;

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

struct Cursor<'a> {
    source: &'a str,
    nodes: Vec<Node<'a>>,
    idx: usize,
}

impl<'a> Cursor<'a> {
    fn new(nodes: Vec<Node<'a>>, source: &'a str) -> Self {
        Cursor { source, nodes, idx: 0 }
    }

    fn consume(self: &mut Self, kind: &str) -> Result<(), String> {
        if self.current().kind() == kind {
            self.idx += 1;
            Ok(())
        } else {
            Err(format!("Expected {}, got {}", kind, self.current().kind()))
        }
    }

    fn consume_all(self: &mut Self, kinds: &[&str]) -> Result<(), String> {
        for kind in kinds.iter() {
            self.consume(kind)?;
        }

        Ok(())
    }

    fn try_consume(self: &mut Self, kind: &str) -> bool {
        match self.consume(kind) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    fn try_consume_all(self: &mut Self, kinds: &[&str]) -> bool {
        for kind in kinds.iter() {
            if !self.try_consume(kind) {
                return false
            }
        }

        true
    }

    fn skip(self: &mut Self) {
        self.idx += 1;
    }

    fn current(self: &Self) -> &Node {
        &self.nodes[self.idx]
    }

    fn eos(self: &Self) -> bool {
        self.idx >= self.nodes.len()
    }
}

pub fn parse(schema_str: &str) -> Result<Schema, String> {
    let mut parser = Parser::new();
    parser.set_language(tree_sitter_prisma_io::language()).expect("Error loading prisma grammar");

    let tree = parser.parse(schema_str, None).unwrap();
    let mut schema = Schema::new();
    let nodes = gather_nodes(tree.root_node());
    let mut cursor = Cursor::new(nodes, schema_str);

    cursor.consume("program")?;

    while !cursor.eos() {
        match cursor.current().kind() {
            "model_declaration" => {
                let model = handle_model_decl(&mut cursor)?;
                schema.models.insert(model.name.clone(), model);
            }

            _ => cursor.skip()
        }
    }

    Ok(schema)
}

fn handle_model_decl(cursor: &mut Cursor) -> Result<Model, String> {
    cursor.consume("model_declaration")?;
    cursor.consume("model")?;

    let mut columns = vec![];
    let mut unique: Option<Unique> = None;
    let name = handle_identifier(cursor)?;

    if cursor.current().kind() == "statement_block" {
        cursor.consume("statement_block")?;
        cursor.consume("{")?;

        loop {
            match cursor.current().kind() {
                "column_declaration" => columns.push(handle_column_decl(cursor)?),
                "block_attribute_declaration" => {
                    cursor.consume("block_attribute_declaration")?;

                    if cursor.try_consume_all(&["@@", "call_expression"]) {
                        let method_name = handle_identifier(cursor)?;

                        if method_name == "unique" {
                            unique = Some(handle_unique(cursor)?);
                        }
                    }
                }

                "}" => break,
                _ => cursor.skip()
            }
        }
    }

    if unique.is_none() {
        for column in &columns {
            if column.unique {
                unique = Some(Unique { column_names: vec![column.name.clone()] });
                break;
            }
        }
    }

    Ok(Model::new(name, columns, unique))
}

fn handle_unique(cursor: &mut Cursor) -> Result<Unique, String> {
    let mut args = handle_args(cursor)?;
    let column_names = args.remove("fields").unwrap_or_else(|| vec![]);
    Ok(Unique { column_names })
}

fn handle_identifier(cursor: &mut Cursor) -> Result<String, String> {
    let current = cursor.current();
    let identifier = &cursor.source[current.start_byte()..current.end_byte()];
    cursor.consume("identifier")?;
    Ok(identifier.to_string())
}

fn handle_column_type(cursor: &mut Cursor) -> Result<ColumnType, String> {
    cursor.consume("column_type")?;

    let name = handle_identifier(cursor)?;
    let mut collection = false;
    let mut nullable = false;

    match cursor.current().kind() {
        "array" => {
            collection = true;
            cursor.consume_all(&["array", "[", "]"])?;
        }

        "maybe" => {
            nullable = true;
            cursor.consume("maybe")?;
        },

        _ => ()
    }

    Ok(ColumnType { name, collection, nullable })
}

fn handle_column_decl(cursor: &mut Cursor) -> Result<Column, String> {
    cursor.consume("column_declaration")?;

    let name = handle_identifier(cursor)?;
    let ty = handle_column_type(cursor)?;
    let mut relation: Option<Relation> = None;
    let mut unique = false;
    let mut primary_key = false;

    if cursor.try_consume_all(&["attribute", "@"]) {
        match cursor.current().kind() {
            "call_expression" => {
                cursor.consume("call_expression")?;

                if handle_identifier(cursor)? == "relation" {
                    relation = Some(handle_relation(cursor)?);
                }
            }

            "identifier" => {
                match handle_identifier(cursor)?.as_str() {
                    "unique" => unique = true,
                    "id" => primary_key = true,
                    _ => ()
                }
            },

            _ => ()
        }
    }

    Ok(Column { name: name, ty, relation, unique, primary_key })
}

fn handle_relation(cursor: &mut Cursor) -> Result<Relation, String> {
    let mut args = handle_args(cursor)?;
    let fields = args.remove("fields").unwrap_or_else(|| vec![]);
    let references = args.remove("references").unwrap_or_else(|| vec![]);
    Ok(Relation { fields, references })
}

fn handle_args(cursor: &mut Cursor) -> Result<HashMap<String, Vec<String>>, String> {
    let mut paren_count = 1;
    let mut args: HashMap<String, Vec<String>> = HashMap::new();

    if cursor.current().kind() != "arguments" {
        return Ok(args);
    }

    cursor.consume("arguments")?;
    cursor.consume("(")?;

    while paren_count > 0 {
        if cursor.try_consume("type_expression") {
            let key = handle_identifier(cursor)?;

            cursor.consume(":")?;

            match cursor.current().kind() {
                "array" => { args.insert(key, handle_array(cursor)?); },
                _ => ()
            };
        } else if cursor.try_consume("(") {
            paren_count += 1;
        } else if cursor.try_consume(")") {
            paren_count -= 1;
        } else {
            cursor.skip();
        }
    }

    Ok(args)
}

fn handle_array(cursor: &mut Cursor) -> Result<Vec<String>, String> {
    cursor.consume("array")?;
    cursor.consume("[")?;

    let mut arr = vec![];

    while cursor.current().kind() != "]" {
        arr.push(handle_identifier(cursor)?);
        cursor.try_consume(",");
    }

    cursor.consume("]")?;
    Ok(arr)
}

fn gather_nodes(root: Node) -> Vec<Node> {
    let mut cursor = root.walk();
    let mut nodes = vec![root];
    let children: Vec<Node> = root.children(&mut cursor).collect();

    for child in children {
        let mut child_nodes= gather_nodes(child);
        nodes.append(&mut child_nodes);
    }

    nodes
}
