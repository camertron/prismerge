use tree_sitter_prisma_io;
use tree_sitter::{Node, Parser};
use std::collections::HashMap;

use crate::data::{
    Column,
    ColumnType,
    Model,
    Relation,
    Schema,
    Unique
};

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
