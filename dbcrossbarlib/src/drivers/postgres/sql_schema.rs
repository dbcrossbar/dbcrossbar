//! Parser for PostgreSQL `CREATE TABLE` declarations.

use failure::ResultExt;

use crate::schema::Table;
use crate::Result;

/// Include our `rust-peg` grammar.
///
/// We disable lots of clippy warnings because this is machine-generated code.
#[allow(clippy::all, rust_2018_idioms, elided_lifetimes_in_paths)]
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/postgres.rs"));
}

/// Parse a PostgreSQL `CREATE TABLE` statement and return a `Table`.
pub fn parse_create_table(input: &str) -> Result<Table> {
    Ok(grammar::create_table(input)
        .context("error parsing Postgres `CREATE TABLE`")?)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::{Column, DataType};

    #[test]
    fn simple_table() {
        let input = include_str!("postgres_example.sql");
        let table = parse_create_table(input).unwrap();
        let expected = Table {
            name: "example".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    is_nullable: true,
                    data_type: DataType::Text,
                    comment: None,
                },
                Column {
                    name: "b".to_string(),
                    is_nullable: true,
                    data_type: DataType::Int32,
                    comment: None,
                },
                Column {
                    name: "c".to_string(),
                    is_nullable: false,
                    data_type: DataType::Uuid,
                    comment: None,
                },
                Column {
                    name: "d".to_string(),
                    is_nullable: true,
                    data_type: DataType::Date,
                    comment: None,
                },
                Column {
                    name: "e".to_string(),
                    is_nullable: true,
                    data_type: DataType::Float64,
                    comment: None,
                },
                Column {
                    name: "f".to_string(),
                    is_nullable: true,
                    data_type: DataType::Array(Box::new(DataType::Text)),
                    comment: None,
                },
                Column {
                    name: "g".to_string(),
                    is_nullable: true,
                    data_type: DataType::Array(Box::new(DataType::Int32)),
                    comment: None,
                },
                Column {
                    name: "h".to_string(),
                    is_nullable: true,
                    data_type: DataType::GeoJson,
                    comment: None,
                },
                Column {
                    name: "i".to_string(),
                    is_nullable: true,
                    data_type: DataType::Text,
                    comment: None,
                },
                Column {
                    name: "j".to_string(),
                    is_nullable: true,
                    data_type: DataType::Int16,
                    comment: None,
                },
                Column {
                    name: "k".to_string(),
                    is_nullable: true,
                    data_type: DataType::TimestampWithoutTimeZone,
                    comment: None,
                },
            ],
        };
        assert_eq!(table, expected);
    }
}
