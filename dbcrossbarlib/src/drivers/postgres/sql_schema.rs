//! Parser for PostgreSQL `CREATE TABLE` declarations.

use failure::{format_err, ResultExt};
use std::io::prelude::*;

use crate::schema::{Column, DataType, Table};
use crate::{IfExists, Result};

/// Include our `rust-peg` grammar.
///
/// We disable lots of clippy warnings because this is machine-generated code.
#[allow(clippy::all, rust_2018_idioms, elided_lifetimes_in_paths)]
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/postgres.rs"));
}

/// Parse a PostgreSQL `CREATE TABLE` statement and return a `Table`.
pub(crate) fn parse_create_table(input: &str) -> Result<Table> {
    Ok(grammar::create_table(input)
        .context("error parsing Postgres `CREATE TABLE`")?)
}

/// Write out a Postgres `CREATE TABLE` statement based on `table`.
pub(crate) fn write_create_table(
    out: &mut Write,
    table: &Table,
    if_exists: IfExists,
) -> Result<()> {
    match if_exists {
        IfExists::Error | IfExists::Overwrite => {
            writeln!(out, "CREATE TABLE {:?} (", table.name)?;
        }
        IfExists::Append => {
            writeln!(out, "CREATE TABLE IF NOT EXISTS {:?} (", table.name)?;
        }
    }
    for (idx, col) in table.columns.iter().enumerate() {
        write!(out, "    {:?} ", col.name)?;
        write_data_type(out, col, &col.data_type, false)?;
        if !col.is_nullable {
            write!(out, " NOT NULL")?;
        }
        if idx + 1 == table.columns.len() {
            writeln!(out)?;
        } else {
            writeln!(out, ",")?;
        }
    }
    writeln!(out, ");")?;
    Ok(())
}

/// Write out the data type of a column.
fn write_data_type(
    out: &mut Write,
    col: &Column,
    data_type: &DataType,
    in_array: bool,
) -> Result<()> {
    match data_type {
        DataType::Array(_) if in_array => {
            return Err(format_err!(
                "nested array in column {} unsupported",
                col.name
            ));
        }
        DataType::Array(nested) => {
            write_data_type(out, col, nested, true)?;
            write!(out, "[]")?;
        }
        DataType::Bool => write!(out, "boolean")?,
        DataType::Date => write!(out, "date")?,
        DataType::Decimal => write!(out, "numeric")?,
        DataType::Float32 => write!(out, "real")?,
        DataType::Float64 => write!(out, "double precision")?,
        DataType::GeoJson => write!(out, "public.geometry(Geometry, 4326)")?,
        DataType::Int16 => write!(out, "smallint")?,
        DataType::Int32 => write!(out, "int")?,
        DataType::Int64 => write!(out, "bigint")?,
        DataType::Json => write!(out, "jsonb")?,
        DataType::Other(name) => {
            return Err(format_err!(
                "don't know how to output column type {:?}",
                name
            ));
        }
        DataType::Text => write!(out, "text")?,
        DataType::TimestampWithoutTimeZone => {
            write!(out, "timestamp without time zone")?
        }
        DataType::TimestampWithTimeZone => write!(out, "timestamp with time zone")?,
        DataType::Uuid => write!(out, "uuid")?,
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::{Column, DataType};

    use std::str;

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

        // Now try writing and re-reading.
        let mut out = vec![];
        write_create_table(&mut out, &table, IfExists::Error)
            .expect("error writing table");
        let parsed_again = parse_create_table(&str::from_utf8(&out).unwrap())
            .expect("error parsing table");
        assert_eq!(parsed_again, expected);
    }
}
