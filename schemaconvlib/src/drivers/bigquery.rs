//! Driver for working with BigQuery schemas.

use serde_json;
use std::io::Write;

use Result;
use table::{DataType, Table};

/// A BigQuery column declaration.
#[derive(Debug, Eq, PartialEq, Serialize)]
struct ColumnSchema {
    /// An optional description of the BigQuery column.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    /// The name of the BigQuery column.
    name: String,

    /// The type of the BigQuery column.
    #[serde(rename = "type")]
    ty: String,

    // The mode of the column: Is it nullable?
    mode: Mode,
}

/// A column mode.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Mode {
    /// This column is `NOT NULL`.
    Required,

    /// This column can contain `NULL` values.
    Nullable,
}

/// A driver for working with BigQuery.
pub struct BigQueryDriver;

impl BigQueryDriver {
    /// Write out a table's columns as BigQuery schema JSON.
    pub fn write_json(f: &mut Write, table: &Table) -> Result<()> {
        let mut cols = vec![];
        for col in &table.columns {
            cols.push(ColumnSchema {
                name: col.name.to_owned(),
                description: None,
                ty: bigquery_type(&col.name, &col.data_type, false)?,
                mode: if col.is_nullable {
                    Mode::Nullable
                } else {
                    Mode::Required
                }
            });
        }
        serde_json::to_writer_pretty(f, &cols)?;
        Ok(())
    }
}

/// Convert `DataType` to a BigQuery type. See
/// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.
fn bigquery_type(
    column_name: &str,
    data_type: &DataType,
    inside_array: bool,
) -> Result<String> {
    match data_type {
        DataType::Array(nested) => {
            let bq_nested = bigquery_type(column_name, nested, true)?;
            if inside_array {
                Ok(format!("ARRAY<{}>", bq_nested))
            } else {
                Ok(format!("STRUCT<ARRAY<{}>>", bq_nested))
            }
        }
        DataType::Bigint => Ok("INT64".to_owned()),
        DataType::Boolean => Ok("BOOL".to_owned()),
        DataType::CharacterVarying => Ok("STRING".to_owned()),
        DataType::Date => Ok("DATE".to_owned()),
        DataType::DoublePrecision => Ok("FLOAT64".to_owned()),
        DataType::Integer => Ok("INT64".to_owned()),
        DataType::Json => Ok("STRING".to_owned()),
        DataType::Jsonb => Ok("STRING".to_owned()),
        DataType::Numeric => Ok("DECIMAL".to_owned()),
        DataType::Other(unknown_type) => {
            eprintln!(
                "WARNING: Converting unknown type {:?} of {:?} to STRING",
                unknown_type,
                column_name,
            );
            Ok("STRING".to_owned())
        }
        DataType::Real => Ok("FLOAT64".to_owned()),
        DataType::Smallint => Ok("INT64".to_owned()),
        DataType::Text => Ok("STRING".to_owned()),
        // Timestamps without timezones will be interpreted as UTC.
        DataType::TimestampWithoutTimeZone => Ok("TIMESTAMP".to_owned()),
        // As far as I can tell, BigQuery will convert timestamps with timezones
        // to UTC.
        DataType::TimestampWithTimeZone => Ok("TIMESTAMP".to_owned()),
        DataType::Uuid => Ok("STRING".to_owned()),
    }
}
