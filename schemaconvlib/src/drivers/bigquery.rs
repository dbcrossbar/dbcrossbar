//! Driver for working with BigQuery schemas.

use serde::{Serialize, Serializer};
use serde_json;
use std::{fmt, io::Write, result};

use table::{Column, DataType, Table};
use Result;

/// A BigQuery type.
#[derive(Debug, Eq, PartialEq)]
enum Type {
    /// An array type. May not contain another directly nested array inside
    /// it. Use a nested struct with only one field instead.
    Array(NonArrayType),
    /// A non-array type.
    NonArray(NonArrayType),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Array(element_type) => write!(f, "ARRAY<{}>", element_type),
            Type::NonArray(ty) => write!(f, "{}", ty),
        }
    }
}

impl Serialize for Type {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a string and serialize that.
        format!("{}", self).serialize(serializer)
    }
}

/// Any type except `ARRAY` (which cannot be nested in another `ARRAY`).
#[derive(Debug, Eq, PartialEq)]
#[allow(dead_code)]
enum NonArrayType {
    Bool,
    Bytes,
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Numeric,
    String,
    Struct(Vec<StructField>),
    Time,
    Timestamp,
}

impl fmt::Display for NonArrayType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NonArrayType::Bool => write!(f, "BOOL"),
            NonArrayType::Bytes => write!(f, "BYTES"),
            NonArrayType::Date => write!(f, "DATE"),
            NonArrayType::Datetime => write!(f, "DATETIME"),
            NonArrayType::Float64 => write!(f, "FLOAT64"),
            NonArrayType::Geography => write!(f, "GEOGRAPHY"),
            NonArrayType::Int64 => write!(f, "INT64"),
            NonArrayType::Numeric => write!(f, "NUMERIC"),
            NonArrayType::String => write!(f, "STRING"),
            NonArrayType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                let mut is_first = true;
                for field in fields {
                    if is_first {
                        is_first = false;
                    } else {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ">")
            }
            NonArrayType::Time => write!(f, "TIME"),
            NonArrayType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

/// A field of a `STRUCT`.
#[derive(Debug, Eq, PartialEq)]
struct StructField {
    name: Option<String>,
    ty: Type,
}

impl fmt::Display for StructField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.name {
            // TODO: It's not clear whether we can/should escape this using
            // `Ident` to insert backticks.
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.ty)
    }
}

/// A BigQuery column declaration.
#[derive(Debug, Eq, PartialEq, Serialize)]
struct BqColumn {
    /// An optional description of the BigQuery column.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    /// The name of the BigQuery column.
    name: String,

    /// The type of the BigQuery column.
    #[serde(rename = "type")]
    ty: Type,

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

/// A BigQuery identifier, for formatting purposes.
struct Ident<'a>(&'a str);

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0.contains('`') {
            error!("cannot output BigQuery identifier containing backtick (`)");
            Err(fmt::Error)
        } else {
            write!(f, "`{}`", self.0)
        }
    }
}

/// A driver for working with BigQuery.
pub struct BigQueryDriver;

impl BigQueryDriver {
    /// Write out a table's columns as BigQuery schema JSON. If you set
    /// `csv_compatible`, this will only use types that can be loaded from a CSV
    /// file.
    pub fn write_json(
        f: &mut Write,
        table: &Table,
        csv_compatible: bool,
    ) -> Result<()> {
        let mut cols = vec![];
        for col in &table.columns {
            cols.push(bigquery_column(col, csv_compatible)?);
        }
        serde_json::to_writer_pretty(f, &cols)?;
        Ok(())
    }

    /// Generate SQL which `SELECT`s from a temp table, and fixes the types
    /// of columns that couldn't be imported from CSVs.
    pub fn write_import_sql(f: &mut Write, table: &Table) -> Result<()> {
        for (i, col) in table.columns.iter().enumerate() {
            write_col_import_udf(f, i, col)?;
        }
        write!(f, "SELECT ")?;
        for (i, col) in table.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write_col_import_sql(f, i, col)?;
        }
        write!(f, " FROM {}", Ident(&table.name))?;
        Ok(())
    }
}

/// Build a BigQuery column schema.
fn bigquery_column(col: &Column, csv_compatible: bool) -> Result<BqColumn> {
    Ok(BqColumn {
        name: col.name.to_owned(),
        description: None,
        ty: bigquery_type(&col.name, &col.data_type, csv_compatible)?,
        mode: if col.is_nullable {
            Mode::Nullable
        } else {
            Mode::Required
        },
    })
}

/// Convert `DataType` to a BigQuery type. See
/// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.
fn bigquery_type(
    column_name: &str,
    data_type: &DataType,
    csv_compatible: bool,
) -> Result<Type> {
    match data_type {
        // Arrays cannot be directly loaded from a CSV file, according to the
        // docs. So if we're working with CSVs, output them as STRING.
        DataType::Array(_) if csv_compatible => {
            Ok(Type::NonArray(NonArrayType::String))
        }
        DataType::Array(nested) => {
            let bq_nested =
                bigquery_non_array_type(column_name, nested, csv_compatible)?;
            Ok(Type::Array(bq_nested))
        }
        other => {
            let bq_other =
                bigquery_non_array_type(column_name, other, csv_compatible)?;
            Ok(Type::NonArray(bq_other))
        }
    }
}

/// Convert `DataType` to any non-`ARRAY` BigQuery type, because `ARRAY` can't
/// nest.
fn bigquery_non_array_type(
    column_name: &str,
    data_type: &DataType,
    csv_compatible: bool,
) -> Result<NonArrayType> {
    match data_type {
        // We should only be able to get here if we're nested inside another
        // `Array`, but the top-level `ARRAY` should already have been converted
        // to a `STRING`.
        DataType::Array(_) if csv_compatible => Err(format_err!(
            "should never encounter nested arrays in CSV mode"
        )),
        DataType::Array(nested) => {
            let bq_nested =
                bigquery_non_array_type(column_name, nested, csv_compatible)?;
            let field = StructField {
                name: None,
                ty: Type::Array(bq_nested),
            };
            Ok(NonArrayType::Struct(vec![field]))
        }
        DataType::Bool => Ok(NonArrayType::Bool),
        DataType::Date => Ok(NonArrayType::Date),
        DataType::Decimal => Ok(NonArrayType::Numeric),
        DataType::Float32 => Ok(NonArrayType::Float64),
        DataType::Float64 => Ok(NonArrayType::Float64),
        DataType::GeoJson => Ok(NonArrayType::Geography),
        DataType::Int16 => Ok(NonArrayType::Int64),
        DataType::Int32 => Ok(NonArrayType::Int64),
        DataType::Int64 => Ok(NonArrayType::Int64),
        DataType::Json => Ok(NonArrayType::String),
        DataType::Other(unknown_type) => {
            eprintln!(
                "WARNING: Converting unknown type {:?} of {:?} to STRING",
                unknown_type, column_name,
            );
            Ok(NonArrayType::String)
        }
        DataType::Text => Ok(NonArrayType::String),
        // Timestamps without timezones will be interpreted as UTC.
        DataType::TimestampWithoutTimeZone => Ok(NonArrayType::Timestamp),
        // As far as I can tell, BigQuery will convert timestamps with timezones
        // to UTC.
        DataType::TimestampWithTimeZone => Ok(NonArrayType::Timestamp),
        DataType::Uuid => Ok(NonArrayType::String),
    }
}

#[test]
fn nested_arrays() {
    let input = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
        Box::new(DataType::Int32),
    )))));

    // What we expect when loading from a CSV file.
    let bq = bigquery_type("col", &input, true).unwrap();
    assert_eq!(format!("{}", bq), "STRING");

    // What we expect in the final BigQuery table.
    let bq = bigquery_type("col", &input, false).unwrap();
    assert_eq!(
        format!("{}", bq),
        "ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<INT64>>>>>"
    );
}

/// Output JavaScript UDF for importing a column (if necessary). This can
/// be used to patch up types that can't be loaded directly from a CSV.
fn write_col_import_udf(f: &mut Write, idx: usize, col: &Column) -> Result<()> {
    if let DataType::Array(_) = &col.data_type {
        let bq_type = bigquery_type(&col.name, &col.data_type, false)?;
        write!(
            f,
            r#"CREATE TEMP FUNCTION ImportJson_{idx}(input STRING)
RETURNS {bq_type}
LANGUAGE js AS """
return JSON.parse(input);
""";

"#,
            idx = idx,
            bq_type = bq_type,
        );
    }
    Ok(())
}

/// Output SQL for importing a column.
fn write_col_import_sql(f: &mut Write, idx: usize, col: &Column) -> Result<()> {
    let ident = Ident(&col.name);
    if let DataType::Array(_) = &col.data_type {
        write!(
            f,
            "ImportJson_{idx}({ident}) AS {ident}",
            idx = idx,
            ident = ident,
        )?;
    } else {
        write!(f, "{}", ident)?;
    }
    Ok(())
}
