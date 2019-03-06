//! Data types supported BigQuery.

use serde::{Serialize, Serializer};
use std::{fmt, result};

use crate::common::*;
use crate::schema::DataType;

/// Extensions to `DataType` (the portable version) to handle BigQuery-query
/// specific stuff.
pub(crate) trait DataTypeBigQueryExt {
    /// Can BigQuery import this type from a CSV file?
    fn bigquery_can_import_from_csv(&self) -> Result<bool>;
}

impl DataTypeBigQueryExt for DataType {
    fn bigquery_can_import_from_csv(&self) -> Result<bool> {
        // Convert this to the corresponding BigQuery type and check that.
        let bq_data_type = BqDataType::for_data_type(self, Usage::FinalTable)?;
        Ok(bq_data_type.bigquery_can_import_from_csv())
    }
}

/// How do we intend to use a BigQuery type?
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Usage {
    /// We intend to use this type for loading from a CSV, which means we can't
    /// that certain data types will need to be treated as `STRING`.
    CsvLoad,

    /// We intend to use the type for
    FinalTable,
}

/// A BigQuery data type.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum BqDataType {
    /// An array type. May not contain another directly nested array inside
    /// it. Use a nested struct with only one field instead.
    Array(BqNonArrayDataType),
    /// A non-array type.
    NonArray(BqNonArrayDataType),
}

impl BqDataType {
    /// Give a database-independent `DataType`, and the intended usage within
    /// BigQuery, map it to a corresponding `BqDataType`.
    ///
    /// See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.
    pub(crate) fn for_data_type(
        data_type: &DataType,
        usage: Usage,
    ) -> Result<BqDataType> {
        match (data_type, usage) {
            // Arrays cannot be directly loaded from a CSV file, according to the
            // docs. So if we're working with CSVs, output them as STRING.
            (DataType::Array(_), Usage::CsvLoad) => {
                Ok(BqDataType::NonArray(BqNonArrayDataType::String))
            }
            (DataType::Array(nested), _) => {
                match nested.as_ref() {
                    DataType::Json => {
                        return Err(format_err!(
                            "cannot represent arrays of JSON in BigQuery yet"
                        ));
                    }
                    _ => {}
                }
                let bq_nested = BqNonArrayDataType::for_data_type(nested, usage)?;
                Ok(BqDataType::Array(bq_nested))
            }
            (other, _) => {
                let bq_other = BqNonArrayDataType::for_data_type(other, usage)?;
                Ok(BqDataType::NonArray(bq_other))
            }
        }
    }

    /// Can BigQuery import this type from a CSV file?
    pub(crate) fn bigquery_can_import_from_csv(&self) -> bool {
        match self {
            BqDataType::Array(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for BqDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqDataType::Array(element_type) => write!(f, "ARRAY<{}>", element_type),
            BqDataType::NonArray(ty) => write!(f, "{}", ty),
        }
    }
}

impl Serialize for BqDataType {
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
pub(crate) enum BqNonArrayDataType {
    Bool,
    Bytes,
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Numeric,
    String,
    Struct(Vec<BqStructField>),
    Time,
    Timestamp,
}

impl BqNonArrayDataType {
    /// Give a database-independent `DataType`, and the intended usage within
    /// BigQuery, map it to a corresponding `BqNonArrayDataType`.
    ///
    /// If this is passed an array data type, it will do one of two things:
    ///
    /// 1. If we have `Usage::CsvLoad`, we will fail, because nested array types
    ///    should never occur in CSV mode.
    /// 2. Otherwise, we will assume we're dealing with a nested array, which
    ///    means that we need to wrap it in a single-element
    ///    `BqNonArrayDataType::Struct`, because BigQuery always needs to have
    ///    `ARRAY<STRUCT<ARRAY<...>>` instead of `ARRAY<ARRAY<...>>`.
    ///
    /// Getting (2) right is the whole reason for separating `BqDataType` and
    /// `BqNonArrayDataType`.
    fn for_data_type(
        data_type: &DataType,
        usage: Usage,
    ) -> Result<BqNonArrayDataType> {
        match data_type {
            // We should only be able to get here if we're nested inside another
            // `Array`, but the top-level `ARRAY` should already have been converted
            // to a `STRING`.
            DataType::Array(_) if usage == Usage::CsvLoad => Err(format_err!(
                "should never encounter nested arrays in CSV mode"
            )),
            DataType::Array(nested) => {
                let bq_nested = BqNonArrayDataType::for_data_type(nested, usage)?;
                let field = BqStructField {
                    name: None,
                    ty: BqDataType::Array(bq_nested),
                };
                Ok(BqNonArrayDataType::Struct(vec![field]))
            }
            DataType::Bool => Ok(BqNonArrayDataType::Bool),
            DataType::Date => Ok(BqNonArrayDataType::Date),
            DataType::Decimal => Ok(BqNonArrayDataType::Numeric),
            DataType::Float32 => Ok(BqNonArrayDataType::Float64),
            DataType::Float64 => Ok(BqNonArrayDataType::Float64),
            DataType::GeoJson => Ok(BqNonArrayDataType::Geography),
            DataType::Int16 => Ok(BqNonArrayDataType::Int64),
            DataType::Int32 => Ok(BqNonArrayDataType::Int64),
            DataType::Int64 => Ok(BqNonArrayDataType::Int64),
            DataType::Json => Ok(BqNonArrayDataType::String),
            // Unknown types will become strings.
            DataType::Other(_unknown_type) => Ok(BqNonArrayDataType::String),
            DataType::Text => Ok(BqNonArrayDataType::String),
            // Timestamps without timezones will be mapped to `DATETIME`.
            DataType::TimestampWithoutTimeZone => Ok(BqNonArrayDataType::Datetime),
            // As far as I can tell, BigQuery will convert timestamps with timezones
            // to UTC.
            DataType::TimestampWithTimeZone => Ok(BqNonArrayDataType::Timestamp),
            DataType::Uuid => Ok(BqNonArrayDataType::String),
        }
    }
}

impl fmt::Display for BqNonArrayDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqNonArrayDataType::Bool => write!(f, "BOOL"),
            BqNonArrayDataType::Bytes => write!(f, "BYTES"),
            BqNonArrayDataType::Date => write!(f, "DATE"),
            BqNonArrayDataType::Datetime => write!(f, "DATETIME"),
            BqNonArrayDataType::Float64 => write!(f, "FLOAT64"),
            BqNonArrayDataType::Geography => write!(f, "GEOGRAPHY"),
            BqNonArrayDataType::Int64 => write!(f, "INT64"),
            BqNonArrayDataType::Numeric => write!(f, "NUMERIC"),
            BqNonArrayDataType::String => write!(f, "STRING"),
            BqNonArrayDataType::Struct(fields) => {
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
            BqNonArrayDataType::Time => write!(f, "TIME"),
            BqNonArrayDataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

/// A field of a `STRUCT`.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BqStructField {
    /// An optional field name. BigQuery `STRUCT`s are basically tuples, but
    /// with optional names for each position in the tuple.
    name: Option<String>,
    /// The field type.
    ty: BqDataType,
}

impl fmt::Display for BqStructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            // TODO: It's not clear whether we can/should escape this using
            // `Ident` to insert backticks.
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.ty)
    }
}

#[test]
fn nested_arrays() {
    let input = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
        Box::new(DataType::Int32),
    )))));

    // What we expect when loading from a CSV file.
    let bq = BqDataType::for_data_type(&input, Usage::CsvLoad).unwrap();
    assert_eq!(format!("{}", bq), "STRING");

    // What we expect in the final BigQuery table.
    let bq = BqDataType::for_data_type(&input, Usage::FinalTable).unwrap();
    assert_eq!(
        format!("{}", bq),
        "ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<INT64>>>>>"
    );
}
