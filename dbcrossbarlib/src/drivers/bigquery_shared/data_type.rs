//! Data types supported BigQuery.

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, result};

use super::column::Mode;
use crate::common::*;
use crate::schema::{DataType, Srid};
use crate::separator::Separator;

/// Include our `rust-peg` grammar.
///
/// We disable lots of clippy warnings because this is machine-generated code.
#[allow(clippy::all, rust_2018_idioms, elided_lifetimes_in_paths)]
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/data_type.rs"));
}

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
///
/// This is marked `pub` instead of `pub(crate)` because of limitations in
/// `rust-peg`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BqDataType {
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
                if let DataType::Json = nested.as_ref() {
                    return Err(format_err!(
                        "cannot represent arrays of JSON in BigQuery yet"
                    ));
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

impl<'de> Deserialize<'de> for BqDataType {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let parsed = grammar::data_type(&raw).map_err(|err| {
            D::Error::custom(format!(
                "error parsing BigQuery data type {:?}: {}",
                raw, err
            ))
        })?;
        Ok(parsed)
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
///
/// This should really be `pub(crate)`, see [BqDataType].
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum BqNonArrayDataType {
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
            DataType::GeoJson(srid) if *srid == Srid::wgs84() => {
                Ok(BqNonArrayDataType::Geography)
            }
            DataType::GeoJson(_) => Ok(BqNonArrayDataType::String),
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

    /// Convert this `BqNonArrayDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self, mode: Mode) -> Result<DataType> {
        if mode == Mode::Repeated {
            // I _think_ all values in arrays are always nullable.
            Ok(DataType::Array(Box::new(
                self.to_data_type(Mode::Nullable)?,
            )))
        } else {
            match self {
                BqNonArrayDataType::Bool => Ok(DataType::Bool),
                BqNonArrayDataType::Date => Ok(DataType::Date),
                BqNonArrayDataType::Numeric => Ok(DataType::Decimal),
                BqNonArrayDataType::Float64 => Ok(DataType::Float64),
                BqNonArrayDataType::Geography => Ok(DataType::GeoJson(Srid::wgs84())),
                BqNonArrayDataType::Int64 => Ok(DataType::Int64),
                BqNonArrayDataType::String => Ok(DataType::Text),
                BqNonArrayDataType::Datetime => Ok(DataType::TimestampWithoutTimeZone),
                BqNonArrayDataType::Timestamp => Ok(DataType::TimestampWithTimeZone),
                BqNonArrayDataType::Bytes
                | BqNonArrayDataType::Struct(_)
                | BqNonArrayDataType::Time => Err(format_err!(
                    "cannot convert {} to portable type (yet)",
                    self
                )),
            }
        }
    }
}

impl<'de> Deserialize<'de> for BqNonArrayDataType {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let parsed = grammar::non_array_data_type(&raw).map_err(|err| {
            D::Error::custom(format!(
                "error parsing BigQuery data type {:?}: {}",
                raw, err
            ))
        })?;
        Ok(parsed)
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
                let mut sep = Separator::new(",");
                for field in fields {
                    write!(f, "{}{}", sep.display(), field)?;
                }
                write!(f, ">")
            }
            BqNonArrayDataType::Time => write!(f, "TIME"),
            BqNonArrayDataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

impl Serialize for BqNonArrayDataType {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a string and serialize that.
        format!("{}", self).serialize(serializer)
    }
}

/// A field of a `STRUCT`.
///
/// This should really be `pub(crate)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BqStructField {
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

#[test]
fn parsing() {
    use BqDataType as DT;
    use BqNonArrayDataType as NADT;
    let examples = [
        ("BOOL", DT::NonArray(NADT::Bool)),
        // Not documented, but it exists.
        ("BOOLEAN", DT::NonArray(NADT::Bool)),
        ("BYTES", DT::NonArray(NADT::Bytes)),
        ("DATE", DT::NonArray(NADT::Date)),
        ("DATETIME", DT::NonArray(NADT::Datetime)),
        ("FLOAT64", DT::NonArray(NADT::Float64)),
        ("GEOGRAPHY", DT::NonArray(NADT::Geography)),
        ("INT64", DT::NonArray(NADT::Int64)),
        ("NUMERIC", DT::NonArray(NADT::Numeric)),
        ("STRING", DT::NonArray(NADT::String)),
        ("TIME", DT::NonArray(NADT::Time)),
        ("TIMESTAMP", DT::NonArray(NADT::Timestamp)),
        ("ARRAY<STRING>", DT::Array(NADT::String)),
        (
            "STRUCT<FLOAT64, FLOAT64>",
            DT::NonArray(NADT::Struct(vec![
                BqStructField {
                    name: None,
                    ty: DT::NonArray(NADT::Float64),
                },
                BqStructField {
                    name: None,
                    ty: DT::NonArray(NADT::Float64),
                },
            ])),
        ),
        (
            "STRUCT<x FLOAT64, y FLOAT64>",
            DT::NonArray(NADT::Struct(vec![
                BqStructField {
                    name: Some("x".to_owned()),
                    ty: DT::NonArray(NADT::Float64),
                },
                BqStructField {
                    name: Some("y".to_owned()),
                    ty: DT::NonArray(NADT::Float64),
                },
            ])),
        ),
        (
            "ARRAY<STRUCT<ARRAY<INT64>>>",
            DT::Array(NADT::Struct(vec![BqStructField {
                name: None,
                ty: DT::Array(NADT::Int64),
            }])),
        ),
    ];
    for (input, expected) in &examples {
        let quoted = format!("\"{}\"", input);
        let parsed: BqDataType = serde_json::from_str(&quoted).unwrap();
        assert_eq!(&parsed, expected);
    }
}
