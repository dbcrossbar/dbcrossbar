//! Construct various types from parsed JSON values.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use geo_types::Geometry;
use serde_json::Value;
use uuid::Uuid;

use crate::common::*;
use crate::from_csv_cell::FromCsvCell;

/// Construct this type from a `serde_json::Value`.
pub(crate) trait FromJsonValue: FromCsvCell {
    /// Parse `json` into a value of this type.
    ///
    /// Numeric types may be represented as either JSON floats or JSON strings.
    /// The latter is supported because it's not possible to represent all
    /// values of types like `i64` as a JSON number, which is a `f64`. So we
    /// accept strings every place we want a numeric type, and handle them
    /// appropriately. We also use `serde_json`'s support for conversions like
    /// `as_i64`, which may interpret literal numbers that don't fit in an
    /// `f64`.
    ///
    /// For types that are most naturally represented as a string, we use the
    /// same string parsing rules as [`FromCsvCell`].
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {}", json)),
        }
    }
}

impl FromJsonValue for bool {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Bool(b) => Ok(*b),
            _ => Err(format_err!("expected JSON bool, found {}", json)),
        }
    }
}

impl FromJsonValue for NaiveDate {}

impl FromJsonValue for f32 {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Number(n) => {
                let f = n
                    .as_f64()
                    .ok_or_else(|| format_err!("cannot represent {} as f64", json))?;
                // TODO: This is the only reason we still need the `cast` crate.
                // It checks for out-of-bounds values when converting from `f64`
                // to `f32`.
                Ok(cast::f32(f).map_err(|err| {
                    format_err!("could not convert {} to f32: {}", f, err)
                })?)
            }
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {} as f32", json)),
        }
    }
}

impl FromJsonValue for f64 {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Number(n) => {
                let f = n
                    .as_f64()
                    .ok_or_else(|| format_err!("cannot represent {} as f64", json))?;
                Ok(f)
            }
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {} as f64", json)),
        }
    }
}

impl FromJsonValue for Geometry<f64> {}

impl FromJsonValue for i16 {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Number(n) => {
                let i = n
                    .as_i64()
                    .ok_or_else(|| format_err!("cannot represent {} as i64", json))?;
                Ok(i16::try_from(i)?)
            }
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {} as i16", json)),
        }
    }
}

impl FromJsonValue for i32 {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Number(n) => {
                let i = n
                    .as_i64()
                    .ok_or_else(|| format_err!("cannot represent {} as i64", json))?;
                Ok(i32::try_from(i)?)
            }
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {} as i32", json)),
        }
    }
}

impl FromJsonValue for i64 {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::Number(n) => {
                let i = n
                    .as_i64()
                    .ok_or_else(|| format_err!("cannot represent {} as i64", json))?;
                Ok(i)
            }
            Value::String(s) => Self::from_csv_cell(s),
            _ => Err(format_err!("could not parse JSON value {} as i64", json)),
        }
    }
}

impl FromJsonValue for String {
    fn from_json_value(json: &Value) -> Result<Self> {
        match json {
            Value::String(s) => Ok(s.to_owned()),
            _ => Err(format_err!("could not parse JSON value {} as string", json)),
        }
    }
}

impl FromJsonValue for Value {
    fn from_json_value(json: &Value) -> Result<Self> {
        Ok(json.to_owned())
    }
}

impl FromJsonValue for NaiveDateTime {}

impl FromJsonValue for NaiveTime {}

impl FromJsonValue for DateTime<FixedOffset> {}

impl FromJsonValue for DateTime<Utc> {}

impl FromJsonValue for Uuid {}
