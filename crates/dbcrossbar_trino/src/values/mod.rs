//! Trino values (or at least those we care about).

use std::fmt;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use geo_types::Geometry;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use uuid::Uuid;
use wkt::ToWkt;

use crate::{DataType, QuotedString};

pub use self::is_close_enough_to::IsCloseEnoughTo;

mod is_close_enough_to;

/// A Trino value of one of our supported types.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Value {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    /// A precise, fixed-point decimal value. Typically used to represent
    /// monetary values. Note that this only holds 96 bits of precision, and
    /// several popular databases have more, so this may not able to actually
    /// represent all possible values supported by Trino.
    Decimal(Decimal),
    Varchar(String),
    Varbinary(Vec<u8>),
    Json(JsonValue),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampWithTimeZone(DateTime<FixedOffset>),
    Array {
        /// The values in the array.
        values: Vec<Value>,
        /// The type of this array. Needed to help print empty arrays.
        literal_type: DataType,
    },
    Row {
        /// The values in the row.
        values: Vec<Value>,
        /// The type of this row. Needed to specify the field names of a literal
        /// array value.
        literal_type: DataType,
    },
    Uuid(Uuid),
    SphericalGeography(Geometry<f64>),
}

impl Value {
    /// Does a printed literal of this value require a cast?
    ///
    /// We go out of our way to only do this when necessry to make
    /// it easier to read generated test code.
    fn cast_required_by_literal(&self) -> Option<&DataType> {
        match self {
            Value::Array {
                values,
                literal_type,
            } => {
                if values.is_empty()
                    || values
                        .iter()
                        .any(|v| v.cast_required_by_literal().is_some())
                {
                    Some(literal_type)
                } else {
                    None
                }
            }

            Value::Row {
                values,
                literal_type,
            } => {
                if literal_type.is_row_with_named_fields()
                    || values
                        .iter()
                        .any(|v| v.cast_required_by_literal().is_some())
                {
                    Some(literal_type)
                } else {
                    None
                }
            }

            _ => None,
        }
    }

    /// Recursive [`fmt::Display::fmt`] helper.
    fn fmt_helper(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Int(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            // Use scientific notation to prevent giant decimal literals that
            // Trino can't parse.
            Value::Real(fl) => write!(f, "REAL '{:e}'", fl),
            Value::Double(fl) => write!(f, "{:e}", fl),
            Value::Decimal(d) => write!(f, "DECIMAL '{}'", d),
            Value::Varchar(s) => write!(f, "{}", QuotedString(s)),
            Value::Varbinary(vec) => {
                write!(f, "X'")?;
                for byte in vec {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
            Value::Json(value) => {
                let json_str =
                    serde_json::to_string(value).expect("could not serialize JSON");
                write!(f, "JSON {}", QuotedString(&json_str))
            }
            Value::Date(naive_date) => {
                write!(f, "DATE '{}'", naive_date.format("%Y-%m-%d"))
            }
            Value::Time(naive_time) => {
                write!(f, "TIME '{}'", naive_time.format("%H:%M:%S%.6f"))
            }
            Value::Timestamp(naive_date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    naive_date_time.format("%Y-%m-%d %H:%M:%S%.6f")
                )
            }
            Value::TimestampWithTimeZone(date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    date_time.format("%Y-%m-%d %H:%M:%S%.6f %:z")
                )
            }
            Value::Array {
                values,
                literal_type: _,
            } => {
                write!(f, "ARRAY[")?;
                for (idx, elem) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }
            Value::Row {
                values,
                literal_type: _,
            } => {
                write!(f, "ROW(")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, ")")
            }
            Value::Uuid(uuid) => write!(f, "UUID '{}'", uuid),
            Value::SphericalGeography(value) => {
                write!(
                    f,
                    "to_spherical_geography(ST_GeometryFromText({}))",
                    QuotedString(&value.wkt_string())
                )
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cast_to = self.cast_required_by_literal();
        if cast_to.is_some() {
            write!(f, "CAST(")?;
        }
        self.fmt_helper(f)?;
        if let Some(data_type) = cast_to {
            write!(f, " AS {})", data_type)?;
        }
        Ok(())
    }
}
