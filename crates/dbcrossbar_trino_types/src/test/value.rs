//! Trino values (or at least those we care about).

use std::fmt;

use geo_types::Geometry;
use serde_json::Value;
use uuid::Uuid;
use wkt::ToWkt;

use crate::TrinoDataType;

/// A Trino value of one of our supported types.
#[derive(Debug, Clone)]
pub enum TrinoValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Decimal(String),
    Varchar(String),
    Varbinary(Vec<u8>),
    Json(Value),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampWithTimeZone(chrono::DateTime<chrono::FixedOffset>),
    Array {
        values: Vec<TrinoValue>,
        lit_type: TrinoDataType,
    },
    Row {
        values: Vec<TrinoValue>,
        lit_type: TrinoDataType,
    },
    Uuid(Uuid),
    SphericalGeography(Geometry<f64>),
}

impl fmt::Display for TrinoValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TrinoValue::Boolean(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            TrinoValue::TinyInt(i) => write!(f, "{}", i),
            TrinoValue::SmallInt(i) => write!(f, "{}", i),
            TrinoValue::Int(i) => write!(f, "{}", i),
            TrinoValue::BigInt(i) => write!(f, "{}", i),
            // Use scientific notation to prevent giant decimal literals that
            // Trino can't parse.
            TrinoValue::Real(fl) => write!(f, "REAL '{:e}'", fl),
            TrinoValue::Double(fl) => write!(f, "{:e}", fl),
            TrinoValue::Decimal(s) => write!(f, "DECIMAL {}", QuotedString(s)),
            TrinoValue::Varchar(s) => write!(f, "{}", QuotedString(s)),
            TrinoValue::Varbinary(vec) => {
                write!(f, "X'")?;
                for byte in vec {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
            TrinoValue::Json(value) => {
                let json_str =
                    serde_json::to_string(value).expect("could not serialize JSON");
                write!(f, "JSON {}", QuotedString(&json_str))
            }
            TrinoValue::Date(naive_date) => {
                write!(f, "DATE '{}'", naive_date.format("%Y-%m-%d"))
            }
            TrinoValue::Time(naive_time) => {
                write!(f, "TIME '{}'", naive_time.format("%H:%M:%S%.6f"))
            }
            TrinoValue::Timestamp(naive_date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    naive_date_time.format("%Y-%m-%d %H:%M:%S%.6f")
                )
            }
            TrinoValue::TimestampWithTimeZone(date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    date_time.format("%Y-%m-%d %H:%M:%S%.6f %:z")
                )
            }
            TrinoValue::Array {
                values,
                lit_type: original_type,
            } => {
                write!(f, "CAST(ARRAY[")?;
                for (idx, elem) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "] AS {})", original_type)
            }
            TrinoValue::Row {
                values,
                lit_type: original_type,
            } => {
                write!(f, "CAST(ROW(")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, ") AS {})", original_type)
            }
            TrinoValue::Uuid(uuid) => write!(f, "UUID '{}'", uuid),
            TrinoValue::SphericalGeography(value) => {
                write!(
                    f,
                    "to_spherical_geography(ST_GeometryFromText({}))",
                    QuotedString(&value.wkt_string())
                )
            }
        }
    }
}

/// Formatting wrapper for quoted strings.
struct QuotedString<'a>(&'a str);

impl<'a> fmt::Display for QuotedString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "'{}'", self.0.replace("'", "''"))
    }
}
