//! Approximate equality to JSON values.

use std::{collections::HashSet, fmt};

use chrono::{DateTime, FixedOffset, NaiveDateTime, NaiveTime, Timelike as _, Utc};
use float_cmp::approx_eq;
use geo_types::{Geometry, Point};
use serde_json::Value as JsonValue;

use super::TrinoValue;

/// Interface for testing whether a value is "close enough" to another value.
/// This is necessary because:
///
/// 1. Some Trino connectors may lose information, typically because they only
///    support TIMESTAMP(3) or something similar.
/// 2. Floating point numbers don't always compare exactly.
///
/// So our strategy is to define "close enough", or an acceptable precision for
/// storing a value and loading it back.
pub trait IsCloseEnoughTo: fmt::Debug {
    /// Is this value approximately equal to the supplied value?
    fn is_close_enough_to(&self, other: &Self) -> bool;
}

impl IsCloseEnoughTo for TrinoValue {
    fn is_close_enough_to(&self, other: &TrinoValue) -> bool {
        // Recursive.
        match (self, other) {
            (TrinoValue::Boolean(a), TrinoValue::Boolean(b)) => *a == *b,
            (TrinoValue::TinyInt(a), TrinoValue::TinyInt(b)) => *a == *b,
            (TrinoValue::SmallInt(a), TrinoValue::SmallInt(b)) => *a == *b,
            (TrinoValue::Int(a), TrinoValue::Int(b)) => *a == *b,
            (TrinoValue::BigInt(a), TrinoValue::BigInt(b)) => *a == *b,
            (TrinoValue::Real(a), TrinoValue::Real(b)) => a.is_close_enough_to(b),
            (TrinoValue::Double(a), TrinoValue::Double(b)) => a.is_close_enough_to(b),
            (TrinoValue::Decimal(a), TrinoValue::Decimal(b)) => a == b,
            (TrinoValue::Varchar(a), TrinoValue::Varchar(b)) => a == b,
            (TrinoValue::Varbinary(a), TrinoValue::Varbinary(b)) => a == b,
            (TrinoValue::Json(a), TrinoValue::Json(b)) => a.is_close_enough_to(b),
            (TrinoValue::Date(a), TrinoValue::Date(b)) => a == b,
            (TrinoValue::Time(a), TrinoValue::Time(b)) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (TrinoValue::Timestamp(a), TrinoValue::Timestamp(b)) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (
                TrinoValue::TimestampWithTimeZone(a),
                TrinoValue::TimestampWithTimeZone(b),
            ) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (
                TrinoValue::Array { values: a, .. },
                TrinoValue::Array { values: b, .. },
            ) => {
                a.len() == b.len()
                    && a.iter().zip(b.iter()).all(|(a, b)| a.is_close_enough_to(b))
            }
            (TrinoValue::Row { values: a, .. }, TrinoValue::Row { values: b, .. }) => {
                a.len() == b.len()
                    && a.iter().zip(b.iter()).all(|(a, b)| a.is_close_enough_to(b))
            }
            (TrinoValue::Uuid(a), TrinoValue::Uuid(b)) => a == b,
            (TrinoValue::SphericalGeography(a), TrinoValue::SphericalGeography(b)) => {
                a.is_close_enough_to(b)
            }
            _ => false,
        }
    }
}

impl IsCloseEnoughTo for f32 {
    fn is_close_enough_to(&self, other: &f32) -> bool {
        approx_eq!(f32, *self, *other, ulps = 2)
    }
}

impl IsCloseEnoughTo for f64 {
    fn is_close_enough_to(&self, other: &f64) -> bool {
        approx_eq!(f64, *self, *other, ulps = 2)
    }
}

impl IsCloseEnoughTo for Geometry<f64> {
    fn is_close_enough_to(&self, other: &Geometry<f64>) -> bool {
        match (self, other) {
            (Geometry::Point(a), Geometry::Point(b)) => a.is_close_enough_to(b),
            _ => unimplemented!("IsCloseEnoughTo for Geometry<64> for {:?}", self),
        }
    }
}

impl IsCloseEnoughTo for Point<f64> {
    fn is_close_enough_to(&self, other: &Point<f64>) -> bool {
        approx_eq!(f64, self.x(), other.x(), epsilon = 0.000_000_020)
            && approx_eq!(f64, self.y(), other.y(), epsilon = 0.000_000_020)
    }
}

impl IsCloseEnoughTo for JsonValue {
    fn is_close_enough_to(&self, other: &JsonValue) -> bool {
        match (self, other) {
            (JsonValue::Null, JsonValue::Null) => true,
            (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
            (JsonValue::Number(a), JsonValue::Number(b)) => a.is_close_enough_to(b),
            (JsonValue::String(a), JsonValue::String(b)) => a == b,
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| a.is_close_enough_to(b))
            }
            (JsonValue::Object(a), JsonValue::Object(b)) => {
                let a_keys: HashSet<&String> = HashSet::from_iter(a.keys());
                let b_keys: HashSet<&String> = HashSet::from_iter(b.keys());
                if a_keys != b_keys {
                    return false;
                }
                a_keys.iter().all(|&key| a[key].is_close_enough_to(&b[key]))
            }
            _ => false,
        }
    }
}

impl IsCloseEnoughTo for serde_json::Number {
    fn is_close_enough_to(&self, other: &serde_json::Number) -> bool {
        // `serde_json` supports u64, i64, and f64 as JSON number literals. This
        // code attempts to perform approximate comparison, but it might fail
        // for edge cases.
        if let (Some(a), Some(b)) = (self.as_u64(), other.as_u64()) {
            a == b
        } else if let (Some(a), Some(b)) = (self.as_i64(), other.as_i64()) {
            a == b
        } else if let (Some(a), Some(b)) = (self.as_f64(), other.as_f64()) {
            a.is_close_enough_to(&b)
        } else {
            unimplemented!("don't know how to compare {:?} and {:?}", self, other)
        }
    }
}

/// Convert a [`Timelike`] value to a f64 for comparison purposes.
trait ToF64Seconds {
    /// Convert a [`Timelike`] value to a f64 for comparison purposes.
    fn to_f64_seconds(&self) -> f64;
}

impl ToF64Seconds for NaiveTime {
    fn to_f64_seconds(&self) -> f64 {
        self.num_seconds_from_midnight() as f64
            + self.nanosecond() as f64 / 1_000_000_000.0
    }
}

impl ToF64Seconds for NaiveDateTime {
    fn to_f64_seconds(&self) -> f64 {
        let utc = self.and_utc();
        utc.timestamp() as f64 + utc.timestamp_subsec_nanos() as f64 / 1_000_000_000.0
    }
}

impl ToF64Seconds for DateTime<FixedOffset> {
    fn to_f64_seconds(&self) -> f64 {
        let utc = self.with_timezone(&Utc);
        utc.timestamp() as f64 + utc.timestamp_subsec_nanos() as f64 / 1_000_000_000.0
    }
}
