//! Approximate equality to JSON values.

use std::{collections::HashSet, fmt};

use chrono::{DateTime, FixedOffset, NaiveDateTime, NaiveTime, Timelike as _, Utc};
use float_cmp::approx_eq;
use geo_types::{Geometry, Point};
use serde_json::Value as JsonValue;

use super::Value;

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
    ///
    /// **WARNING:** This is currently intended for test code, and it may panic
    /// on certain comparisons that we haven't seen in practice.
    fn is_close_enough_to(&self, other: &Self) -> bool;
}

impl IsCloseEnoughTo for Value {
    fn is_close_enough_to(&self, other: &Value) -> bool {
        // Recursive.
        match (self, other) {
            (Value::Null { .. }, Value::Null { .. }) => true,
            (Value::Boolean(a), Value::Boolean(b)) => *a == *b,
            (Value::TinyInt(a), Value::TinyInt(b)) => *a == *b,
            (Value::SmallInt(a), Value::SmallInt(b)) => *a == *b,
            (Value::Int(a), Value::Int(b)) => *a == *b,
            (Value::BigInt(a), Value::BigInt(b)) => *a == *b,
            (Value::Real(a), Value::Real(b)) => a.is_close_enough_to(b),
            (Value::Double(a), Value::Double(b)) => a.is_close_enough_to(b),
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Varbinary(a), Value::Varbinary(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a.is_close_enough_to(b),
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (Value::Timestamp(a), Value::Timestamp(b)) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (Value::TimestampWithTimeZone(a), Value::TimestampWithTimeZone(b)) => {
                approx_eq!(
                    f64,
                    a.to_f64_seconds(),
                    b.to_f64_seconds(),
                    epsilon = 0.002
                )
            }
            (Value::Array { values: a, .. }, Value::Array { values: b, .. }) => {
                a.is_close_enough_to(b)
            }
            (Value::Row { values: a, .. }, Value::Row { values: b, .. }) => {
                // TODO: Should we also check the literal_type fields are the same?
                a.is_close_enough_to(b)
            }
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::SphericalGeography(a), Value::SphericalGeography(b)) => {
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

impl<T: IsCloseEnoughTo> IsCloseEnoughTo for Vec<T> {
    fn is_close_enough_to(&self, other: &Vec<T>) -> bool {
        (&self[..]).is_close_enough_to(&&other[..])
    }
}

impl<T: IsCloseEnoughTo> IsCloseEnoughTo for &'_ [T] {
    fn is_close_enough_to(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| a.is_close_enough_to(b))
    }
}

impl<T: IsCloseEnoughTo> IsCloseEnoughTo for Option<T> {
    fn is_close_enough_to(&self, other: &Option<T>) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => a.is_close_enough_to(b),
            (None, None) => true,
            _ => false,
        }
    }
}

impl IsCloseEnoughTo for JsonValue {
    fn is_close_enough_to(&self, other: &JsonValue) -> bool {
        match (self, other) {
            (JsonValue::Null, JsonValue::Null) => true,
            (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
            (JsonValue::Number(a), JsonValue::Number(b)) => a.is_close_enough_to(b),
            (JsonValue::String(a), JsonValue::String(b)) => a == b,
            (JsonValue::Array(a), JsonValue::Array(b)) => a.is_close_enough_to(b),
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
