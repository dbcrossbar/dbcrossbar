//! Approximate equality to JSON values.

use std::{collections::HashSet, fmt};

use float_cmp::approx_eq;
use geo_types::{Geometry, Point};
use serde_json::Value;

use crate::test::time::ToF64Seconds;

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
pub(crate) trait IsCloseEnoughTo: fmt::Debug {
    /// Is this value approximately equal to the supplied JSON value?
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
            (TrinoValue::Decimal(a), TrinoValue::Decimal(b)) => {
                // Parse both values as JSON numbers and compare them. We'll
                // probably want to improve this
                let a_f64 = a.parse::<f64>().expect("could not parse decimal");
                let b_f64 = b.parse::<f64>().expect("could not parse decimal");
                a_f64.is_close_enough_to(&b_f64)
            }
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

impl IsCloseEnoughTo for Value {
    fn is_close_enough_to(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Number(a), Value::Number(b)) => a.is_close_enough_to(b),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| a.is_close_enough_to(b))
            }
            (Value::Object(a), Value::Object(b)) => {
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
            false
        }
    }
}
