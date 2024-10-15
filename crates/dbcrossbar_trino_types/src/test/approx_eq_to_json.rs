//! Approximate equality to JSON values.

use std::fmt;

use base64::{prelude::BASE64_STANDARD, Engine as _};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use float_cmp::approx_eq;
use geo_types::Geometry;
use geojson::GeoJson;
use serde_json::Value;
use uuid::Uuid;
use wkt::TryFromWkt as _;

use super::{time::round_timelike, TrinoValue};

/// Interface for testing whether a value is "close enough" to a value
/// represented as JSON. This is because:
///
/// 1. Our Trino client outputs JSON values, and
/// 2. Trino connectors can't always represent values exactly.
///
/// This API defines "good enough".
pub(crate) trait ApproxEqToJson: fmt::Debug {
    /// Is this value approximately equal to the supplied JSON value?
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        panic!(
            "approx_eq_to_json not implemented for {:?} and {:?}",
            self, other
        );
    }
}

impl ApproxEqToJson for TrinoValue {
    /// Is this value approximately equal to the supplied JSON value?
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        // Recursive.
        match self {
            TrinoValue::Boolean(b) => b.approx_eq_to_json(other),
            TrinoValue::TinyInt(i) => i.approx_eq_to_json(other),
            TrinoValue::SmallInt(i) => i.approx_eq_to_json(other),
            TrinoValue::Int(i) => i.approx_eq_to_json(other),
            TrinoValue::BigInt(i) => i.approx_eq_to_json(other),
            TrinoValue::Real(fl) => fl.approx_eq_to_json(other),
            TrinoValue::Double(fl) => fl.approx_eq_to_json(other),
            TrinoValue::Decimal(s) => {
                // Parse both values as JSON numbers and compare them. We'll
                // probably want to improve this
                let self_f64 = s.parse::<f64>().expect("could not parse decimal");
                let other_f64 = match other {
                    Value::String(s) => {
                        s.parse::<f64>().expect("could not parse decimal JSON")
                    }
                    _ => return false,
                };
                self_f64 == other_f64
            }
            TrinoValue::Varchar(s) => s.approx_eq_to_json(other),
            TrinoValue::Varbinary(vec) => vec.approx_eq_to_json(other),
            TrinoValue::Json(value) => value.approx_eq_to_json(other),
            TrinoValue::Date(naive_date) => naive_date.approx_eq_to_json(other),
            TrinoValue::Time(naive_time) => naive_time.approx_eq_to_json(other),
            TrinoValue::Timestamp(naive_date_time) => {
                naive_date_time.approx_eq_to_json(other)
            }
            TrinoValue::TimestampWithTimeZone(date_time) => {
                date_time.approx_eq_to_json(other)
            }
            TrinoValue::Array { values, .. } => values.approx_eq_to_json(other),
            // TODO: Verify wire representation to see how field names are handled.
            TrinoValue::Row { values, .. } => values.approx_eq_to_json(other),
            TrinoValue::Uuid(uuid) => uuid.approx_eq_to_json(other),
            TrinoValue::SphericalGeography(value) => {
                // Convert `value` from GeoJSON to a geometry.
                let value_str = value.to_string();
                let value_geojson = value_str
                    .parse::<GeoJson>()
                    .expect("could not parse GeoJSON");
                let value_geom: Geometry<f64> = value_geojson
                    .try_into()
                    .expect("could not convert GeoJSON to geometry");

                // Convert `other` from WKT to a geometry.
                let other_geom = Geometry::<f64>::try_from_wkt_str(
                    other.as_str().expect("expected string"),
                )
                .expect("could not convert WKT to geometry");

                // Compare the two geometries.
                value_geom == other_geom
            }
        }
    }
}

impl ApproxEqToJson for bool {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Bool(b) => self == b,
            _ => false,
        }
    }
}

impl ApproxEqToJson for i8 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => n.as_i64() == Some(i64::from(*self)),
            _ => false,
        }
    }
}
impl ApproxEqToJson for i16 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => n.as_i64() == Some(i64::from(*self)),
            _ => false,
        }
    }
}
impl ApproxEqToJson for i32 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => n.as_i64() == Some(i64::from(*self)),
            _ => false,
        }
    }
}
impl ApproxEqToJson for i64 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => n.as_i64() == Some(*self),
            _ => false,
        }
    }
}
impl ApproxEqToJson for f32 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => {
                approx_eq!(
                    f32,
                    n.as_f64().expect("expected f64") as f32,
                    *self,
                    ulps = 2
                )
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for f64 {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Number(n) => {
                approx_eq!(f64, n.as_f64().expect("expected f64"), *self, ulps = 2)
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for String {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => self == s,
            _ => false,
        }
    }
}
impl ApproxEqToJson for Vec<u8> {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => {
                BASE64_STANDARD.decode(s).expect("could not decode Base64") == *self
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for Value {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        let parsed_other = match other {
            Value::String(s) => serde_json::from_str(s).expect("could not parse JSON"),
            _ => other.clone(),
        };
        self == &parsed_other
    }
}
impl ApproxEqToJson for NaiveDate {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => {
                self == &NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for NaiveTime {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => {
                // Round to precision 2 because Hive stores with precision 3 and
                // we may not have consistent rounding behavior?
                round_timelike(self.to_owned(), 2)
                    == round_timelike(
                        NaiveTime::parse_from_str(s, "%H:%M:%S%.6f").unwrap(),
                        2,
                    )
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for NaiveDateTime {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => {
                // Round to precision 2 because Hive stores with precision 3 and
                // we may not have consistent rounding behavior?
                round_timelike(self.to_owned(), 2)
                    == round_timelike(
                        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f")
                            .unwrap(),
                        2,
                    )
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for DateTime<FixedOffset> {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => {
                let parsed = if s.ends_with(" UTC") {
                    NaiveDateTime::parse_from_str(
                        s.trim_end_matches(" UTC"),
                        "%Y-%m-%d %H:%M:%S%.6f",
                    )
                    .unwrap()
                    .and_utc()
                    .fixed_offset()
                } else {
                    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f %:z")
                        .unwrap()
                        .with_timezone(&FixedOffset::east_opt(0).unwrap())
                };
                // Round to precision 2 because Hive stores with precision 3 and
                // we may not have consistent rounding behavior?
                round_timelike(self.to_owned(), 2) == round_timelike(parsed, 2)
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for &'_ Vec<TrinoValue> {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        self.as_slice().approx_eq_to_json(other)
    }
}
impl ApproxEqToJson for &'_ [TrinoValue] {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::Array(arr) => {
                if self.len() != arr.len() {
                    return false;
                }
                for (a, b) in self.iter().zip(arr.iter()) {
                    if !a.approx_eq_to_json(b) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}
impl ApproxEqToJson for Uuid {
    fn approx_eq_to_json(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => self == &s.parse::<Uuid>().unwrap(),
            _ => false,
        }
    }
}
