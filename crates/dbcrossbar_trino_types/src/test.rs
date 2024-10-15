//! Tools for testing code that works with Trino types. Exported when
//! `#[cfg(test)]` is true.

use std::fmt;

use base64::prelude::{Engine as _, BASE64_STANDARD};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use float_cmp::approx_eq;
use geo_types::{Geometry, Point};
use geojson::GeoJson;
use proptest::prelude::*;
use proptest_arbitrary_interop::arb;
use proptest_derive::Arbitrary;
use serde_json::{Map, Value};
use uuid::Uuid;
use wkt::TryFromWkt;

use crate::types::TrinoDataType;

pub trait ArbValue {
    fn arb_value(&self) -> BoxedStrategy<TrinoValue>
    where
        Self: Sized;
}

/// Generate a Trino value and its type.
pub fn any_trino_value_with_type() -> impl Strategy<Value = (TrinoValue, TrinoDataType)>
{
    any::<TrinoDataType>()
        .prop_flat_map(|ty| ty.arb_value().prop_map(move |val| (val, ty.clone())))
}

impl ArbValue for TrinoDataType {
    /// Return a [`proptest::Arbitrary`] strategy for generating a Trino value
    /// of this type.
    #[cfg(test)]
    fn arb_value(&self) -> BoxedStrategy<TrinoValue> {
        use serde_json::Number;

        match self {
            TrinoDataType::Boolean => {
                any::<bool>().prop_map(TrinoValue::Boolean).boxed()
            }
            TrinoDataType::TinyInt => {
                any::<i8>().prop_map(TrinoValue::TinyInt).boxed()
            }
            TrinoDataType::SmallInt => {
                any::<i16>().prop_map(TrinoValue::SmallInt).boxed()
            }
            TrinoDataType::Int => any::<i32>().prop_map(TrinoValue::Int).boxed(),
            TrinoDataType::BigInt => any::<i64>().prop_map(TrinoValue::BigInt).boxed(),
            TrinoDataType::Real => any::<f32>().prop_map(TrinoValue::Real).boxed(),
            TrinoDataType::Double => any::<f64>().prop_map(TrinoValue::Double).boxed(),
            TrinoDataType::Decimal { precision, scale } => {
                arb_decimal(*precision, *scale)
            }
            TrinoDataType::Varchar { length: None } => {
                any::<String>().prop_map(TrinoValue::Varchar).boxed()
            }
            TrinoDataType::Varchar {
                length: Some(length),
            } => prop::collection::vec(any::<char>(), 0..*length as usize)
                .prop_map(|chars| chars.into_iter().collect())
                .prop_map(TrinoValue::Varchar)
                .boxed(),
            TrinoDataType::Varbinary => prop::collection::vec(any::<u8>(), 0..100)
                .prop_map(TrinoValue::Varbinary)
                .boxed(),
            TrinoDataType::Json => arb_json().prop_map(TrinoValue::Json).boxed(),
            TrinoDataType::Date => {
                arb::<NaiveDate>().prop_map(TrinoValue::Date).boxed()
            }
            &TrinoDataType::Time { precision } => arb::<NaiveTime>()
                .prop_filter(LEAP_SECONDS_NOT_SUPPORTED, |t| !is_leap_second(t))
                .prop_map(move |t| TrinoValue::Time(round_timelike(t, precision)))
                .boxed(),
            &TrinoDataType::Timestamp { precision } => arb_timestamp()
                .prop_map(move |t| TrinoValue::Timestamp(round_timelike(t, precision)))
                .boxed(),
            &TrinoDataType::TimestampWithTimeZone { precision } => {
                arb_timestamp_with_time_zone()
                    .prop_map(move |t| {
                        TrinoValue::TimestampWithTimeZone(round_timelike(t, precision))
                    })
                    .boxed()
            }
            TrinoDataType::Array(elem_ty) => {
                let original_type = self.clone();
                prop::collection::vec(elem_ty.arb_value(), 0..3)
                    .prop_map(move |values| TrinoValue::Array {
                        values,
                        lit_type: original_type.clone(),
                    })
                    .boxed()
            }
            TrinoDataType::Row(fields) => {
                let original_type = self.clone();
                fields
                    .iter()
                    .map(|field| field.data_type.arb_value())
                    .collect::<Vec<_>>()
                    .prop_map(move |values| TrinoValue::Row {
                        values,
                        lit_type: original_type.clone(),
                    })
                    .boxed()
            }
            TrinoDataType::Uuid => arb::<Uuid>().prop_map(TrinoValue::Uuid).boxed(),
            // Just test points for now.
            TrinoDataType::SphericalGeography => (-180f64..=180f64, -90f64..=90f64)
                .prop_map(|(lon, lat)| {
                    let mut map = Map::new();
                    map.insert("type".to_string(), Value::String("Point".to_string()));
                    map.insert(
                        "coordinates".to_string(),
                        Value::Array(vec![
                            Value::Number(Number::from_f64(lon).unwrap()),
                            Value::Number(Number::from_f64(lat).unwrap()),
                        ]),
                    );
                    TrinoValue::SphericalGeography(Value::Object(map))
                })
                .boxed(),
        }
    }
}

proptest! {
    #[test]
    fn test_arb_value(ty in any::<TrinoDataType>()) {
        let _ = ty.arb_value();
    }
}

/// Error message to show if we fail to filter out leap seconds.
const LEAP_SECONDS_NOT_SUPPORTED: &str = "Trino does not support leap seconds";

/// Is a [`chrono::Timelike`] value a leap second? These are not supported by
/// Trino. See [`chrono::Timelike::nanosecond`] for details.
fn is_leap_second<TL: Timelike>(tl: &TL) -> bool {
    tl.nanosecond() >= 1_000_000_000
}

/// Set the precision of a [`chrono::Timelike`] value.
fn round_timelike<TL: Timelike>(tl: TL, precision: u32) -> TL {
    let nanos = tl.nanosecond();
    let nanos = if precision < 9 {
        let factor = 10u32.pow(9 - precision);
        nanos / factor * factor
    } else {
        nanos
    };
    tl.with_nanosecond(nanos)
        .expect("could not construct rounded time")
}

/// How many days are in a given month of a given year?
fn days_per_month(year: i32, month: u32) -> u32 {
    const DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && NaiveDate::from_ymd_opt(year, 2, 29).is_some() {
        29
    } else {
        DAYS_PER_MONTH[month as usize - 1]
    }
}

/// Generate a decimal value with a given precision and scale.
fn arb_decimal(precision: u32, scale: u32) -> BoxedStrategy<TrinoValue> {
    prop::collection::vec(0..=9u8, precision as usize)
        .prop_map(move |digits| {
            let mut s = String::new();
            for (idx, digit) in digits.into_iter().enumerate() {
                if idx == precision as usize - scale as usize {
                    s.push('.');
                }
                s.push_str(&digit.to_string());
            }
            // Canonicize the decimal representation by removing
            // leading zeros.
            s = s.trim_start_matches('0').to_string();
            if s.is_empty() || s.starts_with('.') {
                s = format!("0{}", s);
            }
            TrinoValue::Decimal(s)
        })
        .boxed()
}

/// Generate a [`NaiveDateTime`] that Trino and Hive will actually accept. This
/// is narrower than the full range of [`NaiveDateTime`] in a number of ways:
///
/// 1. Trino does not support leap seconds.
/// 2. Athena and Hive seem to dislike timestamps before 1970.
fn arb_timestamp() -> impl Strategy<Value = NaiveDateTime> {
    (1970i32..=3500, 1u32..=12).prop_flat_map(|(year, month)| {
        let day = 1..=days_per_month(year, month);
        let hour = 0u32..=23;
        let minute = 0u32..=59;
        // No leap seconds.
        let second = 0u32..=59;
        let nanosecond = 0u32..=999_999_999;
        (day, hour, minute, second, nanosecond).prop_map(
            move |(day, hour, minute, second, nanosecond)| {
                NaiveDate::from_ymd_opt(year, month, day)
                    .and_then(|date| {
                        date.and_hms_nano_opt(hour, minute, second, nanosecond)
                    })
                    .expect("could not construct valid timestamp")
            },
        )
    })
}

/// Generate a timestamp with a time zone that Trino and Hive will actually
/// accept. This is narrower than the full range of [`DateTime<FixedOffset>`] in
/// all the ways that [`arb_timestamp`] is, plus:
///
/// 1. Time zone offsets seem to be limited to -14:00 to +14:00.
fn arb_timestamp_with_time_zone() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (arb_timestamp(), -14 * 60..=14 * 60).prop_map(|(timestamp, offset_minutes)| {
        let offset = FixedOffset::east_opt(offset_minutes * 60)
            .expect("could not construct a valid time zone offset");
        DateTime::from_naive_utc_and_offset(timestamp, offset)
    })
}

/// Years and months.
#[derive(Debug, Clone, PartialEq, Eq, Arbitrary)]
pub enum YearToMonthUnit {
    Year,
    Month,
}

/// Days, hours, minutes, and seconds (plus fractional seconds).
#[derive(Debug, Clone, PartialEq, Eq, Arbitrary)]
pub enum DayToSecondUnit {
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
}

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
    SphericalGeography(Value),
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
                    "FROM_GEOJSON_GEOMETRY({})",
                    QuotedString(&value.to_string())
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

/// Interface for testing whether a value is "close enough" to a value represented as JSON. This is because:
///
/// 1. Our Trino client outputs JSON values, and
/// 2. Trino connectors can't always represent values exactly.
///
/// This API defines "good enough".
pub trait ApproxEqToJson: fmt::Debug {
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

/// Generate an arbitrary [`serde_json::Value`]. There are crates that can
/// do this, but they're not worth the dependency for this one use.
fn arb_json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_map(Value::from),
        any::<String>().prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 10, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 1..=3).prop_map(Value::Array),
            prop::collection::hash_map(any::<String>(), inner, 1..=3)
                .prop_map(|map| Value::Object(Map::from_iter(map.into_iter()))),
        ]
    })
}
