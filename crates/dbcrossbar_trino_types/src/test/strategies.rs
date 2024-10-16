//! Generate values for testing using [`proptest`].

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use proptest::prelude::*;
use serde_json::{Map, Value};

use crate::TrinoDataType;

use super::{
    time::{
        days_per_month, is_leap_second, round_timelike, LEAP_SECONDS_NOT_SUPPORTED,
    },
    TrinoValue,
};

/// Generate a decimal value with a given precision and scale.
fn any_decimal(precision: u32, scale: u32) -> BoxedStrategy<TrinoValue> {
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
fn any_timestamp() -> impl Strategy<Value = NaiveDateTime> {
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
fn any_timestamp_with_time_zone() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (any_timestamp(), -14 * 60..=14 * 60).prop_map(|(timestamp, offset_minutes)| {
        let offset = FixedOffset::east_opt(offset_minutes * 60)
            .expect("could not construct a valid time zone offset");
        DateTime::from_naive_utc_and_offset(timestamp, offset)
    })
}

/// Generate an arbitrary [`serde_json::Value`]. There are crates that can
/// do this, but they're not worth the dependency for this one use.
fn any_json() -> impl Strategy<Value = Value> {
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

/// Generate a Trino value and its type.
pub fn any_trino_value_with_type() -> impl Strategy<Value = (TrinoValue, TrinoDataType)>
{
    any::<TrinoDataType>()
        .prop_flat_map(|ty| ty.arb_value().prop_map(move |val| (val, ty.clone())))
}

trait ArbValue {
    fn arb_value(&self) -> BoxedStrategy<TrinoValue>
    where
        Self: Sized;
}

impl ArbValue for TrinoDataType {
    /// Return a [`proptest::Arbitrary`] strategy for generating a Trino value
    /// of this type.
    #[cfg(test)]
    fn arb_value(&self) -> BoxedStrategy<TrinoValue> {
        use chrono::NaiveTime;
        use geo_types::Geometry;
        use proptest_arbitrary_interop::arb;
        use uuid::Uuid;

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
                any_decimal(*precision, *scale)
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
            TrinoDataType::Json => any_json().prop_map(TrinoValue::Json).boxed(),
            TrinoDataType::Date => {
                arb::<NaiveDate>().prop_map(TrinoValue::Date).boxed()
            }
            &TrinoDataType::Time { precision } => arb::<NaiveTime>()
                .prop_filter(LEAP_SECONDS_NOT_SUPPORTED, |t| !is_leap_second(t))
                .prop_map(move |t| TrinoValue::Time(round_timelike(t, precision)))
                .boxed(),
            &TrinoDataType::Timestamp { precision } => any_timestamp()
                .prop_map(move |t| TrinoValue::Timestamp(round_timelike(t, precision)))
                .boxed(),
            &TrinoDataType::TimestampWithTimeZone { precision } => {
                any_timestamp_with_time_zone()
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
                    TrinoValue::SphericalGeography(Geometry::Point((lon, lat).into()))
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
