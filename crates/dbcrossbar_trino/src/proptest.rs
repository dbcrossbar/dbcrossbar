//! Generate values for testing using
//! [`proptest`](https://proptest-rs.github.io/proptest/intro.html).

use std::{cmp::min, str::FromStr as _};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Timelike};
use proptest::prelude::*;
use rust_decimal::Decimal;
use serde_json::{Map, Value as JsonValue};

use crate::{DataType, Field, Ident, Value};

impl Arbitrary for Ident {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: ()) -> Self::Strategy {
        prop_oneof![
            // C-style identifiers.
            "[a-zA-Z_][a-zA-Z0-9_]*",
            // Non-empty ASCII strings.
            // TODO: This breaks on "`" in some backends.
            // "[ -~]+",
        ]
        .prop_map(|s| Ident::new(&s).unwrap())
        .boxed()
    }
}

impl Arbitrary for DataType {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: ()) -> Self::Strategy {
        let leaf = prop_oneof![
            Just(DataType::Boolean),
            Just(DataType::TinyInt),
            Just(DataType::SmallInt),
            Just(DataType::Int),
            Just(DataType::BigInt),
            Just(DataType::Real),
            Just(DataType::Double),
            // Make sure we keep at least one digit before the decimal
            // point, for simplicity. Feel free to look at the support for
            // other precision/scale values in Trino and the storage drivers
            // and generalize this as needed.
            (3..=38u32, 0..=2u32).prop_map(|(precision, scale)| {
                DataType::Decimal { precision, scale }
            }),
            Just(DataType::Varchar { length: None }),
            (1..=255u32).prop_map(|length| DataType::Varchar {
                length: Some(length)
            }),
            Just(DataType::Varbinary),
            Just(DataType::Json),
            Just(DataType::Date),
            (1..=6u32).prop_map(|precision| DataType::Time { precision }),
            (1..=6u32).prop_map(|precision| DataType::Timestamp { precision }),
            (1..=6u32).prop_map(|precision| {
                DataType::TimestampWithTimeZone { precision }
            }),
            Just(DataType::Uuid),
            Just(DataType::SphericalGeography),
        ];
        leaf.prop_recursive(3, 10, 3, |inner| {
            prop_oneof![
                inner
                    .clone()
                    .prop_map(|elem_ty| DataType::Array(Box::new(elem_ty))),
                prop::collection::vec((any::<Option<Ident>>(), inner), 1..=3)
                    .prop_map(|fields| {
                        DataType::Row(
                            fields
                                .into_iter()
                                .map(|(name, data_type)| Field { name, data_type })
                                .collect(),
                        )
                    }),
            ]
        })
        .boxed()
    }
}

/// Generate a decimal value with a given precision and scale.
fn any_decimal(precision: u32, scale: u32) -> BoxedStrategy<Value> {
    // We can't generate a decimal with more than 28 digits of precision because
    // it may not fit in a `Decimal`. Ideally we would support up to 38 digits
    // of precision, which is what BigQuery supports.
    prop::collection::vec(0..=9u8, min(precision, 28) as usize)
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
            Value::Decimal(Decimal::from_str(&s).unwrap_or_else(|err| {
                panic!("could not parse decimal {:?} from string: {}", s, err)
            }))
        })
        .boxed()
}

/// Generate a [`NaiveDateTime`] that Trino and Hive will actually accept. This
/// is narrower than the full range of [`NaiveDateTime`] in a number of ways:
///
/// 1. Trino does not support leap seconds.
/// 2. Athena might dislike timestamps before 1970 in certain circumstances, but
///    we don't work around that here.
fn any_trino_compatible_timestamp() -> impl Strategy<Value = NaiveDateTime> {
    (1900i32..=3500, 1u32..=12).prop_flat_map(|(year, month)| {
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
fn any_trino_compatible_timestamp_with_time_zone(
) -> impl Strategy<Value = DateTime<FixedOffset>> {
    (any_trino_compatible_timestamp(), -14 * 60..=14 * 60).prop_map(
        |(timestamp, offset_minutes)| {
            let offset = FixedOffset::east_opt(offset_minutes * 60)
                .expect("could not construct a valid time zone offset");
            DateTime::from_naive_utc_and_offset(timestamp, offset)
        },
    )
}

/// Generate an arbitrary [`serde_json::Value`]. There are crates that can
/// do this, but they're not worth the dependency for this one use.
fn any_json() -> impl Strategy<Value = JsonValue> {
    let leaf = prop_oneof![
        Just(JsonValue::Null),
        any::<bool>().prop_map(JsonValue::Bool),
        any::<u64>().prop_map(JsonValue::from),
        any::<i64>().prop_map(JsonValue::from),
        any::<f64>().prop_map(JsonValue::from),
        any::<String>().prop_map(JsonValue::String),
    ];
    leaf.prop_recursive(3, 10, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 1..=3).prop_map(JsonValue::Array),
            prop::collection::hash_map(any::<String>(), inner, 1..=3)
                .prop_map(|map| JsonValue::Object(Map::from_iter(map.into_iter()))),
        ]
    })
}

/// Generate a Trino value and its type.
pub fn any_trino_value_with_type() -> impl Strategy<Value = (Value, DataType)> {
    any::<DataType>().prop_flat_map(|ty| {
        arb_value_of_type(&ty).prop_map(move |val| (val, ty.clone()))
    })
}

fn arb_value_of_type(ty: &DataType) -> BoxedStrategy<Value> {
    use chrono::NaiveTime;
    use geo_types::Geometry;
    use proptest_arbitrary_interop::arb;
    use uuid::Uuid;

    match ty {
        DataType::Boolean => any::<bool>().prop_map(Value::Boolean).boxed(),
        DataType::TinyInt => any::<i8>().prop_map(Value::TinyInt).boxed(),
        DataType::SmallInt => any::<i16>().prop_map(Value::SmallInt).boxed(),
        DataType::Int => any::<i32>().prop_map(Value::Int).boxed(),
        DataType::BigInt => any::<i64>().prop_map(Value::BigInt).boxed(),
        DataType::Real => any::<f32>().prop_map(Value::Real).boxed(),
        DataType::Double => any::<f64>().prop_map(Value::Double).boxed(),
        DataType::Decimal { precision, scale } => any_decimal(*precision, *scale),
        DataType::Varchar { length: None } => {
            any::<String>().prop_map(Value::Varchar).boxed()
        }
        DataType::Varchar {
            length: Some(length),
        } => prop::collection::vec(any::<char>(), 0..*length as usize)
            .prop_map(|chars| chars.into_iter().collect())
            .prop_map(Value::Varchar)
            .boxed(),
        DataType::Varbinary => prop::collection::vec(any::<u8>(), 0..100)
            .prop_map(Value::Varbinary)
            .boxed(),
        DataType::Json => any_json().prop_map(Value::Json).boxed(),
        DataType::Date => arb::<NaiveDate>().prop_map(Value::Date).boxed(),
        &DataType::Time { precision } => arb::<NaiveTime>()
            .prop_filter(LEAP_SECONDS_NOT_SUPPORTED, |t| !is_leap_second(t))
            .prop_map(move |t| Value::Time(round_timelike(t, precision)))
            .boxed(),
        &DataType::Timestamp { precision } => any_trino_compatible_timestamp()
            .prop_map(move |t| Value::Timestamp(round_timelike(t, precision)))
            .boxed(),
        &DataType::TimestampWithTimeZone { precision } => {
            any_trino_compatible_timestamp_with_time_zone()
                .prop_map(move |t| {
                    Value::TimestampWithTimeZone(round_timelike(t, precision))
                })
                .boxed()
        }
        DataType::Array(elem_ty) => {
            let original_type = ty.clone();
            prop::collection::vec(arb_value_of_type(elem_ty), 0..3)
                .prop_map(move |values| Value::Array {
                    values,
                    literal_type: original_type.clone(),
                })
                .boxed()
        }
        DataType::Row(fields) => {
            let original_type = ty.clone();
            fields
                .iter()
                .map(|field| arb_value_of_type(&field.data_type))
                .collect::<Vec<_>>()
                .prop_map(move |values| Value::Row {
                    values,
                    literal_type: original_type.clone(),
                })
                .boxed()
        }
        DataType::Uuid => arb::<Uuid>().prop_map(Value::Uuid).boxed(),
        // Just test points for now.
        DataType::SphericalGeography => (-180f64..=180f64, -90f64..=90f64)
            .prop_map(|(lon, lat)| {
                Value::SphericalGeography(Geometry::Point((lon, lat).into()))
            })
            .boxed(),
    }
}

/// Error message to show if we fail to filter out leap seconds.
const LEAP_SECONDS_NOT_SUPPORTED: &str = "Trino does not support leap seconds";

/// Is a [`chrono::Timelike`] value a leap second? These are not supported by
/// Trino. See [`chrono::Timelike::nanosecond`] for details.
fn is_leap_second<TL: Timelike>(tl: &TL) -> bool {
    tl.nanosecond() >= 1_000_000_000
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

proptest! {
    #[test]
    fn test_arb_value_of_type(ty in any::<DataType>()) {
        let _ = arb_value_of_type(&ty);
    }
}
