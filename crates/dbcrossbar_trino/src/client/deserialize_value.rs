//! Deserialize a Trino JSON value into a [`Value`].

use std::str::FromStr as _;

use base64::{prelude::BASE64_STANDARD, Engine as _};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use geo_types::Geometry;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use uuid::Uuid;
use wkt::TryFromWkt as _;

use crate::{values::Value, DataType};

use super::ClientError;

/// Deserialize a Trino JSON value into a [`Value`] of type [`DataType`].
pub(crate) fn deserialize_json_value(
    data_type: &DataType,
    value: &JsonValue,
) -> Result<Value, ClientError> {
    let failed = || ClientError::CouldNotDeserializeValue {
        value: value.clone(),
        data_type: data_type.clone(),
    };
    match (data_type, value) {
        (DataType::Boolean, JsonValue::Bool(b)) => Ok(Value::Boolean(*b)),
        (DataType::TinyInt, JsonValue::Number(n)) => Ok(Value::TinyInt(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::SmallInt, JsonValue::Number(n)) => Ok(Value::SmallInt(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::Int, JsonValue::Number(n)) => Ok(Value::Int(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::BigInt, JsonValue::Number(n)) => {
            Ok(Value::BigInt(n.as_i64().ok_or_else(failed)?))
        }
        (DataType::Real, JsonValue::Number(n)) => {
            Ok(Value::Real(n.as_f64().ok_or_else(failed)? as f32))
        }
        (DataType::Double, JsonValue::Number(n)) => {
            Ok(Value::Double(n.as_f64().ok_or_else(failed)?))
        }
        (DataType::Decimal { .. }, JsonValue::String(s)) => {
            Ok(Value::Decimal(Decimal::from_str(s).map_err(|_| failed())?))
        }
        (DataType::Varchar { .. }, JsonValue::String(s)) => {
            Ok(Value::Varchar(s.clone()))
        }
        (DataType::Varbinary, JsonValue::String(s)) => Ok(Value::Varbinary(
            BASE64_STANDARD.decode(s.as_bytes()).map_err(|_| failed())?,
        )),
        (DataType::Json, JsonValue::String(s)) => {
            let json = serde_json::from_str(s).map_err(|_| failed())?;
            Ok(Value::Json(json))
        }
        (DataType::Date, JsonValue::String(s)) => Ok(Value::Date(
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| failed())?,
        )),
        (DataType::Time { .. }, JsonValue::String(s)) => Ok(Value::Time(
            NaiveTime::parse_from_str(s, "%H:%M:%S%.f").map_err(|_| failed())?,
        )),
        (DataType::Timestamp { .. }, JsonValue::String(s)) => Ok(Value::Timestamp(
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f")
                .map_err(|_| failed())?,
        )),
        (DataType::TimestampWithTimeZone { .. }, JsonValue::String(s)) => {
            let parsed = if s.ends_with(" UTC") {
                NaiveDateTime::parse_from_str(
                    s.trim_end_matches(" UTC"),
                    "%Y-%m-%d %H:%M:%S%.6f",
                )
                .map_err(|_| failed())?
                .and_utc()
                .fixed_offset()
            } else {
                DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f %:z")
                    .map_err(|_| failed())?
                    .with_timezone(&FixedOffset::east_opt(0).unwrap())
            };
            Ok(Value::TimestampWithTimeZone(parsed))
        }
        (DataType::Array(elem_ty), JsonValue::Array(values)) => {
            let values = values
                .iter()
                .map(|v| deserialize_json_value(elem_ty, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Array {
                values,
                literal_type: data_type.clone(),
            })
        }
        (DataType::Row(fields), JsonValue::Array(values)) => {
            let values = fields
                .iter()
                .zip(values)
                .map(|(field, v)| deserialize_json_value(&field.data_type, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Row {
                values,
                literal_type: data_type.clone(),
            })
        }
        (DataType::Uuid, JsonValue::String(s)) => {
            Ok(Value::Uuid(Uuid::parse_str(s).map_err(|_| failed())?))
        }
        (DataType::SphericalGeography, JsonValue::String(wkt_str)) => {
            let geom =
                Geometry::<f64>::try_from_wkt_str(wkt_str).map_err(|_| failed())?;
            Ok(Value::SphericalGeography(geom))
        }
        _ => Err(failed()),
    }
}
