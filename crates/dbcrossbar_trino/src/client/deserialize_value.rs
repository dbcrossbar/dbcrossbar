//! Deserialize a Trino JSON value into a [`TrinoValue`].

use std::str::FromStr as _;

use base64::{prelude::BASE64_STANDARD, Engine as _};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use geo_types::Geometry;
use rust_decimal::Decimal;
use serde_json::Value;
use uuid::Uuid;
use wkt::TryFromWkt as _;

use crate::{values::TrinoValue, DataType};

use super::ClientError;

/// Deserialize a Trino JSON value into a [`TrinoValue`] of type
/// [`DataType`].
pub(crate) fn deserialize_value(
    data_type: &DataType,
    value: &Value,
) -> Result<TrinoValue, ClientError> {
    let failed = || ClientError::CouldNotDeserializeValue {
        value: value.clone(),
        data_type: data_type.clone(),
    };
    match (data_type, value) {
        (DataType::Boolean, Value::Bool(b)) => Ok(TrinoValue::Boolean(*b)),
        (DataType::TinyInt, Value::Number(n)) => Ok(TrinoValue::TinyInt(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::SmallInt, Value::Number(n)) => Ok(TrinoValue::SmallInt(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::Int, Value::Number(n)) => Ok(TrinoValue::Int(
            n.as_i64()
                .ok_or_else(failed)?
                .try_into()
                .map_err(|_| failed())?,
        )),
        (DataType::BigInt, Value::Number(n)) => {
            Ok(TrinoValue::BigInt(n.as_i64().ok_or_else(failed)?))
        }
        (DataType::Real, Value::Number(n)) => {
            Ok(TrinoValue::Real(n.as_f64().ok_or_else(failed)? as f32))
        }
        (DataType::Double, Value::Number(n)) => {
            Ok(TrinoValue::Double(n.as_f64().ok_or_else(failed)?))
        }
        (DataType::Decimal { .. }, Value::String(s)) => Ok(TrinoValue::Decimal(
            Decimal::from_str(s).map_err(|_| failed())?,
        )),
        (DataType::Varchar { .. }, Value::String(s)) => {
            Ok(TrinoValue::Varchar(s.clone()))
        }
        (DataType::Varbinary, Value::String(s)) => Ok(TrinoValue::Varbinary(
            BASE64_STANDARD.decode(s.as_bytes()).map_err(|_| failed())?,
        )),
        (DataType::Json, Value::String(s)) => {
            let json = serde_json::from_str(s).map_err(|_| failed())?;
            Ok(TrinoValue::Json(json))
        }
        (DataType::Date, Value::String(s)) => Ok(TrinoValue::Date(
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| failed())?,
        )),
        (DataType::Time { .. }, Value::String(s)) => Ok(TrinoValue::Time(
            NaiveTime::parse_from_str(s, "%H:%M:%S%.f").map_err(|_| failed())?,
        )),
        (DataType::Timestamp { .. }, Value::String(s)) => Ok(TrinoValue::Timestamp(
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.6f")
                .map_err(|_| failed())?,
        )),
        (DataType::TimestampWithTimeZone { .. }, Value::String(s)) => {
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
            Ok(TrinoValue::TimestampWithTimeZone(parsed))
        }
        (DataType::Array(elem_ty), Value::Array(values)) => {
            let values = values
                .iter()
                .map(|v| deserialize_value(elem_ty, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(TrinoValue::Array {
                values,
                literal_type: data_type.clone(),
            })
        }
        (DataType::Row(fields), Value::Array(values)) => {
            let values = fields
                .iter()
                .zip(values)
                .map(|(field, v)| deserialize_value(&field.data_type, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(TrinoValue::Row {
                values,
                literal_type: data_type.clone(),
            })
        }
        (DataType::Uuid, Value::String(s)) => {
            Ok(TrinoValue::Uuid(Uuid::parse_str(s).map_err(|_| failed())?))
        }
        (DataType::SphericalGeography, Value::String(wkt_str)) => {
            let geom =
                Geometry::<f64>::try_from_wkt_str(wkt_str).map_err(|_| failed())?;
            Ok(TrinoValue::SphericalGeography(geom))
        }
        _ => Err(failed()),
    }
}
