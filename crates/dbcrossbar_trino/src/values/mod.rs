//! Trino values (or at least those we care about).

use std::{convert::Infallible, error, fmt};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use geo_types::Geometry;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use uuid::Uuid;
use wkt::ToWkt;

use crate::{DataType, Ident, QuotedString};

pub use self::is_close_enough_to::IsCloseEnoughTo;

mod is_close_enough_to;

/// A Trino value of one of our supported types.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Value {
    /// A NULL value. We need to know the type because this is a fairly
    /// low-level library and we don't do things like type inference or
    /// unification.
    Null {
        literal_type: DataType,
    },
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    /// A precise, fixed-point decimal value. Typically used to represent
    /// monetary values. Note that this only holds 96 bits of precision, and
    /// several popular databases have more, so this may not able to actually
    /// represent all possible values supported by Trino.
    Decimal(Decimal),
    Varchar(String),
    Varbinary(Vec<u8>),
    Json(JsonValue),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampWithTimeZone(DateTime<FixedOffset>),
    Array {
        /// The values in the array.
        values: Vec<Value>,
        /// The type of this array. Needed to help print empty arrays.
        literal_type: DataType,
    },
    Row {
        /// The values in the row.
        values: Vec<Value>,
        /// The type of this row. Needed to specify the field names of a literal
        /// array value.
        literal_type: DataType,
    },
    Uuid(Uuid),
    SphericalGeography(Geometry<f64>),
}

impl Value {
    /// Does a printed literal of this value require a cast?
    ///
    /// We go out of our way to only do this when necessry to make
    /// it easier to read generated test code.
    fn cast_required_by_literal(&self) -> Option<&DataType> {
        match self {
            Value::Array {
                values,
                literal_type,
            } => {
                if values.is_empty()
                    || values
                        .iter()
                        .any(|v| v.cast_required_by_literal().is_some())
                {
                    Some(literal_type)
                } else {
                    None
                }
            }

            Value::Row {
                values,
                literal_type,
            } => {
                if literal_type.is_row_with_named_fields()
                    || values
                        .iter()
                        .any(|v| v.cast_required_by_literal().is_some())
                {
                    Some(literal_type)
                } else {
                    None
                }
            }

            _ => None,
        }
    }

    /// Recursive [`fmt::Display::fmt`] helper.
    fn fmt_helper(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Null { literal_type } => {
                write!(f, "CAST(NULL AS {})", literal_type)
            }
            Value::Boolean(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Int(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            // Use scientific notation to prevent giant decimal literals that
            // Trino can't parse.
            Value::Real(fl) => write!(f, "REAL '{:e}'", fl),
            Value::Double(fl) => write!(f, "{:e}", fl),
            Value::Decimal(d) => write!(f, "DECIMAL '{}'", d),
            Value::Varchar(s) => write!(f, "{}", QuotedString(s)),
            Value::Varbinary(vec) => {
                write!(f, "X'")?;
                for byte in vec {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
            Value::Json(value) => {
                let json_str =
                    serde_json::to_string(value).expect("could not serialize JSON");
                write!(f, "JSON {}", QuotedString(&json_str))
            }
            Value::Date(naive_date) => {
                write!(f, "DATE '{}'", naive_date.format("%Y-%m-%d"))
            }
            Value::Time(naive_time) => {
                write!(f, "TIME '{}'", naive_time.format("%H:%M:%S%.6f"))
            }
            Value::Timestamp(naive_date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    naive_date_time.format("%Y-%m-%d %H:%M:%S%.6f")
                )
            }
            Value::TimestampWithTimeZone(date_time) => {
                write!(
                    f,
                    "TIMESTAMP '{}'",
                    date_time.format("%Y-%m-%d %H:%M:%S%.6f %:z")
                )
            }
            Value::Array {
                values,
                literal_type: _,
            } => {
                write!(f, "ARRAY[")?;
                for (idx, elem) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }
            Value::Row {
                values,
                literal_type: _,
            } => {
                write!(f, "ROW(")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, ")")
            }
            Value::Uuid(uuid) => write!(f, "UUID '{}'", uuid),
            Value::SphericalGeography(value) => {
                write!(
                    f,
                    "to_spherical_geography(ST_GeometryFromText({}))",
                    QuotedString(&value.wkt_string())
                )
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cast_to = self.cast_required_by_literal();
        if cast_to.is_some() {
            write!(f, "CAST(")?;
        }
        self.fmt_helper(f)?;
        if let Some(data_type) = cast_to {
            write!(f, " AS {})", data_type)?;
        }
        Ok(())
    }
}

/// A type conversion error.
#[derive(Debug, Clone)]
pub struct ConversionError {
    /// The value that could not be converted.
    pub found: Value,
    /// The expected type.
    pub expected_type: DataTypeOrAny,
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "expected a value of type {}, got {}",
            self.expected_type, self.found
        )
    }
}

impl error::Error for ConversionError {}

/// Some conversions cannot fail, and thus return [`Infallible`] as an error. No
/// values of [`Infallible`] can ever exist. By declaring this conversion, we
/// can allow Rust's automatic `TryFrom<Value> for Value` (instantiated from
/// `TryFrom<T> for T`) to be mixed with the other conversions we define, even
/// though it cannot fail.
impl From<Infallible> for ConversionError {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

/// Either a specific [`DataType`], or any Trino type. This is used when
/// converting values to and from Trino.
#[derive(Clone, Debug, PartialEq)]
pub enum DataTypeOrAny {
    DataType(DataType),
    Array(Box<DataTypeOrAny>),
    Row(Vec<FieldWithDataTypeOrAny>),
    Any,
}

impl fmt::Display for DataTypeOrAny {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataTypeOrAny::DataType(data_type) => write!(f, "{}", data_type),
            DataTypeOrAny::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
            DataTypeOrAny::Row(fields) => {
                write!(f, "ROW(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            DataTypeOrAny::Any => write!(f, "ANY"),
        }
    }
}

// A field with where the data type may not be known. Used for conversions.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldWithDataTypeOrAny {
    pub name: Option<Ident>,
    pub data_type: DataTypeOrAny,
}

impl fmt::Display for FieldWithDataTypeOrAny {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

/// Get the [`DataType`] that would be used to represent a Rust value.
pub trait ExpectedDataType {
    fn expected_data_type() -> DataTypeOrAny;
}

impl ExpectedDataType for Value {
    fn expected_data_type() -> DataTypeOrAny {
        DataTypeOrAny::Any
    }
}

// Macro for From and TryFrom implementations for scalar types.
macro_rules! conversions {
    ($from:ty, $variant:ident, $expected_type:expr) => {
        impl ExpectedDataType for $from {
            fn expected_data_type() -> DataTypeOrAny {
                DataTypeOrAny::DataType($expected_type)
            }
        }

        impl From<$from> for Value {
            fn from(value: $from) -> Self {
                Value::$variant(value)
            }
        }

        impl TryFrom<Value> for $from {
            type Error = ConversionError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(v) => Ok(v),
                    other => Err(ConversionError {
                        found: other,
                        expected_type: DataTypeOrAny::DataType($expected_type),
                    }),
                }
            }
        }
    };
}

conversions!(bool, Boolean, DataType::Boolean);
conversions!(i8, TinyInt, DataType::TinyInt);
conversions!(i16, SmallInt, DataType::SmallInt);
conversions!(i32, Int, DataType::Int);
conversions!(i64, BigInt, DataType::BigInt);
conversions!(f32, Real, DataType::Real);
conversions!(f64, Double, DataType::Double);
conversions!(Decimal, Decimal, DataType::bigquery_sized_decimal());
conversions!(String, Varchar, DataType::varchar());
conversions!(Vec<u8>, Varbinary, DataType::Varbinary);
conversions!(JsonValue, Json, DataType::Json);
conversions!(NaiveDate, Date, DataType::Date);
conversions!(NaiveTime, Time, DataType::time());
conversions!(NaiveDateTime, Timestamp, DataType::timestamp());
conversions!(
    DateTime<FixedOffset>,
    TimestampWithTimeZone,
    DataType::timestamp_with_time_zone()
);
conversions!(Uuid, Uuid, DataType::Uuid);
conversions!(
    Geometry<f64>,
    SphericalGeography,
    DataType::SphericalGeography
);

impl<T> ExpectedDataType for Vec<T>
where
    T: ExpectedDataType,
{
    fn expected_data_type() -> DataTypeOrAny {
        DataTypeOrAny::Array(Box::new(T::expected_data_type()))
    }
}

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value> + ExpectedDataType,
    ConversionError: From<<T as TryFrom<Value>>::Error>,
{
    type Error = ConversionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            // We also allow unpacking a single row into a Vec. This will fail
            // unless all the fields in the row have a compatible type.
            Value::Array { values, .. } | Value::Row { values, .. } => Ok(values
                .into_iter()
                .map(T::try_from)
                .collect::<Result<Vec<_>, _>>()?),
            other => Err(ConversionError {
                found: other,
                expected_type: Self::expected_data_type(),
            }),
        }
    }
}
