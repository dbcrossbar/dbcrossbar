//! A Trino data type.

use std::fmt;

use crate::{
    common::*,
    schema::{DataType, StructField},
};

use super::TrinoIdent;

/// A Trino data type.
#[derive(Clone, Debug, PartialEq)]
pub enum TrinoDataType {
    /// A boolean value.
    Boolean,
    /// An 8-bit signed integer value.
    TinyInt,
    /// A 16-bit signed integer value.
    SmallInt,
    /// A 32-bit signed integer value.
    Int,
    /// A 64-bit signed integer value.
    BigInt,
    /// A 32-bit floating-point value.
    Real,
    /// A 64-bit floating-point value.
    Double,
    /// A fixed-point decimal value.
    Decimal {
        /// The total number of digits in the decimal value.
        precision: u32,
        /// The number of digits after the decimal point. Defaults to 0.
        scale: u32,
    },
    /// Variable-length character data.
    Varchar {
        /// The maximum number of characters in the string.
        length: Option<u32>,
    },
    /// Fixed-length character data.
    Char {
        /// The number of characters in the string. Defaults to 1.
        length: u32,
    },
    /// Variable-length binary data.
    Varbinary,
    /// JSON data.
    Json,
    /// A calendar date (year, month, day), with no time zone.
    Date,
    /// A time of day (hour, minute, second), with no time zone.
    Time {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// A time (hour, minute, second, milliseconds), with a time zone.
    TimeWithTimeZone {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with no time zone.
    Timestamp {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with a time zone.
    TimestampWithTimeZone {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// A time interval involving days, hours, minutes, and seconds.
    IntervalDayToSecond,
    /// A time interval involving years and months.
    IntervalYearToMonth,
    /// An array of values.
    Array(Box<TrinoDataType>),
    /// A map of keys to values.
    Map {
        /// The data type of the keys.
        key_type: Box<TrinoDataType>,
        /// The data type of the values.
        value_type: Box<TrinoDataType>,
    },
    /// A row of fields.
    Row(Vec<TrinoField>),
    /// A UUID.
    Uuid,
    /// A spherical geographic value. This isn't documented in the official list
    /// of Trino types, but it's mentioned in [their geospatial
    /// documentation](https://trino.io/docs/current/functions/geospatial.html).
    SphericalGeography,
    // Left out for now: IP address, HyperLogLog, digests, etc.
}

impl TrinoDataType {
    pub(crate) fn bigquery_sized_decimal() -> Self {
        TrinoDataType::Decimal {
            precision: 38,
            scale: 9,
        }
    }

    pub(crate) fn varchar() -> Self {
        TrinoDataType::Varchar { length: None }
    }

    pub(crate) fn timestamp() -> Self {
        TrinoDataType::Timestamp { precision: 3 }
    }

    pub(crate) fn timestamp_with_time_zone() -> Self {
        TrinoDataType::TimestampWithTimeZone { precision: 3 }
    }

    /// Given a `DataType`, try to find a corresponding `TrinoDataType`.
    pub(crate) fn from_data_type(
        schema: &Schema,
        ty: &DataType,
    ) -> Result<TrinoDataType> {
        match ty {
            DataType::Array(ty) => Ok(TrinoDataType::Array(Box::new(
                Self::from_data_type(schema, ty)?,
            ))),
            DataType::Bool => Ok(TrinoDataType::Boolean),
            DataType::Date => Ok(TrinoDataType::Date),
            // TODO: Document `DataType::Decimal` as having some limited
            // precision and scale?
            DataType::Decimal => Ok(Self::bigquery_sized_decimal()),
            DataType::Float32 => Ok(TrinoDataType::Real),
            DataType::Float64 => Ok(TrinoDataType::Double),
            // Map all SRIDs to spherical geography. You're responsible for
            // remembering what SRIDs you're using. Unlike BigQuery, Trino doesn't
            // seem to have an expected SRID, and unlike Postgres, it doesn't
            // record the SRID in the column type.
            DataType::GeoJson(_srid) => Ok(TrinoDataType::SphericalGeography),
            DataType::Int16 => Ok(TrinoDataType::SmallInt),
            DataType::Int32 => Ok(TrinoDataType::Int),
            DataType::Int64 => Ok(TrinoDataType::BigInt),
            DataType::Json => Ok(TrinoDataType::Json),
            DataType::Named(name) => {
                let ty = schema.data_type_for_name(name);
                Self::from_data_type(schema, ty)
            }
            // Enums/categoricals become strings.
            DataType::OneOf(_) => Ok(TrinoDataType::varchar()),
            DataType::Struct(fields) => Ok(TrinoDataType::Row(
                fields
                    .iter()
                    .map(|field| TrinoField::from_struct_field(schema, field))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Text => Ok(TrinoDataType::varchar()),
            DataType::TimestampWithoutTimeZone => Ok(TrinoDataType::timestamp()),
            DataType::TimestampWithTimeZone => {
                Ok(TrinoDataType::timestamp_with_time_zone())
            }
            DataType::Uuid => Ok(TrinoDataType::Uuid),
        }
    }

    /// Convert this `PgDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            TrinoDataType::Boolean => Ok(DataType::Bool),
            // We don't support 8-bit ints in our portable schema, so promote
            // them.
            TrinoDataType::TinyInt | TrinoDataType::SmallInt => Ok(DataType::Int16),
            TrinoDataType::Int => Ok(DataType::Int32),
            TrinoDataType::BigInt => Ok(DataType::Int64),
            TrinoDataType::Real => Ok(DataType::Float32),
            TrinoDataType::Double => Ok(DataType::Float64),
            TrinoDataType::Decimal { .. } => Ok(DataType::Decimal),
            TrinoDataType::Varchar { .. } | TrinoDataType::Char { .. } => {
                Ok(DataType::Text)
            }
            TrinoDataType::Varbinary => Err(format_err!(
                "VARBINARY is not yet supported in portable schemas"
            )),
            TrinoDataType::Json => Ok(DataType::Json),
            TrinoDataType::Date => Ok(DataType::Date),
            TrinoDataType::Time { .. } => {
                Err(format_err!("TIME is not yet supported in portable schemas"))
            }
            TrinoDataType::TimeWithTimeZone { .. } => Err(format_err!(
                "TIME WITH TIME ZONE is not yet supported in portable schemas"
            )),
            TrinoDataType::Timestamp { .. } => Ok(DataType::TimestampWithoutTimeZone),
            TrinoDataType::TimestampWithTimeZone { .. } => {
                Ok(DataType::TimestampWithTimeZone)
            }
            TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth => Err(format_err!(
                "INTERVAL types are not supported in portable schemas"
            )),
            TrinoDataType::Array(elem_ty) => {
                Ok(DataType::Array(Box::new(elem_ty.to_data_type()?)))
            }
            TrinoDataType::Map { key_type, .. } => {
                // Try to convert maps to JSON.
                let key_type = key_type.to_data_type()?;
                if key_type == DataType::Text {
                    Ok(DataType::Json)
                } else {
                    Err(format_err!("MAP key type must be TEXT"))
                }
            }
            TrinoDataType::Row(fields) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| field.to_struct_field(idx))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(fields))
            }
            TrinoDataType::Uuid => Ok(DataType::Uuid),
            // We don't know the SRID for a spherical geography, so we can't
            // map it to [`DataType::GeoJson(srid)`]. So just export it as JSON.
            TrinoDataType::SphericalGeography => Ok(DataType::Json),
        }
    }
}

impl fmt::Display for TrinoDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrinoDataType::Boolean => write!(f, "BOOLEAN"),
            TrinoDataType::TinyInt => write!(f, "TINYINT"),
            TrinoDataType::SmallInt => write!(f, "SMALLINT"),
            TrinoDataType::Int => write!(f, "INT"),
            TrinoDataType::BigInt => write!(f, "BIGINT"),
            TrinoDataType::Real => write!(f, "REAL"),
            TrinoDataType::Double => write!(f, "DOUBLE"),
            TrinoDataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({}, {})", precision, scale)
            }
            TrinoDataType::Varchar { length: None } => write!(f, "VARCHAR"),
            TrinoDataType::Varchar {
                length: Some(length),
            } => write!(f, "VARCHAR({})", length),
            TrinoDataType::Char { length: 1 } => write!(f, "CHAR"),
            TrinoDataType::Char { length } => write!(f, "CHAR({})", length),
            TrinoDataType::Varbinary => write!(f, "VARBINARY"),
            TrinoDataType::Json => write!(f, "JSON"),
            TrinoDataType::Date => write!(f, "DATE"),
            TrinoDataType::Time { precision } => write!(f, "TIME({})", precision),
            TrinoDataType::TimeWithTimeZone { precision } => {
                write!(f, "TIME({}) WITH TIME ZONE", precision)
            }
            TrinoDataType::Timestamp { precision } => {
                write!(f, "TIMESTAMP({})", precision)
            }
            TrinoDataType::TimestampWithTimeZone { precision } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", precision)
            }
            TrinoDataType::IntervalDayToSecond => write!(f, "INTERVAL DAY TO SECOND"),
            TrinoDataType::IntervalYearToMonth => write!(f, "INTERVAL YEAR TO MONTH"),
            TrinoDataType::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
            TrinoDataType::Map {
                key_type,
                value_type,
            } => {
                write!(f, "MAP({}, {})", key_type, value_type)
            }
            TrinoDataType::Row(fields) => {
                write!(f, "ROW(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            TrinoDataType::Uuid => write!(f, "UUID"),
            // This is capitalized differently in Trino's output.
            TrinoDataType::SphericalGeography => write!(f, "SphericalGeography"),
        }
    }
}

/// A field in a [`TrinoDataType::Row`] data type.
#[derive(Clone, Debug, PartialEq)]
pub struct TrinoField {
    /// The name of the field.
    name: Option<TrinoIdent>,
    /// The data type of the field.
    data_type: TrinoDataType,
}

impl TrinoField {
    /// Create an anonymous `TrinoField` with a data type.
    pub(crate) fn anonymous(data_type: TrinoDataType) -> Self {
        TrinoField {
            name: None,
            data_type,
        }
    }

    /// Create a named `TrinoField` with a data type.
    pub(crate) fn named(name: TrinoIdent, data_type: TrinoDataType) -> Self {
        TrinoField {
            name: Some(name),
            data_type,
        }
    }

    /// Given a `StructField`, try to find a corresponding `TrinoField`.
    pub(crate) fn from_struct_field(
        schema: &Schema,
        field: &StructField,
    ) -> Result<Self> {
        Ok(TrinoField {
            name: Some(TrinoIdent::new(&field.name)?),
            data_type: TrinoDataType::from_data_type(schema, &field.data_type)?,
        })
    }

    /// Convert this `TrinoField` to a portable `StructField`.
    pub(crate) fn to_struct_field(&self, idx: usize) -> Result<StructField> {
        let name = if let Some(name) = &self.name {
            name.to_string()
        } else {
            format!("_f{}", idx)
        };
        Ok(StructField {
            name,
            data_type: self.data_type.to_data_type()?,
            // Unless shown otherwise, assume fields are nullable.
            is_nullable: true,
        })
    }
}

impl fmt::Display for TrinoField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for TrinoDataType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        /// Generate an arbitrary [`TrinoDataType`]. We need to implement this
        /// manually because [`TrinoDataType`] is recursive, both on its own,
        /// and mututally recursive with [`TrinoField`].
        ///
        /// To learn more about this, read [the `proptest` book][proptest], and
        /// specifically the section on [recursive data][recursive].
        ///
        /// We don't export this directly. Instead, we wrap it in an `Arbitrary`
        /// implementation so it can be called as `any::<TrinoDataType>()`.
        ///
        /// [proptest]: https://proptest-rs.github.io/proptest/intro.html
        /// [recursion]: https://proptest-rs.github.io/proptest/proptest/tutorial/recursive.html
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            {
                let leaf = prop_oneof![
                    Just(TrinoDataType::Boolean),
                    Just(TrinoDataType::TinyInt),
                    Just(TrinoDataType::SmallInt),
                    Just(TrinoDataType::Int),
                    Just(TrinoDataType::BigInt),
                    Just(TrinoDataType::Real),
                    Just(TrinoDataType::Double),
                    (1..=38u32, 0..=9u32).prop_map(|(precision, scale)| {
                        TrinoDataType::Decimal { precision, scale }
                    }),
                    Just(TrinoDataType::Varchar { length: None }),
                    (1..=255u32).prop_map(|length| TrinoDataType::Varchar {
                        length: Some(length)
                    }),
                    Just(TrinoDataType::Char { length: 1 }),
                    (1..=255u32).prop_map(|length| TrinoDataType::Char { length }),
                    Just(TrinoDataType::Varbinary),
                    Just(TrinoDataType::Json),
                    Just(TrinoDataType::Date),
                    Just(TrinoDataType::Time { precision: 3 }),
                    (0..=9u32).prop_map(|precision| TrinoDataType::Time { precision }),
                    Just(TrinoDataType::TimeWithTimeZone { precision: 3 }),
                    (0..=9u32).prop_map(|precision| TrinoDataType::TimeWithTimeZone {
                        precision
                    }),
                    Just(TrinoDataType::Timestamp { precision: 3 }),
                    (0..=9u32)
                        .prop_map(|precision| TrinoDataType::Timestamp { precision }),
                    Just(TrinoDataType::TimestampWithTimeZone { precision: 3 }),
                    (0..=9u32).prop_map(|precision| {
                        TrinoDataType::TimestampWithTimeZone { precision }
                    }),
                    Just(TrinoDataType::IntervalDayToSecond),
                    Just(TrinoDataType::IntervalYearToMonth),
                    Just(TrinoDataType::Uuid),
                    Just(TrinoDataType::SphericalGeography),
                ];
                leaf.prop_recursive(3, 10, 5, |inner| {
                    prop_oneof![
                        inner
                            .clone()
                            .prop_map(|ty| TrinoDataType::Array(Box::new(ty))),
                        (inner.clone(), inner.clone()).prop_map(
                            |(key_type, value_type)| {
                                TrinoDataType::Map {
                                    key_type: Box::new(key_type),
                                    value_type: Box::new(value_type),
                                }
                            }
                        ),
                        (prop::collection::vec(
                            (any::<Option<TrinoIdent>>(), inner),
                            1..=3
                        ))
                        .prop_map(|fields| {
                            TrinoDataType::Row(
                                fields
                                    .into_iter()
                                    .map(|(name, data_type)| TrinoField {
                                        name,
                                        data_type,
                                    })
                                    .collect(),
                            )
                        }),
                    ]
                })
                .boxed()
            }
        }
    }
}
