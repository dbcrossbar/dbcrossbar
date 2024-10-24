//! A Trino data type.

use std::fmt;

use crate::ident::Ident;

/// A Trino data type.
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
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
    /// An array of values.
    Array(Box<DataType>),
    /// A row of fields.
    Row(Vec<Field>),
    /// A UUID.
    Uuid,
    /// A spherical geographic value. This isn't documented in the official list
    /// of Trino types, but it's mentioned in [their geospatial
    /// documentation](https://trino.io/docs/current/functions/geospatial.html).
    SphericalGeography,
}

impl DataType {
    pub fn bigquery_sized_decimal() -> Self {
        DataType::Decimal {
            precision: 38,
            scale: 9,
        }
    }

    pub fn varchar() -> Self {
        DataType::Varchar { length: None }
    }

    pub fn timestamp() -> Self {
        DataType::Timestamp { precision: 3 }
    }

    pub fn timestamp_with_time_zone() -> Self {
        DataType::TimestampWithTimeZone { precision: 3 }
    }

    /// Is this a ROW type with any named fields?
    #[cfg(feature = "values")]
    pub(crate) fn is_row_with_named_fields(&self) -> bool {
        match self {
            DataType::Row(fields) => fields.iter().any(|field| field.name.is_some()),
            _ => false,
        }
    }
}

// We keep around an implementation of `fmt::Display` for [`DataType`] mostly
// for use in error messages, where we don't need fancy formatting.
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Real => write!(f, "REAL"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({}, {})", precision, scale)
            }
            DataType::Varchar { length: None } => write!(f, "VARCHAR"),
            DataType::Varchar {
                length: Some(length),
            } => write!(f, "VARCHAR({})", length),
            DataType::Varbinary => write!(f, "VARBINARY"),
            DataType::Json => write!(f, "JSON"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time { precision } => write!(f, "TIME({})", precision),
            DataType::Timestamp { precision } => {
                write!(f, "TIMESTAMP({})", precision)
            }
            DataType::TimestampWithTimeZone { precision } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", precision)
            }
            DataType::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
            DataType::Row(fields) => {
                write!(f, "ROW(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            DataType::Uuid => write!(f, "UUID"),
            // This is capitalized differently in Trino's output.
            DataType::SphericalGeography => write!(f, "SphericalGeography"),
        }
    }
}

/// A field in a [`DataType::Row`] data type.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The name of the field.
    pub name: Option<Ident>,
    /// The data type of the field.
    pub data_type: DataType,
}

impl Field {
    /// Create an anonymous [`Field`] with a data type.
    pub fn anonymous(data_type: DataType) -> Self {
        Field {
            name: None,
            data_type,
        }
    }

    /// Create a named [`Field`] with a data type.
    pub fn named(name: Ident, data_type: DataType) -> Self {
        Field {
            name: Some(name),
            data_type,
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

#[cfg(all(test, feature = "proptest"))]
mod test {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn test_printable(data_type: DataType) {
            // Make sure we can print the data type without panicking.
            format!("{}", data_type);
        }
    }
}
