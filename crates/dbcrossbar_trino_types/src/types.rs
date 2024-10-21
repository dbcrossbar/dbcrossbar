//! A Trino data type.

use std::fmt;

use crate::ident::TrinoIdent;

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
    Array(Box<TrinoDataType>),
    /// A row of fields.
    Row(Vec<TrinoField>),
    /// A UUID.
    Uuid,
    /// A spherical geographic value. This isn't documented in the official list
    /// of Trino types, but it's mentioned in [their geospatial
    /// documentation](https://trino.io/docs/current/functions/geospatial.html).
    SphericalGeography,
}

impl TrinoDataType {
    pub fn bigquery_sized_decimal() -> Self {
        TrinoDataType::Decimal {
            precision: 38,
            scale: 9,
        }
    }

    pub fn varchar() -> Self {
        TrinoDataType::Varchar { length: None }
    }

    pub fn timestamp() -> Self {
        TrinoDataType::Timestamp { precision: 3 }
    }

    pub fn timestamp_with_time_zone() -> Self {
        TrinoDataType::TimestampWithTimeZone { precision: 3 }
    }

    /// Is this a ROW type with any named fields?
    #[cfg(feature = "values")]
    pub(crate) fn is_row_with_named_fields(&self) -> bool {
        match self {
            TrinoDataType::Row(fields) => {
                fields.iter().any(|field| field.name.is_some())
            }
            _ => false,
        }
    }
}

// We keep around a separate implementation of `fmt::Display` for
// `TrinoDataType` mostly for use in error messages, where we don't need fancy
// formatting.
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
            TrinoDataType::Varbinary => write!(f, "VARBINARY"),
            TrinoDataType::Json => write!(f, "JSON"),
            TrinoDataType::Date => write!(f, "DATE"),
            TrinoDataType::Time { precision } => write!(f, "TIME({})", precision),
            TrinoDataType::Timestamp { precision } => {
                write!(f, "TIMESTAMP({})", precision)
            }
            TrinoDataType::TimestampWithTimeZone { precision } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", precision)
            }
            TrinoDataType::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
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
    pub(super) name: Option<TrinoIdent>,
    /// The data type of the field.
    pub(super) data_type: TrinoDataType,
}

impl TrinoField {
    /// Create an anonymous `TrinoField` with a data type.
    pub fn anonymous(data_type: TrinoDataType) -> Self {
        TrinoField {
            name: None,
            data_type,
        }
    }

    /// Create a named `TrinoField` with a data type.
    pub fn named(name: TrinoIdent, data_type: TrinoDataType) -> Self {
        TrinoField {
            name: Some(name),
            data_type,
        }
    }
}

// We keep this around for `impl fmt::Display for TrinoDataType` to use.
impl fmt::Display for TrinoField {
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
        fn test_printable(data_type: TrinoDataType) {
            // Make sure we can print the data type without panicking.
            format!("{}", data_type);
        }
    }
}
