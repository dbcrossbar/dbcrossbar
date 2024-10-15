//! A Trino data type.

use std::fmt;

use crate::ident::TrinoIdent;

/// A Trino data type.
///
/// If you add new types here, be sure to also add them to our
/// [`proptest::Arbitrary`] implementation.
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
    // /// This is a type that exists in Trino's type system, but that doesn't
    // /// exist for a particular [`super::TrinoConnectionType`].
    // ///
    // /// TODO: What about recusive types? Where do we put `Downgraded`? I _think_
    // /// we only want to use this for "leaf" types.
    // Downgraded {
    //     original_type: Box<TrinoDataType>,
    //     storage_type: Box<TrinoDataType>,
    // },
    // Left out for now: IP address, HyperLogLog, digests, etc.
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

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for TrinoDataType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            let leaf = prop_oneof![
                Just(TrinoDataType::Boolean),
                Just(TrinoDataType::TinyInt),
                Just(TrinoDataType::SmallInt),
                Just(TrinoDataType::Int),
                Just(TrinoDataType::BigInt),
                Just(TrinoDataType::Real),
                Just(TrinoDataType::Double),
                // Make sure we keep at least one digit before the decimal
                // point, for simplicity. Feel free to look at the support for
                // other precision/scale values in Trino and the storage drivers
                // and generalize this as needed.
                (3..=38u32, 0..=2u32).prop_map(|(precision, scale)| {
                    TrinoDataType::Decimal { precision, scale }
                }),
                Just(TrinoDataType::Varchar { length: None }),
                (1..=255u32).prop_map(|length| TrinoDataType::Varchar {
                    length: Some(length)
                }),
                Just(TrinoDataType::Varbinary),
                Just(TrinoDataType::Json),
                Just(TrinoDataType::Date),
                (1..=6u32).prop_map(|precision| TrinoDataType::Time { precision }),
                (1..=6u32)
                    .prop_map(|precision| TrinoDataType::Timestamp { precision }),
                (1..=6u32).prop_map(|precision| {
                    TrinoDataType::TimestampWithTimeZone { precision }
                }),
                Just(TrinoDataType::Uuid),
                Just(TrinoDataType::SphericalGeography),
            ];
            leaf.prop_recursive(3, 10, 3, |inner| {
                prop_oneof![
                    inner
                        .clone()
                        .prop_map(|elem_ty| TrinoDataType::Array(Box::new(elem_ty))),
                    prop::collection::vec((any::<Option<TrinoIdent>>(), inner), 1..=3)
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

    proptest! {
        #[test]
        fn test_printable(data_type: TrinoDataType) {
            // Make sure we can print the data type without panicking.
            format!("{}", data_type);
        }
    }
}
