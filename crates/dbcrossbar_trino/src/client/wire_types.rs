//! Trino data types, as represented in the Trino REST API.

use serde::Deserialize;

use crate::{DataType, Field, Ident};

use super::ClientError;

/// A Trino type signature.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub(crate) struct TypeSignature {
    pub(crate) raw_type: RawType,
    pub(crate) arguments: Vec<Argument>,
}

impl TypeSignature {
    /// Get the element type of an array type.
    pub(crate) fn array_element_type(&self) -> Result<&TypeSignature, ClientError> {
        match (&self.raw_type, self.arguments.as_slice()) {
            (RawType::Array, [Argument::Type { value }]) => Ok(value),
            _ => Err(ClientError::UnsupportedTypeSignature {
                type_signature: Box::new(self.clone()),
            }),
        }
    }

    // /// Get a named type argument.
    // pub(crate) fn named_type_argument(
    //     &self,
    //     n: usize,
    // ) -> Result<&NamedType, ClientError> {
    //     match self.arguments.get(n) {
    //         Some(Argument::NamedType { value }) => Ok(value),
    //         _ => Err(ClientError::UnsupportedTypeSignature {
    //             type_signature: Box::new(self.clone()),
    //         }),
    //     }
    // }

    /// Get a numeric argument.
    pub(crate) fn numeric_argument(
        &self,
        n: usize,
    ) -> Result<Option<i64>, ClientError> {
        match self.arguments.get(n) {
            None => Ok(None),
            Some(Argument::Long { value }) => Ok(Some(*value)),
            _ => Err(ClientError::UnsupportedTypeSignature {
                type_signature: Box::new(self.clone()),
            }),
        }
    }

    /// Get a numeric argument of type `u32`.
    pub fn numeric_argument_u32(&self, n: usize) -> Result<Option<u32>, ClientError> {
        match self.numeric_argument(n)? {
            None => Ok(None),
            Some(v) => Ok(Some(v.try_into().map_err(|_| {
                ClientError::UnsupportedTypeSignature {
                    type_signature: Box::new(self.clone()),
                }
            })?)),
        }
    }
}

impl TryFrom<&'_ TypeSignature> for DataType {
    type Error = ClientError;

    fn try_from(value: &TypeSignature) -> Result<Self, Self::Error> {
        match value.raw_type {
            RawType::Array => {
                let element_type = value.array_element_type()?;
                Ok(DataType::Array(Box::new(element_type.try_into()?)))
            }
            RawType::BigInt => Ok(DataType::BigInt),
            RawType::Boolean => Ok(DataType::Boolean),
            RawType::Date => Ok(DataType::Date),
            RawType::Decimal => Ok(DataType::Decimal {
                precision: value.numeric_argument_u32(0)?.ok_or_else(|| {
                    ClientError::UnsupportedTypeSignature {
                        type_signature: Box::new(value.clone()),
                    }
                })?,
                scale: value.numeric_argument_u32(1)?.unwrap_or(0),
            }),
            RawType::Double => Ok(DataType::Double),
            RawType::Integer => Ok(DataType::Int),
            RawType::Json => Ok(DataType::Json),
            RawType::Real => Ok(DataType::Real),
            RawType::Row => {
                let fields = value
                    .arguments
                    .iter()
                    .map(|arg| match arg {
                        Argument::NamedType { value } => {
                            let data_type = (&value.type_signature).try_into()?;
                            Ok(Field {
                                name: value
                                    .field_name
                                    .as_ref()
                                    .map(|f| f.name.clone()),
                                data_type,
                            })
                        }
                        _ => Err(ClientError::UnsupportedTypeSignature {
                            type_signature: Box::new(value.clone()),
                        }),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Row(fields))
            }
            RawType::SmallInt => Ok(DataType::SmallInt),
            RawType::SphericalGeography => Ok(DataType::SphericalGeography),
            RawType::TinyInt => Ok(DataType::TinyInt),
            RawType::Time => Ok(DataType::Time {
                precision: value.numeric_argument_u32(0)?.unwrap_or(3),
            }),
            RawType::Timestamp => Ok(DataType::Timestamp {
                precision: value.numeric_argument_u32(0)?.unwrap_or(3),
            }),
            RawType::TimestampWithTimeZone => Ok(DataType::TimestampWithTimeZone {
                precision: value.numeric_argument_u32(0)?.unwrap_or(3),
            }),
            RawType::Uuid => Ok(DataType::Uuid),
            RawType::Varbinary => Ok(DataType::Varbinary),
            RawType::Varchar => Ok(DataType::Varchar {
                // Ignore length on load because we don't need it right now.
                length: None,
            }),
        }
    }
}

/// A raw Trino type.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub(crate) enum RawType {
    Array,
    BigInt,
    Boolean,
    Date,
    Decimal,
    Double,
    Integer,
    Json,
    Real,
    Row,
    SmallInt,
    #[serde(rename = "SphericalGeography")]
    SphericalGeography,
    TinyInt,
    Time,
    Timestamp,
    #[serde(rename = "timestamp with time zone")]
    TimestampWithTimeZone,
    Uuid,
    Varbinary,
    Varchar,
}

/// An argument to a Trino type signature.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind")]
#[non_exhaustive]
pub(crate) enum Argument {
    #[serde(rename = "LONG")]
    Long { value: i64 },

    #[serde(rename = "NAMED_TYPE")]
    NamedType { value: NamedType },

    #[serde(rename = "TYPE")]
    Type { value: TypeSignature },
}

/// A named type in a Trino type signature.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub(crate) struct NamedType {
    pub field_name: Option<FieldName>,
    pub type_signature: TypeSignature,
}

/// A field name in a Trino type signature.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub(crate) struct FieldName {
    pub name: Ident,
}
