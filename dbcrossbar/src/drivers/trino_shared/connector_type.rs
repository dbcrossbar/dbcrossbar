//! Information about Trino's backend storage "catalogs" and the features they
//! support.

//! Trino connector types.

use std::{fmt, str::FromStr};

use crate::common::*;

use super::{TrinoDataType, TrinoField};

/// What type of Trino data storage connector are we working with? We need to
/// know this so that we can generate `NOT NULL` and similar SQL features for
/// backends that support, while leaving them out where they'd cause an error.
/// `dbcrossbar`'s goal is always to produce the best representation it can, but
/// there's no one perfect answer for all Trino connectors.
///
/// If you add a new connector type here, you should also update the integration
/// test `trino_connector_types_downgrade_as_needed` to make sure we can
/// actually copy data into it.
#[allow(missing_docs)]
pub enum TrinoConnectorType {
    Hive,
    Iceberg,
    Memory,
    Postgresql,
    /// Unknown type. Assume worst-case version of all features.
    Other(String),
}

impl TrinoConnectorType {
    /// Do we know that this backend supports `NOT NULL`?
    pub(crate) fn supports_not_null_constraint(&self) -> bool {
        match self {
            TrinoConnectorType::Memory => false,
            TrinoConnectorType::Postgresql => true,
            _ => false,
        }
    }

    /// Do we know that this backend supports `OR REPLACE`?
    pub(crate) fn supports_replace_table(&self) -> bool {
        match self {
            TrinoConnectorType::Memory => false,
            TrinoConnectorType::Postgresql => false,
            _ => false,
        }
    }

    /// "Downgrade" a [`TrinoDataType`] as needed to work with a specific
    /// backend.
    pub(super) fn downgrade_data_type(
        &self,
        data_type: &TrinoDataType,
    ) -> TrinoDataType {
        match (self, data_type) {
            // Hive doesn't support `TIMESTAMP WITH TIME ZONE`, so we'll use
            // `TIMESTAMP` instead (since we always use `"Z"` for time zones in
            // our wire format anyways).
            (
                TrinoConnectorType::Hive,
                TrinoDataType::TimestampWithTimeZone { precision },
            ) => TrinoDataType::Timestamp {
                precision: *precision,
            },

            // Hive is missing a bunch of other types we'll need to convert to
            // strings.
            (TrinoConnectorType::Hive, TrinoDataType::Json)
            | (TrinoConnectorType::Hive, TrinoDataType::Uuid)
            | (TrinoConnectorType::Hive, TrinoDataType::SphericalGeography) => {
                TrinoDataType::varchar()
            }

            // Iceberg doesn't support smaller integer types.
            (TrinoConnectorType::Iceberg, TrinoDataType::TinyInt)
            | (TrinoConnectorType::Iceberg, TrinoDataType::SmallInt) => {
                TrinoDataType::Int
            }

            // Iceberg only supports precision 6 for time types.
            (TrinoConnectorType::Iceberg, TrinoDataType::Time { .. }) => {
                TrinoDataType::Time { precision: 6 }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::TimeWithTimeZone { .. }) => {
                TrinoDataType::TimeWithTimeZone { precision: 6 }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::Timestamp { .. }) => {
                TrinoDataType::Timestamp { precision: 6 }
            }
            (
                TrinoConnectorType::Iceberg,
                TrinoDataType::TimestampWithTimeZone { .. },
            ) => TrinoDataType::TimestampWithTimeZone { precision: 6 },

            // Iceberg is also missing a bunch of types we'll need to convert to
            // strings.
            (TrinoConnectorType::Iceberg, TrinoDataType::Json)
            | (TrinoConnectorType::Iceberg, TrinoDataType::SphericalGeography) => {
                TrinoDataType::varchar()
            }

            // Process arrays recursively.
            (_, TrinoDataType::Array(elem_ty)) => {
                TrinoDataType::Array(Box::new(self.downgrade_data_type(elem_ty)))
            }

            // Process maps recursively.
            (
                _,
                TrinoDataType::Map {
                    key_type,
                    value_type,
                },
            ) => TrinoDataType::Map {
                key_type: Box::new(self.downgrade_data_type(key_type)),
                value_type: Box::new(self.downgrade_data_type(value_type)),
            },

            // Process rows recursively.
            (_, TrinoDataType::Row(fields)) => TrinoDataType::Row(
                fields
                    .iter()
                    .map(|field| TrinoField {
                        name: field.name.clone(),
                        data_type: self.downgrade_data_type(&field.data_type),
                    })
                    .collect(),
            ),

            // Pass everything else through and hope it works.
            _ => data_type.clone(),
        }
    }
}

impl FromStr for TrinoConnectorType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "hive" => TrinoConnectorType::Hive,
            "iceberg" => TrinoConnectorType::Iceberg,
            "memory" => TrinoConnectorType::Memory,
            "postgresql" => TrinoConnectorType::Postgresql,
            other => TrinoConnectorType::Other(other.to_owned()),
        })
    }
}

impl fmt::Display for TrinoConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TrinoConnectorType::Hive => "hive".fmt(f),
            TrinoConnectorType::Iceberg => "iceberg".fmt(f),
            TrinoConnectorType::Memory => "memory".fmt(f),
            TrinoConnectorType::Postgresql => "postgresql".fmt(f),
            TrinoConnectorType::Other(other) => other.fmt(f),
        }
    }
}
