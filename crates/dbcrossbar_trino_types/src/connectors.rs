//! Information about Trino's backend storage "catalogs" and the features they
//! support.

//! Trino connector types.

use std::{fmt, str::FromStr};

#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::{
    errors::ConnectorError,
    transforms::{FieldName, FieldStorageTransform, StorageTransform},
};

use super::TrinoDataType;

/// What type of Trino data storage connector are we working with? We need to
/// know this so that we can generate `NOT NULL` and similar SQL features for
/// backends that support, while leaving them out where they'd cause an error.
/// `dbcrossbar`'s goal is always to produce the best representation it can, but
/// there's no one perfect answer for all Trino connectors.
///
/// If you add a new connector type here, you should also update the integration
/// test `trino_connector_types_downgrade_as_needed` to make sure we can
/// actually copy data into it.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
#[allow(missing_docs)]
#[non_exhaustive]
pub enum TrinoConnectorType {
    Hive,
    Iceberg,
    Memory,
}

impl TrinoConnectorType {
    /// All connector types.
    #[cfg(test)]
    pub fn all() -> impl Iterator<Item = TrinoConnectorType> {
        [
            TrinoConnectorType::Memory,
            TrinoConnectorType::Hive,
            TrinoConnectorType::Iceberg,
        ]
        .into_iter()
    }

    /// What catalog name should we use for this connector type in test mode?
    #[cfg(test)]
    pub fn test_catalog(&self) -> &'static str {
        match self {
            TrinoConnectorType::Hive => "hive",
            TrinoConnectorType::Iceberg => "iceberg",
            TrinoConnectorType::Memory => "memory",
        }
    }

    /// What schema name should we use for this connector type in test mode?
    #[cfg(test)]
    pub fn test_schema(&self) -> &'static str {
        match self {
            TrinoConnectorType::Hive => "default",
            TrinoConnectorType::Iceberg => "default",
            TrinoConnectorType::Memory => "default",
        }
    }

    /// Do we know that this backend supports `NOT NULL`?
    pub fn supports_not_null_constraint(&self) -> bool {
        #[allow(clippy::match_single_binding)]
        match self {
            // TODO: Add a test which verifies this.
            //TrinoConnectorType::Memory => false,
            _ => true,
        }
    }

    /// Do we know that this backend supports `OR REPLACE`?
    pub fn supports_replace_table(&self) -> bool {
        #[allow(clippy::match_single_binding)]
        match self {
            // TODO: Add a test which verifies this.
            //TrinoConnectorType::Memory => false,
            _ => true,
        }
    }

    /// Does this backend support anonymous `ROW` fields?
    pub fn supports_anonymous_row_fields(&self) -> bool {
        match self {
            TrinoConnectorType::Hive => false,
            TrinoConnectorType::Iceberg => false,
            TrinoConnectorType::Memory => true,
        }
    }

    /// How should we transform a given data type for storage in this backend?
    pub fn storage_transform_for(&self, ty: &TrinoDataType) -> StorageTransform {
        match (self, ty) {
            // Iceberg.
            (
                TrinoConnectorType::Iceberg,
                TrinoDataType::TinyInt | TrinoDataType::SmallInt,
            ) => StorageTransform::SmallerIntAsInt,
            (TrinoConnectorType::Iceberg, TrinoDataType::Time { precision })
                if *precision != 6 =>
            {
                StorageTransform::TimeWithPrecision {
                    stored_precision: 6,
                }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::Timestamp { precision })
                if *precision != 6 =>
            {
                StorageTransform::TimestampWithPrecision {
                    stored_precision: 6,
                }
            }
            (
                TrinoConnectorType::Iceberg,
                TrinoDataType::TimestampWithTimeZone { precision },
            ) if *precision != 6 => {
                StorageTransform::TimestampWithTimeZoneWithPrecision {
                    stored_precision: 6,
                }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::Json) => {
                StorageTransform::JsonAsVarchar
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::SphericalGeography) => {
                StorageTransform::SphericalGeographyAsVarchar
            }

            // Hive.
            (TrinoConnectorType::Hive, TrinoDataType::Time { .. }) => {
                StorageTransform::TimeAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::Timestamp { precision })
                if *precision != 3 =>
            {
                StorageTransform::TimestampWithPrecision {
                    stored_precision: 3,
                }
            }
            (
                TrinoConnectorType::Hive,
                TrinoDataType::TimestampWithTimeZone { .. },
            ) => StorageTransform::TimestampWithTimeZoneAsTimezone {
                stored_precision: 3,
            },
            (TrinoConnectorType::Hive, TrinoDataType::Json) => {
                StorageTransform::JsonAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::Uuid) => {
                StorageTransform::UuidAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::SphericalGeography) => {
                StorageTransform::SphericalGeographyAsVarchar
            }

            // Recursive types.
            (_, TrinoDataType::Array(elem_ty)) => StorageTransform::Array {
                element_transform: Box::new(self.storage_transform_for(elem_ty)),
            }
            .simplify_top_level(),
            (_, TrinoDataType::Row(fields)) => StorageTransform::Row {
                name_anonymous_fields: !self.supports_anonymous_row_fields(),
                field_transforms: fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| FieldStorageTransform {
                        name: match &field.name {
                            Some(name) => FieldName::Named(name.clone()),
                            None => FieldName::Indexed(idx + 1),
                        },
                        transform: self.storage_transform_for(&field.data_type),
                    })
                    .collect(),
            }
            .simplify_top_level(),

            // Start with just the identity transform until we have more tests.
            _ => StorageTransform::Identity,
        }
    }

    /// What type should we use to store the given type in this backend?
    pub fn storage_type_for(&self, ty: &TrinoDataType) -> TrinoDataType {
        self.storage_transform_for(ty).storage_type_for(ty)
    }
}

impl FromStr for TrinoConnectorType {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "hive" => TrinoConnectorType::Hive,
            "iceberg" => TrinoConnectorType::Iceberg,
            "memory" => TrinoConnectorType::Memory,
            _ => return Err(ConnectorError::UnsupportedType(s.to_string())),
        })
    }
}

impl fmt::Display for TrinoConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TrinoConnectorType::Hive => "hive".fmt(f),
            TrinoConnectorType::Iceberg => "iceberg".fmt(f),
            TrinoConnectorType::Memory => "memory".fmt(f),
        }
    }
}
