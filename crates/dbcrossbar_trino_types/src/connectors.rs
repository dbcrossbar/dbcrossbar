//! Information about Trino's backend storage "catalogs" and the features they
//! support.

//! Trino connector types.

use std::{collections::HashMap, fmt, str::FromStr};

#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::{
    errors::ConnectorError,
    transforms::{FieldName, FieldStorageTransform, StorageTransform},
    TableOptionValue, TableOptions, TrinoIdent,
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

    /// What table name should we use for a test?
    #[cfg(test)]
    pub fn test_table_name(&self, test_name: &str) -> String {
        // We need unique table names for each connector, because some of them
        // are actually implemented on top of others and share a table
        // namespace. For example, `hive.default.x` and `iceberg.default.x`
        // would conflict.
        format!(
            "{}.{}.{}_{}",
            self.test_catalog(),
            self.test_schema(),
            test_name,
            self
        )
    }

    /// Does this backend supports `NOT NULL`?
    pub fn supports_not_null_constraint(&self) -> bool {
        match self {
            TrinoConnectorType::Hive => false,
            TrinoConnectorType::Iceberg => true,
            TrinoConnectorType::Memory => false,
        }
    }

    /// Does this backend supports `OR REPLACE`?
    pub fn supports_replace_table(&self) -> bool {
        match self {
            TrinoConnectorType::Hive => false,
            TrinoConnectorType::Iceberg => true,
            TrinoConnectorType::Memory => false,
        }
    }

    /// Does this backend support upserts using `MERGE`?
    ///
    /// Note that you will need to create the table with options specified
    /// by [`Self::table_options_for_merge`] to make `MERGE` work.
    ///
    /// Note that it may be possible to use upserts by setting table-specific
    /// options. Also note that upserts probably require rewriting the complete
    /// stored table on disk. They tend to be proportional to the total stored
    /// data size, not the size of the changed/inserted rows, unless the backend
    /// supports indices (unlikely) or some kind of partitioning scheme (which
    /// you should carefully verify manually).
    pub fn supports_merge(&self) -> bool {
        match self {
            // Use `WITH(format = 'ORC', transactional=true)` to make `MERGE`
            // work with Hive.
            TrinoConnectorType::Hive => true,
            TrinoConnectorType::Iceberg => true,
            TrinoConnectorType::Memory => false,
        }
    }

    /// What table options, if any, are needed to make `MERGE` work?
    pub fn table_options_for_merge(&self) -> TableOptions {
        let mut options = HashMap::new();
        match self {
            TrinoConnectorType::Hive => {
                options.insert(
                    TrinoIdent::new("format").expect("bad ident"),
                    TableOptionValue::String("ORC".to_string()),
                );
                options.insert(
                    TrinoIdent::new("transactional").expect("bad ident"),
                    TableOptionValue::Boolean(true),
                );
            }
            TrinoConnectorType::Iceberg => {
                // No special options needed.
            }
            TrinoConnectorType::Memory => {
                // Not supported anyway.
            }
        }
        TableOptions(options)
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
                StorageTransform::SphericalGeographyAsWkt
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
            ) => StorageTransform::TimestampWithTimeZoneAsTimestamp {
                stored_precision: 3,
            },
            (TrinoConnectorType::Hive, TrinoDataType::Json) => {
                StorageTransform::JsonAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::Uuid) => {
                StorageTransform::UuidAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::SphericalGeography) => {
                StorageTransform::SphericalGeographyAsWkt
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

#[cfg(test)]
mod tests {
    use crate::test::client::Client;

    use super::*;

    /// Drop a table if it exists.
    async fn drop_table_if_exists(client: &Client, table_name: &str) {
        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", table_name);
        client
            .run_statement(&drop_table_sql)
            .await
            .expect("could not drop table");
    }

    #[tokio::test]
    async fn test_supports_not_null_constraint() {
        let client = Client::default();
        for connector in TrinoConnectorType::all() {
            // If the connector doesn't support `NOT NULL`, we don't need
            // to test it.
            if !connector.supports_not_null_constraint() {
                continue;
            }

            let table_name =
                connector.test_table_name("test_supports_not_null_constraint");
            drop_table_if_exists(&client, &table_name).await;

            let create_table_sql =
                format!("CREATE TABLE {} (x INT NOT NULL)", table_name);
            eprintln!("create_table_sql: {}", create_table_sql);
            client
                .run_statement(&create_table_sql)
                .await
                .expect("could not create table");
        }
    }

    #[tokio::test]
    async fn test_supports_replace_table() {
        let client = Client::default();
        for connector in TrinoConnectorType::all() {
            // If the connector doesn't support `OR REPLACE`, we don't need
            // to test it.
            if !connector.supports_replace_table() {
                continue;
            }

            let table_name = connector.test_table_name("test_supports_replace_table");
            drop_table_if_exists(&client, &table_name).await;

            let create_table_sql = format!("CREATE TABLE {} (x INT)", table_name);
            eprintln!("create_table_sql: {}", create_table_sql);
            client
                .run_statement(&create_table_sql)
                .await
                .expect("could not create table");

            let create_or_replace_table_sql =
                format!("CREATE OR REPLACE TABLE {} (x INT)", table_name);
            eprintln!(
                "create_or_replace_table_sql: {}",
                create_or_replace_table_sql
            );
            client
                .run_statement(&create_or_replace_table_sql)
                .await
                .expect("could not create or replace table");
        }
    }

    #[tokio::test]
    async fn test_supports_merge() {
        let client = Client::default();
        for connector in TrinoConnectorType::all() {
            // If the connector doesn't support upserts, we don't need to test
            // it.
            if !connector.supports_merge() {
                continue;
            }

            let table_name = connector.test_table_name("test_supports_merge");
            drop_table_if_exists(&client, &table_name).await;

            let table_options = connector.table_options_for_merge();

            let create_table_sql = format!(
                "CREATE TABLE {} (id INT, name VARCHAR){}",
                table_name, table_options,
            );
            eprintln!("create_table_sql: {}", create_table_sql);
            client
                .run_statement(&create_table_sql)
                .await
                .expect("could not create table");

            // Try a merge statement. We don't test the result of the merge
            // yet, just that the connector claims to run it.
            let merge_sql = format!(
                "MERGE INTO {} AS target
                    USING (SELECT 1 AS id, 'Alice' AS name) AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN UPDATE SET name = source.name
                    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)",
                table_name
            );
            eprintln!("merge_sql: {}", merge_sql);
            client
                .run_statement(&merge_sql)
                .await
                .expect("could not merge");
        }
    }
}
