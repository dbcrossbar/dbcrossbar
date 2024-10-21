//! Information about Trino's backend storage "catalogs" and the features they
//! support.

//! Trino connector types.

use std::{collections::HashMap, fmt, str::FromStr};

#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::{
    errors::ConnectorError,
    transforms::{
        FieldName, FieldStorageTransform, StorageTransform, TypeStorageTransform,
    },
    TableOptionValue, TableOptions, TrinoIdent,
};

use super::TrinoDataType;

/// Compatibility information about each supported Trino connector type.
///
/// ### Usage
///
/// ```
/// use dbcrossbar_trino_types::{
///     TableOptions, TrinoConnectorType, TrinoDataType,
/// };
///
/// // Choose our connector type.
/// let connector = TrinoConnectorType::Hive;
/// let table_name = "hive.default.my_table";
///
/// /// Get some SQL fragments we'll need to create a table.
/// let not_null_sql = if connector.supports_not_null_constraint() {
///    " NOT NULL"
/// } else {
///   // This connector cannot enforce `NOT NULL` constraints.
///   ""
/// };
/// let or_replace_sql = if connector.supports_replace_table() {
///   " OR REPLACE"
/// } else {
///   // You will need a separate `DROP TABLE` statement in this case.
///   ""
/// };
///
/// /// Get options to support `MERGE`, if it's available.
/// let table_options = if connector.supports_merge() {
///    connector.table_options_for_merge()
/// } else {
///   // You won't be able to use `MERGE` with this connector.
///   TableOptions::default()
/// };
///
/// /// Define a column type.
/// let col_ty = TrinoDataType::Array(Box::new(TrinoDataType::Json));
///
/// /// Get a storage transform for a specific data type.
/// let storage_transform = connector.storage_transform_for(&col_ty);
/// let storage_type = storage_transform.storage_type();
///
/// /// SQL to create our table.
/// let create_table_sql = format!(
///    "CREATE TABLE {table_name} (
///       x {storage_type} {not_null_sql}
///   ){table_options}"
/// );
///
/// /// SQL to insert a row.
/// let insert_sql = format!(
///   "INSERT INTO {table_name} (x) VALUES ({});",
///   storage_transform.store_expr(&"JSON '[1, 2, 3]'"),
/// );
///
/// /// SQL to select a row.
/// let select_sql = format!(
///   "SELECT {} AS x FROM {table_name};",
///   storage_transform.load_expr(&"x"),
/// );
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
#[allow(missing_docs)]
#[non_exhaustive]
pub enum TrinoConnectorType {
    /// The [Hive](https://trino.io/docs/current/connector/hive.html) connector.
    /// This is the "default" connector used in most Trino installations.
    Hive,
    /// The [Iceberg](https://trino.io/docs/current/connector/iceberg.html)
    /// connector. This is a newer format for large data sets, maintained by the
    /// Apache project. It runs on top of Hive, using the same underlying
    /// storage and namespaces.
    Iceberg,
    /// The built-in
    /// [memory](https://trino.io/docs/current/connector/memory.html) connector,
    /// most useful for tests. This supports an unusually high number of data
    /// types. So don't assume that just because something works with this
    /// connector, that it will work with "real" connectors.
    Memory,
    // (Athena3's default connector may need a separate value here.)
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
        let type_storage_transform = self.type_storage_transform_for(ty);
        StorageTransform::new(ty.clone(), type_storage_transform)
    }

    /// Internal recursive helper for [`Self::storage_transform_for`].
    fn type_storage_transform_for(&self, ty: &TrinoDataType) -> TypeStorageTransform {
        match (self, ty) {
            // Iceberg.
            (
                TrinoConnectorType::Iceberg,
                TrinoDataType::TinyInt | TrinoDataType::SmallInt,
            ) => TypeStorageTransform::SmallerIntAsInt,
            (TrinoConnectorType::Iceberg, TrinoDataType::Time { precision })
                if *precision != 6 =>
            {
                TypeStorageTransform::TimeWithPrecision {
                    stored_precision: 6,
                }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::Timestamp { precision })
                if *precision != 6 =>
            {
                TypeStorageTransform::TimestampWithPrecision {
                    stored_precision: 6,
                }
            }
            (
                TrinoConnectorType::Iceberg,
                TrinoDataType::TimestampWithTimeZone { precision },
            ) if *precision != 6 => {
                TypeStorageTransform::TimestampWithTimeZoneWithPrecision {
                    stored_precision: 6,
                }
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::Json) => {
                TypeStorageTransform::JsonAsVarchar
            }
            (TrinoConnectorType::Iceberg, TrinoDataType::SphericalGeography) => {
                TypeStorageTransform::SphericalGeographyAsWkt
            }

            // Hive.
            (TrinoConnectorType::Hive, TrinoDataType::Time { .. }) => {
                TypeStorageTransform::TimeAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::Timestamp { precision })
                if *precision != 3 =>
            {
                TypeStorageTransform::TimestampWithPrecision {
                    stored_precision: 3,
                }
            }
            (
                TrinoConnectorType::Hive,
                TrinoDataType::TimestampWithTimeZone { .. },
            ) => TypeStorageTransform::TimestampWithTimeZoneAsTimestamp {
                stored_precision: 3,
            },
            (TrinoConnectorType::Hive, TrinoDataType::Json) => {
                TypeStorageTransform::JsonAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::Uuid) => {
                TypeStorageTransform::UuidAsVarchar
            }
            (TrinoConnectorType::Hive, TrinoDataType::SphericalGeography) => {
                TypeStorageTransform::SphericalGeographyAsWkt
            }

            // Recursive types.
            (_, TrinoDataType::Array(elem_ty)) => TypeStorageTransform::Array {
                element_transform: Box::new(self.type_storage_transform_for(elem_ty)),
            }
            .simplify_top_level(),
            (_, TrinoDataType::Row(fields)) => TypeStorageTransform::Row {
                name_anonymous_fields: !self.supports_anonymous_row_fields(),
                field_transforms: fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| FieldStorageTransform {
                        name: match &field.name {
                            Some(name) => FieldName::Named(name.clone()),
                            None => FieldName::Indexed(idx + 1),
                        },
                        transform: self.type_storage_transform_for(&field.data_type),
                    })
                    .collect(),
            }
            .simplify_top_level(),

            // Start with just the identity transform until we have more tests.
            _ => TypeStorageTransform::Identity,
        }
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

#[cfg(all(test, feature = "client"))]
mod tests {
    use crate::client::Client;

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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
