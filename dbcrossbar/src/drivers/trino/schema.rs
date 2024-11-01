//! Extract a table declaration from Trino.

use dbcrossbar_trino::TrinoRow;

use crate::{
    common::*,
    drivers::trino_shared::{
        parse_data_type, TrinoColumn, TrinoCreateTable, TrinoIdent, TrinoStringLiteral,
    },
};

use super::TrinoLocator;

/// Extract a table declaration from Trino.
#[instrument(level = "debug", name = "trino::schema", skip_all)]
pub(crate) async fn schema_helper(
    src: TrinoLocator,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<Schema>> {
    let _source_args = source_args.verify(TrinoLocator::features())?;

    let client = src.client()?;
    let table_name = src.table_name()?;
    let catalog = table_name
        .catalog()
        .ok_or_else(|| format_err!("no catalog in {}", src))?;
    let schema = table_name
        .schema()
        .ok_or_else(|| format_err!("no schema in {}", src))?;
    let sql = format!(
        "\
SELECT column_name, is_nullable, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_catalog = {catalog_str}
        AND table_schema = {schema_str}
        AND table_name = {table_str}
    ORDER BY ordinal_position",
        catalog = catalog,
        catalog_str = TrinoStringLiteral(catalog.as_unquoted_str()),
        schema_str = TrinoStringLiteral(schema.as_unquoted_str()),
        table_str = TrinoStringLiteral(table_name.table().as_unquoted_str()),
    );
    debug!(%sql, "getting table schema");
    let column_infos = client.get_all::<ColumnInfo>(&sql).await?;
    trace!(columns = ?column_infos, "got columns");
    let trino_columns = column_infos
        .into_iter()
        .map(|column_info| column_info.to_trino_column())
        .collect::<Result<Vec<_>>>()?;
    let trino_create_table =
        TrinoCreateTable::from_trino_columns_and_name(trino_columns, table_name)?;
    let schema = trino_create_table.to_schema()?;
    Ok(Some(schema))
}

/// A row with selected information from `information_schema.columns`.
#[derive(Debug, TrinoRow)]
struct ColumnInfo {
    column_name: String,
    is_nullable: String,
    data_type: String,
}

impl ColumnInfo {
    /// Convert this to a [`TrinoColumn`].
    fn to_trino_column(&self) -> Result<TrinoColumn> {
        let name = TrinoIdent::new(&self.column_name)?;
        let data_type = parse_data_type(&self.data_type)?;
        let is_nullable = self.is_nullable == "YES";
        Ok(TrinoColumn {
            name,
            data_type,
            is_nullable,
        })
    }
}
