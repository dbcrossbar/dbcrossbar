//! Write a portable schema to Trino, creating a new table.

use crate::{
    common::*,
    drivers::trino_shared::{TrinoCreateTable, TrinoDriverArguments},
};

use super::TrinoLocator;

#[instrument(
    level = "debug",
    name = "trino::write_schema",
    skip_all,
    fields(
        dest = %dest,
        if_exists = %if_exists,
    )
)]
pub(super) async fn write_schema_helper(
    dest: TrinoLocator,
    schema: Schema,
    if_exists: IfExists,
    dest_args: DestinationArguments<Unverified>,
) -> Result<()> {
    // Get our destination arguments.
    let dest_args = dest_args.verify(TrinoLocator::features()).unwrap();
    let _driver_args = dest_args
        .driver_args()
        .deserialize::<TrinoDriverArguments>()
        .unwrap();

    let client = dest.client()?;
    let connector_type = dest.connector_type(&client).await?;

    let table_name = dest.table_name()?;
    let mut create_table =
        TrinoCreateTable::from_schema_and_name(&schema, &table_name)?;
    create_table.set_if_exists_options(if_exists);
    create_table.downgrade_for_connector_type(&connector_type);
    if let Some(separate_drop_if_exists) = create_table.separate_drop_if_exists() {
        debug!(sql = %separate_drop_if_exists, "dropping table if it exists");
        client
            .run_statement(&separate_drop_if_exists)
            .await
            .with_context(|| format!("error dropping table {}", dest))?;
    }
    let sql = create_table.to_string();
    debug!(%sql, "creating table");
    client.run_statement(&sql).await?;
    Ok(())
}
