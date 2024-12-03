//! Write data from a remote storage location into Trino without passing it
//! through `dbcrossbar`.

use crate::{
    common::*,
    drivers::{
        s3::S3Locator,
        trino_shared::{TrinoCreateTable, TrinoDriverArguments, PRETTY_WIDTH},
    },
};

use super::TrinoLocator;

/// Implementation of `write_remote_data` for Trino.
#[instrument(
    level = "debug",
    name = "trino::write_remote_data",
    skip_all,
    fields(dest = %dest)
)]
pub(super) async fn write_remote_data_helper(
    dest: TrinoLocator,
    source: Box<dyn Locator>,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Make sure the source is an `S3Locator` and get the URL. We should only be
    // called if `supports_write_remote_data` returned `true` for `source`.
    let source_url = source
        .as_any()
        .downcast_ref::<S3Locator>()
        .ok_or_else(|| format_err!("not a s3:// locator: {}", source))?
        .as_url()
        .to_owned();

    let shared_args = shared_args.verify(TrinoLocator::features())?;
    let _source_args = source_args.verify(Features::empty())?;
    let dest_args = dest_args.verify(TrinoLocator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let _to_args = dest_args
        .driver_args()
        .deserialize::<TrinoDriverArguments>()?;
    let if_exists = dest_args.if_exists().to_owned();

    // Convert our destination schema into a `TrinoCreateTable`, and fix it up.
    //
    // TODO: Most of this is duplicated with `write_schema`. Fix that.
    let client = dest.client()?;
    let connector_type = dest.connector_type(&client).await?;
    let table_name = dest.table_name()?;
    let mut create_ideal_table =
        TrinoCreateTable::from_schema_and_name(schema, &table_name)?;
    create_ideal_table.set_if_exists_options(&if_exists, &connector_type);
    let create_storage_table =
        create_ideal_table.storage_table_for_connector_type(&connector_type);

    // Generate a `TrinoCreateTable` wrapping our S3 data.
    let create_s3_wrapper_table =
        create_ideal_table.hive_csv_wrapper_table(&source_url)?;
    let create_s3_wrapper_table_sql = create_s3_wrapper_table.to_string();
    debug!(sql = %create_s3_wrapper_table_sql, "creating S3 wrapper table");
    client.run_statement(&create_s3_wrapper_table_sql).await?;

    // Create our destination table (using our `create_storage_table`, so that
    // we can include things like `NOT NULL` constraints, if they're
    // supported).
    if let Some(separate_drop_if_exists) =
        create_storage_table.separate_drop_if_exists()
    {
        debug!(sql = %separate_drop_if_exists, "dropping destination table if it exists");
        client.run_statement(&separate_drop_if_exists).await?;
    }
    let create_storage_table_sql = create_storage_table.to_string();
    debug!(sql = %create_storage_table_sql, "creating destination table");
    client.run_statement(&create_storage_table_sql).await?;

    let insert_sql = match &if_exists {
        IfExists::Error | IfExists::Append | IfExists::Overwrite => {
            // Insert data from the S3 wrapper table into our destination table.
            format!(
                "{}",
                create_ideal_table
                    .insert_from_wrapper_table_doc(
                        &connector_type,
                        &create_s3_wrapper_table,
                    )?
                    .pretty(PRETTY_WIDTH)
            )
        }
        IfExists::Upsert(upsert_on) => {
            // Merge data from the S3 wrapper table into our destination table.
            format!(
                "{}",
                create_ideal_table
                    .merge_from_wrapper_table_doc(
                        &connector_type,
                        &create_s3_wrapper_table,
                        upsert_on,
                    )?
                    .pretty(PRETTY_WIDTH)
            )
        }
    };
    debug!(sql = %insert_sql, "inserting data");
    client.run_statement(&insert_sql).await?;

    // Clean up our S3 wrapper table.
    let drop_s3_wrapper_table_sql = format!(
        "DROP TABLE IF EXISTS {name}",
        name = create_s3_wrapper_table.name,
    );
    debug!(sql = %drop_s3_wrapper_table_sql, "dropping S3 wrapper table");
    client.run_statement(&drop_s3_wrapper_table_sql).await?;

    Ok(vec![dest.boxed()])
}
