//! Implementation of `GsLocator::write_remote_data`.

use super::{prepare_as_destination_helper, S3Locator};
use crate::common::*;
use crate::drivers::trino::TrinoLocator;
use crate::drivers::trino_shared::{
    TrinoCreateTable, TrinoDriverArguments, PRETTY_WIDTH,
};
use crate::drivers::{
    postgres_shared::{connect, pg_quote, CheckCatalog, PgSchema},
    redshift::{RedshiftDriverArguments, RedshiftLocator},
};

/// Copy `source` to `dest`.
///
/// We put the instrumentation here, because putting it on each of the functions
/// we dispatch to doesn't add useful information.
#[instrument(
    level = "debug",
    name = "s3::write_remote_data",
    skip_all,
    fields(source = %source, dest = %dest)
)]
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    source: BoxLocator,
    dest: S3Locator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Convert the source locator into `RedshiftLocator`.
    let source_any = source.as_any();

    if let Some(source) = source_any.downcast_ref::<RedshiftLocator>() {
        write_redshift_remote_data_helper(
            ctx,
            source.to_owned(),
            dest,
            shared_args,
            source_args,
            dest_args,
        )
        .await
    } else if let Some(source) = source_any.downcast_ref::<TrinoLocator>() {
        write_trino_remote_data_helper(
            ctx,
            source.to_owned(),
            dest,
            shared_args,
            source_args,
            dest_args,
        )
        .await
    } else {
        Err(format_err!(
            "not a redshift:// or trino:// locator: {}",
            source
        ))
    }
}

/// Copy `source` to `dest`.
async fn write_redshift_remote_data_helper(
    ctx: Context,
    source: RedshiftLocator,
    dest: S3Locator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    let shared_args = shared_args.verify(S3Locator::features())?;
    let source_args = source_args.verify(RedshiftLocator::features())?;
    let dest_args = dest_args.verify(S3Locator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let from_args = source_args
        .driver_args()
        .deserialize::<RedshiftDriverArguments>()?;
    let if_exists = dest_args.if_exists().to_owned();

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(dest.as_url().to_owned(), if_exists).await?;

    // Convert our schema to a native PostgreSQL schema.
    let table_name = source.table_name();
    let pg_schema = PgSchema::from_pg_catalog_or_default(
        // Always check the catalog, because `if_exists` is for our S3
        // destination, not for Redshift source.
        &ctx,
        CheckCatalog::Yes,
        source.url(),
        table_name,
        schema,
    )
    .await?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_schema.write_export_select_sql(&mut sql_bytes, &source_args)?;
    let select_sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!("export SQL: {}", select_sql);

    // Export as CSV.
    let client = connect(&ctx, source.url()).await?;
    let unload_sql = format!(
        "{partner}UNLOAD ({source}) TO {dest}\n{credentials}HEADER FORMAT CSV",
        partner = from_args.partner_sql()?,
        source = pg_quote(&select_sql),
        dest = pg_quote(dest.as_url().as_str()),
        credentials = from_args.credentials_sql()?,
    );
    let unload_stmt = client.prepare(&unload_sql).await?;
    client.execute(&unload_stmt, &[]).await.with_context(|| {
        format!("error copying {} to {}", table_name.quoted(), dest)
    })?;
    Ok(vec![dest.boxed()])
}

/// Copy `source` to `dest`.
async fn write_trino_remote_data_helper(
    _ctx: Context,
    source: TrinoLocator,
    dest: S3Locator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    let shared_args = shared_args.verify(S3Locator::features())?;
    let source_args = source_args.verify(TrinoLocator::features())?;
    let dest_args = dest_args.verify(S3Locator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let _from_args = source_args
        .driver_args()
        .deserialize::<TrinoDriverArguments>()?;
    let if_exists = dest_args.if_exists().to_owned();

    // Make sure we can actually export this data to S3 correctly. dbcrossbar
    // is supposed to preserve case, but Trino can't. It's better to error
    // out rather than export incorrect column names, because the user can
    // work around this using a `--schema=` argument with only lowercase
    // column names.
    for column in &schema.table.columns {
        let name = &column.name;
        if &name.to_lowercase() != name {
            return Err(format_err!(
                "Trino can only export lowercase columns to S3: {}",
                name
            ));
        }
    }

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(dest.as_url().to_owned(), if_exists).await?;

    // Figure out what our source _should_ look like, according to `schema`.
    //
    // TODO: We will eventually add code to "reconcile" this with the actual
    // source table, in order to handle weird corner cases. This happens in
    // almost every major dbcrossbar `Locator` implementation. But not yet.
    let create_ideal_table =
        TrinoCreateTable::from_schema_and_name(schema, &source.table_name()?)?;
    let client = source.client()?;
    let connector_type = source.connector_type(&client).await?;

    // We need to create a temporary Trino table "wrapping" the S3 location.
    // Figure out what it should look like.
    let create_s3_wrapper_table =
        create_ideal_table.hive_csv_wrapper_table(dest.as_url())?;
    let sql = format!(
        "{}",
        create_s3_wrapper_table
            .create_wrapper_table_doc(
                &connector_type,
                &create_ideal_table,
                &source_args
            )?
            .pretty(PRETTY_WIDTH)
    );
    debug!(%sql, "export SQL");
    client.run_statement(&sql).await?;

    // TODO: Drop our wrapper table from Trino without deleting the data.

    Ok(vec![dest.boxed()])
}
