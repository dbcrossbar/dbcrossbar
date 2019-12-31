//! Implementation of `GsLocator::write_remote_data`.

use super::{prepare_as_destination_helper, S3Locator};
use crate::common::*;
use crate::drivers::{
    postgres::connect,
    postgres_shared::{pg_quote, CheckCatalog, PgCreateTable},
    redshift::{credentials_sql, RedshiftLocator},
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    source: BoxLocator,
    dest: S3Locator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Convert the source locator into `RedshiftLocator`.
    let source = source
        .as_any()
        .downcast_ref::<RedshiftLocator>()
        .ok_or_else(|| format_err!("not a redshift:// locator: {}", source))?;

    let shared_args = shared_args.verify(S3Locator::features())?;
    let source_args = source_args.verify(RedshiftLocator::features())?;
    let dest_args = dest_args.verify(S3Locator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let from_args = source_args.driver_args();
    let if_exists = dest_args.if_exists().to_owned();

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(ctx.clone(), dest.as_url().to_owned(), if_exists)
        .await?;

    // Convert our schema to a native PostgreSQL schema.
    let table_name = source.table_name();
    let pg_create_table = PgCreateTable::from_pg_catalog_or_default(
        // Always check the catalog, because `if_exists` is for our S3
        // destination, not for Redshift source.
        CheckCatalog::Yes,
        source.url(),
        table_name,
        schema,
    )
    .await?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_create_table.write_export_select_sql(&mut sql_bytes, &source_args)?;
    let select_sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", select_sql);

    // Export as CSV.
    let mut client = connect(ctx.clone(), source.url().to_owned()).await?;
    let unload_sql = format!(
        "UNLOAD ({source}) TO {dest}\n{credentials}HEADER FORMAT CSV",
        source = pg_quote(&select_sql),
        dest = pg_quote(dest.as_url().as_str()),
        credentials = credentials_sql(from_args)?,
    );
    let unload_stmt = client.prepare(&unload_sql).compat().await?;
    client
        .execute(&unload_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error copying {} to {}", table_name, dest))?;
    Ok(vec![dest.boxed()])
}
