//! Implementation of `GsLocator::write_remote_data`.

use super::{prepare_as_destination_helper, S3Locator};
use crate::common::*;
use crate::drivers::{
    postgres::connect,
    postgres_shared::{pg_quote, PgCreateTable},
    redshift::{credentials_sql, RedshiftLocator},
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    schema: Table,
    source: BoxLocator,
    dest: S3Locator,
    query: Query,
    from_args: DriverArgs,
    to_args: DriverArgs,
    if_exists: IfExists,
) -> Result<()> {
    to_args.fail_if_present()?;

    // Convert the source locator into `RedshiftLocator`.
    let source = source
        .as_any()
        .downcast_ref::<RedshiftLocator>()
        .ok_or_else(|| format_err!("not a redshift:// locator: {}", source))?;

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(ctx.clone(), dest.as_url().to_owned(), if_exists)
        .await?;

    // Convert our schema to a native PostgreSQL schema.
    let table_name = source.table_name();
    let pg_create_table =
        PgCreateTable::from_name_and_columns(table_name.to_owned(), &schema.columns)?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_create_table.write_export_select_sql(&mut sql_bytes, &query)?;
    let select_sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", select_sql);

    // Export as CSV.
    let mut client = connect(ctx.clone(), source.url().to_owned()).await?;
    let unload_sql = format!(
        "UNLOAD ({source}) TO {dest}\n{credentials}HEADER FORMAT CSV",
        source = pg_quote(&select_sql),
        dest = pg_quote(dest.as_url().as_str()),
        credentials = credentials_sql(&from_args)?,
    );
    let unload_stmt = client.prepare(&unload_sql).compat().await?;
    client
        .execute(&unload_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error copying {} to {}", table_name, dest))?;
    Ok(())
}
