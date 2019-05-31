//! Implementation of `GsLocator::write_remote_data`.

use std::process::{Command, Stdio};
use tokio::io;
use tokio_process::CommandExt;

use super::{prepare_as_destination_helper, GsLocator};
use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{if_exists_to_bq_load_arg, BqTable, Usage},
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    schema: Table,
    source: BoxLocator,
    dest: GsLocator,
    temporary_storage: TemporaryStorage,
    if_exists: IfExists,
) -> Result<()> {
    // Convert the source locator into the underlying `TableName. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `GsLocator` type, and Rust doesn't make that especially easy.
    let source_table_name = source
        .as_any()
        .downcast_ref::<BigQueryLocator>()
        .ok_or_else(|| format_err!("not a gs:// locator: {}", source))?
        .as_table_name()
        .to_owned();

    // Construct a `BqTable` describing our source table.
    let source_table = BqTable::for_table_name_and_columns(
        source_table_name,
        &schema.columns,
        Usage::FinalTable,
    )?;

    // We need to build a temporary export table.
    let temp_table_name = source_table
        .name()
        .temporary_table_name(&temporary_storage)?;
    let mut export_sql_data = vec![];
    source_table.write_export_sql(&mut export_sql_data)?;
    let export_sql =
        String::from_utf8(export_sql_data).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", export_sql);

    // Run our query.
    debug!(ctx.log(), "running `bq query`");
    let mut query_child = Command::new("bq")
        // We'll pass the query on `stdin`.
        .stdin(Stdio::piped())
        // Run query with no output.
        .args(&[
            "query",
            "--format=none",
            &format!("--destination_table={}", temp_table_name),
            if_exists_to_bq_load_arg(&IfExists::Overwrite)?,
            "--nouse_legacy_sql",
        ])
        .spawn_async()
        .context("error starting `bq query`")?;
    let child_stdin = query_child
        .stdin()
        .take()
        .expect("don't have stdio that we requested");
    await!(io::write_all(child_stdin, export_sql))
        .context("error piping query to `bq load`")?;
    let status = await!(query_child).context("error running `bq query`")?;
    if !status.success() {
        return Err(format_err!("`bq load` failed with {}", status));
    }

    // Delete the existing output, if it exists.
    await!(prepare_as_destination_helper(
        ctx.clone(),
        dest.as_url().to_owned(),
        if_exists
    ))?;

    // Build and run a `bq extract` command.
    debug!(ctx.log(), "running `bq extract`");
    let load_child = Command::new("bq")
        // These arguments can all be represented as UTF-8 `&str`.
        .args(&[
            "extract",
            "--destination_format=CSV",
            &temp_table_name.to_string(),
            &format!("{}/*.csv", dest),
        ])
        .spawn_async()
        .context("error starting `bq load`")?;
    let status = await!(load_child).context("error running `bq load`")?;
    if !status.success() {
        return Err(format_err!("`bq load` failed with {}", status));
    }

    // Delete temp table.
    debug!(ctx.log(), "deleting export temp table: {}", temp_table_name);
    let rm_child = Command::new("bq")
        .args(&["rm", "-f", "-t", &temp_table_name.to_string()])
        .spawn_async()
        .context("error starting `bq rm`")?;
    let status = await!(rm_child).context("error running `bq rm`")?;
    if !status.success() {
        return Err(format_err!("`bq rm` failed with {}", status));
    }

    Ok(())
}
