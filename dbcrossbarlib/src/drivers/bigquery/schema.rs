//! Implementation of `schema`.

use std::process::{Command, Stdio};
use tokio_process::CommandExt;

use super::BigQueryLocator;
use crate::common::*;
use crate::drivers::bigquery_shared::{BqColumn, BqTable};
use crate::schema::Table;

/// Implementation of `schema`, but as a real `async` function.
pub(crate) async fn schema_helper(
    ctx: Context,
    source: BigQueryLocator,
) -> Result<Option<Table>> {
    let output = Command::new("bq")
        .args(&[
            "show",
            "--headless",
            "--schema",
            "--format=json",
            &source.table_name.to_string(),
        ])
        .stderr(Stdio::inherit())
        .output_async()
        .compat()
        .await
        .context("error running `bq show --schema`")?;
    if !output.status.success() {
        return Err(format_err!(
            "`bq show --schema` failed with {}",
            output.status,
        ));
    }
    debug!(
        ctx.log(),
        "BigQuery schema: {}",
        String::from_utf8_lossy(&output.stdout).trim(),
    );
    let columns: Vec<BqColumn> = serde_json::from_slice(&output.stdout)
        .context("error parsing BigQuery schema")?;
    let table = BqTable {
        name: source.table_name.clone(),
        columns,
    };
    Ok(Some(table.to_table()?))
}
