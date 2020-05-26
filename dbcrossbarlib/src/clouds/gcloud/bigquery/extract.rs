//! Extract data from BigQuery into Google Cloud Storage.

use super::{
    super::Client,
    jobs::{run_job, Job, JobConfigurationExtract, Labels, TableReference},
};

use crate::common::*;
use crate::drivers::bigquery_shared::TableName;

/// Extract a table from BigQuery to Google Cloud Storage.
pub(crate) async fn extract(
    ctx: &Context,
    source_table: &TableName,
    dest_gs_url: &Url,
    labels: &Labels,
) -> Result<()> {
    trace!(ctx.log(), "extract {} into {}", source_table, dest_gs_url);

    // Configure our job.
    let config = JobConfigurationExtract {
        destination_uris: vec![format!("{}/*.csv", dest_gs_url)],
        source_table: TableReference::from(source_table),
    };

    // Run our job.
    let client = Client::new(ctx).await?;
    run_job(
        ctx,
        &client,
        source_table.project(),
        Job::new_extract(config, labels.to_owned()),
    )
    .await?;
    Ok(())
}
