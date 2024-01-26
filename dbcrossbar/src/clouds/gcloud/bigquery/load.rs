//! Load data from Google Cloud Storage into BigQuery.

use super::{
    super::Client,
    jobs::{
        run_job, CreateDisposition, Job, JobConfigurationLoad, Labels, TableReference,
        WriteDisposition,
    },
    TableSchema,
};
use crate::common::*;
use crate::drivers::bigquery_shared::BqTable;
use std::convert::TryFrom;

/// Load data from `gs_url` into `dest_table`.
#[instrument(level = "trace", skip(client, dest_table, labels), fields(dest_name = ?dest_table.name))]
pub(crate) async fn load(
    client: &Client,
    gs_url: &Url,
    dest_table: &BqTable,
    if_exists: &IfExists,
    labels: &Labels,
    job_project_id: &str,
) -> Result<()> {
    trace!("loading {} into {}", gs_url, dest_table.name);

    // Configure our job.
    let config = JobConfigurationLoad {
        source_uris: vec![gs_url.to_string()],
        schema: Some(TableSchema {
            fields: dest_table.columns.clone(),
        }),
        destination_table: TableReference::from(&dest_table.name),
        create_disposition: Some(CreateDisposition::CreateIfNeeded),
        write_disposition: Some(WriteDisposition::try_from(if_exists)?),
        skip_leading_rows: Some(1),
        allow_quoted_newlines: Some(true),
    };

    // Run our job.
    run_job(
        client,
        job_project_id,
        Job::new_load(config, labels.to_owned()),
    )
    .await?;
    Ok(())
}
