//! Implementation of `count`, but as a real `async` function.

use serde::Deserialize;

use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{BqTable, GCloudDriverArguments, Usage},
};

/// Implementation of `count`, but as a real `async` function.
#[instrument(
    level = "trace",
    name = "bigquery::count",
    skip(shared_args, source_args)
)]
pub(crate) async fn count_helper(
    locator: BigQueryLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<usize> {
    let shared_args = shared_args.verify(BigQueryLocator::features())?;
    let source_args = source_args.verify(BigQueryLocator::features())?;

    // Get our billing labels.
    let job_labels = source_args
        .driver_args()
        .deserialize::<GCloudDriverArguments>()
        .context("error parsing --from-args")?
        .job_labels
        .to_owned();

    let job_project_id = source_args
        .driver_args()
        .deserialize::<GCloudDriverArguments>()
        .context("error parsing --from-args")?
        .job_project_id
        .to_owned();

    // In case the user wants to run the job in a different project for billing purposes
    let final_job_project_id =
        job_project_id.unwrap_or_else(|| locator.project().to_owned());

    // Look up the arguments we need.
    let schema = shared_args.schema();

    // Construct a `BqTable` describing our source table.
    let table_name = locator.as_table_name().to_owned();
    let table = BqTable::for_table_name_and_columns(
        schema,
        table_name,
        &schema.table.columns,
        Usage::FinalTable,
    )?;

    // Generate our count SQL.
    let mut count_sql_data = vec![];
    table.write_count_sql(&source_args, &mut count_sql_data)?;
    let count_sql = String::from_utf8(count_sql_data).expect("should always be UTF-8");
    debug!("count SQL: {}", count_sql);

    // Run our query.
    #[derive(Deserialize)]
    struct CountRow {
        count: String,
    }
    let count_str = bigquery::query_one::<CountRow>(
        &final_job_project_id,
        &count_sql,
        &job_labels,
    )
    .await?
    .count;
    count_str
        .parse::<usize>()
        .context("could not parse count output")
}
