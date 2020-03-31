//! Running queries against BigQuery.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json;
use std::convert::TryFrom;

use super::{
    super::client::{percent_encode, Client},
    jobs::{
        run_job, CreateDisposition, Job, JobConfigurationQuery, TableReference,
        WriteDisposition,
    },
    TableSchema,
};
use crate::common::*;
use crate::drivers::bigquery_shared::{BqColumn, TableName};

/// Execute an SQL statement.
pub(crate) async fn execute_sql(
    ctx: &Context,
    project: &str,
    sql: &str,
) -> Result<()> {
    trace!(ctx.log(), "executing SQL: {}", sql);
    let config = JobConfigurationQuery::new(sql);
    let client = Client::new(ctx).await?;
    run_job(ctx, &client, project, Job::new_query(config)).await?;
    Ok(())
}

/// Run an SQL query and save the results to a table.
pub(crate) async fn query_to_table(
    ctx: &Context,
    project: &str,
    sql: &str,
    dest_table: &TableName,
    if_exists: &IfExists,
) -> Result<()> {
    trace!(ctx.log(), "writing query to {}: {}", dest_table, sql);

    // Configure our query.
    let mut config = JobConfigurationQuery::new(sql);
    config.destination_table = Some(TableReference::from(dest_table));
    config.create_disposition = Some(CreateDisposition::CreateIfNeeded);
    config.write_disposition = Some(WriteDisposition::try_from(if_exists)?);

    // Run our query.
    let client = Client::new(ctx).await?;
    run_job(ctx, &client, project, Job::new_query(config)).await?;
    Ok(())
}

/// Parameters used to look up information about a query.
///
/// See the [documentation][docs] for more details.
///
/// [docs]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryResultsQuery {
    /// Geographic location. Mandatory outside of US and Europe.
    location: String,
}

/// Results of a query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResults {
    /// The schema of our query results.
    schema: TableSchema,

    /// Rows returned from the query.
    rows: Vec<Row>,

    /// Has this query completed?
    job_complete: bool,
}

impl QueryResults {
    fn to_json_objects(&self, ctx: &Context) -> Result<Vec<serde_json::Value>> {
        let objects = self
            .rows
            .iter()
            .map(|row| row.to_json_object(ctx, &self.schema.fields))
            .collect::<Result<Vec<serde_json::Value>>>()?;
        trace!(
            ctx.log(),
            "rows as objects: {}",
            serde_json::to_string(&objects).expect("should be able to serialize rows"),
        );
        Ok(objects)
    }
}

/// A row returned in `QueryResults`.
#[derive(Debug, Deserialize)]
struct Row {
    /// The fields in this row.
    #[serde(rename = "f")]
    fields: Vec<Value>,
}

impl Row {
    /// Convert this row into a JSON object using names and other metadata from
    /// columns. We don't try to decode anything that `serde_json` can later
    /// decode for us.
    fn to_json_object(
        &self,
        ctx: &Context,
        columns: &[BqColumn],
    ) -> Result<serde_json::Value> {
        // Check that we have the right number of columns.
        if columns.len() != self.fields.len() {
            return Err(format_err!(
                "schema contained {} columns, but row contains {}",
                columns.len(),
                self.fields.len(),
            ));
        }
        let mut obj = serde_json::Map::with_capacity(columns.len());
        for (col, value) in columns.iter().zip(self.fields.iter()) {
            obj.insert(col.name.to_portable_name(), value.to_json_value(ctx)?);
        }
        Ok(serde_json::Value::Object(obj))
    }
}

/// A value returned in query results.
#[derive(Debug, Deserialize)]
struct Value {
    /// The actual value. This is normally represented as a string.
    ///
    /// This might also be a nested `Row` object, but we don't handle that yet.
    #[serde(rename = "v")]
    value: serde_json::Value,
}

impl Value {
    /// Convert this value into a JSON value.
    fn to_json_value(&self, _ctx: &Context) -> Result<serde_json::Value> {
        Ok(self.value.clone())
    }
}

/// Run a query that should return a small number of records, and return them as
/// a JSON string.
async fn query_all_json(
    ctx: &Context,
    project: &str,
    sql: &str,
) -> Result<Vec<serde_json::Value>> {
    trace!(ctx.log(), "executing SQL: {}", sql);

    // Run our query.
    let config = JobConfigurationQuery::new(sql);
    let client = Client::new(ctx).await?;
    let job = run_job(ctx, &client, project, Job::new_query(config)).await?;

    // Look up our query results.
    let reference = job.reference()?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries/{}",
        percent_encode(project),
        percent_encode(&reference.job_id),
    );
    let query = QueryResultsQuery {
        location: reference.location.clone(),
    };
    let results = client.get::<QueryResults, _, _>(ctx, &url, query).await?;
    if results.job_complete {
        results.to_json_objects(ctx)
    } else {
        Err(format_err!(
            "expected query to have finished, but it hasn't",
        ))
    }
}

/// Run a query that should return a small number of records, and deserialize them.
pub(crate) async fn query_all<T>(
    ctx: &Context,
    project: &str,
    sql: &str,
) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let output = query_all_json(ctx, project, sql).await?;
    let rows = output
        .into_iter()
        .map(|row| serde_json::from_value::<T>(row))
        .collect::<Result<Vec<T>, _>>()
        .context("could not parse count output")?;
    Ok(rows)
}

/// Run a query that should return exactly one record, and deserialize it.
pub(crate) async fn query_one<T>(ctx: &Context, project: &str, sql: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut rows = query_all(ctx, project, sql).await?;
    if rows.len() == 1 {
        Ok(rows.remove(0))
    } else {
        Err(format_err!("expected 1 row, found {}", rows.len()))
    }
}
