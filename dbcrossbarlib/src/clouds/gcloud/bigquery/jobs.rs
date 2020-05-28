//! BigQuery batch jobs.
//!
//! These use a number of closely-related types.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryFrom};
use tokio::time::{delay_for, Duration};

use super::{
    super::{Client, NoQuery},
    BigQueryError, TableSchema,
};
use crate::common::*;
use crate::drivers::bigquery_shared::TableName;

/// Key/value pairs. See [JobConfiguration][config].
///
/// [config]: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
pub(crate) type Labels = HashMap<String, String>;

/// A BigQuery job.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Job {
    /// Output only. The ID of this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,

    /// Output only. A link which can be used to access this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) self_link: Option<String>,

    /// The configuration for this job.
    pub(crate) configuration: JobConfiguration,

    /// Output only. A reference to this job.
    pub(crate) job_reference: Option<JobReference>,

    /// Output only. The status of this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<JobStatus>,
}

impl Job {
    /// Create a new job using the specified configuration details.
    fn from_config(configuration: JobConfiguration) -> Self {
        Job {
            id: None,
            self_link: None,
            configuration,
            job_reference: None,
            status: None,
        }
    }

    /// Create a new query job.
    pub(crate) fn new_query(
        query_config: JobConfigurationQuery,
        labels: Labels,
    ) -> Self {
        let mut config = JobConfiguration::default();
        config.query = Some(query_config);
        config.labels = labels;
        Self::from_config(config)
    }

    /// Create a new load job.
    pub(crate) fn new_load(load_config: JobConfigurationLoad, labels: Labels) -> Self {
        let mut config = JobConfiguration::default();
        config.load = Some(load_config);
        config.labels = labels;
        Self::from_config(config)
    }

    /// Create a new load job.
    pub(crate) fn new_extract(
        extract_config: JobConfigurationExtract,
        labels: Labels,
    ) -> Self {
        let mut config = JobConfiguration::default();
        config.extract = Some(extract_config);
        config.labels = labels;
        Self::from_config(config)
    }

    /// Get the job ID, with the project and region prefixes stripped.
    pub(crate) fn reference(&self) -> Result<&JobReference> {
        Ok(self
            .job_reference
            .as_ref()
            .ok_or_else(|| format_err!("newly created job has no jobReference"))?)
    }

    /// Get a URL which can be used for this job.
    pub(crate) fn url(&self) -> Result<Url> {
        Ok(self
            .self_link
            .as_ref()
            .ok_or_else(|| format_err!("newly created job has no selfLink"))?
            .parse::<Url>()
            .context("BigQuery returned invalid selfLink")?)
    }
}

/// A compound job ID containing project and region information.
#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobReference {
    /// The project containing this job.
    pub(crate) project_id: String,

    /// The bare ID, suitable for use in URL.
    pub(crate) job_id: String,

    /// The location of this job.
    pub(crate) location: String,
}

/// Configuration for a job.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobConfiguration {
    /// Configuration information query jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query: Option<JobConfigurationQuery>,

    /// Configuration information load jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) load: Option<JobConfigurationLoad>,

    /// Configuration information extract jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) extract: Option<JobConfigurationExtract>,

    /// Don't run the job, just calculate what we would need to do.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dry_run: Option<bool>,

    /// Labels to attach to jobs.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub(crate) labels: Labels,
}

/// Configuration for query jobs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobConfigurationQuery {
    /// A table in which to save our query results. If this is `None`, a
    /// temporary table will be created.
    pub(crate) destination_table: Option<TableReference>,

    /// Should we create a table if it doesn't exist?
    pub(crate) create_disposition: Option<CreateDisposition>,

    /// What should we do with any existing data?
    pub(crate) write_disposition: Option<WriteDisposition>,

    /// The SQL query to run.
    pub(crate) query: String,

    /// Should be use "legacy SQL" mode? Hint: No, we don't. Defaults to true.
    pub(crate) use_legacy_sql: Option<bool>,
}

impl JobConfigurationQuery {
    /// Create a new query using standard SQL.
    pub(crate) fn new<S: Into<String>>(query: S) -> Self {
        JobConfigurationQuery {
            destination_table: None,
            create_disposition: None,
            write_disposition: None,
            query: query.into(),
            use_legacy_sql: Some(false),
        }
    }
}

/// Configuration for data load jobs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobConfigurationLoad {
    pub(crate) source_uris: Vec<String>,
    pub(crate) schema: Option<TableSchema>,
    pub(crate) destination_table: TableReference,
    pub(crate) create_disposition: Option<CreateDisposition>,
    pub(crate) write_disposition: Option<WriteDisposition>,
    pub(crate) skip_leading_rows: Option<i32>,
    pub(crate) allow_quoted_newlines: Option<bool>,
}

/// Configuration for data extraction jobs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobConfigurationExtract {
    /// Where to write our data.
    pub(crate) destination_uris: Vec<String>,

    /// The location of our data.
    pub(crate) source_table: TableReference,
}

/// The status of a job.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobStatus {
    /// The state of this job.
    state: JobState,

    /// If present, indicates that the job failed.
    error_result: Option<BigQueryError>,

    /// Errors encountered while running the job. These do not necessarily
    /// indicate that the job has finished or was unsuccessful.
    #[serde(default)]
    errors: Vec<BigQueryError>,
}

impl JobStatus {
    /// Check to see if we've encountered an error.
    fn check_for_error(&self) -> Result<(), BigQueryError> {
        if let Some(err) = &self.error_result {
            Err(err.clone())
        } else {
            Ok(())
        }
    }
}

/// The state of a job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Serialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum JobState {
    /// This job is waiting to run.
    Pending,
    /// This job is currently running.
    Running,
    /// This job has finished.
    Done,
}

/// The name of a table.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableReference {
    pub(crate) project_id: String,
    pub(crate) dataset_id: String,
    pub(crate) table_id: String,
}

impl From<&TableName> for TableReference {
    fn from(name: &TableName) -> Self {
        Self {
            project_id: name.project().to_owned(),
            dataset_id: name.dataset().to_owned(),
            table_id: name.table().to_owned(),
        }
    }
}

/// Should this job create new tables?
#[derive(Clone, Copy, Debug, Deserialize, Eq, Serialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)]
pub(crate) enum CreateDisposition {
    CreateIfNeeded,
    CreateNever,
}

/// Should this job create new tables?
#[derive(Clone, Copy, Debug, Deserialize, Eq, Serialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)]
pub(crate) enum WriteDisposition {
    WriteTruncate,
    WriteAppend,
    WriteEmpty,
}

impl TryFrom<&IfExists> for WriteDisposition {
    type Error = Error;

    fn try_from(if_exists: &IfExists) -> Result<Self> {
        match if_exists {
            IfExists::Append => Ok(WriteDisposition::WriteAppend),
            IfExists::Error => Ok(WriteDisposition::WriteEmpty),
            IfExists::Overwrite => Ok(WriteDisposition::WriteTruncate),
            // If you want to upsert, you'll need to use `execute_sql`.
            IfExists::Upsert(_) => {
                Err(format_err!("cannot upsert to using writeDisposition"))
            }
        }
    }
}

/// Run a BigQuery job.
pub(crate) async fn run_job(
    ctx: &Context,
    client: &Client,
    project_id: &str,
    mut job: Job,
) -> Result<Job> {
    trace!(
        ctx.log(),
        "starting BigQuery job on {} {:?}",
        project_id,
        job,
    );

    // Create our job.
    let insert_url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/jobs",
        project_id,
    );
    job = client
        .post::<Job, _, _, _>(ctx, &insert_url, NoQuery, job)
        .await?;

    // Get the URL for polling the job.
    let job_url = job.url()?;

    // Check our current job status.
    let mut sleep_duration = Duration::from_secs(2);
    loop {
        // Check to see if the job is done.
        let state = job.status.as_ref().map(|s| s.state);
        if state == Some(JobState::Done) {
            break;
        }

        // Wait for a while.
        delay_for(sleep_duration).await;
        if sleep_duration < Duration::from_secs(16) {
            sleep_duration *= 2;
        }

        // Update our job.
        job = client
            .get::<Job, _, _>(ctx, job_url.as_str(), NoQuery)
            .await?;
    }

    // Return either an error or a finished job.
    job.status
        .as_ref()
        .expect("should have already checked for status")
        .check_for_error()?;
    Ok(job)
}
