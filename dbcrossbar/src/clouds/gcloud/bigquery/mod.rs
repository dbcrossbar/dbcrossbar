//! Interfaces to BigQuery.

use serde::{Deserialize, Serialize};
use std::{error, fmt};

use crate::common::*;
use crate::drivers::bigquery_shared::{BqColumn, TableName};

mod extract;
pub(crate) mod jobs;
mod load;
mod queries;
mod schema;
mod tables;

pub(crate) use extract::*;
pub(crate) use jobs::Labels;
pub(crate) use load::*;
pub(crate) use queries::*;
pub(crate) use schema::*;
pub(crate) use tables::*;

use super::Client;

/// A BigQuery error.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BigQueryError {
    /// The reason for this error.
    reason: String,

    /// If present, where this error occurred.
    location: Option<String>,

    /// Internal Google information about this error.
    debug_info: Option<String>,

    /// A human-readable description of this error.
    message: String,
}

impl BigQueryError {
    /// Is this an "access denied" error?
    pub(crate) fn is_access_denied(&self) -> bool {
        self.reason.starts_with("accessDenied")
    }
}

impl fmt::Display for BigQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.reason)?;
        if let Some(location) = &self.location {
            write!(f, " at {}", location)?;
        }
        write!(f, ": {}", self.message)
    }
}

impl error::Error for BigQueryError {}

/// Given an `Error`, look to see if it's a wrapper around `BigQueryError`, and
/// if so, return the original error. Otherwise return `None`.
pub(crate) fn original_bigquery_error(err: &Error) -> Option<&BigQueryError> {
    // Walk the chain of all errors, ending with the original root cause.
    for cause in err.chain() {
        // If this error is a `BigQueryError`, return it.
        if let Some(bigquery_error) = cause.downcast_ref::<BigQueryError>() {
            return Some(bigquery_error);
        }
    }
    None
}

/// The schema of our query results.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableSchema {
    /// The fields in the table.
    fields: Vec<BqColumn>,
}

/// Drop a table from BigQuery.
#[instrument(level = "trace", skip(client, labels))]
pub(crate) async fn drop_table(
    client: &Client,
    table_name: &TableName,
    labels: &Labels,
) -> Result<()> {
    // Delete temp table.
    debug!("deleting table: {}", table_name);
    let sql = format!("DROP TABLE {};\n", table_name.dotted_and_quoted());
    execute_sql(client, table_name.project(), &sql, labels).await
}
