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

pub(crate) use extract::*;
pub(crate) use jobs::Labels;
pub(crate) use load::*;
pub(crate) use queries::*;
pub(crate) use schema::*;

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

/// The schema of our query results.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableSchema {
    /// The fields in the table.
    fields: Vec<BqColumn>,
}

/// Drop a table from BigQuery.
#[instrument(level = "trace", skip(labels))]
pub(crate) async fn drop_table(table_name: &TableName, labels: &Labels) -> Result<()> {
    // Delete temp table.
    debug!("deleting table: {}", table_name);
    let sql = format!("DROP TABLE {};\n", table_name.dotted_and_quoted());
    execute_sql(table_name.project(), &sql, labels).await
}
