//! Support for looking up BigQuery schemas.

use serde::{Deserialize, Serialize};

use super::super::{percent_encode, Client, NoQuery};
use super::jobs::TableReference;
use crate::clouds::gcloud::ClientError;
use crate::common::*;
use crate::drivers::bigquery_shared::TableName;

/// Delete the specified table.
#[instrument(level = "trace", skip(client))]
pub(crate) async fn delete_table(
    client: &Client,
    name: &TableName,
    not_found_ok: bool,
) -> Result<()> {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
        percent_encode(name.project()),
        percent_encode(name.dataset()),
        percent_encode(name.table()),
    );

    // Delete the specified table.
    match client.delete(&url, NoQuery).await {
        Ok(_) => Ok(()),
        Err(ClientError::NotFound { .. }) if not_found_ok => Ok(()),
        Err(ClientError::Other(err)) => Err(err),
        Err(err) => Err(err.into()),
    }
}

/// Information needed to create a view.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TableViewNew<'a> {
    table_reference: TableReference,
    view: ViewDefintion<'a>,
}

/// View details for BigQuery.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ViewDefintion<'a> {
    query: &'a str,
    use_legacy_sql: bool,
}

/// Our response type. We don't care about what's in here.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Table {}

/// Create a view using the specied table name and SQL.
#[instrument(level = "trace", skip(client))]
pub(crate) async fn create_view(
    client: &Client,
    name: &TableName,
    view_sql: &str,
) -> Result<()> {
    // Build our URL.
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables",
        percent_encode(name.project()),
        percent_encode(name.dataset()),
    );

    // Build our request body.
    let table = TableViewNew {
        table_reference: TableReference::from(name),
        view: ViewDefintion {
            query: view_sql,
            use_legacy_sql: false,
        },
    };

    // Create our view.
    client.post::<Table, _, _, _>(&url, NoQuery, table).await?;
    Ok(())
}
