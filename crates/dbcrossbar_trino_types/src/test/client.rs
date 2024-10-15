//! A basic Trino REST API client based on `reqwest`.
//!
//! We do not attempt to support HTTPS or anything fancy. This is intended to be
//! good enough to run tests locally. We could use
//! [`prusto`](https://github.com/nooberfsh/prusto) but doesn't support JSON,
//! timestamps with timezones, or VARBINARY. And it gives rather opaque errors.
//!
//! See the [Trino REST Client
//! Protocol](https://trino.io/docs/current/develop/client-protocol.html) for
//! more details.

// The old `prusto` code for connecting without a password looked like this:
//
// ```rust
// prusto::ClientBuilder::new("admin", "localhost")
//     .port(8080)
//     .catalog(catalog)
//     .schema(schema)
//     .build()
//     .expect("could not connect to Trino")
// ```

use core::fmt;
use std::error;

use serde::Deserialize;
use serde_json::Value;

/// An error returned by our Trino client.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// An error returned by the Trino server.
    QueryError(QueryError),
    /// An error returned by the HTTP client.
    ReqwestError(reqwest::Error),
    /// Expected a single row with a single column, but got something else.
    WrongResultSize {
        /// The number of rows returned.
        rows: usize,
        /// The number of columns returned.
        columns: usize,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::QueryError(e) => write!(f, "Trino query error: {}", e),
            Self::ReqwestError(e) => write!(f, "HTTP error: {}", e),
            Self::WrongResultSize { rows, columns } => {
                write!(
                    f,
                    "expected 1 row with 1 column, got {} rows and {} columns",
                    rows, columns
                )
            }
        }
    }
}

impl error::Error for ClientError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::QueryError(e) => Some(e),
            Self::ReqwestError(e) => Some(e),
            Self::WrongResultSize { .. } => None,
        }
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(e: reqwest::Error) -> Self {
        Self::ReqwestError(e)
    }
}

/// An error returned from a Trino query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct QueryError {
    pub message: String,
    pub error_code: i32, // Not 100% sure about the size.
    pub error_name: String,
    pub error_type: String,

    // Any other fields we don't handle yet.
    _other: serde_json::Map<String, Value>,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.error_name, self.error_code, self.message
        )
    }
}

impl error::Error for QueryError {}

/// The result of a Trino query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
#[allow(dead_code)]
pub struct TrinoResponse {
    pub id: String,
    pub query_error: Option<QueryError>,
    pub next_uri: Option<String>,
    // TODO: Make a nice type for this before exposing it.
    #[allow(dead_code)]
    columns: Option<Vec<Value>>,
    pub data: Option<Vec<Vec<Value>>>,
    pub update_type: Option<String>,

    // Any other fields we don't handle yet.
    #[serde(flatten)]
    _other: serde_json::Map<String, Value>,
}

/// A basic Trino REST API client, useful for testing.
pub struct Client {
    user: String,
    host: String,
    port: u16,
    client: reqwest::Client,
}

impl Client {
    /// Create a new client.
    pub fn new(user: &str, host: &str, port: u16) -> Self {
        Self {
            user: user.to_string(),
            host: host.to_string(),
            port,
            client: reqwest::Client::new(),
        }
    }

    /// Build a statement URL.
    fn statement_url(&self) -> String {
        format!("http://{}:{}/v1/statement", self.host, self.port)
    }

    /// Execute a query.
    async fn execute(&self, query: &str) -> Result<TrinoResponse, ClientError> {
        let url = self.statement_url();
        loop {
            let response = self
                .client
                .post(&url)
                .basic_auth(&self.user, None::<&str>)
                .body(query.to_owned())
                .send()
                .await?;
            if response.status().is_success() {
                return Ok(response.json().await?);
            } else if [429, 502, 503, 504]
                .iter()
                .any(|&s| response.status().as_u16() == s)
            {
                // Wait briefly and try again.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            } else {
                return Err(ClientError::ReqwestError(
                    response.error_for_status().unwrap_err(),
                ));
            }
        }
    }

    /// Continue a query. You should only call this if `next_uri` is present.
    async fn continue_query(
        &self,
        query_response: &TrinoResponse,
    ) -> Result<TrinoResponse, ClientError> {
        let url = query_response.next_uri.as_ref().expect("missing next_uri");
        Ok(self
            .client
            .get(url)
            .basic_auth(&self.user, None::<&str>)
            .send()
            .await?
            .json()
            .await?)
    }

    /// Run a statement, ignoring any results.
    pub async fn run_statement(&self, query: &str) -> Result<(), ClientError> {
        let mut response = self.execute(query).await?;
        loop {
            if let Some(error) = response.query_error {
                return Err(ClientError::QueryError(error));
            } else if response.next_uri.is_some() {
                // TODO: Wait?
                response = self.continue_query(&response).await?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Collect all the results of a query.
    pub async fn get_all(&self, query: &str) -> Result<Vec<Vec<Value>>, ClientError> {
        let mut response = self.execute(query).await?;
        let mut results = Vec::new();
        loop {
            if let Some(data) = &response.data {
                results.extend(data.iter().cloned());
            }
            if let Some(error) = response.query_error {
                return Err(ClientError::QueryError(error));
            } else if response.next_uri.is_some() {
                // TODO: Wait?
                response = self.continue_query(&response).await?;
            } else {
                break;
            }
        }
        Ok(results)
    }

    /// Get a the first column of the first row of a query.
    pub async fn get_one(&self, query: &str) -> Result<Value, ClientError> {
        let mut response = self.get_all(query).await?;
        if response.len() != 1 || response[0].len() != 1 {
            return Err(ClientError::WrongResultSize {
                rows: response.len(),
                columns: response[0].len(),
            });
        }
        Ok(response.remove(0).remove(0))
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new("admin", "localhost", 8080)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute() {
        let client = Client::default();
        let rows = client
            .get_all("SELECT JSON_PARSE('[1, 2, 3]')")
            .await
            .unwrap();
        eprintln!("rows: {:?}", rows);
    }
}
