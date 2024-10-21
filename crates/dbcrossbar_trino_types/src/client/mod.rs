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

use std::fmt;

use reqwest::RequestBuilder;
use serde::Deserialize;
use serde_json::Value;

pub use self::{
    deserialize_value::deserialize_value,
    errors::{ClientError, QueryError},
    wire_types::{Argument, FieldName, NamedType, RawType, TypeSignature},
};
use crate::{TrinoDataType, TrinoIdent};

use super::TrinoValue;

mod deserialize_value;
mod errors;
mod wire_types;

/// The result of a Trino query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
#[allow(dead_code)]
pub struct Response {
    pub id: String,
    pub error: Option<QueryError>,
    /// The docs claim that this exists, too, but I'm not sure what's going with
    /// this.
    pub query_error: Option<QueryError>,
    pub next_uri: Option<String>,
    pub columns: Option<Vec<Column>>,
    pub data: Option<Vec<Vec<Value>>>,
    pub update_type: Option<String>,

    // Any other fields we don't handle yet.
    #[serde(flatten)]
    _other: serde_json::Map<String, Value>,
}

/// A column description in a Trino response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Column {
    pub name: TrinoIdent,
    #[serde(rename = "type")]
    type_string: String,
    pub type_signature: TypeSignature,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.type_string)
    }
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

    async fn request(
        &self,
        mkreq: &dyn Fn() -> RequestBuilder,
    ) -> Result<Response, ClientError> {
        loop {
            let response = mkreq().send().await?;
            if response.status().is_success() {
                let body: Response = response.json().await?;
                //eprintln!("response: {:#?}", body);
                if let Some(error) = body.error {
                    return Err(ClientError::QueryError(error));
                } else if let Some(error) = body.query_error {
                    return Err(ClientError::QueryError(error));
                } else {
                    return Ok(body);
                }
            } else if [429, 502, 503, 504]
                .iter()
                .any(|&s| response.status().as_u16() == s)
            {
                // Wait briefly and try again. 50-100 milliseconds is
                // recommended by the Trino docs, but 50 seems a little
                // aggressively fast.
                //
                // TODO: Time out eventually?
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            } else {
                return Err(ClientError::ReqwestError(
                    response.error_for_status().unwrap_err(),
                ));
            }
        }
    }

    /// Execute a query.
    async fn start_query(&self, query: &str) -> Result<Response, ClientError> {
        let url = self.statement_url();
        self.request(&|| {
            self.client
                .post(&url)
                .basic_auth(&self.user, None::<&str>)
                .body(query.to_owned())
        })
        .await
    }

    /// Continue a query. You should only call this if `next_uri` is present.
    async fn continue_query(
        &self,
        query_response: &Response,
    ) -> Result<Response, ClientError> {
        let url = query_response.next_uri.as_ref().expect("missing next_uri");
        self.request(&|| self.client.get(url).basic_auth(&self.user, None::<&str>))
            .await
    }

    /// Run a statement, ignoring any results.
    pub async fn run_statement(&self, query: &str) -> Result<(), ClientError> {
        let mut response = self.start_query(query).await?;
        loop {
            if response.next_uri.is_some() {
                // TODO: Wait?
                response = self.continue_query(&response).await?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Collect all the results of a query.
    pub async fn get_all(
        &self,
        query: &str,
    ) -> Result<Vec<Vec<TrinoValue>>, ClientError> {
        let mut response = self.start_query(query).await?;
        let mut results = Vec::new();
        let mut col_types: Option<Vec<TrinoDataType>> = None;
        loop {
            if let Some(cols) = &response.columns {
                if col_types.is_none() {
                    col_types = Some(
                        cols.iter()
                            .map(|c| (&c.type_signature).try_into())
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(data) = &response.data {
                if let Some(col_types) = &col_types {
                    for row in data.iter() {
                        let mut deserialized_row = Vec::new();
                        for (ty, val) in col_types.iter().zip(row.iter()) {
                            deserialized_row.push(deserialize_value(ty, val)?);
                        }
                        results.push(deserialized_row);
                    }
                } else {
                    return Err(ClientError::MissingColumnInfo);
                }
            }
            if response.next_uri.is_some() {
                // TODO: Wait?
                response = self.continue_query(&response).await?;
            } else {
                break;
            }
        }
        Ok(results)
    }

    /// Get a the first column of the first row of a query. Raise an error if
    /// there is any other data returned, or if no data is returned.
    pub async fn get_one(&self, query: &str) -> Result<TrinoValue, ClientError> {
        let mut response = self.get_all(query).await?;
        if response.len() != 1 || response[0].len() != 1 {
            return Err(ClientError::WrongResultSize {
                rows: response.len(),
                columns: response.first().map_or(0, |r| r.len()),
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
    #[ignore]
    async fn test_execute() {
        let client = Client::default();
        let rows = client
            .get_all("SELECT JSON_PARSE('[1, 2, 3]')")
            .await
            .unwrap();
        eprintln!("rows: {:?}", rows);
    }
}
