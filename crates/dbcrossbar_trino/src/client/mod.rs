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
use serde_json::Value as JsonValue;

pub use self::errors::{ClientError, QueryError};
use self::{deserialize_value::deserialize_json_value, wire_types::TypeSignature};
use crate::{
    values::{ConversionError, ExpectedDataType},
    DataType, Field, Ident,
};

use super::Value;

mod deserialize_value;
mod errors;
mod wire_types;

/// The result of a Trino query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
#[allow(dead_code)]
pub(crate) struct Response {
    pub(crate) id: String,
    pub(crate) error: Option<QueryError>,
    /// The docs claim that this exists, too, but I'm not sure what's going with
    /// this.
    pub(crate) query_error: Option<QueryError>,
    pub(crate) next_uri: Option<String>,
    pub(crate) columns: Option<Vec<Column>>,
    pub(crate) data: Option<Vec<Vec<JsonValue>>>,
    pub(crate) update_type: Option<String>,

    // Any other fields we don't handle yet.
    #[serde(flatten)]
    _other: serde_json::Map<String, JsonValue>,
}

impl Response {
    /// Get any error from the response.
    pub fn error(&self) -> Option<&QueryError> {
        self.error.as_ref().or(self.query_error.as_ref())
    }
}

/// A column description in a Trino response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub(crate) struct Column {
    pub(crate) name: Ident,
    #[serde(rename = "type")]
    type_string: String,
    pub(crate) type_signature: TypeSignature,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.type_string)
    }
}

// Convert a `Column` into a ROW `Field`.
impl TryInto<Field> for &'_ Column {
    type Error = ClientError;

    fn try_into(self) -> Result<Field, ClientError> {
        Ok(Field::named(
            self.name.to_owned(),
            DataType::try_from(&self.type_signature)?,
        ))
    }
}

/// Build a client.
pub struct ClientBuilder {
    user: String,
    host: String,
    port: u16,
    password: Option<String>,
    use_https: bool,
}

impl ClientBuilder {
    /// Create a new builder.
    pub fn new(user: String, host: String, port: u16) -> Self {
        Self {
            user,
            host,
            port,
            use_https: false,
            password: None,
        }
    }

    /// Set the password.
    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    /// Enable HTTPS.
    pub fn use_https(mut self) -> Self {
        self.use_https = true;
        self
    }

    /// Build the client.
    pub fn build(self) -> Client {
        Client {
            user: self.user,
            host: self.host,
            port: self.port,
            use_https: self.use_https,
            password: self.password,
            client: reqwest::Client::new(),
        }
    }
}

/// A basic Trino REST API client, useful for testing.
pub struct Client {
    user: String,
    host: String,
    port: u16,
    use_https: bool,
    password: Option<String>,
    client: reqwest::Client,
}

impl Client {
    /// Build a statement URL.
    fn statement_url(&self) -> String {
        let scheme = if self.use_https { "https" } else { "http" };
        format!("{}://{}:{}/v1/statement", scheme, self.host, self.port)
    }

    /// Make a request created by `mkreq`, handling retries and errors.
    async fn request(
        &self,
        mkreq: &(dyn Fn() -> RequestBuilder + Send + Sync),
    ) -> Result<Response, ClientError> {
        loop {
            let response = mkreq().send().await?;
            if response.status().is_success() {
                let body: Response = response.json().await?;
                //eprintln!("response: {:#?}", body);
                if let Some(error) = body.error() {
                    return Err(ClientError::QueryError(error.to_owned()));
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
                .basic_auth(&self.user, self.password.as_ref())
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
        self.request(&|| {
            self.client
                .get(url)
                .basic_auth(&self.user, self.password.as_ref())
        })
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
    pub async fn get_all<T>(&self, query: &str) -> Result<Vec<T>, ClientError>
    where
        T: TryFrom<Value>,
        ConversionError: From<<T as TryFrom<Value>>::Error>,
    {
        let mut response = self.start_query(query).await?;
        let mut results = Vec::new();
        let mut fields: Option<Vec<Field>> = None;
        loop {
            if let Some(cols) = &response.columns {
                if fields.is_none() {
                    fields = Some(
                        cols.iter()
                            .map(|c| c.try_into())
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(data) = &response.data {
                if let Some(fields) = &fields {
                    for row in data.iter() {
                        let mut deserialized_row = Vec::new();
                        for (field, val) in fields.iter().zip(row.iter()) {
                            deserialized_row
                                .push(deserialize_json_value(&field.data_type, val)?);
                        }
                        results.push(
                            Value::Row {
                                values: deserialized_row,
                                literal_type: DataType::Row(fields.clone()),
                            }
                            .try_into()
                            .map_err(|e| {
                                ClientError::Conversion(ConversionError::from(e))
                            })?,
                        );
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

    /// Get the first row of a query. Raise an error if we do not get exactly
    /// one row.
    pub async fn get_one_row<T>(&self, query: &str) -> Result<T, ClientError>
    where
        T: TryFrom<Value>,
        ConversionError: From<<T as TryFrom<Value>>::Error>,
    {
        let mut response = self.get_all::<T>(query).await?;
        if response.len() != 1 {
            return Err(ClientError::TooManyRows {
                rows: response.len(),
            });
        }
        Ok(response.remove(0))
    }

    /// Get a the first column of the first row of a query. Raise an error if
    /// there is any other data returned, or if no data is returned.
    pub async fn get_one_value<T>(&self, query: &str) -> Result<T, ClientError>
    where
        T: TryFrom<Value> + ExpectedDataType,
        ConversionError: From<<T as TryFrom<Value>>::Error>,
    {
        let mut response = self.get_one_row::<Vec<T>>(query).await?;
        if response.len() != 1 {
            return Err(ClientError::TooManyColumns {
                columns: response.len(),
            });
        }
        Ok(response.remove(0))
    }
}

impl Default for Client {
    fn default() -> Self {
        ClientBuilder::new("admin".to_owned(), "localhost".to_owned(), 8080).build()
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
            .get_one_value::<JsonValue>("SELECT JSON_PARSE('[1, 2, 3]')")
            .await
            .unwrap();
        eprintln!("rows: {:?}", rows);
    }
}
