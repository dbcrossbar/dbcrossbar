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
    values::{ConversionError, DataTypeOrAny, ExpectedDataType},
    ConnectorType, DataType, Field, Ident, QuotedString,
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
    pub(crate) columns: Option<Vec<ResponseColumn>>,
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
pub(crate) struct ResponseColumn {
    pub(crate) name: Ident,
    #[serde(rename = "type")]
    type_string: String,
    pub(crate) type_signature: TypeSignature,
}

impl fmt::Display for ResponseColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.type_string)
    }
}

/// Convert a [`ResponseColumn`] into a ROW [`Field`].
impl TryInto<Field> for &'_ ResponseColumn {
    type Error = ClientError;

    fn try_into(self) -> Result<Field, ClientError> {
        Ok(Field::named(
            self.name.to_owned(),
            DataType::try_from(&self.type_signature)?,
        ))
    }
}

/// The default catalog and schema for a client.
struct CatalogAndSchema {
    catalog: String,
    schema: String,
}

/// Build a client.
pub struct ClientBuilder {
    user: String,
    host: String,
    port: u16,
    password: Option<String>,
    use_https: bool,
    catalog_and_schema: Option<CatalogAndSchema>,
}

impl ClientBuilder {
    /// Create a new builder with default values for running tests.
    pub fn for_tests() -> Self {
        Self::new("admin".to_owned(), "localhost".to_owned(), 8080)
    }

    /// Create a new builder.
    pub fn new(user: String, host: String, port: u16) -> Self {
        Self {
            user,
            host,
            port,
            use_https: false,
            password: None,
            catalog_and_schema: None,
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

    /// Set the default catalog and schema and assume. Note that this does not
    /// currently update in a [`Client`] if you run SQL statments to change the
    /// catalog. You need to specify it up front for now.
    pub fn catalog_and_schema(mut self, catalog: String, schema: String) -> Self {
        self.catalog_and_schema = Some(CatalogAndSchema { catalog, schema });
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
            catalog_and_schema: self.catalog_and_schema,
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
    catalog_and_schema: Option<CatalogAndSchema>,
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
            let mut req = self
                .client
                .post(&url)
                .basic_auth(&self.user, self.password.as_ref());
            if let Some(catalog_and_schema) = &self.catalog_and_schema {
                req = req
                    .header("X-Trino-Catalog", &catalog_and_schema.catalog)
                    .header("X-Trino-Schema", &catalog_and_schema.schema);
            }
            req.body(query.to_owned())
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

    /// Get the [`ConnectorType`] for a catalog.
    pub async fn catalog_connector_type(
        &self,
        catalog: &Ident,
    ) -> Result<ConnectorType, ClientError> {
        let sql = format!(
            "SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = {}",
            QuotedString(catalog.as_unquoted_str()),
        );
        let connector_name = self.get_one_value::<String>(&sql).await?;
        Ok(connector_name.parse::<ConnectorType>()?)
    }

    /// Get the schema of a table.
    pub async fn get_table_column_info(
        &self,
        catalog: &Ident,
        schema: &Ident,
        table_name: &Ident,
    ) -> Result<Vec<ColumnInfo>, ClientError> {
        let sql = format!(
            "\
    SELECT column_name, data_type, is_nullable
        FROM {catalog}.information_schema.columns
        WHERE table_catalog = {catalog_str}
            AND table_schema = {schema_str}
            AND table_name = {table_str}
        ORDER BY ordinal_position",
            catalog = catalog,
            // Quote as strings, not as identifiers.
            catalog_str = QuotedString(catalog.as_unquoted_str()),
            schema_str = QuotedString(schema.as_unquoted_str()),
            table_str = QuotedString(table_name.as_unquoted_str()),
        );
        //debug!(%sql, "getting table schema");
        Ok(self
            .get_all::<ColumnInfoWrapper>(&sql)
            .await?
            .into_iter()
            // Strip `ColumnInfoWrapper` from our result.
            .map(|c| c.0)
            .collect())
    }
}

impl Default for Client {
    fn default() -> Self {
        ClientBuilder::for_tests().build()
    }
}

/// Information about a column in a table, returned by
/// [`Client::get_column_info`].
#[derive(Debug)]
#[non_exhaustive]
pub struct ColumnInfo {
    pub column_name: Ident,
    pub data_type: DataType,
    pub is_nullable: bool,
}

/// An internal wrapper for [`ColumnInfo`] that implements deserialization.
///
/// We do this to avoid leaking our `TryFrom<Value>` and `ExpectedDataType`
/// `impl`s into the public API.
struct ColumnInfoWrapper(ColumnInfo);

// This is basically a customized manual version of `#[derive(TrinoRow)]`, but
// without forcing a dependency on `dbcrossbar_trino_macros`.
impl TryFrom<Value> for ColumnInfoWrapper {
    type Error = ConversionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let mkerr = || ConversionError {
            found: value.clone(),
            expected_type: Self::expected_data_type(),
        };
        match &value {
            Value::Row { values, .. } if values.len() == 3 => {
                let column_name_str = String::try_from(values[0].clone())?;
                let data_type_str = String::try_from(values[1].clone())?;
                let is_nullable_str = String::try_from(values[2].clone())?;
                Ok(ColumnInfoWrapper(ColumnInfo {
                    column_name: Ident::new(&column_name_str).map_err(|_| mkerr())?,
                    data_type: data_type_str.parse().map_err(|_| mkerr())?,
                    is_nullable: is_nullable_str == "YES",
                }))
            }
            _ => Err(mkerr()),
        }
    }
}

impl ExpectedDataType for ColumnInfoWrapper {
    fn expected_data_type() -> DataTypeOrAny {
        // These `unwrap`s are safe because the column name is known
        // at compile time.
        DataTypeOrAny::DataType(DataType::Row(vec![
            Field::named(Ident::new("column_name").unwrap(), DataType::varchar()),
            Field::named(Ident::new("data_type").unwrap(), DataType::varchar()),
            Field::named(Ident::new("is_nullable").unwrap(), DataType::varchar()),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::{values::IsCloseEnoughTo, ConnectorType};

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

    #[tokio::test]
    #[ignore]
    async fn test_default_catalog_and_schema() {
        for connector in ConnectorType::all_testable() {
            let catalog = connector.test_catalog();
            let schema = connector.test_schema();
            let client = ClientBuilder::for_tests()
                .catalog_and_schema(catalog.to_owned(), schema.to_owned())
                .build();
            let table = format!("test_default_catalog_and_schema_{}", connector);
            let full_table_name = format!("{}.{}.{}", catalog, schema, table);

            // Drop our test table if it exists.
            client
                .run_statement(&format!("DROP TABLE IF EXISTS {}", full_table_name))
                .await
                .expect("could not drop table");

            // Create a table with the default catalog and schema.
            let create_table_sql = format!("CREATE TABLE {} (id INT)", table);
            client
                .run_statement(&create_table_sql)
                .await
                .expect("could not create table");

            // Query the table with an explicit catalog and schema.
            let query_sql = format!("SELECT * FROM {}", full_table_name);
            let _rows = client
                .get_all::<Vec<Value>>(&query_sql)
                .await
                .expect("could not query table");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_connector_type() {
        for connector in ConnectorType::all_testable() {
            let client = Client::default();
            let catalog = Ident::new(connector.test_catalog()).unwrap();
            let found = client
                .catalog_connector_type(&catalog)
                .await
                .expect("could not get connector type");
            assert_eq!(found, connector);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_table_column_info() {
        for connector in ConnectorType::all_testable() {
            let client = Client::default();

            // Build our table name manually so we can pass it to
            // `get_table_column_info`.
            let catalog = Ident::new(connector.test_catalog()).unwrap();
            let schema = Ident::new(connector.test_schema()).unwrap();
            let table =
                Ident::new(&format!("get_table_column_info_{}", connector)).unwrap();
            let table_name = format!("{}.{}.{}", catalog, schema, table);

            // Drop our test table if it exists.
            client
                .run_statement(&format!("DROP TABLE IF EXISTS {}", table_name))
                .await
                .expect("could not drop table");

            let not_null = if connector.supports_not_null_constraint() {
                " NOT NULL"
            } else {
                ""
            };

            // Create a new table with the transformed type.
            let create_table_sql = format!(
                "CREATE TABLE {} (id INT{not_null}, name VARCHAR)",
                table_name,
            );
            client
                .run_statement(&create_table_sql)
                .await
                .expect("could not create table");

            let column_info = client
                .get_table_column_info(&catalog, &schema, &table)
                .await
                .expect("could not get column info");

            assert_eq!(column_info.len(), 2);

            assert_eq!(column_info[0].column_name.as_unquoted_str(), "id");
            assert_eq!(column_info[0].data_type, DataType::Int);
            assert_eq!(
                column_info[0].is_nullable,
                !connector.supports_not_null_constraint()
            );

            assert_eq!(column_info[1].column_name.as_unquoted_str(), "name");
            assert_eq!(column_info[1].data_type, DataType::varchar());
            assert!(column_info[1].is_nullable);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn deserialize_null() {
        let client = Client::default();
        let value = client
            .get_one_value::<Value>("SELECT CAST(NULL AS VARCHAR)")
            .await
            .unwrap();
        assert!(value.is_close_enough_to(&Value::Null {
            literal_type: DataType::varchar()
        }));
    }
}
