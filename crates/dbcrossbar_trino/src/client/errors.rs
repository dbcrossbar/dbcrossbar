//! Trino client errors.

use std::{error, fmt};

use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};

use crate::{errors::ConnectorError, values::ConversionError, DataType};

/// An error returned by our Trino client.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Connector error.
    Connector(ConnectorError),
    /// We could not convert a value to the expected type.
    Conversion(ConversionError),
    /// Could not deserialize a JSON value as a [`crate::Value`].
    CouldNotDeserializeValue {
        /// The JSON value that could not be deserialized.
        value: JsonValue,
        /// The type signature that was expected.
        data_type: DataType,
    },
    /// No column information was returned by the server.
    MissingColumnInfo,
    /// An error returned by the Trino server.
    QueryError(QueryError),
    /// An error returned by the HTTP client.
    ReqwestError(reqwest::Error),
    /// An unsupported type signature.
    UnsupportedTypeSignature {
        /// The type signature that was unsupported.
        type_signature: Box<dyn std::fmt::Debug + Send + Sync + 'static>,
    },
    /// Expected a single column, but got something else.
    TooManyColumns {
        /// The number of columns returned.
        columns: usize,
    },
    /// Expected a single row, but got something else.
    TooManyRows {
        /// The number of rows returned.
        rows: usize,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Connector(e) => write!(f, "{}", e),
            Self::Conversion(e) => write!(f, "could not conver returned value: {}", e),
            Self::CouldNotDeserializeValue { value, data_type } => {
                write!(f, "could not deserialize value {} as {}", value, data_type)
            }
            Self::MissingColumnInfo => write!(f, "missing column information"),
            Self::QueryError(e) => write!(f, "Trino query error: {}", e),
            Self::ReqwestError(e) => write!(f, "HTTP error: {}", e),
            Self::UnsupportedTypeSignature { type_signature } => {
                write!(f, "unsupported type signature: {:#?}", type_signature)
            }
            Self::TooManyColumns { columns } => {
                write!(f, "expected 1 column, found {} columns", columns)
            }
            Self::TooManyRows { rows } => {
                write!(f, "expected 1 row, found {} rows", rows)
            }
        }
    }
}

impl error::Error for ClientError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Connector(e) => Some(e),
            Self::Conversion(e) => Some(e),
            Self::CouldNotDeserializeValue { .. } => None,
            Self::MissingColumnInfo => None,
            Self::QueryError(e) => Some(e),
            Self::ReqwestError(e) => Some(e),
            Self::UnsupportedTypeSignature { .. } => None,
            Self::TooManyColumns { .. } => None,
            Self::TooManyRows { .. } => None,
        }
    }
}

impl From<ConnectorError> for ClientError {
    fn from(e: ConnectorError) -> Self {
        Self::Connector(e)
    }
}

impl From<ConversionError> for ClientError {
    fn from(e: ConversionError) -> Self {
        Self::Conversion(e)
    }
}

impl From<QueryError> for ClientError {
    fn from(e: QueryError) -> Self {
        Self::QueryError(e)
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(e: reqwest::Error) -> Self {
        Self::ReqwestError(e)
    }
}

/// An error returned from a Trino query.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct QueryError {
    pub message: String,
    pub error_code: i32, // Not 100% sure about the size.
    pub error_name: String,
    pub error_type: String,

    // Any other fields we don't handle yet.
    #[serde(flatten)]
    _other: Map<String, JsonValue>,
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
