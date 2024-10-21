//! Trino client errors.

use std::{error, fmt};

use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};

use crate::DataType;

/// An error returned by our Trino client.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Could not deserialize a JSON value as a [`crate::TrinoValue`].
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
        type_signature: Box<dyn std::fmt::Debug>,
    },
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
            Self::CouldNotDeserializeValue { value, data_type } => {
                write!(f, "could not deserialize value {} as {}", value, data_type)
            }
            Self::MissingColumnInfo => write!(f, "missing column information"),
            Self::QueryError(e) => write!(f, "Trino query error: {}", e),
            Self::ReqwestError(e) => write!(f, "HTTP error: {}", e),
            Self::UnsupportedTypeSignature { type_signature } => {
                write!(f, "unsupported type signature: {:#?}", type_signature)
            }
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
            Self::CouldNotDeserializeValue { .. } => None,
            Self::MissingColumnInfo => None,
            Self::QueryError(e) => Some(e),
            Self::ReqwestError(e) => Some(e),
            Self::UnsupportedTypeSignature { .. } => None,
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
