use std::{error, result};

use thiserror::Error;

/// The result type of this library.
pub type Result<T, E = Error> = result::Result<T, E>;

/// A monitoring-related error.
#[derive(Debug, Error)]
pub enum Error {
    #[error("The environment variable {0} was not specified")]
    EnvVarNotSet(String),
    #[error("Could not connect to trace collector")]
    CouldNotConnectToTraceExporter(Box<dyn error::Error + Send + Sync>),
}
