use std::{error, result};

use thiserror::Error;

/// The result type of this library.
pub type Result<T, E = Error> = result::Result<T, E>;

/// A monitoring-related error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("The environment variable {0} was not specified")]
    EnvVarNotSet(String),
    #[error("Could not connect to trace collector")]
    CouldNotConfigureTracing(Box<dyn error::Error + Send + Sync>),
    #[error("Could not configure metrics reporting")]
    CouldNotConfigureMetrics(Box<dyn error::Error + Send + Sync>),
}
