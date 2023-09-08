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
    #[error("Could not report metrics")]
    CouldNotReportMetrics(Box<dyn error::Error + Send + Sync>),
}

impl Error {
    /// Build a new `Error::EnvVarNotSet` error.
    pub(crate) fn env_var_not_set<S>(var: S) -> Self
    where
        S: Into<String>,
    {
        Error::EnvVarNotSet(var.into())
    }

    /// Build a new `Error::EnvVarNotSet` error.
    pub(crate) fn could_not_configure_tracing<E>(err: E) -> Self
    where
        E: error::Error + Send + Sync + 'static,
    {
        Error::CouldNotConfigureTracing(Box::new(err))
    }

    /// Build a new `Error::CouldNotConfigureMetrics` error.
    pub(crate) fn could_not_configure_metrics<E>(err: E) -> Self
    where
        E: error::Error + Send + Sync + 'static,
    {
        Error::CouldNotConfigureMetrics(Box::new(err))
    }

    // Build a new `Error::CouldNotReportMetrics` error.
    pub(crate) fn could_not_report_metrics<E>(err: E) -> Self
    where
        E: error::Error + Send + Sync + 'static,
    {
        Error::CouldNotReportMetrics(Box::new(err))
    }
}
