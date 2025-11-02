//! Retry utilities with exponential backoff.

use std::time::Duration;
use tokio::time::sleep;

use crate::common::*;

/// Helper macro to convert a Result into a WaitStatus, marking errors as permanent failures.
#[macro_export]
macro_rules! try_with_permanent_failure {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => return $crate::wait::WaitStatus::FailedPermanently(err.into()),
        }
    };
}

/// Options for retrying operations.
#[derive(Clone, Debug)]
pub(crate) struct WaitOptions {
    retry_interval: Duration,
    allowed_errors: usize,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            allowed_errors: 3,
        }
    }
}

impl WaitOptions {
    /// Set the initial retry interval.
    pub(crate) fn retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    /// Set the number of allowed errors before giving up.
    pub(crate) fn allowed_errors(mut self, allowed_errors: usize) -> Self {
        self.allowed_errors = allowed_errors;
        self
    }
}

/// The status of a wait operation.
#[derive(Debug)]
pub(crate) enum WaitStatus<T, E> {
    /// The operation finished successfully.
    Finished(T),
    /// The operation failed temporarily and should be retried.
    FailedTemporarily(E),
    /// The operation failed permanently and should not be retried.
    FailedPermanently(E),
}

/// Retry an async operation with exponential backoff.
pub(crate) async fn wait<T, E, F, Fut>(
    options: &WaitOptions,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = WaitStatus<T, E>>,
{
    let mut errors = 0;
    let mut interval = options.retry_interval;

    loop {
        match f().await {
            WaitStatus::Finished(result) => return Ok(result),
            WaitStatus::FailedPermanently(err) => return Err(err),
            WaitStatus::FailedTemporarily(err) => {
                errors += 1;
                if errors > options.allowed_errors {
                    return Err(err);
                }

                trace!(
                    "operation failed temporarily, retrying in {:?} (attempt {}/{})",
                    interval,
                    errors,
                    options.allowed_errors
                );

                sleep(interval).await;

                interval = interval.saturating_mul(2);
            }
        }
    }
}

