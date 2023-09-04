//! Common monitoring tools we'll break out into an open source crate.
//!
//! We focus on supporting the following Rust APIs:
//!
//! - `tracing` for tracing, with support for fowarding from `log`.
//! - `metrics` for monitoring.
//!
//! We specifically try to integrate with OpenTelemetry and to support standard
//! `"traceparent"` and `"tracestate"` headers.

use futures::Future;
// Re-export all the APIs we encourage people to use.
pub use ::metrics::{
    self, counter, decrement_gauge, gauge, histogram, increment_counter,
    increment_gauge, register_counter, register_gauge, register_histogram,
};
pub use ::tracing::{
    self, debug, debug_span, error, error_span, event, info, info_span, instrument,
    span, trace, trace_span, warn, warn_span, Instrument, Level,
};

mod debug_exporter;
mod env_extractor;
mod env_injector;
mod error;
mod glue;
mod our_metrics;
mod our_tracing;

pub use self::error::{Error, Result};
pub use self::our_tracing::{
    current_span_as_env, current_span_as_headers, end_tracing,
    inject_current_span_into, set_parent_span_from, set_parent_span_from_env,
    start_tracing, SetParentFromExtractor,
};

/// Start all telemetry subsystems. Normally, you will call this via
/// [`run_with_telemetry`], but you may call it directly if you're writing a
/// server that never exits.
///
/// `service_name` and `service_version` will be used to identify your service,
/// unless they are overriden by OpenTelemetry environment variables.
pub async fn start_telemetry(service_name: &str, service_version: &str) -> Result<()> {
    start_tracing(service_name, service_version).await
}

/// Stop all telemetry subsystems. Especially in CLI tools, this will often be
/// needed to flush any remaining traces and metrics before shutting down.
///
/// Normally, you will call this via [`run_with_telemetry`].
pub async fn stop_telemetry() {
    end_tracing().await;
}

/// Start all telemetry subsystems, run the given future, and then stop all
/// telemetry subsystems.
///
/// The error type returned by `fut` must support a conversion from
/// [`opinionated_telemetry::Error`].
///
/// ```
/// use anyhow::Result;
/// use opinionated_telemetry::{
///   instrument, run_with_telemetry, set_parent_span_from_env,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   run_with_telemetry(
///     env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), main_helper(),
///   ).await
/// }
///
/// // Note that `instrument` will only work correctly on functions called from
/// // inside `run_with_telemetry`.
/// #[instrument(
///   name = "my-app",
///   fields(version = env!("CARGO_PKG_VERSION"))
/// )]
/// async fn main_helper() -> Result<()> {
///  // Use TRACEPARENT and TRACESTATE from the environment to link into any
///  // existing trace. Or start a new trace if none are present.
///  set_parent_span_from_env();
///  Ok(())
/// }
/// ```
pub async fn run_with_telemetry<T, E, F>(
    service_name: &str,
    service_version: &str,
    fut: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: From<Error>,
{
    start_telemetry(service_name, service_version).await?;
    let result = fut.await;
    stop_telemetry().await;
    result
}
