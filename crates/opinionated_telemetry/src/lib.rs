//! Common telemetry tools.
//!
//! We focus on supporting the following Rust APIs:
//!
//! - `tracing` for tracing, with support for fowarding from `log`.
//! - `metrics` for monitoring.
//!
//! We specifically try to integrate with OpenTelemetry and to support standard
//! `"traceparent"` and `"tracestate"` headers.
//!
//! ## Supported backends
//!
//! Tracing:
//!
//! - [x] Google Cloud Trace
//! - [ ] Jaeger (not yet supported, but we'd love a PR)
//! - [x] Debug (printed to stderr)
//!
//! Metrics:
//!
//! - [x] Prometheus
//! - [x] Prometheus (push gateway)
//! - [x] Debug (logged via `tracing`)
//!
//! ## Environment Variables
//!
//! The following variables can be used to configure
//!
//! - `RUST_LOG` can be used to control our logging levels in the normal
//!   fashion.
//! - `OPINIONATED_TELEMETRY_TRACER` can be set to `cloud_trace` or `debug` to
//!   enable OpenTelelmetry tracing. If not set, we will log to stderr using
//!   [`tracing`], honoring the filter specified by `RUST_LOG`.
//! - `OPINIONATED_TELEMETRY_METRICS` can be set to `prometheus` to enable
//!   Prometheus metrics, or `debug` to log metrics. Otherwise metrics will not
//!   be reported.
//! - `OPINIONATED_TELEMETRY_PROMETHEUS_LISTEN_ADDR` defaults to
//!   `"0.0.0.0:9090".
//! - `OPINIONATED_TELEMETRY_PROMETHEUS_PUSHGATEWAY_URL` must be specified for
//!   CLI tools using Prometheus. We strongly recommend using
//!   [`prom-aggregation-gateway`](https://github.com/zapier/prom-aggregation-gateway)
//!   instead of [Prometheus's default
//!   `pushgateway`](https://github.com/prometheus/pushgateway/).
//! - `OTEL_SERVICE_NAME` and `OTEL_SERVICE_VERSION` can be used to identify
//!   your service. If not set, we will use the `service_name` and
//!   `service_version` parameters to [`start_telemetry`] or
//!   [`run_with_telemetry`]. Other `OTEL_` variables supported by the
//!   [`opentelemetry`] crate may also be respected.
//!
//! For CLI tools, these variables will normally be set by the calling app:
//!
//! - `TRACEPARENT` and `TRACESTATE` can be passed to CLI tools to link them
//!   into an existing trace. These follow the conventions of the [W3C Trace
//!   Context](https://www.w3.org/TR/trace-context/).
//!
//! ## Metric naming conventions
//!
//! For best results across different metrics reporting systems, we recommend
//! following the [Prometheus metric naming
//! conventions](https://prometheus.io/docs/practices/naming/).
//!
//! Example metric names:
//!
//! - `myapp_requests_total`: Counter with no units.
//! - `myapp_processed_bytes_total`: Counter with units.
//! - `myapp_memory_usage_bytes`: Gauge with units.
//!
//! ## Label naming
//!
//! Labels should be "low-arity". Specifically, that means that labels should
//! have only a small number of possible values, because each possible label
//! value will require most backends to store a new time series.

use futures::Future;
// Re-export all the APIs we encourage people to use.
pub use ::metrics::{
    self, counter, decrement_gauge, describe_counter, describe_gauge,
    describe_histogram, gauge, histogram, increment_counter, increment_gauge,
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
mod metrics_support;
mod tracing_support;

pub use self::error::{Error, Result};
pub use self::metrics_support::{start_metrics, stop_metrics};
pub use self::tracing_support::{
    current_span_as_env, current_span_as_headers, inject_current_span_into,
    set_parent_span_from, set_parent_span_from_env, start_tracing, stop_tracing,
    SetParentFromExtractor,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppType {
    /// A CLI tool.
    Cli,
    /// A server.
    Server,
}

/// Start all telemetry subsystems. Normally, you will call this via
/// [`run_with_telemetry`], but you may call it directly if you're writing a
/// server that never exits.
///
/// `service_name` and `service_version` will be used to identify your service,
/// unless they are overriden by OpenTelemetry environment variables.
pub async fn start_telemetry(
    app_type: AppType,
    service_name: &str,
    service_version: &str,
) -> Result<()> {
    // Tracing first, then metrics, so metrics can use tracing.
    start_tracing(app_type, service_name, service_version).await?;
    start_metrics(app_type, service_name, service_version).await
}

/// Stop all telemetry subsystems. Especially in CLI tools, this will often be
/// needed to flush any remaining traces and metrics before shutting down.
///
/// Normally, you will call this via [`run_with_telemetry`].
pub async fn stop_telemetry() {
    // Reverse the order of `start_telemetry`. This allows metrics to flush to
    // the tracing system, if desired.
    stop_metrics().await;
    stop_tracing().await;
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
///     AppType::Cli,
///     env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),
///     main_helper(),
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
    app_type: AppType,
    service_name: &str,
    service_version: &str,
    fut: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: From<Error>,
{
    start_telemetry(app_type, service_name, service_version).await?;
    let result = fut.await;
    stop_telemetry().await;
    result
}
