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
//! ## Usage
//!
//! For a simple async CLI tool, you could use this library like this:
//! ```
//! use anyhow::Result;
//! use opinionated_telemetry::{
//!   run_with_telemetry, set_parent_span_from_env, AppType,
//! };
//! use tracing::instrument;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!   run_with_telemetry(
//!     AppType::Cli,
//!     env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),
//!     main_helper(),
//!   ).await
//! }
//!
//! // Note that `instrument` will only work correctly on functions called from
//! // inside `run_with_telemetry`.
//! #[instrument(
//!   name = "my-app",
//!   fields(version = env!("CARGO_PKG_VERSION"))
//! )]
//! async fn main_helper() -> Result<()> {
//!  // Use TRACEPARENT and TRACESTATE from the environment to link into any
//!  // existing trace. Or start a new trace if none are present.
//!  set_parent_span_from_env();
//!  Ok(())
//! }
//! ```
//!
//! For more complex applications, you can use [`TelemetryConfig`]. For
//! synchronous applications, see [`run_with_telemetry_sync`] and
//! [`TelemetryConfig::install_sync`], which are available if the `sync` feature
//! is enabled.
//!
//! ## Features
//!
//! - `sync`: Enable synchronous telemetry support. Use this for otherwise
//!   synchronous applications. This is not enabled by default.
//!
//! ## Environment Variables
//!
//! The following variables can be used to configure telemetry:
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
//!   `service_version` parameters to [`TelemetryConfig`],
//!   [`run_with_telemetry`] or [`run_with_telemetry_sync`]. Other `OTEL_`
//!   variables supported by the [`opentelemetry`] crate may also be respected.
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

use std::collections::HashMap;

// We re-export `tracing` and `metrics`, but only so users can access
// traits and functions. The macros won't re-export correctly.
use futures::Future;
pub use metrics;
use metrics_support::{start_metrics, stop_metrics};
pub use tracing;
use tracing::{debug, error};
use tracing_support::{start_tracing, stop_tracing};

mod debug_exporter;
mod env_extractor;
mod env_injector;
mod error;
mod glue;
mod metrics_support;
mod prometheus_recorder;
#[cfg(feature = "sync")]
mod sync;
mod tracing_support;

pub use self::error::{Error, Result};
#[cfg(feature = "sync")]
pub use self::sync::TelemetrySyncHandle;
pub use self::tracing_support::{
    current_span_as_env, current_span_as_headers, inject_current_span_into,
    set_parent_span_from, set_parent_span_from_env, SetParentFromExtractor,
};

/// What type of application should we configure telemetry for? This will affect
/// how various kinds of telemetry are configured. For example,
/// [`AppType::Server`] would create a Prometheus scape endpoint, but
/// [`AppType::Cli`] would push metrics to a Prometheus push gateway once on
/// shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppType {
    /// A CLI tool.
    Cli,
    /// A server.
    Server,
}

/// Interface used to configure and install telemetry.
///
/// ```
/// use anyhow::Result;
/// use opinionated_telemetry::{
///   set_parent_span_from_env, AppType, TelemetryConfig,
/// };
/// use tracing::instrument;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   // Configure and install our telemetry.
///   let handle = TelemetryConfig::new(
///     AppType::Cli, env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),
///   ).install().await?;
///
///   // Call our real `main` function.
///   let result = main_helper().await;
///
///   // Flush telemetry data, shut down, and return our result.
///   handle.flush_and_shutdown().await;
///   result
/// }
///
/// // Note that `instrument` will only work correctly on functions called
/// // _after_ we call `TelemetryConfig::install`.
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
pub struct TelemetryConfig {
    app_type: AppType,
    service_name: String,
    service_version: String,
    global_metrics_labels: HashMap<String, String>,
}

impl TelemetryConfig {
    /// Create a new `TelemetryConfig`.
    ///
    /// `service_name` and `service_version` will be used to identify your
    /// service, unless they are overriden by OpenTelemetry environment
    /// variables.
    pub fn new<S1, S2>(
        app_type: AppType,
        service_name: S1,
        service_version: S2,
    ) -> TelemetryConfig
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        TelemetryConfig {
            app_type,
            service_name: service_name.into(),
            service_version: service_version.into(),
            global_metrics_labels: HashMap::new(),
        }
    }

    /// Add a label to all metrics.
    pub fn add_global_metrics_label(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.global_metrics_labels.insert(key.into(), value.into());
        self
    }

    /// Install our telemetry recorders and exporters.
    pub async fn install(self) -> Result<TelemetryHandle> {
        start_tracing(&self).await?;
        start_metrics(&self).await?;
        Ok(TelemetryHandle { running: true })
    }

    /// Install telemetry synchronously. This creates a single-threaded `tokio`
    /// runtime behind the scenes. This is only available if the `sync` feature
    /// is enabled.
    ///
    /// ```
    /// use anyhow::Result;
    /// use opinionated_telemetry::{
    ///   set_parent_span_from_env, AppType, TelemetryConfig,
    /// };
    ///
    /// fn main() -> Result<()> {
    ///   // Configure and install our telemetry.
    ///   let handle = TelemetryConfig::new(
    ///     AppType::Cli, env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),
    ///   ).install_sync()?;
    ///
    ///   // Call our real `main` function.
    ///   let result = main_helper();
    ///   handle.flush_and_shutdown();
    ///   result
    /// }
    ///
    /// // Note that `instrument` will only work correctly on functions called
    /// // _after_ we call `TelemetryConfig::install_sync`.
    /// #[tracing::instrument(
    ///   name = "my-app",
    ///   fields(version = env!("CARGO_PKG_VERSION"))
    /// )]
    /// fn main_helper() -> Result<()> {
    ///   // Use TRACEPARENT and TRACESTATE from the environment to link into any
    ///   // existing trace. Or start a new trace if none are present.
    ///   set_parent_span_from_env();
    ///   Ok(())
    /// }
    /// ```
    #[cfg(feature = "sync")]
    pub fn install_sync(self) -> Result<TelemetrySyncHandle> {
        use crate::sync::install_sync_helper;
        install_sync_helper(self)
    }
}

/// A handle that can be used to shut down telemetry and flush any remaining
/// data. If you do not call [`TelemetryHandle::flush_and_shutdown`], telemetry data may be
/// lost.
#[must_use]
pub struct TelemetryHandle {
    running: bool,
}

impl TelemetryHandle {
    /// Halt all telemetry subsystems, flushing any remaining data.
    pub async fn flush_and_shutdown(mut self) {
        self.running = false;
        stop_metrics().await;
        stop_tracing().await;
    }
}

impl Drop for TelemetryHandle {
    fn drop(&mut self) {
        // If this happens, it's a bug in the caller. We use `eprintln!` and
        // not `error!` because we have no idea if logging is working at this
        // point.
        if self.running {
            eprintln!("WARNING: Telemetry was not stopped cleanly, telemetry data may be lost")
        }
    }
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
///   run_with_telemetry, set_parent_span_from_env, AppType,
/// };
/// use tracing::instrument;
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
    let handle = TelemetryConfig::new(app_type, service_name, service_version)
        .install()
        .await?;
    let result = fut.await;
    handle.flush_and_shutdown().await;
    result
}

/// Like [`run_with_telemetry`], but for synchronous telemetry.
///
/// ```
/// use anyhow::Result;
/// use opinionated_telemetry::{
///   run_with_telemetry_sync, set_parent_span_from_env, AppType,
/// };
/// use tracing::instrument;
///
/// fn main() -> Result<()> {
///   run_with_telemetry_sync(
///     AppType::Cli,
///     env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"),
///     main_helper,
///   )
/// }
///
/// // Note that `instrument` will only work correctly on functions called from
/// // inside `run_with_telemetry_sync`.
/// #[instrument(
///   name = "my-app",
///   fields(version = env!("CARGO_PKG_VERSION"))
/// )]
/// fn main_helper() -> Result<()> {
///  // Use TRACEPARENT and TRACESTATE from the environment to link into any
///  // existing trace. Or start a new trace if none are present.
///  set_parent_span_from_env();
///  Ok(())
/// }
/// ```
#[cfg(feature = "sync")]
pub fn run_with_telemetry_sync<T, E, F>(
    app_type: AppType,
    service_name: &str,
    service_version: &str,
    fut: F,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
    E: From<Error>,
{
    let handle = TelemetryConfig::new(app_type, service_name, service_version)
        .install_sync()?;
    let result = fut();
    handle.flush_and_shutdown();
    result
}
