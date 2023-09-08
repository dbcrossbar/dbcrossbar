//! An "opinioned" interface to the
//! [`metrics`](https://docs.rs/metrics/latest/metrics/) library.
//!
//! ## Features
//!
//! Here's what's implemented, and what might get done depending on what the
//! maintainers need at work.
//!
//! - Backends
//!   - [x] Logging
//!   - [x] Prometheus (scraping)
//!   - [x] Prometheus (push gateway)
//!   - [ ] Jaeger
//! - Modes
//!   - [x] Metrics reporting for CLI tools.
//!   - [x] Metrics reporting for servers.
//!

use std::{convert::Infallible, env, fmt, str::FromStr};

use futures::channel::oneshot;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Client, Method, Request, Response, Server, StatusCode,
};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::{
    debug, error,
    prometheus_recorder::{PrometheusRecorder, PrometheusRenderer},
    AppType, Error, Result, TelemetryConfig,
};

/// If set, use this `Reporter` to report final metrics on program exit. This is
/// only really relevant for CLI tools.
static METRICS_REPORTER: Lazy<RwLock<Option<Reporter>>> =
    Lazy::new(|| RwLock::new(None));

/// A sender we can use to stop the Prometheus server, if we have one.
static STOP_PROMETHEUS_SERVER: Lazy<RwLock<Option<oneshot::Sender<()>>>> =
    Lazy::new(|| RwLock::new(None));

/// An error occurred parsing a [`MetricsType`].
#[derive(Debug)]
struct MetricsTypeParseError(String);

impl fmt::Display for MetricsTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown metrics type: {:?}", self.0)
    }
}

impl std::error::Error for MetricsTypeParseError {}

/// An metrics backend type to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetricsType {
    /// Export metrics to Prometheus. This will choose either an embedded
    /// webserver or a push gateway, depending on [`AppType`].
    Prometheus,
    /// Print metrics using `tracing::debug!()`.
    Debug,
}

impl FromStr for MetricsType {
    type Err = MetricsTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "prometheus" => Ok(MetricsType::Prometheus),
            "debug" => Ok(MetricsType::Debug),
            _ => Err(MetricsTypeParseError(s.to_owned())),
        }
    }
}

impl fmt::Display for MetricsType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricsType::Prometheus => f.write_str("prometheus"),
            MetricsType::Debug => f.write_str("debug"),
        }
    }
}

/// Set up metrics reporting for a CLI tool.
///
/// This should be called after `start_tracing()`, because it assumes that
/// it can log via the tracing subsystem.
pub async fn start_metrics(config: &TelemetryConfig) -> Result<()> {
    let metrics_type = env::var("OPINIONATED_TELEMETRY_METRICS")
        .ok()
        .map(|s| s.parse())
        .transpose()
        .map_err(Error::could_not_configure_metrics)?;
    if let Some(metrics_type) = metrics_type {
        // Set up our `PrometheusRecorder`.
        let recorder = PrometheusRecorder::new(config.global_metrics_labels.clone());
        let renderer = recorder.renderer();
        recorder.install()?;
        let reporter = match metrics_type {
            MetricsType::Prometheus => match config.app_type {
                AppType::Server => {
                    start_prometheus_server(renderer).await?;
                    None
                }
                AppType::Cli => Some(Reporter::PrometheusPushGateway(renderer)),
            },
            MetricsType::Debug => Some(Reporter::Debug(renderer)),
        };
        *METRICS_REPORTER.write().await = reporter;
    } else {
        debug!("No metrics reporting configured");
    }
    Ok(())
}

/// Shut down metrics reporting, and flush any remaining metrics.
///
/// This should be called before `stop_tracing()`, because it assumes that
/// it can still log via the tracing subsystem.
pub async fn stop_metrics() {
    // Shut down our Prometheus server, if we have one.
    if let Some(sender) = STOP_PROMETHEUS_SERVER.write().await.take() {
        debug!("Shutting down Prometheus server");
        if sender.send(()).is_err() {
            error!("Error shutting down Prometheus server");
        }
    }

    // Flush our metrics reporter, if we have one.
    if let Some(handle) = METRICS_REPORTER.write().await.take() {
        debug!("Shutting down metrics reporting");
        if let Err(err) = handle.report().await {
            error!("Error reporting metrics: {:?}", err);
        }
    }
}

/// An implementation-specific handle wrapper.
enum Reporter {
    PrometheusPushGateway(PrometheusRenderer),
    Debug(PrometheusRenderer),
}

impl Reporter {
    /// Report metrics using the chosen reporter.
    pub async fn report(&self) -> Result<()> {
        match self {
            Reporter::PrometheusPushGateway(renderer) => {
                push_prometheus_metrics(renderer).await?;
            }
            Reporter::Debug(renderer) => {
                let rendered = renderer.render()?;
                debug!("Metrics:\n{}", &rendered);
            }
        }
        Ok(())
    }
}

/// Start runnning a Prometheus server in the background.
async fn start_prometheus_server(renderer: PrometheusRenderer) -> Result<()> {
    // Parse our listening address.
    let addr_string = env::var("OPINIONATED_TELEMETRY_PROMETHEUS_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9090".to_string());
    let addr = addr_string.parse().map_err(|_| {
        Error::CouldNotConfigureMetrics(
            format!(
                "cannot parse Prometheus listener address: {:?}",
                addr_string
            )
            .into(),
        )
    })?;

    // Configure our server.
    let make_svc = make_service_fn(move |_conn| {
        let renderer = renderer.clone();
        async {
            Ok::<_, Infallible>(service_fn(move |body| {
                prometheus_request_handler(renderer.clone(), body)
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    // Allow server shutdown.
    let (tx, rx) = oneshot::channel();
    *STOP_PROMETHEUS_SERVER.write().await = Some(tx);
    let graceful = server.with_graceful_shutdown(async {
        rx.await.ok();
    });

    // Run our server in the background.
    tokio::spawn(async move {
        if let Err(err) = graceful.await {
            error!("Error running Prometheus server: {:?}", err);
        }
    });

    Ok(())
}

/// Handle a request for Prometheus metrics.
async fn prometheus_request_handler(
    renderer: PrometheusRenderer,
    _: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    match renderer.render() {
        Ok(rendered) => Ok(Response::new(Body::from(rendered))),
        Err(err) => {
            error!("Error rendering Prometheus metrics: {}", err);
            let mut response =
                Response::new(Body::from("Error rendering Prometheus metrics"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(response)
        }
    }
}

/// Push Prometheus metrics to a push gateway.
async fn push_prometheus_metrics(renderer: &PrometheusRenderer) -> Result<()> {
    // Parse our push gateway address.
    let url = env::var("OPINIONATED_TELEMETRY_PROMETHEUS_PUSHGATEWAY_URL").map_err(
        |_| Error::env_var_not_set("OPINIONATED_TELEMETRY_PROMETHEUS_PUSHGATEWAY_URL"),
    )?;

    // Push our metrics.
    let rendered = renderer.render()?;
    let request = Request::builder()
        .method(Method::POST)
        .uri(&url)
        .body(Body::from(rendered))
        .map_err(Error::could_not_report_metrics)?;
    let response = Client::new()
        .request(request)
        .await
        .map_err(Error::could_not_report_metrics)?;
    let status = response.status();
    if !status.is_success() {
        let body = hyper::body::to_bytes(response.into_body())
            .await
            .map_err(Error::could_not_report_metrics)?;
        let body = String::from_utf8_lossy(&body);
        return Err(Error::CouldNotReportMetrics(
            format!(
                "error pushing metrics to push gateway: {:?}: {:?}",
                status, body
            )
            .into(),
        ));
    }
    Ok(())
}
