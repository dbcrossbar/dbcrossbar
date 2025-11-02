//! Tools for tracing, with OpenTelemetry integration.

use std::{collections::HashMap, env, error, fmt, str::FromStr};

use futures::{future::BoxFuture, FutureExt};
use once_cell::sync::Lazy;
use opentelemetry::{
    global,
    propagation::{Extractor, Injector, TextMapCompositePropagator},
    trace::TracerProvider as _,
    KeyValue,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    propagation::{BaggagePropagator, TraceContextPropagator},
    resource::{EnvResourceDetector, ResourceDetector, SdkProvidedResourceDetector},
    trace::{SdkTracerProvider, SpanData, SpanExporter},
    Resource,
};
use opentelemetry_stackdriver::{GcpAuthorizer, StackDriverExporter};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*, Registry};

use crate::{env_extractor::EnvExtractor, env_injector::EnvInjector, TelemetryConfig};

use super::{debug_exporter::DebugExporter, Error, Result};

/// Our extensions to the `tracing::Span` type.
pub trait SpanExt {
    /// Record `result` in this `Span`, and return it. Expects the span to have
    /// the following properties:
    ///
    /// - `otel.status_code`
    /// - `otel.status_message`
    fn record_result<T, E>(&self, result: Result<T, E>) -> Result<T, E>
    where
        E: fmt::Display;
}

impl SpanExt for tracing::Span {
    fn record_result<T, E>(&self, result: Result<T, E>) -> Result<T, E>
    where
        E: fmt::Display,
    {
        match result {
            Ok(value) => {
                // It's apparently discouraged to set "OK" status codes
                // from instrumentation libraries?
                Ok(value)
            }
            Err(err) => {
                self.record("otel.status_code", "ERROR");
                self.record("otel.status_message", err.to_string().as_str());
                Err(err)
            }
        }
    }
}

/// An error occurred parsing a [`TracerType`].
#[derive(Debug)]
struct TracerTypeParseError(String);

impl fmt::Display for TracerTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown tracer type: {:?}", self.0)
    }
}

impl error::Error for TracerTypeParseError {}

/// An OpenTracing tracer type to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TracerType {
    /// Log spans to CloudTrace.
    CloudTrace,
    /// Print spans on `stderr`. Handy for debugging.
    Debug,
}

impl FromStr for TracerType {
    type Err = TracerTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cloud_trace" => Ok(TracerType::CloudTrace),
            "debug" => Ok(TracerType::Debug),
            _ => Err(TracerTypeParseError(s.to_owned())),
        }
    }
}

impl fmt::Display for TracerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TracerType::CloudTrace => "cloud_trace".fmt(f),
            TracerType::Debug => "debug".fmt(f),
        }
    }
}

/// Set up OpenTracing.
fn install_opentracing_globals() {
    // Install our propagator, which takes care of reading and writing the
    // various headers that we receive from other services and pass on to
    // services we call.
    let propagator = TextMapCompositePropagator::new(vec![
        // Handle user-defined `baggage` that we pass along a trace.
        Box::new(BaggagePropagator::new()),
        // Handle `traceparent` and `tracestate`, which identify the trace we're
        // a part of.
        Box::new(TraceContextPropagator::new()),
    ]);
    global::set_text_map_propagator(propagator);
}

/// Enum to hold different exporter types.
#[derive(Debug)]
enum ExporterType {
    CloudTrace(StackDriverExporter),
    Debug(DebugExporter),
}

impl ExporterType {
    /// Construct an exporter for the specified `tracer_type`.
    async fn for_tracer_type(
        tracer_type: TracerType,
    ) -> Result<(Self, BoxFuture<'static, ()>)> {
        match tracer_type {
            TracerType::CloudTrace => {
                env::var("GCLOUD_SERVICE_ACCOUNT_KEY_PATH").map_err(|_| {
                    Error::env_var_not_set("GCLOUD_SERVICE_ACCOUNT_KEY_PATH")
                })?;
                let authenticator = GcpAuthorizer::new()
                    .await
                    .map_err(Error::could_not_configure_tracing)?;
                let (exporter, future) = StackDriverExporter::builder()
                    .build(authenticator)
                    .await
                    .map_err(Error::could_not_configure_tracing)?;
                Ok((ExporterType::CloudTrace(exporter), future.boxed()))
            }
            TracerType::Debug => {
                Ok((ExporterType::Debug(DebugExporter), async {}.boxed()))
            }
        }
    }
}

impl SpanExporter for ExporterType {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        match self {
            ExporterType::CloudTrace(exporter) => exporter.export(batch).boxed(),
            ExporterType::Debug(exporter) => exporter.export(batch).boxed(),
        }
    }
}

/// Our library name.
static CRATE_NAME: &str = env!("CARGO_PKG_NAME");

/// A future returned by our tracer provider.
static TRACER_JOIN_HANDLE: Lazy<RwLock<Option<JoinHandle<()>>>> =
    Lazy::new(|| RwLock::new(None));

/// Configure tracing.
pub async fn start_tracing(config: &TelemetryConfig) -> Result<()> {
    install_opentracing_globals();

    let filter = tracing_subscriber::EnvFilter::from_default_env();

    let tracer_type = env::var("OPINIONATED_TELEMETRY_TRACER")
        .ok()
        .map(|t| t.parse())
        .transpose()
        .map_err(Error::could_not_configure_tracing)?;
    if let Some(tracer_type) = tracer_type {
        //eprintln!("tracer_type: {}", tracer_type);

        // Configure our tracer.
        let (exporter, future) = ExporterType::for_tracer_type(tracer_type).await?;
        *TRACER_JOIN_HANDLE.write().await = Some(tokio::spawn(future));

        // Detect information about our environment and build resource.
        let mut resource_kvs = vec![
            KeyValue::new("service.name", config.service_name.clone()),
            KeyValue::new("service.version", config.service_version.clone()),
        ];

        // Add detected resources
        let sdk_resource = SdkProvidedResourceDetector.detect();
        let env_resource = EnvResourceDetector::default().detect();
        for (key, value) in sdk_resource.iter().chain(env_resource.iter()) {
            resource_kvs.push(KeyValue::new(key.clone(), value.clone()));
        }

        let resource = Resource::builder().with_attributes(resource_kvs).build();

        // Configure our tracer provider.
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer(CRATE_NAME);
        global::set_tracer_provider(provider);

        // Send all logs to our OpenTracing tracer.
        let subscriber = Registry::default()
            //.with(MetricsLayer::new())
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(filter);
        tracing::subscriber::set_global_default(subscriber)
            .expect("Could not set up global logger");

        // We also need to set this up manually.
        tracing_log::LogTracer::init().expect("could not hook up `log` to tracing");
    } else {
        tracing_subscriber::fmt::Subscriber::builder()
            .with_writer(std::io::stderr)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_env_filter(filter)
            .finish()
            //.with(MetricsLayer::new())
            .try_init()
            .expect("could not install tracing subscriber");
    }
    Ok(())
}

/// Shut down tracing and flush any pending trace information.
pub async fn stop_tracing() {
    if let Some(handle) = TRACER_JOIN_HANDLE.write().await.take() {
        handle.await.expect("could not join trace exporter");
    }
}

/// Trait that allows adding an external [`opentelemetry::Context`] to an
/// existing [`tracing::Span`].
pub trait SetParentFromExtractor: Sized {
    /// Extract an OpenTracing trace [`opentelemetry::Context`] from
    /// `extractor`, and add it to this span.
    fn set_parent_from_extractor(&mut self, extractor: &dyn Extractor);

    /// Extract an OpenTracing trace [`opentelemetry::Context`] from
    /// the environment, and add it to this span.
    fn set_parent_from_env(&mut self) {
        self.set_parent_from_extractor(&EnvExtractor::from_env());
    }
}

impl SetParentFromExtractor for tracing::Span {
    fn set_parent_from_extractor(&mut self, extractor: &dyn Extractor) {
        global::get_text_map_propagator(|propagator| {
            let context = propagator.extract(extractor);

            // eprintln!(
            //     "context: {:?} {:?} {:?} {:?}, {:?}",
            //     context.get::<TraceId>(),
            //     context.get::<SpanId>(),
            //     context.get::<TraceState>(),
            //     context.span(),
            //     context.baggage(),
            // );

            let _ = self.set_parent(context);
        });
    }
}

/// Set the parent of the current span using the given extractor. If no
/// trace span can be found using the extractor, start a new trace instead
pub fn set_parent_span_from(extractor: &dyn Extractor) {
    let mut span: tracing::Span = tracing::Span::current();
    span.set_parent_from_extractor(extractor);
}

/// Set the parent of the current span using the environment. This will use
/// the `TRACEPARENT` and `TRACESTATE` if present.
pub fn set_parent_span_from_env() {
    let mut span: tracing::Span = tracing::Span::current();
    span.set_parent_from_env();
}

/// Export the current [`tracing::Span`] using [`Injector`].
pub fn inject_current_span_into(injector: &mut dyn Injector) {
    global::get_text_map_propagator(|propagator| {
        let span: tracing::Span = tracing::Span::current();
        let context: opentelemetry::Context = span.context();
        propagator.inject_context(&context, injector);
    });
}

/// Export the current [`tracing::Span`] in a format suitable for passing to
/// [`tokio::process::Command::envs`].
pub fn current_span_as_env() -> impl Iterator<Item = (String, String)> {
    let mut injector = EnvInjector::new();
    inject_current_span_into(&mut injector);
    injector.into_iter()
}

/// Export the current [`tracing::Span`] as HTTP-style headers stored in a
/// `HashMap`.
pub fn current_span_as_headers() -> HashMap<String, String> {
    let mut injector = HashMap::new();
    inject_current_span_into(&mut injector);
    injector
}
