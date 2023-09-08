//! Support for debugging the data captured by OpenTelemetry.

use std::{
    error, fmt,
    io::{self, Write},
};

use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use opentelemetry::{
    sdk::export::{
        trace::{ExportResult, SpanData, SpanExporter},
        ExportError,
    },
    trace::TraceError,
};

/// An exporter which prints spans to `stderr` as JSON.
#[derive(Debug)]
pub(crate) struct DebugExporter;

#[async_trait]
impl SpanExporter for DebugExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        export_helper(batch)
            .map_err(|e| TraceError::Other(Box::new(DebugExportError::Io(e))))
            .boxed()
    }
}

async fn export_helper(batch: Vec<SpanData>) -> io::Result<()> {
    let stderr = io::stderr();
    let mut out = stderr.lock();
    for span in &batch {
        writeln!(
            &mut out,
            "Span {} (trace_id: {}, span_id: {})",
            span.name,
            span.span_context.trace_id(),
            span.span_context.span_id(),
        )?;
        writeln!(&mut out, "  Parent: {:?}", span.parent_span_id)?;
        writeln!(&mut out, "  Status: {:?}", span.status)?;
        for (key, value) in &span.attributes {
            writeln!(&mut out, "  {} = {}", key, value)?;
        }
        for event in span.events.iter() {
            writeln!(&mut out, "  Event: {:?}", event)?;
        }
        for link in span.links.iter() {
            writeln!(&mut out, "  Link: {:?}", link)?;
        }

        let res = &span.resource;
        for (key, value) in res.iter() {
            writeln!(&mut out, "  (Resource) {} = {}", key, value)?;
        }

        // We're still missing some interesting stuff.
        //writeln!(&mut out, "{:#?}", span)?;
    }
    Ok(())
}

/// Errors that can occur exporting span information to the console.
#[derive(Debug)]
enum DebugExportError {
    Io(io::Error),
}

impl fmt::Display for DebugExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DebugExportError::Io(err) => {
                write!(f, "could not write trace to stderr: {}", err)
            }
        }
    }
}

impl error::Error for DebugExportError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DebugExportError::Io(err) => Some(err),
        }
    }
}

impl ExportError for DebugExportError {
    fn exporter_name(&self) -> &'static str {
        "JsonDebugExporter"
    }
}
