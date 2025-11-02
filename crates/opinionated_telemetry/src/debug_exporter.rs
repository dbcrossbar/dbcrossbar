//! Support for debugging the data captured by OpenTelemetry.

use std::{
    error, fmt,
    io::{self, Write},
};

use futures::FutureExt;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};

/// An exporter which prints spans to `stderr` as JSON.
#[derive(Debug)]
pub(crate) struct DebugExporter;

impl SpanExporter for DebugExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        async move {
            export_helper(batch)
                .await
                .map_err(|e| OTelSdkError::InternalFailure(format!("{:?}", DebugExportError::Io(e))))
        }.boxed()
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
        for kv in span.attributes.iter() {
            writeln!(&mut out, "  {} = {:?}", kv.key, kv.value)?;
        }
        for event in span.events.iter() {
            writeln!(&mut out, "  Event: {:?}", event)?;
        }
        for link in span.links.iter() {
            writeln!(&mut out, "  Link: {:?}", link)?;
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
