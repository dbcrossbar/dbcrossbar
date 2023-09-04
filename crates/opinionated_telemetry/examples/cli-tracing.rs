use anyhow::Result;
use opinionated_telemetry::{debug, debug_span, trace_with_parent_span_from_env};

#[tokio::main]
async fn main() -> Result<()> {
    trace_with_parent_span_from_env(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        // Your root span. Add any fields you want.
        || debug_span!("cli-tracing", version = env!("CARGO_PKG_VERSION")),
        // The body of the span.
        async {
            debug!("Hello, world!");
            Ok(())
        },
    )
    .await
}
