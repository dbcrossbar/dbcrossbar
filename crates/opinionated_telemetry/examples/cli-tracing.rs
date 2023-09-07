use anyhow::Result;
use opinionated_telemetry::{run_with_telemetry, set_parent_span_from_env, AppType};
use tracing::{debug, instrument, Level};

#[tokio::main]
async fn main() -> Result<()> {
    // Set up all our telemetry.
    //
    // We can't create any spans until we're inside `main_helper`, because we
    // need to wait for `run_with_telemetry` to start the tracing subsystem.
    run_with_telemetry(
        AppType::Cli,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        main_helper(),
    )
    .await
}

#[instrument(
    level = Level::INFO,
    name = "cli-tracing",
    fields(version = env!("CARGO_PKG_VERSION"))
)]
async fn main_helper() -> Result<()> {
    // Hook into any existing trace passed via `TRACEPARENT` and `TRACESTATE`
    // headers. If we can't find one, start a new trace.
    set_parent_span_from_env();

    debug!("Hello, world!");
    Ok(())
}
