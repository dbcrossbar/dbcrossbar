use std::env::current_exe;

use anyhow::{anyhow, Result};
use opinionated_telemetry::{
    current_span_as_env, current_span_as_headers, describe_counter, increment_counter,
    instrument, run_with_telemetry, set_parent_span_from_env, AppType, Level,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::tcp::OwnedWriteHalf,
    process::Command,
};

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
    name = "client-tracing",
    fields(version = env!("CARGO_PKG_VERSION"))
)]
async fn main_helper() -> Result<()> {
    // Hook into any existing trace passed via `TRACEPARENT` and `TRACESTATE`
    // headers. If we can't find one, start a new trace.
    set_parent_span_from_env();

    // Update a metric. Note that if you want to do this from a CLI tool, you
    // probably want to run https://github.com/zapier/prom-aggregation-gateway
    // instead of the standard Prometheus push gateway.
    describe_counter!("clienttracing.run.count", "Number of times we've run");
    increment_counter!("clienttracing.run.count");

    // Make some sample requests.
    make_request_to_server().await?;
    call_cli_tool().await
}

/// Make a request to an HTTP server, passing the current trace information.
#[instrument]
async fn make_request_to_server() -> Result<()> {
    // Open a TCP connection to the server.
    let stream = tokio::net::TcpStream::connect("127.0.0.1:9321").await?;
    let (read_half, write_half) = stream.into_split();
    let mut wtr = BufWriter::new(write_half);
    let mut rdr = BufReader::new(read_half);

    // Get our headers from OpenTracing and write them.
    let headers = current_span_as_headers();
    eprintln!("Headers: {:?}", headers);
    for (header, value) in headers {
        if !value.is_empty() {
            write_header_line(&mut wtr, &header, &value).await?;
        }
    }
    wtr.write_all(b"\r\n").await?;
    wtr.flush().await?;
    wtr.shutdown().await?;

    // Read the response.
    let mut body = String::new();
    rdr.read_to_string(&mut body).await?;
    Ok(())
}

async fn write_header_line(
    wtr: &mut BufWriter<OwnedWriteHalf>,
    header: &str,
    value: &str,
) -> Result<()> {
    wtr.write_all(header.as_bytes()).await?;
    wtr.write_all(b": ").await?;
    wtr.write_all(value.as_bytes()).await?;
    wtr.write_all(b"\r\n").await?;
    Ok(())
}

/// Invoke a CLI tool, passing the current trace information.
#[instrument]
async fn call_cli_tool() -> Result<()> {
    // Find our example CLI tool.
    let exe = current_exe()?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("expected executable to have a parent directory"))?;
    let cli_tracing_exe = exe_dir.join("cli-tracing");

    // Build and run our command.
    let status = Command::new(cli_tracing_exe)
        .envs(current_span_as_env())
        .status()
        .await?;
    if !status.success() {
        return Err(anyhow!("CLI tool failed: {:?}", status));
    }

    Ok(())
}
