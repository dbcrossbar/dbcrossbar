use std::{collections::HashMap, time::Instant};

use anyhow::{anyhow, Result};
use futures::Future;
use opinionated_telemetry::{
    describe_counter, describe_histogram, histogram, increment_counter, info_span,
    start_telemetry, AppType, Instrument, SetParentFromExtractor, Unit,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up all our telemetry.
    start_telemetry(
        AppType::Server,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    )
    .await?;

    // Declare any metrics.
    describe_counter!(
        "servertracing.request.count",
        "Number of requests handled by the server"
    );
    describe_histogram!(
        "servertracing.request.duration_seconds",
        Unit::Seconds,
        "Duration of requests handled by the server"
    );

    // Listen for incoming connections and dispatch them.
    let listener = TcpListener::bind("127.0.0.1:9321").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(log_error_wrapper(handle_request(socket)));
    }
}

/// Wrap a future, logging any errors.
async fn log_error_wrapper<T>(fut: impl Future<Output = Result<T>>) {
    if let Err(err) = fut.await {
        tracing::error!(%err, "Error");
    }
}

/// Handle a single request.
async fn handle_request(socket: TcpStream) -> Result<()> {
    let start_time = Instant::now();

    // Update a metric.
    increment_counter!("servertracing.request.count");

    // Figure out who we're talking to.
    let peer_addr = socket
        .peer_addr()
        .ok()
        .map(|addr| addr.to_string())
        .unwrap_or_default();

    // Split our socket into a reader and writer.
    let (read_half, write_half) = socket.into_split();
    let mut rdr = BufReader::new(read_half);
    let mut wtr = BufWriter::new(write_half);

    // Get our headers. We don't allow headers to wrap over lines, unlike HTTP.
    let mut headers = HashMap::<String, String>::new();
    let mut line = String::new();
    loop {
        line.clear();
        let header_line = rdr.read_line(&mut line).await?;
        //eprintln!("Header line: {:?}", line);
        let line = line.trim_end_matches(|c| c == '\r' || c == '\n');
        if line.is_empty() {
            break;
        }
        let (header, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("expected header line: {:?}", header_line))?;
        headers.insert(header.trim().to_string(), value.trim().to_string());
    }
    eprintln!("Headers: {:?}", headers);

    // Create our span and call an appropriate handler. In a web server, we
    // might name this span after the route (minus any IDs) like `"GET
    // /foo/{ID}"`.
    //
    // We use `set_parent_from_extractor` to either get an existing trace and
    // span from the headers, or start a new trace if none is present.
    let mut span = info_span!(
        "server-tracing::respond_to_request",
        protocol = "tcp",
        peer_addr = %peer_addr,
    );
    span.set_parent_from_extractor(&headers);
    let response = respond_to_request(&mut wtr).instrument(span).await;

    // Record elapsed time.
    histogram!(
        "servertracing.request.duration_seconds",
        start_time.elapsed().as_secs_f64()
    );

    response
}

/// Respond to a request.
///
/// We don't both with `#[instrument]` here, because the server request loop
/// knows more about the request than we do, and it can build a more informative
/// span.
async fn respond_to_request(wtr: &mut BufWriter<OwnedWriteHalf>) -> Result<()> {
    // Write our response.
    wtr.write_all(b"Hello, world!\n").await?;
    wtr.flush().await?;
    Ok(())
}
