use std::collections::HashMap;

use anyhow::{anyhow, Result};
use futures::Future;
use opinionated_tracing::{
    info_span, instrument, start_tracing, Instrument, WithExternalContext,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    start_tracing(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).await?;

    // A simple TCP server.
    let listener = TcpListener::bind("127.0.0.1:9321").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(log_error_wrapper(handle_request(socket)));
    }

    // If our server shuts down cleanly, we would also want to do this.
    //
    // end_tracing().await;
}

/// Wrap a future, logging any errors.
async fn log_error_wrapper<T>(fut: impl Future<Output = Result<T>>) {
    if let Err(err) = fut.await {
        tracing::error!(%err, "Error");
    }
}

/// Handle a single request.
async fn handle_request(socket: TcpStream) -> Result<()> {
    let (read_half, write_half) = socket.into_split();
    let mut rdr = BufReader::new(read_half);
    let mut wtr = BufWriter::new(write_half);

    // Get our headers. We don't allow headers to wrap over lines, unlike HTTP.
    let mut headers = HashMap::<String, String>::new();
    let mut line = String::new();
    loop {
        line.clear();
        let header_line = rdr.read_line(&mut line).await?;
        eprintln!("Header line: {:?}", line);
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

    // Create our span.
    let span =
        info_span!("server-tracing::handle_request").with_external_context(&headers);
    respond_to_request(&mut wtr).instrument(span).await
}

#[instrument(skip(wtr))]
async fn respond_to_request(wtr: &mut BufWriter<OwnedWriteHalf>) -> Result<()> {
    // Write our response.
    wtr.write_all(b"Hello, world!\n").await?;
    wtr.flush().await?;
    Ok(())
}
