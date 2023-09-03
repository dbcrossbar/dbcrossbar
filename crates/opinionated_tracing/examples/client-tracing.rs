use std::{collections::HashMap, env::current_exe};

use anyhow::{anyhow, Result};
use opinionated_tracing::{
    debug_span, inject_current_context, instrument, trace_with_parent_span_from_env,
    EnvInjector,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::tcp::OwnedWriteHalf,
    process::Command,
};

#[tokio::main]
async fn main() -> Result<()> {
    trace_with_parent_span_from_env(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        // Your root span. Add any fields you want.
        || debug_span!("client-tracing", version = env!("CARGO_PKG_VERSION")),
        // The body of the span.
        async {
            // Make some sample requests.
            make_request_to_server().await?;
            call_cli_tool().await
        },
    )
    .await
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
    let mut headers = HashMap::new();
    inject_current_context(&mut headers);
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
    // Find our example CLI tool and start building our command.
    let exe = current_exe()?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("expected executable to have a parent directory"))?;
    let cli_tracing_exe = exe_dir.join("cli-tracing");
    let mut cmd = Command::new(cli_tracing_exe);

    // Figure out what environment to pass to our child process.
    let mut injector = EnvInjector::new();
    inject_current_context(&mut injector);
    for name in injector.remove_from_env() {
        cmd.env_remove(name);
    }
    cmd.envs(injector.add_to_env());

    // Run our child process.
    cmd.status().await?;
    Ok(())
}
