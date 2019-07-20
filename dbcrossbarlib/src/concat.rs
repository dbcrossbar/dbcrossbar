//! Support for concatenating multiple CSV streams.

use tokio::sync::mpsc::Sender;

use crate::common::*;
use crate::tokio_glue::bytes_channel;

/// Given a stream of CSV streams, merge them into a single CSV stream, removing
/// the headers from every CSV stream except the first.
///
/// This is a bit complicated because it needs to be asynchronous, and it tries
/// to impose near-zero overhead on the underlying data copies.
#[allow(dead_code)]
pub(crate) fn concatenate_csv_streams(
    ctx: Context,
    mut csv_streams: BoxStream<CsvStream>,
) -> Result<CsvStream> {
    // Create an asynchronous background worker to do the actual work.
    let (mut sender, receiver) = bytes_channel(1);
    let worker_ctx = ctx.child(o!("streams_transform" => "concatenate_csv_streams"));
    let worker = async move {
        let mut first = true;
        loop {
            match csv_streams.into_future().compat().await {
                Err((err, _rest_of_csv_streams)) => {
                    error!(
                        worker_ctx.log(),
                        "error reading stream of streams: {}", err,
                    );
                    return send_err(sender, err).await;
                }
                Ok((None, _rest_of_csv_streams)) => {
                    trace!(worker_ctx.log(), "end of CSV streams");
                    return Ok(());
                }
                Ok((Some(csv_stream), rest_of_csv_streams)) => {
                    csv_streams = rest_of_csv_streams;
                    debug!(worker_ctx.log(), "concatenating {}", csv_stream.name);
                    let mut data = csv_stream.data;

                    // If we're not the first CSV stream, remove the CSV header.
                    if first {
                        first = false;
                    } else {
                        data = strip_csv_header(worker_ctx.clone(), data)?;
                    }

                    // Forward the rest of the stream.
                    sender = forward_stream(worker_ctx.clone(), data, sender).await?;
                }
            }
        }
    };

    // Build our combined `CsvStream`.
    let new_csv_stream = CsvStream {
        name: "combined".to_owned(),
        data: Box::new(receiver) as BoxStream<BytesMut>,
    };

    // Run the worker in the background, and return our combined stream.
    ctx.spawn_worker(worker.boxed().compat());
    Ok(new_csv_stream)
}

#[test]
fn concatenate_csv_streams_strips_all_but_first_header() {
    use tokio::sync::mpsc;

    let input_1 = b"a,b\n1,2\n";
    let input_2 = b"a,b\n3,4\n";
    let expected = b"a,b\n1,2\n3,4\n";

    let (ctx, worker_fut) = Context::create_for_test("concatenate_csv_streams");

    let cmd_fut = async move {
        debug!(ctx.log(), "testing");

        // Build our `BoxStream<CsvStream>`.
        let (mut sender, receiver) = mpsc::channel(2);
        sender = sender
            .send(CsvStream::from_bytes(&input_1[..]).await)
            .compat()
            .await
            .unwrap();
        sender
            .send(CsvStream::from_bytes(&input_2[..]).await)
            .compat()
            .await
            .unwrap();
        let csv_streams =
            Box::new(receiver.map_err(|e| e.into())) as BoxStream<CsvStream>;

        // Test concatenation.
        let combined = concatenate_csv_streams(ctx.clone(), csv_streams)
            .unwrap()
            .into_bytes(ctx)
            .await
            .unwrap();
        assert_eq!(combined, &expected[..]);

        Ok(())
    };

    run_futures_with_runtime(cmd_fut.boxed(), worker_fut).unwrap();
}

/// Remove the CSV header from a CSV stream, passing everything else through
/// untouched.
fn strip_csv_header(
    ctx: Context,
    mut stream: BoxStream<BytesMut>,
) -> Result<BoxStream<BytesMut>> {
    // Create an asynchronous background worker to do the actual work.
    let (mut sender, receiver) = bytes_channel(1);
    let worker_ctx = ctx.child(o!("transform" => "strip_csv_header"));
    let worker = async move {
        // Accumulate bytes in this buffer until we see a full CSV header.
        let mut buffer: Option<BytesMut> = None;

        // Look for a full CSV header.
        loop {
            match stream.into_future().compat().await {
                Err((err, _rest_of_stream)) => {
                    error!(worker_ctx.log(), "error reading stream: {}", err);
                    return send_err(sender, err).await;
                }
                Ok((None, _rest_of_stream)) => {
                    trace!(worker_ctx.log(), "end of stream");
                    return send_err(
                        sender,
                        format_err!("end of CSV file while reading headers"),
                    )
                    .await;
                }
                Ok((Some(bytes), rest_of_stream)) => {
                    stream = rest_of_stream;
                    trace!(worker_ctx.log(), "received {} bytes", bytes.len());
                    let mut new_buffer = if let Some(mut buffer) = buffer.take() {
                        buffer.extend_from_slice(&bytes);
                        buffer
                    } else {
                        bytes
                    };
                    match csv_header_length(&new_buffer) {
                        Ok(Some(header_len)) => {
                            trace!(
                                worker_ctx.log(),
                                "stripping {} bytes of headers",
                                header_len
                            );
                            let _headers = new_buffer.split_to(header_len);
                            sender = sender
                                .send(Ok(new_buffer))
                                .compat()
                                .await
                                .context("broken pipe prevented sending data")?;
                            break;
                        }
                        Ok(None) => {
                            // Save our buffer and keep looking for the end of
                            // the headers.
                            trace!(
                                worker_ctx.log(),
                                "didn't find full headers in {} bytes, looking...",
                                new_buffer.len(),
                            );
                            buffer = Some(new_buffer);
                        }
                        Err(err) => {
                            return send_err(sender, err).await;
                        }
                    }
                }
            }
        }
        let _sender = forward_stream(worker_ctx.clone(), stream, sender).await?;
        Ok(())
    };

    // Run the worker in the background, and return our receiver.
    ctx.spawn_worker(worker.boxed().compat());
    Ok(Box::new(receiver) as BoxStream<BytesMut>)
}

/// Forward `stream` to `sender`, and return `sender`. If an error occurs while
/// forwarding, it will be forwarded to `sender` (if possible), and this
/// function will return an error.
async fn forward_stream(
    ctx: Context,
    stream: BoxStream<BytesMut>,
    sender: Sender<Result<BytesMut>>,
) -> Result<Sender<Result<BytesMut>>> {
    trace!(ctx.log(), "forwarding byte stream");
    let err_sender = sender.clone();
    match stream.map(Ok).forward(sender).compat().await {
        // We successfully consumed `stream`, so just return `sender`.
        Ok((_stream, sender)) => Ok(sender),
        // We failed while forwarding data.
        Err(err) => {
            error!(ctx.log(), "error while forwarding byte stream: {}", err);
            let local_err = format_err!("error forwarding stream");
            send_err(err_sender, err).await?;
            Err(local_err)
        }
    }
}

// Send `err` using `sender`.
async fn send_err(sender: Sender<Result<BytesMut>>, err: Error) -> Result<()> {
    sender
        .send(Err(err))
        .compat()
        .await
        .context("broken pipe prevented sending error")?;
    Ok(())
}

/// Given a slice of bytes, determine if it contains a complete set of CSV
/// headers, and if so, return their length.
fn csv_header_length(data: &[u8]) -> Result<Option<usize>> {
    // We could try to use the `csv` crate for this, but the `csv` crate will
    // go to great lengths to recover from malformed CSV files, so it's not
    // very useful for detecting whether we have a complete header line.
    if let Some(pos) = data.iter().position(|b| *b == b'\n') {
        if data[..pos].iter().any(|b| *b == b'"') {
            Err(format_err!(
                "cannot yet concatenate CSV streams with quoted headers"
            ))
        } else {
            Ok(Some(pos + 1))
        }
    } else {
        Ok(None)
    }
}

#[test]
fn csv_header_length_handles_corner_cases() {
    assert_eq!(csv_header_length(b"").unwrap(), None);
    assert_eq!(csv_header_length(b"a,b,c").unwrap(), None);
    assert_eq!(csv_header_length(b"a,b,c\n").unwrap(), Some(6));
    assert_eq!(csv_header_length(b"a,b,c\nd,e,f\n").unwrap(), Some(6));
    assert_eq!(csv_header_length(b"a,b,c\r\n").unwrap(), Some(7));

    // If we wanted to be more clever, we could handle quoted headers with
    // embedded newlines, and other such complications.
    assert!(csv_header_length(b"a,\"\n\",c\n").is_err());
    //assert_eq!(csv_header_length(b"a,\"\na").unwrap(), None);
    //assert_eq!(csv_header_length(b"a,\"\n\",c\n").unwrap(), Some(8));
}
