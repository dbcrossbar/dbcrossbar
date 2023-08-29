//! Support for concatenating multiple CSV streams.

use tokio::sync::mpsc::Sender;

use crate::common::*;
use crate::tokio_glue::{bytes_channel, try_forward_to_sender};

/// Given a stream of CSV streams, merge them into a single CSV stream, removing
/// the headers from every CSV stream except the first.
///
/// This is a bit complicated because it needs to be asynchronous, and it tries
/// to impose near-zero overhead on the underlying data copies.
pub(crate) fn concatenate_csv_streams(
    ctx: Context,
    mut csv_streams: BoxStream<CsvStream>,
) -> Result<CsvStream> {
    // Create an asynchronous background worker to do the actual work.
    let (mut sender, receiver) = bytes_channel(1);
    let worker_ctx = ctx.clone();
    let worker = async move {
        let mut first = true;
        while let Some(result) = csv_streams.next().await {
            match result {
                Err(err) => {
                    error!("error reading stream of streams: {}", err,);
                    return send_err(sender, err).await;
                }
                Ok(csv_stream) => {
                    debug!("concatenating {}", csv_stream.name);
                    let mut data = csv_stream.data;

                    // If we're not the first CSV stream, remove the CSV header.
                    if first {
                        first = false;
                    } else {
                        data = strip_csv_header(worker_ctx.clone(), data)?;
                    }

                    // Forward the rest of the stream.
                    try_forward_to_sender(data, &mut sender).await?;
                }
            }
        }
        trace!("end of CSV streams");
        Ok(())
    }
    .instrument(debug_span!("concatenante_csv_streams"));

    // Build our combined `CsvStream`.
    let new_csv_stream = CsvStream {
        name: "combined".to_owned(),
        data: receiver.boxed(),
    };

    // Run the worker in the background, and return our combined stream.
    ctx.spawn_worker(worker.boxed());
    Ok(new_csv_stream)
}

#[tokio::test]
async fn concatenate_csv_streams_strips_all_but_first_header() {
    use tokio_stream::wrappers::ReceiverStream;

    let input_1 = b"a,b\n1,2\n";
    let input_2 = b"a,b\n3,4\n";
    let expected = b"a,b\n1,2\n3,4\n";

    let (ctx, worker_fut) = Context::create();

    let cmd_fut = async move {
        debug!("testing concatenate_csv_streams");

        // Build our `BoxStream<CsvStream>`.
        let (sender, receiver) = mpsc::channel::<Result<CsvStream>>(2);
        sender
            .send(Ok(CsvStream::from_bytes(&input_1[..]).await))
            .await
            .map_send_err()
            .unwrap();
        sender
            .send(Ok(CsvStream::from_bytes(&input_2[..]).await))
            .await
            .map_send_err()
            .unwrap();
        let csv_streams = ReceiverStream::new(receiver).boxed();

        // Close our sender so that our receiver knows we're done.
        drop(sender);

        // Test concatenation.
        let combined = concatenate_csv_streams(ctx.clone(), csv_streams)
            .unwrap()
            .into_bytes()
            .await
            .unwrap();
        assert_eq!(combined, &expected[..]);

        Ok(())
    };

    try_join!(cmd_fut, worker_fut).unwrap();
}

/// Remove the CSV header from a CSV stream, passing everything else through
/// untouched.
fn strip_csv_header(
    ctx: Context,
    mut stream: BoxStream<BytesMut>,
) -> Result<BoxStream<BytesMut>> {
    // Create an asynchronous background worker to do the actual work.
    let (mut sender, receiver) = bytes_channel(1);
    let worker = async move {
        // Accumulate bytes in this buffer until we see a full CSV header.
        let mut buffer: Option<BytesMut> = None;

        // Look for a full CSV header.
        while let Some(result) = stream.next().await {
            match result {
                Err(err) => {
                    error!("error reading stream: {}", err);
                    return send_err(sender, err).await;
                }
                Ok(bytes) => {
                    trace!("received {} bytes", bytes.len());
                    let mut new_buffer = if let Some(mut buffer) = buffer.take() {
                        buffer.extend_from_slice(&bytes);
                        buffer
                    } else {
                        bytes
                    };
                    match csv_header_length(&new_buffer) {
                        Ok(Some(header_len)) => {
                            trace!("stripping {} bytes of headers", header_len);
                            let _headers = new_buffer.split_to(header_len);
                            sender
                                .send(Ok(new_buffer))
                                .await
                                .context("broken pipe prevented sending data")?;
                            try_forward_to_sender(stream, &mut sender).await?;
                            return Ok(());
                        }
                        Ok(None) => {
                            // Save our buffer and keep looking for the end of
                            // the headers.
                            trace!(
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
        trace!("end of stream");
        let err = format_err!("end of CSV file while reading headers");
        send_err(sender, err).await
    }
    .instrument(debug_span!("strip_csv_header"));

    // Run the worker in the background, and return our receiver.
    ctx.spawn_worker(worker.boxed());
    Ok(receiver.boxed())
}

// Send `err` using `sender`.
async fn send_err(sender: Sender<Result<BytesMut>>, err: Error) -> Result<()> {
    sender
        .send(Err(err))
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
