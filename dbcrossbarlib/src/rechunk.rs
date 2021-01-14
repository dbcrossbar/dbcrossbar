//! Given a stream of streams CSV data, rechunk the stream sizes.

use futures::executor::block_on;
use std::{cell::Cell, cmp::min, io, rc::Rc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::common::*;
use crate::concat::concatenate_csv_streams;
use crate::tokio_glue::{SyncStreamReader, SyncStreamWriter};

/// Max buffer size for `csv::Writer`.
const MAX_CSV_BUFFER_SIZE: usize = 8 * (1 << 10);

/// Given a stream of streams CSV data, return another stream of CSV streams
/// where the CSV data is approximately `chunk_size` long whenever possible.
pub fn rechunk_csvs(
    ctx: Context,
    chunk_size: usize,
    streams: BoxStream<CsvStream>,
) -> Result<BoxStream<CsvStream>> {
    // Convert out input `BoxStream<CsvStream>` into a single, concatenated
    // synchronous `Read` object.
    let ctx = ctx.child(o!("streams_transform" => "rechunk_csvs"));
    let input_csv_stream = concatenate_csv_streams(ctx.clone(), streams)?;
    let csv_rdr = SyncStreamReader::new(ctx.clone(), input_csv_stream.data);

    // Create a channel to which we can write `CsvStream` values once we've
    // created them.
    let (csv_stream_sender, csv_stream_receiver) =
        mpsc::channel::<Result<CsvStream>>(1);

    // Run a synchronous background worker thread that parsers our sync CSV
    // `Read`er into a stream of `CsvStream`s.
    let worker_ctx = ctx.clone();
    let worker_fut = spawn_blocking(move || -> Result<()> {
        let mut rdr = csv::Reader::from_reader(csv_rdr);
        let hdr = rdr
            .byte_headers()
            .context("cannot read chunk header")?
            .to_owned();

        /// A single output chunk. The state we need to generate a single
        /// `CsvStream`.
        struct Chunk<W: Write> {
            /// Write to this to add data to the chunk.
            wtr: csv::Writer<W>,
            /// Approximately how much data have we written, not counting the
            /// buffer in `wtr`?
            total_written: Rc<Cell<usize>>,
            /// The `CsvStream` which will output the data produced by `wtr`.
            /// Once we publish this vaue to `csv_stream_sender`, we'll set the
            /// field `csv_stream` to `None`.
            csv_stream: Option<CsvStream>,
        }

        // What chunk number are we on? Used to give unique names to
        // `CsvStream`s.
        let mut chunk_id: usize = 0;

        // Construct a new `CsvStream`, and return a `Chunk` with a
        // `csv::Writer` which can be used to write data to it.
        let mut new_chunk = || -> Result<Chunk<_>> {
            chunk_id = chunk_id.checked_add(1).expect("too many chunks");
            trace!(worker_ctx.log(), "starting new CSV chunk {}", chunk_id);

            // Build a `CsvStream` that we can write to synchronously using
            // `wtr`. Here, `wtr` is a synchronous `Write` implementation,
            // and `data` is an `impl Stream<Item = BytesMut, ..>`.
            let (wtr, data) = SyncStreamWriter::pipe(worker_ctx.clone());
            let csv_stream = CsvStream {
                name: format!("chunk_{:04}", chunk_id),
                data: data.boxed(),
            };

            // Keep rough track of how many bytes we've written.
            let wtr = CountingWriter::new(wtr);
            let total_written = wtr.total_written();

            // Now, make a `csv::Writer` we can write to. We limit our buffer
            // size so that `chunk_size` is vaguely accurate.
            let wtr = csv::WriterBuilder::default()
                .buffer_capacity(min(MAX_CSV_BUFFER_SIZE, chunk_size))
                .from_writer(wtr);
            Ok(Chunk {
                wtr,
                total_written,
                csv_stream: Some(csv_stream),
            })
        };
        let mut chunk = new_chunk()?;

        let mut row = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut row).context("cannot read row")? {
            // If this is the first row we've seen, we can safely send our
            // `CsvStream` to our `csv_stream_sender: BoxStream<CsvStream>`. We
            // do this before writing any data, including the headers, so that
            // somebody hooks up a consumer and prevents `chunk.wtr` from
            // blocking forever.
            if let Some(csv_stream) = chunk.csv_stream.take() {
                block_on(csv_stream_sender.send(Ok(csv_stream))).map_send_err()?;

                // Now that we potentially have a consumer, we can safely write our
                // headers.
                chunk
                    .wtr
                    .write_byte_record(&hdr)
                    .context("cannot write chunk header")?;
            }

            // Write our row.
            chunk
                .wtr
                .write_byte_record(&row)
                .context("cannot write row")?;

            // If total written exceeds chunk size, then start a new chunk.
            if chunk.total_written.get() >= chunk_size {
                trace!(worker_ctx.log(), "finishing chunk");
                chunk = new_chunk()?;
            }
        }
        trace!(worker_ctx.log(), "finished rechunking CSV data");
        Ok(())
    });
    ctx.spawn_worker(worker_fut.boxed());

    let csv_streams = ReceiverStream::new(csv_stream_receiver).boxed();
    Ok(csv_streams)
}

#[test]
fn rechunk_csvs_honors_chunk_size() {
    use std::str;

    let inputs: &[&[u8]] = &[b"a,b\n1,1\n2,1\n", b"a,b\n1,2\n2,2\n"];
    let expected: &[&[u8]] =
        &[b"a,b\n1,1\n", b"a,b\n2,1\n", b"a,b\n1,2\n", b"a,b\n2,2\n"];

    let (ctx, worker_fut) = Context::create_for_test("rechunk_csvs");

    let cmd_fut = async move {
        debug!(ctx.log(), "testing rechunk_csvs");

        // Build our `BoxStream<CsvStream>`.
        let (sender, receiver) = mpsc::channel::<Result<CsvStream>>(2);
        for &input in inputs {
            sender
                .send(Ok(CsvStream::from_bytes(input).await))
                .await
                .map_send_err()
                .expect("could not write to stream");
        }
        drop(sender);
        let csv_streams = ReceiverStream::new(receiver).boxed();

        let rechunked_csv_streams = rechunk_csvs(ctx.clone(), 7, csv_streams).unwrap();

        let outputs = rechunked_csv_streams
            // We need to use `map` here (and handle both `Ok` and `Err`)
            // instead of using `map_ok`, because we're going to call
            // `buffered`, and there's no `try_buffered`. In real code, we'd be
            // using `try_buffer_unordered`, which would allow use to use `map`.
            .map(move |csv_stream_result| -> BoxFuture<_> {
                match csv_stream_result {
                    Ok(csv_stream) => {
                        let ctx = ctx.clone();
                        async move {
                            let bytes =
                                csv_stream.into_bytes(ctx.clone()).await.unwrap();
                            trace!(
                                ctx.log(),
                                "collected CSV stream: {:?}",
                                str::from_utf8(&bytes[..]).unwrap()
                            );
                            Ok(bytes)
                        }
                        .boxed()
                    }
                    Err(err) => async { Err(err) }.boxed(),
                }
            })
            .buffered(4)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(outputs.len(), expected.len());
        for (output, &expected) in outputs.into_iter().zip(expected) {
            assert_eq!(output, expected);
        }

        Ok(())
    };

    run_futures_with_runtime(cmd_fut.boxed(), worker_fut).unwrap();
}

/// A `Write` implementation that keeps track of how much data has been written
/// so far. Note that if you wrap this in a buffered type like `csv::Writer`, it
/// won't keep track of the data in `csv::Writer`'s buffer, only the data that
/// has been flushed.
struct CountingWriter<W: Write> {
    /// Our writer.
    inner: W,
    /// The total data that we've written. This is wrapped in `Rc<Cell<_>>` so
    /// that is can be easily accessed from anywhere in the same thread even if
    /// the `CountingWriter` is completely owned by another type such as
    /// `csv::Writer`.
    total_written: Rc<Cell<usize>>,
}

impl<W: Write> CountingWriter<W> {
    /// Create a new `CountingWriter` that wraps `inner`.
    fn new(inner: W) -> Self {
        Self {
            inner,
            total_written: Rc::new(Cell::new(0)),
        }
    }

    /// How much data has been written? This returns an `Rc<Cell<_>>` that will
    /// be updated by this `CountingWriter`. Setting the value in this `Cell`
    /// directly may result in future reads returning an unspecified value.
    fn total_written(&self) -> Rc<Cell<usize>> {
        self.total_written.clone()
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        let total_written = self.total_written.get() + written;
        self.total_written.set(total_written);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
