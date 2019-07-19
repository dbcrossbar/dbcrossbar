//! Glue code for working with `tokio`'s async I/O.
//!
//! This is mostly smaller things that happen to recur in our particular
//! application.

use std::{cmp::min, future::Future as StdFuture, pin::Pin, thread};
use tokio::io;

use crate::common::*;

/// Standard future type for this library. Like `Result`, but used by async. We
/// mark it as `Send` to ensure it can be sent between threads safely (even when
/// blocked on `.await`!), and we `Pin<Box<...>>` it using `.boxed()` to make it
/// an abstract, heap-based type, for convenience. All we know is that it will
/// return a `Result<T>`.
pub type BoxFuture<T> = Pin<Box<dyn StdFuture<Output = Result<T>> + Send + 'static>>;

/// A stream of values of type `T`, using our standard error type, and imposing
/// enough restrictions to be able send streams between threads.
pub type BoxStream<T> = Box<dyn Stream<Item = T, Error = Error> + Send + 'static>;

/// Given a `Stream` of data chunks of type `BytesMut`, write the entire stream
/// to an `AsyncWrite` implementation.
pub(crate) async fn copy_stream_to_writer<S, W>(
    ctx: Context,
    mut stream: S,
    mut wtr: W,
) -> Result<()>
where
    S: Stream<Item = BytesMut, Error = Error> + 'static,
    W: AsyncWrite + 'static,
{
    loop {
        match stream.into_future().compat().await {
            Err((err, _rest_of_stream)) => {
                error!(ctx.log(), "error reading stream: {}", err);
                return Err(err);
            }
            Ok((None, _rest_of_stream)) => {
                trace!(ctx.log(), "end of stream");
                return Ok(());
            }
            Ok((Some(bytes), rest_of_stream)) => {
                stream = rest_of_stream;
                trace!(ctx.log(), "writing {} bytes", bytes.len());
                io::write_all(&mut wtr, bytes).compat().await.map_err(|e| {
                    error!(ctx.log(), "write error: {}", e);
                    format_err!("error writing data: {}", e)
                })?;
            }
        }
    }
}

/// Given an `AsyncRead` implement, copy it to a stream `Stream` of data chunks
/// of type `BytesMut`. Returns the stream.
pub(crate) fn copy_reader_to_stream<R>(
    ctx: Context,
    mut rdr: R,
) -> Result<impl Stream<Item = BytesMut, Error = Error> + Send + 'static>
where
    R: AsyncRead + Send + 'static,
{
    let (mut sender, receiver) = mpsc::channel(1);
    let worker = async move {
        let mut buffer = vec![0; 64 * 1024];
        loop {
            // Read the data. This consumes `rdr`, so we'll have to put it back
            // below.
            match io::read(rdr, &mut buffer).compat().await {
                Err(err) => {
                    let nice_err = format_err!("stream read error: {}", err);
                    error!(ctx.log(), "{}", nice_err);
                    if sender.send(Err(nice_err)).compat().await.is_err() {
                        error!(
                            ctx.log(),
                            "broken pipe prevented sending error: {}", err
                        );
                    }
                    return Ok(());
                }
                Ok((new_rdr, data, count)) => {
                    if count == 0 {
                        trace!(ctx.log(), "done copying AsyncRead to stream");
                        return Ok(());
                    }

                    // Put back our reader.
                    rdr = new_rdr;

                    // Copy our bytes into a `BytesMut`, and send it. This consumes
                    // `sender`, so we'll have to put it back below.
                    let bytes = BytesMut::from(&data[..count]);
                    trace!(ctx.log(), "sending {} bytes to stream", bytes.len());
                    match sender.send(Ok(bytes)).wait() {
                        Ok(new_sender) => {
                            sender = new_sender;
                        }
                        Err(_err) => {
                            error!(
                                ctx.log(),
                                "broken pipe forwarding async data to stream"
                            );
                            return Ok(());
                        }
                    }
                }
            }
        }
    };
    tokio::spawn(worker.boxed().compat());

    let receiver = receiver
        // Change `Error` from `mpsc::Error` to our standard `Error`.
        .map_err(|_| format_err!("stream read error"))
        // Change `Item` from `Result<BytesMut>` to `BytesMut`, pushing
        // the error into the stream's `Error` channel instead.
        .and_then(|result| result);

    Ok(receiver)
}

/// Provides a synchronous `Write` interface that copies data to an async
/// `Stream<BytesMut>`.
pub(crate) struct SyncStreamWriter {
    /// Context used for logging.
    ctx: Context,
    /// The sender end of our pipe. If this is `None`, our receiver disappeared
    /// unexpectedly and we have nobody to pipe to, so return
    /// `io::ErrorKind::BrokenPipe` (analogous to `EPIPE` or `SIGPIPE` for Unix
    /// CLI tools).
    sender: Option<mpsc::Sender<Result<BytesMut>>>,
}

impl SyncStreamWriter {
    /// Create a new `SyncStreamWriter` and a receiver that implements
    /// `Stream<Item = BytesMut, Error = Error>`.
    pub fn pipe(ctx: Context) -> (Self, impl Stream<Item = BytesMut, Error = Error>) {
        let (sender, receiver) = mpsc::channel(1);
        (
            SyncStreamWriter {
                ctx,
                sender: Some(sender),
            },
            receiver
                // Change `Error` from `mpsc::Error` to our standard `Error`.
                .map_err(|_| format_err!("stream read error"))
                // Change `Item` from `Result<BytesMut>` to `BytesMut`, pushing
                // the error into the stream's `Error` channel instead.
                .and_then(|result| result),
        )
    }
}

impl SyncStreamWriter {
    /// Send an error to our stream.
    #[allow(dead_code)]
    pub(crate) fn send_error(&mut self, err: Error) -> io::Result<()> {
        debug!(self.ctx.log(), "sending error: {}", err);
        if let Some(sender) = self.sender.take() {
            match sender.send(Err(err)).wait() {
                Ok(sender) => {
                    self.sender = Some(sender);
                    Ok(())
                }
                Err(_err) => Err(io::ErrorKind::BrokenPipe.into()),
            }
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
}

impl Write for SyncStreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!(self.ctx.log(), "sending {} bytes", buf.len());
        if let Some(sender) = self.sender.take() {
            match sender.send(Ok(BytesMut::from(buf))).wait() {
                Ok(sender) => {
                    self.sender = Some(sender);
                    Ok(buf.len())
                }
                Err(_err) => Err(io::ErrorKind::BrokenPipe.into()),
            }
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!(self.ctx.log(), "flushing");
        if let Some(sender) = self.sender.take() {
            match sender.flush().wait() {
                Ok(sender) => {
                    self.sender = Some(sender);
                    Ok(())
                }
                Err(_err) => Err(io::ErrorKind::BrokenPipe.into()),
            }
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
}

/// Provides a synchronous `Read` interface that receives data from an async
/// `Stream<BytesMut>`.
pub(crate) struct SyncStreamReader {
    ctx: Context,
    stream: Option<BoxStream<BytesMut>>,
    buffer: BytesMut,
}

impl SyncStreamReader {
    /// Create a new `SyncStreamReader` from a stream of bytes.
    pub(crate) fn new(ctx: Context, stream: BoxStream<BytesMut>) -> Self {
        Self {
            ctx,
            stream: Some(stream),
            buffer: BytesMut::default(),
        }
    }
}

impl Read for SyncStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Assume no zero-sized reads for now.
        assert!(!buf.is_empty());

        // We have no bytes to return, so try to read some from our stream.
        if self.buffer.is_empty() {
            // Try to take the stream stored in this object.
            if let Some(stream) = self.stream.take() {
                match stream.into_future().wait() {
                    // End of the stream.
                    Ok((None, _rest_of_stream)) => {
                        trace!(self.ctx.log(), "end of stream");
                        return Ok(0);
                    }
                    // A bytes buffer.
                    Ok((Some(bytes), rest_of_stream)) => {
                        // Put the stream back into the object.
                        self.stream = Some(rest_of_stream);
                        trace!(
                            self.ctx.log(),
                            "read {} bytes from stream",
                            bytes.len()
                        );
                        assert!(!bytes.is_empty());
                        self.buffer = bytes;
                    }
                    // An error on the stream.
                    Err((err, _rest_of_stream)) => {
                        error!(self.ctx.log(), "error reading from stream: {}", err);
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            Box::new(err.compat()),
                        ));
                    }
                }
            } else {
                // This happens once we've already returned either 0 bytes
                // (marking the end of the stream) or an error, but somebody is
                // still trying to read, so we'll just return 0 bytes
                // indefinitely.
                trace!(self.ctx.log(), "stream is already closed");
                return Ok(0);
            }
        }

        // We know we have bytes, so copy them into our output buffer.
        assert!(!self.buffer.is_empty());
        let count = min(self.buffer.len(), buf.len());
        buf[..count].copy_from_slice(&self.buffer.split_to(count));
        trace!(self.ctx.log(), "read returned {} bytes", count);
        Ok(count)
    }
}

/// Given a `value`, create a boxed stream which returns just that single value.
pub(crate) fn box_stream_once<T>(value: Result<T>) -> BoxStream<T>
where
    T: Send + 'static,
{
    Box::new(stream::once(value))
}

/// Run a synchronous function `f` in a background worker thread and return its
/// value.
pub(crate) async fn run_sync_fn_in_background<F, T>(
    thread_name: String,
    f: F,
) -> Result<T>
where
    F: (FnOnce() -> Result<T>) + Send + 'static,
    T: Send + 'static,
{
    // Spawn a worker thread outside our thread pool to do the actual work.
    let (sender, receiver) = mpsc::channel(1);
    let thr = thread::Builder::new().name(thread_name);
    let handle = thr
        .spawn(move || {
            sender.send(f()).wait().expect(
                "should always be able to send results from background thread",
            );
        })
        .context("could not spawn thread")?;

    // Wait for our worker to report its results.
    let background_result = receiver.into_future().compat().await;
    let result = match background_result {
        // The background thread sent an `Ok`.
        Ok((Some(Ok(value)), _receiver)) => Ok(value),
        // The background thread sent an `Err`.
        Ok((Some(Err(err)), _receiver)) => Err(err),
        // The background thread exitted without sending anything. This
        // shouldn't happen.
        Ok((None, _receiver)) => {
            unreachable!("background thread did not send any results");
        }
        // We couldn't read a result from the background thread, probably
        // because it panicked.
        Err(_) => Err(format_err!("background thread panicked")),
    };

    // Block until our worker exits. This is a synchronous block in an
    // asynchronous task, but the background worker already reported its result,
    // so the wait should be short.
    handle.join().expect("background worker thread panicked");
    result
}
