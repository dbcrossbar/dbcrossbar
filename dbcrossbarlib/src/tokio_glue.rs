//! Glue code for working with `tokio`'s async I/O.
//!
//! This is mostly smaller things that happen to recur in our particular
//! application.

use bytes::Bytes;
use futures::{
    self, executor::block_on, stream, Sink, SinkExt, TryStream, TryStreamExt,
};
use std::{cmp::min, error, fmt, panic, pin::Pin, result};
use tokio::{io, process::Child, sync::mpsc, task};
use tokio_stream::wrappers::ReceiverStream;

use crate::common::*;

/// Standard future type for this library. Like `Result`, but used by async. We
/// mark it as `Send` to ensure it can be sent between threads safely (even when
/// blocked on `.await`!), and we `Pin<Box<...>>` it using `.boxed()` to make it
/// an abstract, heap-based type, for convenience. All we know is that it will
/// return a `Result<T>`.
pub type BoxFuture<T, E = Error> = futures::future::BoxFuture<'static, Result<T, E>>;

/// A stream of values of type `T`, using our standard error type, and imposing
/// enough restrictions to be able send streams between threads.
pub type BoxStream<T, E = Error> = futures::stream::BoxStream<'static, Result<T, E>>;

/// Extension for `BoxStream<BoxFuture<()>>`.
pub trait ConsumeWithParallelism<T>: Sized {
    /// Consume futures from the stream, running `parallelism` futures at any
    /// given time.
    fn consume_with_parallelism(self, parallelism: usize) -> BoxFuture<Vec<T>>;
}

impl<T: Send + Sized + 'static> ConsumeWithParallelism<T> for BoxStream<BoxFuture<T>> {
    fn consume_with_parallelism(self, parallelism: usize) -> BoxFuture<Vec<T>> {
        self
            // Run up to `parallelism` futures in parallel.
            .try_buffer_unordered(parallelism)
            // Collect our resulting zero-byte `()` values as a zero-byte
            // vector.
            .try_collect::<Vec<T>>()
            // This `boxed` is needed to prevent weird lifetime issues from
            // seeping into the type of this function and its callers.
            .boxed()
    }
}

/// Create a new channel with an output end of type `BoxStream<BytesMut>`.
pub(crate) fn bytes_channel(
    buffer: usize,
) -> (
    mpsc::Sender<Result<BytesMut>>,
    impl Stream<Item = Result<BytesMut>> + Send + Unpin + 'static,
) {
    let (sender, receiver) = mpsc::channel(buffer);
    (sender, ReceiverStream::new(receiver))
}

/// Copy `stream` into `sink`. If `stream` returns an `Err` value, stop
/// immediately.
///
/// This is basically similar to [`futures::StreamExt::forward`], except that we
/// return an error of type `Error`, and not of type `<Si as Sink>::Error`,
/// which makes things more flexible.
pub async fn try_forward<T, St, Si>(
    ctx: &Context,
    mut stream: St,
    mut sink: Si,
) -> Result<()>
where
    St: Stream<Item = Result<T>> + Unpin,
    Si: Sink<T> + Unpin,
    Error: From<Si::Error>,
{
    trace!(ctx.log(), "forwarding stream to sink");
    while let Some(result) = stream.next().await {
        match result {
            Ok(value) => sink
                .send(value)
                .await
                .map_err(Error::from)
                .context("error sending value to sink")?,
            Err(err) => {
                return Err(err.context("error reading from stream").into());
            }
        }
    }
    sink.close()
        .await
        .map_err(Error::from)
        .context("error sending value to sink")?;
    trace!(ctx.log(), "done forwarding stream to sink");
    Ok(())
}

/// Copy `stream` into `sender`. If `stream` returns an `Err` value, forward it
/// to `sender` and stop copying.
///
/// In `tokio` 0.1, `Sender` implemented `Sink`, but apparently that's not a
/// thing anymore.
pub(crate) async fn try_forward_to_sender<T, St>(
    ctx: &Context,
    mut stream: St,
    sender: &mut mpsc::Sender<Result<T>>,
) -> Result<()>
where
    T: Send,
    St: Stream<Item = Result<T>> + Unpin,
{
    trace!(ctx.log(), "forwarding stream to sender");
    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => sender.send(Ok(bytes)).await.map_send_err()?,
            Err(err) => {
                let ret_err = format_err!("error reading from stream: {}", err);
                sender.send(Err(err)).await.map_err(|_| {
                    format_err!("could not forward error to sender: {}", ret_err)
                })?;
                return Err(ret_err);
            }
        }
    }
    trace!(ctx.log(), "done forwarding stream to sender");
    Ok(())
}

/// Given a `Stream` of data chunks of type `BytesMut`, write the entire stream
/// to an `AsyncWrite` implementation.
pub(crate) async fn copy_stream_to_writer<S, W>(
    ctx: Context,
    mut stream: S,
    mut wtr: W,
) -> Result<()>
where
    S: Stream<Item = Result<BytesMut>> + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    trace!(ctx.log(), "begin copy_stream_to_writer");
    while let Some(result) = stream.next().await {
        match result {
            Err(err) => {
                error!(ctx.log(), "error reading stream: {}", err);
                return Err(err);
            }
            Ok(bytes) => {
                trace!(ctx.log(), "writing {} bytes", bytes.len());
                wtr.write_all(&bytes).await.map_err(|e| {
                    error!(ctx.log(), "write error: {}", e);
                    format_err!("error writing data: {}", e)
                })?;
                trace!(ctx.log(), "wrote to writer");
            }
        }
    }
    wtr.flush().await?;
    trace!(ctx.log(), "end copy_stream_to_writer");
    Ok(())
}

/// Given an `AsyncRead` implement, copy it to a stream `Stream` of data chunks
/// of type `BytesMut`. Returns the stream.
pub(crate) fn copy_reader_to_stream<R>(
    ctx: Context,
    mut rdr: R,
) -> Result<impl Stream<Item = Result<BytesMut>> + Send + 'static>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let (sender, receiver) = bytes_channel(1);
    let worker: BoxFuture<()> = async move {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            // Read the data. This consumes `rdr`, so we'll have to put it back
            // below.
            trace!(ctx.log(), "reading bytes from reader");
            match rdr.read(&mut buffer).await {
                Err(err) => {
                    let nice_err = format_err!("read error: {}", err);
                    error!(ctx.log(), "{}", nice_err);
                    if sender.send(Err(nice_err)).await.is_err() {
                        error!(
                            ctx.log(),
                            "broken pipe prevented sending error: {}", err
                        );
                    }
                    return Ok(());
                }
                Ok(count) => {
                    if count == 0 {
                        trace!(ctx.log(), "done copying AsyncRead to stream");
                        return Ok(());
                    }

                    // Copy our bytes into a `BytesMut`, and send it. This consumes
                    // `sender`, so we'll have to put it back below.
                    let bytes = BytesMut::from(&buffer[..count]);
                    trace!(ctx.log(), "sending {} bytes to stream", bytes.len());
                    match sender.send(Ok(bytes)).await {
                        Ok(()) => {
                            trace!(ctx.log(), "sent bytes to stream");
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
    }
    .boxed();
    tokio::spawn(worker);
    Ok(receiver)
}

/// Provides a synchronous `Write` interface that copies data to an async
/// `Stream<BytesMut>`.
pub(crate) struct SyncStreamWriter {
    /// Context used for logging.
    ctx: Context,
    /// The sender end of our pipe.
    sender: mpsc::Sender<Result<BytesMut>>,
}

impl SyncStreamWriter {
    /// Create a new `SyncStreamWriter` and a receiver that implements
    /// `Stream<Item = BytesMut, Error = Error>`.
    pub fn pipe(
        ctx: Context,
    ) -> (Self, impl Stream<Item = Result<BytesMut>> + Send + 'static) {
        let (sender, receiver) = bytes_channel(1);
        (SyncStreamWriter { ctx, sender }, receiver)
    }
}

impl SyncStreamWriter {
    /// Send an error to our stream.
    #[allow(dead_code)]
    pub(crate) fn send_error(&mut self, err: Error) -> io::Result<()> {
        debug!(self.ctx.log(), "sending error: {}", err);
        block_on(self.sender.send(Err(err)))
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }
}

impl Write for SyncStreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!(self.ctx.log(), "sending {} bytes", buf.len());
        block_on(self.sender.send(Ok(BytesMut::from(buf))))
            .map_err(|_| -> io::Error { io::ErrorKind::BrokenPipe.into() })?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // There's nothing we can actually do here as of `tokio` 0.2, so just
        // ignore `flush`.
        trace!(self.ctx.log(), "pretending to flush to an async sender");
        Ok(())
    }
}

/// Provides a synchronous `Read` interface that receives data from an async
/// `Stream<BytesMut>`.
pub(crate) struct SyncStreamReader {
    ctx: Context,
    stream: stream::Fuse<BoxStream<BytesMut>>,
    seen_error: bool,
    buffer: BytesMut,
}

impl SyncStreamReader {
    /// Create a new `SyncStreamReader` from a stream of bytes.
    pub(crate) fn new(ctx: Context, stream: BoxStream<BytesMut>) -> Self {
        Self {
            ctx,
            // "Fuse" our stream so that once it returns none, it will always
            // return none.
            stream: stream.fuse(),
            seen_error: false,
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
            if self.seen_error {
                // If we've already errored once, keep doing it. This is probably paranoid.
                error!(self.ctx.log(), "tried to read from stream after error");
                return Err(io::ErrorKind::Other.into());
            }
            match block_on(self.stream.next()) {
                // End of the stream.
                None => {
                    trace!(self.ctx.log(), "end of stream");
                    return Ok(0);
                }
                // A bytes buffer.
                Some(Ok(bytes)) => {
                    trace!(self.ctx.log(), "read {} bytes from stream", bytes.len());
                    assert!(!bytes.is_empty());
                    self.buffer = bytes;
                }
                // An error on the stream.
                Some(Err(err)) => {
                    error!(self.ctx.log(), "error reading from stream: {}", err);
                    self.seen_error = true;
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        Box::new(err.compat()),
                    ));
                }
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
    stream::once(async { value }).boxed()
}

/// Run a synchronous function `f` in a background worker thread and return its
/// value.
pub async fn spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: (FnOnce() -> Result<T>) + Send + 'static,
    T: Send + 'static,
{
    match task::spawn_blocking(f).await {
        Ok(f_result) => f_result,
        Err(join_err) => match join_err.try_into_panic() {
            Ok(panic_value) => panic::resume_unwind(panic_value),
            Err(join_err) => {
                Err(format_err!("background thread failed: {}", join_err))
            }
        },
    }
}

/// Create a new `tokio` runtime and use it to run `cmd_future` (which carries
/// out whatever task we want to perform), and `worker_future` (which should
/// have been created by `Context::create` or `Context::create_for_test`).
///
/// Return when at least one future has failed, or both futures have completed
/// successfully.
///
/// This can be safely used from within a test, but it may only be called from a
/// synchronous context.
///
/// If this hangs, make sure all `Context` values are getting dropped once the
/// work is done.
pub fn run_futures_with_runtime(
    cmd_future: BoxFuture<()>,
    worker_future: BoxFuture<()>,
) -> Result<()> {
    // Wait for both `cmd_fut` and `copy_fut` to finish, but bail out as soon
    // as either returns an error. This involves some pretty deep `tokio` magic:
    // If a background worker fails, then `copy_fut` will be automatically
    // dropped, or vice vera.
    let combined_fut = async move {
        try_join!(cmd_future, worker_future)?;
        let result: Result<()> = Ok(());
        result
    };

    // Pass `combined_fut` to our `tokio` runtime, and wait for it to finish.
    let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
    runtime.block_on(combined_fut.boxed())?;
    Ok(())
}

/// Read all data from `input` and return it as bytes.
pub(crate) async fn async_read_to_end<R>(mut input: R) -> Result<Vec<u8>>
where
    R: AsyncRead + Send + Unpin,
{
    let mut buf = vec![];
    input.read_to_end(&mut buf).await?;
    Ok(buf)
}

/// Read all data from `input` and return it as a string.
pub(crate) async fn async_read_to_string<R>(input: R) -> Result<String>
where
    R: AsyncRead + Send + Unpin,
{
    let bytes = async_read_to_end(input).await?;
    Ok(String::from_utf8(bytes)?)
}

/// Write data to the standard input of a child process.
///
/// WARNING: The child process must consume the entire input without blocking,
/// or our caller must otherwise arrange to consume any output from the child
/// process to avoid the risk of blocking.
#[allow(dead_code)]
pub(crate) async fn write_to_stdin(
    child_name: &str,
    child: &mut Child,
    data: &[u8],
) -> Result<()> {
    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| format_err!("`{}` doesn't have a stdin handle", child_name))?;
    child_stdin
        .write_all(data)
        .await
        .with_context(|_| format!("error piping to `{}`", child_name))?;
    child_stdin
        .shutdown()
        .await
        .with_context(|_| format!("error shutting down pipe to `{}`", child_name))?;
    Ok(())
}

/// Given a function `f`, pass it a sync `Write` implementation, and collect the
/// data that it writes to `f`. Then write that data asynchronously to the async
/// `wtr`. This is a convenience function for outputting small amounts of data.
///
/// TODO: Does this particular function API still make sense with `tokio` 0.2,
/// or can we simplify it nicely?
pub(crate) async fn buffer_sync_write_and_copy_to_async<W, F, E>(
    mut wtr: W,
    f: F,
) -> Result<W>
where
    W: AsyncWrite + Send + Unpin,
    F: FnOnce(&mut dyn Write) -> result::Result<(), E>,
    E: Into<Error>,
{
    let mut buffer = vec![];
    f(&mut buffer).map_err(|e| e.into())?;
    wtr.write_all(&buffer).await?;
    Ok(wtr)
}

/// An internal "broken pipe" error, for when we try to send to a channel but
/// the correspoding receiver has already been destroyed.
///
/// This exists because [`tokio::sync::mpsc::error::SendError`] only implements
/// `Error` if `T` implements `Debug`, and we're using sending `BytesMut`, which
/// doesn't. So basically what we want to do is throw away the original
/// `SendError` and introduce our own, which always implements `Error`.
#[derive(Debug)]
pub(crate) struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot send data to closed channel")
    }
}

impl error::Error for SendError {}

/// A handy extension trait which allows us to replace `sender.send(..)` with
/// `sender.send(..).into_send_err()`, and get a type guaranteed to implement
/// `Error`. See [`SendError`].
pub(crate) trait SendResultExt<T> {
    /// Convert the error payload of this result to [`SendError`].
    fn map_send_err(self) -> Result<T, SendError>;
}

impl<T, ErrInfo> SendResultExt<T> for Result<T, mpsc::error::SendError<ErrInfo>> {
    fn map_send_err(self) -> Result<T, SendError> {
        match self {
            Ok(val) => Ok(val),
            Err(_err) => Err(SendError),
        }
    }
}

/// A bytes stream type simailar to our `BoxStream<BytesMut>`, but instead
/// using more idomatic Rust types.
///
/// - We replace `failure::Error` with `Box<dyn std::error::Error>`.
/// - We replace `BytesMut` with `Bytes`.
/// - We require `Sync` everywhere.
///
/// This is used for interoperability with other crates such as `reqwest`,
/// and we may eventually use it to replace `BoxStream<BytesMut>`.
pub(crate) type IdiomaticBytesStream = Pin<
    Box<
        dyn TryStream<
                Ok = Bytes,
                Error = Box<dyn error::Error + Send + Sync>,
                Item = Result<Bytes, Box<dyn error::Error + Send + Sync>>,
            > + Send
            + Sync
            + 'static,
    >,
>;

/// Convert an HTTP response into a `BoxStream<BytesMut>`.
///
/// This is limited to a single concrete input stream type.
pub(crate) fn http_response_stream(
    response: reqwest::Response,
) -> BoxStream<BytesMut> {
    response
        .bytes_stream()
        // Convert `Bytes` to `BytesMut` by copying, which is slightly
        // expensive.
        .map_ok(|chunk| BytesMut::from(chunk.as_ref()))
        .map_err(|err| err.into())
        .boxed()
}

/// Convert a `BoxStream<BytesMut>` to something more idiomatic.
pub(crate) fn idiomatic_bytes_stream(
    ctx: &Context,
    stream: BoxStream<BytesMut>,
) -> IdiomaticBytesStream {
    // Adjust our payload type.
    let to_forward = stream.map_ok(|bytes| bytes.freeze());

    // `stream` is a `BoxStream`, so we can't assume that it's `Sync`.
    // But our return type needs to be `Sync`, so we need to take fairly
    // drastic measures, and forward our stream through a channel.
    let (mut sender, receiver) = mpsc::channel::<Result<Bytes, Error>>(1);
    let forwarder_ctx = ctx.to_owned();
    let forwarder: BoxFuture<()> = async move {
        try_forward_to_sender(&forwarder_ctx, to_forward, &mut sender).await
    }
    .boxed();
    ctx.spawn_worker(forwarder);

    let stream = ReceiverStream::new(receiver).map_err(
        |err| -> Box<dyn error::Error + Send + Sync> { Box::new(err.compat()) },
    );
    Box::pin(stream)
}
