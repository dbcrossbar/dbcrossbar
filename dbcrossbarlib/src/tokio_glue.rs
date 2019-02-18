//! Glue code for working with `tokio`'s async I/O.
//!
//! This is mostly smaller things that happen to recur in our particular
//! application.

use std::{cmp::min, future::Future as StdFuture};
use tokio::io;
use tokio_async_await::compat;

use crate::common::*;

/// Standard future type for this library. Like `Result`, but used by async.
pub type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

/// Convert a `std::future::Future` to a `tokio::Future`.
pub fn tokio_fut<T, F>(f: F) -> compat::backward::Compat<F>
where
    F: StdFuture<Output = Result<T>> + Send,
{
    compat::backward::Compat::new(f)
}

/// Extensions to `tokio::Future`.
///
/// This needs to be separate trait from `StdFutureExt`, because both are
/// implemented for a blanket type `F`, which causes a conflict the Rust
/// compiler refuses to sort out.
pub(crate) trait FutureExt<T> {
    /// Convert a `tokio::Future` into a `BoxFuture<T>`. This moves the future to the
    /// heap, and it prevents us from needing to care about exactly what _type_
    /// of future we have.
    fn into_boxed(self) -> BoxFuture<T>;
}

impl<T, F> FutureExt<T> for F
where
    F: Future<Item = T, Error = Error> + Send + 'static,
{
    fn into_boxed(self) -> BoxFuture<T> {
        Box::new(self)
    }
}

/// Extensions to `std::future::Future`.
///
/// `std::future::Future` will eventually replace `tokio::Future`, but it is
/// already used by the `async` keyword. So we're mixing two different kinds of
/// futures together during the transition period.
pub trait StdFutureExt<T> {
    /// Convert a `std::future::Future` into a `BoxFuture`. This moves the
    /// future to the heap, and it prevents us from needing to care about
    /// exactly what _type_ of future we have.
    fn into_boxed(self) -> BoxFuture<T>;
}

impl<T, F> StdFutureExt<T> for F
where
    F: StdFuture<Output = Result<T>> + Send + 'static,
{
    fn into_boxed(self) -> BoxFuture<T> {
        // We need to use `backward::Compat` to turn new-style
        // `std::future::Future` values into the soon-to-be-replaced
        // `tokio::Future` values.
        Box::new(tokio_fut(self))
    }
}

/// Extensions to `Result`, allowing us to easy translate it to a `BoxFuture`.
pub(crate) trait ResultExt<T> {
    /// Transform a `Result` into a `BoxFuture`.
    fn into_boxed_future(self) -> BoxFuture<T>;
}

impl<T: Send + 'static> ResultExt<T> for Result<T> {
    fn into_boxed_future(self) -> BoxFuture<T> {
        self.into_future().into_boxed()
    }
}

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
        match await!(stream.into_future()) {
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
                await!(io::write_all(&mut wtr, bytes)).map_err(|e| {
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
            match await!(io::read(rdr, &mut buffer)) {
                Err(err) => {
                    let nice_err = format_err!("stream read error: {}", err);
                    error!(ctx.log(), "{}", nice_err);
                    if await!(sender.send(Err(nice_err))).is_err() {
                        error!(
                            ctx.log(),
                            "broken pipe prevented sending error: {}", err
                        );
                    }
                    return;
                }
                Ok((new_rdr, data, count)) => {
                    if count == 0 {
                        trace!(ctx.log(), "done copying AsyncRead to stream");
                        return;
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
                            return;
                        }
                    }
                }
            }
        }
    };
    tokio::spawn_async(worker);

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

// Convert an `async fn() -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
#[allow(dead_code)]
pub(crate) fn tokio_fn_0<F, R, SF>(f: F) -> (impl Fn() -> compat::backward::Compat<SF>)
where
    F: (Fn() -> SF) + Send,
    SF: StdFuture<Output = Result<R>> + Send,
{
    move || tokio_fut(f())
}

// Convert an `async fn(A1) -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
#[allow(dead_code)]
pub(crate) fn tokio_fn_1<F, A1, R, SF>(
    f: F,
) -> (impl Fn(A1) -> compat::backward::Compat<SF>)
where
    F: (Fn(A1) -> SF) + Send,
    SF: StdFuture<Output = Result<R>> + Send,
{
    move |a1: A1| tokio_fut(f(a1))
}

// Convert an `async fn(A1, A2) -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
#[allow(dead_code)]
pub(crate) fn tokio_fn_2<F, A1, A2, R, SF>(
    f: F,
) -> (impl Fn(A1, A2) -> compat::backward::Compat<SF>)
where
    F: (Fn(A1, A2) -> SF) + Send,
    SF: StdFuture<Output = Result<R>> + Send,
{
    move |a1: A1, a2: A2| tokio_fut(f(a1, a2))
}

#[test]
fn tokio_fn_n_converts_closures() {
    let wrapped_fn_0 = tokio_fn_0(async || Ok(10));
    assert_eq!(wrapped_fn_0().wait().unwrap(), 10);

    let wrapped_fn_1 = tokio_fn_1(async move |n| Ok(n * 2));
    assert_eq!(wrapped_fn_1(5).wait().unwrap(), 10);

    let wrapped_fn_2 = tokio_fn_2(async move |x, y| Ok(x + y));
    assert_eq!(wrapped_fn_2(2, 3).wait().unwrap(), 5);
}
