//! Glue code for working with `tokio`'s async I/O.
//!
//! This is mostly smaller things that happen to recur in our particular
//! application.

use bytes::BytesMut;
use failure::format_err;
use std::{
    future::Future as StdFuture,
    ops::DerefMut,
    sync::{Arc, RwLock},
};
use tokio::{io, prelude::*};
use tokio_async_await::compat;

use crate::{Error, Result};

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
pub(crate) trait StdFutureExt<T> {
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
pub(crate) async fn copy_stream_to_writer<S, W>(mut stream: S, mut wtr: W) -> Result<()>
where
    S: Stream<Item = BytesMut, Error = Error> + 'static,
    W: AsyncWrite + 'static,
{
    loop {
        match await!(stream.into_future()) {
            Err((err, _rest_of_stream)) => return Err(err),
            Ok((None, _rest_of_stream)) => return Ok(()),
            Ok((Some(bytes), rest_of_stream)) => {
                stream = rest_of_stream;
                await!(io::write_all(&mut wtr, bytes))
                    .map_err(|e| format_err!("error writing data: {}", e))?;
            }
        }
    }
}

// Convert an `async fn() -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
pub(crate) fn tokio_fn_0<F, R, SF>(f: F) -> (impl Fn() -> compat::backward::Compat<SF>)
where
    F: (Fn() -> SF) + Send,
    SF: StdFuture<Output = Result<R>> + Send,
{
    move || tokio_fut(f())
}

// Convert an `async fn(A1) -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
pub(crate) fn tokio_fn_1<F, A1, R, SF>(f: F) -> (impl Fn(A1) -> compat::backward::Compat<SF>)
where
    F: (Fn(A1) -> SF) + Send,
    SF: StdFuture<Output = Result<R>> + Send,
{
    move |a1: A1| tokio_fut(f(a1))
}

// Convert an `async fn(A1, A2) -> Result<R>` to an equivalent `tokio` function
// returning a `tokio::Future`.
pub(crate) fn tokio_fn_2<F, A1, A2, R, SF>(f: F) -> (impl Fn(A1, A2) -> compat::backward::Compat<SF>)
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
