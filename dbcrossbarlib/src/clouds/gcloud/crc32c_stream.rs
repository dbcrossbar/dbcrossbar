//! Streams which keep a running CRC32 digest of the data that passes through them.

use crc32c::crc32c_append;
use futures::TryStream;
use std::{pin::Pin, task::Poll};
use tokio::sync::oneshot;

use crate::common::*;

/// A GCloud-compatible CRC32C hasher.
///
/// This uses the popular Rust `Hasher` API to wrap a lower-level library.
#[derive(Clone, Debug)]
pub(crate) struct Hasher {
    state: u32,
}

impl Hasher {
    /// Create a new `Hasher` with the default state.
    fn new() -> Self {
        Self { state: 0 }
    }

    /// Update the hasher with new data.
    fn update(&mut self, data: &[u8]) {
        self.state = crc32c_append(self.state, data);
    }

    /// Finish hashing and return our underlying value.
    ///
    /// This consumes `self` because that's how some other Rust `Hasher` types
    /// work.
    pub(crate) fn finish(self) -> u32 {
        self.state
    }

    /// Finish hashing and return our underlying value as a big-endian Base64
    /// string.
    pub(crate) fn finish_encoded(self) -> String {
        let bytes = self.finish().to_be_bytes();
        base64::encode(&bytes)
    }
}

#[test]
fn crc32c_matches_gcloud() {
    // Check that `data` hashes to `expected`.
    let check = |data: &[u8], expected: u32| {
        let mut hasher = Hasher::new();
        hasher.update(data);
        assert_eq!(hasher.finish(), expected);
    };

    // These test cases are from https://tools.ietf.org/html/rfc3720#page-217
    // and https://github.com/google/crc32c/blob/master/src/crc32c_unittest.cc
    check(&[0u8; 32], 0x8a91_36aa);
    check(&[0xff; 32], 0x62a8_ab43);
    let mut buf = [0u8; 32];
    for i in 0u8..=31 {
        buf[usize::from(i)] = i;
    }
    check(&buf, 0x46dd_794e);
    for i in 0u8..=31 {
        buf[usize::from(i)] = 31 - i;
    }
    check(&buf, 0x113f_db5c);
}

/// Wrap the stream `S`, and keep a running CRC32 hash of the data we see on the
/// stream. When the stream is finished, send the hash to a listener.
pub(crate) struct Crc32cStream<S>
where
    // We require `Unpin` here, because it allows us to access `self.inner`
    // without using `unsafe`, which we avoid in `dbcrossbar`. Happily,
    // `BoxStream` implements `Unpin`, so this restriction isn't overly
    // limiting.
    S: TryStream<Error = Error> + Send + Unpin + 'static,
    S::Ok: AsRef<[u8]>,
{
    /// The wrapped stream.
    inner: S,

    /// A CRC32 hasher.
    hasher: Hasher,

    /// A sender which will receive `hasher` when the stream has finished.
    sender: Option<oneshot::Sender<Hasher>>,
}

impl<S> Crc32cStream<S>
where
    S: TryStream<Error = Error> + Send + Unpin + 'static,
    S::Ok: AsRef<[u8]>,
{
    /// Create a new `Crc32Stream` wrapping `inner`.
    pub(crate) fn new(inner: S) -> (Self, oneshot::Receiver<Hasher>) {
        let hasher = Hasher::new();
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                inner,
                hasher,
                sender: Some(sender),
            },
            receiver,
        )
    }
}

impl<S, D> Stream for Crc32cStream<S>
where
    // We need a slightly more complicated version of these bounds here.
    S: TryStream<Ok = D, Error = Error, Item = Result<D, Error>>
        + Send
        + Unpin
        + 'static,
    D: AsRef<[u8]>,
{
    type Item = Result<S::Ok, S::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = <S as Stream>::poll_next(Pin::new(&mut self.inner), cx);
        match result {
            // We've received data.
            Poll::Ready(Some(Ok(data))) => {
                self.hasher.update(data.as_ref());
                Poll::Ready(Some(Ok(data)))
            }

            // We've reached the end of the stream.
            Poll::Ready(None) => {
                // Send our hash. We can do this in a `poll_*` method because
                // `oneshot::Sender::send` is synchronous, and we don't have to
                // wait for it.
                if let Some(sender) = self.sender.take() {
                    if sender.send(self.hasher.clone()).is_ok() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Err(format_err!(
                            "broken pipe forwarding checksum from Crc32Stream",
                        ))))
                    }
                } else {
                    Poll::Ready(Some(Err(format_err!(
                        "Crc32Stream tried to end twice",
                    ))))
                }
            }

            // Something else has happened.
            other => other,
        }
    }
}
