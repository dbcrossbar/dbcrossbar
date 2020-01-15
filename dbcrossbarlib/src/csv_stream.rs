//! Our basic data representation.

use bytes::Bytes;
use failure::Compat;
use reqwest::{self, Response};
use std::pin::Pin;

use crate::common::*;
use crate::tokio_glue::try_forward_to_sender;

/// A stream of CSV data, with a unique name.
pub struct CsvStream {
    /// The name of this stream.
    pub name: String,
    /// Our data.
    pub data: BoxStream<BytesMut>,
}

impl CsvStream {
    /// Construct a CSV stream from bytes.
    #[cfg(test)]
    pub(crate) async fn from_bytes<B>(bytes: B) -> Self
    where
        B: Into<BytesMut>,
    {
        use crate::tokio_glue::bytes_channel;
        let (mut sender, receiver) = bytes_channel(1);
        sender
            .send(Ok(bytes.into()))
            .await
            .expect("could not send bytes to channel");
        CsvStream {
            name: "bytes".to_owned(),
            data: receiver.boxed(),
        }
    }

    /// Receive all data on a CSV stream and return it as bytes.
    #[cfg(test)]
    pub(crate) async fn into_bytes(self, ctx: Context) -> Result<BytesMut> {
        let ctx = ctx.child(o!("fn" => "into_bytes"));
        let mut stream = self.data;
        let mut bytes = BytesMut::new();
        while let Some(result) = stream.next().await {
            match result {
                Err(err) => {
                    error!(ctx.log(), "error reading stream: {}", err);
                    return Err(err);
                }
                Ok(new_bytes) => {
                    trace!(ctx.log(), "received {} bytes", new_bytes.len());
                    bytes.extend_from_slice(&new_bytes);
                }
            }
        }
        trace!(ctx.log(), "end of stream");
        return Ok(bytes);
    }

    /// Convert an HTTP `Body` into a `CsvStream`.
    pub(crate) fn from_http_response(
        name: String,
        response: Response,
    ) -> Result<CsvStream> {
        let data = response
            .bytes_stream()
            // Convert `Bytes` to `BytesMut` by copying, which is slightly
            // expensive.
            .map_ok(|chunk| BytesMut::from(chunk.as_ref()))
            .map_err(|err| err.into());
        Ok(CsvStream {
            name,
            data: data.boxed(),
        })
    }

    /// Convert this `CsvStream` into a `Stream` that can be used with
    /// `hyper`, `reqwest`, and possibly other Rust libraries. Returns
    /// the stream name and the stream.
    #[allow(clippy::type_complexity)]
    pub(crate) fn into_name_and_portable_stream(
        self,
        ctx: &Context,
    ) -> (
        String,
        Pin<Box<dyn Stream<Item = Result<Bytes, Compat<Error>>> + Send + Sync>>,
    ) {
        // Adjust our payload type.
        let to_forward = self.data.map_ok(|bytes| bytes.freeze());

        // `self.data` is a `BoxStream`, so we can't assume that it's `Sync`.
        // But our return type needs to be `Sync`, so we need to take fairly
        // drastic measures, and forward our stream through a channel.
        let (mut sender, receiver) = mpsc::channel::<Result<Bytes, Error>>(1);
        let forwarder_ctx = ctx.to_owned();
        let forwarder: BoxFuture<()> = async move {
            try_forward_to_sender(&forwarder_ctx, to_forward, &mut sender).await
        }
        .boxed();
        ctx.spawn_worker(forwarder);

        let stream = receiver.map_err(|err| err.compat());
        (self.name, Box::pin(stream))
    }
}

/// Given a `base_path` refering to one of more CSV files, and a `file_path`
/// refering to a single CSV file, figure out the best name to use for a
/// `CsvStream` for that CSV file.
pub(crate) fn csv_stream_name<'a>(
    base_path: &str,
    file_path: &'a str,
) -> Result<&'a str> {
    let basename_or_relative = if file_path == base_path {
        // Our base_path and our file_path are the same, which means that we had
        // only a single input, and we therefore want to extract the "basename",
        // or filename without any directories.
        file_path
            .rsplitn(2, '/')
            .next()
            .expect("should have '/' in URL")
    } else if file_path.starts_with(base_path) {
        if base_path.ends_with('/') {
            // Our file_path starts with our base_path, which means that we have an
            // entire directory tree full of files and this is one. This means we
            // want to take the relative path within this directory.
            &file_path[base_path.len()..]
        } else if file_path.len() > base_path.len()
            && file_path[base_path.len()..].starts_with('/')
        {
            &file_path[base_path.len() + 1..]
        } else {
            return Err(format_err!(
                "expected {} to start with {}",
                file_path,
                base_path,
            ));
        }
    } else {
        return Err(format_err!(
            "expected {} to start with {}",
            file_path,
            base_path,
        ));
    };

    // Now strip any extension.
    let name = basename_or_relative
        .splitn(2, '.')
        .next()
        .ok_or_else(|| format_err!("can't get basename of {}", file_path))?;
    Ok(name)
}

#[test]
fn csv_stream_name_handles_file_inputs() {
    let expected = &[
        ("/path/to/file1.csv", "file1"),
        ("file2.csv", "file2"),
        ("s3://bucket/dir/file3.csv", "file3"),
        ("gs://bucket/dir/file4.csv", "file4"),
    ];
    for &(file_path, stream_name) in expected {
        assert_eq!(csv_stream_name(file_path, file_path).unwrap(), stream_name);
    }
}

#[test]
fn csv_stream_name_handles_directory_inputs() {
    let expected = &[
        ("dir/", "dir/file1.csv", "file1"),
        ("dir", "dir/file1.csv", "file1"),
        ("dir/", "dir/subdir/file2.csv", "subdir/file2"),
        (
            "s3://bucket/dir/",
            "s3://bucket/dir/subdir/file3.csv",
            "subdir/file3",
        ),
    ];
    for &(base_path, file_path, stream_name) in expected {
        assert_eq!(csv_stream_name(base_path, file_path).unwrap(), stream_name);
    }
}
