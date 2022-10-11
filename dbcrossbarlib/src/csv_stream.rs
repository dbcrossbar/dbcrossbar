//! Our basic data representation.

use reqwest::{self, Response};

use crate::common::*;
use crate::tokio_glue::{
    http_response_stream, idiomatic_bytes_stream, IdiomaticBytesStream,
};

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
        let (sender, receiver) = bytes_channel(1);
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
    #[instrument(level = "trace", skip(self))]
    pub(crate) async fn into_bytes(self) -> Result<BytesMut> {
        let mut stream = self.data;
        let mut bytes = BytesMut::new();
        while let Some(result) = stream.next().await {
            match result {
                Err(err) => {
                    error!("error reading stream: {}", err);
                    return Err(err);
                }
                Ok(new_bytes) => {
                    trace!("received {} bytes", new_bytes.len());
                    bytes.extend_from_slice(&new_bytes);
                }
            }
        }
        trace!("end of stream");
        Ok(bytes)
    }

    /// Convert an HTTP `Body` into a `CsvStream`.
    pub(crate) fn from_http_response(
        name: String,
        response: Response,
    ) -> Result<CsvStream> {
        Ok(CsvStream {
            name,
            data: http_response_stream(response),
        })
    }

    /// Convert this `CsvStream` into a `Stream` that can be used with
    /// `hyper`, `reqwest`, and possibly other Rust libraries. Returns
    /// the stream name and the stream.
    #[allow(dead_code)]
    pub(crate) fn into_name_and_idiomatic_stream(
        self,
        ctx: &Context,
    ) -> (String, IdiomaticBytesStream) {
        (self.name, idiomatic_bytes_stream(ctx, self.data))
    }
}

/// Given a `base_path` refering to one of more CSV files, and a `file_path`
/// refering to a single CSV file, figure out the best name to use for a
/// `CsvStream` for that CSV file.
///
/// (We allow manual prefix stripping in this function because the logic makes
/// `strip_prefix` inconvenient.)
#[allow(clippy::manual_strip)]
pub(crate) fn csv_stream_name<'a>(
    base_path: &str,
    file_path: &'a str,
) -> Result<&'a str> {
    let basename_or_relative = if file_path == base_path {
        // Our base_path and our file_path are the same, which means that we had
        // only a single input, and we therefore want to extract the "basename",
        // or filename without any directories.
        file_path
            .rsplit('/')
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
        .split('.')
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
