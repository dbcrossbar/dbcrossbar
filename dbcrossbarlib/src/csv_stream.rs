//! Our basic data representation.

use crate::common::*;

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
            .compat()
            .await
            .expect("could not send bytes to channel");
        CsvStream {
            name: "bytes".to_owned(),
            data: Box::new(receiver),
        }
    }

    /// Receive all data on a CSV stream and return it as bytes.
    #[cfg(test)]
    pub(crate) async fn into_bytes(self, ctx: Context) -> Result<BytesMut> {
        let ctx = ctx.child(o!("fn" => "into_bytes"));
        let mut stream = self.data;
        let mut bytes = BytesMut::new();
        loop {
            match stream.into_future().compat().await {
                Err((err, _rest_of_stream)) => {
                    error!(ctx.log(), "error reading stream: {}", err);
                    return Err(err);
                }
                Ok((None, _rest_of_stream)) => {
                    trace!(ctx.log(), "end of stream");
                    return Ok(bytes);
                }
                Ok((Some(new_bytes), rest_of_stream)) => {
                    trace!(ctx.log(), "received {} bytes", new_bytes.len());
                    stream = rest_of_stream;
                    bytes.extend_from_slice(&new_bytes);
                }
            }
        }
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
            &file_path[base_path.len()+1..]
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
