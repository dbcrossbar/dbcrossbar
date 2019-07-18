//! Our basic data representation.

use crate::common::*;

/// A stream of CSV data, with a unique name.
pub struct CsvStream {
    /// The name of this stream.
    pub name: String,
    /// A reader associated with this stream.
    pub data: Box<dyn Stream<Item = BytesMut, Error = Error> + Send + 'static>,
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
    } else if base_path.ends_with('/') && file_path.starts_with(base_path) {
        // Our file_path starts with our base_path, which means that we have an
        // entire directory tree full of files and this is one. This means we
        // want to take the relative path within this directory.
        &file_path[base_path.len()..]
    } else {
        return Err(format_err!(
            "expected {} to start with {}",
            file_path,
            base_path
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
