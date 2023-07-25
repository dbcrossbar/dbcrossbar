use std::{ffi::OsStr, fmt, str::FromStr};

use crate::{common::*, json_to_csv::json_lines_to_csv};

/// The format of a stream of data.
///
/// TODO: We might add a `StreamFormat` that handles "wrapper" formats like
/// `gzip` or `bzip2`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataFormat {
    /// Comma-separated values.
    #[default]
    Csv,
    /// One JSON value per line. See [JSON Lines](http://jsonlines.org/).
    JsonLines,
    /// Another data format that we don't support. This will be the file extension,
    /// minus any leading "." character.
    Unsupported(String),
}

impl DataFormat {
    /// Fetch the `DataFormat` for a given file extension, or `None` if we don't
    /// have a file extension.
    pub(crate) fn from_extension(ext: &OsStr) -> Self {
        // `to_string_lossy` will replace any non-UTF-8 bytes with the Unicode
        // replacement character U+FFFD. Any such extension will wind up as
        // `DataFormat::Unsupported`, so it doesn't matter than we lose
        // non-UTF-8 information in this case.
        let ext = ext.to_string_lossy();
        let ext = ext.to_ascii_lowercase();
        match &ext[..] {
            "csv" => Self::Csv,
            "jsonl" => Self::JsonLines,
            _ => Self::Unsupported(ext),
        }
    }
}

impl FromStr for DataFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(Self::from_extension(OsStr::new(s)))
    }
}

impl fmt::Display for DataFormat {
    /// Format data formats as their file extensions, without the leading ".".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::JsonLines => write!(f, "jsonl"),
            Self::Unsupported(s) => write!(f, "{}", s),
        }
    }
}

#[test]
fn data_format_default_is_csv() {
    assert_eq!(DataFormat::default(), DataFormat::Csv);
}

/// An async stream similar to a [`CsvStream`], but it can hold different kinds of
/// data.
pub(crate) struct DataStream {
    /// The name of this stream.
    pub(crate) name: String,
    /// The format of this stream.
    pub(crate) format: DataFormat,
    /// Our data.
    pub(crate) data: BoxStream<BytesMut>,
}

impl DataStream {
    /// Convert this `DataStream` into a `CsvStream`. This is very cheap if
    /// the data is already in CSV format,
    pub(crate) async fn into_csv_stream(
        self,
        ctx: &Context,
        schema: &Schema,
    ) -> Result<CsvStream> {
        match self.format {
            DataFormat::Csv => Ok(CsvStream {
                name: self.name,
                data: self.data,
            }),
            DataFormat::JsonLines => Ok(CsvStream {
                name: self.name,
                data: json_lines_to_csv(ctx, schema, self.data).await?,
            }),
            other => Err(format_err!("cannot convert `*.{}` to CSV", other)),
        }
    }
}
