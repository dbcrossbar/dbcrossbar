use std::{ffi::OsStr, fmt, str::FromStr};

use async_trait::async_trait;

use crate::common::*;

mod csv_converter;
mod jsonl_converter;

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

    /// Look up the [`DataFormatConverter`] for a given data format.
    fn converter(&self) -> Result<Box<dyn DataFormatConverter>> {
        match self {
            DataFormat::Csv => Ok(Box::new(csv_converter::CsvConverter)),
            DataFormat::JsonLines => Ok(Box::new(jsonl_converter::JsonLinesConverter)),
            other => Err(format_err!("cannot convert between `*.{}` and CSV", other)),
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
    /// Try to infer a schema from this `DataStream`.
    pub(crate) async fn schema(self, ctx: &Context) -> Result<Option<Schema>> {
        self.format
            .converter()?
            .schema(ctx, &self.name, self.data)
            .await
    }

    /// Convert this `DataStream` into a `CsvStream`. This is very cheap if
    /// the data is already in CSV format.
    pub(crate) async fn into_csv_stream(
        self,
        ctx: &Context,
        schema: &Schema,
    ) -> Result<CsvStream> {
        let data = self
            .format
            .converter()?
            .data_format_to_csv(ctx, schema, self.data)
            .await?;
        Ok(CsvStream {
            name: self.name,
            data,
        })
    }

    /// Convert a `CsvStream` into a `DataStream`. This is very cheap if
    /// the data is already in CSV format.
    pub(crate) async fn from_csv_stream(
        ctx: &Context,
        format: DataFormat,
        schema: &Schema,
        stream: CsvStream,
    ) -> Result<Self> {
        let data = format
            .converter()?
            .csv_to_data_format(ctx, schema, stream.data)
            .await?;
        Ok(Self {
            name: stream.name,
            format,
            data,
        })
    }
}

/// Convert a format to and from CSV format.
#[async_trait]
pub(self) trait DataFormatConverter: Send + Sync {
    /// Infer a schema from a stream of data.
    async fn schema(
        &self,
        _ctx: &Context,
        _table_name: &str,
        _data: BoxStream<BytesMut>,
    ) -> Result<Option<Schema>> {
        Ok(None)
    }

    /// Convert a stream to CSV format.
    async fn data_format_to_csv(
        &self,
        ctx: &Context,
        schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>>;

    /// Convert a stream from CSV format.
    async fn csv_to_data_format(
        &self,
        ctx: &Context,
        schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>>;
}
