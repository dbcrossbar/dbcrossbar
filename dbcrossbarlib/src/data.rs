//! Data formats used for communicating between sources and sinks.

use std::io::Read;
use url::Url;

use crate::Result;

/// A directory in a cloud bucket, containing zero or more CSV files with the
/// same columns.
pub struct CsvBucketLocator {
    url: Url,
}

/// A stream of CSV data, with a unique name.
pub struct CsvStream {
    pub name: String,
    pub data: Box<dyn Read + 'static>,
}

pub trait LocalSource {
    fn streams(&self) -> Result<Vec<CsvStream>>;
}

pub trait LocalSink {
    fn add_streams(&mut self, streams: Vec<CsvStream>) -> Result<()>;

    fn join(&mut self) -> Result<()>;
}
