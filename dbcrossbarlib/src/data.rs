//! Data formats used for communicating between sources and sinks.

use std::io::Read;
use url::Url;

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
