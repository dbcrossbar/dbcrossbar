//! Our basic data representation.

use crate::common::*;

/// A stream of CSV data, with a unique name.
pub struct CsvStream {
    /// The name of this stream.
    pub name: String,
    /// A reader associated with this stream.
    pub data: Box<dyn Stream<Item = BytesMut, Error = Error> + Send + 'static>,
}
