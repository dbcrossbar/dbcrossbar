//! Data types used by the planner.

use std::{fmt, iter};

#[cfg(test)]
use proptest_derive::Arbitrary;

/// An iterator which we can use to generate alternatives in a backtracking
/// computation.
///
/// NOPE: It provides a bunch of guarantees, including `Clone`, which gives us the
/// option of saving iterator state and retrying it later.
///
/// TODO: Decide what to do about `Clone`.
pub(crate) trait BacktrackIterator: Iterator + iter::FusedIterator {}

impl<Iter> BacktrackIterator for Iter where Iter: Iterator + iter::FusedIterator {}

/// A simple format representing tabular data.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum DataFormat {
    Csv,
}

impl fmt::Display for DataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataFormat::Csv => write!(f, "csv"),
        }
    }
}

/// A compression format which operates on a single stream of data.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum CompressionFormat {
    Gz,
}

impl fmt::Display for CompressionFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionFormat::Gz => write!(f, "gz"),
        }
    }
}
/// The format of a byte stream.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum StreamFormat {
    /// We can transfer raw data in any supported format.
    Data(DataFormat),
    /// Or we can compress it.
    Compressed(DataFormat, CompressionFormat),
}

impl fmt::Display for StreamFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamFormat::Data(data_format) => data_format.fmt(f),
            StreamFormat::Compressed(data_format, compression_format) => {
                write!(f, "{}.{}", data_format, compression_format)
            }
        }
    }
}

/// Do we have a single stream/operation, or many?
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum Parallelism {
    One,
    Many,
}

impl fmt::Display for Parallelism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Parallelism::One => write!(f, "1"),
            Parallelism::Many => write!(f, "N"),
        }
    }
}

/// The format of an overall transfer.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) struct TransferFormat {
    /// How many streams do we have?
    pub(crate) parallelism: Parallelism,
    /// The data format we're using.
    pub(crate) stream_format: StreamFormat,
}

impl fmt::Display for TransferFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}×{}", self.parallelism, self.stream_format)
    }
}

/// Various representations of tabular data on BigMl.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum BigMlResource {
    NewSource(Parallelism),
    NewDataset(Parallelism),
    DatasetId,
}

impl BigMlResource {
    /// Is this `BigMlResource` parallel? Used in cost estimation.
    fn parallelism(&self) -> Parallelism {
        match self {
            BigMlResource::NewSource(parallelism) => *parallelism,
            BigMlResource::NewDataset(parallelism) => *parallelism,
            BigMlResource::DatasetId => Parallelism::One,
        }
    }
}

impl fmt::Display for BigMlResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BigMlResource::NewSource(parallelism) => {
                write!(f, "{}×createSource", parallelism)
            }
            BigMlResource::NewDataset(parallelism) => {
                write!(f, "{}×createDataset", parallelism)
            }
            BigMlResource::DatasetId => write!(f, "dataset"),
        }
    }
}

/// Formats in which data can actually be stored.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum StorageFormat {
    BigMl(BigMlResource),
    BigQuery,
    File(TransferFormat),
    Gs(TransferFormat),
    Postgres,
    S3(TransferFormat),
    Shopify,
    Streaming(TransferFormat),
}

impl StorageFormat {
    /// Can we read from this format?
    pub(crate) fn supports_read(&self) -> bool {
        !matches!(
            self,
            StorageFormat::BigMl(BigMlResource::NewSource(_))
                | StorageFormat::BigMl(BigMlResource::NewDataset(_))
        )
    }

    /// Can we write to this format?
    pub(crate) fn supports_write(&self) -> bool {
        !matches!(
            self,
            StorageFormat::BigMl(BigMlResource::DatasetId) | StorageFormat::Shopify,
        )
    }

    /// Does this format use the local machine to store data or pass it through?
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, StorageFormat::File(_) | StorageFormat::Streaming(_))
    }

    /// Is this `StorageFormat` parallel? Used in cost estimation.
    pub(crate) fn parallelism(&self) -> Parallelism {
        match self {
            StorageFormat::BigMl(r) => r.parallelism(),
            StorageFormat::BigQuery => Parallelism::Many,
            StorageFormat::File(tf) => tf.parallelism,
            StorageFormat::Gs(tf) => tf.parallelism,
            StorageFormat::Postgres => Parallelism::One,
            StorageFormat::S3(tf) => tf.parallelism,
            StorageFormat::Shopify => Parallelism::Many,
            StorageFormat::Streaming(tf) => tf.parallelism,
        }
    }
}

impl fmt::Display for StorageFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageFormat::BigMl(res) => write!(f, "bigml({})", res),
            StorageFormat::BigQuery => write!(f, "bigquery"),
            StorageFormat::File(tf) => write!(f, "file({})", tf),
            StorageFormat::Gs(tf) => write!(f, "gs({})", tf),
            StorageFormat::Postgres => write!(f, "postgres"),
            StorageFormat::S3(tf) => write!(f, "s3({})", tf),
            StorageFormat::Shopify => write!(f, "shopify"),
            StorageFormat::Streaming(tf) => write!(f, "streaming({})", tf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parallelism_one_is_less_than_many() {
        // This is needed for cost estimations to work.
        assert!(Parallelism::One < Parallelism::Many);
    }

    proptest! {
        #[test]
        fn storage_readable_or_writable(storage_format in any::<StorageFormat>()) {
            assert!(storage_format.supports_read() || storage_format.supports_write());
        }
    }
}
