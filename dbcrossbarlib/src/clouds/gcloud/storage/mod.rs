//! Interfaces to Google Cloud Storage.

use bigml::{wait::BackoffType, WaitOptions};
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize,
};
use std::{fmt, marker::PhantomData, str::FromStr, time::Duration};

use crate::common::*;

mod download_file;
mod ls;
mod rm_r;
mod upload_file;

pub(crate) use download_file::download_file;
pub(crate) use ls::ls;
pub(crate) use rm_r::rm_r;
pub(crate) use upload_file::upload_file;

/// Chunk size to use when working with Google Cloud Storage.
///
/// This needs to satisfy two constraints:
///
/// 1. It needs to be small enough that we can keep, say, 40 chunks in memory
///    with no problem, if we assume 8 streams and 5 active chunks per stream.
///    (Plus more for any other driver running at the same time, and for the
///    pipeline between.)
/// 2. It needs to be large enough to ensure good performance from the storage
///    APIs. Google [recommends][opt] 1 MiB minimum chunks.
///
/// [opt]: https://cloud.google.com/blog/products/gcp/optimizing-your-cloud-storage-performance-google-cloud-performance-atlas
#[cfg(not(debug_assertions))]
pub(crate) const CHUNK_SIZE: u64 = 1024 * 1024;

// Use a much smaller chunk size when testing to force our chunking code to be
// used. We key off "debug_assertions" because that seems to be the easiest way
// to test whether we're in release mode.
#[cfg(debug_assertions)]
pub(crate) const CHUNK_SIZE: u64 = 128;

/// Split a `gs://` URL into a bucket and an object name.
pub(crate) fn parse_gs_url(url: &Url) -> Result<(String, String)> {
    if url.scheme() != "gs" {
        Err(format_err!("expected a gs:// URL, found {}", url))
    } else {
        let bucket = url
            .host_str()
            .ok_or_else(|| format_err!("could not get bucket from {}", url))?
            .to_owned();
        let object = (&url.path()[1..]).to_owned();
        Ok((bucket, object))
    }
}

/// Information about an individual object.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageObject {
    /// The bucket of this object.
    pub(crate) bucket: String,
    /// The name of this object. This typically looks like a path without the leading slash.
    pub(crate) name: String,
    /// The etag of this object. This is used to make sure it doesn't change unexpectedly.
    pub(crate) etag: String,
    /// The size of this oject, in bytes.
    #[serde(deserialize_with = "deserialize_int::<'_, u64, _>")]
    pub(crate) size: u64,
    /// A CRC32C sum of this object, used for checking integrity.
    pub(crate) crc32c: String,
    /// The generation number for this object's data.
    #[serde(deserialize_with = "deserialize_int::<'_, i64, _>")]
    pub(crate) generation: i64,
    /// The generation number for this object's metadata.
    #[serde(deserialize_with = "deserialize_int::<'_, i64, _>")]
    #[allow(dead_code)]
    pub(crate) metageneration: i64,
}

impl StorageObject {
    /// Convert this to a `gs://` URL.
    pub(crate) fn to_url_string(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.name)
    }
}

/// A helper function which can deserialize integers represented as either
/// numbers or strings.
fn deserialize_int<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
    D: Deserializer<'de>,
{
    // We deserialize this using a visitor as described at
    // https://serde.rs/impl-deserialize.html because we may want to add support
    // for transparently handling a mix of strings and floats, if we ever get
    // that back from any API.
    struct IntVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for IntVisitor<T>
    where
        T: FromStr,
        <T as FromStr>::Err: fmt::Display,
    {
        type Value = T;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string containing an integer")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            v.parse::<T>().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(IntVisitor(PhantomData))
}

/// If we encounter an "access denied" option trying to write to a bucket, what
/// retry policy should we use?
///
/// The [`rm_r`] and [`crate::clouds::gcloud::bigquery::extract`] operations
/// occasionally fails with internal permission errors. These appear to be
/// transient, possible caused by some sort of race condition authorizing either
/// either dbcrossbar or BigQuery workers to write to our temp bucket.
///
/// See [issue #181](https://github.com/dbcrossbar/dbcrossbar/issues/181) for
/// more discussion.
pub(crate) fn gcs_write_access_denied_wait_options() -> WaitOptions {
    WaitOptions::default()
        .backoff_type(BackoffType::Exponential)
        .retry_interval(Duration::from_secs(10))
        // Don't retry too much because we're probably classifying some permanent
        // errors as temporary, and because `extract` may be very expensive.
        .allowed_errors(2)
}
