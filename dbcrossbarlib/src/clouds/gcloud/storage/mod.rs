//! Interfaces to Google Cloud Storage.

use serde::Deserialize;

use crate::common::*;

mod download_file;
mod ls;
mod rmdir;
mod upload_file;

pub(crate) use download_file::download_file;
pub(crate) use ls::ls;
pub(crate) use rmdir::rmdir;
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
    pub(crate) size: String,
}

impl StorageObject {
    /// Convert this to a `gs://` URL.
    pub(crate) fn to_url_string(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.name)
    }

    /// Parse the `size` field that gets returned as a string.
    pub(crate) fn size(&self) -> Result<u64> {
        Ok(self
            .size
            .parse::<u64>()
            .context("could not parse object size")?)
    }
}
