//! Interfaces to Google Cloud Storage.

use crate::common::*;

mod download_file;
mod ls;
mod rmdir;
mod upload_file;

pub(crate) use download_file::download_file;
pub(crate) use ls::ls;
pub(crate) use rmdir::rmdir;
pub(crate) use upload_file::upload_file;

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
