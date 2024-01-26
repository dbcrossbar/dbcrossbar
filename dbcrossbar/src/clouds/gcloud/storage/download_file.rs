//! Download a file from Google Cloud storage.

use bytes::BufMut;
use futures::stream;
use headers::{ContentRange, Header, HeaderMapExt, Range};
use reqwest::header::{HeaderMap, HeaderValue, IF_MATCH};
use serde::Serialize;
use std::{cmp::min, convert::TryFrom, ops};
use tokio::spawn;

use super::{
    super::{percent_encode, Alt, Client},
    parse_gs_url, StorageObject, CHUNK_SIZE,
};
use crate::common::*;

/// Maximum number of parallel downloads.
const PARALLEL_DOWNLOADS: usize = 5;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadQuery {
    /// What format should we return?
    alt: Alt,

    /// What object generation do we expect to download?
    if_generation_match: i64,
}

/// Download the file at the specified URL as a stream.
#[instrument(level = "trace", skip(client, item), fields(item = %item.to_url_string()))]
pub(crate) async fn download_file(
    client: &Client,
    item: &StorageObject,
) -> Result<BoxStream<BytesMut>> {
    let file_url = item.to_url_string().parse::<Url>()?;
    debug!("streaming from {}", file_url);
    let (bucket, object) = parse_gs_url(&file_url)?;

    // Build our URL & common headers.
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        percent_encode(&bucket),
        percent_encode(&object),
    );
    let mut common_headers = HeaderMap::default();
    common_headers.insert(IF_MATCH, HeaderValue::from_str(&item.etag)?);

    // Build a stream of download tasks.
    let generation = item.generation;
    let client = client.to_owned();
    let stream = stream::iter(chunk_ranges(CHUNK_SIZE, item.size))
        .map(move |range| {
            download_range(
                client.clone(),
                url.clone(),
                generation,
                common_headers.clone(),
                range,
            )
            .boxed()
        })
        // Use `tokio` magic to download up to `PARALLEL_DOWNLOADS` chunks in parallel.
        .buffered(PARALLEL_DOWNLOADS)
        .boxed();

    Ok(stream)
}

/// Download a single range of the file.
///
/// Unlike typical Rust futures *will not block the download*, even if you don't
/// poll it. This runs the download in a separate `tokio` task. We do this to avoid
/// downloads that get stalled halfway through by backpressure.
#[instrument(level = "trace", skip(client, headers))]
async fn download_range(
    // We take `client` by value so that we can move it into the background
    // task.
    client: Client,
    url: String,
    generation: i64,
    mut headers: HeaderMap,
    range: ops::Range<u64>,
) -> Result<BytesMut> {
    trace!("downloading {} bytes {}-{}", url, range.start, range.end,);

    // Do our work in a separately spawned task to avoid blocking during
    // backpressure. This is reasonable because we're downloading a chunk of
    // predictable size.
    let task_fut = async move {
        // Make our request.
        headers.typed_insert(Range::bytes(range.clone())?);
        let query = DownloadQuery {
            alt: Alt::Media,
            if_generation_match: generation,
        };
        let response = client.get_response(&url, query, headers).await?;

        // Make sure we're downloading the range we expect.
        let content_range = get_header::<ContentRange>(&response)?;
        let (start, end_inclusive) = content_range
            .bytes_range()
            .ok_or_else(|| format_err!("could not get range from Content-Range"))?;
        if start != range.start || end_inclusive + 1 != range.end {
            return Err(format_err!(
                "expected to download range [{}, {}), but server offered [{}, {})",
                range.start,
                range.end,
                start,
                end_inclusive + 1,
            ));
        }

        // Download the data to a buffer.
        let bytes_to_download = usize::try_from(range.end - range.start)
            .with_context(|| {
                format!("range {:?} is to big to fit in memory", range)
            })?;
        let mut buffer = BytesMut::with_capacity(bytes_to_download);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            buffer.put(chunk?);
        }

        // Did we download the number of bytes the `Content-Range` header promised?
        if bytes_to_download == buffer.len() {
            Ok(buffer)
        } else {
            Err(format_err!(
                "expected to download {} bytes, received {}",
                bytes_to_download,
                buffer.len(),
            ))
        }
    };
    let task = spawn(task_fut);
    let buffer = task.await.context("error joining background task")??;
    Ok(buffer)
}

/// Get a typed header, and return an error if it isn't present.
fn get_header<H>(response: &reqwest::Response) -> Result<H>
where
    H: Header,
{
    response
        .headers()
        .typed_try_get::<H>()
        .with_context(|| format!("error parsing {}", H::name()))?
        .ok_or_else(|| format_err!("expected {} header", H::name()))
}

/// An iterator which returns ranges for each chunk in a file.
#[derive(Debug)]
struct ChunkRanges {
    /// The size of chunk we want to return.
    chunk_size: u64,
    /// The total length of our file.
    len: u64,
    /// The place to start our next range.
    next_start: u64,
}

impl Iterator for ChunkRanges {
    type Item = ops::Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start < self.len {
            let end = min(self.next_start + self.chunk_size, self.len);
            let range = self.next_start..end;
            self.next_start = end;
            Some(range)
        } else {
            None
        }
    }
}

/// Return an iterator over successive subranges of a file, each containing
/// `chunk_size` bytes except the last.
fn chunk_ranges(chunk_size: u64, len: u64) -> ChunkRanges {
    assert!(chunk_size > 0);
    ChunkRanges {
        chunk_size,
        len,
        next_start: 0,
    }
}

#[test]
fn chunk_ranges_returns_sequential_ranges() {
    let ranges = chunk_ranges(10, 25).collect::<Vec<_>>();
    assert_eq!(ranges, &[0..10, 10..20, 20..25]);
}
