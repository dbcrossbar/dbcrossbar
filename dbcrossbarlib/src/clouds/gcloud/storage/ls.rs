//! Interfaces to Google Cloud Storage.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::sync::mpsc;

use super::{
    super::{percent_encode, Client},
    parse_gs_url, StorageObject,
};
use crate::common::*;

/// URL query parameters.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery<'a> {
    prefix: &'a str,

    #[serde(skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
}

/// Response body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    kind: String,

    next_page_token: Option<String>,

    #[serde(default)]
    items: Vec<StorageObject>,
}

/// A local helper macro that works like `?`, except that it report errors
/// by sending them to `sender` and returning `Ok(())`.
macro_rules! try_and_forward_errors {
    ($ctx:expr, $expression:expr, $sender:expr) => {
        match $expression {
            Ok(val) => val,
            Err(err) => {
                error!($ctx.log(), "error in gcloud worker: {}", err);
                $sender.send(Err(err.into())).await.map_send_err()?;
                return Ok(());
            }
        }
    };
    ($ctx:expr, $expression:expr, $sender:expr,) => {
        try_and_forward_errors!($ctx, $expression, $sender)
    };
}

/// List all the files at the specified `gs://` URL, recursively.
///
/// See the [documentation][list]. We treat "/" a directory separate, and try to
/// handle prefix matches using ordinary file-system behavior.
///
/// [list]: https://cloud.google.com/storage/docs/json_api/v1/objects/list
pub(crate) async fn ls(
    ctx: &Context,
    url: &Url,
) -> Result<impl Stream<Item = Result<StorageObject>> + Send + Unpin + 'static> {
    debug!(ctx.log(), "listing {}", url);
    let (bucket, object) = parse_gs_url(url)?;

    // We were asked to list `object`, so everything we return should either be
    // `object` itself, or something in a subdirectory.
    let dir_prefix = if object.ends_with('/') {
        object.clone()
    } else {
        format!("{}/", object)
    };

    // Set up a background worker which forwards list output to `sender`. This
    // should also forward all errors to `sender`, except errors that occur when
    // fowarding other errors.
    let (mut sender, receiver) = mpsc::channel::<Result<StorageObject>>(1);
    let worker_ctx = ctx.child(o!("worker" => "gcloud storage ls"));
    let worker: BoxFuture<()> = async move {
        // Make our client.
        let client = try_and_forward_errors!(
            worker_ctx,
            Client::new(&worker_ctx).await,
            sender,
        );

        // Keep track of URLs that we've seen.
        let mut seen = HashSet::new();

        // Keep asking for results until there are no more.
        let req_url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o",
            percent_encode(&bucket),
        );
        let mut page_token = None;
        loop {
            // Set up our request.
            let query = ListQuery {
                prefix: &object,
                page_token: page_token.clone(),
            };

            // Make our request.
            let get_result = client
                .get::<ListResponse, _, _>(&worker_ctx, &req_url, query)
                .await;
            let mut res = try_and_forward_errors!(worker_ctx, get_result, sender);
            let next_page_token = res.next_page_token.take();
            if page_token.is_some() && page_token == next_page_token {
                return Err(format_err!(
                    "tried to list page {:?} of files twice",
                    page_token
                ));
            }
            page_token = next_page_token;

            // Forward the listed objects to the stream.
            for item in res.items {
                // Filter out duplicate items. I don't know whether this
                // actually happens, but it does on AWS.
                if !seen.insert(item.name.clone()) {
                    continue;
                }

                // Filter out non-CSV files.
                if !item.name.to_ascii_lowercase().ends_with(".csv") {
                    continue;
                }

                // Make sure that we either return the file that we were asked
                // for, or something in a subdirectory. We don't want to accidentally
                // return `object + "_trailing"`, but since cloud bucket stores don't
                // actually treat "/" as special, that's what we'll have to do.
                if item.name != object && !item.name.starts_with(&dir_prefix) {
                    trace!(worker_ctx.log(), "filtered false match {:?}", item.name);
                    continue;
                }

                // Send our item.
                sender.send(Ok(item)).await.map_err(|_| {
                    format_err!(
                        "error sending data to stream (perhaps it was closed)",
                    )
                })?;
            }

            // Exit if this is the last page of results.
            if page_token.is_none() {
                break;
            }
        }
        Ok(())
    }
    .boxed();
    ctx.spawn_worker(worker);
    Ok(receiver)
}
