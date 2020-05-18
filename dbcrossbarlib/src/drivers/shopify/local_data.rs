//! Fetch data from Shopify and convert to CSV.

use common_failures::display::DisplayCausesAndBacktraceExt;
use itertools::Itertools;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, str::FromStr};
use tokio::{
    sync::mpsc::Sender,
    time::{delay_for, Duration},
};

use super::{json_to_csv::write_rows, ShopifyLocator};
use crate::common::*;
use crate::credentials::CredentialsManager;
use crate::tokio_glue::{box_stream_once, bytes_channel, SendResultExt};

pub(crate) async fn local_data_helper(
    ctx: Context,
    source: ShopifyLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let shared_args = shared_args.verify(ShopifyLocator::features())?;
    let _source_args = source_args.verify(ShopifyLocator::features())?;
    let schema = shared_args.schema().to_owned();

    // Get our HTTPS URL, and specify that we always want the maximum number of records per request, because
    // we're rate-limited per request, not per-record.
    let mut url = source.to_https_url()?;
    url.query_pairs_mut().append_pair("limit", "250").finish();

    // Get our credentials.
    let creds = CredentialsManager::singleton()
        .get("shopify_secret")
        .await?;
    let auth_token = creds.get("auth_token")?.to_owned();

    // Loop over pages until we run out.
    let mut include_headers = true;
    let worker_ctx = ctx.clone();
    let (mut sender, receiver) = bytes_channel(1);
    let worker: BoxFuture<()> = async move {
        let client = Client::new();
        let mut next_url = url.clone();
        loop {
            // Query Shopify and forward any errors to our consumer
            let result = get_shopify_response(
                &worker_ctx,
                &client,
                next_url,
                auth_token.to_owned(),
            )
            .await;
            let resp = match result {
                Ok(resp) => resp,
                Err(err) => {
                    error!(
                        worker_ctx.log(),
                        "ERROR: {}",
                        err.display_causes_without_backtrace(),
                    );
                    sender.send(Err(err)).await.map_send_err()?;
                    return Ok(());
                }
            };

            // Convert our data to CSV and send it, bailing if we hit an error.
            if let Err(err) = convert_rows_to_csv_and_send(
                &mut sender,
                &schema,
                resp.rows,
                include_headers,
            )
            .await
            {
                sender.send(Err(err)).await.map_send_err()?;
                return Ok(());
            }
            include_headers = false;

            // Figure out what to do next.
            if let Some(next_page_url) = resp.next_page_url {
                next_url = next_page_url;

                // If we're starting to overheat, wait a full second, giving enough
                // time regenerate at least 2 API calls worth of credit.
                if resp.call_limit.should_wait() {
                    delay_for(Duration::from_millis(1000)).await;
                }
            } else {
                // No more pages of data to fetch!
                return Ok::<_, Error>(());
            }
        }
    }
    .boxed();
    ctx.spawn_worker(worker);

    Ok(Some(box_stream_once(Ok(CsvStream {
        name: "data".to_owned(),
        data: receiver.boxed(),
    }))))
}

/// A parsed response from Shopify.
#[derive(Debug)]
struct ShopifyResponse {
    /// How much of our API have we used?
    call_limit: CallLimit,

    /// The URL of the next page of data.
    next_page_url: Option<Url>,

    /// Individual data rows.
    rows: Vec<Value>,
}

/// A Shopify "call limit", specifying how much of our API quota we've used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CallLimit {
    /// How much of our call limit have we used?
    used: u32,
    /// How much is remaining?
    limit: u32,
}

impl CallLimit {
    /// Are we close enough to our call limit that we should chill out a bit?
    fn should_wait(self) -> bool {
        self.used.saturating_mul(2) >= self.limit
    }
}

impl FromStr for CallLimit {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(split_pos) = s.find('/') {
            let used = s[..split_pos]
                .parse::<u32>()
                .with_context(|_| format!("could not parse call limit {:?}", s))?;
            let limit = s[split_pos + 1..]
                .parse::<u32>()
                .with_context(|_| format!("could not parse call limit {:?}", s))?;
            Ok(CallLimit { used, limit })
        } else {
            Err(format_err!("could not parse call limit {:?}", s))
        }
    }
}

#[test]
fn parse_call_limit() {
    let cl = CallLimit::from_str("2/10").unwrap();
    assert_eq!(cl, CallLimit { used: 2, limit: 10 });
}

/// Shopify wraps all responses in single-item objects, but we don't know the
/// field name. So we need a smart deserialization wrapper.
#[derive(Deserialize)]
#[serde(transparent)]
struct RowsJson(HashMap<String, Vec<Value>>);

impl RowsJson {
    /// If we only have a single key in our `HashMap`, return the corresponding value.
    fn into_rows(self) -> Result<Vec<Value>> {
        if self.0.len() == 1 {
            Ok(self
                .0
                .into_iter()
                .next()
                .expect("checked for exactly one value, didn't find it")
                .1)
        } else {
            Err(format_err!(
                "found multiple keys in Shopify response: {}",
                self.0.keys().join(",")
            ))
        }
    }
}

/// Given an HTTPS URL, look return the Shopify response.
async fn get_shopify_response(
    ctx: &Context,
    client: &Client,
    url: Url,
    auth_token: String,
) -> Result<ShopifyResponse> {
    let ctx = ctx.child(o!("shopify_url" => url.to_string()));
    debug!(ctx.log(), "Fetching Shopify data");

    // Fetch the next page.
    let resp: reqwest::Response = client
        .get(url)
        .header("X-Shopify-Access-Token", auth_token)
        .send()
        .await
        .context("error accessing Shopify REST API")?;

    if resp.status().is_success() {
        // Parse our call limit.
        let call_limit = resp
            .headers()
            .get("x-shopify-shop-api-call-limit")
            .ok_or_else(|| {
                format_err!("could not find x-shopify-shop-api-call-limit header")
            })?
            .to_str()
            .context("could not convert x-shopify-shop-api-call-limit to string")?
            .parse::<CallLimit>()?;

        // Look for a "next page" URL in the Link header. This is actually
        // tricky to parse correctly, so we'll use an external library.
        let next_page_url =
            if let Some(link) = resp.headers().get("link") {
                let link = link
                    .to_str()
                    .context("could not convert link header to string")?;
                let links = parse_link_header::parse(link)
                    .map_err(|_| format_err!("error parsing Link header"))?;
                if let Some(next) = links.get(&Some("next".to_owned())) {
                    Some(Url::from_str(&next.uri.to_string()).with_context(|_| {
                        format_err!("could not parse URL {:?}", next)
                    })?)
                } else {
                    None
                }
            } else {
                None
            };
        let rows_json = resp
            .json::<RowsJson>()
            .await
            .context("error fetching Shopify data")?;

        Ok(ShopifyResponse {
            call_limit,
            next_page_url,
            rows: rows_json.into_rows()?,
        })
    } else {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("error reading Shopify error response")?;
        Err(format_err!(
            "could not read data from Shopify: {} {}",
            status,
            body,
        ))
    }
}

/// Convert rows to CSV and send them.
async fn convert_rows_to_csv_and_send(
    sender: &mut Sender<Result<BytesMut>>,
    schema: &Table,
    rows: Vec<Value>,
    include_headers: bool,
) -> Result<()> {
    // Convert our rows to CSV.
    let mut buffer = Vec::with_capacity(8 * 1024);
    write_rows(&mut buffer, schema, rows, include_headers)?;

    // Convert to `BytesMut` and send.
    //
    // TODO: If we switched our main bytes type from `BytesMut` to `Bytes`, this
    // could be done more cheaply.
    let bytes = BytesMut::from(&buffer[..]);
    sender.send(Ok(bytes)).await.map_send_err()?;
    Ok(())
}
