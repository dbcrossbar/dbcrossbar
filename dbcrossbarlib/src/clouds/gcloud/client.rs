//! A Google Cloud REST client.

use failure::ResultExt;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{self, header::HeaderMap, IntoUrl};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error, fmt};

use super::auth::{authenticator, AccessToken, Authenticator};
use crate::common::*;
use crate::tokio_glue::IdiomaticBytesStream;

/// The OAuth2 scopes that we'll need.
///
/// TODO: For pure storage operations, consider having a storage-only scope.
static SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/bigquery",
];

/// An empty `GET` query.
#[derive(Debug, Serialize)]
pub(crate) struct NoQuery;

/// Alternative media types for Google Cloud REST APIs.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub(crate) enum Alt {
    /// Return data in JSON format.
    Json,
    /// Return the underlying media data.
    Media,
    /// Return data in Protobuf format.
    Proto,
}

/// A Google Cloud REST client using OAuth2.
pub(crate) struct Client {
    /// An authenticator that provides OAuth2 tokens.
    authenticator: Authenticator,

    /// Our HTTP client.
    client: reqwest::Client,
}

impl Client {
    /// Create a new Google Cloud client.
    pub(crate) async fn new(ctx: &Context) -> Result<Client> {
        let authenticator = authenticator(ctx).await?;
        let client = reqwest::Client::new();
        Ok(Client {
            authenticator,
            client,
        })
    }

    /// Make an HTTP GET request and return the response.
    async fn get_helper(
        &self,
        ctx: &Context,
        url: &Url,
        headers: HeaderMap,
    ) -> Result<reqwest::Response> {
        trace!(ctx.log(), "GET {}", url);
        let token = self.token().await?;
        Ok(self
            .client
            .get(url.as_str())
            .bearer_auth(token.as_str())
            .headers(headers)
            .send()
            .await
            .with_context(|_| format!("could not GET {}", url))?)
    }

    /// Make an HTTP GET request with the specified URL and query parameters,
    /// and deserialize the result.
    pub(crate) async fn get<Output, U, Query>(
        &self,
        ctx: &Context,
        url: U,
        query: Query,
    ) -> Result<Output>
    where
        Output: fmt::Debug + DeserializeOwned,
        U: IntoUrl,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        let headers = HeaderMap::default();
        let http_resp = self.get_helper(ctx, &url, headers).await?;
        self.handle_response(ctx, "GET", &url, http_resp).await
    }

    /// Make an HTTP GET request with the specified URL and query parameters,
    /// and return the result as a stream.
    pub(crate) async fn get_response<U, Query>(
        &self,
        ctx: &Context,
        url: U,
        query: Query,
        headers: HeaderMap,
    ) -> Result<reqwest::Response>
    where
        U: IntoUrl,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        let http_resp = self.get_helper(ctx, &url, headers).await?;
        if http_resp.status().is_success() {
            Ok(http_resp)
        } else {
            self.handle_error(ctx, "GET", &url, http_resp).await
        }
    }

    /// Make an HTTP POST request with the specified URL and body.
    pub(crate) async fn post<Output, U, Query, Body>(
        &self,
        ctx: &Context,
        url: U,
        query: Query,
        body: Body,
    ) -> Result<Output>
    where
        Output: fmt::Debug + DeserializeOwned,
        U: IntoUrl,
        Query: fmt::Debug + Serialize,
        Body: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        trace!(ctx.log(), "POST {} {:?}", url, body);
        trace!(ctx.log(), "serialied {}", serde_json::to_string(&body)?);
        let token = self.token().await?;
        let http_resp = self
            .client
            .post(url.as_str())
            .bearer_auth(token.as_str())
            .json(&body)
            .send()
            .await
            .with_context(|_| format!("could not POST {}", url))?;
        self.handle_response(ctx, "POST", &url, http_resp).await
    }

    /// Post a stream of data to the specified URL.
    pub(crate) async fn post_stream<U, Query>(
        &self,
        // Pass `ctx` by value, not reference, because of a weird async lifetime error.
        ctx: Context,
        url: U,
        query: Query,
        stream: IdiomaticBytesStream,
    ) -> Result<()>
    where
        U: IntoUrl,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        trace!(ctx.log(), "POST {} with stream", url);
        let body = reqwest::Body::wrap_stream(stream);
        let token = self.token().await?;
        let http_resp = self
            .client
            .post(url.as_str())
            .bearer_auth(token.as_str())
            .body(body)
            .send()
            .await
            .with_context(|_| format!("could not POST {}", url))?;
        if http_resp.status().is_success() {
            Ok(())
        } else {
            self.handle_error(&ctx, "POST", &url, http_resp).await
        }
    }

    /// Delete the specified URL.
    pub(crate) async fn delete<U, Query>(
        &self,
        ctx: &Context,
        url: U,
        query: Query,
    ) -> Result<()>
    where
        U: IntoUrl,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        trace!(ctx.log(), "DELETE {}", url);
        let token = self.token().await?;
        let http_resp = self
            .client
            .delete(url.as_str())
            .bearer_auth(token.as_str())
            .send()
            .await
            .with_context(|_| format!("error deleting {}", url))?;
        if http_resp.status().is_success() {
            Ok(())
        } else {
            Err(format_err!(
                "error deleting {}: {}",
                url,
                http_resp.status(),
            ))
        }
    }

    /// Get an access token.
    async fn token(&self) -> Result<AccessToken> {
        Ok(self
            .authenticator
            .token(SCOPES)
            .await
            .context("could not get Google Cloud OAuth2 token")?)
    }

    /// Handle an HTTP response.
    async fn handle_response<Output>(
        &self,
        ctx: &Context,
        method: &str,
        url: &Url,
        http_resp: reqwest::Response,
    ) -> Result<Output>
    where
        Output: fmt::Debug + DeserializeOwned,
    {
        if http_resp.status().is_success() {
            let resp = http_resp.json::<Output>().await.with_context(|_| {
                format!("error fetching JSON response from {}", url)
            })?;
            trace!(ctx.log(), "{} returned {:?}", method, resp);
            Ok(resp)
        } else {
            self.handle_error(ctx, method, url, http_resp).await
        }
    }

    /// Handle an HTPP error response.
    ///
    /// This can never return `Ok`. The return type is declared as
    /// `Result<Any>`, but once the [never][] type stabilizes, it should return
    /// `Result<!>`.
    ///
    /// [never]: https://doc.rust-lang.org/std/primitive.never.html
    async fn handle_error<Any>(
        &self,
        ctx: &Context,
        method: &str,
        url: &Url,
        http_resp: reqwest::Response,
    ) -> Result<Any> {
        let resp = http_resp.json::<ErrorResponse>().await.with_context(|_| {
            format!("error fetching JSON error response from {}", url)
        })?;
        trace!(ctx.log(), "{} error {:?}", method, resp);
        let err: Error = resp.error.into();
        Err(err.context(format!("{} error {}", method, url)).into())
    }
}

/// Construct a URL from something we can convert to URL, and something that we
/// can serialize as a query string.
fn build_url<U, Query>(url: U, query: Query) -> Result<Url>
where
    U: IntoUrl,
    Query: fmt::Debug + Serialize,
{
    let mut url = url.into_url().context("could not parse URL")?;
    let query_str = serde_urlencoded::to_string(&query)?;
    if !query_str.is_empty() {
        url.set_query(Some(&query_str));
    }
    Ok(url)
}

/// A Google Cloud error response.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    /// The actual error.
    error: GCloudError,
}

/// Information about a GCloud error.
#[derive(Debug, Deserialize)]
pub(crate) struct GCloudError {
    pub(crate) code: i32,
    pub(crate) message: String,
    pub(crate) errors: Vec<ErrorDetail>,
}

impl fmt::Display for GCloudError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Google Cloud error: {} {}", self.code, self.message)
    }
}

impl error::Error for GCloudError {}

/// Details about an individial GCloud error.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorDetail {
    pub(crate) domain: String,
    pub(crate) reason: String,
    pub(crate) message: String,
    pub(crate) location_type: Option<String>,
    pub(crate) location: Option<String>,
}

/// Percent-encode a string for use as a URL path component.
pub(crate) fn percent_encode<'a>(s: &'a str) -> impl fmt::Display + 'a {
    utf8_percent_encode(s, NON_ALPHANUMERIC)
}
