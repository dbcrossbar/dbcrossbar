//! A Google Cloud REST client.

use crate::wait::{wait, WaitOptions, WaitStatus};
use hyper::StatusCode;
use mime::{self, Mime};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{
    self,
    header::{HeaderMap, CONTENT_TYPE},
    IntoUrl,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error, fmt, time::Duration};

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

/// An HTTP client error. We break out a few specified statuses our caller might
/// care about.
#[derive(Debug)]
pub(crate) enum ClientError {
    /// The resource at URL was not found.
    NotFound { method: String, url: Url },
    /// Another error occured. We don't currently care about the details.
    Other(Error),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NotFound { method, url } => {
                write!(f, "cannot {} {}: Not Found", method, url)
            }
            ClientError::Other(err) => err.fmt(f),
        }
    }
}

impl error::Error for ClientError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            ClientError::NotFound { .. } => None,
            ClientError::Other(err) => err.source(),
        }
    }
}

impl From<Error> for ClientError {
    fn from(err: Error) -> Self {
        ClientError::Other(err)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(err: serde_json::Error) -> Self {
        ClientError::Other(err.into())
    }
}


/// Is it safe to retry a request? This should always be true for GET requests,
/// but by default POST requests are not safe to retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Idempotency {
    SafeToRetry,
    UnsafeToRetry,
}

/// A Google Cloud REST client using OAuth2.
#[derive(Clone)]
pub(crate) struct Client {
    /// OAuth2 scopes needed for this client. We allow overriding these because
    /// there are some weird kinds of BigQuery tables (including Google
    /// Drive-mapped tables) that require special scopes.
    scopes: Vec<String>,

    /// An authenticator that provides OAuth2 tokens.
    authenticator: Authenticator,

    /// Our HTTP client.
    client: reqwest::Client,
}

impl Client {
    /// Create a new Google Cloud client.
    #[instrument(level = "trace")]
    pub(crate) async fn new(extra_scopes: &[String]) -> Result<Client, ClientError> {
        let scopes = SCOPES
            .iter()
            .cloned()
            .map(str::to_string)
            .chain(extra_scopes.iter().cloned())
            .collect::<Vec<_>>();
        let authenticator = authenticator().await?;
        let client = reqwest::Client::new();
        Ok(Client {
            scopes,
            authenticator,
            client,
        })
    }

    /// Make an HTTP GET request and return the response.
    async fn get_helper(
        &self,
        url: &Url,
        headers: HeaderMap,
    ) -> Result<reqwest::Response, ClientError> {
        trace!("GET {}", url);
        let token = self.token().await?;
        let wait_options = WaitOptions::default()
            .retry_interval(Duration::from_secs(10))
            // Don't retry too much because we're probably classifying some
            // permanent errors as temporary.
            .allowed_errors(3);
        wait(&wait_options, move || {
            let token = token.clone();
            let headers = headers.clone();
            async move {
                let resp_result = self
                    .client
                    .get(url.as_str())
                    .bearer_auth(token.as_str())
                    .headers(headers)
                    .send()
                    .await;
                self.response_to_wait_status(
                    "GET",
                    url,
                    // HTTP defines GET as idempotent, and we believe Google
                    // follows this convention in their APIs.
                    Idempotency::SafeToRetry,
                    resp_result,
                )
                .await
            }
            .boxed()
        })
        .await
    }

    /// Make an HTTP GET request with the specified URL and query parameters,
    /// and deserialize the result.
    #[instrument(level = "trace", skip(self))]
    pub(crate) async fn get<Output, U, Query>(
        &self,
        url: U,
        query: Query,
    ) -> Result<Output, ClientError>
    where
        Output: fmt::Debug + DeserializeOwned,
        U: IntoUrl + fmt::Debug,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        let headers = HeaderMap::default();
        let http_resp = self.get_helper(&url, headers).await?;
        self.handle_response("GET", &url, http_resp).await
    }

    /// Make an HTTP GET request with the specified URL and query parameters,
    /// and return the result as a stream.
    #[instrument(level = "trace", skip(self, headers))]
    pub(crate) async fn get_response<U, Query>(
        &self,
        url: U,
        query: Query,
        headers: HeaderMap,
    ) -> Result<reqwest::Response, ClientError>
    where
        U: IntoUrl + fmt::Debug,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        let http_resp = self.get_helper(&url, headers).await?;
        if http_resp.status().is_success() {
            Ok(http_resp)
        } else {
            Err(self.handle_error("GET", &url, http_resp).await)
        }
    }

    /// Make an HTTP POST request with the specified URL and body.
    ///
    ///
    /// This may POST the request multiple times, which may cause an action to be
    /// performed multiple times. The caller is responsible for ensuring that the action
    /// is idempotent (perhaps by using `IF NOT EXISTS` for an SQL operation), and
    /// being ready to handle the case where the underlying action succeeds, but the
    /// server still returns an error.
    #[instrument(level = "trace", skip(self, body))]
    pub(crate) async fn post<Output, U, Query, Body>(
        &self,
        url: U,
        idempotency: Idempotency,
        query: Query,
        body: Body,
    ) -> Result<Output, ClientError>
    where
        Output: fmt::Debug + DeserializeOwned,
        U: IntoUrl + fmt::Debug,
        Query: fmt::Debug + Serialize,
        Body: fmt::Debug + Serialize + Sync + Send,
    {
        let url = build_url(url, query)?;
        trace!("POST {} {:?}", url, body);
        trace!("serialied {}", serde_json::to_string(&body)?);

        let token = self.token().await?;
        let wait_options = WaitOptions::default()
            // We could probably make this shorter than 4 for Google Cloud, but
            // the `bigml` crate that we're currently using for `wait` rounds
            // all short intervals up to 4 anyway.
            .retry_interval(Duration::from_secs(4))
            // Don't retry too much because we're probably classifying some
            // permanent errors as temporary.
            .allowed_errors(4);

        let token_ref = &token;
        let url_ref = &url;
        let body_ref = &body;
        let http_resp = wait(&wait_options, move || {
            async move {
                let resp_result = self
                    .client
                    .post(url_ref.as_str())
                    .bearer_auth(token_ref.as_str())
                    .json(body_ref)
                    .send()
                    .await;
                self.response_to_wait_status("POST", url_ref, idempotency, resp_result)
                    .await
            }
            .boxed()
        })
        .await?;
        self.handle_response("POST", &url, http_resp).await
    }

    /// Post a stream of data to the specified URL.
    #[instrument(level = "trace", skip(self, stream))]
    pub(crate) async fn post_stream<U, Query>(
        &self,
        url: U,
        query: Query,
        stream: IdiomaticBytesStream,
    ) -> Result<(), ClientError>
    where
        U: IntoUrl + fmt::Debug,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        trace!("POST {} with stream", url);
        let body = reqwest::Body::wrap_stream(stream);
        let token = self.token().await?;
        let http_resp = self
            .client
            .post(url.as_str())
            .bearer_auth(token.as_str())
            .body(body)
            .send()
            .await
            .with_context(|| format!("could not POST {}", url))?;
        if http_resp.status().is_success() {
            Ok(())
        } else {
            Err(self.handle_error("POST", &url, http_resp).await)
        }
    }

    /// Delete the specified URL.
    #[instrument(level = "trace", skip(self))]
    pub(crate) async fn delete<U, Query>(
        &self,
        url: U,
        query: Query,
    ) -> Result<(), ClientError>
    where
        U: IntoUrl + fmt::Debug,
        Query: fmt::Debug + Serialize,
    {
        let url = build_url(url, query)?;
        trace!("DELETE {}", url);
        let token = self.token().await?;
        let http_resp = self
            .client
            .delete(url.as_str())
            .bearer_auth(token.as_str())
            .send()
            .await
            .with_context(|| format!("error deleting {}", url))?;
        if http_resp.status().is_success() {
            Ok(())
        } else {
            Err(self.handle_error("DELETE", &url, http_resp).await)
        }
    }

    /// Get an access token.
    #[instrument(level = "trace", skip(self))]
    async fn token(&self) -> Result<AccessToken> {
        self.authenticator
            .token(&self.scopes)
            .await
            .context("could not get Google Cloud OAuth2 token")
    }

    /// Is this HTTP status code something we should retry?
    ///
    /// Our policy for retries in a distributed system is basically "Don't retry
    /// things you haven't seen fail in practice." This means that our caller
    /// will get rapid feedback for configuration or user errors. It also means
    /// that latency-to-error in complex distributed systems will be as short as
    /// possible. And it minimized "retry amplification" where our target system
    /// is overloaded, it fails, and we send a bunch of retries to which
    /// overload it more. This is why we're conservative.
    fn should_retry_status_code(&self, status_code: &StatusCode) -> bool {
        [
            // 503: This seems to happen pretty commonly, according to our logs.
            StatusCode::SERVICE_UNAVAILABLE,
            // 403: Google has a number of different rate limits, and if you
            // exceed them, it will send errors like:
            //
            // > 403 Exceeded rate limits: too many api requests per user per
            // > method for this user_method
            //
            // We want to retry just the 403s coming from rate limits, not the
            // 403s coming from other kinds of "forbidden" errors. So in a
            // perfect world, we'd look at this error in more detail and retry
            // it more narrowly.
            StatusCode::FORBIDDEN,
            // The following are things we _might_ want to retry. But as noted
            // above, we're waiting to see them in practice, especially because
            // we haven't dug into either the exact HTTP semantics or the
            // observed behavior of Google Cloud.
            //
            // StatusCode::TOO_MANY_REQUESTS,   //429
            // StatusCode::BAD_GATEWAY,         //502
            // StatusCode::GATEWAY_TIMEOUT,     //504
        ]
        .contains(status_code)
    }

    /// Convert an HTTP response into a [`WaitStatus`].
    async fn response_to_wait_status(
        &self,
        method: &str,
        url: &Url,
        idempotency: Idempotency,
        response_result: Result<reqwest::Response, reqwest::Error>,
    ) -> WaitStatus<reqwest::Response, ClientError> {
        match response_result {
            // The HTTP request failed outright, because of something
            // like a DNS error or whatever.
            Err(err) => {
                // Request and timeout errors look like the kind of
                // things we should probably retry. But this is based on
                // guesswork not experience.
                let temporary = idempotency == Idempotency::SafeToRetry
                    && (err.is_request() || err.is_timeout());
                let err: Error = err.into();
                let err: ClientError =
                    err.context(format!("could not {} {}", method, url)).into();
                if temporary {
                    WaitStatus::FailedTemporarily(err)
                } else {
                    WaitStatus::FailedPermanently(err)
                }
            }

            // We talked to the server and it returned a server-side
            // error that we expect to be transient so should retry.
            Ok(resp)
                if idempotency == Idempotency::SafeToRetry
                    && self.should_retry_status_code(&resp.status()) =>
            {
                WaitStatus::FailedTemporarily(
                    self.handle_error(method, url, resp).await,
                )
            }

            // We talked to the server and it returned a server-side
            // error (50-599). There's a chance that things might work
            // next time, but we're not sure so we'll just fail.
            Ok(resp) if resp.status().is_server_error() => {
                WaitStatus::FailedPermanently(
                    self.handle_error(method, url, resp).await,
                )
            }
            Ok(resp) => WaitStatus::Finished(resp),
        }
    }

    /// Handle an HTTP response.
    async fn handle_response<Output>(
        &self,
        method: &str,
        url: &Url,
        http_resp: reqwest::Response,
    ) -> Result<Output, ClientError>
    where
        Output: fmt::Debug + DeserializeOwned,
    {
        if http_resp.status().is_success() {
            let resp = http_resp.json::<Output>().await.with_context(|| {
                format!("error fetching JSON response from {}", url)
            })?;
            trace!("{} returned {:?}", method, resp);
            Ok(resp)
        } else {
            Err(self.handle_error(method, url, http_resp).await)
        }
    }

    /// Handle an HTPP error response.
    async fn handle_error(
        &self,
        method: &str,
        url: &Url,
        http_resp: reqwest::Response,
    ) -> ClientError {
        // Return 404 Not Found as a special case.
        if http_resp.status() == StatusCode::NOT_FOUND {
            return ClientError::NotFound {
                method: method.to_owned(),
                url: url.to_owned(),
            };
        }

        // Decide if we should even try to parse this response as JSON before we
        // consume our http_resp.
        let should_parse_as_json = response_claims_to_be_json(&http_resp);

        // Fetch the error body.
        let err_body_result = http_resp
            .bytes()
            .await
            .with_context(|| format!("error fetching error response from {}", url));
        let err_body = match err_body_result {
            Ok(err_body) => err_body,
            Err(err) => return err.into(),
        };

        // Try to return a nice JSON error.
        if should_parse_as_json {
            if let Ok(resp) = serde_json::from_slice::<ErrorResponse>(&err_body) {
                trace!("{} error {:?}", method, resp);
                let err: Error = resp.error.into();
                return err.context(format!("{} error {}", method, url)).into();
            }
        }

        // We've run afoul of
        // https://github.com/googleapis/google-cloud-ruby/issues/5180 or
        // something equally terrible, so just report whatever we have.
        let raw_err = String::from_utf8_lossy(&err_body);
        trace!(
            "{} {}: expected JSON describing error, but got {:?}",
            method,
            url,
            raw_err,
        );
        let err = format_err!("expected JSON describing error, but got {:?}", raw_err);
        err.context(format!("{} error {}", method, url)).into()
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) struct ErrorDetail {
    pub(crate) domain: String,
    pub(crate) reason: String,
    pub(crate) message: String,
    pub(crate) location_type: Option<String>,
    pub(crate) location: Option<String>,
}

/// Percent-encode a string for use as a URL path component.
pub(crate) fn percent_encode(s: &str) -> impl fmt::Display + '_ {
    utf8_percent_encode(s, NON_ALPHANUMERIC)
}

/// Returns `true` if `http_response` claims to be a JSON response.
pub(crate) fn response_claims_to_be_json(http_resp: &reqwest::Response) -> bool {
    let content_type = match http_resp.headers().get(CONTENT_TYPE) {
        Some(content_type) => content_type,
        None => return false,
    };
    let content_type_str = match content_type.to_str() {
        Ok(content_type_str) => content_type_str,
        Err(err) => {
            error!("Non-ASCII content type {:?}: {}", content_type, err);
            return false;
        }
    };
    let content_type_mime = match content_type_str.parse::<Mime>() {
        Ok(content_type_mime) => content_type_mime,
        Err(err) => {
            error!(
                "Could not parse content type {:?}: {}",
                content_type_str, err,
            );
            return false;
        }
    };
    content_type_mime.type_() == mime::APPLICATION
        && content_type_mime.subtype() == mime::JSON
}

/// Given an `Error`, look to see if it's a wrapper around `reqwest::Error`, and
/// if so, return the original error. Otherwise return `None`.
pub(crate) fn original_http_error(err: &Error) -> Option<&reqwest::Error> {
    // Walk the chain of all errors, ending with the original root cause.
    for cause in err.chain() {
        // If this error is a `reqwest::Error`, return it.
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            return Some(reqwest_error);
        }
    }
    None
}
