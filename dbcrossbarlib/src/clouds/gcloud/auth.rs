//! Authentication support for Google Cloud.

use dirs;
use hyper::{self, client::connect::HttpConnector};
use hyper_rustls::HttpsConnector;
use serde_json;
use std::{env, path::PathBuf};
pub(crate) use yup_oauth2::AccessToken;
use yup_oauth2::{
    ApplicationSecret, ConsoleApplicationSecret, InstalledFlowReturnMethod,
    ServiceAccountKey,
};

use crate::common::*;

/// The connector type used to create `hyper` connections.
pub(crate) type HyperConnector = HttpsConnector<HttpConnector>;

pub(crate) type Authenticator =
    yup_oauth2::authenticator::Authenticator<HyperConnector>;

/// The path to the file where we store our OAuth2 tokens.
fn token_file_path() -> Result<PathBuf> {
    Ok(dirs::data_local_dir()
        .ok_or_else(|| {
            format_err!("cannot find directory to store authentication keys")
        })?
        .join("dbcrossbar-gcloud-oauth2.json"))
}

/// Get the service account key needed to connect a server app to BigQuery.
fn service_account_key() -> Result<ServiceAccountKey> {
    let json = env::var("GCLOUD_SERVICE_ACCOUNT_KEY")
        .context("could not find GCLOUD_SERVICE_ACCOUNT_KEY value")?;
    Ok(serde_json::from_str(&json)
        .context("could not parse GCLOUD_SERVICE_ACCOUNT_KEY value")?)
}

/// Build an authenticator using service account credentials.
async fn service_account_authenticator() -> Result<Authenticator> {
    Ok(
        yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key()?)
            .persist_tokens_to_disk(token_file_path()?)
            .build()
            .await
            .context("failed to create authenticator")?,
    )
}

/// Our OAuth2 client secret. This is intended for use in a CLI application that
/// gets distributed to end users, but it's not actually enough to authenticate
/// against Google Cloud itself. It needs to be used together with a
/// browser-based OAuth2 confirmation.
const CLIENT_SECRET: &str = ""; // include_str!("client_secret.json");

/// Get the application secret needed to connect an interactive app to BigQuery.
fn application_secret() -> Result<ApplicationSecret> {
    serde_json::from_str::<ConsoleApplicationSecret>(CLIENT_SECRET)
        .context("built-in client_secret.json is invalid")?
        .installed
        .ok_or_else(|| {
            format_err!("built-in client_secret.json does not contain `installed` key")
        })
}

/// Build an interactive authenticator for a CLI tool.
#[allow(dead_code)]
async fn installed_flow_authenticator() -> Result<Authenticator> {
    Ok(yup_oauth2::InstalledFlowAuthenticator::builder(
        application_secret()?,
        InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_file_path()?)
    .build()
    .await
    .context("failed to create authenticator")?)
}

/// Create an authenticator using service account credentials if available, and
/// interactive credentials otherwise.
pub(crate) async fn authenticator(_ctx: &Context) -> Result<Authenticator> {
    // We do not yet have an approved `client_secret.json`, so just require a
    // service account for now.
    service_account_authenticator().await

    //match service_account_authenticator().await {
    //    // We have a service account configured, so use it.
    //    Ok(auth) => Ok(auth),
    //    Err(err) => {
    //        trace!(
    //            ctx.log(),
    //            "no service account found, using interactive auth: {}",
    //            err,
    //        );
    //        installed_flow_authenticator().await
    //    }
    //}
}
