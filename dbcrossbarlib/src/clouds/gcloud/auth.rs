//! Authentication support for Google Cloud.

use hyper::{self, client::connect::HttpConnector};
use hyper_rustls::HttpsConnector;
use std::path::PathBuf;
use tokio::fs;
pub(crate) use yup_oauth2::AccessToken;
use yup_oauth2::{
    ApplicationSecret, ConsoleApplicationSecret, InstalledFlowReturnMethod,
    ServiceAccountKey,
};

use crate::common::*;
use crate::credentials::CredentialsManager;

/// The connector type used to create `hyper` connections.
pub(crate) type HyperConnector = HttpsConnector<HttpConnector>;

pub(crate) type Authenticator =
    yup_oauth2::authenticator::Authenticator<HyperConnector>;

/// The path to the file where we store our OAuth2 tokens.
async fn token_file_path() -> Result<PathBuf> {
    let data_local_dir = dirs::data_local_dir().ok_or_else(|| {
        format_err!("cannot find directory to store authentication keys")
    })?;
    // `yup_oauth2` will fail with a cryptic error if the containing directory
    // doesn't exist.
    fs::create_dir_all(&data_local_dir)
        .await
        .with_context(|_| {
            format!("could not create directory {}", data_local_dir.display())
        })?;
    Ok(data_local_dir.join("dbcrossbar-gcloud-oauth2.json"))
}

/// Get the service account key needed to connect a server app to BigQuery.
async fn service_account_key() -> Result<ServiceAccountKey> {
    let creds = CredentialsManager::singleton()
        .get("gcloud_service_account_key")
        .await?;
    Ok(serde_json::from_str(creds.get("value")?)
        .context("could not parse service account key")?)
}

/// Build an authenticator using service account credentials.
async fn service_account_authenticator() -> Result<Authenticator> {
    Ok(
        yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key().await?)
            .persist_tokens_to_disk(token_file_path().await?)
            .build()
            .await
            .context("failed to create authenticator")?,
    )
}

/// Get the application secret needed to connect an interactive app to Google Cloud.
///
/// This is intended for use in a CLI application that
/// gets distributed to end users, but it's not actually enough to authenticate
/// against Google Cloud itself. It needs to be used together with a
/// browser-based OAuth2 confirmation.
async fn application_secret() -> Result<ApplicationSecret> {
    let creds = CredentialsManager::singleton()
        .get("gcloud_client_secret")
        .await?;
    serde_json::from_str::<ConsoleApplicationSecret>(creds.get("value")?)
        .context("could not parse client secret")?
        .installed
        .ok_or_else(|| format_err!("client secret does not contain `installed` key"))
}

/// Build an interactive authenticator for a CLI tool.
async fn installed_flow_authenticator() -> Result<Authenticator> {
    Ok(yup_oauth2::InstalledFlowAuthenticator::builder(
        application_secret().await?,
        InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_file_path().await?)
    .build()
    .await
    .context("failed to create authenticator")?)
}

/// Create an authenticator using service account credentials if available, and
/// interactive credentials otherwise.
pub(crate) async fn authenticator(ctx: &Context) -> Result<Authenticator> {
    match service_account_authenticator().await {
        // We have a service account configured, so use it.
        Ok(auth) => Ok(auth),
        Err(err) => {
            trace!(
                ctx.log(),
                "no service account found, using interactive auth: {}",
                err,
            );
            installed_flow_authenticator().await
        }
    }
}
