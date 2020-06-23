//! Authentication support for Google Cloud.

use hyper::{self, client::connect::HttpConnector};
use hyper_rustls::HttpsConnector;
use sha2::{Digest, Sha256};
use std::{
    fmt::Write,
    path::{Path, PathBuf},
};
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

/// Convert `s` into a hexadecimal digest using a hash function.
///
/// The details of this hash function don't matter. We only care that it returns
/// something that's safe to include in a file name, and that has an extremely
/// low probability of colliding.
fn string_to_hex_digest(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let bytes = hasher.finalize();
    let mut out = String::with_capacity(2 * bytes.len());
    for b in bytes {
        write!(&mut out, "{:02x}", b).expect("write should never fail");
    }
    out
}

/// The path to the file where we store our OAuth2 tokens.
///
/// This should be unique per `token_id` (at least with extremely high probability).
async fn token_file_path(token_id: &str) -> Result<PathBuf> {
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
    let filename = format!("gcloud-oauth2-{}.json", string_to_hex_digest(token_id));
    Ok(data_local_dir.join("dbcrossbar").join(filename))
}

/// Make sure the parent directory of `path` exists.
async fn ensure_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Get the service account key needed to connect a server app to BigQuery.
async fn service_account_key() -> Result<ServiceAccountKey> {
    let creds = CredentialsManager::singleton()
        .get("gcloud_service_account_key")
        .await?;
    Ok(serde_json::from_str(creds.get_required("value")?)
        .context("could not parse service account key")?)
}

/// Build an authenticator using service account credentials.
async fn service_account_authenticator() -> Result<Authenticator> {
    let service_account_key = service_account_key().await?;
    // We're going to use the private key ID to indentify our stored token. As far
    // as I can tell, this is not especially sensitive information.
    let key_id = service_account_key.private_key_id.as_ref().ok_or_else(|| {
        format_err!("could not find private_key_id for GCloud service account key")
    })?;
    let token_file_path = token_file_path(key_id).await?;
    ensure_parent_directory(&token_file_path).await?;
    Ok(
        yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key)
            .persist_tokens_to_disk(token_file_path)
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
    serde_json::from_str::<ConsoleApplicationSecret>(creds.get_required("value")?)
        .context("could not parse client secret")?
        .installed
        .ok_or_else(|| format_err!("client secret does not contain `installed` key"))
}

/// Build an interactive authenticator for a CLI tool.
async fn installed_flow_authenticator() -> Result<Authenticator> {
    let application_secret = application_secret().await?;
    // Keying our token file path by the client ID seems to work here, because
    // there might be multiple client IDs passed in as env vars at different
    // times, but scoping session keys to the client ID seems reasonable.
    let token_file_path = token_file_path(&application_secret.client_id).await?;
    ensure_parent_directory(&token_file_path).await?;
    Ok(yup_oauth2::InstalledFlowAuthenticator::builder(
        application_secret,
        InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_file_path)
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
