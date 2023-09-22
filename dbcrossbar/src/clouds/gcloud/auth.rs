//! Authentication support for Google Cloud.

use bigml::wait::{wait, BackoffType, WaitOptions, WaitStatus};
use hyper::{self, client::connect::HttpConnector};
use hyper_rustls::HttpsConnector;
use sha2::{Digest, Sha256};
use std::{
    fmt::Write,
    path::{Path, PathBuf},
    process,
    time::Duration,
};
use tokio::fs;
pub(crate) use yup_oauth2::AccessToken;
use yup_oauth2::{
    authenticator::ApplicationDefaultCredentialsTypes,
    authorized_user::AuthorizedUserSecret, ApplicationDefaultCredentialsFlowOpts,
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
    fs::create_dir_all(&data_local_dir).await.with_context(|| {
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
    serde_json::from_str(creds.get_required("value")?)
        .context("could not parse service account key")
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

    // Create our authenticator and retry any failures. (This will generally
    // fail at least once in a 10 hour copy.)s
    let opt = WaitOptions::default()
        .backoff_type(BackoffType::Exponential)
        .retry_interval(Duration::from_secs(1))
        // I'd like to make this at least 5 but not until we can distinguish
        // between temporary or permanent failures.
        .allowed_errors(4);
    let authenticator = wait(&opt, move || {
        let service_account_key = service_account_key.clone();
        let token_file_path = token_file_path.clone();
        async move {
            let result =
                yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key)
                    .persist_tokens_to_disk(token_file_path)
                    .build()
                    .await;

            match result {
                Ok(value) => WaitStatus::Finished(value),
                Err(err) => {
                    // TODO: We should do a better job of distinguishing between
                    // temporary and permanent failures here. Because we
                    // classify all failures as temporary, something like a
                    // "user does not exist" error will be retried repeatedly,
                    // silently hanging the program for a while, even though
                    // there is no chance that such as error would ever succeed.
                    WaitStatus::FailedTemporarily(
                        Error::new(err).context("failed to create authenticator"),
                    )
                }
            }
        }
    })
    .await?;
    Ok(authenticator)
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
    info!("building InstalledFlowAuthenticator");
    yup_oauth2::InstalledFlowAuthenticator::builder(
        application_secret,
        InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_file_path)
    .build()
    .await
    .context("failed to create authenticator")
}

async fn authorized_user_secret() -> Result<AuthorizedUserSecret> {
    let creds = CredentialsManager::singleton()
        .get("gcloud_authorized_user_secret")
        .await?;
    serde_json::from_str(creds.get_required("value")?)
        .context("could not parse autorized user secret")
}

/// Build an authenticator for a user logged in with
/// "gcloud auth application-default login""
async fn authorized_user_authenticator() -> Result<Authenticator> {
    let authorized_user_secret = authorized_user_secret().await?;
    // Keying our token file path by the client ID seems to work here, because
    // there might be multiple client IDs passed in as env vars at different
    // times, but scoping session keys to the client ID seems reasonable.
    let token_file_path = token_file_path(&authorized_user_secret.client_id).await?;
    ensure_parent_directory(&token_file_path).await?;
    info!("building InstalledFlowAuthenticator");
    yup_oauth2::AuthorizedUserAuthenticator::builder(authorized_user_secret)
        .persist_tokens_to_disk(token_file_path)
        .build()
        .await
        .context("failed to create authenticator")
}

/// Build an authenticator for application default credentials
/// This will forst look for a service account key stored in the
/// location indicated by the $GOOGLE_APPLICATION_CREDENTIALS
/// env variable. If that is not defined, or authentication fails,
/// it will assume we're running on a Google Compute Engine instance,
/// and query its metadata service.
async fn application_default_authenticator() -> Result<Authenticator> {
    // Keying our token file path by the current process id. We don't have any information
    // about which user will eventually be authenticated at this point
    let token_file_path = token_file_path(&format!("pid-{}", process::id())).await?;
    ensure_parent_directory(&token_file_path).await?;
    info!("building ApplicationDefaultCredentialsAuthenticator");
    let adc_authenticator =
        yup_oauth2::ApplicationDefaultCredentialsAuthenticator::builder(
            ApplicationDefaultCredentialsFlowOpts { metadata_url: None },
        );

    match adc_authenticator.await {
        ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => auth
            .persist_tokens_to_disk(token_file_path)
            .build()
            .await
            .context("failed to create instance metadata authenticator"),
        ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => auth
            .persist_tokens_to_disk(token_file_path)
            .build()
            .await
            .context("failed to create service account authenticator"),
    }
}

/// Create an authenticator using service account credentials if available, and
/// interactive credentials otherwise.
#[instrument(level = "trace")]
pub(crate) async fn authenticator() -> Result<Authenticator> {
    match service_account_authenticator().await {
        // We have a service account configured, so use it.
        Ok(auth) => Ok(auth),
        Err(err) => {
            trace!(
                "trying \"installed flow\" auth because service account auth failed because: {:?}",
                err,
            );
            match installed_flow_authenticator().await {
                Ok(auth) => Ok(auth),
                Err(err) => {
                    trace!(
                        "trying \"application default credentials\" auth because installed flow auth failed because: {:?}",
                        err,
                    );
                    match application_default_authenticator().await {
                        Ok(auth) => Ok(auth),
                        Err(err) => {
                            trace!(
                                "trying \"authorized user\" auth because application default credentials auth failed because: {:?}",
                                err,
                            );
                            authorized_user_authenticator().await
                        }
                    }
                }
            }
        }
    }
}
