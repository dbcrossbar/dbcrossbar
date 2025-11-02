//! Authentication support for Google Cloud.

use gcp_auth::{CustomServiceAccount, TokenProvider};
use std::sync::Arc;

use crate::common::*;
use crate::credentials::CredentialsManager;

/// A wrapper around gcp_auth's TokenProvider that provides the interface
/// our code expects.
#[derive(Clone)]
pub(crate) struct Authenticator {
    provider: Arc<dyn TokenProvider>,
}

impl Authenticator {
    /// Create a new authenticator from a TokenProvider.
    fn new(provider: Arc<dyn TokenProvider>) -> Self {
        Self { provider }
    }

    /// Get an access token for the specified scopes.
    pub(crate) async fn token(&self, scopes: &[String]) -> Result<AccessToken> {
        let scopes_refs: Vec<&str> = scopes.iter().map(|s| s.as_str()).collect();
        let token = self
            .provider
            .token(&scopes_refs)
            .await
            .context("failed to get access token")?;
        Ok(AccessToken {
            value: token.as_str().to_string(),
        })
    }
}

/// An access token for Google Cloud services.
#[derive(Clone, Debug)]
pub(crate) struct AccessToken {
    value: String,
}

impl AccessToken {
    /// Get the token as a string.
    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }
}

/// Build an authenticator using service account credentials if available.
async fn service_account_authenticator() -> Result<Authenticator> {
    let creds = CredentialsManager::singleton()
        .get("gcloud_service_account_key")
        .await?;
    let service_account_key_json = creds.get_required("value")?;
    
    let provider = CustomServiceAccount::from_json(service_account_key_json)
        .context("failed to create service account provider from JSON")?;
    
    Ok(Authenticator::new(Arc::new(provider)))
}

/// Build an authenticator using the default provider chain.
async fn default_authenticator() -> Result<Authenticator> {
    let provider = gcp_auth::provider()
        .await
        .context("failed to get default GCP authentication provider")?;
    
    Ok(Authenticator::new(provider))
}

/// Create an authenticator using service account credentials if available, and
/// application default credentials otherwise.
#[instrument(level = "trace")]
pub(crate) async fn authenticator() -> Result<Authenticator> {
    match service_account_authenticator().await {
        // We have a service account configured, so use it.
        Ok(auth) => Ok(auth),
        Err(err) => {
            trace!(
                "trying default credentials because service account auth failed: {:?}",
                err,
            );
            default_authenticator().await
        }
    }
}
