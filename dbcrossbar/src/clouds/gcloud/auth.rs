//! Authentication support for Google Cloud.

use google_cloud_auth::credentials::service_account::AccessSpecifier;
use google_cloud_auth::credentials::{
    AccessTokenCredentials, Builder as ApplicationDefaultCredentialsBuilder,
};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::common::*;
use crate::credentials::CredentialsManager;

/// A wrapper around Google Cloud credentials that provides the interface our
/// code expects.
#[derive(Clone)]
pub(crate) struct Authenticator {
    credentials: AccessTokenCredentials,
}

impl Authenticator {
    /// Get an access token. Scopes were applied when the credentials were built.
    pub(crate) async fn token(&self, _scopes: &[String]) -> Result<AccessToken> {
        let token = self
            .credentials
            .access_token()
            .await
            .context("failed to get access token")?;
        Ok(AccessToken { value: token.token })
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

/// Where Application Default Credentials will be loaded from.
#[derive(Debug)]
enum CredentialSource {
    DbcrossbarServiceAccountKey,
    GoogleApplicationCredentials {
        path: PathBuf,
        credential_type: Option<String>,
    },
    WellKnownApplicationDefault {
        path: PathBuf,
        credential_type: Option<String>,
    },
    MetadataServer,
}

impl CredentialSource {
    fn log(&self) {
        match self {
            CredentialSource::DbcrossbarServiceAccountKey => {
                debug!("using dbcrossbar gcloud_service_account_key credential");
            }
            CredentialSource::GoogleApplicationCredentials {
                path,
                credential_type,
            } => match credential_type {
                Some(credential_type) => {
                    debug!(
                        path = %path.display(),
                        credential_type = %credential_type,
                        "using GOOGLE_APPLICATION_CREDENTIALS"
                    );
                }
                None => {
                    debug!(
                        path = %path.display(),
                        "using GOOGLE_APPLICATION_CREDENTIALS"
                    );
                }
            },
            CredentialSource::WellKnownApplicationDefault {
                path,
                credential_type,
            } => match credential_type {
                Some(credential_type) => {
                    debug!(
                        path = %path.display(),
                        credential_type = %credential_type,
                        "using well-known application-default credentials"
                    );
                }
                None => {
                    debug!(
                        path = %path.display(),
                        "using well-known application-default credentials"
                    );
                }
            },
            CredentialSource::MetadataServer => {
                debug!(
                    "using GCE/GKE metadata server for application-default credentials"
                );
            }
        }
    }
}

fn json_type_field(path: &Path) -> Option<String> {
    let contents = fs::read_to_string(path).ok()?;
    let json: Value = serde_json::from_str(&contents).ok()?;
    json.get("type")?.as_str().map(str::to_owned)
}

fn well_known_adc_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        env::var_os("APPDATA").map(|root| {
            PathBuf::from(root).join("gcloud/application_default_credentials.json")
        })
    }
    #[cfg(not(target_os = "windows"))]
    {
        env::var_os("HOME").map(|root| {
            PathBuf::from(root)
                .join(".config/gcloud/application_default_credentials.json")
        })
    }
}

fn application_default_credential_source() -> CredentialSource {
    if let Some(path) = env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
        let path = PathBuf::from(path);
        let credential_type = json_type_field(&path);
        return CredentialSource::GoogleApplicationCredentials {
            path,
            credential_type,
        };
    }
    if let Some(path) = well_known_adc_path() {
        if path.is_file() {
            let credential_type = json_type_field(&path);
            return CredentialSource::WellKnownApplicationDefault {
                path,
                credential_type,
            };
        }
    }
    CredentialSource::MetadataServer
}

fn service_account_credentials_from_json(
    service_account_key_json: &str,
    scopes: &[String],
) -> Result<AccessTokenCredentials> {
    let json: Value = serde_json::from_str(service_account_key_json)
        .context("gcloud_service_account_key is not valid JSON")?;
    google_cloud_auth::credentials::service_account::Builder::new(json)
        .with_access_specifier(AccessSpecifier::from_scopes(scopes.iter().cloned()))
        .build_access_token_credentials()
        .context("failed to create service account credentials from JSON")
}

fn application_default_credentials(
    scopes: &[String],
) -> Result<AccessTokenCredentials> {
    ApplicationDefaultCredentialsBuilder::default()
        .with_scopes(scopes.iter().cloned())
        .build_access_token_credentials()
        .context("failed to load Google Application Default Credentials")
}

/// Create an authenticator using service account credentials if available, and
/// application default credentials otherwise.
#[instrument(level = "trace", skip(scopes))]
pub(crate) async fn authenticator(scopes: &[String]) -> Result<Authenticator> {
    match CredentialsManager::singleton()
        .get("gcloud_service_account_key")
        .await
    {
        Ok(creds) => {
            CredentialSource::DbcrossbarServiceAccountKey.log();
            let credentials = service_account_credentials_from_json(
                creds.get_required("value")?,
                scopes,
            )?;
            Ok(Authenticator { credentials })
        }
        Err(err) => {
            trace!(
                "trying application default credentials because service account key was not used: {:?}",
                err,
            );
            application_default_credential_source().log();
            Ok(Authenticator {
                credentials: application_default_credentials(scopes)?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scoped_env::ScopedEnv;
    use serial_test::serial;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn test_scopes() -> Vec<String> {
        vec![
            "https://www.googleapis.com/auth/devstorage.read_write".to_owned(),
            "https://www.googleapis.com/auth/bigquery".to_owned(),
        ]
    }

    fn write_fixture(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(contents.as_bytes()).expect("write fixture");
        file
    }

    fn credentials_from_google_application_credentials(
        contents: &str,
    ) -> Result<AccessTokenCredentials> {
        let file = write_fixture(contents);
        let _gac = ScopedEnv::set(
            "GOOGLE_APPLICATION_CREDENTIALS",
            file.path().to_str().expect("utf-8 path"),
        );
        let _key = ScopedEnv::remove("GCLOUD_SERVICE_ACCOUNT_KEY");
        application_default_credentials(&test_scopes())
    }

    fn debug_name(credentials: &AccessTokenCredentials) -> String {
        format!("{credentials:?}")
    }

    #[tokio::test]
    #[serial]
    async fn constructs_service_account_from_google_application_credentials() {
        let credentials = credentials_from_google_application_credentials(
            include_str!("auth_fixtures/service_account.json"),
        )
        .expect("service_account credentials should construct");
        let debug = debug_name(&credentials);
        assert!(debug.contains("ServiceAccountCredentials"), "{debug}");
    }

    #[tokio::test]
    #[serial]
    async fn constructs_authorized_user_from_google_application_credentials() {
        let credentials = credentials_from_google_application_credentials(
            include_str!("auth_fixtures/authorized_user.json"),
        )
        .expect("authorized_user credentials should construct");
        let debug = debug_name(&credentials);
        assert!(debug.contains("UserCredentials"), "{debug}");
    }

    #[tokio::test]
    #[serial]
    async fn constructs_impersonated_service_account_from_google_application_credentials(
    ) {
        let credentials = credentials_from_google_application_credentials(
            include_str!("auth_fixtures/impersonated_service_account.json"),
        )
        .expect("impersonated_service_account credentials should construct");
        let debug = debug_name(&credentials);
        assert!(debug.contains("ImpersonatedServiceAccount"), "{debug}");
    }

    #[tokio::test]
    #[serial]
    async fn constructs_external_account_from_google_application_credentials() {
        let credentials = credentials_from_google_application_credentials(
            include_str!("auth_fixtures/external_account.json"),
        )
        .expect("external_account credentials should construct");
        let debug = debug_name(&credentials);
        assert!(
            debug.contains("FileSourcedCredentials")
                || debug.contains("ExternalAccount"),
            "{debug}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn malformed_google_application_credentials_is_error() {
        let err = credentials_from_google_application_credentials("{ not json")
            .expect_err("malformed credentials file should be an error");
        let message = format!("{err:#}");
        assert!(
            message.contains("GOOGLE_APPLICATION_CREDENTIALS")
                || message.contains("parse")
                || message.contains("type")
                || message.contains("expected"),
            "{message}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn missing_google_application_credentials_file_names_the_path() {
        let missing_path =
            "/tmp/dbcrossbar-missing-adc-credentials-do-not-create.json";
        let _gac = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", missing_path);
        let _key = ScopedEnv::remove("GCLOUD_SERVICE_ACCOUNT_KEY");
        let err = application_default_credentials(&test_scopes()).expect_err(
            "missing GOOGLE_APPLICATION_CREDENTIALS file should be an error",
        );
        let message = format!("{err:#}");
        assert!(
            message.contains("dbcrossbar-missing-adc-credentials-do-not-create.json"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn constructs_from_gcloud_service_account_key_json() {
        let credentials = service_account_credentials_from_json(
            include_str!("auth_fixtures/service_account.json"),
            &test_scopes(),
        )
        .expect("service account key JSON should construct");
        let debug = debug_name(&credentials);
        assert!(debug.contains("ServiceAccountCredentials"), "{debug}");
    }

    #[test]
    #[serial]
    fn application_default_source_names_google_application_credentials_type() {
        let file = write_fixture(include_str!(
            "auth_fixtures/impersonated_service_account.json"
        ));
        let _gac = ScopedEnv::set(
            "GOOGLE_APPLICATION_CREDENTIALS",
            file.path().to_str().expect("utf-8 path"),
        );
        match application_default_credential_source() {
            CredentialSource::GoogleApplicationCredentials {
                path,
                credential_type,
            } => {
                assert_eq!(path, file.path());
                assert_eq!(
                    credential_type.as_deref(),
                    Some("impersonated_service_account")
                );
            }
            other => panic!("unexpected source: {other:?}"),
        }
    }

    #[tokio::test]
    #[serial]
    async fn fetches_real_access_token() {
        if env::var("DBCROSSBAR_TEST_GCLOUD_AUTH").is_err() {
            eprintln!(
                "skipping: set DBCROSSBAR_TEST_GCLOUD_AUTH=1 to fetch a real token"
            );
            return;
        }
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let authenticator = authenticator(&test_scopes())
            .await
            .expect("should build authenticator");
        let token = authenticator
            .token(&test_scopes())
            .await
            .expect("should fetch a real access token");
        assert!(!token.as_str().is_empty());
    }
}
