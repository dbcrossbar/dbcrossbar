//! AWS authentication.

use crate::common::*;
use crate::credentials::CredentialsManager;

/// Credentials used to access S3.
pub(crate) struct AwsCredentials {
    /// The value of `AWS_ACCESS_KEY_ID`.
    pub(crate) access_key_id: String,
    /// The value of `AWS_SECRET_ACCESS_KEY`.
    pub(crate) secret_access_key: String,
    /// The value of `AWS_SESSION_TOKEN`.
    pub(crate) session_token: Option<String>,
}

impl AwsCredentials {
    /// Try to look up a default value for our AWS credentials.
    pub(crate) async fn try_default() -> Result<AwsCredentials> {
        let creds = CredentialsManager::singleton().get("aws").await?;
        let access_key_id = creds.get_required("access_key_id")?.to_owned();
        let secret_access_key = creds.get_required("secret_access_key")?.to_owned();
        let session_token = creds.get_optional("session_token").map(|t| t.to_owned());
        Ok(AwsCredentials {
            access_key_id,
            secret_access_key,
            session_token,
        })
    }
}
