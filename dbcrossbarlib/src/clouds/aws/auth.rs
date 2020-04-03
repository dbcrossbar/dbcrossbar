//! AWS authentication.

use std::env;

use crate::common::*;

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
    pub(crate) fn try_default() -> Result<AwsCredentials> {
        let access_key_id = env::var("AWS_ACCESS_KEY_ID")
            .context("could not find AWS_ACCESS_KEY_ID")?;
        let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY")
            .context("could not find AWS_ACCESS_KEY_ID")?;
        let session_token = env::var("AWS_SESSION_TOKEN").ok();
        Ok(AwsCredentials {
            access_key_id,
            secret_access_key,
            session_token,
        })
    }
}
