//! Wrappers for `aws` CLI commands.
//!
//! We'll probably replace these with a native implementation at some point.

use tokio::process::Command;

use crate::common::*;
use crate::credentials::CredentialsManager;

mod download_file;
mod ls;
mod rmdir;
mod upload_file;

pub(crate) use download_file::download_file;
pub(crate) use ls::ls;
pub(crate) use rmdir::rmdir;
pub(crate) use upload_file::upload_file;

/// Create a new `tokio::process::Command` that invokes `aws s3` with the
/// necessary `AWS` variables set.
///
/// The plan is for this to someday take a `bucket` argument that looks up
/// bucket-specific credentials, once [`CredentialsManager`] supports per-host
/// credentials. For now, this basically exists to (try to) ensure that we're
/// not relying on `aws`'s built-in authentication.
pub(self) async fn aws_s3_command() -> Result<Command> {
    let creds = CredentialsManager::singleton().get("aws").await?;

    let mut command = Command::new("aws");
    command.env("AWS_ACCESS_KEY_ID", creds.get_required("access_key_id")?);
    command.env(
        "AWS_SECRET_ACCESS_KEY",
        creds.get_required("secret_access_key")?,
    );
    if let Some(session_token) = creds.get_optional("session_token") {
        command.env("AWS_SESSION_TOKEN", session_token);
    } else {
        command.env_remove("AWS_SESSION_TOKEN");
    }
    command.env("AWS_DEFAULT_REGION", creds.get_required("default_region")?);
    command.arg("s3");
    Ok(command)
}
