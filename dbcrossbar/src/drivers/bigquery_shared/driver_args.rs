//! Arguments which can be passed to various Google Cloud drivers.

use std::convert::TryFrom;

use serde::Deserialize;

use crate::{
    clouds::gcloud::{bigquery::Labels, Client, ClientError},
    common::*,
    DriverArguments,
};

/// Parse version of `--to-arg` and `--from-arg` labels.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GCloudDriverArguments {
    /// Billing labels to apply to objects and jobs.
    #[serde(default)]
    pub(crate) job_labels: Labels,

    /// Project id, in case you want to not use the project implied by the source/dest table locator.
    #[serde(default)]
    pub(crate) job_project_id: Option<String>,

    /// Extra scopes that are potentially needed to access things like Google
    /// Drive files mapped as BigQuery tables.
    #[serde(default)]
    pub(crate) extra_scopes: Vec<String>,
}

impl GCloudDriverArguments {
    /// Create a Google Cloud client with any necessary configuration.
    pub(crate) async fn client(&self) -> Result<Client, ClientError> {
        Client::new(&self.extra_scopes).await
    }

    fn extract_from(
        cli_flag_name: &str,
        driver_args: &DriverArguments,
    ) -> Result<Self> {
        driver_args
            .deserialize::<Self>()
            .with_context(|| format!("error parsing {}", cli_flag_name))
    }
}

impl<'a> TryFrom<&'a SourceArguments<Verified>> for GCloudDriverArguments {
    type Error = Error;

    fn try_from(args: &'a SourceArguments<Verified>) -> Result<Self> {
        Self::extract_from("--from-args", args.driver_args())
    }
}

impl<'a> TryFrom<&'a DestinationArguments<Verified>> for GCloudDriverArguments {
    type Error = Error;

    fn try_from(args: &'a DestinationArguments<Verified>) -> Result<Self> {
        Self::extract_from("--to-args", args.driver_args())
    }
}
