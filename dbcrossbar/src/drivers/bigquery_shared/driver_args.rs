//! Arguments which can be passed to various Google Cloud drivers.

use serde::Deserialize;

use crate::clouds::gcloud::bigquery::Labels;

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
}
