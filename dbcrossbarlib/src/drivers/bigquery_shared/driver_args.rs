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
}
