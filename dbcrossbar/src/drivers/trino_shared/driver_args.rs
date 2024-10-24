//! Trino driver arguments.
//!
//! TODO: Do we need this? Should this look like this, or should we have
//! separate arguments for source and dest?

use serde::Deserialize;

//use crate::common::*;

/// Parse driver arguments from `--to-arg` and `--from-arg` labels.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoDriverArguments {}
