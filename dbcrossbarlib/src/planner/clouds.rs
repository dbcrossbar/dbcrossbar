//! Information about various cloud providers.

use std::fmt;

#[cfg(test)]
use proptest_derive::Arbitrary;

/// Cloud providers.
///
/// This list should contain, at a minimum, any large cloud provider which has
/// more than one type of storage.
///
/// We may eventually need to replace this with interned symbols, but this will
/// do for now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) enum Cloud {
    Aws,
    GCloud,
}

impl fmt::Display for Cloud {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cloud::Aws => write!(f, "aws"),
            Cloud::GCloud => write!(f, "gcloud"),
        }
    }
}
