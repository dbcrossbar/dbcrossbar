//! What to do if the destination already exists.

use std::fs as std_fs;
use strum_macros::{Display, EnumString};
use tokio::fs as tokio_fs;

use crate::common::*;

/// What to do if the destination already exists.
#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum IfExists {
    /// If the destination exists, return an error.
    Error,
    /// If the destination exists, try to append the new data.
    Append,
    /// If the destination exists, overrwrite the existing data.
    Overwrite,
}

impl IfExists {
    /// Convert to an `tokio::OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_async_open_options_no_append(
        self,
    ) -> Result<tokio_fs::OpenOptions> {
        let mut open_options = tokio_fs::OpenOptions::new();
        open_options.write(true);
        match self {
            IfExists::Error => {
                open_options.create_new(true);
            }
            IfExists::Overwrite => {
                open_options.create(true).append(true);
            }
            IfExists::Append => {
                return Err(format_err!("appending not supported"));
            }
        }
        Ok(open_options)
    }

    /// Convert to an `std::fs::OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_sync_open_options_no_append(self) -> Result<std_fs::OpenOptions> {
        let mut open_options = std_fs::OpenOptions::new();
        open_options.write(true);
        match self {
            IfExists::Error => {
                open_options.create_new(true);
            }
            IfExists::Overwrite => {
                open_options.create(true).append(true);
            }
            IfExists::Append => {
                return Err(format_err!("appending not supported"));
            }
        }
        Ok(open_options)
    }

    pub(crate) fn warn_if_not_default_for_stdout(self, ctx: &Context) {
        if self != IfExists::default() {
            warn!(ctx.log(), "{} ignored for stdout", self)
        }
    }
}

impl Default for IfExists {
    fn default() -> Self {
        IfExists::Error
    }
}
