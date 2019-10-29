//! What to do if the destination already exists.

use itertools::Itertools;
use std::{fmt, str::FromStr};
use tokio::fs as tokio_fs;

use crate::args::DisplayEnumSet;
use crate::common::*;
use crate::separator::Separator;

/// Which `IfExists` features are supported by a given driver or API?
#[derive(Debug, EnumSetType)]
pub enum IfExistsFeatures {
    Error,
    Append,
    Overwrite,
    Upsert,
}

impl IfExistsFeatures {
    /// Returns the features supported for `to_async_open_options_no_append` and
    /// `to_sync_open_options_no_append`.
    pub(crate) fn no_append() -> EnumSet<Self> {
        IfExistsFeatures::Error | IfExistsFeatures::Overwrite
    }
}

impl fmt::Display for DisplayEnumSet<IfExistsFeatures> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut sep = Separator::new(" ");
        let mut write_flag = |flag, value| -> fmt::Result {
            if self.0.contains(flag) {
                write!(f, "{}--if-exists={}", sep.display(), value)?;
            }
            Ok(())
        };
        write_flag(IfExistsFeatures::Error, IfExists::Error)?;
        write_flag(IfExistsFeatures::Append, IfExists::Append)?;
        write_flag(IfExistsFeatures::Overwrite, IfExists::Overwrite)?;
        write_flag(
            IfExistsFeatures::Upsert,
            IfExists::Upsert(vec!["col".to_owned()]),
        )?;
        Ok(())
    }
}

/// What to do if the destination already exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IfExists {
    /// If the destination exists, return an error.
    Error,
    /// If the destination exists, try to append the new data.
    Append,
    /// If the destination exists, overwrite the existing data.
    Overwrite,
    /// If the destination exists, either update or insert using the specified
    /// columns as the key. The list of keys must be non-empty, but we currently
    /// only enforce that when parsing in `FromStr`.
    Upsert(Vec<String>),
}

impl IfExists {
    /// Are we supposed to perform an upsert?
    pub(crate) fn is_upsert(&self) -> bool {
        match self {
            IfExists::Upsert(_) => true,
            _ => false,
        }
    }

    /// Convert to an `tokio::OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_async_open_options_no_append(
        &self,
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
            IfExists::Upsert(_) => {
                return Err(format_err!("upsert not supported"));
            }
        }
        Ok(open_options)
    }

    pub(crate) fn warn_if_not_default_for_stdout(&self, ctx: &Context) {
        if self != &IfExists::default() {
            warn!(ctx.log(), "{} ignored for stdout", self)
        }
    }

    /// Verify that this `if_exists` is one of the possibilities allowed by
    /// `features`.
    pub(crate) fn verify(&self, features: EnumSet<IfExistsFeatures>) -> Result<()> {
        match self {
            IfExists::Error if !features.contains(IfExistsFeatures::Error) => Err(
                format_err!("this driver does not support --if-exists=error"),
            ),
            IfExists::Overwrite if !features.contains(IfExistsFeatures::Overwrite) => {
                Err(format_err!(
                    "this driver does not support --if-exists=overwrite"
                ))
            }
            IfExists::Append if !features.contains(IfExistsFeatures::Append) => Err(
                format_err!("this driver does not support --if-exists=append"),
            ),
            IfExists::Upsert(_) if !features.contains(IfExistsFeatures::Upsert) => {
                Err(format_err!(
                    "this driver does not support --if-exists=upsert-on:..."
                ))
            }
            _ => Ok(()),
        }
    }
}

impl Default for IfExists {
    fn default() -> Self {
        IfExists::Error
    }
}

/// The prefix used for the serialized version of `IfExists::Upsert`.
const UPSERT_PREFIX: &str = "upsert-on:";

impl fmt::Display for IfExists {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IfExists::Error => "error".fmt(f),
            IfExists::Append => "append".fmt(f),
            IfExists::Overwrite => "overwrite".fmt(f),
            IfExists::Upsert(merge_keys) => {
                write!(f, "{}{}", UPSERT_PREFIX, merge_keys.iter().join(","))
            }
        }
    }
}

impl FromStr for IfExists {
    type Err = Error;

    fn from_str(s: &str) -> Result<IfExists> {
        match s {
            "error" => Ok(IfExists::Error),
            "append" => Ok(IfExists::Append),
            "overwrite" => Ok(IfExists::Overwrite),
            _ if s.starts_with(UPSERT_PREFIX) => {
                let merge_keys = s[UPSERT_PREFIX.len()..]
                    .split(',')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                if merge_keys.is_empty()
                    || (merge_keys.len() == 1 && merge_keys[0] == "")
                {
                    return Err(format_err!("must specify keys after `upsert-on:`"));
                }
                if merge_keys.iter().any(|k| k == "") {
                    return Err(format_err!("`{}` contains an empty merge key", s));
                }
                Ok(IfExists::Upsert(merge_keys))
            }
            _ => Err(format_err!("unknown if-exists value: {}", s)),
        }
    }
}

#[test]
fn parse_and_display() {
    let examples = [
        ("error", IfExists::Error),
        ("append", IfExists::Append),
        ("overwrite", IfExists::Overwrite),
        ("upsert-on:id", IfExists::Upsert(vec!["id".to_owned()])),
        (
            "upsert-on:first,last",
            IfExists::Upsert(vec!["first".to_owned(), "last".to_owned()]),
        ),
    ];
    for (serialized, value) in &examples {
        assert_eq!(&serialized.parse::<IfExists>().unwrap(), value);
        assert_eq!(serialized, &value.to_string());
    }
}

#[test]
fn must_have_upsert_keys() {
    assert!("upsert-on:".parse::<IfExists>().is_err());
}
