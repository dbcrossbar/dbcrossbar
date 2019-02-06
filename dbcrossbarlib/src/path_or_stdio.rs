//! Support for working with either files or standard I/O.

use failure::{format_err, ResultExt};
use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::{
    fs::File,
    io,
    prelude::*,
};

use crate::{Error, IfExists, Result};

/// A local input or output location, specified using either a path, or `"-"`
/// for standard I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PathOrStdio {
    Path(PathBuf),
    Stdio,
}

impl PathOrStdio {
    /// Verify that a locator has the right scheme (which must here end with
    /// a colon), and extract the trailing path. This differs substantially from
    /// the normal behavior of `file://` URIs, which among other things do not
    /// support relative paths.
    pub(crate) fn from_str_locator_helper(
        scheme: &str,
        locator: &str,
    ) -> Result<PathOrStdio> {
        assert!(scheme.ends_with(':'));
        if locator.starts_with(scheme) {
            PathOrStdio::from_str(&locator[scheme.len()..])
        } else {
            Err(format_err!("expected {} to start with {}", locator, scheme))
        }
    }

    /// Given a locator scheme (with a trailing `:`) and a path, format them as
    /// locator pointing to a file. See `file_locator_from_str` for further
    /// notes.
    pub(crate) fn fmt_locator_helper(
        &self,
        scheme: &str,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        assert!(scheme.ends_with(':'));
        write!(f, "{}{}", scheme, self)
    }

    /// Open the file (or standard input) for reading, and pass the `Read`
    /// reference to `body`. We have to do this using a callback because of how
    /// `lock` works on standard I/O.
    pub(crate) async fn open(&self) -> Result<Box<dyn AsyncRead>> {
        match self {
            PathOrStdio::Path(p) => {
                let p = p.to_owned();
                let mut f = await!(File::open(p.clone()))
                    .with_context(|_| format!("error opening {}", p.display()))?;
                Ok(Box::new(f) as Box<dyn AsyncRead>)
            }
            PathOrStdio::Stdio => {
                Ok(Box::new(io::stdin()) as Box<dyn AsyncRead>)
            }
        }
    }

    /// Open the file (or standard output) for reading, and pass the `Write`
    /// reference to `body`. We have to do this using a callback because of how
    /// `lock` works on standard I/O.
    pub(crate) async fn create(
        &self,
        if_exists: IfExists,
    ) -> Result<Box<dyn AsyncWrite>> {
        match self {
            PathOrStdio::Path(p) => {
                let p = p.to_owned();
                let f = await!(if_exists
                    .to_open_options_no_append()?
                    .open(p.clone()))
                    .with_context(|_| format!("error opening {}", p.display()))?;
                Ok(Box::new(f) as Box<dyn AsyncWrite>)
            }
            PathOrStdio::Stdio => {
                if_exists.warn_if_not_default_for_stdout();
                Ok(Box::new(io::stdout()) as Box<dyn AsyncWrite>)
            }
        }
    }
}

impl fmt::Display for PathOrStdio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathOrStdio::Stdio => write!(f, "-"),
            PathOrStdio::Path(p) => write!(f, "{}", p.display()),
        }
    }
}

impl FromStr for PathOrStdio {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "-" {
            Ok(PathOrStdio::Stdio)
        } else {
            Ok(PathOrStdio::Path(Path::new(s).to_owned()))
        }
    }
}
