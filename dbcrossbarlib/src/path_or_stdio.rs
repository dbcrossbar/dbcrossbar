//! Support for working with either files or standard I/O.

use failure::{format_err, ResultExt};
use std::{
    fmt,
    fs::File,
    io::{self, prelude::*},
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{Error, Result};

/// A local input or output location, specified using either a path, or `"-"`
/// for standard I/O.
#[derive(Debug, Eq, PartialEq)]
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
    pub(crate) fn open<F, T>(&self, body: F) -> Result<T>
    where
        F: FnOnce(&mut dyn Read) -> Result<T>,
    {
        match self {
            PathOrStdio::Path(p) => {
                let mut f = File::open(p)
                    .with_context(|_| format!("error opening {}", p.display()))?;
                body(&mut f)
            }
            PathOrStdio::Stdio => {
                let stdin = io::stdin();
                let mut stdin_lock = stdin.lock();
                body(&mut stdin_lock)
            }
        }
    }

    /// Open the file (or standard output) for reading, and pass the `Write`
    /// reference to `body`. We have to do this using a callback because of how
    /// `lock` works on standard I/O.
    pub(crate) fn create<F, T>(&self, body: F) -> Result<T>
    where
        F: FnOnce(&mut dyn Write) -> Result<T>,
    {
        match self {
            PathOrStdio::Path(p) => {
                let mut f = File::create(p)
                    .with_context(|_| format!("error opening {}", p.display()))?;
                body(&mut f)
            }
            PathOrStdio::Stdio => {
                let stdout = io::stdout();
                let mut stdout_lock = stdout.lock();
                body(&mut stdout_lock)
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
