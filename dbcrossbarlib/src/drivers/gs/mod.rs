//! Support for Google Cloud Storage.

use failure::{format_err, ResultExt};
use log::debug;
use std::{
    fmt, io,
    process::{Command, Stdio},
    str::FromStr,
    thread,
};
use url::Url;

use crate::schema::Table;
use crate::{CsvStream, Error, IfExists, Locator, Result};

/// Locator scheme for Google Cloud Storage.
pub(crate) const GS_SCHEME: &str = "gs:";

#[derive(Debug)]
pub(crate) struct GsLocator {
    url: Url,
}

impl fmt::Display for GsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for GsLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(GS_SCHEME) {
            let url = s
                .parse::<Url>()
                .with_context(|_| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with gs://", url))
            } else if !url.path().ends_with('/') {
                Err(format_err!("{} must end with a '/'", url))
            } else {
                Ok(GsLocator { url })
            }
        } else {
            Err(format_err!("expected {} to begin with gs://", s))
        }
    }
}

impl Locator for GsLocator {
    fn write_local_data(
        &self,
        _schema: &Table,
        data: Vec<CsvStream>,
        if_exists: IfExists,
    ) -> Result<()> {
        // Delete the existing output, if it exists.
        if if_exists == IfExists::Overwrite {
            // Delete all the files under `self.url`, but be careful not to
            // delete the entire bucket. See `gsutil rm --help` for details.
            debug!("deleting existing {}", self.url);
            assert!(self.url.path().ends_with('/'));
            let delete_url = self.url.join("**")?;
            let status = Command::new("gsutil")
                .args(&["rm", delete_url.as_str()])
                .status()
                .context("error running gsutil")?;
            if !status.success() {
                return Err(format_err!("gsutil failed: {}", status));
            }
        } else {
            return Err(format_err!(
                "must specify `overwrite` for gs:// destination"
            ));
        }

        // Spawn our uploader threads.
        let mut handles = vec![];
        for mut stream in data {
            let url = self.url.join(&format!("{}.csv", stream.name))?;
            handles.push(thread::spawn(move || -> Result<()> {
                // Run `gsutil cp - $URL` as a background process.
                debug!("uploading stream to {}", url);
                let mut child = Command::new("gsutil")
                    .args(&["cp", "-", url.as_str()])
                    .stdin(Stdio::piped())
                    .spawn()
                    .context("error running gsutil")?;

                // Copy our stream to the stdin of `gsutil`.
                {
                    let mut sink =
                        child.stdin.take().expect("should always have child stdin");
                    io::copy(&mut stream.data, &mut sink)
                        .with_context(|_| format!("error uploading to {}", url))?;
                    // TODO: We should probably call `wait` if the copy fails, to prevent
                    // zombie upload processes in the process table.
                }

                // Wait for `gsutil` to finish.
                let status = child
                    .wait()
                    .with_context(|_| format!("error finishing upload to {}", url))?;
                if status.success() {
                    Ok(())
                } else {
                    Err(format_err!("gsutil returned error: {}", status))
                }
            }));
        }

        // Wait for our threads to finish.
        for handle in handles {
            handle.join().expect("panic in background thread")?;
        }
        Ok(())
    }
}
