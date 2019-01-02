//! Driver for working with CSV files.

use failure::{format_err, ResultExt};
use std::{
    fmt,
    fs::{self, File},
    io,
    str::FromStr,
    thread,
};

use crate::path_or_stdio::PathOrStdio;
use crate::schema::Table;
use crate::{CsvStream, Error, Locator, Result};

/// Locator scheme for CSV files.
pub(crate) const CSV_SCHEME: &str = "csv:";

/// (Incomplete.) A CSV file containing data, or a directory containing CSV
/// files.
///
/// TODO: Right now, we take a file path as input and a directory path as
/// output, because we're lazy and haven't finished building this.
#[derive(Debug)]
pub(crate) struct CsvLocator {
    path: PathOrStdio,
}

impl fmt::Display for CsvLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(CSV_SCHEME, f)
    }
}

impl FromStr for CsvLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(CSV_SCHEME, s)?;
        Ok(CsvLocator { path })
    }
}

impl Locator for CsvLocator {
    // TODO: Implement a primitive schema reader for local files that just grabs
    // the column names and sets each type to text.

    fn local_data(&self) -> Result<Option<Vec<CsvStream>>> {
        match &self.path {
            PathOrStdio::Stdio => {
                // TODO - There's a stupid gotcha with `stdin.lock()` that makes this
                // much harder to do than you'd expect without a bunch of extra
                // messing around, so don't implement it for now. We need to fix
                // the API of `PathOrStdio` to _return_ locked stdin like any
                // other stream, which probably means using a background copy
                // thread like we do for Postgres export.
                Err(format_err!("cannot yet read CSV data from stdin"))
            }
            PathOrStdio::Path(path) => {
                // TODO - Paths to directories of files.
                let data = File::open(path)
                    .with_context(|_| format!("cannot open {}", path.display()))?;
                let name =
                    path.file_stem().and_then(|name| name.to_str()).ok_or_else(
                        || format_err!("cannot get file name from {}", path.display()),
                    )?;
                Ok(Some(vec![CsvStream {
                    name: name.to_owned(),
                    data: Box::new(data),
                }]))
            }
        }
    }

    fn write_local_data(&self, _schema: &Table, data: Vec<CsvStream>) -> Result<()> {
        match &self.path {
            PathOrStdio::Stdio => {
                Err(format_err!("cannot yet read CSV data to stdout"))
            }
            PathOrStdio::Path(path) => {
                // TODO - Handle to an individual file.

                // Make sure our directory exists.
                fs::create_dir_all(path).with_context(|_| {
                    format!("unable to create directory {}", path.display())
                })?;

                // Write streams to our directory.
                let mut handles = vec![];
                for mut stream in data {
                    // TODO: This join does not handle `..` or nested `/` in a
                    // particularly safe fashion.
                    let csv_path = path.join(&format!("{}.csv", stream.name));
                    handles.push(thread::spawn(move || -> Result<()> {
                        let mut wtr = File::create(&csv_path).with_context(|_| {
                            format!("cannot create {}", csv_path.display())
                        })?;
                        io::copy(&mut stream.data, &mut wtr).with_context(|_| {
                            format!("error writing {}", csv_path.display())
                        })?;
                        Ok(())
                    }));
                }
                for handle in handles {
                    handle.join().expect("panic in worker thread")?;
                }
                Ok(())
            }
        }
    }
}
