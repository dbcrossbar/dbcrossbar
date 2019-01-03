//! Driver for working with CSV files.

use csv;
use failure::{format_err, ResultExt};
use std::{
    fmt,
    fs::{self, File},
    io,
    path::Path,
    str::FromStr,
    thread,
};

use crate::path_or_stdio::PathOrStdio;
use crate::schema::{Column, DataType, Table};
use crate::{CsvStream, Error, IfExists, Locator, Result};

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
    fn schema(&self) -> Result<Option<Table>> {
        match &self.path {
            PathOrStdio::Stdio => {
                // This is actually fairly tricky, because we may need to first
                // read the columns from stdin, _then_ start re-reading from the
                // beginning to read the data when `local_data` is called.
                Err(format_err!("cannot yet read CSV schema from stdin"))
            }
            PathOrStdio::Path(path) => {
                // Build our columns.
                let mut rdr = csv::Reader::from_path(path)
                    .with_context(|_| format!("error opening {}", path.display()))?;
                let mut columns = vec![];
                let headers = rdr
                    .headers()
                    .with_context(|_| format!("error reading {}", path.display()))?;
                for col_name in headers {
                    columns.push(Column {
                        name: col_name.to_owned(),
                        is_nullable: true,
                        data_type: DataType::Text,
                        comment: None,
                    })
                }

                // Build our table.
                let name = stream_name(path)?.to_owned();
                Ok(Some(Table { name, columns }))
            }
        }
    }

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
                let name = stream_name(path)?;
                Ok(Some(vec![CsvStream {
                    name: name.to_owned(),
                    data: Box::new(data),
                }]))
            }
        }
    }

    fn write_local_data(
        &self,
        _schema: &Table,
        data: Vec<CsvStream>,
        if_exists: IfExists,
    ) -> Result<()> {
        match &self.path {
            PathOrStdio::Stdio => {
                if_exists.warn_if_not_default_for_stdout();
                Err(format_err!("cannot yet write CSV data to stdout"))
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
                        let mut wtr = if_exists
                            .to_open_options_no_append()?
                            .open(&csv_path)
                            .with_context(|_| {
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

/// Given a path, extract the base name of the file.
fn stream_name(path: &Path) -> Result<&str> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format_err!("cannot get file name from {}", path.display()))
}
