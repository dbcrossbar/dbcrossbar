//! Driver for working with CSV files.

use csv;
use failure::{format_err, ResultExt};
use std::{fmt, fs, path::Path, str::FromStr, thread};
use tokio::{
    codec::{BytesCodec, Decoder},
    fs::File,
    io,
    prelude::*,
};
use tokio_async_await::compat;

use crate::path_or_stdio::PathOrStdio;
use crate::schema::{Column, DataType, Table};
use crate::tokio_glue::{
    copy_stream_to_writer, tokio_fut, BoxFuture, BoxStream, FutureExt, StdFutureExt,
};
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

    fn local_data(&self) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(self.path.clone()).into_boxed()
    }

    fn write_local_data(
        &self,
        schema: Table,
        data: BoxStream<CsvStream>,
        if_exists: IfExists,
    ) -> BoxFuture<()> {
        write_local_data_helper(self.path.clone(), schema, data, if_exists)
            .into_boxed()
    }
}

async fn local_data_helper(path: PathOrStdio) -> Result<Option<BoxStream<CsvStream>>> {
    match path {
        PathOrStdio::Stdio => {
            // TODO - There's a stupid gotcha with `stdin.lock()` that makes
            // this much harder to do than you'd expect without a bunch of
            // extra messing around, so don't implement it for now. We need
            // to fix the API of `PathOrStdio` to _return_ locked stdin like
            // any other stream, which probably means using a background
            // copy thread like we do for Postgres export. Or maybe `tokio`
            // will make this easy?
            Err(format_err!("cannot yet read CSV data from stdin"))
        }
        PathOrStdio::Path(path) => {
            let data = await!(File::open(path.clone()))
                .with_context(|_| format!("cannot open {}", path.display()))?;
            let codec = BytesCodec::new();
            let (_, stream) = codec.framed(data).split();
            let name = stream_name(&path)?;
            let box_stream: BoxStream<CsvStream> =
                Box::new(stream::once(Ok(CsvStream {
                    name: name.to_owned(),
                    data: Box::new(stream.map_err(move |e| {
                        format_err!("cannot read {}: {}", path.display(), e)
                    })),
                })));
            Ok(Some(box_stream))
        }
    }
}

async fn write_local_data_helper(
    path: PathOrStdio,
    _schema: Table,
    data: BoxStream<CsvStream>,
    if_exists: IfExists,
) -> Result<()> {
    match path {
        PathOrStdio::Stdio => {
            if_exists.warn_if_not_default_for_stdout();
            Err(format_err!("cannot yet write CSV data to stdout"))
        }
        PathOrStdio::Path(path) => {
            // TODO - Handle to an individual file.

            // Make sure our directory exists.
            fs::create_dir_all(path.clone()).with_context(|_| {
                format!("unable to create directory {}", path.display())
            })?;

            // Write streams to our directory.
            let result_stream = data.map(|stream| {
                let path = path.clone();
                tokio_fut(
                    async move {
                        // TODO: This join does not handle `..` or nested `/` in a
                        // particularly safe fashion.
                        let csv_path = path.join(&format!("{}.csv", stream.name));
                        let wtr = await!(if_exists
                            .to_async_open_options_no_append()?
                            .open(csv_path.clone()))?;
                        await!(copy_stream_to_writer(stream.data, wtr)).with_context(
                            |_| format!("error writing {}", csv_path.display()),
                        )?;
                        Ok(())
                    },
                )
            });
            await!(result_stream.buffered(4).collect())?;

            Ok(())
        }
    }
}

/// Given a path, extract the base name of the file.
fn stream_name(path: &Path) -> Result<&str> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format_err!("cannot get file name from {}", path.display()))
}
