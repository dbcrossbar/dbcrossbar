//! Driver for working with CSV files.

use std::{ffi::OsStr, fmt, path::PathBuf, str::FromStr};
use tokio::{
    fs,
    io::{self, BufReader},
};
use tracing::{field, Span};
use walkdir::WalkDir;

use crate::common::*;
use crate::concat::concatenate_csv_streams;
use crate::csv_stream::csv_stream_name;
use crate::schema::{Column, DataType, Table};
use crate::tokio_glue::{copy_reader_to_stream, copy_stream_to_writer};

/// (Incomplete.) A CSV file containing data, or a directory containing CSV
/// files.
///
/// TODO: Right now, we take a file path as input and a directory path as
/// output, because we're lazy and haven't finished building this.
#[derive(Clone, Debug)]
pub(crate) struct CsvLocator {
    path: PathOrStdio,
}

impl CsvLocator {
    /// Construt a `CsvLocator` from a path.
    fn from_path<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: PathOrStdio::Path(path.into()),
        }
    }
}

impl fmt::Display for CsvLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for CsvLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(CsvLocator { path })
    }
}

impl Locator for CsvLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[instrument(level = "trace", name = "csv::schema")]
    fn schema(&self, _ctx: Context) -> BoxFuture<Option<Schema>> {
        // We're going to use a helper thread to do this, because `csv` is a
        // purely synchrnous library.
        let source = self.to_owned();
        spawn_blocking(move || {
            match &source.path {
                PathOrStdio::Stdio => {
                    // This is actually fairly tricky, because we may need to first
                    // read the columns from stdin, _then_ start re-reading from the
                    // beginning to read the data when `local_data` is called.
                    Err(format_err!("cannot yet read CSV schema from stdin"))
                }
                PathOrStdio::Path(path) => {
                    // Build our columns.
                    let mut rdr = csv::Reader::from_path(path).with_context(|| {
                        format!("error opening {}", path.display())
                    })?;
                    let mut columns = vec![];
                    let headers = rdr.headers().with_context(|| {
                        format!("error reading {}", path.display())
                    })?;
                    for col_name in headers {
                        columns.push(Column {
                            name: col_name.to_owned(),
                            is_nullable: true,
                            data_type: DataType::Text,
                            comment: None,
                        })
                    }

                    // Build our table.
                    let name = path
                        .file_stem()
                        .unwrap_or_else(|| OsStr::new("data"))
                        .to_string_lossy()
                        .into_owned();
                    Ok(Some(Schema::from_table(Table { name, columns })?))
                }
            }
        })
        .boxed()
    }

    fn local_data(
        &self,
        _ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(self.path.clone(), shared_args, source_args).boxed()
    }

    fn display_output_locators(&self) -> DisplayOutputLocators {
        match &self.path {
            // If we write our data to standard output, we don't also want to
            // print out "csv:-" to the same standard output.
            PathOrStdio::Stdio => DisplayOutputLocators::Never,
            _ => DisplayOutputLocators::IfRequested,
        }
    }

    fn write_local_data(
        &self,
        ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        write_local_data_helper(ctx, self.path.clone(), data, shared_args, dest_args)
            .boxed()
    }
}

#[instrument(
    level = "trace",
    name = "csv::local_data",
    skip_all,
    fields(path = %path)
)]
async fn local_data_helper(
    path: PathOrStdio,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(CsvLocator::features())?;
    let _source_args = source_args.verify(CsvLocator::features())?;
    match path {
        PathOrStdio::Stdio => {
            let data = BufReader::with_capacity(BUFFER_SIZE, io::stdin());
            let stream = copy_reader_to_stream(data)?;
            let csv_stream = CsvStream {
                name: "data".to_owned(),
                data: stream
                    .map_err(move |e| format_err!("cannot read stdin: {}", e))
                    .boxed(),
            };
            Ok(Some(box_stream_once(Ok(csv_stream))))
        }
        PathOrStdio::Path(base_path) => {
            // Recursively look at our paths, picking out the ones that look
            // like CSVs. We do this synchronously because it's reasonably
            // fast and we'd like to catch errors up front.
            let mut paths = vec![];
            debug!("walking {}", base_path.display());
            let walker = WalkDir::new(&base_path).follow_links(true);
            for dirent in walker.into_iter() {
                let dirent = dirent.with_context(|| {
                    format!("error listing files in {}", base_path.display())
                })?;
                let p = dirent.path();
                trace!("found dirent {}", p.display());
                if dirent.file_type().is_dir() {
                    continue;
                } else if !dirent.file_type().is_file() {
                    return Err(format_err!("not a file: {}", p.display()));
                }

                let ext = p.extension();
                if ext == Some(OsStr::new("csv")) || ext == Some(OsStr::new("CSV")) {
                    paths.push(p.to_owned());
                } else {
                    return Err(format_err!(
                        "{} must end in *.csv or *.CSV",
                        p.display()
                    ));
                }
            }

            let csv_streams = stream::iter(paths).map(Ok).and_then(move |file_path| {
                let base_path = base_path.clone();
                let file_path_copy = file_path.clone();
                async move {
                    // Get the name of our stream.
                    let name = csv_stream_name(
                        &base_path.to_string_lossy(),
                        &file_path.to_string_lossy(),
                    )?
                    .to_owned();
                    Span::current().record("stream.name", &field::display(&name));

                    // Open our file.
                    let data = fs::File::open(file_path.clone()).await.with_context(
                        || format!("cannot open {}", file_path.display()),
                    )?;
                    let data = BufReader::with_capacity(BUFFER_SIZE, data);
                    let stream = copy_reader_to_stream(data)?;

                    Ok(CsvStream {
                        name,
                        data: stream
                            .map_err(move |e| {
                                format_err!(
                                    "cannot read {}: {}",
                                    file_path.display(),
                                    e
                                )
                            })
                            .boxed(),
                    })
                }
                .instrument(debug_span!("stream_from_file", file_path = %file_path_copy.display(), stream.name = field::Empty))
                .boxed()
            });

            Ok(Some(csv_streams.boxed()))
        }
    }
}

#[instrument(
    level = "debug",
    name = "csv::write_local_data",
    skip_all,
    fields(path = %path)
)]
async fn write_local_data_helper(
    ctx: Context,
    path: PathOrStdio,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let _shared_args = shared_args.verify(CsvLocator::features())?;
    let dest_args = dest_args.verify(CsvLocator::features())?;
    let if_exists = dest_args.if_exists().to_owned();
    match path {
        PathOrStdio::Stdio => {
            if_exists.warn_if_not_default_for_stdout();
            let stream = concatenate_csv_streams(ctx.clone(), data)?;
            let fut = async move {
                copy_stream_to_writer(stream.data, io::stdout())
                    .await
                    .context("error writing to stdout")?;
                Ok(CsvLocator {
                    path: PathOrStdio::Stdio,
                }
                .boxed())
            };
            Ok(box_stream_once(Ok(fut.boxed())))
        }
        PathOrStdio::Path(path) => {
            if path.to_string_lossy().ends_with('/') {
                // Write streams to our directory as multiple files.
                let result_stream = data.map_ok(move |stream| {
                    let path = path.clone();
                    let if_exists = if_exists.clone();
                    let stream_name = stream.name.clone();

                    async move {
                        // TODO: This join does not handle `..` or nested `/` in
                        // a particularly safe fashion.
                        let csv_path = path.join(&format!("{}.csv", stream.name));
                        Span::current().record("path", &field::display(csv_path.display()));
                        write_stream_to_file(
                            stream.data,
                            csv_path.clone(),
                            if_exists,
                        )
                        .await?;
                        Ok(CsvLocator::from_path(csv_path).boxed())
                    }.instrument(trace_span!("stream_to_file", stream.name = %stream_name, path = field::Empty))
                    .boxed()
                });
                Ok(result_stream.boxed())
            } else {
                // Write all our streams as a single file.
                let stream = concatenate_csv_streams(ctx.clone(), data)?;
                let stream_name = stream.name.clone();
                let path_copy = path.clone();
                let fut = async move {
                    write_stream_to_file(stream.data, path.clone(), if_exists)
                        .await?;
                    Ok(CsvLocator::from_path(path).boxed())
                }.instrument(trace_span!("stream_to_file", stream.name = %stream_name, path = %path_copy.display()));
                Ok(box_stream_once(Ok(fut.boxed())))
            }
        }
    }
}

/// Write `data` to `dest`, honoring `if_exists`.
async fn write_stream_to_file(
    data: BoxStream<BytesMut>,
    dest: PathBuf,
    if_exists: IfExists,
) -> Result<()> {
    // Make sure our destination directory exists.
    let dir = dest
        .parent()
        .ok_or_else(|| format_err!("cannot find parent dir for {}", dest.display()))?;
    fs::create_dir_all(dir)
        .await
        .with_context(|| format!("unable to create directory {}", dir.display()))?;

    // Write our our CSV stream.
    debug!("writing stream to file {}", dest.display());
    let wtr = if_exists
        .to_async_open_options_no_append()?
        .open(dest.clone())
        .await
        .with_context(|| format!("cannot open {}", dest.display()))?;
    copy_stream_to_writer(data, wtr)
        .await
        .with_context(|| format!("error writing {}", dest.display()))?;
    Ok(())
}

impl LocatorStatic for CsvLocator {
    fn scheme() -> &'static str {
        "csv:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema
                | LocatorFeatures::LocalData
                | LocatorFeatures::WriteLocalData,
            write_schema_if_exists: EnumSet::empty(),
            source_args: EnumSet::empty(),
            dest_args: EnumSet::empty(),
            dest_if_exists: IfExistsFeatures::no_append(),
            _placeholder: (),
        }
    }
}
