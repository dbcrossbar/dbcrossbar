//! Driver for working with CSV files.

use csv;
use std::{ffi::OsStr, fmt, io::BufReader, path::PathBuf, str::FromStr};
use tokio::{fs, io};
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

    fn schema(&self, _ctx: &Context) -> Result<Option<Table>> {
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
                let name = path
                    .file_stem()
                    .unwrap_or_else(|| OsStr::new("data"))
                    .to_string_lossy()
                    .into_owned();
                Ok(Some(Table { name, columns }))
            }
        }
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.path.clone(), shared_args, source_args).boxed()
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

async fn local_data_helper(
    ctx: Context,
    path: PathOrStdio,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(CsvLocator::features())?;
    let _source_args = source_args.verify(CsvLocator::features())?;
    match path {
        PathOrStdio::Stdio => {
            let data = BufReader::with_capacity(BUFFER_SIZE, io::stdin());
            let stream = copy_reader_to_stream(ctx, data)?;
            let csv_stream = CsvStream {
                name: "data".to_owned(),
                data: Box::new(
                    stream.map_err(move |e| format_err!("cannot read stdin: {}", e)),
                ),
            };
            Ok(Some(box_stream_once(Ok(csv_stream))))
        }
        PathOrStdio::Path(base_path) => {
            // Recursively look at our paths, picking out the ones that look
            // like CSVs. We do this synchronously because it's reasonably
            // fast and we'd like to catch errors up front.
            let mut paths = vec![];
            debug!(ctx.log(), "walking {}", base_path.display());
            let walker = WalkDir::new(&base_path).follow_links(true);
            for dirent in walker.into_iter() {
                let dirent = dirent.with_context(|_| {
                    format!("error listing files in {}", base_path.display())
                })?;
                let p = dirent.path();
                trace!(ctx.log(), "found dirent {}", p.display());
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

            let csv_streams = stream::iter_ok(paths).and_then(move |file_path| {
                let ctx = ctx.clone();
                let base_path = base_path.clone();
                async move {
                    // Get the name of our stream.
                    let name = csv_stream_name(
                        &base_path.to_string_lossy(),
                        &file_path.to_string_lossy(),
                    )?
                    .to_owned();
                    let ctx = ctx.child(o!(
                        "stream" => name.clone(),
                        "path" => format!("{}", file_path.display())
                    ));

                    // Open our file.
                    let data = fs::File::open(file_path.clone())
                        .compat()
                        .await
                        .with_context(|_| {
                            format!("cannot open {}", file_path.display())
                        })?;
                    let data = BufReader::with_capacity(BUFFER_SIZE, data);
                    let stream = copy_reader_to_stream(ctx, data)?;

                    Ok(CsvStream {
                        name,
                        data: Box::new(stream.map_err(move |e| {
                            format_err!("cannot read {}: {}", file_path.display(), e)
                        })),
                    })
                }
                    .boxed()
                    .compat()
            });

            Ok(Some(Box::new(csv_streams) as BoxStream<CsvStream>))
        }
    }
}

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
            if_exists.warn_if_not_default_for_stdout(&ctx);
            let stream = concatenate_csv_streams(ctx.clone(), data)?;
            let fut = async move {
                copy_stream_to_writer(ctx.clone(), stream.data, io::stdout())
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
                let result_stream = data.map(move |stream| {
                    let path = path.clone();
                    let ctx = ctx.clone();
                    let if_exists = if_exists.clone();

                    async move {
                        // TODO: This join does not handle `..` or nested `/` in
                        // a particularly safe fashion.
                        let csv_path = path.join(&format!("{}.csv", stream.name));
                        let ctx = ctx.child(o!(
                            "stream" => stream.name.clone(),
                            "path" => format!("{}", csv_path.display()),
                        ));
                        write_stream_to_file(
                            ctx,
                            stream.data,
                            csv_path.clone(),
                            if_exists,
                        )
                        .await?;
                        Ok(CsvLocator::from_path(csv_path).boxed())
                    }
                        .boxed()
                });
                Ok(Box::new(result_stream) as BoxStream<BoxFuture<BoxLocator>>)
            } else {
                // Write all our streams as a single file.
                let stream = concatenate_csv_streams(ctx.clone(), data)?;
                let fut = async move {
                    let ctx = ctx.child(o!(
                        "stream" => stream.name.clone(),
                        "path" => format!("{}", path.display()),
                    ));
                    write_stream_to_file(ctx, stream.data, path.clone(), if_exists)
                        .await?;
                    Ok(CsvLocator::from_path(path).boxed())
                };
                Ok(box_stream_once(Ok(fut.boxed())))
            }
        }
    }
}

/// Write `data` to `dest`, honoring `if_exists`.
async fn write_stream_to_file(
    ctx: Context,
    data: BoxStream<BytesMut>,
    dest: PathBuf,
    if_exists: IfExists,
) -> Result<()> {
    // Make sure our destination directory exists.
    let dir = dest
        .parent()
        .ok_or_else(|| format_err!("cannot find parent dir for {}", dest.display()))?;
    fs::create_dir_all(dir)
        .compat()
        .await
        .with_context(|_| format!("unable to create directory {}", dir.display()))?;

    // Write our our CSV stream.
    debug!(ctx.log(), "writing stream to file {}", dest.display());
    let wtr = if_exists
        .to_async_open_options_no_append()?
        .open(dest.clone())
        .compat()
        .await
        .with_context(|_| format!("cannot open {}", dest.display()))?;
    copy_stream_to_writer(ctx.clone(), data, wtr)
        .await
        .with_context(|_| format!("error writing {}", dest.display()))?;
    Ok(())
}

impl LocatorStatic for CsvLocator {
    fn scheme() -> &'static str {
        "csv:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::SCHEMA
                | LocatorFeatures::LOCAL_DATA
                | LocatorFeatures::WRITE_LOCAL_DATA,
            write_schema_if_exists: IfExistsFeatures::empty(),
            source_args: SourceArgumentsFeatures::empty(),
            dest_args: DestinationArgumentsFeatures::empty(),
            dest_if_exists: IfExistsFeatures::no_append(),
            _placeholder: (),
        }
    }
}
