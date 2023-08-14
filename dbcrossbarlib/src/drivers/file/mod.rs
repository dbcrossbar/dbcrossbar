//! Driver for working with CSV files.

use std::{
    ffi::{OsStr, OsString},
    fmt,
    path::PathBuf,
    str::FromStr,
};
use tokio::{
    fs,
    io::{self, BufReader},
};
use tracing::{field, Span};
use walkdir::WalkDir;

use crate::tokio_glue::{copy_reader_to_stream, copy_stream_to_writer};
use crate::{common::*, locator::PathLikeLocator};
use crate::{concat::concatenate_csv_streams, data_streams::DataStream};
use crate::{csv_stream::csv_stream_name, DataFormat};

/// (Incomplete.) A CSV file containing data, or a directory containing CSV
/// files.
///
/// TODO: Right now, we take a file path as input and a directory path as
/// output, because we're lazy and haven't finished building this.
#[derive(Clone, Debug)]
pub(crate) struct FileLocator {
    path: PathOrStdio,
}

impl FileLocator {
    /// Construt a `FileLocator` from a path.
    pub(crate) fn from_path<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: PathOrStdio::Path(path.into()),
        }
    }

    /// Construct a `FileLocator` using stdin/stdout.
    pub(crate) fn from_stdio() -> Self {
        Self {
            path: PathOrStdio::Stdio,
        }
    }
}

impl fmt::Display for FileLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for FileLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(FileLocator { path })
    }
}

impl Locator for FileLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Schema>> {
        schema_helper(ctx, self.clone()).boxed()
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
            // print out "file:-" to the same standard output.
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

#[instrument(level = "trace", name = "file::schema")]
async fn schema_helper(ctx: Context, locator: FileLocator) -> Result<Option<Schema>> {
    match &locator.path {
        PathOrStdio::Stdio => {
            // This is actually fairly tricky, because we may need to first
            // read the columns from stdin, _then_ start re-reading from the
            // beginning to read the data when `local_data` is called.
            Err(format_err!("cannot yet read schema from stdin"))
        }
        PathOrStdio::Path(_) if locator.is_directory_like() => {
            Err(format_err!("cannot read schema from directory {}", locator))
        }
        PathOrStdio::Path(path) => {
            let data_stream = path_to_data_stream(
                ctx.clone(),
                path.parent().unwrap().to_owned(),
                path.to_owned(),
                DataFormat::Csv,
            )
            .await?;
            data_stream.schema(&ctx).await
        }
    }
}

#[instrument(
    level = "trace",
    name = "file::local_data",
    skip_all,
    fields(path = %path)
)]
async fn local_data_helper(
    ctx: Context,
    path: PathOrStdio,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let shared_args = shared_args.verify(FileLocator::features())?;
    let schema = shared_args.schema().to_owned();

    let source_args = source_args.verify(FileLocator::features())?;
    let from_format = source_args.format().cloned();

    match path {
        PathOrStdio::Stdio => {
            let data = BufReader::with_capacity(BUFFER_SIZE, io::stdin());
            let stream = copy_reader_to_stream(data)?;
            let data_stream = DataStream {
                name: "data".to_owned(),
                format: from_format.unwrap_or_default(),
                data: stream
                    .map_err(move |e| format_err!("cannot read stdin: {}", e))
                    .boxed(),
            };
            let csv_stream = data_stream.into_csv_stream(&ctx, &schema).await?;
            Ok(Some(box_stream_once(Ok(csv_stream))))
        }
        PathOrStdio::Path(base_path) => {
            // Recursively look at our paths, picking out the ones that look
            // like CSVs. We do this synchronously because it's reasonably
            // fast and we'd like to catch errors up front.
            let mut paths = vec![];
            debug!("walking {}", base_path.display());
            let walker = WalkDir::new(&base_path).follow_links(true);
            let mut common_ext: Option<Option<OsString>> = None;
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

                let ext = p.extension().map(OsStr::to_ascii_lowercase);
                if let Some(common_ext) = &common_ext {
                    if ext != *common_ext {
                        return Err(format_err!(
                            "all files in {} must have the same extension",
                            base_path.display()
                        ));
                    }
                } else {
                    common_ext = Some(ext);
                }
                paths.push(p.to_owned());
            }
            let common_ext = common_ext.ok_or_else(|| {
                format_err!("no files found in {}", base_path.display())
            })?;
            let format = from_format
                .or(common_ext.map(|ext| DataFormat::from_extension(&ext)))
                .unwrap_or_default();

            let csv_streams = stream::iter(paths).map(Ok).and_then(move |file_path| {
                let ctx = ctx.clone();
                let schema = schema.clone();
                let base_path = base_path.clone();
                let file_path_copy = file_path.clone();
                let format = format.clone();
                async move {
                    let data_stream = path_to_data_stream(
                        ctx.clone(),
                        base_path.clone(),
                        file_path,
                        format,
                    ).await?;
                    data_stream.into_csv_stream(&ctx, &schema).await
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
    name = "file::path_to_data_stream",
    skip_all,
    fields(file_path = %file_path.display(), stream.name = field::Empty),
)]
async fn path_to_data_stream(
    _ctx: Context,
    base_path: PathBuf,
    file_path: PathBuf,
    format: DataFormat,
) -> Result<DataStream> {
    // Get the name of our stream.
    let name =
        csv_stream_name(&base_path.to_string_lossy(), &file_path.to_string_lossy())?
            .to_owned();
    Span::current().record("stream.name", &field::display(&name));

    // Open our file.
    let f = fs::File::open(file_path.clone())
        .await
        .with_context(|| format!("cannot open {}", file_path.display()))?;
    let rdr = BufReader::with_capacity(BUFFER_SIZE, f);
    let stream = copy_reader_to_stream(rdr)?;
    let data = stream
        .map_err(move |e| format_err!("cannot read {}: {}", file_path.display(), e))
        .boxed();
    let data_stream = DataStream { name, format, data };
    Ok(data_stream)
}

#[instrument(
    level = "debug",
    name = "file::write_local_data",
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
    let shared_args = shared_args.verify(FileLocator::features())?;
    let schema = shared_args.schema().to_owned();
    let dest_args = dest_args.verify(FileLocator::features())?;
    let if_exists = dest_args.if_exists().to_owned();
    match path {
        PathOrStdio::Stdio => {
            let format = dest_args.format().cloned().unwrap_or_default();
            if_exists.warn_if_not_default_for_stdout();
            let csv_stream = concatenate_csv_streams(ctx.clone(), data)?;
            let data_stream =
                DataStream::from_csv_stream(&ctx, format, &schema, csv_stream).await?;
            let fut = async move {
                copy_stream_to_writer(data_stream.data, io::stdout())
                    .await
                    .context("error writing to stdout")?;
                Ok(FileLocator {
                    path: PathOrStdio::Stdio,
                }
                .boxed())
            };
            Ok(box_stream_once(Ok(fut.boxed())))
        }
        PathOrStdio::Path(path) => {
            if path.to_string_lossy().ends_with('/') {
                // Write streams to our directory as multiple files.
                let format = dest_args.format().cloned().unwrap_or_default();
                let result_stream = data.map_ok(move |stream| {
                    let ctx = ctx.clone();
                    let path = path.clone();
                    let schema = schema.clone();
                    let format = format.clone();
                    let if_exists = if_exists.clone();
                    let stream_name = stream.name.clone();

                    async move {
                        // TODO: This join does not handle `..` or nested `/` in
                        // a particularly safe fashion.
                        let ext = format.extension();
                        let csv_path = path.join(format!("{}.{}", stream.name, ext));
                        Span::current().record("path", &field::display(csv_path.display()));
                        let data_stream =
                            DataStream::from_csv_stream(&ctx, format, &schema, stream).await?;
                        write_stream_to_file(
                            data_stream.data,
                            csv_path.clone(),
                            if_exists,
                        )
                        .await?;
                        Ok(FileLocator::from_path(csv_path).boxed())
                    }.instrument(trace_span!("stream_to_file", stream.name = %stream_name, path = field::Empty))
                    .boxed()
                });
                Ok(result_stream.boxed())
            } else {
                // Write all our streams as a single file.
                let format_for_ext = path.extension().map(DataFormat::from_extension);
                let format = dest_args
                    .format()
                    .cloned()
                    .or(format_for_ext)
                    .unwrap_or_default();

                let stream = concatenate_csv_streams(ctx.clone(), data)?;
                let stream_name = stream.name.clone();
                let path_copy = path.clone();
                let data_stream =
                    DataStream::from_csv_stream(&ctx, format, &schema, stream).await?;
                let fut = async move {
                    write_stream_to_file(data_stream.data, path.clone(), if_exists)
                        .await?;
                    Ok(FileLocator::from_path(path).boxed())
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

impl LocatorStatic for FileLocator {
    fn scheme() -> &'static str {
        "file:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::LocalData | LocatorFeatures::WriteLocalData,
            write_schema_if_exists: EnumSet::empty(),
            source_args: SourceArgumentsFeatures::Format.into(),
            dest_args: DestinationArgumentsFeatures::Format.into(),
            dest_if_exists: IfExistsFeatures::no_append(),
            _placeholder: (),
        }
    }
}

impl PathLikeLocator for FileLocator {
    fn path(&self) -> Option<&OsStr> {
        match &self.path {
            PathOrStdio::Path(path) => Some(path.as_os_str()),
            PathOrStdio::Stdio => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_streams::DataFormat;

    use super::*;

    #[test]
    fn test_directory_locator_has_correct_path_like_properties() {
        let locator = FileLocator::from_str("file:/path/").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/");
        assert!(locator.is_directory_like());
        assert!(locator.extension().is_none());
        assert!(locator.data_format().is_none());
    }

    #[test]
    fn test_csv_file_locator_has_correct_path_like_properties() {
        let locator = FileLocator::from_str("file:/path/file.csv").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/file.csv");
        assert!(!locator.is_directory_like());
        assert_eq!(locator.extension().unwrap(), "csv");
        assert_eq!(locator.data_format(), Some(DataFormat::Csv));
    }

    #[test]
    fn test_jsonl_file_locator_has_correct_path_like_properties() {
        let locator = FileLocator::from_str("file:/path/file.jsonl").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/file.jsonl");
        assert!(!locator.is_directory_like());
        assert_eq!(locator.extension().unwrap(), "jsonl");
        assert_eq!(locator.data_format(), Some(DataFormat::JsonLines));
    }
}
