//! Support for Google Cloud Storage.

use std::ffi::OsStr;
use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;
use crate::locator::PathLikeLocator;

mod local_data;
mod prepare_as_destination;
mod write_local_data;
mod write_remote_data;

use local_data::local_data_helper;
pub(crate) use prepare_as_destination::prepare_as_destination_helper;
use write_local_data::write_local_data_helper;
use write_remote_data::write_remote_data_helper;

#[derive(Clone, Debug)]
pub(crate) struct GsLocator {
    url: Url,
}

impl GsLocator {
    /// Access the `gs://` URL in this locator.
    pub(crate) fn as_url(&self) -> &Url {
        &self.url
    }

    /// Does this locator point at a `gs://` directory?
    pub(crate) fn is_directory(&self) -> bool {
        self.url.path().ends_with('/')
    }

    /// Does this locator point at a `gs://` CSV file?
    pub(crate) fn is_csv_file(&self) -> bool {
        self.url.path().to_ascii_lowercase().ends_with(".csv")
    }
}

impl fmt::Display for GsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for GsLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(Self::scheme()) {
            let url = s
                .parse::<Url>()
                .with_context(|| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with gs://", url))
            } else {
                let locator = GsLocator { url };
                if !locator.is_directory() && !locator.is_csv_file() {
                    Err(format_err!("{} must end with a '/' or '.csv'", locator))
                } else {
                    Ok(locator)
                }
            }
        } else {
            Err(format_err!("expected {} to begin with gs://", s))
        }
    }
}

impl Locator for GsLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.url.clone(), shared_args, source_args).boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        write_local_data_helper(ctx, self.clone(), data, shared_args, dest_args)
            .boxed()
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        // We can only do `write_remote_data` if `source` is a
        // `BigQueryLocator`. Otherwise, we need to do `write_local_data` like
        // normal.
        //
        // Also, BigQuery can only write directories of CSV files, so if we're
        // not pointed to a directory, don't use remote operations.
        source.as_any().is::<BigQueryLocator>() && self.is_directory()
    }

    fn write_remote_data(
        &self,
        ctx: Context,
        source: BoxLocator,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<Vec<BoxLocator>> {
        write_remote_data_helper(
            ctx,
            source,
            self.to_owned(),
            shared_args,
            source_args,
            dest_args,
        )
        .boxed()
    }
}

impl LocatorStatic for GsLocator {
    fn scheme() -> &'static str {
        "gs:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::LocalData | LocatorFeatures::WriteLocalData,
            write_schema_if_exists: EnumSet::empty(),
            source_args: SourceArgumentsFeatures::Format.into(),
            dest_args: DestinationArgumentsFeatures::Format.into(),
            dest_if_exists: IfExistsFeatures::Overwrite.into(),
            _placeholder: (),
        }
    }
}

impl PathLikeLocator for GsLocator {
    fn path(&self) -> Option<&OsStr> {
        Some(OsStr::new(self.url.path()))
    }
}

/// Given a `TemporaryStorage`, extract a unique `gs://` temporary directory,
/// including a random component.
pub(crate) fn find_gs_temp_dir(
    temporary_storage: &TemporaryStorage,
) -> Result<GsLocator> {
    let mut temp = temporary_storage
        .find_scheme(GsLocator::scheme())
        .ok_or_else(|| format_err!("need `--temporary=gs://...` argument"))?
        .to_owned();
    if !temp.ends_with('/') {
        temp.push('/');
    }
    temp.push_str(&TemporaryStorage::random_tag());
    temp.push('/');
    GsLocator::from_str(&temp)
}

#[cfg(test)]
mod tests {
    use crate::data_stream::DataFormat;

    use super::*;

    #[test]
    fn test_s3_locator_url_parses() {
        let locator = GsLocator::from_str("gs://bucket/path/").unwrap();
        assert_eq!(locator.url.scheme(), "gs");
        assert_eq!(locator.url.host_str(), Some("bucket"));
        assert_eq!(locator.url.path(), "/path/");
    }

    #[test]
    fn test_directory_locator_has_correct_path_like_properties() {
        let locator = GsLocator::from_str("gs://bucket/path/").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/");
        assert!(locator.is_directory_like());
        assert!(locator.extension().is_none());
        assert!(locator.data_format().is_none());
    }

    #[test]
    fn test_file_locator_has_correct_path_like_properties() {
        let locator = GsLocator::from_str("gs://bucket/path/file.csv").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/file.csv");
        assert!(!locator.is_directory_like());
        assert_eq!(locator.extension().unwrap(), "csv");
        assert_eq!(locator.data_format(), Some(DataFormat::Csv));
    }
}
