//! Support for Amazon's S3.

use std::ffi::OsStr;
use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::redshift::RedshiftLocator;
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
pub(crate) struct S3Locator {
    url: Url,
}

impl S3Locator {
    /// Access the `s3://` URL in this locator.
    pub(crate) fn as_url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Display for S3Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for S3Locator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(Self::scheme()) {
            let url = s
                .parse::<Url>()
                .with_context(|| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with s3://", url))
            } else if !url.path().ends_with('/') {
                Err(format_err!("{} must end with a '/'", url))
            } else {
                Ok(S3Locator { url })
            }
        } else {
            Err(format_err!("expected {} to begin with s3://", s))
        }
    }
}

impl Locator for S3Locator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
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
        _ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        write_local_data_helper(self.url.clone(), data, shared_args, dest_args).boxed()
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        // We can only do `write_remote_data` if `source` is a
        // `RedshiftLocator`. Otherwise, we need to do `write_local_data` like
        // normal.
        source.as_any().is::<RedshiftLocator>()
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

impl LocatorStatic for S3Locator {
    fn scheme() -> &'static str {
        "s3:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::LocalData | LocatorFeatures::WriteLocalData,
            write_schema_if_exists: EnumSet::empty(),
            source_args: Default::default(),
            dest_args: Default::default(),
            dest_if_exists: IfExistsFeatures::Overwrite.into(),
            _placeholder: (),
        }
    }
}

/// Given a `TemporaryStorage`, extract a unique `s3://` temporary directory,
/// including a random component.
pub(crate) fn find_s3_temp_dir(
    temporary_storage: &TemporaryStorage,
) -> Result<S3Locator> {
    let mut temp = temporary_storage
        .find_scheme(S3Locator::scheme())
        .ok_or_else(|| format_err!("need `--temporary=s3://...` argument"))?
        .to_owned();
    if !temp.ends_with('/') {
        temp.push('/');
    }
    temp.push_str(&TemporaryStorage::random_tag());
    temp.push('/');
    S3Locator::from_str(&temp)
}

impl PathLikeLocator for S3Locator {
    fn path(&self) -> Option<&OsStr> {
        Some(OsStr::new(self.url.path()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_locator_url_parses() {
        let locator = S3Locator::from_str("s3://bucket/path/").unwrap();
        assert_eq!(locator.url.scheme(), "s3");
        assert_eq!(locator.url.host_str(), Some("bucket"));
        assert_eq!(locator.url.path(), "/path/");
    }

    #[test]
    fn test_directory_locator_has_correct_path_like_properties() {
        let locator = S3Locator::from_str("s3://bucket/path/").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/");
        assert!(locator.is_directory_like());
        assert!(locator.extension().is_none());
        assert!(locator.data_format().is_none());
    }

    // Actually not yet supported!
    //
    // #[test]
    // fn test_file_locator_has_correct_path_like_properties() {
    //     let locator = S3Locator::from_str("s3://bucket/path/file.csv").unwrap();
    //     assert_eq!(locator.path().unwrap(), "/path/file.csv");
    //     assert!(!locator.is_directory_like());
    //     assert_eq!(locator.extension().unwrap(), "csv");
    //     assert_eq!(locator.data_format(), Some(DataFormat::Csv));
    // }
}
