//! Driver for working with CSV files.

use std::{ffi::OsStr, fmt, str::FromStr};

use super::file::FileLocator;
use crate::{common::*, locator::PathLikeLocator};

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
    /// Convert this CSV locator into a `file:` locator. We use this for
    /// forwarding calls to [`FileLocator`], which actually implements all our
    /// logic now.
    fn to_file_locator(&self) -> FileLocator {
        match &self.path {
            PathOrStdio::Path(path) => FileLocator::from_path(path),
            PathOrStdio::Stdio => FileLocator::from_stdio(),
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

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
    }

    #[instrument(level = "trace", name = "csv::schema", skip(source_args))]
    fn schema(
        &'_ self,
        ctx: Context,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        if self.is_directory_like() {
            async { Err(format_err!("cannot get schema for directory")) }.boxed()
        } else if self.extension() != Some(OsStr::new("csv")) {
            async { Err(format_err!("cannot get schema for non-CSV file")) }.boxed()
        } else {
            let locator = self.to_file_locator();
            locator.schema(ctx, source_args)
        }
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        let locator = self.to_file_locator();
        match source_args.with_format_csv() {
            Ok(source_args) => locator.local_data(ctx, shared_args, source_args),
            Err(e) => async { Err(e) }.boxed(),
        }
    }

    fn display_output_locators(&self) -> DisplayOutputLocators {
        let locator = self.to_file_locator();
        locator.display_output_locators()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        let locator = self.to_file_locator();
        match dest_args.with_format_csv() {
            Ok(dest_args) => {
                locator.write_local_data(ctx, data, shared_args, dest_args)
            }
            Err(e) => async { Err(e) }.boxed(),
        }
    }
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

impl PathLikeLocator for CsvLocator {
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
        let locator = CsvLocator::from_str("csv:/path/").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/");
        assert!(locator.is_directory_like());
        assert!(locator.extension().is_none());
        assert!(locator.data_format().is_none());
    }

    #[test]
    fn test_file_locator_has_correct_path_like_properties() {
        let locator = CsvLocator::from_str("csv:/path/file.csv").unwrap();
        assert_eq!(locator.path().unwrap(), "/path/file.csv");
        assert!(!locator.is_directory_like());
        assert_eq!(locator.extension().unwrap(), "csv");
        assert_eq!(locator.data_format(), Some(DataFormat::Csv));
    }
}
