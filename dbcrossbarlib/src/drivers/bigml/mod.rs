//! Support for BigML data sets.

use bigml::resource::{Dataset, Id};
use std::{fmt, str::FromStr};

use crate::common::*;

//mod local_data;
//mod prepare_as_destination;
//mod write_local_data;
//mod write_remote_data;
//
//use local_data::local_data_helper;
//pub(crate) use prepare_as_destination::prepare_as_destination_helper;
//use write_local_data::write_local_data_helper;
//use write_remote_data::write_remote_data_helper;

/// Various read and write actions we can take with BigML.
#[derive(Clone, Debug)]
enum BigMlAction {
    /// Create a single `source/$ID` resource on BigML, containing all the data.
    CreateSource,
    /// Create one or more `source/$ID` resources on BigML.
    CreateSources,
    /// Read data from the specified dataset.
    ReadDataset(Id<Dataset>),
}

/// A locator specifying either how to upload data to BigML, or where to
/// download it from.
#[derive(Clone, Debug)]
pub(crate) struct BigMlLocator {
    action: BigMlAction,
}

impl BigMlLocator {
    /// Create a `bigml:source` locator, which writes all the data to a single
    /// BigML "source" object.
    pub fn create_source() -> Self {
        Self {
            action: BigMlAction::CreateSource,
        }
    }

    /// Create a `bigml:sources` locator, which writes all the data to one or
    /// more BigML "source" objects.
    pub fn create_sources() -> Self {
        Self {
            action: BigMlAction::CreateSources,
        }
    }

    /// Create a `bigml:dataset/$ID` locator, which reads data from the
    /// specified data set.
    pub fn read_dataset(id: Id<Dataset>) -> Self {
        Self {
            action: BigMlAction::ReadDataset(id),
        }
    }
}

impl fmt::Display for BigMlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.action {
            BigMlAction::CreateSource => write!(f, "bigml:source"),
            BigMlAction::CreateSources => write!(f, "bigml:sources"),
            BigMlAction::ReadDataset(id) => write!(f, "bigml:{}", id),
        }
    }
}

impl FromStr for BigMlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "bigml:source" {
            Ok(BigMlLocator::create_source())
        } else if s == "bigml:sources" {
            Ok(BigMlLocator::create_sources())
        } else if s.starts_with(Self::scheme()) {
            let id = s[Self::scheme().len()..].parse::<Id<Dataset>>()?;
            Ok(BigMlLocator::read_dataset(id))
        } else {
            Err(format_err!("expected {} to begin with bigml:", s))
        }
    }
}

impl Locator for BigMlLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    //fn local_data(
    //    &self,
    //    ctx: Context,
    //    shared_args: SharedArguments<Unverified>,
    //    source_args: SourceArguments<Unverified>,
    //) -> BoxFuture<Option<BoxStream<CsvStream>>> {
    //    local_data_helper(ctx, self.url.clone(), shared_args, source_args).boxed()
    //}

    //fn write_local_data(
    //    &self,
    //    ctx: Context,
    //    data: BoxStream<CsvStream>,
    //    shared_args: SharedArguments<Unverified>,
    //    dest_args: DestinationArguments<Unverified>,
    //) -> BoxFuture<BoxStream<BoxFuture<()>>> {
    //    write_local_data_helper(ctx, self.url.clone(), data, shared_args, dest_args)
    //        .boxed()
    //}
}

impl LocatorStatic for BigMlLocator {
    fn scheme() -> &'static str {
        "bigml:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::empty(), //LocatorFeatures::LOCAL_DATA | LocatorFeatures::WRITE_LOCAL_DATA,
            write_schema_if_exists: IfExistsFeatures::empty(),
            source_args: SourceArgumentsFeatures::empty(),
            dest_args: DestinationArgumentsFeatures::empty(),
            // We allow all `--if-exists` features because we always generate a
            // unique destination name.
            dest_if_exists: IfExistsFeatures::all(),
            _placeholder: (),
        }
    }
}
