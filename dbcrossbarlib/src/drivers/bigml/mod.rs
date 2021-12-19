//! Support for BigML data sets.

use bigml::resource::{Dataset, Id, Source};
use std::{fmt, str::FromStr};

use crate::common::*;

mod data_type;
mod local_data;
mod schema;
mod source;
mod write_local_data;

use local_data::local_data_helper;
use schema::schema_helper;
use write_local_data::write_local_data_helper;

/// Various read and write actions we can take with BigML.
#[derive(Clone, Debug)]
enum BigMlAction {
    /// Create a single `dataset/$ID` resource on BigML, containing all the data.
    CreateDataset,
    /// Create one or more `dataset/$ID` resources on BigML.
    CreateDatasets,
    /// Create a single `source/$ID` resource on BigML, containing all the data.
    CreateSource,
    /// Create one or more `source/$ID` resources on BigML.
    CreateSources,
    /// Read data from the specified dataset.
    ReadDataset(Id<Dataset>),
    /// This cannot be directly used as a source or destination, but it can be
    /// printed as output from our driver.
    OutputSource(Id<Source>),
}

/// (Internal.) Options for resource creation.
pub(self) struct CreateOptions {
    /// Should we concatenate our input CSVs into a single stream?
    pub(self) concat_csv_streams: bool,
    /// Should we convert our initial source into a dataset?
    pub(self) convert_to_dataset: bool,
}

/// A locator specifying either how to upload data to BigML, or where to
/// download it from.
#[derive(Clone, Debug)]
pub(crate) struct BigMlLocator {
    action: BigMlAction,
}

impl BigMlLocator {
    /// Create a `bigml:dataset` locator, which writes all the data to a single
    /// BigML "dataset" object.
    pub fn create_dataset() -> Self {
        Self {
            action: BigMlAction::CreateDataset,
        }
    }

    /// Create a `bigml:datasets` locator, which writes all the data to one or
    /// more BigML "dataset" objects.
    pub fn create_datasets() -> Self {
        Self {
            action: BigMlAction::CreateDatasets,
        }
    }

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

    /// (Internal only.) Create a `bigml:source/$ID` locator.
    pub(self) fn output_source(id: Id<Source>) -> Self {
        Self {
            action: BigMlAction::OutputSource(id),
        }
    }

    /// Given a `BigMlAction`, convert it into flags specifying how to create
    /// a resource on BigML. If this is a _source_ locator, return `None`.
    pub(self) fn to_create_options(&self) -> Option<CreateOptions> {
        match &self.action {
            BigMlAction::CreateDataset => Some(CreateOptions {
                concat_csv_streams: true,
                convert_to_dataset: true,
            }),
            BigMlAction::CreateDatasets => Some(CreateOptions {
                concat_csv_streams: false,
                convert_to_dataset: true,
            }),
            BigMlAction::CreateSource => Some(CreateOptions {
                concat_csv_streams: true,
                convert_to_dataset: false,
            }),
            BigMlAction::CreateSources => Some(CreateOptions {
                concat_csv_streams: false,
                convert_to_dataset: false,
            }),
            BigMlAction::ReadDataset(_) | BigMlAction::OutputSource(_) => None,
        }
    }
}

impl fmt::Display for BigMlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.action {
            BigMlAction::CreateDataset => write!(f, "bigml:dataset"),
            BigMlAction::CreateDatasets => write!(f, "bigml:datasets"),
            BigMlAction::CreateSource => write!(f, "bigml:source"),
            BigMlAction::CreateSources => write!(f, "bigml:sources"),
            BigMlAction::ReadDataset(id) => write!(f, "bigml:{}", id),
            BigMlAction::OutputSource(id) => write!(f, "bigml:{}", id),
        }
    }
}

impl FromStr for BigMlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "bigml:dataset" {
            Ok(BigMlLocator::create_dataset())
        } else if s == "bigml:datasets" {
            Ok(BigMlLocator::create_datasets())
        } else if s == "bigml:source" {
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

    fn schema(&self, _ctx: Context) -> BoxFuture<Option<Schema>> {
        schema_helper(self.to_owned()).boxed()
    }

    fn local_data(
        &self,
        _ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(self.clone(), shared_args, source_args).boxed()
    }

    fn display_output_locators(&self) -> DisplayOutputLocators {
        match &self.action {
            // Our actual destination locators can't be inferred from what
            // the user specified, because BigML assigns unique IDs. So we
            // need to display where we put the data.
            BigMlAction::CreateDataset
            | BigMlAction::CreateDatasets
            | BigMlAction::CreateSource
            | BigMlAction::CreateSources => DisplayOutputLocators::ByDefault,
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
        write_local_data_helper(ctx, self.clone(), data, shared_args, dest_args)
            .boxed()
    }
}

impl LocatorStatic for BigMlLocator {
    fn scheme() -> &'static str {
        "bigml:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema
                | LocatorFeatures::LocalData
                | LocatorFeatures::WriteLocalData,
            write_schema_if_exists: EnumSet::empty(),
            source_args: EnumSet::empty(),
            dest_args: DestinationArgumentsFeatures::DriverArgs.into(),
            // We allow all `--if-exists` features because we always generate a
            // unique destination name.
            dest_if_exists: EnumSet::all(),
            _placeholder: (),
        }
    }
}
