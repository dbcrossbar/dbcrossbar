//! Driver for working with BigQuery.

use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::{bigquery_shared::TableName, gs::GsLocator};

mod count;
mod local_data;
mod schema;
mod write_local_data;
mod write_remote_data;

use self::count::count_helper;
use self::local_data::local_data_helper;
use self::schema::schema_helper;
use self::write_local_data::write_local_data_helper;
use self::write_remote_data::write_remote_data_helper;

/// A locator for a BigQuery table.
#[derive(Debug, Clone)]
pub struct BigQueryLocator {
    /// The table pointed to by this locator.
    table_name: TableName,
}

impl BigQueryLocator {
    /// The table name for this locator.
    pub(crate) fn as_table_name(&self) -> &TableName {
        &self.table_name
    }

    /// This locator's BigQuery project.
    pub(crate) fn project(&self) -> &str {
        self.table_name.project()
    }
}

impl fmt::Display for BigQueryLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bigquery:{}", self.table_name)
    }
}

impl FromStr for BigQueryLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with(Self::scheme()) {
            return Err(format_err!("expected a bigquery: locator, found {}", s));
        }
        let table_name = s[Self::scheme().len()..].parse()?;
        Ok(BigQueryLocator { table_name })
    }
}

impl Locator for BigQueryLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Table>> {
        schema_helper(ctx, self.to_owned()).boxed()
    }

    fn count(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        count_helper(ctx, self.to_owned(), shared_args, source_args).boxed()
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.clone(), shared_args, source_args).boxed()
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
        // We can only do `write_remote_data` if `source` is a `GsLocator`.
        // Otherwise, we need to do `write_local_data` like normal.
        source.as_any().is::<GsLocator>()
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

impl LocatorStatic for BigQueryLocator {
    fn scheme() -> &'static str {
        "bigquery:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema
                | LocatorFeatures::LocalData
                | LocatorFeatures::WriteLocalData
                | LocatorFeatures::Count,
            write_schema_if_exists: EnumSet::empty(),
            source_args: SourceArgumentsFeatures::WhereClause.into(),
            dest_args: EnumSet::empty(),
            dest_if_exists: IfExistsFeatures::Overwrite
                | IfExistsFeatures::Append
                | IfExistsFeatures::Upsert,
            _placeholder: (),
        }
    }
}
