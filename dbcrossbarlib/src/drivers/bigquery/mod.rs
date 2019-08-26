//! Driver for working with BigQuery.

use std::{
    fmt,
    process::{Command, Stdio},
    str::FromStr,
};

use crate::common::*;
use crate::drivers::{
    bigquery_shared::{BqColumn, BqTable, TableName},
    gs::GsLocator,
};

mod local_data;
mod write_local_data;
mod write_remote_data;

use self::local_data::local_data_helper;
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

    fn schema(&self, ctx: &Context) -> Result<Option<Table>> {
        let output = Command::new("bq")
            .args(&[
                "show",
                "--headless",
                "--schema",
                "--format=json",
                &self.table_name.to_string(),
            ])
            .stderr(Stdio::inherit())
            .output()
            .context("error running `bq show --schema`")?;
        if !output.status.success() {
            return Err(format_err!(
                "`bq show --schema` failed with {}",
                output.status,
            ));
        }
        debug!(
            ctx.log(),
            "BigQuery schema: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let columns: Vec<BqColumn> = serde_json::from_slice(&output.stdout)
            .context("error parsing BigQuery schema")?;
        let table = BqTable {
            name: self.table_name.clone(),
            columns,
        };
        Ok(Some(table.to_table()?))
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
            locator: LocatorFeatures::SCHEMA
                | LocatorFeatures::LOCAL_DATA
                | LocatorFeatures::WRITE_LOCAL_DATA,
            write_schema_if_exists: IfExistsFeatures::empty(),
            source_args: SourceArgumentsFeatures::WHERE_CLAUSE,
            dest_args: DestinationArgumentsFeatures::empty(),
            dest_if_exists: IfExistsFeatures::OVERWRITE
                | IfExistsFeatures::APPEND
                | IfExistsFeatures::UPSERT,
            _placeholder: (),
        }
    }
}
