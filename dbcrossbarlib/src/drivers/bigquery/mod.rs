//! Driver for working with BigQuery schemas.

use std::{
    fmt,
    process::{Command, Stdio},
    str::FromStr,
};

use crate::common::*;
use crate::drivers::{
    bigquery_shared::{BqColumn, BqTable, TableName},
    gs::{GsLocator, GS_SCHEME},
};

mod local_data;
mod write_local_data;
mod write_remote_data;

use self::local_data::local_data_helper;
use self::write_local_data::write_local_data_helper;
use self::write_remote_data::write_remote_data_helper;

/// URL scheme for `BigQueryLocator`.
pub(crate) const BIGQUERY_SCHEME: &str = "bigquery:";

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
        if !s.starts_with(BIGQUERY_SCHEME) {
            return Err(format_err!("expected a bigquery: locator, found {}", s));
        }
        let table_name = s[BIGQUERY_SCHEME.len()..].parse()?;
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
        schema: Table,
        temporary_storage: TemporaryStorage,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.clone(), schema, temporary_storage).into_boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        schema: Table,
        data: BoxStream<CsvStream>,
        temporary_storage: TemporaryStorage,
        if_exists: IfExists,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        write_local_data_helper(
            ctx,
            self.clone(),
            schema,
            data,
            temporary_storage,
            if_exists,
        )
        .into_boxed()
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        // We can only do `write_remote_data` if `source` is a `GsLocator`.
        // Otherwise, we need to do `write_local_data` like normal.
        source.as_any().is::<GsLocator>()
    }

    fn write_remote_data(
        &self,
        ctx: Context,
        schema: Table,
        source: BoxLocator,
        temporary_storage: TemporaryStorage,
        if_exists: IfExists,
    ) -> BoxFuture<()> {
        write_remote_data_helper(
            ctx,
            schema,
            source,
            self.to_owned(),
            temporary_storage,
            if_exists,
        )
        .into_boxed()
    }
}

/// Given a `TemporaryStorage`, extract a unique `gs://` temporary directory,
/// including a random component.
pub(crate) fn find_gs_temp_dir(
    temporary_storage: &TemporaryStorage,
) -> Result<GsLocator> {
    let mut temp = temporary_storage
        .find_scheme(GS_SCHEME)
        .ok_or_else(|| format_err!("need `--temporary=gs://...` argument"))?
        .to_owned();
    if !temp.ends_with('/') {
        temp.push_str("/");
    }
    temp.push_str(&TemporaryStorage::random_tag());
    temp.push_str("/");
    GsLocator::from_str(&temp)
}
