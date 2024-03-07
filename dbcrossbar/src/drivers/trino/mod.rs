//! Driver for working with Trino databases.

use std::fmt;
use std::str::FromStr;

use crate::common::*;

use self::schema::schema_helper;
use self::types::TrinoTableName;

/// Quick and dirty retry loop to deal with transient Trino errors. We will
/// probably ultimately want exponential backoff, etc.
macro_rules! retry_trino_error {
    ($e:expr) => {{
        use crate::drivers::trino::errors::should_retry;
        use log::error;
        use std::time::Duration;
        use tokio::time::sleep;

        let mut max_tries = 3;
        let mut sleep_duration = Duration::from_millis(500);
        loop {
            match $e {
                Ok(val) => break Ok(val),
                Err(e) if should_retry(&e) && max_tries > 0 => {
                    error!("Retrying Trino query after error: {}", e);
                    sleep(sleep_duration).await;
                    max_tries -= 1;
                    sleep_duration *= 2;
                    continue;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}

mod errors;
mod schema;
mod types;

/// A Trino database locator.
#[derive(Clone, Debug)]
pub(crate) struct TrinoLocator {
    /// The URL of the Trino database.
    ///
    /// Standard Trino "URL" format:
    /// trino://anyone@localhost:8088/memory/default. We also add "/table" to
    /// get a `dbcrossbar` locator.
    url: Url,
}

impl TrinoLocator {
    /// Get the catalog, schema, and table for this locator.
    fn table_name(&self) -> Result<TrinoTableName> {
        let path_segments = self
            .url
            .path_segments()
            .ok_or_else(|| {
                format_err!("expected path segments in Trino URL: {}", self.url)
            })?
            .collect::<Vec<_>>();
        if path_segments.len() != 3 {
            return Err(format_err!("expected URL of form trino://anyone@localhost:8088/catalog/schema/table, found: {}", self.url));
        }
        Ok(TrinoTableName {
            catalog: path_segments[0].to_owned(),
            schema: path_segments[1].to_owned(),
            table: path_segments[2].to_owned(),
        })
    }

    /// Get a client for this Trino locator.
    fn client(&self) -> Result<prusto::Client> {
        let username = self.url.username();
        let host = self
            .url
            .host_str()
            .ok_or_else(|| format_err!("expected host in Trino URL: {}", self.url))?;
        let port = self.url.port().unwrap_or(8080);
        let table_name = self.table_name()?;
        prusto::ClientBuilder::new(username, host)
            .port(port)
            .catalog(&table_name.catalog)
            .schema(&table_name.schema)
            .build()
            .context("could not create Trino client")
    }
}

impl fmt::Display for TrinoLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for TrinoLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with("trino://") {
            return Err(format_err!("not a trino:// locator: {}", s));
        }
        Ok(TrinoLocator {
            url: Url::from_str(s)?,
        })
    }
}

impl Locator for TrinoLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
    }

    fn schema(
        &'_ self,
        _ctx: Context,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        schema_helper(self.to_owned(), source_args).boxed()
    }

    fn count(
        &self,
        _ctx: Context,
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        todo!()
    }

    fn local_data(
        &self,
        _ctx: Context,
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        todo!("TrinoLocator:local_data")
    }

    fn write_local_data(
        &self,
        _ctx: Context,
        _data: BoxStream<CsvStream>,
        _shared_args: SharedArguments<Unverified>,
        _dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        todo!("TrinoLocator::write_local_data")
    }
}

impl LocatorStatic for TrinoLocator {
    fn scheme() -> &'static str {
        "trino:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema
                | LocatorFeatures::LocalData
                | LocatorFeatures::WriteLocalData
                | LocatorFeatures::Count,
            write_schema_if_exists: EnumSet::empty(),
            source_args: SourceArgumentsFeatures::DriverArgs
                | SourceArgumentsFeatures::WhereClause,
            dest_args: DestinationArgumentsFeatures::DriverArgs.into(),
            // TODO: Which of these can we actually support?
            dest_if_exists: IfExistsFeatures::Error
                | IfExistsFeatures::Overwrite
                | IfExistsFeatures::Append
                | IfExistsFeatures::Upsert,
            _placeholder: (),
        }
    }
}
