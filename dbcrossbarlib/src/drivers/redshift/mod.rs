//! Driver for working with Redshift.

use lazy_static::lazy_static;
use regex::Regex;
use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;
use crate::drivers::postgres::PostgresLocator;
use crate::drivers::{postgres_shared::pg_quote, s3::S3Locator};

mod local_data;
mod write_local_data;
mod write_remote_data;

use local_data::local_data_helper;
use write_local_data::write_local_data_helper;
use write_remote_data::write_remote_data_helper;

/// A locator for a Redshift table.
#[derive(Debug, Clone)]
pub struct RedshiftLocator {
    /// Internally store this as a PostgreSQL locator.
    postgres_locator: PostgresLocator,
}

impl RedshiftLocator {
    /// The URL for this locator.
    pub(crate) fn url(&self) -> &Url {
        self.postgres_locator.url()
    }

    /// The table name for this locator.
    pub(crate) fn table_name(&self) -> &str {
        self.postgres_locator.table_name()
    }
}

impl fmt::Display for RedshiftLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pg = self.postgres_locator.to_string();
        assert!(pg.starts_with("postgres:"));
        pg.replacen("postgres:", "redshift:", 1).fmt(f)
    }
}

#[test]
fn do_not_display_password() {
    let l = "redshift://user:pass@host/db#table"
        .parse::<RedshiftLocator>()
        .expect("could not parse locator");
    assert_eq!(format!("{}", l), "redshift://user:XXXXXX@host/db#table");
}

impl FromStr for RedshiftLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with("redshift:") {
            // Don't print the unparsed locator in the error because that would
            // leak the password.
            return Err(format_err!("Redshift locator must begin with redshift://"));
        }
        let postgres_locator = s.replacen("redshift:", "postgres:", 1).parse()?;
        Ok(RedshiftLocator { postgres_locator })
    }
}

impl Locator for RedshiftLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Table>> {
        self.postgres_locator.schema(ctx)
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.to_owned(), shared_args, source_args).boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        write_local_data_helper(ctx, self.to_owned(), data, shared_args, dest_args)
            .boxed()
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        // We can only do `write_remote_data` if `source` is a `S3Locator`.
        // Otherwise, we need to do `write_local_data` like normal.
        source.as_any().is::<S3Locator>()
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

impl LocatorStatic for RedshiftLocator {
    fn scheme() -> &'static str {
        "redshift:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::SCHEMA
                | LocatorFeatures::LOCAL_DATA
                | LocatorFeatures::WRITE_LOCAL_DATA,
            write_schema_if_exists: IfExistsFeatures::empty(),
            source_args: SourceArgumentsFeatures::DRIVER_ARGS
                | SourceArgumentsFeatures::WHERE_CLAUSE,
            dest_args: DestinationArgumentsFeatures::DRIVER_ARGS,
            dest_if_exists: IfExistsFeatures::OVERWRITE | IfExistsFeatures::APPEND,
            _placeholder: (),
        }
    }
}

/// Given a `DriverArgs` structure, convert it into Redshift credentials SQL.
pub(crate) fn credentials_sql(args: &DriverArguments) -> Result<String> {
    let mut out = vec![];
    for (k, v) in args.iter() {
        lazy_static! {
            static ref KEY_RE: Regex =
                Regex::new("^[-_A-Za-z0-9]+$").expect("invalid regex in source code");
        }
        if !KEY_RE.is_match(k) {
            return Err(format_err!("cannot pass {:?} as Redshift credential", k));
        }
        writeln!(&mut out, "{} {}", k, pg_quote(v))?;
    }
    Ok(String::from_utf8(out).expect("found non-UTF-8 SQL"))
}
