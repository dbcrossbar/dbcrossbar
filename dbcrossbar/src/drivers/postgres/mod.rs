//! A driver for working with Postgres.

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;
use crate::drivers::postgres_shared::{Client, PgName, PgSchema};

mod count;
mod csv_to_binary;
mod local_data;
mod write_local_data;

use self::count::count_helper;
use self::local_data::local_data_helper;
use self::write_local_data::write_local_data_helper;

pub(crate) use write_local_data::{
    columns_to_update_for_upsert, create_temp_table_for, prepare_table,
};

/// A Postgres database URL and a table name.
///
/// This is the central point of access for talking to a running PostgreSQL
/// database.
#[derive(Clone, Debug)]
pub struct PostgresLocator {
    url: UrlWithHiddenPassword,
    table_name: PgName,
}

impl PostgresLocator {
    /// The URL associated with this locator.
    pub(crate) fn url(&self) -> &UrlWithHiddenPassword {
        &self.url
    }

    /// The table name associated with this locator.
    pub(crate) fn table_name(&self) -> &PgName {
        &self.table_name
    }
}

impl fmt::Display for PostgresLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut full_url = self.url.clone();
        full_url
            .as_url_mut()
            .set_fragment(Some(&self.table_name.unquoted()));
        full_url.fmt(f)
    }
}

#[test]
fn do_not_display_password() {
    let l = "postgres://user:pass@host/db#table"
        .parse::<PostgresLocator>()
        .expect("could not parse locator");
    assert_eq!(format!("{}", l), "postgres://user:XXXXXX@host/db#table");
}

impl FromStr for PostgresLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut url: Url = s.parse::<Url>().context("cannot parse Postgres URL")?;
        if url.scheme() != &Self::scheme()[..Self::scheme().len() - 1] {
            Err(format_err!("expected URL scheme postgres: {:?}", s))
        } else {
            // Extract table name from URL.
            let table_name = url
                .fragment()
                .ok_or_else(|| {
                    format_err!("{} needs to be followed by #table_name", url)
                })?
                .parse::<PgName>()?;
            url.set_fragment(None);
            let url = UrlWithHiddenPassword::new(url);
            Ok(PostgresLocator { url, table_name })
        }
    }
}

#[test]
fn from_str_parses_schemas() {
    let examples = &[
        ("postgres://user:pass@host/db#table", "table"),
        ("postgres://user:pass@host/db#public.table", "public.table"),
        (
            "postgres://user:pass@host/db#testme1.table",
            "testme1.table",
        ),
    ];
    for &(url, table_name) in examples {
        assert_eq!(
            PostgresLocator::from_str(url).unwrap().table_name,
            table_name.parse::<PgName>().unwrap(),
        );
    }
}

impl Locator for PostgresLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Schema>> {
        let source = self.to_owned();
        async move {
            let schema =
                PgSchema::from_pg_catalog(&ctx, &source.url, &source.table_name)
                    .await?
                    .ok_or_else(|| format_err!("no such table {}", source))?;
            Ok(Some(schema.to_schema()?))
        }
        .boxed()
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
        local_data_helper(
            ctx,
            self.url.clone(),
            self.table_name.clone(),
            shared_args,
            source_args,
        )
        .boxed()
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

impl LocatorStatic for PostgresLocator {
    fn scheme() -> &'static str {
        "postgres:"
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
                | IfExistsFeatures::Error
                | IfExistsFeatures::Upsert,
            _placeholder: (),
        }
    }
}
