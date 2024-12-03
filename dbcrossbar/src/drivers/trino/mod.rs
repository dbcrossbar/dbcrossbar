//! Driver for the Trino database.

use std::{fmt, str::FromStr};

use dbcrossbar_trino::client::{Client, ClientBuilder};

use crate::{common::*, drivers::trino_shared::TrinoStringLiteral};

use self::count::count_helper;
use self::local_data::local_data_helper;
use self::schema::schema_helper;
use self::write_local_data::write_local_data_helper;
use self::write_remote_data::write_remote_data_helper;
use self::write_schema::write_schema_helper;

use super::{
    s3::S3Locator,
    trino_shared::{TrinoConnectorType, TrinoTableName},
};

mod count;
mod local_data;
mod schema;
mod write_local_data;
mod write_remote_data;
mod write_schema;

/// A locator for a Trino table. JDBC uses `trino://host:port/catalog/schema` to
/// refer to schemas, so we'll just add a table name to that to get
/// `trino://host:port/catalog/schema/table_name`.
#[derive(Clone, Debug)]
pub(crate) struct TrinoLocator {
    /// The URL of the Trino server.
    url: UrlWithHiddenPassword,
    /// The name of the table.
    table_name: String,
}

impl TrinoLocator {
    /// Get the table name.
    pub fn table_name(&self) -> Result<TrinoTableName> {
        // Parse basic parts of our URL.
        let bare_url = self.url.as_url();
        let path = bare_url
            .path_segments()
            .ok_or_else(|| format_err!("missing /catalog/schema in {}", self.url))?
            .collect::<Vec<_>>();
        if path.len() != 2 {
            return Err(format_err!("expected /catalog/schema in {}", self.url));
        }
        let catalog = path[0];
        let schema = path[1];
        TrinoTableName::with_catalog(catalog, schema, &self.table_name)
    }

    /// Get a Trino client from a URL.
    pub(crate) fn client(&self) -> Result<Client> {
        // Parse basic parts of our URL.
        let bare_url = self.url.as_url();
        let host = bare_url
            .host_str()
            .ok_or_else(|| format_err!("missing host in {}", self.url))?;

        // Parse our path using Trino JDBC conventions.
        let mut builder = ClientBuilder::new(
            bare_url.username().to_owned(),
            host.to_owned(),
            bare_url.port().unwrap_or(8080),
        );
        if let Some(password) = bare_url.password() {
            // Basic auth requires a secure connection. If we _don't_ have
            // credentials, then there's probably no point in encrypting the
            // connection, either.
            builder = builder.password(password.to_owned()).use_https();
        }
        let client = builder.build();
        Ok(client)
    }

    /// Get the connector type for this locator.
    #[instrument(level = "debug", name = "TrinoLocator::connector_type", skip_all)]
    pub(crate) async fn connector_type(
        &self,
        client: &Client,
    ) -> Result<TrinoConnectorType> {
        let table_name = self.table_name()?;
        let catalog = table_name
            .catalog()
            .ok_or_else(|| format_err!("expected a catalog in {}", table_name))?;
        let sql = format!(
            "SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = {}",
            TrinoStringLiteral(catalog.as_unquoted_str())
        );
        debug!(%sql, "getting connector type");

        let connector_name = client.get_one_value::<String>(&sql).await?;
        Ok(TrinoConnectorType::from_str(&connector_name)?)
    }
}

impl fmt::Display for TrinoLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut full_url = self.url.clone();
        full_url.as_url_mut().set_fragment(Some(&self.table_name));
        full_url.fmt(f)
    }
}

impl FromStr for TrinoLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if !s.starts_with(Self::scheme()) {
            // Be careful not to leak the password in this error message.
            return Err(format_err!("expected a trino: locator"));
        }
        let mut url = UrlWithHiddenPassword::from_str(s)?;
        let table_name = url
            .as_url()
            .fragment()
            .ok_or_else(|| format_err!("{} needs to be followed by #table_name", url))?
            .to_owned();
        url.as_url_mut().set_fragment(None);
        Ok(TrinoLocator { url, table_name })
    }
}

impl Locator for TrinoLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        Self::scheme()
    }

    fn schema(
        &self,
        _ctx: Context,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        schema_helper(self.to_owned(), source_args).boxed()
    }

    fn write_schema(
        &self,
        _ctx: Context,
        schema: Schema,
        if_exists: IfExists,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<()> {
        write_schema_helper(self.to_owned(), schema, if_exists, dest_args).boxed()
    }

    fn count(
        &self,
        _ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        count_helper(self.to_owned(), shared_args, source_args).boxed()
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
        source.as_any().is::<S3Locator>()
    }

    fn write_remote_data(
        &self,
        _ctx: Context,
        source: BoxLocator,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<Vec<BoxLocator>> {
        write_remote_data_helper(
            self.to_owned(),
            source,
            shared_args,
            source_args,
            dest_args,
        )
        .boxed()
    }
}

impl LocatorStatic for TrinoLocator {
    fn scheme() -> &'static str {
        "trino:"
    }

    fn features() -> Features {
        let if_exists = IfExistsFeatures::Error
            | IfExistsFeatures::Overwrite
            | IfExistsFeatures::Append
            | IfExistsFeatures::Upsert;
        Features {
            locator: LocatorFeatures::Schema
                | LocatorFeatures::WriteSchema
                | LocatorFeatures::LocalData
                | LocatorFeatures::WriteLocalData
                | LocatorFeatures::Count,
            write_schema_if_exists: if_exists,
            source_args: SourceArgumentsFeatures::DriverArgs
                | SourceArgumentsFeatures::WhereClause,
            dest_args: DestinationArgumentsFeatures::DriverArgs.into(),
            dest_if_exists: if_exists,
            _placeholder: (),
        }
    }

    /// Is this driver unstable?
    fn is_unstable() -> bool {
        true
    }
}
