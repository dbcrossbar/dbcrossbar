//! Driver for the Trino database.

use std::{fmt, str::FromStr};

use prusto::Presto;

use crate::{common::*, drivers::trino_shared::TrinoStringLiteral};

use self::local_data::local_data_helper;
use self::schema::schema_helper;
use self::write_local_data::write_local_data_helper;
use self::write_remote_data::write_remote_data_helper;
use self::write_schema::write_schema_helper;

use super::{
    s3::S3Locator,
    trino_shared::{TrinoConnectorType, TrinoTableName},
};

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
    pub(crate) fn client(&self) -> Result<prusto::Client> {
        // Parse basic parts of our URL.
        let bare_url = self.url.as_url();
        let host = bare_url
            .host_str()
            .ok_or_else(|| format_err!("missing host in {}", self.url))?;

        // Parse our path using Trino JDBC conventions.
        let table_name = self.table_name()?;
        let catalog = table_name
            .catalog()
            .ok_or_else(|| format_err!("expected a catalog in {}", self))?;
        let schema = table_name
            .schema()
            .ok_or_else(|| format_err!("expected a schema in {}", self))?;

        // Set up basic auth.
        let auth = bare_url.password().map(|password| {
            prusto::auth::Auth::Basic(
                bare_url.username().to_owned(),
                Some(password.to_owned()),
            )
        });

        let mut builder = prusto::ClientBuilder::new(bare_url.username(), host)
            .port(bare_url.port().unwrap_or(8080))
            .catalog(catalog)
            .schema(schema);
        if let Some(auth) = auth {
            // Basic auth requires a secure connection. If we _don't_ have
            // credentials, then there's probably no point in encrypting the
            // connection, either.
            builder = builder.secure(true).auth(auth);
        }
        builder
            .build()
            .with_context(|| format!("could not connect to {}", self.url))
    }

    /// Get the connector type for this locator.
    #[instrument(level = "debug", name = "TrinoLocator::connector_type", skip_all)]
    pub(crate) async fn connector_type(
        &self,
        client: &prusto::Client,
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

        #[derive(Debug, Presto)]
        struct ConnectorName {
            connector_name: String,
        }
        let rows = client.get_all::<ConnectorName>(sql).await?;
        let row = rows.as_slice().first().ok_or_else(|| {
            format_err!("no connector found for catalog {} in {}", catalog, self)
        })?;
        TrinoConnectorType::from_str(&row.connector_name)
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
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        todo!("TrinoLocator::count")
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
}
