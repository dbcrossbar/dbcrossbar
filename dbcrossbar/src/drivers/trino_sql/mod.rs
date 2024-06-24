//! Schema-only driver for reading and writing Trino `CREATE TABLE` schema.

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;

use super::trino_shared::{TrinoCreateTable, TrinoIdent, TrinoTableName};

/// An SQL file containing a `CREATE TABLE` statement using Trino syntax.
#[derive(Clone, Debug)]
pub struct TrinoSqlLocator {
    path: PathOrStdio,
}

impl fmt::Display for TrinoSqlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for TrinoSqlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(TrinoSqlLocator { path })
    }
}

impl Locator for TrinoSqlLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
    }

    fn schema(
        &self,
        _ctx: Context,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        schema_helper(self.to_owned()).boxed()
    }

    fn write_schema(
        &self,
        _ctx: Context,
        schema: Schema,
        if_exists: IfExists,
        _source_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<()> {
        write_schema_helper(self.to_owned(), schema, if_exists).boxed()
    }
}

impl LocatorStatic for TrinoSqlLocator {
    fn scheme() -> &'static str {
        "trino-sql:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema | LocatorFeatures::WriteSchema,
            write_schema_if_exists: IfExistsFeatures::no_append(),
            source_args: EnumSet::empty(),
            dest_args: EnumSet::empty(),
            dest_if_exists: EnumSet::empty(),
            _placeholder: (),
        }
    }
}

/// Implementation of `schema`, but as a real `async` function.
#[instrument(level = "trace", name = "trino_sql::schema")]
async fn schema_helper(source: TrinoSqlLocator) -> Result<Option<Schema>> {
    let input = source
        .path
        .open_async()
        .await
        .with_context(|| format!("error opening {}", source.path))?;
    let sql = async_read_to_string(input)
        .await
        .with_context(|| format!("error reading {}", source.path))?;
    let create_table = TrinoCreateTable::parse(&source.path.to_string(), &sql)?;
    trace!(table = %create_table, "parsed CREATE TABLE");
    let schema = create_table.to_schema()?;
    Ok(Some(schema))
}

/// Implementation of `write_schema`, but as a real `async` function.
#[instrument(level = "trace", name = "trino_sql::write_schema")]
async fn write_schema_helper(
    dest: TrinoSqlLocator,
    schema: Schema,
    if_exists: IfExists,
) -> Result<()> {
    // TODO: We use the existing `table.name` here, but this might produce
    // odd results if the input table comes from BigQuery or another
    // database with a very different naming scheme.
    let table_name = sanitize_table_name(&schema.table.name)?;
    let create_table = TrinoCreateTable::from_schema_and_name(&schema, &table_name)?;
    let mut out = dest.path.create_async(if_exists).await?;
    buffer_sync_write_and_copy_to_async(&mut out, |buff| {
        write!(buff, "{}", create_table)
    })
    .await
    .with_context(|| format!("error writing {}", dest.path))?;
    out.flush().await?;
    Ok(())
}

/// Clean up a portable table name, which might contain almost any weirdness,
/// into a Trino-friendly table name. This is a bit _ad hoc_, and only gets used
/// when writing out and SQL file with a `CREATE TABLE` statement, which the
/// user will presumably examine and edit anyways.
fn sanitize_table_name(name: &str) -> Result<TrinoTableName> {
    // Convert "." to "_", because we don't want to create names like
    // `catalog.schema."public.table"` when importing from a database that
    // already uses dots for something else.
    let name = name.replace('.', "_");
    let table = TrinoIdent::new(&name)?;
    Ok(TrinoTableName::Table(table))
}
