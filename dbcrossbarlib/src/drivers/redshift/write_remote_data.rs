//! Implementation of `RedshiftLocator::write_remote_data`.

use super::{credentials_sql, RedshiftLocator};
use crate::common::*;
use crate::drivers::{
    postgres::{connect, prepare_table},
    postgres_shared::{pg_quote, Ident, PgCreateTable},
    s3::S3Locator,
};
use crate::schema::{Column, DataType};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    source: BoxLocator,
    dest: RedshiftLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<()> {
    // Convert the source locator into the underlying `s3://` URL. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `S3Locator` type, and Rust doesn't make that especially easy.
    let source_url = source
        .as_any()
        .downcast_ref::<S3Locator>()
        .ok_or_else(|| format_err!("not a s3:// locator: {}", source))?
        .as_url()
        .to_owned();
    let ctx = ctx.child(o!("source_url" => source_url.as_str().to_owned()));

    let shared_args = shared_args.verify(RedshiftLocator::features())?;
    let _source_args = source_args.verify(Features::empty())?;
    let dest_args = dest_args.verify(RedshiftLocator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let to_args = dest_args.driver_args();
    let if_exists = dest_args.if_exists().to_owned();

    // Convert our `schema` to a `PgCreateTable`.
    schema.verify_redshift_can_import_from_csv()?;
    let table_name = dest.table_name();
    let pg_create_table =
        PgCreateTable::from_name_and_columns(table_name.to_owned(), &schema.columns)?;

    // Connect to Redshift and prepare our table.
    let mut client = connect(ctx.clone(), dest.url().to_owned()).await?;
    prepare_table(
        ctx.clone(),
        &mut client,
        pg_create_table.clone(),
        if_exists.clone(),
    )
    .await?;

    // Ask RedShift to import from S3.
    let copy_sql = format!(
        "COPY {dest} FROM {source}\n{credentials}FORMAT CSV\nIGNOREHEADER 1",
        dest = Ident(table_name),
        source = pg_quote(source_url.as_str()), // `$1` doesn't work here.
        credentials = credentials_sql(to_args)?,
    );
    let copy_stmt = client.prepare(&copy_sql).compat().await?;
    client
        .execute(&copy_stmt, &[])
        .compat()
        .await
        .with_context(|_| {
            format!("error copying {} from {}", pg_create_table.name, source_url)
        })?;
    Ok(())
}

/// Extension trait for verifying Redshift compatibility.
trait VerifyRedshiftCanImportFromCsv {
    /// Can Redshift import the data described by this type from a CSV file?
    fn verify_redshift_can_import_from_csv(&self) -> Result<()>;
}

impl VerifyRedshiftCanImportFromCsv for Table {
    fn verify_redshift_can_import_from_csv(&self) -> Result<()> {
        for col in &self.columns {
            col.verify_redshift_can_import_from_csv()?;
        }
        Ok(())
    }
}

impl VerifyRedshiftCanImportFromCsv for Column {
    fn verify_redshift_can_import_from_csv(&self) -> Result<()> {
        self.data_type
            .verify_redshift_can_import_from_csv()
            .with_context(|_| format!("cannot import column {:?}", self.name))?;
        Ok(())
    }
}

impl VerifyRedshiftCanImportFromCsv for DataType {
    fn verify_redshift_can_import_from_csv(&self) -> Result<()> {
        match self {
            DataType::Bool
            | DataType::Date
            | DataType::Float32
            | DataType::Float64
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Text
            | DataType::TimestampWithoutTimeZone
            | DataType::TimestampWithTimeZone => Ok(()),
            DataType::Array(_)
            | DataType::Decimal
            | DataType::GeoJson(_)
            | DataType::Json
            | DataType::Other(_)
            | DataType::Uuid => Err(format_err!(
                "Redshift driver does not support data type {:?}",
                self
            )),
        }
    }
}
