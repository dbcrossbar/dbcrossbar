//! Implementation of `RedshiftLocator::write_remote_data`.

use itertools::Itertools;

use super::{RedshiftDriverArguments, RedshiftLocator};
use crate::common::*;
use crate::drivers::postgres_shared::{PgColumn, PgDataType, PgScalarDataType};
use crate::drivers::{
    postgres::{columns_to_update_for_upsert, create_temp_table_for, prepare_table},
    postgres_shared::{
        connect, pg_quote, CheckCatalog, Client, Ident, PgCreateTable, PgName,
        PgSchema,
    },
    s3::S3Locator,
};
use crate::schema::{Column, DataType};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
#[instrument(
    level = "debug",
    name = "redshift::write_remote_data",
    skip_all,
    fields(source = %source, dest = %dest)
)]
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    source: BoxLocator,
    dest: RedshiftLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Convert the source locator into the underlying `s3://` URL. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `S3Locator` type, and Rust doesn't make that especially easy.
    let source_url = source
        .as_any()
        .downcast_ref::<S3Locator>()
        .ok_or_else(|| format_err!("not a s3:// locator: {}", source))?
        .as_url()
        .to_owned();

    let shared_args = shared_args.verify(RedshiftLocator::features())?;
    let _source_args = source_args.verify(Features::empty())?;
    let dest_args = dest_args.verify(RedshiftLocator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let to_args = dest_args
        .driver_args()
        .deserialize::<RedshiftDriverArguments>()?;
    let if_exists = dest_args.if_exists().to_owned();

    // Try to look up our table schema in the database.
    schema.verify_redshift_can_import_from_csv()?;
    let table_name = dest.table_name();
    let mut pg_schema = PgSchema::from_pg_catalog_or_default(
        &ctx,
        CheckCatalog::from(&if_exists),
        dest.url(),
        table_name,
        schema,
    )
    .await?;

    // Fix any types that need special handling.
    pg_schema.use_redshift_specific_types()?;

    // Connect to Redshift and prepare our table.
    let mut client = connect(&ctx, dest.url()).await?;
    prepare_table(&mut client, pg_schema.clone(), &if_exists).await?;
    if let IfExists::Upsert(upsert_keys) = &if_exists {
        // Create a temporary table to hold our imported data.
        let temp_table = create_temp_table_for(&mut client, &pg_schema).await?;

        // Copy data into our temporary table.
        copy_in(&client, &source_url, &temp_table.name, &to_args).await?;

        // Build our upsert SQL.
        upsert_from_temp_table(
            &mut client,
            &temp_table,
            pg_schema.table()?,
            upsert_keys,
        )
        .await?;
    } else {
        copy_in(&client, &source_url, table_name, &to_args).await?;
    }

    Ok(vec![dest.boxed()])
}

/// Copy data from S3 into a RedShift table.
#[instrument(level = "trace", skip(client))]
async fn copy_in(
    client: &Client,
    source_s3_url: &Url,
    dest_table: &PgName,
    to_args: &RedshiftDriverArguments,
) -> Result<()> {
    debug!(
        "Copying into {} from {}",
        dest_table.unquoted(),
        source_s3_url.as_str(),
    );
    let copy_sql = format!(
        "{partner}COPY {dest} FROM {source}\n{credentials}FORMAT CSV\nIGNOREHEADER 1\nDATEFORMAT 'auto'\nTIMEFORMAT 'auto'",
        partner = to_args.partner_sql()?,
        dest = dest_table.quoted(),
        source = pg_quote(source_s3_url.as_str()), // `$1` doesn't work here.
        credentials = to_args.credentials_sql()?,
    );
    let copy_stmt = client.prepare(&copy_sql).await?;
    client.execute(&copy_stmt, &[]).await.with_context(|| {
        format!(
            "error copying to {} from {}",
            dest_table.quoted(),
            source_s3_url
        )
    })?;
    Ok(())
}

/// Upsert from `temp_table` into `dest_table`, using the columns `upsert_keys`.
#[instrument(
    level = "trace",
    skip(client, temp_table, dest_table),
    fields(temp_table.name = ?temp_table.name, dest_table.name = ?dest_table.name),
)]
async fn upsert_from_temp_table(
    client: &mut Client,
    temp_table: &PgCreateTable,
    dest_table: &PgCreateTable,
    upsert_keys: &[String],
) -> Result<()> {
    let transaction = client.transaction().await?;

    let upsert_sql = upsert_sql(temp_table, dest_table, upsert_keys)?;
    for (idx, sql) in upsert_sql.iter().enumerate() {
        debug!("upsert SQL ({}/{}): {}", idx + 1, upsert_sql.len(), sql,);
        transaction.execute(&sql[..], &[]).await.with_context(|| {
            format!(
                "error upserting into {} from {}",
                dest_table.name.quoted(),
                temp_table.name.quoted(),
            )
        })?;
    }

    debug!("commiting upsert");
    transaction.commit().await?;
    Ok(())
}

/// Generate the SQL needed to perform an upsert.
///
/// This will destructively modify and then delete `temp_table`.
fn upsert_sql(
    temp_table: &PgCreateTable,
    dest_table: &PgCreateTable,
    upsert_keys: &[String],
) -> Result<Vec<String>> {
    let value_cols = columns_to_update_for_upsert(dest_table, upsert_keys)?;
    let dest_table_name = dest_table.name.quoted();
    let temp_table_name = temp_table.name.quoted();
    let keys_match = upsert_keys
        .iter()
        .map(|k| {
            format!(
                "{dest_table}.{name} = {temp_table}.{name}",
                name = Ident(k),
                dest_table = dest_table_name,
                temp_table = temp_table_name,
            )
        })
        .join(" AND\n    ");
    Ok(vec![
        format!(
            r"-- Update matching rows in dest table using source table.
UPDATE {dest_table}
SET {value_updates}
FROM {temp_table}
WHERE {keys_match}",
            dest_table = dest_table_name,
            temp_table = temp_table_name,
            keys_match = keys_match,
            value_updates = value_cols
                .iter()
                .map(|k| format!(
                    "{name} = {temp_table}.{name}",
                    name = Ident(k),
                    temp_table = temp_table_name,
                ))
                .join(",\n    "),
        ),
        format!(
            r"-- Remove updated rows from temp table.
DELETE FROM {temp_table}
USING {dest_table}
WHERE {keys_match}",
            dest_table = dest_table_name,
            temp_table = temp_table_name,
            keys_match = keys_match,
        ),
        format!(
            r"-- Insert new rows into dest table.
INSERT INTO {dest_table} ({all_columns}) (
    SELECT {all_columns}
    FROM {temp_table}
)",
            dest_table = dest_table_name,
            temp_table = temp_table_name,
            all_columns = dest_table.columns.iter().map(|c| Ident(&c.name)).join(", "),
        ),
        format!(r"DROP TABLE {temp_table}", temp_table = temp_table_name),
    ])
}

/// Convert specific types to use their RedShift-specific versions instead.
trait UseRedshiftSpecificTypes {
    /// Convert this type to use its RedShift-specific version.
    fn use_redshift_specific_types(&mut self) -> Result<()>;
}

impl UseRedshiftSpecificTypes for PgSchema {
    fn use_redshift_specific_types(&mut self) -> Result<()> {
        for table in &mut self.tables {
            table.use_redshift_specific_types()?;
        }
        Ok(())
    }
}

impl UseRedshiftSpecificTypes for PgCreateTable {
    fn use_redshift_specific_types(&mut self) -> Result<()> {
        for col in &mut self.columns {
            col.use_redshift_specific_types()?;
        }
        Ok(())
    }
}

impl UseRedshiftSpecificTypes for PgColumn {
    fn use_redshift_specific_types(&mut self) -> Result<()> {
        self.data_type.use_redshift_specific_types()
    }
}

impl UseRedshiftSpecificTypes for PgDataType {
    fn use_redshift_specific_types(&mut self) -> Result<()> {
        match self {
            PgDataType::Array { .. } => Err(format_err!(
                "Redshift driver does not support data type {:?}",
                self
            )),
            PgDataType::Scalar(pg_scalar_data_type) => {
                pg_scalar_data_type.use_redshift_specific_types()
            }
        }
    }
}

impl UseRedshiftSpecificTypes for PgScalarDataType {
    fn use_redshift_specific_types(&mut self) -> Result<()> {
        // We only worry about (1) types we need to change, and (2) types that
        // could hide other types, all of which are unsupported, anyways.
        match self {
            // These may or may not be supported, but we don't need to transform
            // them.
            PgScalarDataType::Boolean
            | PgScalarDataType::Date
            | PgScalarDataType::Numeric
            | PgScalarDataType::Real
            | PgScalarDataType::DoublePrecision
            | PgScalarDataType::Geometry(_)
            | PgScalarDataType::Smallint
            | PgScalarDataType::Int
            | PgScalarDataType::Bigint
            | PgScalarDataType::Json
            | PgScalarDataType::Jsonb
            | PgScalarDataType::RedShiftVarcharMax
            | PgScalarDataType::TimestampWithoutTimeZone
            | PgScalarDataType::TimestampWithTimeZone
            | PgScalarDataType::Uuid => Ok(()),

            // This is the only type we need to transform.
            PgScalarDataType::Text => {
                *self = PgScalarDataType::RedShiftVarcharMax;
                Ok(())
            }

            // This isn't supported, but if it were, it might need special
            // handling.
            PgScalarDataType::Named(_) => Err(format_err!(
                "Redshift driver does not support data type {:?}",
                self
            )),
        }
    }
}

/// Extension trait for verifying Redshift compatibility.
trait VerifyRedshiftCanImportFromCsv {
    /// Can Redshift import the data described by this type from a CSV file?
    fn verify_redshift_can_import_from_csv(&self) -> Result<()>;
}

impl VerifyRedshiftCanImportFromCsv for Schema {
    fn verify_redshift_can_import_from_csv(&self) -> Result<()> {
        self.table.verify_redshift_can_import_from_csv()
    }
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
            .with_context(|| format!("cannot import column {:?}", self.name))?;
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
            | DataType::OneOf(_)
            | DataType::Text
            | DataType::TimestampWithoutTimeZone
            | DataType::TimestampWithTimeZone => Ok(()),
            DataType::Array(_)
            | DataType::Decimal
            | DataType::GeoJson(_)
            | DataType::Json
            | DataType::Named(_) // We could expand these, maybe.
            | DataType::Struct(_)
            | DataType::Uuid => Err(format_err!(
                "Redshift driver does not support data type {:?}",
                self
            )),
        }
    }
}
