//! Support for reading data from a PostgreSQL table.

use bytes::Bytes;

use super::PostgresLocator;
use crate::common::*;
use crate::drivers::postgres_shared::{connect, CheckCatalog, PgName, PgSchema};

/// Copy the specified table from the database, returning a `CsvStream`.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: UrlWithHiddenPassword,
    table_name: PgName,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let shared_args = shared_args.verify(PostgresLocator::features())?;
    let source_args = source_args.verify(PostgresLocator::features())?;

    // Look up the arguments we'll need.
    let schema = shared_args.schema();

    // Set up our logger.
    let ctx = ctx.child(
        o!("stream" => table_name.unquoted(), "table" => table_name.unquoted()),
    );
    debug!(
        ctx.log(),
        "reading data from {} table {}",
        url,
        table_name.quoted()
    );

    // Try to look up our table schema in the database.
    let pg_schema = PgSchema::from_pg_catalog_or_default(
        &ctx,
        CheckCatalog::Yes,
        &url,
        &table_name,
        schema,
    )
    .await?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_schema.write_export_sql(&mut sql_bytes, &source_args)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", sql);

    // Copy the data out of PostgreSQL as a CSV stream.
    let conn = connect(&ctx, &url).await?;
    let stmt = conn.prepare(&sql).await?;
    let rdr = conn
        .copy_out(&stmt)
        .await
        // See if the query itself fails.
        .map_err(|err| -> Error {
            Error::new(err).context("error querying PostgreSQL for data")
        })?
        // Convert data representation to match `dbcrossbar` conventions.
        .map_ok(move |bytes: Bytes| -> BytesMut {
            trace!(ctx.log(), "read {} bytes", bytes.len());
            bytes.as_ref().into()
        })
        // Convert errors to our standard error type.
        .map_err(|err| Error::new(err).context("error reading data from PostgreSQL"));

    let csv_stream = CsvStream {
        name: table_name.unquoted(),
        data: rdr.boxed(),
    };
    let box_stream = stream::once(async { Ok(csv_stream) }).boxed();
    Ok(Some(box_stream))
}
