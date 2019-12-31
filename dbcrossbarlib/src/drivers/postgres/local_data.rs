//! Support for reading data from a PostgreSQL table.

use bytes::Bytes;
use failure::Fail;

use super::{connect, PostgresLocator};
use crate::common::*;
use crate::drivers::postgres_shared::{CheckCatalog, PgCreateTable};

/// Copy the specified table from the database, returning a `CsvStream`.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    table_name: String,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let shared_args = shared_args.verify(PostgresLocator::features())?;
    let source_args = source_args.verify(PostgresLocator::features())?;

    // Look up the arguments we'll need.
    let schema = shared_args.schema();

    // Set up our logger.
    let ctx =
        ctx.child(o!("stream" => table_name.clone(), "table" => table_name.clone()));
    debug!(ctx.log(), "reading data from {} table {}", url, table_name);

    // Try to look up our table schema in the database.
    let pg_create_table = PgCreateTable::from_pg_catalog_or_default(
        CheckCatalog::Yes,
        &url,
        &table_name,
        schema,
    )
    .await?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_create_table.write_export_sql(&mut sql_bytes, &source_args)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", sql);

    // Copy the data out of PostgreSQL as a CSV stream.
    let mut conn = connect(ctx.clone(), url).await?;
    let stmt = conn.prepare(&sql).compat().await?;
    let rdr = conn
        .copy_out(&stmt, &[])
        // Convert data representation to match `dbcrossbar` conventions.
        .map(move |bytes: Bytes| -> BytesMut {
            trace!(ctx.log(), "read {} bytes", bytes.len());
            bytes.into()
        })
        // Convert errors to our standard error type.
        .map_err(|err| err.context("error reading data from PostgreSQL").into());

    let csv_stream = CsvStream {
        name: table_name.clone(),
        data: Box::new(rdr),
    };
    let box_stream: BoxStream<CsvStream> = Box::new(stream::once(Ok(csv_stream)));
    Ok(Some(box_stream))
}
