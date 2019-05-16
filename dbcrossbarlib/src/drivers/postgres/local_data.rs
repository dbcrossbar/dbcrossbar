//! Support for reading data from a PostgreSQL table.

use bytes::Bytes;
use failure::Fail;

use super::connect;
use crate::common::*;
use crate::drivers::postgres_shared::PgCreateTable;

/// Copy the specified table from the database, returning a `CsvStream`.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    table_name: String,
    schema: Table,
    query: Query,
) -> Result<Option<BoxStream<CsvStream>>> {
    // Set up our logger.
    let ctx =
        ctx.child(o!("stream" => table_name.clone(), "table" => table_name.clone()));
    debug!(ctx.log(), "reading data from {} table {}", url, table_name);

    // Convert our schema to a native PostgreSQL schema.
    let pg_create_table =
        PgCreateTable::from_name_and_columns(table_name.clone(), &schema.columns)?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_create_table.write_export_sql(&mut sql_bytes, &query)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", sql);

    // Copy the data out of PostgreSQL as a CSV stream.
    let mut conn = await!(connect(ctx.clone(), url))?;
    let stmt = await!(conn.prepare(&sql))?;
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
