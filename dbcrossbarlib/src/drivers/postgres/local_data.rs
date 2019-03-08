//! Support for reading data from a PostgreSQL table.

use std::thread;

use super::connect;
use crate::common::*;
use crate::drivers::postgres_shared::PgCreateTable;
use crate::tokio_glue::SyncStreamWriter;

/// Copy the specified table from the database, returning a `CsvStream`.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    table_name: String,
    schema: Table,
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
    pg_create_table.write_export_sql(&mut sql_bytes)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", sql);

    // Use `pipe` and a background thread to convert a `Write` to `Read`.
    let url = url.clone();
    let (mut wtr, stream) = SyncStreamWriter::pipe(ctx.clone());
    let thr = thread::Builder::new().name(format!("postgres read: {}", table_name));
    thr.spawn(move || {
        // Run our code in a `try` block so we can capture errors returned by
        // `?` without needing to give up ownership of `wtr` to a local closure.
        let result: Result<()> = try {
            let conn = connect(&url)?;
            let stmt = conn.prepare(&sql)?;
            stmt.copy_out(&[], &mut wtr)?;
        };

        // Report any errors to our stream.
        if let Err(err) = result {
            error!(ctx.log(), "error reading from PostgreSQL: {}", err);
            if wtr.send_error(err).is_err() {
                error!(ctx.log(), "cannot report error to foreground thread");
            }
        }
    })
    .context("could not spawn thread")?;

    let csv_stream = CsvStream {
        name: table_name.clone(),
        data: Box::new(stream),
    };
    let box_stream: BoxStream<CsvStream> = Box::new(stream::once(Ok(csv_stream)));
    Ok(Some(box_stream))
}
