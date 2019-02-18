//! Support for writing local data to Postgres.

use std::{io::prelude::*, str};

use super::{connect, sql_schema};
use crate::common::*;
use crate::schema::DataType;
use crate::tokio_glue::SyncStreamReader;

// Copy a table into Postgres from a CSV stream.
pub(crate) async fn copy_in_table(
    ctx: Context,
    url: Url,
    schema: Table,
    mut data: BoxStream<CsvStream>,
    if_exists: IfExists,
) -> Result<BoxStream<BoxFuture<()>>> {
    let ctx = ctx.child(o!("table" => schema.name.clone()));

    // Generate `CREATE TABLE` SQL.
    let mut table_sql_buff = vec![];
    sql_schema::write_create_table(&mut table_sql_buff, &schema, if_exists)?;
    let table_sql =
        str::from_utf8(&table_sql_buff).expect("generated SQL should always be UTF-8");

    // Generate `COPY FROM` SQL.
    let mut copy_sql_buff = vec![];
    writeln!(&mut copy_sql_buff, "COPY {:?} (", schema.name)?;
    for (idx, col) in schema.columns.iter().enumerate() {
        if let DataType::Array(_) = col.data_type {
            return Err(format_err!("cannot yet import array column {:?}", col.name));
        }
        if idx + 1 == schema.columns.len() {
            writeln!(&mut copy_sql_buff, "    {:?}", col.name)?;
        } else {
            writeln!(&mut copy_sql_buff, "    {:?},", col.name)?;
        }
    }
    writeln!(&mut copy_sql_buff, ") FROM STDIN WITH CSV HEADER")?;
    let copy_sql = str::from_utf8(&copy_sql_buff)
        .expect("generated SQL should always be UTF-8")
        .to_owned();

    // Connect to PostgreSQL.
    let conn = connect(&url)?;

    // Drop the existing table (if any) if we're overwriting it.
    if if_exists == IfExists::Overwrite {
        debug!(ctx.log(), "deleting destination table if exists");
        let drop_sql = format!("DROP TABLE IF EXISTS {:?}", schema.name);
        conn.execute(&drop_sql, &[])
            .with_context(|_| format!("error deleting existing {}", schema.name))?;
    }

    // Create our table.
    debug!(ctx.log(), "creating destination table");
    conn.execute(table_sql, &[])
        .with_context(|_| format!("error creating table {}", schema.name))?;
    drop(conn);

    // Insert data streams one at a time, because parallel insertion
    // _probably_ won't gain much with Postgres (but we haven't measured).
    //
    // TODO: This blocks for an extended period of time on the main thread
    // pool.  We should reconsider that.
    let fut = async move {
        loop {
            match await!(data.into_future()) {
                Err((err, _rest_of_stream)) => {
                    debug!(ctx.log(), "error reading stream of streams: {}", err);
                    return Err(err);
                }
                Ok((None, _rest_of_stream)) => {
                    return Ok(());
                }
                Ok((Some(csv_stream), rest_of_stream)) => {
                    data = rest_of_stream;
                    let ctx = ctx.child(o!("stream" => csv_stream.name.clone()));
                    debug!(ctx.log(), "copying data into table");
                    let conn = connect(&url)?;
                    let stmt = conn.prepare(&copy_sql)?;
                    stmt.copy_in(
                        &[],
                        &mut SyncStreamReader::new(ctx.clone(), csv_stream.data),
                    )
                    .with_context(|_| {
                        format!("error copying data into {}", schema.name)
                    })?;
                }
            }
        }
    };
    Ok(box_stream_once(Ok(fut.into_boxed())))
}
