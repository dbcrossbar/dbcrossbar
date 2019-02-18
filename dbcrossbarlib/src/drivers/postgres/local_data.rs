//! Support for reading data from a PostgreSQL table.

use std::{io::Write, thread};

use super::connect;
use crate::common::*;
use crate::schema::DataType;
use crate::tokio_glue::SyncStreamWriter;

/// Copy the specified table from the database, returning a `CsvStream`.
pub(crate) fn copy_out_table(
    ctx: Context,
    url: &Url,
    table: &Table,
) -> Result<CsvStream> {
    let ctx =
        ctx.child(o!("stream" => table.name.clone(), "table" => table.name.clone()));

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    write_select(&mut sql_bytes, &table)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");

    // Use `pipe` and a background thread to convert a `Write` to `Read`.
    let url = url.clone();
    let (mut wtr, stream) = SyncStreamWriter::pipe(ctx.clone());
    thread::spawn(move || {
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
    });

    Ok(CsvStream {
        name: table.name.clone(),
        data: Box::new(stream),
    })
}

/// Generate a complete `SELECT` statement which outputs the table as CSV,
/// in a format that can likely be imported by other database.
fn write_select(f: &mut dyn Write, table: &Table) -> Result<()> {
    write!(f, "COPY (SELECT ")?;
    write_select_args(f, table)?;
    write!(f, " FROM {:?}", table.name)?;
    write!(f, ") TO STDOUT WITH CSV HEADER")?;
    Ok(())
}

/// Write out a table's column names as `SELECT` arguments.
fn write_select_args(f: &mut dyn Write, table: &Table) -> Result<()> {
    let mut first: bool = true;
    for col in &table.columns {
        if first {
            first = false;
        } else {
            write!(f, ",")?;
        }
        match &col.data_type {
            DataType::Array(_) => {
                write!(f, "array_to_json({:?}) AS {:?}", col.name, col.name)?;
            }
            DataType::GeoJson => {
                // Always transform to SRID 4326.
                write!(
                    f,
                    "ST_AsGeoJSON(ST_Transform({:?}, 4326)) AS {:?}",
                    col.name, col.name,
                )?;
            }
            _ => write!(f, "{:?}", col.name)?,
        }
    }
    Ok(())
}
