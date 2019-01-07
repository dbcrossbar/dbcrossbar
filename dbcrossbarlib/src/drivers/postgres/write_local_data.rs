//! Support for writing local data to Postgres.

use failure::{format_err, ResultExt};
use std::{io::prelude::*, str};
use url::Url;

use super::{connect, sql_schema};
use crate::schema::{DataType, Table};
use crate::{CsvStream, IfExists, Result};

pub(crate) fn copy_in_table(
    url: &Url,
    schema: &Table,
    data: Vec<CsvStream>,
    if_exists: IfExists,
) -> Result<()> {
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
    let copy_sql =
        str::from_utf8(&copy_sql_buff).expect("generated SQL should always be UTF-8");

    // Connect to PostgreSQL.
    let conn = connect(url)?;

    // Drop the existing table (if any) if we're overwriting it.
    if if_exists == IfExists::Overwrite {
        let drop_sql = format!("DROP TABLE IF EXISTS {:?}", schema.name);
        conn.execute(&drop_sql, &[])
            .with_context(|_| format!("error deleting existing {}", schema.name))?;
    }

    // Create our table.
    conn.execute(table_sql, &[])
        .with_context(|_| format!("error creating table {}", schema.name))?;

    // Insert data streams one at a time, because parallel insertion
    // _probably_ won't gain much with Postgres (but we haven't measured).
    let stmt = conn.prepare(&copy_sql)?;
    for mut stream in data {
        stmt.copy_in(&[], &mut stream.data)
            .with_context(|_| format!("error copying data into {}", schema.name))?;
    }

    Ok(())
}
