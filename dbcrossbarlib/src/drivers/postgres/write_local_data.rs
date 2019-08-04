//! Support for writing local data to Postgres.

use std::{io::prelude::*, str};

use super::{connect, csv_to_binary::copy_csv_to_pg_binary, Client, PostgresLocator};
use crate::common::*;
use crate::drivers::postgres_shared::{Ident, PgCreateTable};
use crate::transform::spawn_sync_transform;

/// If `table_name` exists, `DROP` it.
async fn drop_table_if_exists(
    ctx: Context,
    client: &mut Client,
    table_name: String,
) -> Result<()> {
    debug!(ctx.log(), "deleting table {} if exists", table_name);
    let drop_sql = format!("DROP TABLE IF EXISTS {}", Ident(&table_name));
    let drop_stmt = client.prepare(&drop_sql).compat().await?;
    client
        .execute(&drop_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error deleting existing {}", table_name))?;
    Ok(())
}

/// Run the specified `CREATE TABLE` SQL.
async fn create_table(
    ctx: Context,
    client: &mut Client,
    pg_create_table: PgCreateTable,
) -> Result<()> {
    debug!(ctx.log(), "create table {}", pg_create_table.name);
    let create_sql = format!("{}", pg_create_table);
    debug!(ctx.log(), "CREATE TABLE SQL: {}", create_sql);
    let create_stmt = client.prepare(&create_sql).compat().await?;
    client
        .execute(&create_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error creating {}", pg_create_table.name))?;
    Ok(())
}

/// Run `DROP TABLE` and/or `CREATE TABLE` as needed to prepare `table` for
/// copying in data.
///
/// We take ownership of `pg_create_table` because we want to edit it before
/// running it.
pub(crate) async fn prepare_table(
    ctx: Context,
    client: &mut Client,
    mut pg_create_table: PgCreateTable,
    if_exists: IfExists,
) -> Result<()> {
    match if_exists {
        IfExists::Overwrite => {
            drop_table_if_exists(ctx.clone(), client, pg_create_table.name.clone())
                .await?;
            pg_create_table.if_not_exists = false;
        }
        IfExists::Append => {
            // We create the table if it doesn't exist, but we're happy to use
            // whatever is already there. I hope the schema matches! (But we'll
            // provide a schema to `COPY dest (cols) FROM ...`, so that should
            // at least make sure we agree on column names and order.)
            pg_create_table.if_not_exists = true;
        }
        IfExists::Error => {
            // We always want to create the table, so omit `IF NOT EXISTS`. If
            // the table already exists, we will fail with an error.
            pg_create_table.if_not_exists = false;
        }
        IfExists::Upsert(_keys) => {
            return Err(format_err!("UPSERT is not yet implemented for PostgreSQL"));
        }
    }
    Ok(create_table(ctx, client, pg_create_table).await?)
}

/// Generate the `COPY ... FROM ...` SQL we'll pass to `copy_in`. `data_format`
/// should be something like `"CSV HRADER"` or `"BINARY"`.
///
/// We have a separate function for generating this because we'll use it for
/// multiple `COPY` statements.
fn copy_from_sql(
    pg_create_table: &PgCreateTable,
    data_format: &str,
) -> Result<String> {
    let mut copy_sql_buff = vec![];
    writeln!(&mut copy_sql_buff, "COPY {:?} (", pg_create_table.name)?;
    for (idx, col) in pg_create_table.columns.iter().enumerate() {
        if idx + 1 == pg_create_table.columns.len() {
            writeln!(&mut copy_sql_buff, "    {:?}", col.name)?;
        } else {
            writeln!(&mut copy_sql_buff, "    {:?},", col.name)?;
        }
    }
    writeln!(&mut copy_sql_buff, ") FROM STDIN WITH {}", data_format)?;
    let copy_sql = str::from_utf8(&copy_sql_buff)
        .expect("generated SQL should always be UTF-8")
        .to_owned();
    Ok(copy_sql)
}

/// Like `copy_from`, but safely callable from `async` code.
async fn copy_from_async(
    ctx: Context,
    url: Url,
    table_name: String,
    copy_from_sql: String,
    stream: Box<dyn Stream<Item = BytesMut, Error = Error> + Send + 'static>,
) -> Result<()> {
    let mut client = connect(ctx.clone(), url).await?;
    debug!(ctx.log(), "copying data into table");
    let stmt = client.prepare(&copy_from_sql).compat().await?;
    client
        .copy_in(&stmt, &[], stream)
        .compat()
        .await
        .with_context(|_| format!("error copying data into {}", table_name))?;
    Ok(())
}

// The actual implementation of `write_local_data`, in a separate function so we
// can use `async`.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    url: Url,
    table_name: String,
    mut data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<()>>> {
    let shared_args = shared_args.verify(PostgresLocator::features())?;
    let dest_args = dest_args.verify(PostgresLocator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let if_exists = dest_args.if_exists();

    let ctx = ctx.child(o!("table" => schema.name.clone()));
    debug!(
        ctx.log(),
        "writing data streams to {} table {}", url, table_name,
    );

    // Convert our `schema` to a `PgCreateTable`.
    let pg_create_table =
        PgCreateTable::from_name_and_columns(table_name.clone(), &schema.columns)?;

    // Connect to PostgreSQL and prepare our table.
    let mut client = connect(ctx.clone(), url.clone()).await?;
    prepare_table(
        ctx.clone(),
        &mut client,
        pg_create_table.clone(),
        if_exists.clone(),
    )
    .await?;
    drop(client);

    // Generate our `COPY ... FROM` SQL.
    let copy_sql = copy_from_sql(&pg_create_table, "BINARY")?;

    // Insert data streams one at a time, because parallel insertion _probably_
    // won't gain much with Postgres (but we haven't measured).
    let fut = async move {
        loop {
            match data.into_future().compat().await {
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

                    // Convert our CSV stream into a PostgreSQL `BINARY` stream.
                    let transform_table = pg_create_table.clone();
                    let binary_stream = spawn_sync_transform(
                        ctx.clone(),
                        "copy_csv_to_pg_binary".to_owned(),
                        csv_stream.data,
                        move |_ctx, rdr, wtr| {
                            copy_csv_to_pg_binary(&transform_table, rdr, wtr)
                        },
                    )?;

                    // Run our copy code in a background thread.
                    copy_from_async(
                        ctx,
                        url.clone(),
                        table_name.clone(),
                        copy_sql.clone(),
                        binary_stream,
                    )
                    .await?;
                }
            }
        }
    };
    Ok(box_stream_once(Ok(fut.boxed())))
}
