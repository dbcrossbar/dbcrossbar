//! Support for writing local data to Postgres.

use itertools::Itertools;
use std::{collections::HashSet, io::prelude::*, iter::FromIterator, str};

use super::{connect, csv_to_binary::copy_csv_to_pg_binary, Client, PostgresLocator};
use crate::common::*;
use crate::drivers::postgres_shared::{Ident, PgCreateTable, TableName};
use crate::transform::spawn_sync_transform;

/// If `table_name` exists, `DROP` it.
async fn drop_table_if_exists(
    ctx: &Context,
    client: &mut Client,
    table: &PgCreateTable,
) -> Result<()> {
    debug!(ctx.log(), "deleting table {} if exists", table.name);
    let drop_sql = format!("DROP TABLE IF EXISTS {}", TableName(&table.name));
    let drop_stmt = client.prepare(&drop_sql).compat().await?;
    client
        .execute(&drop_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error deleting existing {}", table.name))?;
    Ok(())
}

/// Run the specified `CREATE TABLE` SQL.
async fn create_table(
    ctx: &Context,
    client: &mut Client,
    table: &PgCreateTable,
) -> Result<()> {
    debug!(ctx.log(), "create table {}", table.name);
    let create_sql = format!("{}", table);
    debug!(ctx.log(), "CREATE TABLE SQL: {}", create_sql);
    let create_stmt = client.prepare(&create_sql).compat().await?;
    client
        .execute(&create_stmt, &[])
        .compat()
        .await
        .with_context(|_| format!("error creating {}", &table.name))?;
    Ok(())
}

/// Create a temporary table based on `table`, but using a different name. This
/// table will only live as long as the `client`.
async fn create_temp_table_for(
    ctx: &Context,
    client: &mut Client,
    table: &PgCreateTable,
) -> Result<PgCreateTable> {
    let mut temp_table = table.to_owned();
    let temp_name = {
        // Temporary table names aren't allowed to include namespaces.
        let name = TableName(&table.name);
        let (_, base_name) = name.split()?;
        format!("{}_temp_{}", base_name, TemporaryStorage::random_tag())
    };
    temp_table.name = temp_name;
    temp_table.if_not_exists = false;
    temp_table.temporary = true;
    create_table(ctx, client, &temp_table).await?;
    Ok(temp_table)
}

/// Run `DROP TABLE` and/or `CREATE TABLE` as needed to prepare `table` for
/// copying in data.
///
/// We take ownership of `pg_create_table` because we want to edit it before
/// running it.
pub(crate) async fn prepare_table(
    ctx: &Context,
    client: &mut Client,
    mut table: PgCreateTable,
    if_exists: &IfExists,
) -> Result<()> {
    match if_exists {
        IfExists::Overwrite => {
            drop_table_if_exists(ctx, client, &table).await?;
            table.if_not_exists = false;
        }
        IfExists::Append => {
            // We create the table if it doesn't exist, but we're happy to use
            // whatever is already there. I hope the schema matches! (But we'll
            // provide a schema to `COPY dest (cols) FROM ...`, so that should
            // at least make sure we agree on column names and order.)
            table.if_not_exists = true;
        }
        IfExists::Error => {
            // We always want to create the table, so omit `IF NOT EXISTS`. If
            // the table already exists, we will fail with an error.
            table.if_not_exists = false;
        }
        IfExists::Upsert(_keys) => {
            // Here, we can only make our final destination table. Each incoming
            // data stream will create its own temp table and then upsert into
            // this.
            table.if_not_exists = true;
        }
    }
    create_table(ctx, client, &table).await
}

/// Generate the `COPY ... FROM ...` SQL we'll pass to `copy_in`. `data_format`
/// should be something like `"CSV HRADER"` or `"BINARY"`.
///
/// We have a separate function for generating this because we'll use it for
/// multiple `COPY` statements.
fn copy_from_sql(table: &PgCreateTable, data_format: &str) -> Result<String> {
    let mut copy_sql_buff = vec![];
    writeln!(&mut copy_sql_buff, "COPY {} (", TableName(&table.name),)?;
    for (idx, col) in table.columns.iter().enumerate() {
        if idx + 1 == table.columns.len() {
            writeln!(&mut copy_sql_buff, "    {}", Ident(&col.name))?;
        } else {
            writeln!(&mut copy_sql_buff, "    {},", Ident(&col.name))?;
        }
    }
    writeln!(&mut copy_sql_buff, ") FROM STDIN WITH {}", data_format)?;
    let copy_sql = str::from_utf8(&copy_sql_buff)
        .expect("generated SQL should always be UTF-8")
        .to_owned();
    Ok(copy_sql)
}

/// Given `stream` containing CSV data, plus a the URL and table_name for a
/// destination, as well as `"COPY FROM"` SQL, copy the data into the specified
/// destination.
async fn copy_from_stream<'a>(
    ctx: &'a Context,
    client: &'a mut Client,
    dest: &'a PgCreateTable,
    stream: BoxStream<BytesMut>,
) -> Result<()> {
    debug!(ctx.log(), "copying data into {:?}", dest.name);
    let copy_from_sql = copy_from_sql(&dest, "BINARY")?;
    let stmt = client.prepare(&copy_from_sql).compat().await?;
    client
        .copy_in(&stmt, &[], stream)
        .compat()
        .await
        .with_context(|_| format!("error copying data into {}", dest.name))?;
    Ok(())
}

/// Generate SQL to perform an UPSERT from `src_table_name` into `dest_table`
/// using `upsert_keys`.
fn upsert_sql(
    src_table: &PgCreateTable,
    dest_table: &PgCreateTable,
    upsert_keys: &[String],
) -> Result<String> {
    // Figure out which of our columns are "value" (non-key) columns.
    let upsert_keys_set: HashSet<&str> =
        HashSet::from_iter(upsert_keys.iter().map(|k| &k[..]));
    let value_keys = dest_table
        .columns
        .iter()
        .filter_map(|c| {
            if upsert_keys_set.contains(&c.name[..]) {
                None
            } else {
                Some(&c.name[..])
            }
        })
        .collect::<Vec<_>>();

    // TODO: Do we need to check for NULLable key columns which might
    // produce duplicate rows on upsert, like we do for BigQuery?

    Ok(format!(
        r#"
INSERT INTO {dest_table} ({all_columns}) (
    SELECT {all_columns} FROM {src_table}
)
ON CONFLICT ({key_columns})
DO UPDATE SET
    {value_updates}
"#,
        dest_table = Ident(&dest_table.name),
        src_table = Ident(&src_table.name),
        all_columns = dest_table.columns.iter().map(|c| Ident(&c.name)).join(", "),
        key_columns = upsert_keys.iter().map(|k| Ident(k)).join(", "),
        value_updates = value_keys
            .iter()
            .map(|vk| format!("{name} = EXCLUDED.{name}", name = vk))
            .join(",\n    "),
    ))
}

/// Upsert all rows from `src` into `dest`.
pub(crate) async fn upsert_from(
    ctx: &Context,
    client: &mut Client,
    src_table: &PgCreateTable,
    dest_table: &PgCreateTable,
    upsert_keys: &[String],
) -> Result<()> {
    let sql = upsert_sql(src_table, dest_table, upsert_keys)?;
    debug!(
        ctx.log(),
        "upserting from {} to {} with {}", src_table.name, dest_table.name, sql,
    );
    let stmt = client.prepare(&sql).compat().await?;
    client
        .execute(&stmt, &[])
        .compat()
        .await
        .with_context(|_| {
            format!(
                "error upserting from {} to {}",
                src_table.name, dest_table.name,
            )
        })?;
    Ok(())
}

/// The actual implementation of `write_local_data`, in a separate function so we
/// can use `async`.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: PostgresLocator,
    mut data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let shared_args = shared_args.verify(PostgresLocator::features())?;
    let dest_args = dest_args.verify(PostgresLocator::features())?;

    // Look up our arguments.
    let schema = shared_args.schema();
    let if_exists = dest_args.if_exists().to_owned();

    let url = dest.url.clone();
    let table_name = dest.table_name.clone();
    let ctx = ctx.child(o!("table" => table_name.clone()));
    debug!(
        ctx.log(),
        "writing data streams to {} table {}", url, table_name,
    );

    // Convert our `schema` to a `PgCreateTable`.
    let dest_table =
        PgCreateTable::from_name_and_columns(table_name.clone(), &schema.columns)?;

    // Connect to PostgreSQL and prepare our destination table.
    let mut client = connect(ctx.clone(), url.clone()).await?;
    prepare_table(&ctx, &mut client, dest_table.clone(), &if_exists).await?;

    // Insert data streams one at a time, because parallel insertion _probably_
    // won't gain much with Postgres (but we haven't measured).
    let fut = async move {
        loop {
            match data.into_future().compat().await {
                Err((err, _rest_of_stream)) => {
                    debug!(ctx.log(), "error reading stream of streams: {}", err);
                    return Err(err);
                }
                Ok((Some(csv_stream), rest_of_stream)) => {
                    data = rest_of_stream;

                    let ctx = ctx.child(o!("stream" => csv_stream.name.clone()));

                    // Convert our CSV stream into a PostgreSQL `BINARY` stream.
                    let transform_table = dest_table.clone();
                    let binary_stream = spawn_sync_transform(
                        ctx.clone(),
                        "copy_csv_to_pg_binary".to_owned(),
                        csv_stream.data,
                        move |_ctx, rdr, wtr| {
                            copy_csv_to_pg_binary(&transform_table, rdr, wtr)
                        },
                    )?;

                    // Decide whether to do an upsert or regular insert.
                    if let IfExists::Upsert(cols) = &if_exists {
                        // Create temp table.
                        let temp_table =
                            create_temp_table_for(&ctx, &mut client, &dest_table)
                                .await?;

                        // Copy into temp table.
                        copy_from_stream(
                            &ctx,
                            &mut client,
                            &temp_table,
                            binary_stream,
                        )
                        .await?;

                        // Upsert from temp table into dest.
                        upsert_from(
                            &ctx,
                            &mut client,
                            &temp_table,
                            &dest_table,
                            &cols,
                        )
                        .await?;

                        // Delete temp table (which always exists, but we can
                        // re-use this function).
                        drop_table_if_exists(&ctx, &mut client, &temp_table).await?;
                    } else {
                        // Copy directly into dest.
                        copy_from_stream(
                            &ctx,
                            &mut client,
                            &dest_table,
                            binary_stream,
                        )
                        .await?;
                    }
                }
                Ok((None, _rest_of_stream)) => {
                    return Ok(dest.boxed());
                }
            }
        }
    };
    Ok(box_stream_once(Ok(fut.boxed())))
}
