//! Support for writing local data to Postgres.

use futures::pin_mut;
use itertools::Itertools;
use std::{collections::HashSet, io::prelude::*, str};

use super::{csv_to_binary::copy_csv_to_pg_binary, Client, PostgresLocator};
use crate::drivers::postgres_shared::{
    connect, CheckCatalog, Ident, PgCreateTable, PgSchema,
};
use crate::tokio_glue::try_forward;
use crate::transform::spawn_sync_transform;
use crate::{common::*, drivers::postgres_shared::PgCreateType};

/// If `table_name` exists, `DROP` it.
async fn drop_table_if_exists(
    ctx: &Context,
    client: &mut Client,
    table: &PgCreateTable,
) -> Result<()> {
    debug!(
        ctx.log(),
        "deleting table {} if exists",
        table.name.quoted(),
    );
    let drop_sql = format!("DROP TABLE IF EXISTS {}", &table.name.quoted());
    let drop_stmt = client.prepare(&drop_sql).await?;
    client.execute(&drop_stmt, &[]).await.with_context(|_| {
        format!("error deleting existing {}", table.name.quoted())
    })?;
    Ok(())
}

/// Create any types that we will need.
async fn prepare_types(
    ctx: &Context,
    client: &mut Client,
    schema: &PgSchema,
) -> Result<()> {
    let needed_types = schema.table()?.named_type_names();
    for ty in &schema.types {
        if needed_types.contains(&ty.name) {
            let existing = PgCreateType::from_database(ctx, client, &ty.name).await?;
            match existing {
                None => {
                    // The type doesn't exist, so create it.
                    let create_sql = format!("{}", ty);
                    debug!(ctx.log(), "creating type: {}", create_sql);
                    let create_stmt = client.prepare(&create_sql).await?;
                    client.execute(&create_stmt, &[]).await?;
                }
                Some(_) => {
                    // If we were feeling inspired, we could check to make sure
                    // that `ty` is a non-strict subset of `existing`, but for
                    // now, we'll assume the destination type is good enough,
                    // let PostgreSQL print the errors.
                    debug!(
                        ctx.log(),
                        "assuming existing {} type in destination is compatible",
                        ty.name.quoted()
                    );
                }
            }
        }
    }
    Ok(())
}

/// Run the specified `CREATE TABLE` SQL.
async fn create_table(
    ctx: &Context,
    client: &mut Client,
    schema: &PgSchema,
) -> Result<()> {
    prepare_types(ctx, client, schema).await?;
    let table = schema.table()?;
    debug!(ctx.log(), "create table {}", table.name.quoted());
    let create_sql = format!("{}", table);
    debug!(ctx.log(), "CREATE TABLE SQL: {}", create_sql);
    let create_stmt = client.prepare(&create_sql).await?;
    client
        .execute(&create_stmt, &[])
        .await
        .with_context(|_| format!("error creating {}", &table.name.quoted()))?;
    Ok(())
}

/// Create a temporary table based on `table`, but using a different name. This
/// table will only live as long as the `client`.
pub(crate) async fn create_temp_table_for(
    ctx: &Context,
    client: &mut Client,
    schema: &PgSchema,
) -> Result<PgCreateTable> {
    let table = schema.table()?;
    let mut temp_table = table.to_owned();
    let temp_name = table.name.temporary_table_name()?;
    temp_table.name = temp_name;
    temp_table.if_not_exists = false;
    temp_table.temporary = true;
    let temp_schema = PgSchema {
        tables: vec![temp_table],
        ..schema.to_owned()
    };
    create_table(ctx, client, &temp_schema).await?;
    Ok(temp_schema.table()?.to_owned())
}

/// Run `DROP TABLE` and/or `CREATE TABLE` as needed to prepare `table` for
/// copying in data.
///
/// We take ownership of `pg_create_table` because we want to edit it before
/// running it.
pub(crate) async fn prepare_table(
    ctx: &Context,
    client: &mut Client,
    mut schema: PgSchema,
    if_exists: &IfExists,
) -> Result<()> {
    let table = schema.table_mut()?;
    match if_exists {
        IfExists::Overwrite => {
            drop_table_if_exists(ctx, client, table).await?;
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
    create_table(ctx, client, &schema).await
}

/// Generate the `COPY ... FROM ...` SQL we'll pass to `copy_in`. `data_format`
/// should be something like `"CSV HRADER"` or `"BINARY"`.
///
/// We have a separate function for generating this because we'll use it for
/// multiple `COPY` statements.
fn copy_from_sql(table: &PgCreateTable, data_format: &str) -> Result<String> {
    let mut copy_sql_buff = vec![];
    writeln!(&mut copy_sql_buff, "COPY {} (", table.name.quoted())?;
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
    let copy_from_sql = copy_from_sql(dest, "BINARY")?;
    let stmt = client.prepare(&copy_from_sql).await?;
    let sink = client
        .copy_in::<_, BytesMut>(&stmt)
        .await
        .with_context(|_| format!("error copying data into {}", dest.name.quoted()))?;

    // `CopyInSink` is a weird sink, and we have to "pin" it directly into our
    // stack in order to forward data to it.
    pin_mut!(sink);
    try_forward(ctx, stream, sink).await?;
    Ok(())
}

/// Given a table and list of upsert columns, return a list
pub(crate) fn columns_to_update_for_upsert<'a>(
    dest_table: &'a PgCreateTable,
    upsert_keys: &[String],
) -> Result<Vec<&'a str>> {
    // Build a set of our upsert keys. We could probably implement this linear
    // search with no significant loss of performance.
    let upsert_keys_set = upsert_keys
        .iter()
        .map(|k| &k[..])
        .collect::<HashSet<&str>>();

    // Build our list of columns to update.
    let mut update_cols = vec![];
    for c in &dest_table.columns {
        if upsert_keys_set.contains(&c.name[..]) {
            // Verify that it's actually safe to use this as an upsert key.
            if c.is_nullable {
                return Err(format_err!(
                    "cannot upsert on column {} because it isn't declared NOT NULL",
                    Ident(&c.name),
                ));
            }
        } else {
            update_cols.push(&c.name[..]);
        }
    }
    Ok(update_cols)
}

/// Generate SQL to perform an UPSERT from `src_table_name` into `dest_table`
/// using `upsert_keys`.
fn upsert_sql(
    src_table: &PgCreateTable,
    dest_table: &PgCreateTable,
    upsert_keys: &[String],
) -> Result<String> {
    // Figure out which of our columns are "value" (non-key) columns.
    let value_keys = columns_to_update_for_upsert(dest_table, upsert_keys)?;

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
        dest_table = dest_table.name.quoted(),
        src_table = src_table.name.quoted(),
        all_columns = dest_table.columns.iter().map(|c| Ident(&c.name)).join(", "),
        key_columns = upsert_keys.iter().map(|k| Ident(k)).join(", "),
        value_updates = value_keys
            .iter()
            .map(|vk| format!("{name} = EXCLUDED.{name}", name = Ident(vk)))
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
        "upserting from {} to {} with {}",
        src_table.name.quoted(),
        dest_table.name.quoted(),
        sql,
    );
    let stmt = client.prepare(&sql).await?;
    client.execute(&stmt, &[]).await.with_context(|_| {
        format!(
            "error upserting from {} to {}",
            src_table.name.quoted(),
            dest_table.name.quoted(),
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
    let ctx = ctx.child(o!("table" => table_name.unquoted()));
    debug!(
        ctx.log(),
        "writing data streams to {} table {}",
        url,
        table_name.quoted(),
    );

    // Try to look up our destination table schema in the database.
    let dest_schema = PgSchema::from_pg_catalog_or_default(
        &ctx,
        CheckCatalog::from(&if_exists),
        dest.url(),
        dest.table_name(),
        schema,
    )
    .await?;

    // Connect to PostgreSQL and prepare our destination table.
    let mut client = connect(&ctx, &url).await?;
    prepare_table(&ctx, &mut client, dest_schema.clone(), &if_exists).await?;

    // Insert data streams one at a time, because parallel insertion _probably_
    // won't gain much with Postgres (but we haven't measured).
    let fut = async move {
        while let Some(result) = data.next().await {
            match result {
                Err(err) => {
                    debug!(ctx.log(), "error reading stream of streams: {}", err);
                    return Err(err);
                }
                Ok(csv_stream) => {
                    let ctx = ctx.child(o!("stream" => csv_stream.name.clone()));

                    // Convert our CSV stream into a PostgreSQL `BINARY` stream.
                    let transform_schema = dest_schema.clone();
                    let binary_stream = spawn_sync_transform(
                        ctx.clone(),
                        "copy_csv_to_pg_binary".to_owned(),
                        csv_stream.data,
                        move |_ctx, rdr, wtr| {
                            copy_csv_to_pg_binary(&transform_schema, rdr, wtr)
                        },
                    )?;

                    // Decide whether to do an upsert or regular insert.
                    if let IfExists::Upsert(cols) = &if_exists {
                        // Create temp table.
                        let temp_table =
                            create_temp_table_for(&ctx, &mut client, &dest_schema)
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
                            dest_schema.table()?,
                            cols,
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
                            dest_schema.table()?,
                            binary_stream,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(dest.boxed())
    };
    Ok(box_stream_once(Ok(fut.boxed())))
}
