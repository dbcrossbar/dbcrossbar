# `cp`: Copying tables

The `cp` command copies tabular data from a source location to a destination location. For example, we can copy a CSV file into PostgreSQL, replacing any existing table:

```sh
{{#include examples/my_table_cp_to_postgres.sh}}
```

Or we copy data from PostgreSQL and upsert it into a BigQuery table:

```sh
{{#include examples/my_table_cp_to_bigquery.sh}}
```

## Command-line help

```txt
{{#include generated/cp_help.txt}}
```

## Flags

Not all command-line options are supported by all drivers. See the chapter on each driver for details.

### `--where`

Specify a `WHERE` clause to include in the SQL query. This can be used to select a subset of the source rows.

### `--from-arg`

This can be used to specify driver-specific options for the source driver. See the chapter for that driver.

### `--if-exists=error`

If the destination location already contains data, exit with an error.

### `--if-exists=append`

If the destination location already contains data, append the new data.

### `--if-exists=overwrite`

If the destination location already contains data, replace it with the new data.

### `--if-exists=upset-on:COL1,..`

For every row in the new data:

- If a row with a matching `col1`, `col2`, etc., exists, use the new data to update that row.
- If no row matching `col1`, `col2`, etc., exists, then insert the new row.

The columns `col1`, `col2`, etc., must be marked as `NOT NULL`.

### `--schema`

By default, `dbcrossbar` will use the schema of the source table. But when this can't be inferred automatically, `--schema` can be used to specify a table schema:

- `--schema=postgres-sql:my_table.sql`: A PostgreSQL `CREATE TABLE` statement.
- `--schema=bigquery-schema:my_table.json`: A [BigQuery JSON schema][bigquery].
- `--schema=dbcrossbar-schema:my_table.json`: An [internal `dbcrossbar` schema][schema].

It's also possible to use a schema from an existing database table:

- `--schema=postgres://localhost:5432/db#table`
- `--schema=bigquery:project:dataset.table`

Note that it's possible to create a BigQuery table using a PostgreSQL schema, or vice versa. Internally, all schemes are first converted to the [internal schema format][schema].

[bigquery]: https://cloud.google.com/bigquery/docs/schemas
[schema]: ./schema.html

### `--temporary`

Specify temporary storage, which is required by certain drivers. Typical values include:

- `--temporary=s3://$S3_TEMP_BUCKET`
- `--temporary=gs://$GS_TEMP_BUCKET`
- `--temporary=bigquery:$GCLOUD_PROJECT:temp_dataset`

### `--to-arg`

This can be used to specify driver-specific options for the destination driver. See the chapter for that driver.
