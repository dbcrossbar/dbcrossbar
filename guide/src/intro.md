# What is `dbcrossbar`?

`dbcrossbar` is an [open source][] tool that copies large, tabular datasets between many different databases and storage formats. Data can be copied from any source to any destination.

[open source]: https://github.com/dbcrossbar/dbcrossbar

```dot process
digraph {
    rankdir="LR";

    csv_in [label="CSV"]
    csv_out [label="CSV"]
    csv_in -> dbcrossbar -> csv_out

    postgres_in [label="PostgreSQL"]
    postgres_out [label="PostgreSQL"]
    postgres_in -> dbcrossbar -> postgres_out

    bigquery_in [label="BigQuery"]
    bigquery_out [label="BigQuery"]
    bigquery_in -> dbcrossbar -> bigquery_out

    s3_in [label="S3"]
    s3_out [label="S3"]
    s3_in -> dbcrossbar -> s3_out

    gs_in [label="Cloud Storage"]
    gs_out [label="Cloud Storage"]
    gs_in -> dbcrossbar -> gs_out

    redshift_in [label="RedShift"]
    redshift_out [label="RedShift"]
    redshift_in -> dbcrossbar -> redshift_out

    etc_in [label="..."]
    etc_out [label="..."]
    etc_in -> dbcrossbar -> etc_out
}
```

## An example

If we have a CSV file `my_table.csv` containing data:

```csv
{{#include examples/my_table.csv}}
```

And a file `my_table.sql` containing a table definition:

```sql
{{#include examples/my_table.sql}}
```

Then we can use these to create a PostgreSQL table:

```sh
{{#include examples/my_table_cp_to_postgres.sh}}
```

If we want to use the data to update a table in BigQuery, we can upsert into BigQuery using the `id` column:

```sh
{{#include examples/my_table_cp_to_bigquery.sh}}
```

Notice that we don't need to specify `--schema`, because `dbcrossbar` will automatically translate the PostgreSQL column types to corresponding BigQuery types.
