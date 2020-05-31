# Schema drivers

`dbcrossbar` allows you to specify a table's column names and types in a number of different ways. You can use [Postgres `CREATE TABLE` statements](./postgres-sql.html), or [BigQuery schema JSON](./bigquery-schema.html), or [`dbcrossbar`'s internal schema format](./dbcrossbar-schema.html).

These schema formats are typically used in one of two ways:

- As a `--schema` argument to the [`cp` subcommand](./cp.html).

  ```sh
  {{#include examples/my_table_cp_to_postgres.sh}}
  ```

- As an argument to the [`conv` subcommand](./conv.html), which allows you to convert between different schema formats.

  ```sh
  dbcrossbar conv postgres-sql:table.sql bigquery-schema:table.json
  ```
