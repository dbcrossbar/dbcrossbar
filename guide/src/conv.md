# `conv`: Transforming schemas

The `conv` command can be used to convert between different database schemas. To convert from a PostgreSQL `CREATE TABLE` statement to a BigQuery schema, run:

```sh
dbcrossbar conv postgres-sql:table.sql bigquery-schema:table.json
```

As a handy trick, you can also use a CSV source, which will generate a `CREATE TABLE` where all columns have the type `TEXT`:

```sh
dbcrossbar conv csv:data.csv postgres-sql:table.sql
```

This can then be edited to specify appropriate column types.

## Command-line help

```txt
{{#include generated/conv_help.txt}}
```
