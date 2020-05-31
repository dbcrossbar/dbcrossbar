# Postgres `CREATE TABLE` statements

To specify the column names and types for table in SQL format, use:

```txt
--schema postgres-sql:my_table.sql
```

The file `my_table.sql` can contain a single `CREATE TABLE` statement using a subset of PostgreSQL's syntax:

```sql
{{#include examples/my_table.sql}}
```

## Limitations

This schema format offers support for singly-nested array types, and it doesn't support structure types at all.
