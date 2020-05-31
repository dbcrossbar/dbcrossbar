# BigQuery JSON schemas

To specify the column names and types for table in BigQuery JSON format, use:

```txt
--schema bigquery-schema:my_table.json
```

The file `my_table.json` should be a [BigQuery JSON schema file][bq]:

```json
{{#include examples/my_table.json}}
```

## Limitations

This schema format supports a small number of general types. For example, all integer types are represented as `INT64`, all floating-point types are represented as `FLOAT64`, and both JSON values and UUIDs are represented as `STRING`.

[bq]: https://cloud.google.com/bigquery/docs/schemas
