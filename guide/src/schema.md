# Portable table schema

Internally, `dbcrossbar` uses a portable table "schema" format. This provides a common ground between PostgreSQL's `CREATE TABLE` statements, [BigQuery's JSON schemas][bigquery], and equivalent formats for other databases. Right now, this is only documented in the library documentation:

- [The `dbcrossbar` schema format][schema].
- [The `dbcrossbar` column types][types].

All table schemas and column types are converted into the portable format and then into the appropriate destination format.

[bigquery]: https://cloud.google.com/bigquery/docs/schemas
[schema]: https://docs.rs/dbcrossbarlib/latest/dbcrossbarlib/schema/index.html
[types]: https://docs.rs/dbcrossbarlib/latest/dbcrossbarlib/schema/enum.DataType.html
