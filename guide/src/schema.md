# Portable table schema

Internally, `dbcrossbar` uses a portable table "schema" format. This provides a common ground between PostgreSQL's `CREATE TABLE` statements, [BigQuery's JSON schemas][bigquery], and equivalent formats for other databases. For more information, see:

- [The `dbcrossbar` schema format][schema].
- [The `dbcrossbar` column types][types].

All table schemas and column types are converted into the portable format and then into the appropriate destination format.

Normally, you won't need to work with this schema format directly, because `dbcrossbar` can parse BigQuery schemas, PostgreSQL `CREATE TABLE` statments, and several other popular schema formats. It can also read schemas directly from some databases. See the [`conv` command](./conv.html) for details.

## Example schema

```json
{{#include ../../dbcrossbar/fixtures/dbcrossbar_schema.json}}
```

## Table properties

- `name`: The name of this table. This is normally only used when serializing to schema formats that require a table name.
- `columns`: A list of columns in the table.

## Column properties

- `name`: The name of the column.
- `is_nullable`: Can the column contain `NULL` values?
- `data_type`: The type of data stored in the column.

## Data types

The `data_type` field can contain any of:

- `{ "array": element_type }`: An array of `element_type` values.
- `"bool"`: A boolean value.
- `"date"`: A date, with no associated time value.
- `"decimal"`: A decimal integer (can represent currency, etc., without rounding errors).
- `"float32"`: A 32-bit floating point number.
- `"float64"`: A 64-bit floating point number.
- `{ "geojson": srid }`: Geodata in GeoJSON format, using the specified [SRID][], to specify the spatial reference system.
- `"int16"`: A 16-bit signed integer.
- `"int32"`: A 32-bit signed integer.
- `"int64"`: A 64-bit signed integer.
- `"json"`: An arbitrary JSON value.
- `{ "struct": fields }`: A structure with a list of specific, named fields. Each field has the following properties:
  - `name`: The name of the field.
  - `is_nullable`: Can the field contain `NULL` values?
  - `data_type`: The type of data stored in the field.
- `"text"`: A string.
- `"timestamp_without_time_zone"`: A date and time without an associated timezone.
- `"timestamp_with_time_zone"`: A date and time with an associated timezone.
- `"uuid"`: A UUID value.

[bigquery]: https://cloud.google.com/bigquery/docs/schemas
[schema]: https://docs.rs/dbcrossbarlib/latest/dbcrossbarlib/schema/index.html
[types]: https://docs.rs/dbcrossbarlib/latest/dbcrossbarlib/schema/enum.DataType.html
[SRID]: https://en.wikipedia.org/wiki/Spatial_reference_system
