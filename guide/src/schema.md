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

## Schema properties

- `named_data_types` (experimental): Named data types. These are used to declare custom column types. They're analogous to a Postgres `CREATE TYPE` statement, or a TypeScript `interface` or `type` statement. Two types with different names but the same layout are considered to be different types ([nominal typing][nominal]). Many database drivers place restrictions on these types for now, but we hope to relax those restrictions in the future.
- `tables`: A list of table definitions. For now, this must contain exactly one element.

[nominal]: https://en.wikipedia.org/wiki/Nominal_type_system

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
- `{ "geo_json": srid }`: Geodata in GeoJSON format, using the specified [SRID][], to specify the spatial reference system.
- `"int16"`: A 16-bit signed integer.
- `"int32"`: A 32-bit signed integer.
- `"int64"`: A 64-bit signed integer.
- `"json"`: An arbitrary JSON value.
- `{ "named": name }`: A named type from the `named_types` list in this schema.
- `{ "one_of": string_list }`: One of the specified string values. This may be represented a string enumeration by certain drivers, or as a "categorical" value in machine-learning systems (as opposed to a free-form textual value). **NOTE:** In many cases, it would be more efficient to have a separate table with an `(id, value)` entry for each enum value, and to refer to it using a foreign key. `one_of` is most useful when importing cleaned data, and when working with databases that support efficient string enumerations.
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
[srid]: https://en.wikipedia.org/wiki/Spatial_reference_system
