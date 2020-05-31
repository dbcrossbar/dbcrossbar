# Features & philosophy

`dbcrossbar` is designed to do a few things well. Typically, `dbcrossbar` is used for loading raw data, and for moving data back and forth between production databases and data warehouses. It supports a few core features:

1. Copying tables.
2. Counting records in tables.
3. Converting between different table schema formats, including PostgreSQL `CREATE TABLE` statements and BigQuery schema JSON.

`dbcrossbar` offers a number of handy features:

- A single static binary on Linux, with no dependencies.
- A stream-based architecture that limits the use of RAM and requires no temporary files.
- Support for appending, overwriting or upserting into existing tables.
- Support for selecting records using `--where`.

`dbcrossbar` also supports a rich variety of portable column types:

- Common types, including booleans, dates, timestamps, floats, integers, and text.
- UUIDs.
- JSON.
- GeoJSON.
- Arrays.

## Non-features

The following features are explicitly excluded from `dbcrossbar`'s mission:

- Data cleaning and transformation.
- Fixing invalid column names.
- Copying multiple tables at time.
- Automatically copying constraints, foreign keys, etc.

If you need these features, then take a look at tools like [`scrubcsv`](https://github.com/faradayio/scrubcsv) and [`pgloader`](https://pgloader.io/).
