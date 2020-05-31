# CSV interchange format

Internally, `dbcrossbar` converts all data into CSV streams. For many standard types, all input drivers are required to provide byte-for-byte identical CSV data:

```csv
{{#include ../../dbcrossbar/fixtures/exact_output.csv}}
```

For more complex types such as arrays, structs, JSON, and GeoJSON data, we embed JSON into the CSV file:

```csv
{{#include ../../dbcrossbar/fixtures/many_types.csv}}
```

## Tricks for preparing CSV data

If your input CSV files use an incompatible format, there are several things that might help. If your CSV files are invalid, non-standard, or full of junk, then you may be able to use [`scrubcsv`](https://github.com/faradayio/scrubcsv) or [`xsv`](https://github.com/BurntSushi/xsv) to fix the worst problems.

If you need to clean up your data manually, then you may want to consider using `dbcrossbar` to load your data into BigQuery, and set your columns to type `STRING`. Once this is done, you can parse and normalize your data quickly using SQL queries.
