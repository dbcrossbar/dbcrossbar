# CSV

`dbcrossbar` works with valid CSV files in our [CSV interchange format](./csv_interchange.html). For invalid CSV files, take a look at [`scrubcsv`](https://github.com/faradayio/scrubcsv). For CSV files which need further transformation and parsing, considering loading them into BigQuery and cleaning them up using SQL. This works very well even for large datasets.

## Example locators

The following locators can be used for both input and output:

- `csv:file.csv`: A single CSV file.
- `csv:dir/`: A directory tree containing CSV files.
- `csv:-`: Read from standard input, or write to standard output.

To concatenate CSV files, use:

```sh
dbcrossbar cp csv:input/ csv:merged.csv
```

To split a CSV file, use `--stream-size`:

```sh
dbcrossbar cp --stream-size="100Mb" csv:giant.csv csv:split/
```

## Configuration & authentication

None.

## Supported features

```txt
{{#include generated/features_csv.txt}}
```
