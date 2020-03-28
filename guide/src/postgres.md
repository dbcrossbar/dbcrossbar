# PostgreSQL

[PostgreSQL](https://www.postgresql.org/) is an excellent general-purpose SQL database.

## Example locators

`dbcrossbar` supports standard PostgreSQL locators followed by `#table_name`:

- `postgres://postgres:$PASSWORD@127.0.0.1:5432/postgres#my_table`

Note that PostgreSQL sources will currently output all data as a single stream. This can be split into multiple streams using the `--stream-size` option if desired.

## Configuration & authentication

Authentication is currently handled using standard `postgres://user:pass@...` syntax, similar to `psql`. We may add alternative mechanisms at some point to avoid passing credentials on the command-line.

## Supported features

```txt
{{#include generated/features_postgres.txt}}
```
