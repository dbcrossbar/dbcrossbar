# `schemaconv`: Tools for converting between database table schemas (WIP)

This tool is intended to help convert between schema formats. It's still very incomplete. Right now, `schemaconv` is most useful for moving data from PostgreSQL to Google's BigQuery.

Installation:

```sh
# Install Rust compiler.
curl https://sh.rustup.rs -sSf | sh

# Install schemaconv.
cargo install -f --git https://github.com/faradayio/schemaconv schemaconv
```

Examples:

```sh
# Given a `postgres:` URL, dump a table schema as JSON.
schemaconv "$DATABASE_URL#mytable" > schema.json

# Dump a table schema as BigQuery schema JSON.
schemaconv "$DATABASE_URL#mytable" -O bq:schema > bigquery-schema.json

# Ditto, but using PostgreSQL `CREATE TABLE` SQL as input.
schemaconv -I pg -O bq:schema < table.sql > bigquery-schema.json

# Dump a table schema as quoted PostgreSQL `SELECT ...` arguments.
schemaconv "$DATABASE_URL#mytable" -O pg:select > select-args.txt
```

You can also edit the default schema JSON (generated with no `-O` flag, or with `-O json`), and then run it back through to generate another format:

```sh
schemaconv "$DATABASE_URL#mytable" > schema.json
# (Edit schema.json.)

schemaconv -O bq < schema.json > bigquery-schema.json
```

## Contributing

For more instructions about building `schemaconv`, running tests, and contributing code, see [CONTRIBUTING.md](./CONTRIBUTING.md).
