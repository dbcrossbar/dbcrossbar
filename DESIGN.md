# Design notes

|      Source/Sink      | Schema | Data | Extra requirements  |         Written as          |
| --------------------- | ------ | ---- | ------------------- | --------------------------- |
| Postgres table        | Yes    | Yes  |                     | postgres://...#table_name   |
| Postgres CREATE TABLE | Yes    | No   |                     | postgresschema:file.sql     |
| BigQuery table        | Yes    | Yes  | `gs://` temp bucket | bigquery:proj:dataset.table |
| BigQuery JSON         | Yes    | No   |                     | bigqueryschema:schema.json  |
| Local CSV             | No*    | Yes  | (Schema)            | file.csv OR csv:file.csv    |
| Cloud bucket CSV      | No     | Yes  | Schema              | gs://...                    |
| SQLite table          | Yes    | Yes  |                     | sqlite:file.db#table_name   |
| SQLite CREATE TABLE   | Yes    | No   |                     | sqliteschema:file.sql       |

```sh
dbcrossbar cp --temp=gs://faraday-secret/temp/ \
    'postgres://...#private_data' \
    bigquery:root-123455:private_data.20181217
```

1. Check to see if Citus is available.
2. Dump from either master or Citus workers to in-memory CSV streams.
3. Pipe in memory CSV streams to `gsutil cp - gs://faraday-secret/temp/$PREFIX`
4. Run BigQuery import job to `root-123455:private_data.20181217_temp$RANDID`
5. (Optional) Run SQL to transform `root-123455:private_data.20181217_temp$RANDID` to `root-123455:private_data.20181217`
6. Delete `root-123455:private_data.20181217_temp1234` and `gs://faraday-secret/temp/$PREFIX`

```sh
dbcrossbar cp --schema=postgresschema:file.sql \
    csv:foo.csv \
    sqlite:foo.db
```

```sh
--select zip,state
--where "state = 'CA' AND zip IS NOT NULL"
```

```sh
--where "created_at > '2019-*-*'"
```
