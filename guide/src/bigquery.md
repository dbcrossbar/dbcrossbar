# BigQuery

Google's [BigQuery](https://cloud.google.com/bigquery/) is a extremely scalable data warehouse that supports rich SQL queries and petabytes of data. If you need to transform or analyze huge data sets, it's an excellent tool.

When loading data into BigQuery, or extracting it, we always go via Google Cloud Storage. This is considerably faster than the load and extract functionality supplied by tools like `bq`.

**COMPATIBILITY WARNING:** This driver currently relies on `gsutil` and `bq` for many tasks, but those tools are poorly-suited to the kind of automation we need. In particular, `gsutil` uses too much RAM, and `bq` sometimes print status messages on standard output instead of standard error. We plan to replace those tools with native Rust libraries at some point. This will change how the BigQuery driver handles authentication in a future version.

## Example locators

- `bigquery:$PROJECT:$DATASET.$TABLE`: A BigQuery table.

## Configuration & authentication

See [the Cloud Storage driver](./gs.html#configuration--authentication) for authentication details.

The following command-line options will usually need to be specified for both sources and destinations:

- `--temporary=gs://$GS_TEMP_BUCKET`: A Google Cloud Storage bucket to use for staging data in both directions.
- `--temporary=bigquery:$GCLOUD_PROJECT:temp_dataset`

## Supported features

```txt
{{#include generated/features_bigquery.txt}}
```
