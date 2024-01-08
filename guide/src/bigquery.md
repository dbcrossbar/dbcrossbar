# BigQuery

Google's [BigQuery](https://cloud.google.com/bigquery/) is a extremely scalable data warehouse that supports rich SQL queries and petabytes of data. If you need to transform or analyze huge data sets, it's an excellent tool.

When loading data into BigQuery, or extracting it, we always go via Google Cloud Storage. This is considerably faster than the load and extract functionality supplied by tools like `bq`.

## Example locators

- `bigquery:$PROJECT:$DATASET.$TABLE`: A BigQuery table.
- `bigquery-test-fixture:$PROJECT:$DATASET.$TABLE`: If you only need a tiny, read-only "table" for testing purposes, you may want to try the `bigquery-test-fixture:` locator. It currently uses [`tables.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert) to pass a `table.view.query` with all the table data inlined into the `VIEW` SQL. This runs about 20 times faster than `bigquery:`, at the expense of not creating a regular table. Note that the implementation details of this method may change, if we discover a faster or better way to create a small, read-only table.

## Configuration & authentication

See [the Cloud Storage driver](./gs.html#configuration--authentication) for authentication details.

The following command-line options will usually need to be specified for both sources and destinations:

- `--temporary=gs://$GS_TEMP_BUCKET`: A Google Cloud Storage bucket to use for staging data in both directions.
- `--temporary=bigquery:$GCLOUD_PROJECT:temp_dataset`

You can also specify Google Cloud resource labels to apply to all BigQuery jobs. Labels are often used to track query costs.

- `--from-arg=job_labels[department]=marketing`
- `--to-arg=job_labels[project]=project1`

You can pick a project to run jobs in, for example for billing purposes.

- `--from-arg=job_project_id=my-gcp-billing-project`
- `--to-arg=job_project_id=my-gcp-billing-project`

## Supported features

```txt
{{#include generated/features_bigquery.txt}}
```
