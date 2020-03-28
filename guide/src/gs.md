# Google Cloud Storage

Google Cloud Storage is a bucket-based storage system similar to Amazon's S3. It's frequently used in connection with BigQuery and other Google Cloud services.

**COMPATIBILITY WARNING:** This driver currently relies on `gsutil` for many tasks, but `gsutil` is poorly-suited to the kind of automation we need. In particular, `gsutil` uses too much RAM, and has poor timeout behavio. We plan to replace it with native Rust libraries at some point. This will change how the Cloud Storage driver handles authentication in a future version.

## Example locators

Source locators:

- `gs://bucket/dir/file.csv`
- `gs://bucket/dir/`

Destination locators:

- `gs://bucket/dir/`

At this point, we do not support single-file output to a cloud bucket. This is relatively easy to add, but has not yet been implemented.

## Configuration & authentication

Right now, all authentication is handled using `gcloud auth` from the [Google Cloud SDK](https://cloud.google.com/sdk/). **This will change in a future release.**

## Supported features

```txt
{{#include generated/features_gs.txt}}
```
