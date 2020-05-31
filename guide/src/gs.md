# Google Cloud Storage

Google Cloud Storage is a bucket-based storage system similar to Amazon's S3. It's frequently used in connection with BigQuery and other Google Cloud services.

## Example locators

Source locators:

- `gs://bucket/dir/file.csv`
- `gs://bucket/dir/`

Destination locators:

- `gs://bucket/dir/`

At this point, we do not support single-file output to a cloud bucket. This is relatively easy to add, but has not yet been implemented.

## Configuration & authentication

**0.4.x and later:** You can authenticate using either a client secret or a service key, which you can create using the [console credentials page](https://console.cloud.google.com/apis/credentials).

- Client secrets can be stored in `$DBCROSSBAR_CONFIG_DIR/gcloud_client_secret.json` or in `GCLOUD_CLIENT_SECRET`. These are strongly recommended for interactive use.
- Service account keys can be stored in `$DBCROSSBAR_CONFIG_DIR/gcloud_service_account_key.json` or in `GCLOUD_SERVICE_ACCOUNT_KEY`. These are recommended for server and container use.

For more information on `DBCROSSBAR_CONFIG_DIR`, see [Configuration](./config.html).

For a service account, you can use the following permissions:

- Storage Object Admin (Cloud Storage and BigQuery drivers)
- BigQuery Data Editor (BigQuery driver only)
- BigQuery Job User (BigQuery driver only)
- BigQuery User (BigQuery driver only)

There's probably a more limited set of permissions which will work if you set them up manually.

**0.3.x and earlier:** All authentication is handled using `gcloud auth` from the [Google Cloud SDK](https://cloud.google.com/sdk/).

## Supported features

```txt
{{#include generated/features_gs.txt}}
```
