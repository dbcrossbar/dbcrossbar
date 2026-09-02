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

`dbcrossbar` uses Google's Application Default Credentials, the same search order as Google's official client libraries. It does not run the `gcloud` CLI.

1. If a dbcrossbar service account key is configured, that key is used. Set `GCLOUD_SERVICE_ACCOUNT_KEY` to the JSON, or place the JSON in `$DBCROSSBAR_CONFIG_DIR/gcloud_service_account_key.json`. This is the documented path for servers and containers that already ship a service account key to dbcrossbar.
2. Otherwise credentials come from Application Default Credentials:
   - `GOOGLE_APPLICATION_CREDENTIALS`, if set, must point at a readable credentials file. Supported `type` values include `service_account`, `authorized_user`, `impersonated_service_account` (for example `gcloud auth application-default login --impersonate-service-account`), and `external_account` (for example GitHub's `google-github-actions/auth`). A missing or unparseable file is an error; dbcrossbar will not fall through to another source.
   - If that variable is unset, the well-known ADC file is used (`~/.config/gcloud/application_default_credentials.json` on Linux and macOS).
   - If that file is also absent, credentials come from the GCE or GKE metadata server.

Run with `RUST_LOG=debug` to see which source was selected. The log names the credentials file and its JSON `type` when a file is used. It does not log key material or tokens.

For more information on `DBCROSSBAR_CONFIG_DIR`, see [Configuration](./config.html).

For a service account, you can use the following permissions:

- Storage Object Admin (Cloud Storage and BigQuery drivers)
- BigQuery Data Editor (BigQuery driver only)
- BigQuery Job User (BigQuery driver only)
- BigQuery User (BigQuery driver only)

There's probably a more limited set of permissions which will work if you set them up manually.

## Supported features

```txt
{{#include generated/features_gs.txt}}
```
