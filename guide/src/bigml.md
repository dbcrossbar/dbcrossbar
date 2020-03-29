# BigML

[BigML](https://bigml.com/) is a hosted machine-learning service, with support for many common algorithms and server-side batch scripts.

## Example locators

Source locators:

- `bigml:dataset/$ID`: Read data from a BigML dataset.

Destination locators:

- `bigml:source`: Create a single BigML "source" resource from the input data.
- `bigml:sources`: Create multiple BigML "source" resources from the input data.
- `bigml:dataset`: Create a single BigML "dataset" resource from the input data.
- `bigml:datasets`: Create multiple BigML "dataset" resources from the input data.

If you use BigML as a destination, `dbcrossbar` will automatically activate `--display-output-locators`, and it will print locators for all the created resources on standard output. Column types on created "source" resources will be set something appropriate (but see `optype_for_text` below.)

## Configuration & authentication

The BigML driver requires more configuration than most.

You'll need to set the following environment variables:

- `BIGML_USERNAME`: Set this to your BigML username.
- `BIGML_API_KEY`: Set this to your BigML API key.
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`: Set these to your AWS credentials when using BigML as a destination. Do **not** set `AWS_SESSION_TOKEN`; it will not work with BigML.

You'll also need to pass the following on the command line when using:

- `--temporary=s3://$S3_TEMP_BUCKET`: Specify where to stage files for loading into BigML. This is not needed when using BigML as a source.

You can also specify the following `--to-arg` values:

- `name`: The human-readable name of the resource to create.
- `optype_for_text`: The BigML optype to use for text fields. This defaults to `text`. You may want to set it to `categorical` if your text fields contain a small set of fixed strings.
- `tag`: This may be specified repeatedly to attach tags to the created resources.

## Supported features

```txt
{{#include generated/features_bigml.txt}}
```

Note that `--if-exists` is simply ignored, because BigML will always create new resources.
