//! Implementation of `write_local_data`.

use bigml::{
    self,
    resource::{dataset, source, source::Optype, Resource, Source},
};
use chrono::{Duration, Utc};
use serde::Deserialize;
use tracing::{field, Span};

use super::{source::SourceExt, BigMlLocator, CreateOptions};
use crate::clouds::aws::{sign_s3_url, AwsCredentials};
use crate::common::*;
use crate::concat::concatenate_csv_streams;
use crate::drivers::s3::find_s3_temp_dir;

/// Parsed version of `--to-arg` values.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BigMlDestinationArguments {
    /// The name of the source or dataset to create.
    name: Option<String>,

    /// The default optype to use for text fields.
    optype_for_text: Option<Optype>,

    /// Tags to apply to the resources we create.
    #[serde(default)]
    tags: Vec<String>,
}

/// Implementation of `write_local_data`, but as a real `async` function.
#[instrument(
    level = "debug",
    name = "bigml::write_local_data",
    skip_all,
    fields(dest = %dest)
)]
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: BigMlLocator,
    mut data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let shared_args_v = shared_args.clone().verify(BigMlLocator::features())?;
    let dest_args = dest_args.verify(BigMlLocator::features())?;

    // Get our portable schema.
    let schema = shared_args_v.schema().to_owned();

    // Get our BigML-specific destination arguments.
    let bigml_dest_args = dest_args
        .driver_args()
        .deserialize::<BigMlDestinationArguments>()
        .context("could not parse --to-arg")?;

    // Extract some more options from our destination locator.
    let CreateOptions {
        concat_csv_streams,
        convert_to_dataset,
    } = dest
        .to_create_options()
        .ok_or_else(|| format_err!("cannot to write to {}", dest))?;

    // Concatenate all our CSV data into a single stream if requested.
    if concat_csv_streams {
        data = box_stream_once(Ok(concatenate_csv_streams(ctx.clone(), data)?));
    }

    // See if we have an S3 temporary directory, and transform `data` into a
    // list of BigML source IDs.
    let s3_temp = find_s3_temp_dir(shared_args_v.temporary_storage()).ok();
    let sources: BoxStream<BoxFuture<Source>> = if let Some(s3_temp) = s3_temp {
        // We have S3 temporary storage, so let's copy everything there.

        // Pass our `data` streams to `S3Locator::write_local_data`, which will
        // write them to S3 and return a `BoxStream<BoxFuture<BoxLocator>>>`,
        // that is, a stream a futures yielding the S3 locators where we put
        // our data on S3.
        let s3_dest_args = DestinationArguments::for_temporary();
        let s3_locator_stream: BoxStream<BoxFuture<BoxLocator>> = s3_temp
            .write_local_data(ctx.clone(), data, shared_args, s3_dest_args)
            .await?;

        // Convert our S3 locators into BigML `Source` objects.
        let bigml_dest_args = bigml_dest_args.clone();
        let bigml_source_stream = s3_locator_stream.map_ok(move |locator_fut| {
            let bigml_dest_args = bigml_dest_args.clone();
            let fut = async move {
                // Get our S3 URL back.
                let locator = locator_fut.await?.to_string();
                if !locator.starts_with("s3://") {
                    return Err(format_err!(
                        "expected S3 driver to output s3:// URL, found {}",
                        locator,
                    ));
                }

                Span::current().record("s3_object", &field::display(&locator));
                debug!("creating BigML source from S3 object");

                // Sign the S3 URL.
                let aws_creds = AwsCredentials::try_default().await?;
                let url = locator
                    .parse::<Url>()
                    .context("could not parse S3 temporary URL")?;
                let expires = Utc::now() + Duration::hours(1);
                let (signed_url, x_amz_security_token) =
                    sign_s3_url(&aws_creds, "GET", expires, &url)?;
                if x_amz_security_token.is_some() {
                    return Err(format_err!(
                        "BigML does not support AWS_SESSION_TOKEN"
                    ));
                }

                // Create the source.
                let mut args = source::Args::remote(String::from(signed_url));
                args.disable_datetime = Some(true);
                if let Some(name) = &bigml_dest_args.name {
                    args.name = Some(name.to_owned());
                }
                args.tags = bigml_dest_args.tags.clone();
                let client = bigml::Client::new_from_env()?;
                let source = client.create(&args).await?;

                Span::current().record("bigml_source", &field::display(source.id()));
                debug!("created source from S3 object");
                Ok(source)
            }
            .instrument(debug_span!(
                "create_source",
                s3_object = field::Empty,
                bigml_source = field::Empty
            ));
            fut.boxed()
        });
        bigml_source_stream.boxed()
    } else {
        // We don't have S3 storage, so attempt a direct upload.
        //
        // TODO: Unimplemeted by BigML, sadly.
        return Err(format_err!(
            "WARNING: You must pass --temporary=s3://... for BigML"
        ));
    };

    // Finish setting up our source objects, and optionally convert them to
    // datasets.
    let written = sources.map_ok(move |ctx_source_fut| {
        let schema = schema.clone(); // Expensive.
        let bigml_dest_args = bigml_dest_args.clone();
        let fut = async move {
            let mut source = ctx_source_fut.await?;
            Span::current().record("bigml_source", &field::display(source.id()));

            // Wait for our `source` to finish being created.
            trace!("waiting for source to be ready");
            let client = bigml::Client::new_from_env()?;
            source = client.wait(source.id()).await?;

            // Fix data types.
            let optype_for_text =
                bigml_dest_args.optype_for_text.unwrap_or(Optype::Text);
            let update = source.calculate_column_type_fix(&schema, optype_for_text)?;
            trace!("updating source with {:?}", update);
            client.update(source.id(), &update).await?;
            trace!("waiting for source to be ready (again)");
            source = client.wait(source.id()).await?;

            // Optionally convert our source to a dataset.
            if !convert_to_dataset {
                // No conversion, so we're done.
                Ok(BigMlLocator::output_source(source.id().to_owned()).boxed())
            } else {
                // Convert source to dataset.
                trace!("converting to dataset");
                let mut args = dataset::Args::from_source(source.id().to_owned());
                if let Some(name) = &bigml_dest_args.name {
                    args.name = Some(name.to_owned());
                }
                args.tags = bigml_dest_args.tags.clone();
                let dataset = client.create_and_wait(&args).await?;
                debug!("converted to {}", dataset.id().to_owned());
                Ok(BigMlLocator::read_dataset(dataset.id().to_owned()).boxed())
            }
        }
        .instrument(debug_span!(
            "finish_bigml_import",
            bigml_source = field::Empty
        ));
        fut.boxed()
    });
    Ok(written.boxed())
}
