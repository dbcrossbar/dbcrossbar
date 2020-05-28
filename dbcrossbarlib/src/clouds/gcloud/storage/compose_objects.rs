//! Compose multiple `gs://` objects into one.

use serde::Serialize;

use super::{
    super::{percent_encode, Client},
    parse_gs_url, StorageObject,
};
use crate::common::*;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposeRequest {
    kind: &'static str,
    source_objects: Vec<ComposeObject>,
    destination: DestinationObject,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposeObject {
    /// The name of the object to compose.
    name: String,
    /// The generation that we expect this object to have.
    generation: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DestinationObject {
    content_type: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposeQuery {
    /// Set to 0 to specify that we expect no object to exist.
    if_generation_match: i64,
}

/// Compose `objects` into a single object at `file_url`. `objects` must contain
/// 1 to 32 items.
pub(crate) async fn compose_objects<'a>(
    ctx: &'a Context,
    objects: &'a [StorageObject],
    file_url: &'a Url,
) -> Result<StorageObject> {
    debug!(
        ctx.log(),
        "composing {} objects into {}",
        objects.len(),
        file_url,
    );

    // Check our inputs.
    if objects.is_empty() || objects.len() > 32 {
        return Err(format_err!(
            "expected to compose 1 to 32 objects, found {}",
            objects.len(),
        ));
    }

    // Build our compose request.
    let req = ComposeRequest {
        kind: "storage#composeRequest",
        source_objects: objects
            .iter()
            .map(|obj| ComposeObject {
                name: obj.name.clone(),
                generation: obj.generation,
            })
            .collect(),
        destination: DestinationObject {
            content_type: objects[0].content_type.clone(),
        },
    };

    // Build our URL and query.
    let (bucket, object) = parse_gs_url(&file_url)?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}/compose",
        percent_encode(&bucket),
        percent_encode(&object),
    );
    let query = ComposeQuery {
        if_generation_match: 0,
    };

    // Make our request.
    let client = Client::new(&ctx).await?;
    let composed = client
        .post::<StorageObject, _, _, _>(ctx, &url, req, query)
        .await?;
    Ok(composed)
}

/// Compose a stream of objects into a single object at `file_url`, and return
/// the resulting `StorageObject`.
pub(crate) async fn compose_object_stream<'a>(
    ctx: &'a Context,
    mut objects: BoxStream<StorageObject>,
    file_url: &'a Url,
) -> Result<StorageObject> {
    let composed: Vec<Vec<StorageObject>> = vec![vec![]];

    while let Some(object) = objects.next().await {
        let object = object?;
        //objects.last_mut().expect("should always have at least one item").push(object);
    }

    todo!()
}
