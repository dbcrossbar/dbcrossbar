use async_trait::async_trait;

use super::DataFormatConverter;
use crate::{common::*, json_to_csv::json_lines_to_csv};

pub(crate) struct JsonLinesConverter;

#[async_trait]
impl DataFormatConverter for JsonLinesConverter {
    async fn data_format_to_csv(
        &self,
        ctx: &Context,
        schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        json_lines_to_csv(ctx, schema, data).await
    }

    async fn csv_to_data_format(
        &self,
        _ctx: &Context,
        _schema: &Schema,
        _data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        Err(format_err!(
            "cannot convert CSV to JSON Lines (not yet implemented)"
        ))
    }
}
