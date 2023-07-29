use std::io::Cursor;

use async_trait::async_trait;

use super::DataFormatConverter;
use crate::{
    common::*,
    schema::{Column, DataType},
};

pub(crate) struct CsvConverter;

#[async_trait]
impl DataFormatConverter for CsvConverter {
    async fn schema(
        &self,
        _ctx: &Context,
        table_name: &str,
        mut data: BoxStream<BytesMut>,
    ) -> Result<Option<Schema>> {
        // Read the first line of the CSV file, which contains our headers.
        // It's fairly safe to check for "\n", because Unix uses "\n" and
        // Windows uses "\r\n". The original MacOS used "\r", but that's
        // ancient history.
        let mut bytes = vec![];
        while let Some(chunk) = data.next().await {
            let chunk = chunk?;
            let have_line = chunk.contains(&b'\n');
            bytes.extend_from_slice(&chunk);
            if have_line {
                break;
            }
        }
        if let Some(eol_pos) = bytes.iter().position(|b| *b == b'\n') {
            bytes.truncate(eol_pos);
        }

        // Build our columns.
        let mut rdr = csv::Reader::from_reader(Cursor::new(bytes));
        let mut columns = vec![];
        let headers = rdr
            .headers()
            .with_context(|| format!("error reading {}", table_name))?;
        for col_name in headers {
            columns.push(Column {
                name: col_name.to_owned(),
                is_nullable: true,
                data_type: DataType::Text,
                comment: None,
            })
        }

        // Build our table.
        Ok(Some(Schema::from_table(Table {
            name: table_name.to_owned(),
            columns,
        })?))
    }

    async fn data_format_to_csv(
        &self,
        _ctx: &Context,
        _schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        Ok(data)
    }

    async fn csv_to_data_format(
        &self,
        _ctx: &Context,
        _schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        Ok(data)
    }
}
