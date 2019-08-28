//! Implementation of `schema`.

use super::{data_type::OptypeExt, BigMlAction, BigMlCredentials, BigMlLocator};
use crate::common::*;
use crate::schema::{Column, Table};

/// Implementation of `schema`, but as a real `async` function.
pub(crate) async fn schema_helper(
    _ctx: Context,
    source: BigMlLocator,
) -> Result<Option<Table>> {
    let creds = BigMlCredentials::try_default()?;
    let client = creds.client()?;
    if let BigMlAction::ReadDataset(id) = &source.action {
        let dataset = client.fetch(id).await?;
        let fields = &dataset.fields;
        if fields.is_empty() {
            return Err(format_err!(
                "dataset has no columns, has it finished creating?"
            ));
        }

        let mut columns = vec![];
        for (name, field) in fields {
            columns.push(Column {
                name: name.to_owned(),
                is_nullable: true,
                data_type: field.optype.to_data_type()?,
                comment: None,
            });
        }

        Ok(Some(Table {
            name: "dataset".to_owned(),
            columns,
        }))
    } else {
        Err(format_err!("cannot read schema from {}", source))
    }
}
