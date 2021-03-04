//! Implementation of `schema`.

use super::{data_type::OptypeExt, BigMlAction, BigMlLocator};
use crate::common::*;
use crate::schema::{Column, Table};

/// Implementation of `schema`, but as a real `async` function.
pub(crate) async fn schema_helper(
    _ctx: Context,
    source: BigMlLocator,
) -> Result<Option<Schema>> {
    let client = bigml::Client::new_from_env()?;
    if let BigMlAction::ReadDataset(id) = &source.action {
        let dataset = client.fetch(id).await?;
        let fields = &dataset.fields;
        if fields.is_empty() {
            return Err(format_err!(
                "dataset has no columns, has it finished creating?"
            ));
        }

        // We need to sort the fields by their BigML field ID, but then
        // use the human-readable name.
        let mut columns = vec![];
        let mut fields = fields.iter().collect::<Vec<_>>();
        fields.sort_by(|&(id1, _), &(id2, _)| id1.cmp(id2));
        for (_field_id, field) in fields {
            columns.push(Column {
                name: field.name.clone(),
                is_nullable: true,
                data_type: field.optype.to_data_type()?,
                comment: None,
            });
        }

        Ok(Some(Schema::from_table(Table {
            name: "dataset".to_owned(),
            columns,
        })?))
    } else {
        Err(format_err!("cannot read schema from {}", source))
    }
}
