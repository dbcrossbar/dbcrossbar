/// BigML `Source` extensions.
use bigml::resource::{
    source::{FieldUpdate, Optype, SourceUpdate},
    Source,
};
use std::collections::HashMap;

use super::data_type::OptypeExt;
use crate::common::*;

/// Extensions to a BigML [`Source`] value.
pub(crate) trait SourceExt {
    /// Given a portable `Table` schema describing the data contained in this
    /// source, generate a `SourceUpdate` which will override the inferred
    /// column types with the correct ones.
    fn calculate_column_type_fix(
        &self,
        schema: &Table,
        optype_for_text: Optype,
    ) -> Result<SourceUpdate>;
}

impl SourceExt for Source {
    fn calculate_column_type_fix(
        &self,
        schema: &Table,
        optype_for_text: Optype,
    ) -> Result<SourceUpdate> {
        // Map column names to optypes.
        let mut field_optypes = HashMap::<&str, Optype>::new();
        for col in &schema.columns {
            // TODO: We may need to sanitize names for BigML compatibility here.
            field_optypes.insert(
                &col.name,
                Optype::for_data_type(&col.data_type, optype_for_text)?,
            );
        }

        // Iterate over the fields in the BigML source, mapping them to the
        // optype we just calculated.
        let fields = self
            .fields
            .as_ref()
            .ok_or_else(|| format_err!("BigML source has no fields"))?;
        let mut field_updates = HashMap::<String, FieldUpdate>::new();
        for (name, field) in fields {
            if let Some(&optype) = field_optypes.get(&field.name[..]) {
                let field_update = FieldUpdate {
                    optype: Some(optype),
                    ..FieldUpdate::default()
                };
                field_updates.insert(name.to_owned(), field_update);
            }
        }

        // Build our update.
        Ok(SourceUpdate {
            fields: Some(Some(field_updates)),
            ..SourceUpdate::default()
        })
    }
}
