//! External `dbcrossbar-schema:` format, which isn't quite the same as our
//! internal `Schema` format.

use serde::{Deserialize, Serialize};

use crate::{common::*, schema::NamedDataType};

/// Our external schema format, version 2.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ExternalSchemaV2 {
    pub(crate) named_data_types: Vec<NamedDataType>,
    pub(crate) tables: Vec<Table>,
}

/// Our external schema format. This exists so that `serde` can magically figure
/// out which version of our external schema format we're using.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum ExternalSchema {
    /// A full schema, including a single table and a list of types.
    V2(ExternalSchemaV2),
    /// Just a bare top-level table, from before we added support for tables and
    /// multiple types.
    V1(Table),
}

impl ExternalSchema {
    /// Turn a portable schema into an external schema (we always use v2).
    pub(crate) fn from_schema(schema: Schema) -> Self {
        let v2 = ExternalSchemaV2 {
            named_data_types: schema
                .named_data_types
                .into_iter()
                .map(|(_, v)| v)
                .collect(),
            tables: vec![schema.table],
        };
        ExternalSchema::V2(v2)
    }

    /// Convert an external schema to our internal format.
    pub(crate) fn into_schema(self) -> Result<Schema> {
        match self {
            ExternalSchema::V2(mut v2) => {
                if v2.tables.len() != 1 {
                    return Err(format_err!(
                        "dbcrossbar-schema must contain only a single table for now"
                    ));
                }
                let table = v2.tables.remove(0);
                Schema::from_types_and_table(v2.named_data_types, table)
            }
            ExternalSchema::V1(table) => Schema::from_table(table),
        }
    }
}
