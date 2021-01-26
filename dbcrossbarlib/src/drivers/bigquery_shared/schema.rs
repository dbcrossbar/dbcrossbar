//! BigQuery-related extensions to `Schema`.

use super::TableBigQueryExt;
use crate::common::*;

/// Extensions to `Column` (the portable version) to handle BigQuery-query
/// specific stuff.
pub(crate) trait SchemaBigQueryExt {
    /// Can we import data into this table directly from a CSV file?
    fn bigquery_can_import_from_csv(&self) -> Result<bool>;
}

impl SchemaBigQueryExt for Schema {
    fn bigquery_can_import_from_csv(&self) -> Result<bool> {
        self.table.bigquery_can_import_from_csv(self)
    }
}
