//! Extra query details.

use crate::common::*;

/// Extra details of a query.
#[derive(Clone, Debug, Default)]
pub struct Query {
    /// A `WHERE` clause for this query.
    pub where_clause: Option<String>,
    /// Private field to make the structure extensible without breaking the API.
    _placeholder: (),
}

impl Query {
    /// Return an error if any query details were actually provided. Called by
    /// all backends that don't support query details.
    ///
    /// (We might want to make this more fine-grained as we add more kinds of
    /// query details over time. Perhaps we could pass in a list of supported
    /// query details.)
    pub(crate) fn fail_if_query_details_provided(&self) -> Result<()> {
        if self.where_clause.is_some() {
            return Err(format_err!(
                "`--where` is not supported by this data source"
            ));
        }
        Ok(())
    }
}
