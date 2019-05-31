//! Code shared between various BigQuery-related drivers.
//!
//! Much of this code falls into a few major categories:
//!
//! - Extension traits which extend "portable" types with BigQuery-specific
//!   APIs. These wrappers include [`TableBigQueryExt`], [`ColumnBigQueryExt`]
//!   and [`DataTypeBigQueryExt`].
//! - Native BigQuery equivalents of our portable types, including [`BqTable`],
//!   [`BqColumn`] and [`BqDataType`].
//!
//! The best starting points are probably [`TableBigQueryExt`] and [`BqTable`].

use crate::common::*;

mod column;
mod data_type;
mod table;
mod table_name;

pub(crate) use self::column::*;
pub(crate) use self::data_type::*;
pub(crate) use self::table::*;
pub(crate) use self::table_name::*;

/// Convert an `IfExists` value to the corresponding `bq load` argument, or
/// return an error if we can't.
pub(crate) fn if_exists_to_bq_load_arg(if_exists: &IfExists) -> Result<&'static str> {
    match if_exists {
        IfExists::Overwrite => Ok("--replace"),
        // TODO: Verify that this is the actual behavior of `--noreplace`.
        IfExists::Append | IfExists::Upsert(_) => Ok("--noreplace"),
        // We need to be careful about race conditions--we don't want to try to
        // emulate this if we can't do it natively.
        IfExists::Error => Err(format_err!(
            "BigQuery only supports --if-exists=overwrite or --if-exists=append"
        )),
    }
}
