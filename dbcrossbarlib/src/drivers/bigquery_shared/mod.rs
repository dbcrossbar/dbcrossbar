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

mod column;
mod data_type;
mod table;
mod table_name;

pub(crate) use self::column::*;
pub(crate) use self::data_type::*;
pub(crate) use self::table::*;
pub(crate) use self::table_name::*;
