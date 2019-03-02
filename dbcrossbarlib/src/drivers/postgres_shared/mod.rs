//! Code shared between various PostgreSQL-related drivers.

mod create_table_sql;

pub(crate) use self::create_table_sql::{parse_create_table, write_create_table};
