//! Pretty-printing support.
//!
//! Because Trino doesn't allow us to define local UDFs, we often need to
//! generate deeply nested SQL. In order to make this even slightly readable, we
//! need to pretty-print it.
//!
//! This is based on [Wadler's "A prettier printer" paper][wadler] and the Rust
//! [`pretty`] crate. I highly recommend reading the Wadler paper; it's
//! clear and interesting, and you're unlikely to understand how any of this
//! works without reading it. This is sort of how Haskell programming
//! works—there's a really nice and accessible paper somewhere, and without it,
//! nothing makes sense.
//!
//! [wadler]: https://homepages.inf.ed.ac.uk/wadler/papers/prettier/prettier.pdf

use dbcrossbar_trino::pretty::{comma_sep_list, indent};
use pretty::RcDoc;

use super::TrinoTableName;

/// What's our standard width for pretty-printing?
pub(crate) const WIDTH: usize = 79;

/// A clause in an SQL statement. A clause is:
///
/// - Typically starts with a keyword like `SELECT` or `FROM`.
/// - Followed by a newline (or a space, if gets merged into a line with the
///   next clause).
/// - Wrapped in a `group` so that it can be merged into a single line if
///   needed.
pub(super) fn sql_clause(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
    doc.group().append(RcDoc::line())
}

/// A `SELECT ... FROM ...` clause. This is common enough to be worth a helper.
pub(super) fn select_from(
    select_exprs: impl IntoIterator<Item = RcDoc<'static, ()>>,
    from_table: &TrinoTableName,
) -> RcDoc<'static, ()> {
    RcDoc::concat(vec![
        sql_clause(RcDoc::concat(vec![
            RcDoc::text("SELECT"),
            indent(comma_sep_list(select_exprs.into_iter())),
        ])),
        sql_clause(RcDoc::concat(vec![
            RcDoc::text("FROM"),
            indent(RcDoc::as_string(from_table)),
        ])),
    ])
}
