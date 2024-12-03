//! Pretty-printing support for Trino SQL.
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

use pretty::RcDoc;

/// How many spaces should we indent?
pub const INDENT: isize = 2;

pub mod ast;

/// Wrap an [`RcDoc`] in brackets. This is a bit fiddly if we want to get the
/// indentation right, so do it only once. If we fit on one line, we want to
/// indent like this:
///
/// ```sql
/// MYFUNC(arg1, arg2)
/// ```
///
/// If we need to wrap, we want to indent like this:
///
/// ```sql
/// MYFUNC(
///    arg1,
///    arg2
/// )
/// ```
///
/// Getting this behavior requires a fairly close reading of Wadler's paper, as
/// mentioned in the module-level documentation.
pub fn brackets(
    open: &'static str,
    doc: RcDoc<'static, ()>,
    close: &'static str,
) -> RcDoc<'static, ()> {
    RcDoc::concat(vec![
        RcDoc::text(open),
        // `nest` controls how many spaces we add after each newline,
        // and nothing else.
        RcDoc::line_().append(doc).nest(INDENT),
        // This newline goes _outside_ the `nest` block, so the closing bracket
        // isn't indented.
        RcDoc::line_(),
        RcDoc::text(close),
    ])
    .group()
}

/// Wrap an [`RcDoc`] in parentheses.
pub fn parens(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
    brackets("(", doc, ")")
}

/// Wrap an [`RcDoc`] in square brackets.
pub fn square_brackets(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
    brackets("[", doc, "]")
}

/// Comma separator.
pub fn comma_sep() -> RcDoc<'static, ()> {
    RcDoc::text(",").append(RcDoc::line())
}

/// Comma separated list.
pub fn comma_sep_list(
    docs: impl IntoIterator<Item = RcDoc<'static, ()>>,
) -> RcDoc<'static, ()> {
    RcDoc::intersperse(docs, comma_sep())
}

/// An indentable block (not surrounded by brackets). If you want some kind of
/// brackets, see [`brackets`], [`parens`], etc.
pub fn indent(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
    RcDoc::line().append(doc).nest(INDENT)
}
