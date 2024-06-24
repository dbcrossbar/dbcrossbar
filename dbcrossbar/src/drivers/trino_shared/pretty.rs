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
//! [wadler]: https://homepages.inf.ed.ac.uk/wadler/papers/prettier/prettier.pdf)

use std::fmt;

use pretty::RcDoc;

/// How many spaces should we indent?
pub(super) const INDENT: isize = 2;

/// What's our standard width for pretty-printing?
pub(super) const WIDTH: usize = 79;

/// Helper struct used to pretty-print a [`RcDoc`] using [`fmt::Display`].
pub(super) struct PrettyFmt {
    doc: RcDoc<'static, ()>,
    width: usize,
}

impl PrettyFmt {
    /// Create a new `PrettyFmt` with the specified width.
    pub(super) fn new(doc: RcDoc<'static, ()>, width: usize) -> Self {
        Self { doc, width }
    }
}

impl fmt::Display for PrettyFmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.doc.pretty(self.width).fmt(f)
    }
}

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
pub(super) fn brackets(
    open: &'static str,
    doc: RcDoc<'static, ()>,
    close: &'static str,
) -> RcDoc<'static, ()> {
    RcDoc::concat(vec![
        RcDoc::as_string(open),
        // `nest` controls how many spaces we add after each newline,
        // and nothing else.
        RcDoc::concat(vec![RcDoc::line_(), doc]).nest(INDENT),
        // This newline goes _outside_ the `nest` block, so the closing bracket
        // isn't indented.
        RcDoc::line_(),
        RcDoc::as_string(close),
    ])
    .group()
}

/// Wrap an [`RcDoc`] in parentheses.
pub(super) fn parens(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
    brackets("(", doc, ")")
}

// /// Wrap an [`RcDoc`] in square brackets.
// pub(super) fn square_brackets(doc: RcDoc<'static, ()>) -> RcDoc<'static, ()> {
//     brackets("[", doc, "]")
// }

/// Comma separator.
pub(super) fn comma_sep() -> RcDoc<'static, ()> {
    RcDoc::concat(vec![RcDoc::as_string(","), RcDoc::line()])
}

/// Comma separated list.
pub(super) fn comma_sep_list(
    docs: impl IntoIterator<Item = RcDoc<'static, ()>>,
) -> RcDoc<'static, ()> {
    RcDoc::intersperse(docs, comma_sep())
}
