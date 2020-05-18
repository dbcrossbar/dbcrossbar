//! A generic "parse error" with very fancy formatting.

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term,
};
use std::{error::Error as StdError, fmt, io::Cursor, ops::Range, sync::Arc};
use termcolor::NoColor;

/// An error occurred processing the schema.
#[derive(Debug)]
pub(crate) struct ParseError {
    /// The name of the file in which the error occurred.
    file_name: String,

    /// Our input file. Yes, this is huge, so we store it behind a thread-safe
    /// reference count using `Arc`.
    file_string: Arc<String>,

    /// The location of the error.
    pub(crate) annotations: Vec<Annotation>,

    /// The error message to display.
    pub(crate) message: String,
}

impl ParseError {
    /// Construct a parse error from an input file.
    pub(crate) fn from_file_string(
        file_name: String,
        file_string: Arc<String>,
        annotations: Vec<Annotation>,
        message: String,
    ) -> ParseError {
        ParseError {
            file_name,
            file_string,
            annotations,
            message,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Build a set of source files.
        let mut files = SimpleFiles::new();
        let file_id = files.add(&self.file_name, self.file_string.as_ref());

        // Build our diagnostic.
        let diagnostic = Diagnostic::error().with_message(&self.message).with_labels(
            self.annotations
                .iter()
                .map(|a| match a.ty {
                    AnnotationType::Primary => {
                        Label::primary(file_id, &a.location).with_message(&a.message)
                    }
                    AnnotationType::Secondary => {
                        Label::secondary(file_id, &a.location).with_message(&a.message)
                    }
                })
                .collect(),
        );

        // Normally, we would write this directly to standard error with some
        // pretty colors, but we can't do that inside `Display`, because we
        // don't know if we're displaying to the terminal or not. So write
        // everything to a local buffer.
        let mut buf = Vec::with_capacity(1024);
        let mut wtr = NoColor::new(Cursor::new(&mut buf));
        let config = codespan_reporting::term::Config::default();
        term::emit(&mut wtr, &config, &files, &diagnostic).map_err(|_| fmt::Error)?;
        write!(f, "{}", String::from_utf8_lossy(&buf))
    }
}

impl StdError for ParseError {}

/// An annotation pointing at a particular part of our input.
#[derive(Debug)]
pub(crate) struct Annotation {
    /// What type of annotation is this?
    pub(crate) ty: AnnotationType,

    /// What location are we annotating?
    pub(crate) location: Location,

    /// The message to display for this annotation.
    pub(crate) message: String,
}

/// What type of annotation are we displaying?
#[derive(Debug)]
pub(crate) enum AnnotationType {
    /// This the main source location associated with the error.
    Primary,
    /// This is a secondary source location associated with the error.
    Secondary,
}

/// The location where an error occurred.
#[derive(Debug)]
pub(crate) enum Location {
    /// This error occurred as a specific place in the source code.
    Position(usize),
    /// This error occurred at a span in the source code.
    Range(Range<usize>),
}

impl<'a> From<&'a Location> for Range<usize> {
    fn from(input: &'a Location) -> Self {
        match input {
            Location::Position(p) => *p..(*p + 1),
            Location::Range(r) => r.to_owned(),
        }
    }
}
