//! Error-related utilities for the Trino driver.

use std::sync::Arc;

use prusto::{error::Error as PrustoError, QueryError};

use crate::{
    common::*,
    parse_error::{Annotation, FileInfo, ParseError},
};

/// Should an error be retried?
///
/// Note that the `rusto` crate has internal support for retrying connection
/// and network errors, so we don't need to worry about that. But we do need
/// to look out for `QueryError`s that might need to be retried.
pub(crate) fn should_retry(e: &PrustoError) -> bool {
    matches!(e, PrustoError::QueryError(QueryError { error_name, .. }) if error_name == "NO_NODES_AVAILABLE")
}

/// These errors are pages long.
pub(crate) fn abbreviate_trino_error(sql: &str, e: PrustoError) -> Error {
    if let PrustoError::QueryError(e) = &e {
        // We can make these look pretty.
        let QueryError {
            message,
            error_location,
            ..
        } = e;
        let file_info = FileInfo::new("in.sql".to_owned(), sql.to_owned());

        // We don't want to panic, because we're already processing an
        // error, and the error comes from an external source. So just
        // muddle through and return a bogus location if our input data is
        // too odd.
        let mut offset = 0;
        if let Some(loc) = error_location {
            // Convert from u32, defaulting negative values to 1. (Lines count
            // from 1.)
            let line_number = usize::try_from(loc.line_number)
                .unwrap_or(1)
                .saturating_sub(1);
            let column_number = usize::try_from(loc.column_number)
                .unwrap_or(1)
                .saturating_sub(1);
            for (i, line) in sql.lines().enumerate() {
                if i == line_number {
                    break;
                }
                offset += line.len() + 1;
            }
            offset += column_number;
        };

        let annotation = Annotation::primary(offset, message.clone());
        return From::from(ParseError::new(
            Arc::new(file_info),
            vec![annotation],
            format!("Trino error: {}", message),
        ));
    }

    let msg = e
        .to_string()
        .lines()
        .take(10)
        .collect::<Vec<_>>()
        .join("\n");
    format_err!("Trino error: {}", msg)
}
