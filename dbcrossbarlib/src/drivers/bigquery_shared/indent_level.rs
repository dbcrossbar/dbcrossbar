use std::fmt;

/// How many levels should we indent generated code?
#[derive(Clone, Copy)]
pub(crate) struct IndentLevel(u8);

impl IndentLevel {
    /// No idententation.
    pub(crate) fn none() -> Self {
        IndentLevel(0)
    }

    /// Indent by one more level, up a maximum level.
    pub(crate) fn incr(self) -> Self {
        IndentLevel(self.0.saturating_add(1))
    }
}

impl fmt::Display for IndentLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for _ in 0..(self.0) {
            write!(f, "  ")?
        }
        Ok(())
    }
}
