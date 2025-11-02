/// Formatting utility for printing separators.
use std::fmt;

/// A separator string that will not print the _first_ time it used,
/// but will print every time thereafter. Used to print commas or spaces
/// in between items, but not before the first item.
pub(crate) struct Separator<'a> {
    text: &'a str,
    first_time: bool,
}

impl Separator<'_> {
    /// Create a new separator which displays the specified string.
    pub(crate) fn new(text: &str) -> Separator<'_> {
        Separator {
            text,
            first_time: true,
        }
    }

    /// Return a displayable version of this separator. The first time this
    /// is called, the resulting `SeparatorDisplay` will not print anything.
    /// The next time, it will print the separator text.
    pub(crate) fn display(&mut self) -> SeparatorDisplay<'_> {
        if self.first_time {
            self.first_time = false;
            SeparatorDisplay(None)
        } else {
            SeparatorDisplay(Some(self.text))
        }
    }
}

/// Displays either nothing or a separator string.
pub(crate) struct SeparatorDisplay<'a>(Option<&'a str>);

impl fmt::Display for SeparatorDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let SeparatorDisplay(Some(text)) = *self {
            write!(f, "{}", text)?;
        }
        Ok(())
    }
}
