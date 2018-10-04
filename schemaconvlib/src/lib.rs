//! A library for reading and writing table schemas in various formats.

#![warn(missing_docs)]

extern crate diesel;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod table;

pub use table::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
