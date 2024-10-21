use std::fmt;

/// An error related to a connector.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectorError {
    /// We do not support this connector type.
    UnsupportedType(String),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectorError::UnsupportedType(connector_type) => {
                write!(f, "unsupported connector type: {:?}", connector_type)
            }
        }
    }
}

/// An error related to a Trino identifier.
#[derive(Debug)]
#[non_exhaustive]
pub enum IdentifierError {
    EmptyIdentifier,
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdentifierError::EmptyIdentifier => {
                write!(f, "Trino identifiers cannot be the empty string")
            }
        }
    }
}
