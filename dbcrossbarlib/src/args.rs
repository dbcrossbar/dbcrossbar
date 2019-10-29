//! Arguments passed to various operations.

use std::{fmt, marker::PhantomData};

use crate::common::*;
use crate::separator::Separator;

/// Trait used to add new methods to `EnumSet`.
pub(crate) trait EnumSetExt<T: EnumSetType> {
    /// Display this `EnumSet` using custom pretty-printing. This requires a
    /// wrapper type because we can't define [`fmt::Display`] directly on
    /// `EnumSet`, because it was defined in a different crate.
    fn display(self) -> DisplayEnumSet<T>;
}

impl<T: EnumSetType> EnumSetExt<T> for EnumSet<T> {
    fn display(self) -> DisplayEnumSet<T> {
        DisplayEnumSet(self)
    }
}

/// A wrapper which allows us to perform custom pretty-printing of an `EnumSet`
/// subtype. Created using [`EnumSetExt::display`].
pub(crate) struct DisplayEnumSet<T: EnumSetType>(pub EnumSet<T>);

/// This is a marker trait used by `SharedArguments`, `SourceArguments` and
/// `DestinationArguments`. We use it to keep track whether or not the arguments
/// have been verified against a driver's `Features` list.
///
/// This is used to implement the [type state][] pattern.
///
/// [type state]: http://cliffle.com/blog/rust-typestate/
pub trait ArgumentState: Clone {}

/// This is used to mark an `*Arguments` structure that has not yet been
/// verified for compatibility with a specific driver. See the [type state][]
/// pattern for details.
///
/// [type state]: http://cliffle.com/blog/rust-typestate/
#[derive(Clone)]
pub enum Unverified {}
impl ArgumentState for Unverified {}

/// This is used to mark an `*Arguments` structure that has not yet been
/// verified for compatibility with a specific driver. See the [type state][]
/// pattern for details.
///
/// [type state]: http://cliffle.com/blog/rust-typestate/
#[derive(Clone)]
pub enum Verified {}
impl ArgumentState for Verified {}

/// Arguments used by both the data source and destination.
#[derive(Clone, Debug)]
pub struct SharedArguments<S: ArgumentState> {
    /// The portable data schema describing the table we're transfering.
    schema: Table,

    /// Various locations that can be used to store temporary data during
    /// the transfer.
    temporary_storage: TemporaryStorage,

    /// We need to include a reference to `ArgumentState` somewhere, so use a
    /// 0-byte phantom value.
    _phantom: PhantomData<S>,
}

// These methods are only available in the `Unverified` state.
impl SharedArguments<Unverified> {
    /// Create a new `SharedArguments` structure.
    pub fn new(schema: Table, temporary_storage: TemporaryStorage) -> Self {
        Self {
            schema,
            temporary_storage,
            _phantom: PhantomData,
        }
    }

    /// Verify that this structure only contains supported arguments. This uses
    /// the [type state][] pattern to keep track of whether our arguments have
    /// been verified to be supported.
    ///
    /// [type state]: http://cliffle.com/blog/rust-typestate/
    pub fn verify(self, _features: Features) -> Result<SharedArguments<Verified>> {
        // TODO: We do not currently require verification for any of our fields.
        Ok(SharedArguments {
            schema: self.schema,
            temporary_storage: self.temporary_storage,
            _phantom: PhantomData,
        })
    }
}

// These methods are only available in the `Verified` state.
impl SharedArguments<Verified> {
    /// Get the table scheme used for this transfer.
    pub fn schema(&self) -> &Table {
        &self.schema
    }

    /// Get the temporary storage available for use by this transfer.
    pub fn temporary_storage(&self) -> &TemporaryStorage {
        &self.temporary_storage
    }
}

/// What `SourceArguments` features are supported by a given driver?
#[derive(Debug, EnumSetType)]
pub enum SourceArgumentsFeatures {
    DriverArgs,
    WhereClause,
}

impl fmt::Display for DisplayEnumSet<SourceArgumentsFeatures> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut sep = Separator::new(" ");
        if self.0.contains(SourceArgumentsFeatures::DriverArgs) {
            write!(f, "{}--from-arg=$NAME=$VALUE", sep.display())?;
        }
        if self.0.contains(SourceArgumentsFeatures::WhereClause) {
            write!(f, "{}--where=$SQL_EXPR", sep.display())?;
        }
        Ok(())
    }
}

/// Data source arguments.
#[derive(Clone, Debug, Default)]
pub struct SourceArguments<ArgumentState> {
    /// Driver-specific arguments for our data source.
    driver_args: DriverArguments,

    /// A `WHERE` clause for this query.
    where_clause: Option<String>,

    /// We need to include a reference to `ArgumentState` somewhere, so use a
    /// 0-byte phantom value.
    _phantom: PhantomData<ArgumentState>,
}

// These methods are only available in the `Unverified` state.
impl SourceArguments<Unverified> {
    /// Construct a new `SourceArguments`.
    pub fn new(driver_args: DriverArguments, where_clause: Option<String>) -> Self {
        Self {
            driver_args,
            where_clause,
            _phantom: PhantomData,
        }
    }

    /// Construct a new `SourceArguments` with typical values for a temporary
    /// storage location.
    pub fn for_temporary() -> Self {
        Self::new(DriverArguments::default(), None)
    }

    /// Verify that this structure only contains supported arguments. This uses
    /// the [type state][] pattern to keep track of whether our arguments have
    /// been verified to be supported.
    ///
    /// [type state]: http://cliffle.com/blog/rust-typestate/
    pub fn verify(self, features: Features) -> Result<SourceArguments<Verified>> {
        if !features
            .source_args
            .contains(SourceArgumentsFeatures::DriverArgs)
            && !self.driver_args.is_empty()
        {
            return Err(format_err!("this data source does not support --from-args"));
        }
        if !features
            .source_args
            .contains(SourceArgumentsFeatures::WhereClause)
            && self.where_clause.is_some()
        {
            return Err(format_err!("this data source does not support --where"));
        }
        Ok(SourceArguments {
            driver_args: self.driver_args,
            where_clause: self.where_clause,
            _phantom: PhantomData,
        })
    }
}

// These methods are only available in the `Verified` state.
impl SourceArguments<Verified> {
    /// Driver-specific arguments for our data source.
    pub fn driver_args(&self) -> &DriverArguments {
        &self.driver_args
    }

    /// A `WHERE` clause for this query.
    pub fn where_clause(&self) -> Option<&str> {
        self.where_clause.as_ref().map(|s| &s[..])
    }
}

/// What `DestinationArguments` features are supported by a given driver?
#[derive(Debug, EnumSetType)]
pub enum DestinationArgumentsFeatures {
    DriverArgs,
}

impl fmt::Display for DisplayEnumSet<DestinationArgumentsFeatures> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut sep = Separator::new(" ");
        if self.0.contains(DestinationArgumentsFeatures::DriverArgs) {
            write!(f, "{}--to-arg=$NAME=$VALUE", sep.display())?;
        }
        Ok(())
    }
}
/// Data destination arguments.
#[derive(Clone, Debug, Default)]
pub struct DestinationArguments<ArgumentState> {
    /// Driver-specific arguments for our data destination.
    driver_args: DriverArguments,

    /// What to do it the destination already exists.
    if_exists: IfExists,

    /// We need to include a reference to `ArgumentState` somewhere, so use a
    /// 0-byte phantom value.
    _phantom: PhantomData<ArgumentState>,
}

// These methods are only available in the `Unverified` state.
impl DestinationArguments<Unverified> {
    /// Construct a new `DestinationArguments`.
    pub fn new(driver_args: DriverArguments, if_exists: IfExists) -> Self {
        DestinationArguments {
            driver_args,
            if_exists,
            _phantom: PhantomData,
        }
    }

    /// Construct a new `DestinationArguments` with typical values for a
    /// temporary storage location.
    pub fn for_temporary() -> Self {
        Self::new(DriverArguments::default(), IfExists::Overwrite)
    }

    /// Verify that this structure only contains supported arguments. This uses
    /// the [type state][] pattern to keep track of whether our arguments have
    /// been verified to be supported.
    ///
    /// [type state]: http://cliffle.com/blog/rust-typestate/
    pub fn verify(self, features: Features) -> Result<DestinationArguments<Verified>> {
        if !features
            .dest_args
            .contains(DestinationArgumentsFeatures::DriverArgs)
            && !self.driver_args.is_empty()
        {
            return Err(format_err!(
                "this data destination does not support --to-args"
            ));
        }
        self.if_exists.verify(features.dest_if_exists)?;
        Ok(DestinationArguments {
            driver_args: self.driver_args,
            if_exists: self.if_exists,
            _phantom: PhantomData,
        })
    }
}

// These methods are only available in the `Verified` state.
impl DestinationArguments<Verified> {
    /// Driver-specific arguments for our data destination.
    pub fn driver_args(&self) -> &DriverArguments {
        &self.driver_args
    }

    /// What to do it the destination already exists.
    pub fn if_exists(&self) -> &IfExists {
        &self.if_exists
    }
}
