//! Specify the location of data or a schema.

use bitflags::bitflags;
use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, marker::PhantomData, str::FromStr};

use crate::common::*;
use crate::drivers::find_driver;

/// Specify the the location of data or a schema.
pub trait Locator: fmt::Debug + fmt::Display + Send + Sync + 'static {
    /// Provide a mechanism for casting a `dyn Locator` back to the underlying,
    /// concrete locator type using Rust's `Any` type.
    ///
    /// See [this StackOverflow question][so] for a discussion of the technical
    /// details, and why we need a `Locator::as_any` method to use `Any`.
    ///
    /// This is a bit of a sketchy feature to provide, but we provide it for use
    /// with `supports_write_remote_data` and `write_remote_data`, which are
    /// used for certain locator pairs (i.e., Google Cloud Storage and BigQuery)
    /// to bypass our normal `local_data` and `write_local_data` transfers and
    /// use an external, optimized transfer method (such as direct loads from
    /// Google Cloud Storage into BigQuery).
    ///
    /// This should always be implemented as follows:
    ///
    /// ```no_compile
    /// impl Locator for MyLocator {
    ///     fn as_any(&self) -> &dyn Any {
    ///         self
    ///     }
    /// }
    /// ```
    ///
    /// [so]: https://stackoverflow.com/a/33687996
    fn as_any(&self) -> &dyn Any;

    /// Return a table schema, if available.
    fn schema(&self, _ctx: &Context) -> Result<Option<Table>> {
        Ok(None)
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do.
    fn write_schema(
        &self,
        _ctx: &Context,
        _schema: &Table,
        _if_exists: IfExists,
    ) -> Result<()> {
        Err(format_err!("cannot write schema to {}", self))
    }

    /// If this locator can be used as a local data source, return a stream of
    /// CSV streams. This function type is bit hairy:
    ///
    /// 1. The outermost `BoxFuture` is essentially an async `Result`, returning
    ///    either a value or an error. It's boxed because we don't know what
    ///    concrete type it will actually be, just that it will implement
    ///    `Future`.
    /// 2. The `Option` will be `None` if we have no local data, or `Some` if we
    ///    can provide one or more CSV streams.
    /// 3. The `BoxStream` returns a "stream of streams". This _could_ be a
    ///    `Vec<CsvStream>`, but that would force us to, say, open up hundreds
    ///    of CSV files or S3 objects at once, causing us to run out of file
    ///    descriptors. By returning a stream, we allow our caller to open up
    ///    files or start downloads only when needed.
    /// 4. The innermost `CsvStream` is a stream of raw CSV data plus some other
    ///    information, like the original filename.
    fn local_data(
        &self,
        _ctx: Context,
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        // Turn our result into a future.
        async { Ok(None) }.boxed()
    }

    /// If this locator can be used as a local data sink, write data to it.
    ///
    /// This function takes a stream `data` as input, the elements of which are
    /// individual `CsvStream` values. An implementation should normally use
    /// `map` or `and_then` to write those CSV streams to storage associated
    /// with the locator, and return a stream of `BoxFuture<()>` values:
    ///
    /// ```no_compile
    /// # Pseudo code for parallel output.
    /// data.map(async |csv_stream| {
    ///     write(csv_stream).await?;
    ///     Ok(())
    /// })
    /// ```
    ///
    /// For cases where output must be serialized, it's OK to consume the entire
    /// `data` stream, and return a single-item stream containing `()`.
    ///
    /// The caller of `write_local_data` will pull several items at a time from
    /// the returned `BoxStream<BoxFuture<()>>` and evaluate them in parallel.
    fn write_local_data(
        &self,
        _ctx: Context,
        _data: BoxStream<CsvStream>,
        _shared_args: SharedArguments<Unverified>,
        _dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        let err = format_err!("cannot write data to {}", self);
        async move { Err(err) }.boxed()
    }

    /// Can we access the data at `source` directly using `write_remote_data`?
    fn supports_write_remote_data(&self, _source: &dyn Locator) -> bool {
        false
    }

    /// Take the data at `source`, and write to this locator directly, without
    /// passing it through the local system.
    ///
    /// This is used to bypass `source.local_data` and `dest.write_local_data`
    /// when we don't need them.
    fn write_remote_data(
        &self,
        _ctx: Context,
        source: BoxLocator,
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
        _dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<()> {
        let err = format_err!("cannot write_remote_data from source {}", source);
        async move { Err(err) }.boxed()
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

impl FromStr for BoxLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        // Parse our locator into a URL-style scheme and the rest.
        lazy_static! {
            static ref SCHEME_RE: Regex = Regex::new("^[A-Za-z][-A-Za-z0-9+.]*:")
                .expect("invalid regex in source");
        }
        let cap = SCHEME_RE
            .captures(s)
            .ok_or_else(|| format_err!("cannot parse locator: {:?}", s))?;
        let scheme = &cap[0];

        // Select an appropriate locator type.
        let driver = find_driver(scheme)?;
        driver.parse(s)
    }
}

#[test]
fn locator_from_str_to_string_roundtrip() {
    let locators = vec![
        "bigquery:my_project:my_dataset.my_table",
        "bigquery-schema:dir/my_table.json",
        "csv:file.csv",
        "csv:dir/",
        "dbcrossbar-schema:file.json",
        "gs://example-bucket/tmp/",
        "postgres://localhost:5432/db#my_table",
        "postgres-sql:dir/my_table.sql",
        "s3://example/my-dir/",
    ];
    for locator in locators.into_iter() {
        let parsed: BoxLocator = locator.parse().unwrap();
        assert_eq!(parsed.to_string(), locator);
    }
}

bitflags! {
    /// What `Locator` features are supported by a given driver?
    pub struct LocatorFeatures: u8 {
        const SCHEMA = 0b0000_0001;
        const WRITE_SCHEMA = 0b0000_0010;
        const LOCAL_DATA = 0b0000_0100;
        const WRITE_LOCAL_DATA = 0b0000_1000;
    }
}

/// A collection of all the features supported by a given driver. This is
/// used to automatically verify whether the arguments passed to a driver
/// are actually supported.
#[derive(Debug, Copy, Clone)]
pub struct Features {
    pub locator: LocatorFeatures,
    pub write_schema_if_exists: IfExistsFeatures,
    pub source_args: SourceArgumentsFeatures,
    pub dest_args: DestinationArgumentsFeatures,
    pub dest_if_exists: IfExistsFeatures,
    pub(crate) _placeholder: (),
}

impl Features {
    /// Return the empty set of features.
    pub(crate) fn empty() -> Self {
        Features {
            locator: LocatorFeatures::empty(),
            write_schema_if_exists: IfExistsFeatures::empty(),
            source_args: SourceArgumentsFeatures::empty(),
            dest_args: DestinationArgumentsFeatures::empty(),
            dest_if_exists: IfExistsFeatures::empty(),
            _placeholder: (),
        }
    }
}

impl fmt::Display for Features {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.locator.contains(LocatorFeatures::SCHEMA) {
            writeln!(f, "- conv FROM")?;
        }
        if self.locator.contains(LocatorFeatures::WRITE_SCHEMA) {
            writeln!(f, "- conv TO:")?;
            writeln!(f, "  {}", self.write_schema_if_exists)?;
        }
        if self.locator.contains(LocatorFeatures::LOCAL_DATA) {
            writeln!(f, "- cp FROM:")?;
            if !self.source_args.is_empty() {
                writeln!(f, "  {}", self.source_args)?;
            }
        }
        if self.locator.contains(LocatorFeatures::WRITE_LOCAL_DATA) {
            writeln!(f, "- cp TO:")?;
            if !self.dest_args.is_empty() {
                writeln!(f, "  {}", self.dest_args)?;
            }
            writeln!(f, "  {}", self.dest_if_exists)?;
        }
        Ok(())
    }
}

/// Extra `Locator` methods that can only be called statically. These cannot
/// accessed via a `Box<Locator>`.
pub trait LocatorStatic: Locator + Clone + FromStr<Err = Error> + Sized {
    /// Return the "scheme" used to format this locator, e.g., `"postgres:"`.
    fn scheme() -> &'static str;

    /// Return a mask of `LocatorFeatures` supported by this `Locator` type.
    fn features() -> Features;
}

/// Interface to a locator driver. This exists because we Rust can't treat
/// classes as objects, the way Ruby can. Instead, what we do is take classes
/// that implement [`LocatorStatic`] and wrap them up in objects that implement
/// the `LocatorDriver` interface.
pub trait LocatorDriver: Send + Sync + 'static {
    /// Return the "scheme" used to format this locator, e.g., `"postgres:"`.
    fn scheme(&self) -> &str;

    /// The name of this driver. The same as [`LocatorDriver::schema`], but
    /// without the trailing `:`.
    fn name(&self) -> &str {
        let scheme = self.scheme();
        assert!(scheme.ends_with(':'));
        &scheme[..scheme.len() - 1]
    }

    /// The features supported by this driver.
    fn features(&self) -> Features;

    /// Parse a locator string and return a [`BoxLocator`].
    fn parse(&self, s: &str) -> Result<BoxLocator>;
}

/// A wrapper type which converts a [`LocatorStatic`] class into an
/// implementation of the [`LocatorDriver`] interface. This allows us to treat
/// Rust classes as run-time objects, the way we can in Ruby.
pub(crate) struct LocatorDriverWrapper<L> {
    _phantom: PhantomData<L>,
}

impl<L: LocatorStatic> LocatorDriverWrapper<L> {
    pub(crate) fn new() -> Self {
        LocatorDriverWrapper {
            _phantom: PhantomData,
        }
    }
}

impl<L: LocatorStatic> LocatorDriver for LocatorDriverWrapper<L> {
    fn scheme(&self) -> &str {
        L::scheme()
    }

    fn features(&self) -> Features {
        L::features()
    }

    fn parse(&self, s: &str) -> Result<BoxLocator> {
        Ok(Box::new(s.parse::<L>()?))
    }
}
