//! Specify the location of data or a schema.

use lazy_static::lazy_static;
use regex::Regex;
use std::ffi::OsStr;
use std::path::Path;
use std::{fmt, marker::PhantomData, str::FromStr};

use crate::args::EnumSetExt;
use crate::common::*;
#[cfg(test)]
use crate::data_streams::DataFormat;
use crate::drivers::find_driver;

/// When called from the CLI, should we display a list of individual locators
/// for each data stream?
pub enum DisplayOutputLocators {
    /// Never display where we wrote the data. Used if we wrote the data to
    /// standard output.
    Never,
    /// Display where we wrote the data only if asked to do so.
    IfRequested,
    /// Display where we wrote the data unless asked otherwise.
    ByDefault,
}

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

    /// Get the locator scheme, e.g., `postgres:`. This is similar to
    /// `[LocatorStatic::scheme]`, but it can be called on a `&dyn Locator`.
    fn dyn_scheme(&self) -> &'static str;

    /// Return a table schema, if available.
    ///
    /// This takes `SourceArguments` so that it has access to `DriverArguments`
    /// for things like extra OAuth2 scopes. But it can't take
    /// `SharedArguments`, because we may need call `schema` to build
    /// `SharedArguments` in the first place.
    fn schema(
        &self,
        _ctx: Context,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        async { Ok(None) }.boxed()
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do. This doesn't take `SharedArguments` because we've never
    /// actually needed it.
    ///
    /// TODO: Note that we do _not_ use the `if_exists` field in
    /// `DestinationArguments` here. That is currently meant for use by the
    /// `write_local_data` and `write_remote_data` methods, and it is validated
    /// using `Features::dest_if_exists`. Our `if_exists` parameter would be
    /// validated by to `Features::write_schema_if_exists`. This may need to be
    /// looked at in more detail before we try to use `_dest_args` for anything
    /// serious.
    fn write_schema(
        &self,
        _ctx: Context,
        _schema: Schema,
        _if_exists: IfExists,
        _dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<()> {
        let err = format_err!("cannot write schema to {}", self);
        async move { Err(err) }.boxed()
    }

    /// Count the records specified by this locator.
    fn count(
        &self,
        _ctx: Context,
        _shared_args: SharedArguments<Unverified>,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        let err = format_err!("cannot count records at {}", self);
        async move { Err(err) }.boxed()
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

    /// Should we display the individual output locations?
    fn display_output_locators(&self) -> DisplayOutputLocators {
        DisplayOutputLocators::IfRequested
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
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
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
    ) -> BoxFuture<Vec<BoxLocator>> {
        let err = format_err!("cannot write_remote_data from source {}", source);
        async move { Err(err) }.boxed()
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

fn parse_locator(s: &str, enable_unstable: bool) -> Result<BoxLocator> {
    // Parse our locator into a URL-style scheme and the rest.
    lazy_static! {
        static ref SCHEME_RE: Regex =
            Regex::new("^[A-Za-z][-A-Za-z0-9+.]*:").expect("invalid regex in source");
    }
    let cap = SCHEME_RE
        .captures(s)
        .ok_or_else(|| format_err!("cannot parse locator: {:?}", s))?;
    let scheme = &cap[0];

    // Select an appropriate locator type.
    let driver = find_driver(scheme, enable_unstable)?;
    driver.parse(s)
}

#[test]
fn locator_from_str_to_string_roundtrip() {
    let locators = vec![
        "bigquery:my_project:my_dataset.my_table",
        "bigquery-schema:dir/my_table.json",
        "bigquery-test-fixture:my_project:my_dataset.my_table",
        "bigml:dataset",
        "bigml:datasets",
        "bigml:dataset/abc123",
        "bigml:source",
        "bigml:sources",
        "csv:file.csv",
        "csv:dir/",
        "dbcrossbar-schema:file.json",
        "dbcrossbar-ts:file %231 20%25.ts#Type",
        "file:dir/",
        "file:dir/file.csv",
        "file:dir/file.jsonl",
        "gs://example-bucket/tmp/",
        "postgres://localhost:5432/db#my_table",
        "postgres://user@host/db#public.table",
        "postgres-sql:dir/my_table.sql",
        "s3://example/my-dir/",
        "shopify://example.myshopify.com/admin/api/2020-04/orders.json",
        "trino://localhost:8080/catalog/schema#table",
        "trino-sql:dir/my_table.sql",
    ];
    for locator in locators.into_iter() {
        let parsed: BoxLocator = parse_locator(locator, true).unwrap();
        assert_eq!(parsed.to_string(), locator);
    }
}

pub(crate) trait PathLikeLocator {
    /// Return the path-like part of this locator, or `None`, if this locator
    /// points to something like stdin or stdout.
    ///
    /// This is used to compute the [`DataFormat`] of a locator. We use `OsStr`,
    /// because we may be working with `Path` values that are not valid UTF-8,
    /// and we'd like to keep as much information as possible, as long as
    /// possible. We _don't_ use `Path`, because that is intended for OS paths,
    /// and we may be working with path components of URLs.
    fn path(&self) -> Option<&OsStr>;

    /// Is this locator a directory-like path?
    fn is_directory_like(&self) -> bool {
        match self.path() {
            // `to_string_lossy` will replace invalid UTF-8 with `U+FFFD`, but
            // this won't affect the presence or absence of a trailing slash.
            Some(path) => path.to_string_lossy().ends_with('/'),
            None => false,
        }
    }

    /// The extension of this locator, if any.
    fn extension(&self) -> Option<&OsStr> {
        let path = self.path()?;
        // We convert to a `Path` here for parsing convenience. This may be a
        // bit sketch on Windows, but we have lots of unit tests that should
        // hopefully catch any problems.
        let path = Path::new(path);
        path.extension()
    }

    /// The data format to use for this locator, if any.
    #[cfg(test)]
    fn data_format(&self) -> Option<DataFormat> {
        self.extension().map(DataFormat::from_extension)
    }
}

/// A locator which has not yet been parsed.
///
/// This is separate from `BoxLocator` because `BoxLocator` can only be parsed
/// once we have the `enable_unstable` flag.
#[derive(Clone, Debug)]
pub struct UnparsedLocator(String);

impl UnparsedLocator {
    /// Try to parse this locator.
    pub fn parse(&self, enable_unstable: bool) -> Result<BoxLocator> {
        parse_locator(&self.0, enable_unstable)
    }
}

impl FromStr for UnparsedLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(UnparsedLocator(s.to_owned()))
    }
}

#[derive(Debug, EnumSetType)]
/// What `Locator` features are supported by a given driver?
pub enum LocatorFeatures {
    Schema,
    WriteSchema,
    LocalData,
    WriteLocalData,
    Count,
}

/// A collection of all the features supported by a given driver. This is
/// used to automatically verify whether the arguments passed to a driver
/// are actually supported.
#[derive(Debug, Copy, Clone)]
pub struct Features {
    pub locator: EnumSet<LocatorFeatures>,
    pub write_schema_if_exists: EnumSet<IfExistsFeatures>,
    pub source_args: EnumSet<SourceArgumentsFeatures>,
    pub dest_args: EnumSet<DestinationArgumentsFeatures>,
    pub dest_if_exists: EnumSet<IfExistsFeatures>,
    pub(crate) _placeholder: (),
}

impl Features {
    /// Return the empty set of features.
    pub(crate) fn empty() -> Self {
        Features {
            locator: EnumSet::empty(),
            write_schema_if_exists: EnumSet::empty(),
            source_args: EnumSet::empty(),
            dest_args: EnumSet::empty(),
            dest_if_exists: EnumSet::empty(),
            _placeholder: (),
        }
    }
}

impl fmt::Display for Features {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.locator.contains(LocatorFeatures::Schema) {
            writeln!(f, "- conv FROM")?;
        }
        if self.locator.contains(LocatorFeatures::WriteSchema) {
            writeln!(f, "- conv TO:")?;
            if !self.write_schema_if_exists.is_empty() {
                writeln!(f, "  {}", self.write_schema_if_exists.display())?;
            }
        }
        if self.locator.contains(LocatorFeatures::Count) {
            writeln!(f, "- count")?;
            if !self.source_args.is_empty() {
                writeln!(f, "  {}", self.source_args.display())?;
            }
        }
        if self.locator.contains(LocatorFeatures::LocalData) {
            writeln!(f, "- cp FROM:")?;
            if !self.source_args.is_empty() {
                writeln!(f, "  {}", self.source_args.display())?;
            }
        }
        if self.locator.contains(LocatorFeatures::WriteLocalData) {
            writeln!(f, "- cp TO:")?;
            if !self.dest_args.is_empty() {
                writeln!(f, "  {}", self.dest_args.display())?;
            }
            if !self.dest_if_exists.is_empty() {
                writeln!(f, "  {}", self.dest_if_exists.display())?;
            }
        }
        Ok(())
    }
}

/// Extra `Locator` methods that can only be called statically. These cannot
/// accessed via a `Box<Locator>`.
pub trait LocatorStatic: Locator + Clone + FromStr<Err = Error> + Sized {
    /// Convert this locator into a polymorphic `BoxLocator` on the heap.
    fn boxed(self) -> BoxLocator {
        Box::new(self)
    }

    /// Return the "scheme" used to format this locator, e.g., `"postgres:"`.
    fn scheme() -> &'static str;

    /// Return a mask of `LocatorFeatures` supported by this `Locator` type.
    fn features() -> Features;

    /// Is this driver unstable?
    fn is_unstable() -> bool {
        false
    }
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

    /// Is this driver unstable?
    fn is_unstable(&self) -> bool;

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

    /// Is this driver unstable?
    fn is_unstable(&self) -> bool {
        L::is_unstable()
    }

    fn parse(&self, s: &str) -> Result<BoxLocator> {
        Ok(Box::new(s.parse::<L>()?))
    }
}
