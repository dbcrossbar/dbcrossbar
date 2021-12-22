//! Conversions between different formats.

use std::{cmp::min, fmt};

use immutable_chunkmap::set::SetM;
use stack_list::Node;
use tracing::{debug, instrument, trace};

use super::{
    cost::Cost,
    formats::{
        BacktrackIterator, BigMlResource, CompressionFormat, DataFormat, Parallelism,
        StorageFormat, StreamFormat, TransferFormat,
    },
};

/// Convertors than can operator on individual streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamConvertor {
    Gzip,
    Gunzip,
}

/// Conversions we can perform on individual streams.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamConversion {
    name: StreamConvertor,
    input: StreamFormat,
    output: StreamFormat,
}

impl StreamConversion {
    /// Given `input` as an input format, what conversions do we support?
    ///
    /// We use this to generate forward candidates in our search.
    fn conversions_from(
        input: StreamFormat,
    ) -> Box<dyn BacktrackIterator<Item = Self>> {
        let mut result = vec![];

        // Perform Prolog-style rule lookup by hand, and then perform manual
        // unification. There's a more beautiful way to do this, but it involves
        // generating `FormatPattern` types for each `Format` type, and then
        // building a rule lookup and unification engine.
        match input {
            StreamFormat::Compressed(data_format, CompressionFormat::Gz) => result
                .push(StreamConversion {
                    name: StreamConvertor::Gunzip,
                    input,
                    output: StreamFormat::Data(data_format),
                }),
            StreamFormat::Data(data_format) => result.push(StreamConversion {
                name: StreamConvertor::Gzip,
                input,
                output: StreamFormat::Compressed(data_format, CompressionFormat::Gz),
            }),
        }

        Box::new(result.into_iter())
    }

    /// Get the estimated cost of this conversion, in arbirary units.
    fn cost(&self) -> Cost {
        Cost::default()
    }
}

/// Convertors that operator on storage formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageConvertor {
    BigMlCreateDataset,
    BigMlCreateSource,
    BigMlGetSource,
    BigQueryExtract,
    BigQueryLoad,
    CsvStreamsCat,
    CsvStreamsUnit,
    FileRead,
    FileWrite,
    GsGet,
    GsPut,
    Map(StreamConvertor),
    PostgresCopyIn,
    PostgresCopyOut,
    S3Get,
    S3Put,
    ShopifyGet,
}

/// Conversions we can perform between different storage formats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageConversion {
    name: StorageConvertor,
    input: StorageFormat,
    output: StorageFormat,
}

impl StorageConversion {
    /// Given `input` as an input format, what conversions do we support?
    ///
    /// We use this to generate forward candidates in our search.
    fn conversions_from(
        input: &StorageFormat,
    ) -> Box<dyn BacktrackIterator<Item = Self>> {
        let mut result = vec![];

        match input {
            StorageFormat::BigMl(BigMlResource::NewDataset(_)) => {}
            StorageFormat::BigMl(BigMlResource::NewSource(_)) => {}
            StorageFormat::BigMl(BigMlResource::DatasetId) => {
                result.push(StorageConversion {
                    name: StorageConvertor::BigMlGetSource,
                    input: input.clone(),
                    output: StorageFormat::Streaming(TransferFormat {
                        parallelism: Parallelism::One,
                        stream_format: StreamFormat::Data(DataFormat::Csv),
                    }),
                });
            }
            StorageFormat::BigQuery => result.push(StorageConversion {
                name: StorageConvertor::BigQueryExtract,
                input: input.clone(),
                output: StorageFormat::Gs(TransferFormat {
                    parallelism: Parallelism::Many,
                    stream_format: StreamFormat::Data(DataFormat::Csv),
                }),
            }),
            StorageFormat::File(transfer_format) => result.push(StorageConversion {
                name: StorageConvertor::FileRead,
                input: input.clone(),
                output: StorageFormat::Streaming(transfer_format.to_owned()),
            }),
            StorageFormat::Gs(transfer_format) => {
                result.push(StorageConversion {
                    name: StorageConvertor::GsGet,
                    input: input.clone(),
                    output: StorageFormat::Streaming(transfer_format.to_owned()),
                });

                // BigML can read from Google Cloud Storage.
                if let StreamFormat::Data(DataFormat::Csv) =
                    transfer_format.stream_format
                {
                    result.push(StorageConversion {
                        name: StorageConvertor::BigQueryLoad,
                        input: input.clone(),
                        output: StorageFormat::BigQuery,
                    });
                    result.push(StorageConversion {
                        name: StorageConvertor::BigMlCreateSource,
                        input: input.clone(),
                        output: StorageFormat::BigMl(BigMlResource::NewSource(
                            transfer_format.parallelism,
                        )),
                    });
                    result.push(StorageConversion {
                        name: StorageConvertor::BigMlCreateDataset,
                        input: input.clone(),
                        output: StorageFormat::BigMl(BigMlResource::NewDataset(
                            transfer_format.parallelism,
                        )),
                    });
                }
            }
            StorageFormat::Postgres => result.push(StorageConversion {
                name: StorageConvertor::PostgresCopyOut,
                input: input.clone(),
                output: StorageFormat::Streaming(TransferFormat {
                    parallelism: Parallelism::One,
                    stream_format: StreamFormat::Data(DataFormat::Csv),
                }),
            }),
            StorageFormat::S3(transfer_format) => {
                result.push(StorageConversion {
                    name: StorageConvertor::S3Get,
                    input: input.clone(),
                    output: StorageFormat::Streaming(transfer_format.to_owned()),
                });

                // BigML can read from S3, too.
                if let StreamFormat::Data(DataFormat::Csv) =
                    transfer_format.stream_format
                {
                    result.push(StorageConversion {
                        name: StorageConvertor::BigMlCreateSource,
                        input: input.clone(),
                        output: StorageFormat::BigMl(BigMlResource::NewSource(
                            transfer_format.parallelism,
                        )),
                    });
                    result.push(StorageConversion {
                        name: StorageConvertor::BigMlCreateDataset,
                        input: input.clone(),
                        output: StorageFormat::BigMl(BigMlResource::NewDataset(
                            transfer_format.parallelism,
                        )),
                    });
                }
            }
            StorageFormat::Shopify => result.push(StorageConversion {
                name: StorageConvertor::ShopifyGet,
                input: input.clone(),
                output: StorageFormat::Streaming(TransferFormat {
                    parallelism: Parallelism::Many,
                    stream_format: StreamFormat::Data(DataFormat::Csv),
                }),
            }),
            StorageFormat::Streaming(transfer_format) => {
                // These storage formats can handle files and directories of
                // files.
                result.push(StorageConversion {
                    name: StorageConvertor::FileWrite,
                    input: input.to_owned(),
                    output: StorageFormat::File(transfer_format.to_owned()),
                });
                result.push(StorageConversion {
                    name: StorageConvertor::S3Put,
                    input: input.to_owned(),
                    output: StorageFormat::S3(transfer_format.to_owned()),
                });
                result.push(StorageConversion {
                    name: StorageConvertor::GsPut,
                    input: input.to_owned(),
                    output: StorageFormat::Gs(transfer_format.to_owned()),
                });

                // But we can also convert streams in memory!
                result.extend(
                    StreamConversion::conversions_from(transfer_format.stream_format)
                        .map(|stream_conversion| StorageConversion {
                            name: StorageConvertor::Map(stream_conversion.name),
                            input: input.to_owned(),
                            output: StorageFormat::Streaming(TransferFormat {
                                parallelism: transfer_format.parallelism,
                                stream_format: stream_conversion.output,
                            }),
                        }),
                );

                match transfer_format.parallelism {
                    Parallelism::One => {
                        if let StreamFormat::Data(DataFormat::Csv) =
                            transfer_format.stream_format
                        {
                            result.push(StorageConversion {
                                name: StorageConvertor::PostgresCopyIn,
                                input: input.to_owned(),
                                output: StorageFormat::Postgres,
                            });
                            result.push(StorageConversion {
                                name: StorageConvertor::CsvStreamsUnit,
                                input: input.clone(),
                                output: StorageFormat::Streaming(TransferFormat {
                                    parallelism: Parallelism::Many,
                                    stream_format: StreamFormat::Data(DataFormat::Csv),
                                }),
                            });
                        };
                    }
                    Parallelism::Many => {
                        if let StreamFormat::Data(DataFormat::Csv) =
                            transfer_format.stream_format
                        {
                            result.push(StorageConversion {
                                name: StorageConvertor::CsvStreamsCat,
                                input: input.clone(),
                                output: StorageFormat::Streaming(TransferFormat {
                                    parallelism: Parallelism::One,
                                    stream_format: StreamFormat::Data(DataFormat::Csv),
                                }),
                            })
                        }
                    }
                }
            }
        }

        Box::new(result.into_iter())
    }

    /// Get the estimated cost of this conversion, in arbirary units.
    fn cost(&self) -> Cost {
        let mut cost = Cost::default();

        let parallelism = min(self.input.parallelism(), self.output.parallelism());
        if parallelism == Parallelism::One {
            cost = cost.penalize_for_parallelism_one();
        }

        let is_local = self.input.is_local() || self.output.is_local();
        if is_local {
            cost = cost.penalize_for_local_data()
        }

        cost
    }
}

/// A path of `StorageConverion` values the form a complete conversion path.
///
/// This is a vey thin wrapper for us to hang methods off of, basically.
pub(crate) struct ConversionPath {
    cost: Cost,
    path: Vec<StorageConversion>,
}

impl ConversionPath {
    /// Construct a new conversion path.
    pub(crate) fn new(path: Vec<StorageConversion>) -> Self {
        assert!(!path.is_empty());
        let cost = path.iter().map(|conversion| conversion.cost()).sum();
        Self { cost, path }
    }

    /// Get the estimated cost of this conversion path, in arbirary units.
    pub(crate) fn cost(&self) -> Cost {
        self.cost
    }

    /// Return the shortest conversion paths between `input` and `output`.
    pub(crate) fn shortest_path(
        input: &StorageFormat,
        output: &StorageFormat,
    ) -> Option<ConversionPath> {
        // For now, just call the reference implementation and choose the cheapest.
        let shortest = Self::paths(input, output)
            .into_iter()
            .min_by_key(|p| p.cost());
        if let Some(shortest) = &shortest {
            debug!("SHORTEST {} › {}: {}", input, output, shortest);
        }
        shortest
    }

    /// Return all conversion paths between `input` and `output`.
    ///
    /// No path may contain the same intermediate format twice (except in the
    /// output position, to allow S3 -> stream -> S3).
    #[instrument(
        level = "debug",
        skip_all,
        fields(input = %input, output = %output),
    )]
    pub(crate) fn paths(
        input: &StorageFormat,
        output: &StorageFormat,
    ) -> Vec<ConversionPath> {
        assert!(input.supports_read());
        assert!(output.supports_write());

        let mut already_seen = SetM::default();
        already_seen = already_seen.insert(input).0;
        let current_candidate = Node::new();
        let mut out_paths = vec![];
        Self::paths_helper(
            input,
            output,
            &already_seen,
            &current_candidate,
            &mut out_paths,
        );

        // TODO: Find lowest-cost conversion.
        out_paths
    }

    /// Helper function to actually generate conversion paths, correctly but not
    /// necessarily quickly.
    ///
    /// This is the reference version of our path-finding algorithm. We probably
    /// want to implement a version based on [Dijkstra's_algorithm][dijkstra],
    /// or at least one which prunes candidates that exceed the current shortest
    /// path. If we do implement a better algorithm, we can keep this version as
    /// `#[cfg(test)]` and use `proptest` to verify that the faster algorithm
    /// matches this one.
    ///
    /// [dijkstra]: https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
    ///
    /// This is basically a hand-written Prolog predicate in Rust. We use a mix
    /// of recursion and immutable data structures to implement "backtracing"
    /// and to explore all possible solutions (without pruning).
    ///
    /// We use several neat tricks:
    ///
    /// - `already_seen` is an immutatable set. Inserting an item returns a
    ///   _new_ set without modifying the original. We use this to "roll back"
    /// - `current_candidate` is a linked list _stored on the stack_. Again,
    ///   this is immutable and makes it easy for us to back up.
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            input = %input,
            output = %output,
            current_candidate = %DisplayCurrentCandidate(current_candidate),
            out_paths.len = out_paths.len(),
        ),
    )]
    fn paths_helper(
        input: &StorageFormat,
        output: &StorageFormat,
        already_seen: &SetM<&StorageFormat>,
        current_candidate: &Node<&StorageConversion>,
        out_paths: &mut Vec<ConversionPath>,
    ) {
        assert!(input.supports_read());
        assert!(output.supports_write());

        for conversion in StorageConversion::conversions_from(input) {
            trace!(
                "trying {:?}({}): {}",
                conversion.name,
                conversion.input,
                conversion.output,
            );
            let current_candidate = current_candidate.prepend(&conversion);
            if &conversion.output == output {
                // Record this conversion. The steps will be in reverse order,
                // so we'll need to collect and reverse them.
                debug!("FOUND: {}", DisplayCurrentCandidate(&current_candidate));
                let mut candidate = Vec::with_capacity(current_candidate.len());
                candidate.extend(current_candidate.iter().cloned().cloned());
                candidate.reverse();
                out_paths.push(ConversionPath::new(candidate));
            } else if !conversion.output.supports_read() {
                // This isn't the final format, and we won't be able to read
                // back out of it, so give up.
                trace!("cannot read back out of {}", conversion.output);
            } else {
                let (already_seen, dup) = already_seen.insert(&conversion.output);
                if dup {
                    // We're seen this before, so we have nothing to do here.
                    // This guarantees that we always terminate.
                    trace!("can't reuse {}", conversion.output);
                } else {
                    Self::paths_helper(
                        &conversion.output,
                        output,
                        &already_seen,
                        &current_candidate,
                        out_paths,
                    );
                }
            }
        }
    }
}

impl<'a> fmt::Display for ConversionPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_conversion_path(self.path.iter(), f)?;
        write!(f, " (cost={})", self.cost())
    }
}

/// Wrapper implementing `Display` for a stack-based list representing a
/// conversion path.
struct DisplayCurrentCandidate<'a>(&'a Node<'a, &'a StorageConversion>);

impl<'a> fmt::Display for DisplayCurrentCandidate<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .reverse(|candidate| display_conversion_path(candidate.iter().cloned(), f))
    }
}

/// Helper function to display a conversion path represented as an iterator.
fn display_conversion_path<'a, I>(
    mut path: I,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result
where
    I: Iterator<Item = &'a StorageConversion> + 'a,
{
    match path.next() {
        None => write!(f, "∅")?,
        Some(first) => {
            write!(f, "{:?}", first.name)?;
            for conversion in path {
                write!(f, "›{:?}", conversion.name)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tracing_subscriber::{
        fmt::{format::FmtSpan, Subscriber},
        prelude::*,
        EnvFilter,
    };

    fn init_tracing() {
        let _ = Subscriber::builder()
            //.with_writer(std::io::stderr)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::from_default_env())
            .finish()
            .try_init();
    }

    prop_compose! {
        fn readable_storage_formats()(
            f in any::<StorageFormat>()
                .prop_filter(
                    "storage format must support read",
                    |f| f.supports_read(),
                )
        ) -> StorageFormat { f }
    }

    prop_compose! {
        fn writable_storage_formats()(
            f in any::<StorageFormat>()
                .prop_filter(
                    "storage format must support write",
                    |f| f.supports_write(),
                )
        ) -> StorageFormat { f }
    }

    proptest! {
        #[test]
        fn can_convert_from_storage(storage_format in readable_storage_formats()) {
            let conversions = StorageConversion::conversions_from(&storage_format);
            assert_ne!(conversions.count(), 0);
        }

        #[test]
        fn can_convert_between(
            input in readable_storage_formats(),
            output in writable_storage_formats(),
        ) {
            init_tracing();
            assert!(ConversionPath::shortest_path(&input, &output).is_some());
        }
    }
}
