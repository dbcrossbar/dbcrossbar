# How it works

`dbcrossbar` uses pluggable input and output drivers, allowing any input to be copied to any output:

```dot process
digraph {
    rankdir="LR";

    csv_in [label="CSV"]
    csv_out [label="CSV"]
    csv_in -> dbcrossbar -> csv_out

    postgres_in [label="PostgreSQL"]
    postgres_out [label="PostgreSQL"]
    postgres_in -> dbcrossbar -> postgres_out

    bigquery_in [label="BigQuery"]
    bigquery_out [label="BigQuery"]
    bigquery_in -> dbcrossbar -> bigquery_out

    s3_in [label="S3"]
    s3_out [label="S3"]
    s3_in -> dbcrossbar -> s3_out

    etc_in [label="..."]
    etc_out [label="..."]
    etc_in -> dbcrossbar -> etc_out
}

```

## Parallel data streams

Internally, `dbcrossbar` uses parallel data streams. If we copy `s3://example/` to `csv:out/` using `--max-streams=4`, this will run up to 4 copies in parallel:

```dot process
digraph {
    rankdir="LR";

    src1 [label="s3://example/file_1.csv"]
    dest1 [label="csv:out/file_1.csv"]
    src1 -> dest1

    src2 [label="s3://example/file_2.csv"]
    dest2 [label="csv:out/file_2.csv"]
    src2 -> dest2

    src3 [label="s3://example/file_3.csv"]
    dest3 [label="csv:out/file_3.csv"]
    src3 -> dest3

    dest4 [label="csv:out/file_4.csv"]
    src4 [label="s3://example/file_4.csv"]
    src4 -> dest4

    {
        rank=same;
        rankdir="TB";
        src1 -> src2 -> src3 -> src4 [style="invis"]
    }
}
```

As soon as one stream finishes, a new one will be started:

```dot process
digraph {
    rankdir="LR";

    src5 [label="s3://example/file_5.csv"]
    dest5 [label="csv:out/file_5.csv"]
    src5 -> dest5
}
```

`dbcrossbar` accomplishes this using a **stream of CSV streams.** This allows us to make extensive use of [backpressure](https://ferd.ca/queues-don-t-fix-overload.html) to control how data flows through the system, eliminating the need for temporary files. This makes it easier to work with 100GB+ CSV files and 1TB+ datasets.

## Shortcuts

When copying between certain drivers, `dbcrossbar` supports "shortcuts." For example, it can load data directly from Google Cloud Storage into BigQuery.

## Multi-threaded, asynchronous Rust

`dbcrossbar` is written using [asynchronous](https://rust-lang.github.io/async-book/) [Rust](https://www.rust-lang.org/), and it makes heavy use of a multi-threaded worker pool. Internally, it works something like a set of classic Unix pipelines running in parallel. Thanks to Rust, it bas been possible to get native performance and multithreading without spending too much time debugging.
