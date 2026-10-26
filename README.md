# ftdc-rs

Reads MongoDB FTDC diagnostic archives and converts them to Parquet.

An archive is a concatenation of BSON documents. Type 0 is the file
metadata, type 1 is a compressed metric chunk, and type 2 is a periodic
metadata delta. Each metric chunk rebuilds `sample_count + 1` samples
per metric: the reference document, then one sample per delta. A metric
that appears or disappears between chunks is null for the rows where it
did not exist.

```
Usage: ftdc-rs [OPTIONS] --path <PATH>

Options:
  -p, --path <PATH>                FTDC metrics file, or a directory of them
  -o, --output-path <OUTPUT_PATH>  Directory the Parquet files are written into [default: ./]
  -i, --info                       Print the size and modification time of each input
  -m, --metadata                   Also write <name>.metadata.json next to each Parquet file
  -f, --filter <FILTER>            Keep metrics whose name matches this regular expression
  -h, --help                       Print help
  -V, --version                    Print version
```

A directory is scanned for files whose names start with `metrics.`
(`metrics.interim` and `metrics.<utc>-<seq>`). Other names are left
alone, and the matches are processed in name order. A path that is not
a directory is that one file.

The process exits with a non-zero status when a file cannot be read or
written, when `--filter` is not a regular expression, or when a directory
contains no `metrics.*` files. A chunk type this build does not know is
skipped and reported at `warn` (the default log level). Set `RUST_LOG` to
raise or quiet that.

`--metadata` writes relaxed extended JSON. `metadata` is every type 0
wrapper (`_id` and `doc`) and `metadataDeltas` is every type 2 wrapper
(`_id`, `count`, and `doc`), in file order. `count` is the periodic
metadata encoder's generation: `0` is a full sample, and a later number
is a sparse diff. `documentCount` counts every BSON document in the
archive. `skippedChunkTypes` lists type numbers that were skipped.
`metrics` is the Parquet column names, in field order. `metricTypes`
names the compressor rule for each column (`int`, `double`, `decimal`,
`bool`, `date`, `timestampSeconds`, `timestampIncrement`). The column
values stay the `u64` bit patterns those rules produce.

`Archive::decode` takes a predicate, `|name| ...`, and passes every
name through it once while that metric's deltas are consumed. The
`ftdc-rs` binary turns `--filter` into that predicate. Polars is the
`parquet` feature, on by default. Without it the library still opens
an archive, decodes the columns, and writes the sidecar.

## Example

Decode the archives in `mongo-diag-0` into the current directory, and
write a metadata sidecar for each one:

```
ftdc-rs --path ./mongo-diag-0 --output-path ./ --metadata
Processing ./mongo-diag-0/metrics.2025-11-15T21-17-12Z-00000 -> ./metrics.2025-11-15T21-17-12Z-00000.parquet
Successfully written to ./metrics.2025-11-15T21-17-12Z-00000.parquet
Processing ./mongo-diag-0/metrics.2025-11-16T17-21-08Z-00000 -> ./metrics.2025-11-16T17-21-08Z-00000.parquet
Successfully written to ./metrics.2025-11-16T17-21-08Z-00000.parquet
Processing ./mongo-diag-0/metrics.interim -> ./metrics.interim.parquet
Successfully written to ./metrics.interim.parquet
```

The sidecar for the interim file is `./metrics.interim.metadata.json`.
