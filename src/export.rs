//! Parquet export of decoded FTDC series.
//!
//! Each metric is a column of `u64` values. A missing sample is `None`,
//! which Parquet stores as a null. Many FTDC counters sit at 0 for long
//! stretches, so a filler zero cannot be told apart from a real sample.
//! Columns are written in the map's iteration order. That order is the
//! order names were first seen, which is the reference document's field
//! order.
//! Columns of one file share one height. A map whose columns differ in
//! length is rejected. The merge in `decode` is what inserts the nulls
//! on the side where the metric was absent.
//!
//! The sidecar is relaxed extended JSON. `metadata` is every type 0
//! wrapper (`_id` and `doc`). `metadataDeltas` is every type 2 wrapper
//! (`_id`, `count`, and `doc`), in file order. Dates, ObjectIds, and
//! timestamps keep their extended-JSON forms (`$date`, `$oid`,
//! `$timestamp`). Integers and strings stay JSON values. `count` is the
//! delta encoder's generation, not a sample index. An earlier sidecar kept
//! only the inner `doc`, and before that only the last document of each
//! kind, rendered with `Document`'s `Display`. `metricTypes` names the
//! compressor rule for each column. The Parquet values stay `u64`.

use crate::errors::FileDecoderError;
use crate::extract::MetricColumn;
use indexmap::IndexMap;
use polars::prelude::*;
use std::path::Path;

/// Build a DataFrame whose columns are the decoded series.
///
/// A missing sample is `None`. Callers align the series first
/// ([`crate::decode::append_chunk`] does this while merging chunks).
pub(crate) fn metrics_to_dataframe(
    metrics: IndexMap<String, MetricColumn>,
) -> Result<DataFrame, FileDecoderError> {
    let mut series_vec: Vec<Series> = Vec::new();

    let height = metrics
        .values()
        .map(|column| column.values.len())
        .max()
        .unwrap_or(0);
    for (key, column) in &metrics {
        if column.values.len() != height {
            return Err(FileDecoderError::ragged(format!(
                "series '{key}' has {} samples but another series has {height}. \
                 A sample that did not exist is null, not 0, and it has to be \
                 inserted on the side where the metric was absent.",
                column.values.len()
            )));
        }
    }

    for (key, column) in metrics {
        let series = Series::new(key.into(), column.values);
        series_vec.push(series);
    }

    // DataFrame::from_iter returns a DataFrame directly, not a Result.
    Ok(DataFrame::from_iter(series_vec))
}

/// Write `metrics` to `output_file_name`.
///
/// The sidecar is written by [`crate::archive::Archive::write_sidecar`],
/// which does not depend on Polars. Column order is the map's iteration
/// order, the order names were first seen.
pub(crate) fn write_parquet(
    metrics: IndexMap<String, MetricColumn>,
    output_file_name: &Path,
) -> Result<(), FileDecoderError> {
    let mut df = metrics_to_dataframe(metrics)?;

    let mut file = std::fs::File::create(output_file_name)
        .map_err(|error| FileDecoderError::io("failed to create", output_file_name, error))?;

    ParquetWriter::new(&mut file)
        .finish(&mut df)
        .map_err(|error| {
            FileDecoderError::format(format!(
                "failed to write Parquet {}: {error}",
                output_file_name.display()
            ))
        })?;

    Ok(())
}
