//! One inflated metric chunk, and the series built by appending chunks.
//!
//! A type 1 document stores its payload in `data` as BinData. The first four
//! bytes of that blob are a little-endian `uint32`, the uncompressed size
//! (`metrics_chunk.uncompressed_size`). The rest is a zlib stream. Those four
//! bytes are not zlib bytes.
//!
//! After inflation the layout is:
//!
//! ```text
//! reference document   BSON. Its first 4 bytes are its own length.
//! metric_count         uint32. How many numbers the compressor extracted.
//! sample_count         uint32. How many deltas follow, per metric.
//! deltas               unsigned LEB128, column-major.
//! ```
//!
//! `sample_count` does not include the reference document. Each metric has
//! `sample_count + 1` samples: the reference value, then one reconstructed
//! value per delta. A chunk that only holds the reference (common at the
//! start of a file, and whenever the schema changes before another sample
//! is collected) has `sample_count` 0 and still has one sample.
//!
//! The delta block is column-major. Every delta of metric 0 comes first,
//! then every delta of metric 1. A delta is added to the previous sample
//! with wrapping `u64` addition, which is what the C++ compressor does
//! (`uint64_t`). A delta of `u64::MAX` takes a counter backwards by one.
//! Checked addition would reject those samples.
//!
//! Zeros are run-length encoded across the whole block, including across
//! the boundary between two metrics. A run of *k* zeros is the pair
//! `[0, k - 1]`: the `0` is the first zero, and the next varint is how
//! many further zeros follow. The counter is shared across metrics on
//! purpose. Resetting it at each metric would swallow a run the compressor
//! wrote as one pair.
//!
//! The server refuses a chunk whose `metric_count * sample_count` exceeds
//! 1_000_000, and an inflated payload larger than 10_000_000 bytes. The
//! product is checked before the sample vectors are allocated. The leading
//! `uint32` of the blob is that inflated length: the zlib output has to
//! match it, and a larger claim is rejected before inflate.
//!
//! `metric_count` is authoritative. The reference walk has to produce that
//! many metrics. The check happens before any delta is applied, and the
//! error names the first and last extracted field so a mismatched walk
//! (an ObjectId counted as a metric, an array element skipped) can be
//! seen directly.
//!
//! Chunks of one file have no data dependency on each other. This decoder
//! still inflates and appends them in file order, one at a time: the
//! compressed blobs in a real archive are tens of kilobytes, and the
//! sample order is the file order. A metric that first appears in a
//! later chunk is null for the rows already emitted. A metric that is
//! absent from a later chunk is null for that chunk's height. Every column
//! of one chunk has the same height, because one chunk is one rectangle of
//! `metric_count` columns by `sample_count + 1` rows. The server closes a
//! chunk when the number of metrics changes, so the next chunk is where a
//! new or dropped series shows up.

use crate::errors::FileDecoderError;
use crate::extract::{MetricColumn, MetricData, MetricKind};
use crate::varint::{RangeCursor, unpack};
use indexmap::IndexMap;
use zune_inflate::{DeflateDecoder, DeflateOptions};

/// The server refuses a chunk whose `metric_count * sample_count` exceeds this.
const MAX_METRIC_SAMPLES: usize = 1_000_000;

/// The server refuses an inflated metric chunk larger than this.
///
/// The blob's first `uint32` is that length. A claim above the cap is
/// rejected before zlib runs. The decoder's own output cap is the same
/// number, so a stream that expands past it stops there.
pub(crate) const MAX_UNCOMPRESSED_CHUNK: usize = 10_000_000;

/// Inflate one metric blob.
///
/// The first four bytes are the uncompressed size the server writes in
/// front of the zlib stream. A blob shorter than that is not a metric chunk.
pub(crate) fn inflate_owned(bytes: &[u8]) -> Result<Vec<u8>, FileDecoderError> {
    if bytes.len() < 4 {
        return Err(FileDecoderError::truncated(format!(
            "metric blob is shorter than the 4-byte uncompressed size"
        )));
    }
    let declared = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if declared > MAX_UNCOMPRESSED_CHUNK {
        return Err(FileDecoderError::size_limit(format!(
            "uncompressed size {declared} exceeds the FTDC limit of {MAX_UNCOMPRESSED_CHUNK}"
        )));
    }
    let options = DeflateOptions::default().set_limit(MAX_UNCOMPRESSED_CHUNK);
    let mut decoder = DeflateDecoder::new_with_options(&bytes[4..], options);
    let inflated = match decoder.decode_zlib() {
        Ok(encoded_data) => encoded_data,
        Err(error) => {
            return Err(FileDecoderError::inflate(format!(
                "zlib inflate of an FTDC metric chunk failed: {error}"
            )));
        }
    };
    if inflated.len() != declared {
        return Err(FileDecoderError::inflate(format!(
            "uncompressed size {declared} does not match the inflated length {}",
            inflated.len()
        )));
    }
    Ok(inflated)
}

/// Read the reference length, `metric_count`, and `sample_count`.
///
/// `data_size` is the BSON length of the reference document. The two counts
/// sit in the eight bytes immediately after it. A buffer that cannot hold
/// those eight bytes is truncated.
pub(crate) fn read_header(data: &[u8]) -> Result<(usize, usize, usize), FileDecoderError> {
    if data.len() < 4 {
        return Err(FileDecoderError::truncated(format!(
            "inflated metric chunk is shorter than a BSON length"
        )));
    }
    let data_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data_size < 5 || data.len() < data_size.saturating_add(8) {
        return Err(FileDecoderError::truncated(format!(
            "inflated metric chunk does not contain metric_count and sample_count"
        )));
    }
    let metric_count =
        u32::from_le_bytes(data[data_size..data_size + 4].try_into().unwrap()) as usize;
    let sample_count =
        u32::from_le_bytes(data[data_size + 4..data_size + 8].try_into().unwrap()) as usize;
    if metric_count.saturating_mul(sample_count) > MAX_METRIC_SAMPLES {
        return Err(FileDecoderError::size_limit(format!(
            "metric_count {metric_count} and sample_count {sample_count} exceed the FTDC limit"
        )));
    }
    Ok((data_size, metric_count, sample_count))
}

/// Rebuild every metric in one inflated chunk.
///
/// Returns the chunk height, `sample_count + 1`. Each stored
/// `MetricData::data` vector has that length. A metric rejected by `keep`
/// is still advanced through the delta block, and its samples are not
/// stored. A zero run or a trailing byte past that rectangle is an error.
pub(crate) fn extract_all_metrics_from_buffer(
    metric_data_vec: &mut Vec<MetricData>,
    buffer: &Vec<u8>,
    keep: &mut dyn FnMut(&str) -> bool,
) -> Result<usize, FileDecoderError> {
    let (doc_size, metric_count, delta_count) = read_header(buffer.as_slice())?;

    // The delta block has no names. Column i of the block belongs to
    // metric i of this walk. The server checks the same equality and
    // refuses the chunk when it fails. Doing it here, before any
    // addition, means a bad walk cannot silently shift the series.
    if metric_data_vec.len() != metric_count {
        let first = metric_data_vec
            .first()
            .map(|metric| metric.name.as_str())
            .unwrap_or("(none)");
        let last = metric_data_vec
            .last()
            .map(|metric| metric.name.as_str())
            .unwrap_or("(none)");
        return Err(FileDecoderError::MetricCount {
            extracted: metric_data_vec.len(),
            header: metric_count,
            first: first.to_string(),
            last: last.to_string(),
        });
    }

    let offset = doc_size + 8;
    if offset > buffer.len() {
        return Err(FileDecoderError::truncated(format!(
            "metric chunk header points past the end of the inflated buffer"
        )));
    }
    let mut data_range_cursor = RangeCursor::new(&buffer[offset..]);

    // Additional zeros still to emit. This survives from one metric
    // to the next. See the module docs for the `[0, k - 1]` pair.
    let mut zeros_remaining: u64 = 0;
    for metric in metric_data_vec.iter_mut() {
        let store = keep(&metric.name);
        let mut value = metric.initial;
        let mut samples = if store {
            Vec::with_capacity(delta_count + 1)
        } else {
            Vec::new()
        };
        if store {
            samples.push(value);
        }

        for _ in 0..delta_count {
            let delta = if zeros_remaining != 0 {
                zeros_remaining -= 1;
                0
            } else {
                let decoded = unpack(&mut data_range_cursor)?;
                if decoded == 0 {
                    zeros_remaining = unpack(&mut data_range_cursor)?;
                }
                decoded
            };

            // Wrapping add. A delta of u64::MAX takes a counter
            // backwards by one, which is how a value that decreased
            // is encoded. Checked add would fail those samples.
            value = value.wrapping_add(delta);
            if store {
                samples.push(value);
            }
        }

        // Length includes the reference sample. Callers that kept
        // only `delta_count` elements dropped the last sample of
        // every chunk, and dropped the only sample of a chunk whose
        // sample_count was 0. A filtered-out metric is still walked,
        // so its deltas are consumed, and then discarded.
        if store {
            metric.data = Some(samples);
        }
    }

    // The rectangle is metric_count columns by delta_count rows. A run
    // that still has zeros left, or a byte that was not read, is a delta
    // block that does not match that rectangle.
    if zeros_remaining != 0 {
        return Err(FileDecoderError::varint(format!(
            "delta block has {zeros_remaining} zeros past the last sample"
        )));
    }
    if data_range_cursor.remaining() != 0 {
        return Err(FileDecoderError::varint(format!(
            "delta block has {} trailing bytes",
            data_range_cursor.remaining()
        )));
    }

    Ok(delta_count + 1)
}

/// Reconstruct one inflated chunk as `(name, kind, samples)` triples, in
/// walk order.
///
/// The name is the path `extract` built. `kind` is the compressor rule
/// that produced the `u64` bits. `keep` runs once per name, while that
/// metric's deltas are consumed. A rejected metric is absent from the
/// vector. This function does not append to a shared map: the caller
/// appends, in chunk order, and the order of these triples is the order
/// those names are first recorded.
pub(crate) fn decode_one_chunk(
    chunk: Vec<u8>,
    keep: &mut dyn FnMut(&str) -> bool,
) -> Result<(usize, Vec<(String, MetricKind, Vec<u64>)>), FileDecoderError> {
    let mut metric_data = crate::extract::extract_metric_meta_data(&chunk)?;
    let height = extract_all_metrics_from_buffer(&mut metric_data, &chunk, keep)?;

    let mut columns = Vec::with_capacity(metric_data.len());
    for metric in metric_data {
        // `data` is set only when `keep` accepted the name. The deltas
        // were consumed either way, so a second call would see a FnMut
        // predicate twice.
        if let Some(samples) = metric.data {
            columns.push((metric.name, metric.kind, samples));
        }
    }
    Ok((height, columns))
}

/// Append one chunk's columns to the file-level series.
///
/// `rows` is the number of samples already stored. Every column of this
/// chunk has the same length, because one chunk is one rectangular
/// block: `sample_count + 1` rows and `metric_count` columns.
///
/// A name that was not in earlier chunks gets `rows` leading nulls, then
/// its values. Those nulls are "this metric did not exist yet", which is
/// what the server does when the schema changes and it closes the chunk.
/// Putting the new values at the start of the column, and filling the
/// tail with 0, lines the new metric up with the wrong time and looks
/// like a real zero.
///
/// A name that was in earlier chunks but is absent from this one gets
/// the same number of trailing nulls. The schema changed the other way:
/// the metric went away. A zero there would look like the counter reset.
///
/// Names stay in the order they were first inserted. That is the reference
/// document's field order for the first chunk, then any metric a later
/// chunk introduces, in that chunk's field order. Parquet columns follow
/// this order. A `HashMap` reshuffles them on every run.
pub(crate) fn append_chunk(
    merged: &mut IndexMap<String, MetricColumn>,
    rows: &mut usize,
    columns: Vec<(String, MetricKind, Vec<u64>)>,
    height: usize,
) -> Result<(), FileDecoderError> {
    // Indices of columns that already existed. A new name is appended, so
    // it is past this length and already has its leading nulls. Remembering
    // the index avoids cloning the name into a side set.
    let mut seen = vec![false; merged.len()];

    for (name, kind, samples) in columns {
        if samples.len() != height {
            return Err(FileDecoderError::ragged(format!(
                "metric '{name}' has {} samples in a chunk whose other metrics have {height}",
                samples.len()
            )));
        }
        if let Some(index) = merged.get_index_of(&name) {
            let (_, existing) = merged.get_index_mut(index).expect("index just found");
            // The kind is fixed by the chunk that introduced the name.
            // A later chunk that walks the same path as a different BSON
            // type would make the sidecar describe every row as the first
            // kind. The samples are still u64 either way; the mismatch
            // means the reference documents disagree about the field.
            if existing.kind != kind {
                return Err(FileDecoderError::format(format!(
                    "metric '{name}' is {} in an earlier chunk and {} in this chunk",
                    existing.kind.as_str(),
                    kind.as_str()
                )));
            }
            existing.values.extend(samples.into_iter().map(Some));
            seen[index] = true;
        } else {
            let mut values = Vec::with_capacity(*rows + height);
            values.resize(*rows, None);
            values.extend(samples.into_iter().map(Some));
            merged.insert(name, MetricColumn { kind, values });
        }
    }

    let end = *rows + height;
    for (index, (_, series)) in merged.iter_mut().enumerate() {
        if index < seen.len() && !seen[index] {
            series.values.resize(end, None);
        }
    }
    *rows = end;
    Ok(())
}
