//! An FTDC archive: concatenated BSON documents, read in file order.
//!
//! An archive (`metrics.<utc>-<uniquifier>`, or `metrics.interim`) is not
//! one BSON document. It is a sequence of them. Every document begins with
//! the usual BSON length, a little-endian `int32` that counts itself. This
//! reader pulls that length, then exactly the remaining bytes, then parses
//! the document. A file that ends on a document boundary is complete. A
//! file that ends inside a length or inside a document is truncated.
//!
//! The `type` field says which kind of payload follows:
//!
//! * `0` — metadata, written when the file is opened. The payload is `doc`
//!   and describes the host and the collectors. There is normally one.
//! * `1` — a metric chunk. The payload is `data`, a BinData blob. This is
//!   the only type that carries the time series. See `decode`.
//! * `2` — a periodic metadata delta. MongoDB 7 and later write these for
//!   slow-changing values such as server parameters. The payload is `doc`
//!   plus a `count`. A metrics file usually has one of these before the
//!   first metric chunk. They are not samples.
//!
//! Each of those documents is a wrapper. `_id` is the time the server wrote
//! the document, and it is not inside `doc`. A type 2 wrapper also carries
//! `count`, the generation counter of the metadata delta encoder: `0` with
//! a full collector sample when tracking resets, then an incremented count
//! with a sparse diff. Replaying point-in-time parameters means merging
//! those diffs; the count is what says where the encoder reset. Keeping
//! only the inner `doc` drops both the time and the generation.
//!
//! [`Archive::open`] reads one file into a new value. A second call reads
//! a second file. There is no accumulator to append into.
//!
//! The server has grown this set before (`2` itself was the addition). A
//! type this build does not know is skipped and remembered, so metric
//! chunks that follow it are still decoded.
//!
//! Inflation and delta reconstruction have no cross-chunk dependency.
//! Sample 0 of chunk N+1 is still the sample after the last sample of
//! chunk N, so each blob is inflated, decoded, and appended before the
//! next blob is touched. The compressed blobs in a real archive are tens
//! of kilobytes; one pass keeps a single inflated buffer alive.

use crate::decode::{append_chunk, decode_one_chunk, inflate_owned};
use crate::errors::FileDecoderError;
#[cfg(feature = "parquet")]
use crate::export::write_parquet;
use crate::extract::MetricColumn;
use bson::{Bson, Document};
use indexmap::IndexMap;
use std::io::{BufReader, Read};
use std::path::Path;

/// A framed BSON document larger than this is rejected before the body
/// is allocated. 16 MiB is the BSON maximum document size. A corrupt
/// length near `i32::MAX` would otherwise reserve a multi-gigabyte buffer.
/// An FTDC wrapper is far smaller: the metric blob is zlib of at most
/// 10_000_000 uncompressed bytes.
const MAX_FRAMED_DOCUMENT: usize = 16 * 1024 * 1024;

/// `type` values on the BSON documents concatenated inside an FTDC archive.
const TYPE_METADATA: i32 = 0;
const TYPE_METRIC_CHUNK: i32 = 1;
const TYPE_METADATA_DELTA: i32 = 2;

/// One type 0 document: the wrapper timestamp and the collector payload.
///
/// `doc` is the embedded document the server stored under `doc`. `_id` stays
/// beside it. Startup metadata describes the host (`buildInfo`, `hostInfo`,
/// the collection window) and is not a row of the metric series.
#[derive(Debug, Clone)]
pub struct MetadataDocument {
    pub id: Option<Bson>,
    pub doc: Document,
}

/// One type 2 document: a periodic metadata delta, not a full snapshot.
///
/// The server's delta encoder compares the previous periodic sample with the
/// current one, one collector level deep. Unchanged collectors are omitted.
/// `count` is that encoder's generation, not a count of metric samples:
///
/// * `0` — tracking reset, and `doc` is a full sample
/// * `1`, `2`, ... — a sparse diff, and `count` is how many diffs have been
///   emitted since the reset
///
/// Reconstructing the parameters at a later chunk means merging `doc` values
/// in order, field by field inside each collector. The server does not do
/// that merge when it writes the file. The archive keeps the diffs, the
/// generation, and the wrapper `_id` so a caller can.
#[derive(Debug, Clone)]
pub struct MetadataDelta {
    pub id: Option<Bson>,
    pub count: i64,
    pub doc: Document,
}

/// One type 1 document, still compressed.
///
/// `id` is the wrapper `_id`: the time the chunk was opened. The reference
/// sample inside the blob is the first row of the chunk, and this timestamp
/// is the chunk's own clock. `bytes` is the BinData payload, whose first four
/// bytes are the uncompressed size. See `decode`.
#[derive(Debug, Clone)]
pub struct MetricChunk {
    pub id: Option<Bson>,
    pub bytes: Vec<u8>,
}

/// One FTDC archive, read once.
///
/// The compressed metric blobs stay here until [`Archive::decode`] inflates
/// them. Metadata documents keep their wrapper fields so a later export can
/// write `_id` and `count` without parsing `Document`'s `Display` text.
#[derive(Debug, Clone)]
pub struct Archive {
    /// Type 0 documents, in file order.
    pub metadata: Vec<MetadataDocument>,
    /// Type 2 documents, in file order.
    ///
    /// The old field for this was a single string named `parameters`. Type 2
    /// is a metadata delta. Several of them can appear in one file; keeping
    /// only the last one drops the earlier diffs, and with them the ability
    /// to see when a parameter changed.
    pub metadata_deltas: Vec<MetadataDelta>,
    /// BSON documents visited while reading, of every type, including
    /// types that were skipped.
    pub document_count: usize,
    /// `type` numbers this build did not recognise. Reading continued.
    pub skipped_chunk_types: Vec<i32>,
    /// Type 1 chunks, still compressed, in file order.
    pub chunks: Vec<MetricChunk>,
}

/// Read the next BSON document from a concatenation.
///
/// `Ok(None)` is a clean end: the read of the length returned zero bytes.
/// A short length, a length below 5 (the int32 itself plus the trailing
/// NUL; this also rejects the negative numbers a corrupt file can spell),
/// or a document body that ends early is reported. The length is what
/// sizes the buffer, which is how a BSON reader consumes a framing stream.
fn read_length_prefixed_document<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<Document>, FileDecoderError> {
    let mut len_buf = [0u8; 4];
    let mut filled = 0usize;
    while filled < 4 {
        match reader.read(&mut len_buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => {
                return Err(FileDecoderError::truncated(format!(
                    "FTDC archive ends in the middle of a BSON document length ({filled} of 4 bytes)"
                )));
            }
            Ok(n) => filled += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(FileDecoderError::io("failed to read", path, error));
            }
        }
    }

    let doc_len = i32::from_le_bytes(len_buf);
    if doc_len < 5 {
        return Err(FileDecoderError::truncated(format!(
            "BSON document length {doc_len} is smaller than 5"
        )));
    }
    let doc_len = doc_len as usize;
    if doc_len > MAX_FRAMED_DOCUMENT {
        return Err(FileDecoderError::size_limit(format!(
            "BSON document length {doc_len} exceeds the {MAX_FRAMED_DOCUMENT} byte limit"
        )));
    }

    let mut body = vec![0u8; doc_len];
    body[..4].copy_from_slice(&len_buf);
    if let Err(error) = reader.read_exact(&mut body[4..]) {
        return Err(match error.kind() {
            std::io::ErrorKind::UnexpectedEof => FileDecoderError::truncated(format!(
                "FTDC archive ends in the middle of a BSON document of {doc_len} bytes"
            )),
            _ => FileDecoderError::io("failed to read", path, error),
        });
    }

    let mut cursor = body.as_slice();
    Document::from_reader(&mut cursor)
        .map(Some)
        .map_err(|error| {
            FileDecoderError::format(format!(
                "FTDC archive contains a BSON document that does not parse: {error}"
            ))
        })
}

impl Archive {
    /// Read one archive. A second call reads another file; it does not
    /// append to this value.
    pub fn open(file_name: impl AsRef<Path>) -> Result<Archive, FileDecoderError> {
        let file_name = file_name.as_ref();
        let file = match std::fs::File::open(file_name) {
            Ok(file) => file,
            Err(error) => return Err(FileDecoderError::io("failed to open", file_name, error)),
        };
        // The archive is read front to back, one document at a time. The
        // buffer only cuts down the number of reads; each document's own
        // length is what decides how many bytes belong to it.
        let mut reader = BufReader::new(file);
        let mut archive = Archive {
            metadata: Vec::new(),
            metadata_deltas: Vec::new(),
            document_count: 0,
            skipped_chunk_types: Vec::new(),
            chunks: Vec::new(),
        };

        loop {
            match read_length_prefixed_document(&mut reader, file_name)? {
                None => break,
                Some(metrics) => {
                    // Count this document even when we skip it, so
                    // `document_count` describes the file rather than
                    // only the documents we knew how to use.
                    archive.document_count += 1;
                    let id = metrics.get("_id").cloned();
                    match chunk_type_of(&metrics)? {
                        TYPE_METADATA => {
                            archive.metadata.push(MetadataDocument {
                                id,
                                doc: embedded_doc(&metrics, "doc", "metadata")?,
                            });
                        }
                        TYPE_METRIC_CHUNK => {
                            archive.chunks.push(MetricChunk {
                                id,
                                bytes: metric_blob(&metrics)?,
                            });
                        }
                        TYPE_METADATA_DELTA => {
                            // Keep every delta, with the generation the
                            // encoder stored in `count`. Replacing the
                            // previous value would leave only the last diff.
                            archive.metadata_deltas.push(MetadataDelta {
                                id,
                                count: metadata_count(&metrics)?,
                                doc: embedded_doc(&metrics, "doc", "metadata delta")?,
                            });
                        }
                        other => {
                            // Do not abort. The metric chunks after an
                            // unknown type are still valid samples, and
                            // failing here is how a whole archive used
                            // to be discarded because of one document.
                            log::warn!(
                                "skipping unrecognised FTDC chunk type {other}; metric chunks around it are still read"
                            );
                            archive.skipped_chunk_types.push(other);
                        }
                    }
                }
            }
        }

        Ok(archive)
    }

    /// Decode every metric chunk and append the series in file order.
    ///
    /// One chunk is inflated, reconstructed, and appended before the next
    /// chunk is read. A metric's samples are the concatenation of its
    /// samples from chunk 0, then chunk 1, then chunk 2. Doing that on the
    /// calling thread is what keeps the concatenation in time order.
    ///
    /// `keep` selects names. A rejected metric is still walked, because
    /// the delta block is positional, and its samples are not stored. The
    /// chunk still advances the row cursor by `sample_count + 1`, so a
    /// metric that appears in a later chunk is null for the rows an
    /// earlier chunk contributed. Pass `|_| true` to keep every metric.
    ///
    /// Metric names come back in the order they were first seen. That is
    /// the reference document's field order, then metrics introduced by
    /// later chunks.
    pub fn decode(
        &self,
        mut keep: impl FnMut(&str) -> bool,
    ) -> Result<IndexMap<String, MetricColumn>, FileDecoderError> {
        let mut merged: IndexMap<String, MetricColumn> = IndexMap::new();
        // How many samples the chunks already appended account for.
        // A metric that first appears later is null for these rows.
        let mut rows = 0usize;
        for chunk in &self.chunks {
            let inflated = inflate_owned(&chunk.bytes)?;
            let (height, columns) = decode_one_chunk(inflated, &mut keep)?;
            append_chunk(&mut merged, &mut rows, columns, height)?;
        }
        Ok(merged)
    }

    /// Write the metadata sidecar for `metrics`.
    ///
    /// `metrics` and `metricTypes` follow the column order. `_id` is relaxed
    /// extended JSON. `count` on a delta is the encoder generation. This
    /// does not write Parquet and does not need the `parquet` feature.
    pub fn write_sidecar(
        &self,
        metrics: &IndexMap<String, MetricColumn>,
        path: impl AsRef<Path>,
    ) -> Result<(), FileDecoderError> {
        let path = path.as_ref();
        let mut body = self.sidecar_preamble();
        let Some(object) = body.as_object_mut() else {
            return Err(FileDecoderError::format(format!(
                "metadata sidecar preamble is not a JSON object"
            )));
        };
        let names: Vec<&str> = metrics.keys().map(String::as_str).collect();
        let kinds: serde_json::Map<String, serde_json::Value> = metrics
            .iter()
            .map(|(name, column)| {
                (
                    name.clone(),
                    serde_json::Value::String(column.kind.as_str().to_string()),
                )
            })
            .collect();
        object.insert("metrics".to_string(), serde_json::json!(names));
        object.insert("metricTypes".to_string(), serde_json::Value::Object(kinds));
        let json = serde_json::to_string_pretty(&body).map_err(|error| {
            FileDecoderError::format(format!("failed to serialize metadata to JSON: {error}"))
        })?;
        std::fs::write(path, json)
            .map_err(|error| FileDecoderError::io("failed to write", path, error))
    }

    /// Build a DataFrame whose columns are the decoded series.
    ///
    /// Available with the `parquet` feature, which is on by default.
    #[cfg(feature = "parquet")]
    pub fn metrics_to_dataframe(
        metrics: IndexMap<String, MetricColumn>,
    ) -> Result<polars::prelude::DataFrame, FileDecoderError> {
        crate::export::metrics_to_dataframe(metrics)
    }

    /// Decode an FTDC file and write a Parquet file.
    ///
    /// `metadata`, when set, is the path of the sidecar. It receives every
    /// type 0 wrapper and every type 2 wrapper: `_id`, and for a delta the
    /// generation `count`, plus the embedded `doc` as relaxed extended JSON.
    /// `keep` selects metric names; `|_| true` keeps every column.
    ///
    /// Available with the `parquet` feature, which is on by default.
    #[cfg(feature = "parquet")]
    pub fn write_to_parquet(
        input_file_name: impl AsRef<Path>,
        output_file_name: impl AsRef<Path>,
        metadata: Option<&Path>,
        keep: impl FnMut(&str) -> bool,
    ) -> Result<(), FileDecoderError> {
        let archive = Archive::open(input_file_name)?;
        let metrics = archive.decode(keep)?;
        if let Some(path) = metadata {
            archive.write_sidecar(&metrics, path)?;
        }
        write_parquet(metrics, output_file_name.as_ref())
    }

    /// Sidecar object without the `metrics` list.
    ///
    /// The column names are inserted after the DataFrame exists, so they
    /// follow the same order as the Parquet file. `_id` is relaxed extended
    /// JSON (`$date` for the timestamp the server writes). `doc` is the
    /// embedded collector document, also relaxed extended JSON.
    pub(crate) fn sidecar_preamble(&self) -> serde_json::Value {
        serde_json::json!({
            "metadata": self.metadata.iter().map(metadata_json).collect::<Vec<_>>(),
            "metadataDeltas": self.metadata_deltas.iter().map(delta_json).collect::<Vec<_>>(),
            "documentCount": self.document_count,
            "skippedChunkTypes": self.skipped_chunk_types,
        })
    }
}

fn relaxed_extended_json(value: &Bson) -> serde_json::Value {
    value.clone().into_relaxed_extjson()
}

fn id_json(id: &Option<Bson>) -> serde_json::Value {
    match id {
        Some(value) => relaxed_extended_json(value),
        None => serde_json::Value::Null,
    }
}

fn metadata_json(document: &MetadataDocument) -> serde_json::Value {
    serde_json::json!({
        "_id": id_json(&document.id),
        "doc": relaxed_extended_json(&Bson::Document(document.doc.clone())),
    })
}

fn delta_json(delta: &MetadataDelta) -> serde_json::Value {
    serde_json::json!({
        "_id": id_json(&delta.id),
        "count": delta.count,
        "doc": relaxed_extended_json(&Bson::Document(delta.doc.clone())),
    })
}

/// Read the chunk `type` without panicking when it is missing or not an int.
///
/// The server writes it with `appendNumber`, which is an int32 for these
/// small values. int64 is accepted too, because a permissive writer (and
/// some round-trips through extended JSON) will widen it.
fn chunk_type_of(doc: &Document) -> Result<i32, FileDecoderError> {
    match doc.get("type") {
        Some(Bson::Int32(value)) => Ok(*value),
        Some(Bson::Int64(value)) => i32::try_from(*value).map_err(|_| {
            FileDecoderError::chunk_type(format!(
                "FTDC chunk type {value} does not fit in an int32"
            ))
        }),
        Some(other) => Err(FileDecoderError::chunk_type(format!(
            "FTDC chunk type is {other:?}, expected an int"
        ))),
        None => Err(FileDecoderError::chunk_type(format!(
            "'type' key not found in chunk"
        ))),
    }
}

/// The generation counter on a type 2 wrapper.
///
/// The server writes it with `appendNumber`. int32 is the usual width;
/// int64 is accepted for the same reason as `type`. A type 2 document
/// with no `count` is not a metadata delta this decoder can place in the
/// encoder's sequence.
fn metadata_count(doc: &Document) -> Result<i64, FileDecoderError> {
    match doc.get("count") {
        Some(Bson::Int32(value)) => Ok(i64::from(*value)),
        Some(Bson::Int64(value)) => Ok(*value),
        Some(other) => Err(FileDecoderError::format(format!(
            "'count' in an FTDC metadata delta is {other:?}, expected an int"
        ))),
        None => Err(FileDecoderError::format(format!(
            "'count' key not found in an FTDC metadata delta"
        ))),
    }
}

/// Take the embedded document stored under `key`.
///
/// Type 0 and type 2 both keep their payload in `doc`. A chunk that claims
/// to be one of those and has no document is corrupt; unlike an unknown
/// type number, there is nothing sensible to skip to.
fn embedded_doc(doc: &Document, key: &str, kind: &str) -> Result<Document, FileDecoderError> {
    match doc.get(key) {
        Some(Bson::Document(inner)) => Ok(inner.clone()),
        Some(other) => Err(FileDecoderError::format(format!(
            "'{key}' in an FTDC {kind} chunk is {other:?}, expected a document"
        ))),
        None => Err(FileDecoderError::format(format!(
            "'{key}' key not found in an FTDC {kind} chunk"
        ))),
    }
}

/// Take the compressed blob from a type 1 chunk.
///
/// A metric chunk whose `data` is not BinData used to be ignored, which
/// deleted a block of samples and shifted every later timestamp. That is a
/// broken file, so it is reported.
fn metric_blob(doc: &Document) -> Result<Vec<u8>, FileDecoderError> {
    match doc.get("data") {
        Some(Bson::Binary(blob)) => Ok(blob.bytes.clone()),
        Some(other) => Err(FileDecoderError::format(format!(
            "'data' in an FTDC metric chunk is {other:?}, expected BinData"
        ))),
        None => Err(FileDecoderError::format(format!(
            "'data' key not found in an FTDC metric chunk"
        ))),
    }
}
