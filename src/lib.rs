extern crate core;
mod errors;

use crate::errors::FileDecoderError;
use anyhow::anyhow;
use bson::{Binary, Bson, Document};
use buffered_reader;
use buffered_reader::BufferedReader;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use zune_inflate::DeflateDecoder;
use zune_inflate::errors::{DecodeErrorStatus, InflateDecodeErrors};

/// `type` values on the BSON documents concatenated inside an FTDC archive.
///
/// An archive (`metrics.<utc>-<uniquifier>`, or `metrics.interim`) is not one
/// BSON document. It is a sequence of them. `bsondump` shows each one. The
/// `type` field says which kind of payload follows:
///
/// * `0` — metadata, written when the file is opened. The payload is `doc`
///   and describes the host and the collectors. There is normally one.
/// * `1` — a metric chunk. The payload is `data`, a BinData blob: a `uint32`
///   uncompressed length, then a zlib stream of deltas. This is the only
///   type that carries the time series.
/// * `2` — a periodic metadata delta. MongoDB 7 and later write these for
///   slow-changing values such as server parameters. The payload is `doc`
///   plus a `count`. A metrics file usually has one of these *before* the
///   first metric chunk. They are not samples and they are not a reason to
///   stop reading.
///
/// The server has grown this set before (`2` itself was the addition). A
/// type this build does not know is skipped and remembered, so metric chunks
/// that follow it are still decoded.
const TYPE_METADATA: i32 = 0;
const TYPE_METRIC_CHUNK: i32 = 1;
const TYPE_METADATA_DELTA: i32 = 2;

/// A struct that decodes an FTDC file.
///
/// The compressed metric blobs live here until [`FTDCFileDecoder::uncompress_data`]
/// inflates them. Metadata documents are kept whole so a later export can
/// write them as JSON without parsing `Document`'s `Display` text back apart.
pub struct FTDCFileDecoder {
    /// Type 0 `doc` sub-documents, in file order.
    metadata_docs: Vec<Document>,
    /// Type 2 `doc` sub-documents, in file order.
    ///
    /// The old field name for this was `parameters`. Type 2 is a metadata
    /// delta, not the startup parameter block. Several of them can appear
    /// in one file; keeping only the last one drops the earlier samples.
    metadata_deltas: Vec<Document>,
    /// BSON documents visited while reading, of every type, including
    /// types that were skipped.
    document_count: usize,
    /// `type` numbers this build did not recognise. Reading continued.
    skipped_chunk_types: Vec<i32>,
    /// Type 1 `data` blobs, still compressed, in file order.
    binary_chunks: Vec<Binary>,
}

#[derive(Debug)]
struct MetricData {
    name: String,
    initial: u64,
    metric_type: Bson,
    metrics: Option<usize>,
    data: Option<[u64; FTDCFileDecoder::MAX_METRICS]>,
}

struct RangeCursor {
    buffer: Vec<u8>,
    _size: usize,
    at: usize,
}

/// Unpacks varint deltas
///
/// # Arguments
///
/// * `data_range_cursor` - A structure that holds the decompressed deltas.
fn unpack(data_range_cursor: &mut RangeCursor) -> u64 {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if data_range_cursor.at >= data_range_cursor._size {
            break;
        }
        let b = data_range_cursor.buffer[data_range_cursor.at];
        data_range_cursor.at += 1;

        result |= ((b & 0x07f) as u64) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            return result;
        }
    }

    0
}

impl FTDCFileDecoder {
    pub const MAX_METRICS: usize = 300;

    pub const INITIAL_METRIC_CAPACITY: usize = 10240;

    /// Reads the info on the header of a decompressed data chunk.
    /// This is not very reliable for metric_count.
    fn read_header(data: &[u8]) -> (usize, usize, usize) {
        //  as *const u32;
        let data_size = u32::from_le_bytes(data[0..4].try_into().unwrap());

        let offset: usize = data_size as usize;
        let metric_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        let delta_count = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap());

        (
            data_size as usize,
            metric_count as usize,
            delta_count as usize,
        )
    }

    /// Inflates/uncompressed a zipped blob.
    ///
    /// # Arguments
    /// * `blob` - A binary BSON document.
    ///
    /// # Returns
    /// A vector of u8 values representing the decompressed blob.
    ///
    fn inflate_blob(blob: &Binary) -> Result<Vec<u8>, InflateDecodeErrors> {
        // Get blob
        let compressed_buffer = blob.bytes.as_slice();
        // Compressed buffer. Ignore first 4 bytes.
        let mut decoder = DeflateDecoder::new(&compressed_buffer[4..]);

        // Uncompressed data has metrics_count and delta_count in the header,
        // as u32.
        match decoder.decode_zlib() {
            Ok(encoded_data) => Ok(encoded_data),
            Err(e) => Err(e),
        }
    }

    /// Uncompresses all binary blobs - threaded uncompression
    ///
    /// # Arguments
    ///
    /// # Returns
    /// * A vector of vectors of u8, representing the uncompressed deltas block.
    pub fn uncompress_data(&self) -> Result<Vec<Vec<u8>>, InflateDecodeErrors> {
        let uncompressed_data_vector = Arc::new(Mutex::new(vec![]));
        let binary_chunks_x = Arc::new(self.binary_chunks.clone());

        let mut handles = vec![];

        for i in 0..binary_chunks_x.len() {
            let udv = Arc::clone(&uncompressed_data_vector);
            let bc = Arc::clone(&binary_chunks_x);

            let handle = thread::spawn(move || -> Result<i32, InflateDecodeErrors> {
                if let Some(binary_chunk) = bc.get(i) {
                    let inflated_data: Vec<u8> = FTDCFileDecoder::inflate_blob(binary_chunk)?;

                    let mut uncompressed_data_vector = udv.lock().unwrap();
                    uncompressed_data_vector.push(inflated_data);
                } else {
                    return Err(InflateDecodeErrors::new(
                        DecodeErrorStatus::InsufficientData,
                        vec![],
                    ));
                }

                Ok(0)
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join().unwrap();
        }

        Ok(uncompressed_data_vector.lock().unwrap().clone())
    }

    /// Reads sub-docs from the original BSON doc recursively.
    ///  Auxiliar to `extract_metric_meta_data`
    ///
    /// # Arguments:
    /// * `doc_name` - A str reference with the name of the parent BSON document.
    /// * `metric_data` - A mutable reference to a Vector of MetricData
    ///
    /// # Returns
    /// * Metric data structures are appended/pushed into the `metric_data` vector.
    fn extract_metric_data_from_doc(
        doc_name: &str,
        root: Document,
        metric_data: &mut Vec<MetricData>,
    ) {
        for (k, doc) in root {
            match doc {
                Bson::Double(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::String(_) => {}
                Bson::Array(arr) => {
                    let mut i = 0;
                    for a in arr {
                        let n = format!("{doc_name}.{}.{}", i, k.to_string());

                        match a {
                            Bson::Document(doc) => {
                                FTDCFileDecoder::extract_metric_data_from_doc(&n, doc, metric_data);
                            }
                            _ => {}
                        }

                        i += 1;
                    }
                }
                Bson::Document(sub_doc) => {
                    let n = format!("{doc_name}.{}", k.to_string());
                    FTDCFileDecoder::extract_metric_data_from_doc(&n, sub_doc, metric_data);
                }
                Bson::Boolean(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Null => {}
                Bson::RegularExpression(_) => {}
                Bson::JavaScriptCode(_) => {}
                Bson::JavaScriptCodeWithScope(_) => {}
                Bson::Int32(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Int64(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Timestamp(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}_time", k.to_string()),
                        initial: v.time as u64,
                        metric_type: doc.clone(),
                        metrics: None,
                        data: None,
                    });
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}_increment", k.to_string()),
                        initial: v.increment as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Binary(_) => {}
                Bson::ObjectId(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v.timestamp().timestamp_millis() as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::DateTime(v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v.timestamp_millis() as u64,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Symbol(_) => {}
                Bson::Decimal128(_v) => {
                    metric_data.push(MetricData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: 999999999,
                        metric_type: doc,
                        metrics: None,
                        data: None,
                    });
                }
                Bson::Undefined => {}
                Bson::MaxKey => {}
                Bson::MinKey => {}
                Bson::DbPointer(_) => {}
            }
        }
    }

    /// Extracts the names of the metrics and their count, as the data in the header is not reliable*
    ///
    /// # Arguments
    ///
    /// *  `buffer` - The decompressed data block, that is cast to d BSON Document.
    ///
    /// # Returns
    ///
    /// * A vector of `MetricData`
    ///
    fn extract_metric_meta_data(buffer: &Vec<u8>) -> Result<Vec<MetricData>, FileDecoderError> {
        let mut base = buffer.as_slice();

        match Document::from_reader(&mut base) {
            Ok(bson_doc) => {
                let mut ret_metrics: Vec<MetricData> = vec![];

                let root = bson_doc;

                FTDCFileDecoder::extract_metric_data_from_doc("", root, &mut ret_metrics);

                Ok(ret_metrics)
            }
            Err(e) => Err(FileDecoderError::Other(anyhow!(e))),
        }
    }

    /// Extract metric values from decompressed Metric data as varint deltas added to an initial value.
    ///
    /// # Arguments
    ///
    /// * - `metric_data_vec` -  A vector of MetricData where the uncompressed data is.
    ///
    /// # Returns
    ///
    ///  A count of how many values were found. The actual values are stored in the `metric_data_vec`
    fn extract_all_metrics_from_buffer<'a>(
        metric_data_vec: &mut Vec<MetricData>,
        buffer: &Vec<u8>,
    ) -> Result<usize, FileDecoderError> {
        let (doc_size, _, delta_count_from_header) =
            FTDCFileDecoder::read_header(buffer.as_slice());

        let offset = doc_size + 8;
        let data_range_size = buffer.len() - offset;

        let q = buffer.as_slice()[offset..].to_vec();
        let mut data_range_cursor = RangeCursor {
            buffer: q,
            _size: data_range_size,
            at: 0,
        };

        let mut count = 0;
        let mut num_zeroes: u32 = 0;
        for m in 0..metric_data_vec.len() {
            let mut unpacked_value = metric_data_vec.get(m).unwrap().initial;

            let _name = &metric_data_vec.get(m).unwrap().name;
            let _type = &metric_data_vec.get(m).unwrap().metric_type;
            //println!("{m}: name = {_name}  type ={_type:?}");

            let mut metric_values = [0u64; FTDCFileDecoder::MAX_METRICS];
            metric_values[0] = unpacked_value;
            for s in 1..FTDCFileDecoder::MAX_METRICS {
                metric_values[s] = 0;
            }

            for s in 0..delta_count_from_header {
                let delta: u64;
                if num_zeroes != 0 {
                    delta = 0;
                    num_zeroes -= 1;
                } else {
                    delta = unpack(&mut data_range_cursor);
                    if delta == 0 {
                        num_zeroes = unpack(&mut data_range_cursor) as u32;
                    }
                }

                // Handling overflow on the cheap. The following line works in C++
                // unpacked_value += delta;
                // when  112 + 18446744073709551615
                unpacked_value = (u128::from(unpacked_value) + u128::from(delta)) as u64;
                // But Rust is aware of overflow.
                metric_values[s + 1] = unpacked_value;

                count += 1;
            } //

            metric_data_vec.get_mut(m).unwrap().data = Option::from(metric_values);
            metric_data_vec.get_mut(m).unwrap().metrics = Option::from(delta_count_from_header);
        }

        Ok(count)
    }

    /// Creates a new structure to decode a single file
    ///
    /// # Arguments
    ///
    /// # Returns
    /// A clean `FTDCFileDecoder` structure.
    pub fn new() -> FTDCFileDecoder {
        FTDCFileDecoder {
            metadata_docs: Vec::new(),
            metadata_deltas: Vec::new(),
            document_count: 0,
            skipped_chunk_types: Vec::new(),
            binary_chunks: vec![],
        }
    }

    /// Reads an FTDC metrics file, storing the BSON documents in the structure.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The file to read
    ///
    /// # Returns
    ///
    /// * An empty ()
    ///
    pub fn read_file(&mut self, file_name: &str) -> Result<(), FileDecoderError> {
        // Open file, get buffed_reader
        match buffered_reader::File::open(file_name) {
            Ok(mut reader) => {
                loop {
                    // An FTDC file is a series of BSON documents.
                    // Read them in one by one
                    match Document::from_reader(&mut reader) {
                        // If it can be read...
                        Ok(metrics) => {
                            // Count this document even when we skip it, so
                            // `document_count` describes the file rather than
                            // only the documents we knew how to use.
                            self.document_count += 1;
                            match chunk_type_of(&metrics)? {
                                TYPE_METADATA => {
                                    self.metadata_docs
                                        .push(embedded_doc(&metrics, "doc", "metadata")?);
                                }
                                TYPE_METRIC_CHUNK => {
                                    self.binary_chunks.push(metric_blob(&metrics)?);
                                }
                                TYPE_METADATA_DELTA => {
                                    // Keep every delta. A file from MongoDB 7+
                                    // has one of these before the first metric
                                    // chunk, and a long-running process appends
                                    // more as parameters change. Replacing the
                                    // previous value would leave only the last.
                                    self.metadata_deltas.push(embedded_doc(
                                        &metrics,
                                        "doc",
                                        "metadata delta",
                                    )?);
                                }
                                other => {
                                    // Do not abort. The metric chunks after an
                                    // unknown type are still valid samples, and
                                    // failing here is how a whole archive used
                                    // to be discarded because of one document.
                                    log::warn!(
                                        "skipping unrecognised FTDC chunk type {other}; metric chunks around it are still read"
                                    );
                                    self.skipped_chunk_types.push(other);
                                }
                            }
                        }
                        Err(e) => {
                            if !reader.eof() {
                                return Err(FileDecoderError::Other(anyhow!(e)));
                            }
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                // IO errors
                return Err(FileDecoderError::FileIOError(e));
            }
        }

        Ok(())
    }

    /// Reads an FTDC file and decompresses the data bearing documents in it.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The name of the file to read.
    ///
    /// # Returns
    ///
    /// * A vector of vectors of U8 values.
    ///
    pub fn read_and_uncompress(
        &mut self,
        file_name: &str,
    ) -> Result<Vec<Vec<u8>>, FileDecoderError> {
        match self.read_file(file_name) {
            Ok(_) => match self.uncompress_data() {
                Ok(uncompressed_vec) => Ok(uncompressed_vec),
                Err(e) => Err(FileDecoderError::Other(anyhow!(e))),
            },
            Err(e) => {
                println!("Got expected error {e:?}");
                Err(e)
            }
        }
    }

    /// Decodes data in chunks that have been decompressed. Processing for each chunk is done
    /// in a separate thread.
    ///
    /// # Arguments
    ///
    /// * `data_chunks` - A vector of pre-decompressed chunks
    ///
    /// # Returns
    /// * A hashmap with metric names as String, and a vector of U64 values.
    ///
    pub fn decode_decompressed_data_chunks(
        &self,
        data_chunks: Vec<Vec<u8>>,
    ) -> Result<HashMap<String, Vec<u64>>, FileDecoderError> {
        let metric_data: HashMap<String, Vec<u64>> = HashMap::new();
        let metric_data_arc = Arc::new(Mutex::new(metric_data));
        let data_chunks_arc = Arc::new(data_chunks);

        let mut handles = vec![];

        for i in 0..data_chunks_arc.len() {
            let mdh = Arc::clone(&metric_data_arc);
            let dc = Arc::clone(&data_chunks_arc);

            let handle = thread::spawn(move || -> Result<i32, FileDecoderError> {
                if let Some(data_chunk) = dc.get(i) {
                    let mut metric_data = FTDCFileDecoder::extract_metric_meta_data(&data_chunk)?;
                    let _metric_data_size = FTDCFileDecoder::extract_all_metrics_from_buffer(
                        &mut metric_data,
                        &data_chunk,
                    )?;

                    for metric in metric_data {
                        if let Some(old_slice) = metric.data
                            && let Some(used_metrics) = metric.metrics
                        {
                            // remove first char, which is a dot.
                            let name = String::from(metric.name).split_off(1);

                            let mut out = mdh.lock().unwrap();
                            if out.contains_key(&name) {
                                if let Some(metric_member_data) = out.get_mut(&name) {
                                    // add slice to vector
                                    metric_member_data
                                        .append(&mut old_slice[0..used_metrics].to_vec());
                                } else {
                                    return Err(FileDecoderError::Other(anyhow!(
                                        "Could not find metric name '{name}'"
                                    )));
                                }
                            } else {
                                // insert new
                                let mut vec = Vec::with_capacity(Self::INITIAL_METRIC_CAPACITY);
                                vec.append(&mut old_slice[0..used_metrics].to_vec());
                                out.insert(name, vec);
                            }
                        } else {
                            return Err(FileDecoderError::Other(anyhow!(
                                "Could not find valid data in metrics: {metric:?}"
                            )));
                        }
                    }
                } else {
                    return Err(FileDecoderError::Other(anyhow!(
                        "Could not find data in metrics"
                    )));
                }

                Ok(0)
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join().unwrap();
        }

        Ok(metric_data_arc.lock().unwrap().clone())
    }

    /// Reads an FTDC file, uncompresses binary docs contained in it and decodes deltas.
    ///
    /// # Arguments
    ///
    /// * `file_name` -  The name of the file to decode.
    ///
    /// # Returns
    ///
    /// * A hashmap of the metric name and a vector of U64 values.
    ///
    pub fn read_and_decode(
        &mut self,
        file_name: &str,
        filter: Option<Regex>,
    ) -> Result<HashMap<String, Vec<u64>>, FileDecoderError> {
        let data_chunks = self.read_and_uncompress(file_name)?;
        let decoded_data = self.decode_decompressed_data_chunks(data_chunks)?;

        match filter {
            Some(re) => {
                let mut matched_data: HashMap<String, Vec<u64>> = HashMap::new();

                for (key, value) in decoded_data {
                    if re.is_match(&key) {
                        matched_data.insert(key, value);
                    }
                }
                Ok(matched_data)
            }
            None => Ok(decoded_data),
        }
    }

    /// Converts a HashMap<String, Vec<u64>> to a polars DataFrame
    ///
    /// # Arguments
    /// * `metrics` - A HashMap where keys are metric names and values are vectors of u64 values
    ///
    /// # Returns
    /// * `Result<DataFrame, FileDecoderError>` - A polars DataFrame with columns named after the keys in the hashmap
    pub fn metrics_to_dataframe(
        metrics: HashMap<String, Vec<u64>>,
    ) -> Result<DataFrame, FileDecoderError> {
        // Create a vector to hold our Series objects
        let mut series_vec: Vec<Series> = Vec::new();

        // Find the maximum length of any vector in the hashmap to determine row count
        let max_len = metrics.values().map(|v| v.len()).max().unwrap_or(0);

        // Create a Series for each key-value pair in the hashmap
        for (key, values) in metrics {
            // If the vector is shorter than max_len, pad it with zeros
            let mut padded_values = values.clone();
            if padded_values.len() < max_len {
                padded_values.resize(max_len, 0);
            }

            // Create a Series from the vector and add it to our vector of Series
            // Convert the key to PlSmallStr using .into() without the &
            let series = Series::new(key.into(), padded_values);
            series_vec.push(series);
        }

        // Create a DataFrame from the vector of Series
        // DataFrame::from_iter returns a DataFrame directly, not a Result
        Ok(DataFrame::from_iter(series_vec))
    }

    /// Decodes an FTDC file and writes a Parquet file with the information.
    ///
    /// # Arguments
    ///
    /// * `input_file_name` - The name of the FTDC file to decode.
    /// * `output_file_name` - The name of the file to write parquet data.
    /// * `metadata` - An Option<&str> that if not None will be used to write metadata.
    /// # Returns
    ///
    /// * `Result<(), FileDecoderError>` - Nothing
    pub fn write_to_parquet(
        input_file_name: &str,
        output_file_name: &str,
        metadata: Option<&str>,
        filter: Option<Regex>,
    ) -> Result<(), FileDecoderError> {
        let mut parsed_file = FTDCFileDecoder::new();

        let metrics = parsed_file.read_and_decode(input_file_name, filter)?;

        // Convert the metrics hashmap to a DataFrame
        let mut df = Self::metrics_to_dataframe(metrics)?;

        // Write the DataFrame to a parquet file
        let mut file = std::fs::File::create(output_file_name)
            .map_err(|e| FileDecoderError::Other(anyhow!("Failed to create output file: {}", e)))?;

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| FileDecoderError::Other(anyhow!("Failed to write parquet file: {}", e)))?;

        // Write metadata
        if let Some(metadata_file) = metadata {
            // Create entry
            let metric_names: Vec<String> = df
                .get_column_names()
                .iter()
                .map(move |name| -> String { name.to_string() })
                .collect();

            // Convert the Metadata struct to a JSON string
            // The sidecar still stores one string per group. The reader
            // keeps every document; the export that writes them as a JSON
            // array lands with the CLI changes. `Document`'s `Display` is
            // what the previous field stored, so the same text is kept here.
            let metadata = parsed_file
                .metadata_docs
                .last()
                .map(|doc| doc.to_string())
                .unwrap_or_default();
            let parameters = parsed_file
                .metadata_deltas
                .last()
                .map(|doc| doc.to_string())
                .unwrap_or_default();
            let json_data = serde_json::json!({
                "metadata": metadata,
                "parameters": parameters,
                "chunks_in_file": parsed_file.document_count,
                "metrics": metric_names,

            });

            // Write the JSON string to the specified file
            let json_string = serde_json::to_string_pretty(&json_data).map_err(|e| {
                FileDecoderError::Other(anyhow!("Failed to serialize metadata to JSON: {}", e))
            })?;

            std::fs::write(&metadata_file, json_string).map_err(|e| {
                FileDecoderError::Other(anyhow!("Failed to write metadata file: {}", e))
            })?;
        }

        Ok(())
    }
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
            FileDecoderError::Other(anyhow!("FTDC chunk type {value} does not fit in an int32"))
        }),
        Some(other) => Err(FileDecoderError::Other(anyhow!(
            "FTDC chunk type is {other:?}, expected an int"
        ))),
        None => Err(FileDecoderError::Other(anyhow!(
            "'type' key not found in chunk"
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
        Some(other) => Err(FileDecoderError::Other(anyhow!(
            "'{key}' in an FTDC {kind} chunk is {other:?}, expected a document"
        ))),
        None => Err(FileDecoderError::Other(anyhow!(
            "'{key}' key not found in an FTDC {kind} chunk"
        ))),
    }
}

/// Take the compressed blob from a type 1 chunk.
///
/// A metric chunk whose `data` is not BinData used to be ignored, which
/// deleted a block of samples and shifted every later timestamp. That is a
/// broken file, so it is reported.
fn metric_blob(doc: &Document) -> Result<Binary, FileDecoderError> {
    match doc.get("data") {
        Some(Bson::Binary(blob)) => Ok(blob.clone()),
        Some(other) => Err(FileDecoderError::Other(anyhow!(
            "'data' in an FTDC metric chunk is {other:?}, expected BinData"
        ))),
        None => Err(FileDecoderError::Other(anyhow!(
            "'data' key not found in an FTDC metric chunk"
        ))),
    }
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#[cfg(test)]

mod tests {
    use crate::FTDCFileDecoder;
    use std::collections::HashMap;

    //const TEST_FILE_PATH: &str = "test_data/diagnostic.data/metrics.2025-04-15T16-48-31Z-00000";

    const TEST_FILE_PATH: &str =
        "./test_data/diagnostic.data.8.0.11/metrics.2025-07-23T20-33-08Z-00000";

    #[test]
    fn test_file_doesnt_exist() {
        let mut bad_file = FTDCFileDecoder::new();
        let result = bad_file.read_file("unexistent");
        match result {
            Ok(_read) => {
                assert!(false)
            }
            Err(e) => {
                println!("Got expected error {e:?}");
            }
        }
    }

    #[test]
    #[ignore = "fixture diagnostic.data.8.0.11 is not in the repository"]
    fn test_should_read_5_chunks() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();
        assert_eq!(parsed_file.document_count, 213);
    }

    /// A MongoDB 7+ archive starts with metadata, then a type 2 delta,
    /// and only then the metric chunks. An unknown type must not end
    /// the read, and a second delta must not replace the first.
    #[test]
    fn type2_and_unknown_types_do_not_abort_the_file() {
        let path =
            std::env::temp_dir().join(format!("ftdc-rs-chunk-types-{}.bin", std::process::id()));
        let mut bytes = Vec::new();
        let documents = [
            bson::doc! {
                "_id": bson::DateTime::from_millis(0),
                "type": 0i32,
                "doc": { "host": "a.example" }
            },
            bson::doc! {
                "_id": bson::DateTime::from_millis(1),
                "type": 2i32,
                "count": 0i32,
                "doc": { "param": 1i32 }
            },
            bson::doc! {
                "_id": bson::DateTime::from_millis(2),
                "type": 9i32,
                "doc": { "future": true }
            },
            bson::doc! {
                "_id": bson::DateTime::from_millis(3),
                "type": 2i32,
                "count": 1i32,
                "doc": { "param": 2i32 }
            },
        ];
        for document in &documents {
            document.to_writer(&mut bytes).unwrap();
        }
        std::fs::write(&path, &bytes).unwrap();

        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file
            .read_file(path.to_str().unwrap())
            .expect("type 2 and an unknown type are not fatal");

        assert_eq!(parsed_file.metadata_docs.len(), 1);
        assert_eq!(
            parsed_file.metadata_docs[0].get_str("host").unwrap(),
            "a.example"
        );
        assert_eq!(parsed_file.metadata_deltas.len(), 2);
        assert_eq!(parsed_file.metadata_deltas[0].get_i32("param").unwrap(), 1);
        assert_eq!(parsed_file.metadata_deltas[1].get_i32("param").unwrap(), 2);
        assert_eq!(parsed_file.skipped_chunk_types, vec![9]);
        assert_eq!(parsed_file.document_count, 4);
        assert!(parsed_file.binary_chunks.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[ignore = "fixture diagnostic.data.8.0.11 is not in the repository"]
    fn test_inflate_chunks() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        match uncompressed_vec {
            Ok(uncompressed_vec) => {
                assert_eq!(uncompressed_vec.len(), parsed_file.binary_chunks.len());
                assert_eq!(uncompressed_vec.len(), 211);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    #[ignore = "fixture diagnostic.data.8.0.11 is not in the repository"]
    fn get_metric_meta_data() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();

        for chunk in &all_chunks {
            match FTDCFileDecoder::extract_metric_meta_data(&chunk) {
                Ok(metrics) => {
                    println!("{:?}", metrics);
                }
                Err(_e) => {
                    assert!(false);
                }
            }
        }

        assert!(true);
    }

    #[test]
    #[ignore = "fixture diagnostic.data.8.0.11 is not in the repository"]
    fn test_metric_deltas() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());
        let all_chunks = uncompressed_vec.unwrap();

        let mut chunk_number = 0;
        for chunk in &all_chunks {
            match FTDCFileDecoder::extract_metric_meta_data(&chunk) {
                Ok(mut metrics) => {
                    match FTDCFileDecoder::extract_all_metrics_from_buffer(&mut metrics, chunk) {
                        Ok(metric_count) => {
                            println!(
                                "chunk {chunk_number} {metric_count} = {}",
                                metrics.get(0).unwrap().metrics.unwrap()
                            );
                        }
                        Err(e) => {
                            println!("Chunk number {chunk_number}\nGot error {e:?}");
                            assert!(false);
                        }
                    }
                }
                Err(_e) => {
                    assert!(false);
                }
            }
            chunk_number += 1;
        }

        assert!(true);
    }

    #[test]
    #[ignore = "fixture diagnostic.data.8.0.11 is not in the repository"]
    fn test_get_metric_data_decoded() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        let data_vec = parsed_file.read_and_decode(TEST_FILE_PATH, None).unwrap();

        data_vec.iter().for_each(|x| {
            println!("{}", x.0);
        });

        assert!(true);
    }

    #[test]
    fn test_to_dataframe() {
        // Create a test HashMap with some data
        let mut test_metrics: HashMap<String, Vec<u64>> = HashMap::new();
        test_metrics.insert("metric1".to_string(), vec![1, 2, 3, 4, 5]);
        test_metrics.insert("metric2".to_string(), vec![10, 20, 30, 40, 50]);
        test_metrics.insert("metric3".to_string(), vec![100, 200, 300]); // Shorter vector to test padding

        // Convert the HashMap to a DataFrame
        let df_result = FTDCFileDecoder::metrics_to_dataframe(test_metrics);

        // Verify the result is Ok
        assert!(df_result.is_ok());

        let df = df_result.unwrap();

        // Verify the DataFrame has the expected structure
        assert_eq!(df.width(), 3); // 3 columns
        assert_eq!(df.height(), 5); // 5 rows (max length of the vectors)

        // Verify column names
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(column_names.contains(&"metric1".to_string()));
        assert!(column_names.contains(&"metric2".to_string()));
        assert!(column_names.contains(&"metric3".to_string()));

        // Verify the data in the columns
        let metric1_col = df.column("metric1").unwrap();
        let metric2_col = df.column("metric2").unwrap();
        let metric3_col = df.column("metric3").unwrap();

        // Check first values
        assert_eq!(metric1_col.u64().unwrap().get(0).unwrap(), 1);
        assert_eq!(metric2_col.u64().unwrap().get(0).unwrap(), 10);
        assert_eq!(metric3_col.u64().unwrap().get(0).unwrap(), 100);

        // Check that metric3 was padded with zeros
        assert_eq!(metric3_col.u64().unwrap().get(3).unwrap(), 0);
        assert_eq!(metric3_col.u64().unwrap().get(4).unwrap(), 0);
    }
}
