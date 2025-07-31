extern crate core;
mod errors;

use anyhow::anyhow;
use bson::{Binary, Bson, Document};
use buffered_reader;
use buffered_reader::BufferedReader;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use zune_inflate::DeflateDecoder;
use zune_inflate::errors::{DecodeErrorStatus, InflateDecodeErrors};

use crate::errors::FileDecoderError;

use polars::prelude::*;

#[derive(Debug, Clone)]
pub enum OutputFormat {
    JSON,
    Parquet,
}

/// A struct that de codes an FTDC file.
/// The parsed data should be encapsulated in another struct.
pub struct FTDCFileDecoder {
    metadata: String,
    parameters: String,
    metric_chunks_count: usize,
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

pub struct Metric {
    // pub name: String,
    pub data: Vec<u64>,
}

struct RangeCursor {
    buffer: Vec<u8>,
    _size: usize,
    at: usize,
}

/*
uint64_t  unpack(ConstDataRangeCursor *cdc) {
    uint64_t result = 0;
    uint shift = 0;

    while(true) {
        uint8_t b = cdc->ReadByte();
        result |= static_cast<uint64_t>(b & 0x7f) << shift;
        shift += 7;
        if ((b & 0x80) == 0) {
            return result;
        }
    }
}
*/

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
    const METADATA: i32 = 0;
    const METRICS: i32 = 1;
    const PARAMETERS: i32 = 2;
    pub const MAX_METRICS: usize = 300;

    pub const INITIAL_METRIC_CAPACITY: usize = 10240;

    /// Very unreliable for metric_count

    fn read_header(data: &[u8]) -> (usize, usize, usize) {
        // auto *dataSize = (int32_t*) data;
        //  as *const u32;
        let data_size = u32::from_le_bytes(data[0..4].try_into().unwrap());

        // auto pp = data+(*dataSize);
        // metricCountFromHeader = *(uint32_t*)pp;
        // deltaCountFromHeader = *(uint32_t*)(pp + 4);
        let offset: usize = data_size as usize;
        let metric_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        let delta_count = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap());

        (
            data_size as usize,
            metric_count as usize,
            delta_count as usize,
        )
    }

    fn inflate_blob(b: &Binary) -> Result<Vec<u8>, InflateDecodeErrors> {
        // Get blob
        let compressed_buffer = b.bytes.as_slice();
        // Compressed buffer. Ignore first 4 bytes.
        let mut decoder = DeflateDecoder::new(&compressed_buffer[4..]);

        // Uncompressed data has metrics_count and delta_count in the header,
        // as u32.
        match decoder.decode_zlib() {
            Ok(encoded_data) => Ok(encoded_data),
            Err(e) => Err(e),
        }
    }

    /// Uncompresses all binary blobs - Single threaded uncompression
    pub fn uncompress_data_st(&self) -> Result<Vec<Vec<u8>>, InflateDecodeErrors> {
        let mut uncompressed_data_vector: Vec<Vec<u8>> =
            Vec::with_capacity(self.metric_chunks_count);

        //
        for b in self.binary_chunks.iter() {
            let blob = FTDCFileDecoder::inflate_blob(b);

            match blob {
                Ok(encoded_data) => {
                    // println!( "Uncompressed data: {} -> {}", b.bytes.len(), encoded_data.len() );
                    uncompressed_data_vector.push(encoded_data);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(uncompressed_data_vector)
    }

    /// Uncompresses all binary blobs - Multi threaded uncompression
    ///
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

    /// Returns a valid JSON with metric names and initial value.
    ///
    pub fn get_metric_names_json(buffer: &Vec<u8>) -> Result<String, String> {
        let mut base = buffer.as_slice();
        // The document has
        let bson_doc = Document::from_reader(&mut base);
        if bson_doc.is_err() {
            return Err(bson_doc.err().unwrap().to_string());
        }
        // convert to json
        let json_value = Bson::Document(bson_doc.unwrap());

        let json_str = serde_json::to_string_pretty(&json_value);
        if json_str.is_err() {
            return Err(json_str.unwrap_err().to_string());
        }
        Ok(json_str.unwrap())
    }

    /// Reads sub-docs from the original BSON doc recursively.
    /// This is a bad name for the function.
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

    fn extract_all_metrics_from_buffer<'a>(
        metric_data_vec: &mut Vec<MetricData>,
        buffer: &Vec<u8>,
    ) -> Result<usize, FileDecoderError> {
        let (doc_size, _y, delta_count_from_header) =
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

    pub fn new() -> FTDCFileDecoder {
        FTDCFileDecoder {
            metadata: "".to_string(),
            parameters: "".to_string(),
            metric_chunks_count: 0,
            binary_chunks: vec![],
        }
    }
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
                            if let Some(chunk_type) = metrics.get("type") {
                                // println!("Chunk type: {}", chunk_type);
                                match chunk_type.as_i32().unwrap() {
                                    FTDCFileDecoder::METADATA => {
                                        // Doc key is a JSON structure with the host's information.
                                        match metrics.get("doc") {
                                            Some(doc) => {
                                                self.metadata = doc.to_string();
                                            }
                                            _ => {
                                                //println!("'doc' type not found");
                                                return Err(FileDecoderError::Other(anyhow!(
                                                    "'doc' key not found in METADATA chunk. "
                                                )));
                                            }
                                        }
                                    }
                                    FTDCFileDecoder::METRICS => {
                                        if metrics.contains_key("data") {
                                            if let Bson::Binary(b) = metrics.get("data").unwrap() {
                                                self.binary_chunks.push(b.clone());
                                            }
                                        } else {
                                            //println!("'data' type not found");
                                            return Err(FileDecoderError::Other(anyhow!(
                                                "'data' key not found in METRICS chunk. "
                                            )));
                                        }
                                    }
                                    FTDCFileDecoder::PARAMETERS => {
                                        // Doc key is a JSON structure with the host's information.
                                        match metrics.get("doc") {
                                            Some(doc) => {
                                                self.parameters = doc.to_string();
                                            }
                                            _ => {
                                                //println!("doc type not found");
                                                return Err(FileDecoderError::Other(anyhow!(
                                                    "'doc' key not found in PARAMETERS chunk. "
                                                )));
                                            }
                                        }
                                    }
                                    _ => {
                                        return Err(FileDecoderError::Other(anyhow!(
                                            "Unknown BSON doc type {chunk_type:?}"
                                        )));
                                    }
                                }
                            } else {
                                println!("'type' not found");
                                return Err(FileDecoderError::Other(anyhow!(
                                    "'type' key not found in chunk. "
                                )));
                            }
                        }
                        Err(e) => {
                            if !reader.eof() {
                                return Err(FileDecoderError::Other(anyhow!(e)));
                            }
                            break;
                        }
                    }
                    self.metric_chunks_count += 1;
                }
            }
            Err(e) => {
                // IO errors
                return Err(FileDecoderError::FileIOError(e));
            }
        }

        Ok(())
    }

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

    pub fn decode_data(
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

    /// Will read a file, uncompress chunks and decode deltas.
    pub fn read_and_decode(
        &mut self,
        file_name: &str,
    ) -> Result<HashMap<String, Vec<u64>>, FileDecoderError> {
        //let mut out: HashMap<String, Vec<u64>> = HashMap::new();

        let data_chunks = self.read_and_uncompress(file_name)?;

        let decoded_data = self.decode_data(data_chunks)?;

        Ok(decoded_data)
    }

    /// Converts a HashMap<String, Vec<u64>> to a polars DataFrame
    ///
    /// # Arguments
    ///
    /// * `metrics` - A HashMap where keys are metric names and values are vectors of u64 values
    ///
    /// # Returns
    ///
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

    pub fn write_to_parquet(
        input_file_name: &str,
        output_file_name: &str,
        metadata: Option<String>,
    ) -> Result<(), FileDecoderError> {
        let mut parsed_file = FTDCFileDecoder::new();

        let metrics = parsed_file.read_and_decode(input_file_name)?;

        // Convert the metrics hashmap to a DataFrame
        let mut df = Self::metrics_to_dataframe(metrics)?;

        // Write the DataFrame to a parquet file
        let mut file = std::fs::File::create(output_file_name)
            .map_err(|e| FileDecoderError::Other(anyhow!("Failed to create output file: {}", e)))?;

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| FileDecoderError::Other(anyhow!("Failed to write parquet file: {}", e)))?;

        if let Some(metadata_file) = metadata {
            // Create entry
            let metric_names: Vec<String> = df
                .get_column_names()
                .iter()
                .map(move |name| -> String { name.to_string() })
                .collect();

            // Convert the Metadata struct to a JSON string
            let json_data = serde_json::json!({
                "metadata": parsed_file.metadata,
                "parameters":parsed_file.parameters,
                "chunks_in_file": parsed_file.metric_chunks_count,
                "metrics": metric_names
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
    fn test_should_read_5_chunks() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();
        assert_eq!(parsed_file.metric_chunks_count, 213);
    }

    #[test]
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
    fn test_get_metric_data_decoded() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        let data_vec = parsed_file.read_and_decode(TEST_FILE_PATH).unwrap();

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
