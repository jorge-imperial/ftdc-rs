extern crate core;
mod errors;

use anyhow::anyhow;
use bson::{Binary, Bson, Document};
use buffered_reader;
use buffered_reader::BufferedReader;
use integer_encoding::*;
use std::sync::{Arc, Mutex};
use std::thread;
use zune_inflate::DeflateDecoder;
use zune_inflate::errors::InflateDecodeErrors;

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
    pub name: String,
    pub data: Vec<u64>,
}



impl FTDCFileDecoder {
    const METADATA: i32 = 0;
    const METRICS: i32 = 1;
    const PARAMETERS: i32 = 2;
    pub const MAX_METRICS: usize = 300;

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
    pub fn uncompress_data(&self) -> Result<Vec<Vec<u8>>, InflateDecodeErrors> {

        let uncompressed_data_vector = Arc::new(Mutex::new(vec![]));
        let binary_chunks_x = Arc::new(self.binary_chunks.clone());

        let mut handles = vec![];

        for i in 0..binary_chunks_x.len() {
            let udv = Arc::clone(&uncompressed_data_vector);
            let bc = Arc::clone(&binary_chunks_x);

            let handle = thread::spawn(move || {
                let binary_chunk = bc.get(i).unwrap();
                let inflated_data: Vec<u8> = FTDCFileDecoder::inflate_blob(binary_chunk).unwrap();

                let mut uncompressed_data_vector = udv.lock().unwrap();
                uncompressed_data_vector.push(inflated_data);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
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
    fn get_docs(doc_name: &str, root: Document, metric_data: &mut Vec<MetricData>) {
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
                Bson::String(_) => {
                    //names.push( format!("{doc_name}.{}", k.to_string()));
                    //metrics.push(v.len() as u64);
                    //println!("{doc_name}.{k} : '{}'", v);
                }
                Bson::Array(arr) => {
                    let mut i = 0;
                    for a in arr {
                        let n = format!("{doc_name}.{}.{}", i, k.to_string());

                        match a {
                            Bson::Document(doc) => {
                                FTDCFileDecoder::get_docs(&n, doc, metric_data);
                            }
                            _ => {}
                        }

                        i += 1;
                    }
                }
                Bson::Document(sub_doc) => {
                    let n = format!("{doc_name}.{}", k.to_string());
                    FTDCFileDecoder::get_docs(&n, sub_doc, metric_data);
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

    fn extract_metric_names(buffer: &Vec<u8>) -> Result<Vec<MetricData>, FileDecoderError> {
        let mut base = buffer.as_slice();

        match Document::from_reader(&mut base) {
            Ok(bson_doc) => {
                let mut ret_metrics: Vec<MetricData> = vec![];

                let root = bson_doc;

                FTDCFileDecoder::get_docs("", root, &mut ret_metrics);

                Ok(ret_metrics)
            }
            Err(e) => Err(FileDecoderError::Other(anyhow!(e))),
        }
    }

    /// Converts an array of u8 to a vector of vectors of u64 deltas that will be used to construct
    /// the metric values.
    ///
    fn get_metric_deltas_from_buffer(buffer: &Vec<u8>) -> Option<Vec<u64>> {
        let base = buffer.as_slice();
        let (data_size, metric_count, delta_count) =
            FTDCFileDecoder::read_header(buffer.as_slice());

        // Size of all deltas.
        let mut metrics_deltas: Vec<u64> = Vec::with_capacity(metric_count * delta_count + 1);
        // Number of consecutive zeroes.
        let mut num_zeroes: u64 = 0;
        // Last element in buffer.
        let end = buffer.len();
        // start of variable int block.
        let mut i: usize = data_size as usize + 8;

        while i <= end {
            let delta: u64;
            // zero values are RLE
            if num_zeroes != 0 {
                delta = 0;
                num_zeroes -= 1;
            } else {
                let p = base[i..end].to_vec();
                if let Some(decoded) = u64::decode_var(&p) {
                    i += decoded.1;
                    delta = decoded.0;
                    // If the decoded delta is a zero, next is the numbers of zeroes that follow.
                    if delta == 0 {
                        let p = base[i..end].to_vec();
                        if let Some(decoded) = u64::decode_var(&p) {
                            i += decoded.1;
                            num_zeroes = decoded.0;
                        }
                    }
                } else {
                    break;
                }
            }
            metrics_deltas.push(delta);
        }
        Some(metrics_deltas)
    }

    fn add_delta(value: u64, delta: u64) -> u64 {
        // Well, turns out deltas are signed...
        let mut signed_delta: i64 = delta as i64;

        // This kind-of makes sense.
        if signed_delta < 0 {
            signed_delta *= -1;
            if let Some(v) = value.checked_sub(signed_delta as u64) {
                return v;
            }
        } else {
            if let Some(v) = value.checked_add(signed_delta as u64) {
                return v;
            }
        }

        // otherwise, return original value
        value
    }

    fn extract_metrics_from_buffer<'a>(
        meta_data_vec: &mut Vec<MetricData>,
        buffer: &Vec<u8>,
    ) -> Result<usize, FileDecoderError> {
        // get deltas
        match FTDCFileDecoder::get_metric_deltas_from_buffer(buffer) {
            Some(deltas) => {
                let deltas_slice = deltas.as_slice();

                let (_data_size, metric_count, delta_count) =
                    FTDCFileDecoder::read_header(buffer.as_slice());

                assert_eq!(deltas.len(), metric_count * delta_count);

                // Initialize return vectors
                for i in 0..meta_data_vec.len() {
                    let meta = meta_data_vec.get(i).unwrap();
                    let mut data: [u64; FTDCFileDecoder::MAX_METRICS] =
                        [0; FTDCFileDecoder::MAX_METRICS];
                    data[0] = meta.initial;

                    // Evaluation of metrics array is made here
                    for j in 1..delta_count {
                        data[j] = data[j - 1];
                        data[j] = deltas_slice[(delta_count * i) + j];
                        data[j] = FTDCFileDecoder::add_delta(
                            data[j - 1],
                            deltas_slice[(delta_count * i) + j],
                        );
                    }

                    meta_data_vec.get_mut(i).unwrap().data = Option::from(data);
                    meta_data_vec.get_mut(i).unwrap().metrics = Option::from(delta_count);
                }

                Ok(meta_data_vec.len())
            }
            None => Err(FileDecoderError::Other(anyhow!("No deltas found."))),
        }
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

    pub fn read_and_decode(&mut self, file_name: &str) -> Result<Vec<Metric>, FileDecoderError> {
        let mut out: Vec<Metric> = vec![];

        let data_chunks = self.read_and_uncompress(file_name)?;

        // This is the max size per metric
        let max_size_of_full_metrics = data_chunks.len() * (Self::MAX_METRICS - 1);

        let mut expected_metrics = 0;
        for chunk in data_chunks {
            let mut metric_data = FTDCFileDecoder::extract_metric_names(&chunk)?;

            let _metric_data_size =
                FTDCFileDecoder::extract_metrics_from_buffer(&mut metric_data, &chunk)?;

            // Check for consistency: All chunks have the same number of metrics
            if expected_metrics == 0 {
                expected_metrics = metric_data.len();

                // allocate Metric
                out.extend(metric_data.into_iter().map(|m| {
                    let mut v = Vec::with_capacity(max_size_of_full_metrics);
                    if let Some(slice) = m.data {
                        v.extend_from_slice(&slice);
                    }
                    Metric {
                        name: m.name,
                        data: v,
                    }
                }));
            } else {
                if expected_metrics != metric_data.len() {
                    return Err(FileDecoderError::Other(anyhow!(
                        "Inconsistent number of metrics across chunks."
                    )));
                }

                for (i, md) in metric_data.iter().enumerate() {
                    if let Some(s) = md.data {
                        out[i].data.extend_from_slice(s.as_slice());
                    }
                }
            }
        }
        Ok(out)
    }
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#[cfg(test)]

mod tests {
    use polars::df;
    use polars::frame::DataFrame;
    use crate::{FTDCFileDecoder, Metric};
    use crate::errors::FileDecoderError;

    const TEST_FILE_PATH: &str = "test_data/diagnostic.data/metrics.2025-04-15T16-48-31Z-00000";
    #[test]
    fn file_doesnt_exist() {
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
    fn should_read_5_chunks() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();
        assert_eq!(parsed_file.metric_chunks_count, 5);
    }

    #[test]
    fn inflate_chunks() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        match uncompressed_vec {
            Ok(uncompressed_vec) => {
                assert_eq!(uncompressed_vec.len(), parsed_file.binary_chunks.len());
                assert_eq!(uncompressed_vec.len(), 3);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn get_metric_names() {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];

        let names = FTDCFileDecoder::get_metric_names_json(first_chunk);
        if names.is_ok() {
            assert!(true);
        } else {
            print!("Boo! No metrics found!");
            assert!(false);
        }
    }

    #[test]
    fn get_metric_deltas() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];
        let values = FTDCFileDecoder::get_metric_deltas_from_buffer(first_chunk);

        if let Some(values) = values {
            assert_eq!(values.len(), 3051 * 299);
            for _m in 0..3051 {
                for d in 0..299 {
                    print!("{} ", d);
                }
                println!();
            }
        } else {
            print!("Boo! No metrics found!");
            assert!(false);
        }
    }

    #[test]
    fn get_metric_names_2() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];

        match FTDCFileDecoder::extract_metric_names(first_chunk) {
            Ok(metrics) => {
                assert_eq!(metrics.len(), 3051);
                for m in metrics {
                    println!("{}", m.name);
                }
            }
            Err(_e) => {}
        }
    }

    #[test]
    fn get_metric_data() -> () {
        let mut parsed_file = FTDCFileDecoder::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];

        match FTDCFileDecoder::extract_metric_names(first_chunk) {
            Ok(mut metric_data) => {
                assert_eq!(metric_data.len(), 3051);

                match FTDCFileDecoder::extract_metrics_from_buffer(&mut metric_data, first_chunk) {
                    Ok(metric_data_size) => {
                        assert_eq!(metric_data_size, 3051);
                        println!("{:?}", metric_data.get(0).unwrap());
                    }
                    Err(_e) => {}
                }
            }
            Err(_e) => {}
        }
    }

    #[test]
    fn writing_to_parquet() {
        let mut parsed_file = FTDCFileDecoder::new();

        let data = parsed_file.read_and_decode(TEST_FILE_PATH);
        assert!(data.is_ok());

        match data {
            Ok(metrics) => {
                let df : DataFrame = DataFrame::default();

                //df._add_series();


            }
            Err(e) => {}
        }
    }
}
