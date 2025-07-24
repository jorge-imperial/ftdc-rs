mod errors;

extern crate core;

use bson::{Binary, Bson, Document};
use buffered_reader;
use buffered_reader::BufferedReader;
use integer_encoding::*;
use zune_inflate::errors::InflateDecodeErrors;
use zune_inflate::DeflateDecoder;

use crate::errors::FTDCDecodeError;
use crate::errors::FileParserError;

use polars::prelude::*;


#[derive(Debug, Clone)]
pub enum OutputFormat {
    JSON,
    Parquet,
}

/// A structure that holds parsed info for a file.
pub struct ParsedFile {
    metadata: String,
    parameters: String,
    
    metric_chunks_count: usize,
    binary_chunks: Vec<Binary>,
    
    metrics_in_chunks: usize,
    total_samples: usize,
}


#[derive(Debug)]
pub struct MetricMetaData {
    name: String,
    initial: u64,
    metric_type: Bson,
}

#[derive(Debug)]
pub struct MetricData  {
    pub meta_data: MetricMetaData,
    pub data: [u64; ParsedFile::MAX_METRICS],//& 'a [u64],
    pub metrics: usize,
}

impl ParsedFile {
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

    fn inflate_blob(b: &mut Binary) -> Result<Vec<u8>, InflateDecodeErrors> {
        // Get blob
        let compressed_buffer = b.bytes.as_slice();
        // Compressed buffer. Ignore first 4 bytes.
        let mut decoder = DeflateDecoder::new(&compressed_buffer[4..]);

        // Uncompressed data has metrics_count and delta_count in the header,
        // as u32.
        match decoder.decode_zlib() {
            Ok(encoded_data) => Ok(encoded_data),
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn uncompress_data(&mut self) -> Result<Vec<Vec<u8>>, InflateDecodeErrors> {
        let mut uncompressed_data_vector: Vec<Vec<u8>> = Vec::with_capacity(self.metric_chunks_count);

        for b in self.binary_chunks.iter_mut() {
            let blob = ParsedFile::inflate_blob(&mut *b);

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
    fn get_docs(doc_name: &str, root: Document, metric_data: &mut Vec<MetricMetaData>) {
        for (k, doc) in root {
            match doc {
                Bson::Double(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
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
                                ParsedFile::get_docs(&n, doc, metric_data);
                            }
                            _ => {}
                        }

                        i += 1;
                    }
                }
                Bson::Document(sub_doc) => {
                    let n = format!("{doc_name}.{}", k.to_string());
                    ParsedFile::get_docs(&n, sub_doc, metric_data);
                }
                Bson::Boolean(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                    });
                }
                Bson::Null => {}
                Bson::RegularExpression(_) => {}
                Bson::JavaScriptCode(_) => {}
                Bson::JavaScriptCodeWithScope(_) => {}
                Bson::Int32(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                    });
                }
                Bson::Int64(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v as u64,
                        metric_type: doc,
                    });
                }
                Bson::Timestamp(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}_time", k.to_string()),
                        initial: v.time as u64,
                        metric_type: doc.clone(),
                    });
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}_increment", k.to_string()),
                        initial: v.increment as u64,
                        metric_type: doc,
                    });
                }
                Bson::Binary(_) => {}
                Bson::ObjectId(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v.timestamp().timestamp_millis() as u64,
                        metric_type: doc,
                    });
                }
                Bson::DateTime(v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: v.timestamp_millis() as u64,
                        metric_type: doc,
                    });
                }
                Bson::Symbol(_) => {}
                Bson::Decimal128(_v) => {
                    metric_data.push(MetricMetaData {
                        name: format!("{doc_name}.{}", k.to_string()),
                        initial: 999999999,
                        metric_type: doc,
                    });
                }
                Bson::Undefined => {}
                Bson::MaxKey => {}
                Bson::MinKey => {}
                Bson::DbPointer(_) => {}
            }
        }
    }

    pub fn get_metric_names(buffer: &Vec<u8>) -> Vec<MetricMetaData> {
        let mut base = buffer.as_slice();

        let bson_doc = Document::from_reader(&mut base);
        if bson_doc.is_err() {
            return vec![];
        }

        let mut ret_metrics: Vec<MetricMetaData> = vec![];

        let root = bson_doc.unwrap();

        ParsedFile::get_docs("", root, &mut ret_metrics);

        return ret_metrics;
    }

    /// Converts an array of u8 to a vector of vectors of u64 deltas that will be used to construct
    /// the metric values.
    ///
    fn get_metric_deltas_from_buffer(buffer: &Vec<u8>) -> Option<Vec<u64>> {
        let base = buffer.as_slice();
        let (data_size, metric_count, delta_count) = ParsedFile::read_header(buffer.as_slice());

        // Size of all deltas.
        let mut metrics_deltas: Vec<u64> =
            Vec::with_capacity((metric_count * delta_count + 1) as usize);
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

    /// get_metrics()
    pub fn get_metrics_from_buffer<'a>(
        meta_data: &Vec<MetricMetaData>,
        buffer: &Vec<u8>,
    ) -> Option<Vec<MetricData>> {
        // get deltas
        let deltas_option = ParsedFile::get_metric_deltas_from_buffer(buffer);
        if deltas_option.is_none() {
            return None;
        }
        let deltas = deltas_option.unwrap();
        let deltas_slice = deltas.as_slice();

        let (_data_size, metric_count, delta_count) = ParsedFile::read_header(buffer.as_slice());

        assert_eq!(deltas.len(), metric_count * delta_count);

        // Initialize return vectors
        let mut metrics: Vec<MetricData> = vec![];
        for i in 0..meta_data.len() {
            let meta = meta_data.get(i).unwrap();
            let mut data: [u64; ParsedFile::MAX_METRICS] = [0; ParsedFile::MAX_METRICS];
            data[0] = meta.initial;

            // Evaluation of metrics array is made here
            for j in 1..delta_count {
                data[j] = data[j - 1];
                data[j] = deltas_slice[(delta_count * i) + j];
                data[j] = ParsedFile::add_delta(data[j - 1], deltas_slice[(delta_count * i) + j]);
            }

            metrics.push(MetricData {
                meta_data: MetricMetaData {
                    name: meta.name.clone(),
                    metric_type: meta.metric_type.clone(),
                    initial: meta.initial,
                },
                data,
                metrics: delta_count,
            });
        }

        Some(metrics)
    }

    pub fn new() -> ParsedFile {
        ParsedFile {
            metadata: "".to_string(),
            parameters: "".to_string(),
            metric_chunks_count: 0,
            binary_chunks: vec![],
            metrics_in_chunks: 0,
            total_samples: 0,
        }
    }
    pub fn read_file(&mut self, file_name: &str) -> Result<(), FileParserError> {

        // Open file, get buffed_reader
        match  buffered_reader::File::open(file_name) {
            Ok(mut reader) => {

                loop {
                    //
                    match Document::from_reader(&mut reader) {
                        Ok(metrics) => {
                            if let Some(chunk_type) = metrics.get("type") {
                                // println!("Chunk type: {}", chunk_type);

                                match chunk_type.as_i32().unwrap() {
                                    ParsedFile::METADATA => {
                                        // Doc key is a JSON structure with the host's information.
                                        match metrics.get("doc") {
                                            Some(doc) => {
                                                self.metadata = doc.to_string();
                                            }
                                            _ => {
                                                println!("'doc' type not found");
                                            }
                                        }
                                    }
                                    ParsedFile::METRICS => {
                                        if metrics.contains_key("data") {
                                            if let Bson::Binary(b) = metrics.get("data").unwrap() {
                                                self.binary_chunks.push(b.clone());
                                            }
                                        }
                                    }
                                    ParsedFile::PARAMETERS => {
                                        // Doc key is a JSON structure with the host's information.
                                        match metrics.get("doc") {
                                            Some(doc) => {
                                                self.parameters = doc.to_string();
                                            }
                                            _ => {
                                                println!("doc type not found");
                                            }
                                        }
                                    }
                                    _ => {
                                        println!("Unknown doc type: {:?}", chunk_type);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if !reader.eof() {
                                println!("Failed to read from file: {}", e);
                            }
                            break;
                        }
                    }
                    self.metric_chunks_count += 1;
                }
            }
            Err(e) => {
                return Err( FileParserError::FileIOError(e));
            }
        }

        Ok(())
    }
    
    pub fn get_metrics(&mut self) -> Result<(), &str> {
        match self.uncompress_data() {
            Ok(uncompress_data) => {

                // TODO: Should check that all chunks have the same number of metrics.
                let mut metric_names: Vec<MetricMetaData> = vec![];

                // Uncompress
                for chunk in uncompress_data {
                    
                    // We only need these once.
                    if metric_names.len() == 0 {
                        metric_names = ParsedFile::get_metric_names(&chunk);
                    }

                    if let Some(metrics) =
                        ParsedFile::get_metrics_from_buffer(&metric_names, &chunk) {
                        println!("{}", metrics.len());
                        
                       // for metric in metrics {
                       //      let series = Series
                       // }
                    }
                }
                
                Ok(())
                 
            }
            _ => {
                Err("Fatal: uncompress_data")
            }
        }
    }


    pub fn write_parquet(&mut self, file_name: &str) -> Result<(), &str> {
        match self.uncompress_data() {
            Ok(uncompress_data) => {
                
                // TODO: Should check that all chunks have the same number of metrics.
                let mut metric_names : Vec<MetricMetaData> = vec![]; //  = ParsedFile::get_metric_names(&chunk);
                
                // Uncompress
                for chunk in uncompress_data {
                    if metric_names.len() == 0 {
                        metric_names = ParsedFile::get_metric_names(&chunk);
                    }
                                        
                    if let Some(metrics) =
                        ParsedFile::get_metrics_from_buffer(&metric_names, &chunk) {
                        println!("{}", metrics.len());

                        //let series_name = metrics[0].meta_data.name.as_str();
                        //let data = metrics[0].data;
                        //let s = Series::new(PlSmallStr::from(series_name), data);
                       
                        
                        /*
                        let mut series : Vec<Series> = vec![];
                        for metric in uncompress_data {
                            let s= Series::new(self.)
                            series.push(s);
                        }

                        let file = File::create("example.parquet").expect("could not create file");
                        ParquetWriter::new(file).finish(&mut df); // Write DataFrame to Parquet file.
                        */

                    }
                }

                Ok(())
            }
            Err(e) => Err("Fatal: {}\n parsing file."),
        }
    }
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#[cfg(test)]

mod tests {
    use crate::ParsedFile;

    const TEST_FILE_PATH: &str = "diagnostic.data/metrics.2025-04-15T16-48-31Z-00000";
    #[test]
    fn file_doesnt_exist() {

        let mut bad_file = ParsedFile::new();
        let result = bad_file.read_file("unexistent");
        match result {
            Ok(read) => {
                assert!(false)
            },
            Err(e) => {
                println!("Got expected error {e:?}");
            }
        }


    }

    #[test]
    fn should_read_5_chunks() {
        let mut parsed_file = ParsedFile::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();
        assert_eq!(parsed_file.metric_chunks_count, 5);
    }

    #[test]
    fn inflate_chunks() {
        let mut parsed_file = ParsedFile::new();
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
        let mut parsed_file = ParsedFile::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];

        let names = ParsedFile::get_metric_names_json(first_chunk);
        if names.is_ok() {
            assert!(true);
        } else {
            print!("Boo! No metrics found!");
            assert!(false);
        }
    }

    #[test]
    fn get_metric_deltas() -> () {
        let mut parsed_file = ParsedFile::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];
        let values = ParsedFile::get_metric_deltas_from_buffer(first_chunk);

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
        let mut parsed_file = ParsedFile::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];
        let metrics = ParsedFile::get_metric_names(first_chunk);
        assert_eq!(metrics.len(), 3051);
        for m in metrics {
            println!("{}", m.name);
        }
    }

    #[test]
    fn get_metric_data() -> () {
        let mut parsed_file = ParsedFile::new();
        parsed_file.read_file(TEST_FILE_PATH).unwrap();

        let uncompressed_vec = parsed_file.uncompress_data();

        assert!(uncompressed_vec.is_ok());

        let all_chunks = uncompressed_vec.unwrap();
        let first_chunk = &all_chunks[0];
        let metric_names = ParsedFile::get_metric_names(first_chunk);
        assert_eq!(metric_names.len(), 3051);

        let xx = ParsedFile::get_metrics_from_buffer(&metric_names, first_chunk);

        if let Some(x) = xx {
            assert_eq!(x.len(), 3051);
            println!("{:?}", x.get(0).unwrap().meta_data);
        }
    }
}
