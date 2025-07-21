use std::error::Error;

enum FileParsingError {
    UncompressError,
    FileIOError(std::io::Error),
    MetricCountError,
}

struct ParserError {
    
    message: String,
    source: Option<Box<dyn Error>>,
    err_type: FileParsingError,
}