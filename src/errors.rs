use std::error::Error;
use std::fmt::{Display, Formatter};

/// Enumerates
#[derive(Debug)]
pub enum FileParserError {
    FileIOError(std::io::Error),
    Other(Box::<dyn Error>, String),
}

#[derive(Debug)]
pub  struct FTDCDecodeError {
    pub  message: String,
    pub  source: Option<Box<dyn Error>>,
}


impl Display for FTDCDecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("FTDC decode error")
    }
}

impl Error for FTDCDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }

}
