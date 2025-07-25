use thiserror::Error;

/// Enumerates
#[derive(Debug, Error)]

pub enum FileDecoderError {
    #[error("{0}")]
    FileIOError(std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct FTDCDecodeError {
    pub message: String,
    pub source: Option<anyhow::Error>,
}
