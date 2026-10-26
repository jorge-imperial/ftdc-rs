//! Failures while reading an FTDC archive or writing its output.
//!
//! Filesystem problems keep the path and the OS error. The other variants
//! are the layout rules the decoder actually distinguishes: a truncated
//! frame, a `type` field that cannot be read, zlib, a size cap, a
//! `metric_count` that does not match the reference walk, a delta-block
//! varint, and columns that do not share a height. Anything else is
//! [`FileDecoderError::Format`]. The message names the rule that failed.

use std::io;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileDecoderError {
    /// A filesystem call failed. `action` is a fixed verb such as
    /// "failed to open", so the display reads `failed to open <path>: <os>`.
    #[error("{action} {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The file ended inside a BSON length, inside a document, or before
    /// a chunk header the length had promised.
    #[error("{0}")]
    Truncated(String),

    /// The `type` field was missing or not an integer this decoder can read.
    ///
    /// An unknown type number is not this error. Those documents are skipped
    /// and remembered on the archive. This variant is a wrapper that claims
    /// a type and then fails to say what it is.
    #[error("{0}")]
    ChunkType(String),

    /// zlib failed, or the inflated bytes are not the length the blob's
    /// leading `uint32` claimed.
    #[error("{0}")]
    Inflate(String),

    /// A framed document, an uncompressed chunk, or a metric rectangle is
    /// past the limit the server enforces (or, for the framed length, past
    /// the BSON maximum of 16 MiB).
    #[error("{0}")]
    SizeLimit(String),

    /// The reference walk and the chunk header disagree about how many
    /// metrics the delta block has. Deltas are positional, so decoding
    /// would attach every later column to the wrong series.
    #[error(
        "extracted {extracted} metrics from the reference document, but the chunk header says {header}. \
         Deltas are positional, so these lengths have to match or every later series is built from the wrong column. \
         First extracted name is '{first}', last is '{last}'."
    )]
    MetricCount {
        extracted: usize,
        header: usize,
        first: String,
        last: String,
    },

    /// A varint ran off the end of the delta block, was wider than 64 bits,
    /// or the block did not end on the last sample (a zero run left over,
    /// or trailing bytes).
    #[error("{0}")]
    Varint(String),

    /// Two columns that should share a height do not. One chunk is one
    /// rectangle. A file is that rectangle stacked in order, with nulls
    /// where a metric was absent.
    #[error("{0}")]
    Ragged(String),

    /// A layout problem that is not one of the cases above: a BSON document
    /// that does not parse, a wrapper field of the wrong type, or a metric
    /// whose kind changes between chunks.
    #[error("{0}")]
    Format(String),
}

impl FileDecoderError {
    pub(crate) fn io(action: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            action,
            path: path.into(),
            source,
        }
    }

    pub(crate) fn truncated(message: impl Into<String>) -> Self {
        Self::Truncated(message.into())
    }

    pub(crate) fn chunk_type(message: impl Into<String>) -> Self {
        Self::ChunkType(message.into())
    }

    pub(crate) fn inflate(message: impl Into<String>) -> Self {
        Self::Inflate(message.into())
    }

    pub(crate) fn size_limit(message: impl Into<String>) -> Self {
        Self::SizeLimit(message.into())
    }

    pub(crate) fn varint(message: impl Into<String>) -> Self {
        Self::Varint(message.into())
    }

    pub(crate) fn ragged(message: impl Into<String>) -> Self {
        Self::Ragged(message.into())
    }

    pub(crate) fn format(message: impl Into<String>) -> Self {
        Self::Format(message.into())
    }
}
