//! Unsigned LEB128 integers, as stored in an FTDC delta block.
//!
//! Each byte contributes its low 7 bits. The high bit is set when another
//! byte follows. Ten payload bytes fill a `u64` (9 × 7 + 1). One more byte
//! would shift by 70, which is not a value this encoding can represent.
//!
//! A zero byte is a zero value. In the delta block that zero is also the
//! start of a run of zeros; the run length itself is the next integer and
//! is decoded by the chunk reader. Reaching the end of the buffer while a
//! value is still open means the chunk is truncated.

use crate::errors::FileDecoderError;

/// A read position in a delta block.
///
/// The block is the tail of the inflated chunk, after the reference
/// document and the two header words. The cursor borrows that tail. An
/// owned copy used to be taken here and then thrown away with the chunk.
pub(crate) struct RangeCursor<'a> {
    buffer: &'a [u8],
    at: usize,
}

impl<'a> RangeCursor<'a> {
    pub(crate) fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, at: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.at)
    }
}

/// Read one unsigned LEB128 integer.
///
/// A zero is a real value (it starts a run of zeros). Running off the end
/// of the buffer used to return 0 as well, so a short chunk was decoded as
/// a run of quiet zeros instead of a broken file.
pub(crate) fn unpack(data_range_cursor: &mut RangeCursor) -> Result<u64, FileDecoderError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if data_range_cursor.at >= data_range_cursor.buffer.len() {
            return Err(FileDecoderError::varint(format!(
                "FTDC varint ran past the end of the delta block"
            )));
        }
        // A u64 has room for 10 payload bytes (9 * 7 + 1). One more byte
        // would shift by 70 and panic, and it cannot be a legal value.
        if shift >= 64 {
            return Err(FileDecoderError::varint(format!(
                "FTDC varint is wider than 64 bits"
            )));
        }
        let b = data_range_cursor.buffer[data_range_cursor.at];
        data_range_cursor.at += 1;

        result |= u64::from(b & 0x7f) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}
