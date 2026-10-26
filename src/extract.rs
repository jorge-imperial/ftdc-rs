//! Which fields of a reference document become FTDC metrics.
//!
//! The compressor walks the document in field order and appends a `u64`
//! for each value whose BSON type it knows how to store:
//!
//! * `double`, `int32`, `int64`, `decimal128`, `bool`, `datetime` — one
//!   `u64` each
//! * `timestamp` — two `u64`s, seconds then increment
//! * subdocuments — walked, in the order of their fields
//! * arrays — walked element by element. The index is the key, so an
//!   element's path is `parent.key.<index>.field`
//!
//! Strings, ObjectIds, binary, null, regex, JavaScript, symbol, undefined,
//! MinKey, MaxKey, and DBPointer are skipped. Two ObjectIds show up in real
//! `serverStatus` output and are the usual reason a hand-rolled walk is
//! longer than the chunk header: `repl.topologyVersion.processId` on every
//! sample, and `repl.electionId` after an election. The header's
//! `metric_count` is the length of the server's walk. Deltas are positional,
//! so a longer walk applies every later delta to the wrong series.
//!
//! Decimal128 is the 16 little-endian IEEE 754-2008 BID bytes. The
//! compressor calls `BSONElement::numberLong()`, which truncates toward
//! zero and saturates at the int64 limits. NaN becomes 0. Infinity becomes
//! `i64::MAX` or `i64::MIN` by sign. The int64 bit pattern is the metric.
//! A negative value is therefore a large unsigned number, which is what a
//! later wrapping delta expects. See [`decimal128_to_metric`].
//!
//! A double is squeezed into an int64 before that same bit cast. NaN
//! becomes 0. A value greater than or equal to 2^63, including +Infinity,
//! becomes `i64::MAX`. A value less than -2^63, including -Infinity,
//! becomes `i64::MIN`. Every other value is truncated toward zero. See
//! [`double_to_metric`].
//!
//! The path of a field is its own name at the root, and `parent.key` under
//! a document or an array element. Array elements use the decimal index as
//! the key (`members.0.health`). A timestamp's two metrics are `field_time`
//! and `field_increment`, seconds then increment.
//!
//! Each metric also keeps the rule that produced it ([`MetricKind`]). The
//! stored sample is still the `u64` bit pattern. The kind is how a caller
//! tells a counter, a truncated double, a boolean, and a timestamp apart.

use crate::errors::FileDecoderError;
use bson::{Bson, Document};

/// Which compressor rule produced a metric's `u64` bits.
///
/// int32 and int64 share [`MetricKind::Int`]: both are `numberLong`'s bit
/// pattern. Decimal128 is separate. It also ends as `numberLong`, but the
/// conversion is the decimal truncation, not the double arm and not an
/// integer load. A timestamp is two columns, seconds then increment, and
/// the two kinds stay in that order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Double,
    Int,
    Decimal,
    Bool,
    Date,
    TimestampSeconds,
    TimestampIncrement,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Double => "double",
            Self::Int => "int",
            Self::Decimal => "decimal",
            Self::Bool => "bool",
            Self::Date => "date",
            Self::TimestampSeconds => "timestampSeconds",
            Self::TimestampIncrement => "timestampIncrement",
        }
    }
}

/// One decoded metric: the compressor's kind, and one sample per row.
///
/// `values` are the `u64` bit patterns. A missing sample is `None`. `kind`
/// is taken from the chunk that introduced the name. A later chunk that
/// classifies the same name with a different kind is an error: the sidecar
/// would otherwise describe the column as something the later samples are not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricColumn {
    pub kind: MetricKind,
    pub values: Vec<Option<u64>>,
}

#[derive(Debug)]
pub(crate) struct MetricData {
    pub(crate) name: String,
    pub(crate) initial: u64,
    pub(crate) kind: MetricKind,
    /// Reconstructed samples for one chunk.
    ///
    /// Index 0 is the value taken from the reference document. Each later
    /// index is that value plus the deltas that follow it. The length is
    /// `sample_count + 1`, not `sample_count`: the header counts deltas,
    /// and the reference document is itself a sample. `None` means the
    /// caller rejected the name; the deltas were still consumed.
    pub(crate) data: Option<Vec<u64>>,
}

pub(crate) fn push_metric(name: &str, initial: u64, kind: MetricKind, out: &mut Vec<MetricData>) {
    out.push(MetricData {
        name: name.to_string(),
        initial,
        kind,
        data: None,
    });
}

/// Convert a Decimal128 the way `BSONElement::numberLong()` does.
///
/// The compressor does not have a special case for decimal. It calls
/// `numberLong()`, and `Decimal128::toLong()` truncates toward zero and
/// saturates at the int64 limits. NaN becomes 0. The int64 bit pattern
/// is then stored as the uint64 metric.
///
/// The 16 bytes are IEEE 754-2008 binary integer decimal, stored by BSON
/// in the opposite byte order from the spec. `u128::from_le_bytes` puts
/// the sign in bit 127. Bits 1 through 4 of that spec order, all set,
/// mark a special: bit 5 set is NaN, otherwise Infinity. A finite value
/// whose combination field starts with `11` carries a 14-bit exponent
/// from bit 3 and a coefficient prefixed by `100`. Otherwise the exponent
/// starts at bit 1 and the coefficient prefix is `0`. The exponent bias
/// is 6176. The coefficient is at most 34 digits. The metric is
/// `coefficient * 10^exponent`, truncated toward zero.
pub(crate) fn decimal128_to_metric(value: &bson::Decimal128) -> u64 {
    decimal128_bits_to_i64(value.bytes()) as u64
}

/// `index` counts from the high bit of `bits`, which is spec bit 0.
fn decimal_bit(bits: u128, index: usize) -> bool {
    (bits >> (127 - index)) & 1 == 1
}

fn decimal_field(bits: u128, start: usize, len: usize) -> u128 {
    let shift = 128 - start - len;
    (bits >> shift) & ((1u128 << len) - 1)
}

fn decimal128_bits_to_i64(bytes: [u8; 16]) -> i64 {
    let bits = u128::from_le_bytes(bytes);
    let negative = decimal_bit(bits, 0);
    // Bits 1..=4 all set: NaN or Infinity. A finite coefficient never
    // uses that combination.
    if decimal_field(bits, 1, 4) == 0b1111 {
        if decimal_bit(bits, 5) {
            return 0;
        }
        return if negative { i64::MIN } else { i64::MAX };
    }

    let (exponent_at, prefix) = if decimal_field(bits, 1, 2) == 0b11 {
        (3, 0b100u128)
    } else {
        (1, 0u128)
    };
    let exponent = decimal_field(bits, exponent_at, 14) as i32 - 6176;
    let suffix_at = exponent_at + 14;
    let suffix_len = 128 - suffix_at;
    let suffix = decimal_field(bits, suffix_at, suffix_len);
    let mut coefficient = (prefix << suffix_len) | suffix;
    // 34 digits is the format maximum. A wider coefficient is not a
    // number; the parser treats it as zero rather than saturating.
    const MAX_COEFFICIENT: u128 = 9_999_999_999_999_999_999_999_999_999_999_999;
    if coefficient > MAX_COEFFICIENT {
        coefficient = 0;
    }
    scale_decimal(coefficient, exponent, negative)
}

/// `coefficient * 10^exponent`, truncated toward zero, saturated at the
/// int64 limits. A negative zero stays zero.
fn scale_decimal(mut magnitude: u128, exponent: i32, negative: bool) -> i64 {
    if magnitude == 0 {
        return 0;
    }
    if exponent >= 0 {
        for _ in 0..exponent {
            if magnitude > (i64::MAX as u128) / 10 {
                return if negative { i64::MIN } else { i64::MAX };
            }
            magnitude *= 10;
        }
    } else {
        for _ in 0..(-exponent) {
            magnitude /= 10;
            if magnitude == 0 {
                return 0;
            }
        }
    }
    if magnitude > i64::MAX as u128 {
        return if negative { i64::MIN } else { i64::MAX };
    }
    let magnitude = magnitude as u64;
    if !negative {
        magnitude as i64
    } else if magnitude > i64::MAX as u64 {
        i64::MIN
    } else {
        -(magnitude as i64)
    }
}

/// The `Display` round-trip this decoder used to apply to Decimal128.
///
/// Kept so a test can check the bit walk against the strings `"42.9"`,
/// `"1.05E+3"`, `"NaN"`, and `"Infinity"`. The decoder does not call it.
#[cfg(test)]
pub(crate) fn decimal_text_to_i64(text: &str) -> i64 {
    match text {
        "NaN" | "nan" | "NAN" => return 0,
        "Infinity" | "+Infinity" => return i64::MAX,
        "-Infinity" => return i64::MIN,
        _ => {}
    }

    let negative = text.starts_with('-');
    let body = text.trim_start_matches(['+', '-']);
    let (coeff, exponent) = match body.split_once(['e', 'E']) {
        Some((coeff, exp)) => match exp.parse::<i32>() {
            Ok(exp) => (coeff, exp),
            Err(_) => return 0,
        },
        None => (body, 0),
    };
    let (int_digits, frac_digits) = match coeff.split_once('.') {
        Some((int_digits, frac_digits)) => (int_digits, frac_digits),
        None => (coeff, ""),
    };
    if int_digits.is_empty()
        || !int_digits.bytes().all(|b| b.is_ascii_digit())
        || !frac_digits.bytes().all(|b| b.is_ascii_digit())
    {
        return 0;
    }

    let digits = format!("{int_digits}{frac_digits}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return 0;
    }

    // scale is the power of ten that turns the digit string into the
    // integer part. A negative scale means "drop digits", which is the
    // truncation toward zero.
    let scale = exponent - frac_digits.len() as i32;
    let mut magnitude = digits.to_string();
    if scale >= 0 {
        magnitude.extend(std::iter::repeat('0').take(scale as usize));
    } else {
        let drop = (-scale) as usize;
        if drop >= magnitude.len() {
            return 0;
        }
        magnitude.truncate(magnitude.len() - drop);
    }

    let parsed = match magnitude.parse::<u64>() {
        Ok(value) => value,
        // More digits than a u64. The int64 range is smaller still.
        Err(_) => return if negative { i64::MIN } else { i64::MAX },
    };
    if !negative {
        if parsed > i64::MAX as u64 {
            i64::MAX
        } else {
            parsed as i64
        }
    } else if parsed > i64::MAX as u64 {
        i64::MIN
    } else {
        -(parsed as i64)
    }
}

/// Join `key` onto `parent` the way the reference walk names a metric.
///
/// An empty parent means this field is at the root, so the name is `key`
/// itself. A leading dot used to be prefixed here and stripped again when
/// the column was published. `name.remove(0)` also deletes the first
/// character of a name that did not have that dot.
fn metric_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

/// Reduce a double the way `FTDCBSONUtil::extractMetricsFromDocument` does.
///
/// The server's comment on this conversion is the rule. Doubles that fall
/// outside int64 become:
///
/// * NaN → 0
/// * +Infinity, and any value `>= 2^63`, → `i64::MAX`
/// * -Infinity, and any value `< -2^63`, → `i64::MIN`
///
/// `2^63` is `BSONElement::kLongLongMaxPlusOneAsDouble`. The comparison in
/// the server is `!(value < kLongLongMaxPlusOneAsDouble)`, after NaN has
/// already been handled. `i64::MAX as f64` is that same `2^63`, because
/// `2^63 - 1` is not representable and rounds up.
///
/// Every remaining value is truncated toward zero (`static_cast<long long>`).
/// The int64 bit pattern is what gets stored in the `uint64_t` metric vector.
/// Casting the double straight to `u64` turns a negative into 0 and a value
/// past `2^63` into `u64::MAX` or the raw magnitude. Deltas were computed
/// from the int64 bits, so that reference is the wrong base.
pub(crate) fn double_to_metric(value: f64) -> u64 {
    if value.is_nan() {
        return 0;
    }
    let reduced = if !(value < i64::MAX as f64) {
        i64::MAX
    } else if value < i64::MIN as f64 {
        i64::MIN
    } else {
        value as i64
    };
    reduced as u64
}

/// Walk `root` in field order and append one [`MetricData`] per metric.
pub(crate) fn extract_metric_data_from_doc(
    doc_name: &str,
    root: Document,
    metric_data: &mut Vec<MetricData>,
) {
    for (key, value) in root {
        let path = metric_path(doc_name, &key);
        walk_value(&path, value, metric_data);
    }
}

/// One BSON value in the reference walk.
///
/// A document walks its fields. An array walks its elements with the
/// decimal index as the key (`members.0`, `members.1`). The element is
/// the value itself: a scalar is a metric, a document is walked, and a
/// nested array continues. Wrapping the element in a one-field document
/// built the same path and allocated a document per element to do it.
fn walk_value(path: &str, value: Bson, metric_data: &mut Vec<MetricData>) {
    match value {
        Bson::Double(v) => push_metric(path, double_to_metric(v), MetricKind::Double, metric_data),
        Bson::Int32(v) => push_metric(path, v as u64, MetricKind::Int, metric_data),
        Bson::Int64(v) => push_metric(path, v as u64, MetricKind::Int, metric_data),
        Bson::Decimal128(v) => {
            // `BSONElement::numberLong()` for NumberDecimal, which is
            // what the compressor calls. One uint64, not a sentinel.
            // The sentinel 999999999 used to be the base every delta
            // in this column was added to.
            push_metric(
                path,
                decimal128_to_metric(&v),
                MetricKind::Decimal,
                metric_data,
            );
        }
        Bson::Boolean(v) => push_metric(path, u64::from(v), MetricKind::Bool, metric_data),
        Bson::DateTime(v) => {
            push_metric(
                path,
                v.timestamp_millis() as u64,
                MetricKind::Date,
                metric_data,
            );
        }
        Bson::Timestamp(v) => {
            // Two metrics, seconds then increment. The names gain a
            // suffix so the two columns can be told apart; the order
            // is the order the compressor pushes them.
            push_metric(
                &format!("{path}_time"),
                u64::from(v.time),
                MetricKind::TimestampSeconds,
                metric_data,
            );
            push_metric(
                &format!("{path}_increment"),
                u64::from(v.increment),
                MetricKind::TimestampIncrement,
                metric_data,
            );
        }
        Bson::Document(sub_doc) => extract_metric_data_from_doc(path, sub_doc, metric_data),
        Bson::Array(items) => {
            for (index, item) in items.into_iter().enumerate() {
                let child = metric_path(path, &index.to_string());
                walk_value(&child, item, metric_data);
            }
        }
        // String, ObjectId, Null, Binary, Regex, JavaScript, Symbol,
        // Undefined, MinKey, MaxKey, DBPointer. Not metrics.
        _ => {}
    }
}

/// Parse the reference document at the front of an inflated chunk and walk it.
///
/// The reference document is a BSON document, so its own first four bytes
/// are its length. Bytes after that document are the chunk header and the
/// deltas; `Document::from_reader` stops at the end of the document.
pub(crate) fn extract_metric_meta_data(buffer: &[u8]) -> Result<Vec<MetricData>, FileDecoderError> {
    let mut base = buffer;

    match Document::from_reader(&mut base) {
        Ok(bson_doc) => {
            let mut ret_metrics: Vec<MetricData> = vec![];
            extract_metric_data_from_doc("", bson_doc, &mut ret_metrics);
            Ok(ret_metrics)
        }
        Err(e) => Err(FileDecoderError::format(e.to_string())),
    }
}
