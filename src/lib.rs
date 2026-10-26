//! MongoDB FTDC diagnostic archives, decoded into columnar metrics.
//!
//! An archive (`metrics.<utc>-<uniquifier>`, or `metrics.interim`) is a
//! concatenation of BSON documents. The modules follow that layout:
//!
//! * `archive` reads each length-prefixed document and records it by `type`.
//!   Type 0 is metadata, type 1 is a compressed metric chunk, type 2 is a
//!   periodic metadata delta. The wrapper `_id` (and, for type 2, `count`)
//!   is kept with the embedded `doc`. A type this build does not know is
//!   skipped.
//! * `decode` inflates a metric chunk and rebuilds each series from the
//!   reference sample plus the delta block.
//! * `extract` walks the reference document and keeps the numeric fields
//!   the server compressor keeps.
//! * `varint` reads one unsigned LEB128 integer from the delta block.
//! * `export` writes the aligned series to Parquet. A sample that did not
//!   exist is null. That module is behind the `parquet` feature, which is
//!   on by default. Decoding and the metadata sidecar do not need it.
//!
//! Chunks are inflated and appended one at a time, in file order, which
//! is the order of the samples.

mod archive;
mod decode;
mod errors;
#[cfg(feature = "parquet")]
mod export;
mod extract;
mod varint;

pub use archive::{Archive, MetadataDelta, MetadataDocument, MetricChunk};
pub use errors::FileDecoderError;
pub use extract::{MetricColumn, MetricKind};

#[cfg(test)]
pub(crate) use extract::decimal_text_to_i64;

#[cfg(test)]

mod tests {
    use crate::{Archive, MetricColumn, MetricKind};
    use indexmap::IndexMap;

    #[test]
    fn test_file_doesnt_exist() {
        let error = Archive::open("unexistent").unwrap_err();
        let message = error.to_string();
        assert!(message.contains("unexistent"), "{message}");
        assert!(message.contains("failed to open"), "{message}");
    }

    fn open_decoded(path: impl AsRef<std::path::Path>) -> IndexMap<String, MetricColumn> {
        Archive::open(path).unwrap().decode(|_| true).unwrap()
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

        let parsed_file = Archive::open(&path).expect("type 2 and an unknown type are not fatal");

        assert_eq!(parsed_file.metadata.len(), 1);
        assert_eq!(
            parsed_file.metadata[0].doc.get_str("host").unwrap(),
            "a.example"
        );
        assert!(parsed_file.metadata[0].id.is_some());
        assert_eq!(parsed_file.metadata_deltas.len(), 2);
        assert_eq!(parsed_file.metadata_deltas[0].count, 0);
        assert_eq!(
            parsed_file.metadata_deltas[0].doc.get_i32("param").unwrap(),
            1
        );
        assert_eq!(parsed_file.metadata_deltas[1].count, 1);
        assert_eq!(
            parsed_file.metadata_deltas[1].doc.get_i32("param").unwrap(),
            2
        );
        assert_eq!(parsed_file.skipped_chunk_types, vec![9]);
        assert_eq!(parsed_file.document_count, 4);
        assert!(parsed_file.chunks.is_empty());

        // A second open is a second file. The first archive does not grow.
        let again = Archive::open(&path).unwrap();
        assert_eq!(again.document_count, parsed_file.document_count);
        assert_eq!(parsed_file.document_count, 4);

        let _ = std::fs::remove_file(&path);
    }

    /// Encode `n` as the unsigned LEB128 that [`unpack`] reads back.
    fn encode_varint(mut n: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if n == 0 {
                break;
            }
        }
        out
    }

    /// Encode a column-major delta list the way the compressor does.
    ///
    /// A run of *k* zeros becomes the two varints `[0, k - 1]`. The
    /// first 0 is a zero delta. The second number is how many more
    /// zeros follow it, not how many zeros there were in total.
    fn encode_deltas(deltas: &[u64]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut index = 0;
        while index < deltas.len() {
            if deltas[index] != 0 {
                out.extend(encode_varint(deltas[index]));
                index += 1;
                continue;
            }
            let start = index;
            while index < deltas.len() && deltas[index] == 0 {
                index += 1;
            }
            let run = (index - start) as u64;
            out.extend(encode_varint(0));
            out.extend(encode_varint(run - 1));
        }
        out
    }

    /// Build a one-chunk FTDC file and return its path.
    ///
    /// `sample_count` is the header value: the number of deltas per
    /// metric, not the number of samples. `deltas` is column-major,
    /// metric 0's deltas first.
    fn write_one_chunk(
        reference: &bson::Document,
        metric_count: u32,
        sample_count: u32,
        deltas: &[u64],
    ) -> std::path::PathBuf {
        write_chunk(reference, metric_count, sample_count, deltas, &[])
    }

    /// `tail` is appended to the delta block before zlib. A non-empty tail
    /// is a chunk whose deltas do not end on the last sample.
    fn write_chunk(
        reference: &bson::Document,
        metric_count: u32,
        sample_count: u32,
        deltas: &[u64],
        tail: &[u8],
    ) -> std::path::PathBuf {
        use std::io::Write;

        let mut payload = Vec::new();
        reference.to_writer(&mut payload).unwrap();
        payload.extend_from_slice(&metric_count.to_le_bytes());
        payload.extend_from_slice(&sample_count.to_le_bytes());
        payload.extend(encode_deltas(deltas));
        payload.extend(tail);

        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&payload).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut blob = Vec::with_capacity(4 + compressed.len());
        blob.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        blob.extend(compressed);

        let wrapper = bson::doc! {
            "_id": bson::DateTime::from_millis(0),
            "type": 1i32,
            "data": bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: blob,
            }
        };
        let mut file = Vec::new();
        wrapper.to_writer(&mut file).unwrap();

        // Unique per call. Several tests build a chunk with the same
        // sample_count, and `cargo test` runs them on more than one thread.
        use std::sync::atomic::{AtomicU64, Ordering};
        static CHUNK_SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ftdc-rs-chunk-{}-{}.bin",
            std::process::id(),
            CHUNK_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, file).unwrap();
        path
    }

    /// The walkthrough in the server FTDC README, plus the two edges the
    /// fixed `[u64; 300]` buffer got wrong.
    ///
    /// Four documents `{a, x, s}` become a reference of `{a:1, x:2}`
    /// (the string is not a metric) and the delta block `[1, 1, 1, 0, 2]`.
    /// The reconstructed series have four points, not three. A chunk
    /// with `sample_count == 0` still yields the reference. A chunk with
    /// 300 deltas used to write `metric_values[300]` and panic.
    #[test]
    fn reference_sample_is_kept_and_a_full_chunk_does_not_panic() {
        let reference = bson::doc! { "a": 1i32, "x": 2i32, "s": "t" };
        // a grows 1,2,3,4. x stays 2. Three zeros encode as [0, 2].
        let path = write_one_chunk(&reference, 2, 3, &[1, 1, 1, 0, 0, 0]);
        let series = open_decoded(&path);
        assert_eq!(
            series.get("a").map(|column| column.values.as_slice()),
            Some([Some(1), Some(2), Some(3), Some(4)].as_slice())
        );
        assert_eq!(
            series.get("x").map(|column| column.values.as_slice()),
            Some([Some(2), Some(2), Some(2), Some(2)].as_slice())
        );
        let _ = std::fs::remove_file(&path);

        let reference_only = write_one_chunk(&reference, 2, 0, &[]);
        let series = open_decoded(&reference_only);
        assert_eq!(
            series.get("a").map(|column| column.values.as_slice()),
            Some([Some(1)].as_slice())
        );
        assert_eq!(
            series.get("x").map(|column| column.values.as_slice()),
            Some([Some(2)].as_slice())
        );
        let _ = std::fs::remove_file(&reference_only);

        let wide = bson::doc! { "n": 0i64 };
        let path = write_one_chunk(&wide, 1, 300, &vec![1u64; 300]);
        let series = open_decoded(&path);
        let values = &series.get("n").expect("metric n").values;
        assert_eq!(values.len(), 301);
        assert_eq!(values[0], Some(0));
        assert_eq!(values[300], Some(300));
        let _ = std::fs::remove_file(&path);
    }

    /// ObjectId and string are not metrics. Decimal128 is one int64,
    /// truncated toward zero. An array element is `key.<index>`.
    /// A header count that does not match the walk is an error.
    #[test]
    fn extraction_matches_the_compressor_metric_list() {
        let oid = bson::oid::ObjectId::from_bytes([0x11; 12]);
        let reference = bson::doc! {
            "label": "host",
            "id": oid,
            "n": 7i32,
            "ok": true,
        };
        let path = write_one_chunk(&reference, 2, 0, &[]);
        let series = open_decoded(&path);
        assert_eq!(series.len(), 2, "objectid and string must not be columns");
        assert_eq!(
            series.get("n").map(|column| column.values.as_slice()),
            Some([Some(7)].as_slice())
        );
        assert_eq!(
            series.get("ok").map(|column| column.values.as_slice()),
            Some([Some(1)].as_slice())
        );
        assert!(!series.contains_key("id"));
        assert!(!series.contains_key("label"));
        let _ = std::fs::remove_file(&path);

        // Counting the ObjectId would make the walk 3 long. The header
        // says 3 on purpose, which is what a decoder that emits ObjectIds
        // would believe. The lengths disagree, so the delta block must
        // not be applied.
        let path = write_one_chunk(&reference, 3, 0, &[]);
        let parsed = Archive::open(&path).unwrap();
        let inflated = crate::decode::inflate_owned(&parsed.chunks[0].bytes).unwrap();
        let mut metrics = crate::extract::extract_metric_meta_data(&inflated).unwrap();
        let mut keep_all = |_: &str| true;
        let err =
            crate::decode::extract_all_metrics_from_buffer(&mut metrics, &inflated, &mut keep_all)
                .expect_err("a walk that does not match metric_count must fail");
        let message = err.to_string();
        assert!(message.contains("3"), "{message}");
        let _ = std::fs::remove_file(&path);

        let decimal: bson::Decimal128 = "42.9".parse().unwrap();
        let reference = bson::doc! { "amount": decimal };
        let path = write_one_chunk(&reference, 1, 0, &[]);
        let series = open_decoded(&path);
        assert_eq!(
            series.get("amount").map(|column| column.values.as_slice()),
            Some([Some(42)].as_slice())
        );
        let _ = std::fs::remove_file(&path);

        let reference = bson::doc! {
            "members": [ { "health": 1i32 }, { "health": 2i32 } ]
        };
        let path = write_one_chunk(&reference, 2, 0, &[]);
        let series = open_decoded(&path);
        assert_eq!(
            series
                .get("members.0.health")
                .map(|column| column.values.as_slice()),
            Some([Some(1)].as_slice())
        );
        assert_eq!(
            series
                .get("members.1.health")
                .map(|column| column.values.as_slice()),
            Some([Some(2)].as_slice())
        );
        let _ = std::fs::remove_file(&path);

        // The text parser is the old `Display` round-trip. The decoder now
        // reads the 16 IEEE bits. Both have to agree with `numberLong`.
        for (text, expected) in [
            ("42.9", 42i64),
            ("-3.2", -3),
            ("1.05E+3", 1050),
            ("0.5", 0),
            ("NaN", 0),
            ("Infinity", i64::MAX),
            ("-Infinity", i64::MIN),
            ("1E+20", i64::MAX),
        ] {
            assert_eq!(super::decimal_text_to_i64(text), expected, "{text}");
            let parsed: bson::Decimal128 = text.parse().unwrap();
            assert_eq!(
                crate::extract::decimal128_to_metric(&parsed),
                expected as u64,
                "{text}"
            );
        }

        let scalars = bson::doc! { "ns": [1i32, 2i32] };
        let path = write_one_chunk(&scalars, 2, 0, &[]);
        let series = open_decoded(&path);
        assert_eq!(
            series.get("ns.0").map(|column| column.values.as_slice()),
            Some([Some(1)].as_slice())
        );
        assert_eq!(
            series.get("ns.1").map(|column| column.values.as_slice()),
            Some([Some(2)].as_slice())
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Two chunks must concatenate in file order, and a chunk whose
    /// metric_count does not match the walk must fail the whole decode.
    /// Both used to be swallowed: workers appended under a mutex, and
    /// the worker's Result was discarded.
    #[test]
    fn chunks_stay_in_file_order_and_errors_are_returned() {
        let first = write_one_chunk(&bson::doc! { "n": 10i32 }, 1, 0, &[]);
        let second = write_one_chunk(&bson::doc! { "n": 20i32 }, 1, 0, &[]);
        let mut bytes = std::fs::read(&first).unwrap();
        bytes.extend(std::fs::read(&second).unwrap());
        let path = std::env::temp_dir().join(format!("ftdc-rs-order-{}.bin", std::process::id()));
        std::fs::write(&path, &bytes).unwrap();

        let series = open_decoded(&path);
        assert_eq!(
            series.get("n").map(|column| column.values.as_slice()),
            Some([Some(10), Some(20)].as_slice()),
            "chunk 0's sample has to come before chunk 1's"
        );
        let _ = std::fs::remove_file(&first);
        let _ = std::fs::remove_file(&second);
        let _ = std::fs::remove_file(&path);

        let oid = bson::oid::ObjectId::from_bytes([0x22; 12]);
        let reference = bson::doc! { "id": oid, "n": 1i32 };
        let path = write_one_chunk(&reference, 2, 0, &[]);
        let err = Archive::open(&path)
            .unwrap()
            .decode(|_| true)
            .expect_err("metric_count mismatch must fail decode");
        assert!(err.to_string().contains("2"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    /// A metric that appears in a later chunk is null until then. A metric
    /// that disappears is null afterwards. Neither side is filled with 0.
    #[test]
    fn schema_changes_are_null_not_zero() {
        let only_a = write_one_chunk(&bson::doc! { "a": 1i32 }, 1, 0, &[]);
        let a_and_b = write_one_chunk(&bson::doc! { "a": 2i32, "b": 3i32 }, 2, 0, &[]);
        let only_b = write_one_chunk(&bson::doc! { "b": 4i32 }, 1, 0, &[]);
        let mut bytes = std::fs::read(&only_a).unwrap();
        bytes.extend(std::fs::read(&a_and_b).unwrap());
        bytes.extend(std::fs::read(&only_b).unwrap());
        let path = std::env::temp_dir().join(format!("ftdc-rs-nulls-{}.bin", std::process::id()));
        std::fs::write(&path, bytes).unwrap();

        let series = open_decoded(&path);
        assert_eq!(
            series.get("a").map(|column| column.values.as_slice()),
            Some([Some(1), Some(2), None].as_slice())
        );
        assert_eq!(
            series.get("b").map(|column| column.values.as_slice()),
            Some([None, Some(3), Some(4)].as_slice())
        );

        #[cfg(feature = "parquet")]
        {
            let df = Archive::metrics_to_dataframe(series).unwrap();
            assert_eq!(df.height(), 3);
            assert_eq!(df.column("b").unwrap().u64().unwrap().get(0), None);
            assert_eq!(df.column("a").unwrap().u64().unwrap().get(2), None);
        }

        let _ = std::fs::remove_file(&only_a);
        let _ = std::fs::remove_file(&a_and_b);
        let _ = std::fs::remove_file(&only_b);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn test_to_dataframe() {
        let mut test_metrics: IndexMap<String, MetricColumn> = IndexMap::new();
        test_metrics.insert(
            "metric1".to_string(),
            MetricColumn {
                kind: MetricKind::Int,
                values: vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
            },
        );
        test_metrics.insert(
            "metric2".to_string(),
            MetricColumn {
                kind: MetricKind::Int,
                values: vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
            },
        );
        // Already aligned. The nulls are the samples that did not exist.
        // The builder must not turn them into 0.
        test_metrics.insert(
            "metric3".to_string(),
            MetricColumn {
                kind: MetricKind::Int,
                values: vec![Some(100), Some(200), Some(300), None, None],
            },
        );

        let df = Archive::metrics_to_dataframe(test_metrics).unwrap();
        assert_eq!(df.width(), 3);
        assert_eq!(df.height(), 5);
        // Insertion order is the column order. metric1 was inserted first.
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        assert_eq!(
            column_names,
            vec![
                "metric1".to_string(),
                "metric2".to_string(),
                "metric3".to_string()
            ]
        );

        let metric1_col = df.column("metric1").unwrap();
        let metric2_col = df.column("metric2").unwrap();
        let metric3_col = df.column("metric3").unwrap();
        assert_eq!(metric1_col.u64().unwrap().get(0), Some(1));
        assert_eq!(metric2_col.u64().unwrap().get(0), Some(10));
        assert_eq!(metric3_col.u64().unwrap().get(0), Some(100));
        assert_eq!(metric3_col.u64().unwrap().get(3), None);
        assert_eq!(metric3_col.u64().unwrap().get(4), None);

        let mut ragged = IndexMap::new();
        ragged.insert(
            "a".to_string(),
            MetricColumn {
                kind: MetricKind::Int,
                values: vec![Some(1u64), Some(2)],
            },
        );
        ragged.insert(
            "b".to_string(),
            MetricColumn {
                kind: MetricKind::Int,
                values: vec![Some(3u64)],
            },
        );
        assert!(Archive::metrics_to_dataframe(ragged).is_err());
    }

    /// Each document in an archive starts with its BSON length. Ending on a
    /// document boundary is a finished file. Ending inside the 4-byte length,
    /// or inside the body the length promised, is a truncated archive.
    #[test]
    fn truncated_document_is_an_error_and_empty_file_is_empty() {
        let dir = std::env::temp_dir();
        let empty = dir.join(format!("ftdc-rs-empty-{}.bin", std::process::id()));
        std::fs::write(&empty, []).unwrap();
        let parsed = Archive::open(&empty).unwrap();
        assert_eq!(parsed.document_count, 0);
        let _ = std::fs::remove_file(&empty);

        let short = dir.join(format!("ftdc-rs-short-{}.bin", std::process::id()));
        std::fs::write(&short, [1u8, 0]).unwrap();
        let err = Archive::open(&short).unwrap_err();
        assert!(err.to_string().contains("middle"), "{err}");
        let _ = std::fs::remove_file(&short);

        // A complete metadata document, then a length that claims 50 bytes
        // of which only the length and three more bytes are present.
        let mut bytes = Vec::new();
        bson::doc! { "type": 0i32, "doc": { "host": "a" } }
            .to_writer(&mut bytes)
            .unwrap();
        bytes.extend_from_slice(&50u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 3]);
        let cut = dir.join(format!("ftdc-rs-cut-{}.bin", std::process::id()));
        std::fs::write(&cut, &bytes).unwrap();
        let err = Archive::open(&cut).unwrap_err();
        assert!(err.to_string().contains("middle"), "{err}");
        let _ = std::fs::remove_file(&cut);
    }

    /// Doubles are reduced to int64 and then stored as that bit pattern.
    /// Column order is the reference document's field order. A metric that
    /// shows up in a later chunk is appended after the names already seen,
    /// including when its name sorts earlier.
    #[test]
    fn doubles_follow_int64_range_rules_and_columns_keep_field_order() {
        assert_eq!(crate::extract::double_to_metric(f64::NAN), 0);
        assert_eq!(
            crate::extract::double_to_metric(f64::INFINITY),
            i64::MAX as u64
        );
        assert_eq!(
            crate::extract::double_to_metric(f64::NEG_INFINITY),
            i64::MIN as u64
        );
        assert_eq!(crate::extract::double_to_metric(1.9), 1);
        assert_eq!(crate::extract::double_to_metric(-1.9), (-1i64) as u64);
        assert_eq!(crate::extract::double_to_metric(-0.4), 0);

        // 2^63 is BSONElement::kLongLongMaxPlusOneAsDouble. The f64 just
        // below it is in range and is kept. 2^63 itself saturates.
        let two_63 = (1u64 << 63) as f64;
        assert_eq!(crate::extract::double_to_metric(two_63), i64::MAX as u64);
        let just_below = f64::from_bits(two_63.to_bits() - 1);
        assert!(just_below < two_63);
        assert_eq!(
            crate::extract::double_to_metric(just_below),
            just_below as u64
        );
        assert_eq!(crate::extract::double_to_metric(1e20), i64::MAX as u64);
        assert_eq!(crate::extract::double_to_metric(-1e20), i64::MIN as u64);

        // Field order is z, neg, nested.m, ts_time, ts_increment, wide.
        // Alphabetical order would put neg first. The nested name has no
        // leading dot. The timestamp is two columns, seconds then increment.
        let mut reference = bson::doc! {
            "z": 1.9,
            "neg": -1.9,
            "nested": { "m": 3i32 },
            "ts": bson::Timestamp { time: 5, increment: 7 },
        };
        reference.insert("wide", f64::INFINITY);
        let path = write_one_chunk(&reference, 6, 0, &[]);
        let series = open_decoded(&path);
        let names: Vec<&str> = series.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            ["z", "neg", "nested.m", "ts_time", "ts_increment", "wide"]
        );
        assert_eq!(
            series.get("z").map(|column| column.values.as_slice()),
            Some([Some(1)].as_slice())
        );
        assert_eq!(
            series.get("neg").map(|column| column.values.as_slice()),
            Some([Some((-1i64) as u64)].as_slice())
        );
        assert_eq!(
            series
                .get("nested.m")
                .map(|column| column.values.as_slice()),
            Some([Some(3)].as_slice())
        );
        assert_eq!(
            series.get("ts_time").map(|column| column.values.as_slice()),
            Some([Some(5)].as_slice())
        );
        assert_eq!(
            series
                .get("ts_increment")
                .map(|column| column.values.as_slice()),
            Some([Some(7)].as_slice())
        );
        assert_eq!(
            series.get("wide").map(|column| column.values.as_slice()),
            Some([Some(i64::MAX as u64)].as_slice())
        );
        assert_eq!(series["z"].kind, MetricKind::Double);
        assert_eq!(series["neg"].kind, MetricKind::Double);
        assert_eq!(series["nested.m"].kind, MetricKind::Int);
        assert_eq!(series["ts_time"].kind, MetricKind::TimestampSeconds);
        assert_eq!(series["ts_increment"].kind, MetricKind::TimestampIncrement);
        assert_eq!(series["wide"].kind, MetricKind::Double);

        #[cfg(feature = "parquet")]
        {
            let df = Archive::metrics_to_dataframe(series).unwrap();
            let columns: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|name| name.to_string())
                .collect();
            assert_eq!(
                columns,
                vec![
                    "z".to_string(),
                    "neg".to_string(),
                    "nested.m".to_string(),
                    "ts_time".to_string(),
                    "ts_increment".to_string(),
                    "wide".to_string()
                ]
            );
        }
        let _ = std::fs::remove_file(&path);

        // `m` sorts before `z`, and it is new in the second chunk, so it
        // is appended. It does not jump ahead of `z` and `a`.
        let first = write_one_chunk(&bson::doc! { "z": 1i32, "a": 2i32 }, 2, 0, &[]);
        let second = write_one_chunk(&bson::doc! { "m": 3i32, "z": 4i32 }, 2, 0, &[]);
        let mut bytes = std::fs::read(&first).unwrap();
        bytes.extend(std::fs::read(&second).unwrap());
        let ordered =
            std::env::temp_dir().join(format!("ftdc-rs-order-cols-{}.bin", std::process::id()));
        std::fs::write(&ordered, bytes).unwrap();
        let series = open_decoded(&ordered);
        let names: Vec<&str> = series.keys().map(String::as_str).collect();
        assert_eq!(names, ["z", "a", "m"]);
        assert_eq!(series["z"].kind, MetricKind::Int);
        assert_eq!(series["m"].kind, MetricKind::Int);
        assert_eq!(
            series.get("a").map(|column| column.values.as_slice()),
            Some([Some(2), None].as_slice())
        );
        assert_eq!(
            series.get("m").map(|column| column.values.as_slice()),
            Some([None, Some(3)].as_slice())
        );
        let _ = std::fs::remove_file(&first);
        let _ = std::fs::remove_file(&second);
        let _ = std::fs::remove_file(&ordered);
    }

    /// The sidecar lists every type 0 and type 2 document as relaxed
    /// extended JSON. The last document of each kind used to be stored
    /// alone, as `Document`'s `Display` text, and a type 2 delta was
    /// labelled `parameters`.
    #[test]
    fn metadata_sidecar_is_extended_json_of_every_document() {
        let chunk = write_one_chunk(&bson::doc! { "n": 4i32 }, 1, 0, &[]);
        let mut bytes = Vec::new();
        bson::doc! {
            "_id": bson::DateTime::from_millis(0),
            "type": 0i32,
            "doc": { "host": "a.example" }
        }
        .to_writer(&mut bytes)
        .unwrap();
        bson::doc! {
            "_id": bson::DateTime::from_millis(1),
            "type": 2i32,
            "count": 0i32,
            "doc": { "param": 1i32 }
        }
        .to_writer(&mut bytes)
        .unwrap();
        bson::doc! {
            "_id": bson::DateTime::from_millis(2),
            "type": 9i32,
            "doc": { "future": true }
        }
        .to_writer(&mut bytes)
        .unwrap();
        bytes.extend(std::fs::read(&chunk).unwrap());
        bson::doc! {
            "_id": bson::DateTime::from_millis(3),
            "type": 2i32,
            "count": 1i32,
            "doc": { "param": 2i32 }
        }
        .to_writer(&mut bytes)
        .unwrap();

        let dir = std::env::temp_dir();
        let input = dir.join(format!("ftdc-rs-meta-in-{}.bin", std::process::id()));
        let parquet = dir.join(format!("ftdc-rs-meta-{}.parquet", std::process::id()));
        let sidecar = dir.join(format!("ftdc-rs-meta-{}.metadata.json", std::process::id()));
        std::fs::write(&input, bytes).unwrap();

        // The sidecar does not need Polars. The Parquet file does, so a
        // build without the `parquet` feature still checks the JSON.
        #[cfg(feature = "parquet")]
        Archive::write_to_parquet(&input, &parquet, Some(sidecar.as_path()), |_| true).unwrap();
        #[cfg(not(feature = "parquet"))]
        {
            let archive = Archive::open(&input).unwrap();
            let metrics = archive.decode(|_| true).unwrap();
            archive.write_sidecar(&metrics, &sidecar).unwrap();
        }

        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
        assert!(json.get("parameters").is_none());
        assert_eq!(json["metadata"].as_array().unwrap().len(), 1);
        assert_eq!(json["metadata"][0]["doc"]["host"], "a.example");
        assert!(json["metadata"][0]["_id"]["$date"].is_string());
        assert_eq!(json["metadataDeltas"].as_array().unwrap().len(), 2);
        assert_eq!(json["metadataDeltas"][0]["count"], serde_json::json!(0));
        assert_eq!(
            json["metadataDeltas"][0]["doc"]["param"],
            serde_json::json!(1)
        );
        assert_eq!(json["metadataDeltas"][1]["count"], serde_json::json!(1));
        assert_eq!(
            json["metadataDeltas"][1]["doc"]["param"],
            serde_json::json!(2)
        );
        assert_eq!(json["documentCount"], serde_json::json!(5));
        assert_eq!(json["skippedChunkTypes"], serde_json::json!([9]));
        assert_eq!(json["metrics"], serde_json::json!(["n"]));
        assert_eq!(json["metricTypes"]["n"], "int");

        #[cfg(feature = "parquet")]
        {
            use polars::prelude::SerReader;
            let mut file = std::fs::File::open(&parquet).unwrap();
            let frame = polars::prelude::ParquetReader::new(&mut file)
                .finish()
                .unwrap();
            assert_eq!(frame.column("n").unwrap().u64().unwrap().get(0), Some(4));
        }

        let _ = std::fs::remove_file(&chunk);
        let _ = std::fs::remove_file(&input);
        let _ = std::fs::remove_file(&parquet);
        let _ = std::fs::remove_file(&sidecar);
    }

    /// The server FTDC README's four-document example, inside a small archive.
    ///
    /// The reference is `{a: 1, x: 2}` plus a string and an ObjectId, which
    /// are not metrics. Three deltas follow. `a` becomes 1, 2, 3, 4 and `x`
    /// stays 2. Counting the ObjectId would make the walk longer than
    /// `metric_count` and the decode would fail, or the deltas would land
    /// on the wrong column. The archive around the chunk is the usual
    /// prefix: type 0 metadata, then a type 2 delta, then the metric chunk.
    ///
    /// This stands in for the `diagnostic.data.8.0.11` files. Those are not
    /// in the repository, so a test that opens them cannot run.
    #[test]
    fn readme_example_round_trips_through_parquet() {
        let oid = bson::oid::ObjectId::from_bytes([0x33; 12]);
        let reference = bson::doc! {
            "a": 1i32,
            "x": 2i32,
            "s": "t",
            "id": oid,
        };
        let chunk = write_one_chunk(&reference, 2, 3, &[1, 1, 1, 0, 0, 0]);

        let mut bytes = Vec::new();
        bson::doc! {
            "_id": bson::DateTime::from_millis(0),
            "type": 0i32,
            "doc": {
                "host": "a.example",
                "start": bson::DateTime::from_millis(1_700_000_000_000),
            }
        }
        .to_writer(&mut bytes)
        .unwrap();
        bson::doc! {
            "_id": bson::DateTime::from_millis(1),
            "type": 2i32,
            "count": 0i32,
            "doc": { "param": 7i32 }
        }
        .to_writer(&mut bytes)
        .unwrap();
        bytes.extend(std::fs::read(&chunk).unwrap());

        let dir = std::env::temp_dir();
        let input = dir.join(format!("ftdc-rs-golden-{}.bin", std::process::id()));
        let parquet = dir.join(format!("ftdc-rs-golden-{}.parquet", std::process::id()));
        let sidecar = dir.join(format!(
            "ftdc-rs-golden-{}.metadata.json",
            std::process::id()
        ));
        std::fs::write(&input, bytes).unwrap();

        #[cfg(feature = "parquet")]
        Archive::write_to_parquet(&input, &parquet, Some(sidecar.as_path()), |_| true).unwrap();
        #[cfg(not(feature = "parquet"))]
        {
            let archive = Archive::open(&input).unwrap();
            let metrics = archive.decode(|_| true).unwrap();
            archive.write_sidecar(&metrics, &sidecar).unwrap();
        }

        #[cfg(feature = "parquet")]
        {
            use polars::prelude::SerReader;
            let mut file = std::fs::File::open(&parquet).unwrap();
            let frame = polars::prelude::ParquetReader::new(&mut file)
                .finish()
                .unwrap();
            let columns: Vec<String> = frame
                .get_column_names()
                .iter()
                .map(|name| name.to_string())
                .collect();
            assert_eq!(columns, vec!["a".to_string(), "x".to_string()]);
            assert_eq!(
                frame
                    .column("a")
                    .unwrap()
                    .u64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<_>>(),
                vec![1, 2, 3, 4]
            );
            assert_eq!(
                frame
                    .column("x")
                    .unwrap()
                    .u64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<_>>(),
                vec![2, 2, 2, 2]
            );
        }

        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
        assert_eq!(json["metadata"][0]["doc"]["host"], "a.example");
        assert!(json["metadata"][0]["doc"]["start"]["$date"].is_string());
        assert!(json["metadata"][0]["_id"]["$date"].is_string());
        assert_eq!(json["metadataDeltas"][0]["count"], serde_json::json!(0));
        assert_eq!(
            json["metadataDeltas"][0]["doc"]["param"],
            serde_json::json!(7)
        );
        assert_eq!(json["documentCount"], serde_json::json!(3));
        assert_eq!(json["metrics"], serde_json::json!(["a", "x"]));
        assert_eq!(json["metricTypes"]["a"], "int");
        assert_eq!(json["metricTypes"]["x"], "int");

        let _ = std::fs::remove_file(&chunk);
        let _ = std::fs::remove_file(&input);
        let _ = std::fs::remove_file(&parquet);
        let _ = std::fs::remove_file(&sidecar);
    }

    /// A zero run is one pair for the whole delta block. The pair here is
    /// `[0, 1]`, two zeros, and the second zero belongs to metric `b`.
    /// Resetting the run at the metric boundary would feed `b` the run
    /// length as a real delta.
    ///
    /// A delta of `u64::MAX` steps the counter backwards by one.
    #[test]
    fn zero_run_crosses_metrics_and_deltas_wrap() {
        let reference = bson::doc! { "a": 1i64, "b": 2i64 };
        let path = write_one_chunk(&reference, 2, 2, &[5, 0, 0, 7]);
        let series = open_decoded(&path);
        assert_eq!(
            series.get("a").map(|column| column.values.as_slice()),
            Some([Some(1), Some(6), Some(6)].as_slice())
        );
        assert_eq!(
            series.get("b").map(|column| column.values.as_slice()),
            Some([Some(2), Some(2), Some(9)].as_slice())
        );
        let _ = std::fs::remove_file(&path);

        let path = write_one_chunk(&bson::doc! { "n": 5i64 }, 1, 1, &[u64::MAX]);
        let series = open_decoded(&path);
        assert_eq!(
            series.get("n").map(|column| column.values.as_slice()),
            Some([Some(5), Some(4)].as_slice())
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The delta block has to end on the last sample. Extra zeros in a run,
    /// bytes after the last varint, and a varint that runs off the buffer
    /// are the same class of truncated or padded chunk.
    #[test]
    fn delta_block_must_end_on_the_last_sample() {
        let reference = bson::doc! { "n": 1i64 };
        let leftover = write_one_chunk(&reference, 1, 1, &[0, 0, 0]);
        let err = Archive::open(&leftover)
            .unwrap()
            .decode(|_| true)
            .unwrap_err();
        assert!(err.to_string().contains("zeros past"), "{err}");
        let _ = std::fs::remove_file(&leftover);

        let trailing = write_chunk(&reference, 1, 0, &[], &[0x01]);
        let err = Archive::open(&trailing)
            .unwrap()
            .decode(|_| true)
            .unwrap_err();
        assert!(err.to_string().contains("trailing"), "{err}");
        let _ = std::fs::remove_file(&trailing);

        let short = write_one_chunk(&reference, 1, 1, &[]);
        let err = Archive::open(&short).unwrap().decode(|_| true).unwrap_err();
        assert!(err.to_string().contains("end"), "{err}");
        let _ = std::fs::remove_file(&short);

        let wide_bytes = [0x80u8; 11];
        let mut wide = crate::varint::RangeCursor::new(&wide_bytes);
        let err = crate::varint::unpack(&mut wide).unwrap_err();
        assert!(err.to_string().contains("64"), "{err}");
    }

    /// The leading uint32 is the inflated length. It has to match the zlib
    /// output, and it has to be within the server's 10_000_000 byte cap.
    /// A framed length past 16 MiB is rejected before the body is read.
    #[test]
    fn chunk_sizes_have_to_match_the_header() {
        let path = write_one_chunk(&bson::doc! { "n": 1i32 }, 1, 0, &[]);
        let bytes = std::fs::read(&path).unwrap();
        let mut document = bson::Document::from_reader(&mut bytes.as_slice()).unwrap();
        let bson::Bson::Binary(binary) = document.get_mut("data").unwrap() else {
            panic!("data is BinData");
        };
        let declared = u32::from_le_bytes(binary.bytes[0..4].try_into().unwrap());
        binary.bytes[0..4].copy_from_slice(&(declared - 1).to_le_bytes());
        let mismatched = std::env::temp_dir().join(format!(
            "ftdc-rs-size-{}-{}.bin",
            std::process::id(),
            declared
        ));
        let mut rewritten = Vec::new();
        document.to_writer(&mut rewritten).unwrap();
        std::fs::write(&mismatched, rewritten).unwrap();
        let err = Archive::open(&mismatched)
            .unwrap()
            .decode(|_| true)
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("does not match"), "{message}");
        assert!(message.contains(&(declared - 1).to_string()), "{message}");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&mismatched);

        let mut document = bson::Document::from_reader(&mut bytes.as_slice()).unwrap();
        let bson::Bson::Binary(binary) = document.get_mut("data").unwrap() else {
            panic!("data is BinData");
        };
        binary.bytes[0..4].copy_from_slice(&20_000_000u32.to_le_bytes());
        let huge = std::env::temp_dir().join(format!("ftdc-rs-huge-{}.bin", std::process::id()));
        let mut rewritten = Vec::new();
        document.to_writer(&mut rewritten).unwrap();
        std::fs::write(&huge, rewritten).unwrap();
        let err = Archive::open(&huge).unwrap().decode(|_| true).unwrap_err();
        assert!(err.to_string().contains("10000000"), "{err}");
        let _ = std::fs::remove_file(&huge);

        let framed = std::env::temp_dir().join(format!("ftdc-rs-frame-{}.bin", std::process::id()));
        let claimed = (16 * 1024 * 1024 + 1) as u32;
        std::fs::write(&framed, claimed.to_le_bytes()).unwrap();
        let err = Archive::open(&framed).unwrap_err();
        assert!(err.to_string().contains("16777216"), "{err}");
        let _ = std::fs::remove_file(&framed);
    }

    /// A filter drops columns after their deltas have been consumed. A
    /// metric that the filter keeps, and that first appears in a later
    /// chunk, is null for the earlier rows.
    #[test]
    fn filter_drops_columns_without_shifting_rows() {
        let first = write_one_chunk(&bson::doc! { "a": 1i32, "b": 2i32 }, 2, 0, &[]);
        let second = write_one_chunk(&bson::doc! { "b": 3i32, "c": 4i32 }, 2, 0, &[]);
        let mut bytes = std::fs::read(&first).unwrap();
        bytes.extend(std::fs::read(&second).unwrap());
        let path = std::env::temp_dir().join(format!("ftdc-rs-filter-{}.bin", std::process::id()));
        std::fs::write(&path, bytes).unwrap();

        let series = Archive::open(&path)
            .unwrap()
            .decode(|name| name == "c")
            .unwrap();
        let names: Vec<&str> = series.keys().map(String::as_str).collect();
        assert_eq!(names, ["c"]);
        assert_eq!(
            series.get("c").map(|column| column.values.as_slice()),
            Some([None, Some(4)].as_slice())
        );
        let _ = std::fs::remove_file(&first);
        let _ = std::fs::remove_file(&second);
        let _ = std::fs::remove_file(&path);
    }

    /// The kind is fixed by the chunk that introduces the name. int32 and
    /// int64 are both `int`. A double under the same name is a different rule.
    #[test]
    fn metric_kind_follows_the_reference_field() {
        let path = write_one_chunk(
            &bson::doc! { "n": 1i32, "flag": true, "when": bson::DateTime::from_millis(5) },
            3,
            0,
            &[],
        );
        let series = open_decoded(&path);
        assert_eq!(series["n"].kind, MetricKind::Int);
        assert_eq!(series["flag"].kind, MetricKind::Bool);
        assert_eq!(series["flag"].values, vec![Some(1)]);
        assert_eq!(series["when"].kind, MetricKind::Date);
        assert_eq!(series["when"].values, vec![Some(5)]);
        let _ = std::fs::remove_file(&path);

        let first = write_one_chunk(&bson::doc! { "n": 1i32 }, 1, 0, &[]);
        let second = write_one_chunk(&bson::doc! { "n": 1.5 }, 1, 0, &[]);
        let mut bytes = std::fs::read(&first).unwrap();
        bytes.extend(std::fs::read(&second).unwrap());
        let path = std::env::temp_dir().join(format!("ftdc-rs-kind-{}.bin", std::process::id()));
        std::fs::write(&path, bytes).unwrap();
        let err = Archive::open(&path).unwrap().decode(|_| true).unwrap_err();
        assert!(err.to_string().contains("double"), "{err}");
        assert!(err.to_string().contains("int"), "{err}");
        let _ = std::fs::remove_file(&first);
        let _ = std::fs::remove_file(&second);
        let _ = std::fs::remove_file(&path);
    }
}
