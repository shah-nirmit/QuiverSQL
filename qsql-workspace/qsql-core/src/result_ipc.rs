//! Arrow IPC stream encoding for the paged result path.
//!
//! Phase 9 — adds an opt-in transport format for `QueryPage` so callers can
//! receive Arrow IPC bytes (base64-encoded for the JSON-RPC envelope) instead
//! of the verbose `Vec<serde_json::Value>` payload. JSON stays the default;
//! Arrow IPC preserves Arrow types end-to-end (no `i64 → f64` precision loss,
//! no decimal/timestamp coercion) and is materially faster + smaller for big
//! pages.
//!
//! The crate-level entry point is [`serialize_batches_to_ipc_base64`], which
//! slices the daemon's buffered `RecordBatch` queue to a single page's worth
//! of rows and writes them as an Arrow IPC stream into an in-memory buffer
//! before base64-encoding for the wire.
//!
//! `arrow-ipc` 58.3.0 is already a transitive dependency of DataFusion 53.1.0
//! so no new top-level Rust dep is needed for the IPC machinery — only the
//! direct `base64` pin in this crate's `Cargo.toml`.

use base64::Engine as _;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::VecDeque;

/// Accepted `result_format` discriminants on the JSON-RPC wire. Anything
/// outside this set yields a structured `-32602 Invalid params` error from
/// `QueryResultHandle::page`.
pub const RESULT_FORMAT_JSON: &str = "json";
pub const RESULT_FORMAT_ARROW_IPC: &str = "arrow_ipc";

/// Serializes a window of buffered `RecordBatch`es into a base64-encoded Arrow
/// IPC stream payload.
///
/// `start` and `len` are row coordinates into the **concatenation** of every
/// batch in `batches`, in the same order they're queued. The output IPC stream
/// always carries the schema header (so an empty range still decodes cleanly
/// at the client) and zero or more sliced `RecordBatch`es covering exactly the
/// requested rows.
///
/// Returns `Err(String)` only for unexpected Arrow / IO failures; valid empty
/// ranges and zero-length batches are not errors.
pub fn serialize_batches_to_ipc_base64(
    batches: &VecDeque<RecordBatch>,
    start: usize,
    len: usize,
    schema: &SchemaRef,
) -> Result<String, String> {
    let mut buf: Vec<u8> = Vec::with_capacity(estimate_buffer_capacity(batches, len));
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("Failed to create Arrow IPC StreamWriter: {e}"))?;

    if len > 0 {
        let mut row_cursor = 0_usize;
        let mut remaining = len;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let batch_rows = batch.num_rows();
            let batch_end = row_cursor + batch_rows;

            // Skip whole batches that fall before `start`.
            if batch_end <= start {
                row_cursor = batch_end;
                continue;
            }

            // Compute the (offset, length) slice that overlaps `start..start+len`.
            let slice_offset = start.saturating_sub(row_cursor);
            let slice_max = batch_rows.saturating_sub(slice_offset);
            let slice_len = remaining.min(slice_max);

            if slice_len > 0 {
                let sliced = batch.slice(slice_offset, slice_len);
                writer
                    .write(&sliced)
                    .map_err(|e| format!("Failed to write Arrow IPC batch: {e}"))?;
                remaining -= slice_len;
            }
            row_cursor = batch_end;
        }
    }

    writer
        .finish()
        .map_err(|e| format!("Failed to finalise Arrow IPC stream: {e}"))?;
    drop(writer);

    Ok(base64::engine::general_purpose::STANDARD.encode(&buf))
}

/// Rough upper-bound on the IPC stream byte size for the requested row count.
/// Used as a `Vec::with_capacity` hint only — never relied on for correctness.
/// Picks a small floor (8 KiB) so empty / tiny pages don't churn many tiny
/// allocations during the StreamWriter's header writes.
fn estimate_buffer_capacity(batches: &VecDeque<RecordBatch>, rows: usize) -> usize {
    const FLOOR_BYTES: usize = 8 * 1024;
    if rows == 0 || batches.is_empty() {
        return FLOOR_BYTES;
    }
    // Heuristic: ~64 bytes per row per column. Cheap and good enough for the
    // capacity hint — the IPC writer grows the buffer as needed regardless.
    let cols = batches.front().map(|b| b.num_columns()).unwrap_or(1);
    FLOOR_BYTES.max(rows.saturating_mul(64).saturating_mul(cols))
}

/// Validates a user-supplied `result_format` string against the accepted set.
/// Returns `Ok(canonical)` (where `None` and `Some("json")` both collapse to
/// `"json"`) or `Err(invalid_value)` so callers can build a structured error.
pub fn canonicalise_result_format(raw: Option<&str>) -> Result<&'static str, String> {
    match raw {
        None | Some(RESULT_FORMAT_JSON) => Ok(RESULT_FORMAT_JSON),
        Some(RESULT_FORMAT_ARROW_IPC) => Ok(RESULT_FORMAT_ARROW_IPC),
        Some(other) => Err(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::sync::Arc;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn sample_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = sample_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn decode_ipc(base64_payload: &str) -> (SchemaRef, Vec<RecordBatch>) {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_payload)
            .expect("valid base64");
        let reader =
            StreamReader::try_new(std::io::Cursor::new(bytes), None).expect("valid IPC stream");
        let schema = reader.schema();
        let batches = reader
            .into_iter()
            .map(|r| r.expect("decode batch"))
            .collect();
        (schema, batches)
    }

    #[test]
    fn round_trip_preserves_every_cell_value() {
        let mut batches = VecDeque::new();
        batches.push_back(sample_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Carol"]));
        let schema = sample_schema();

        let payload = serialize_batches_to_ipc_base64(&batches, 0, 3, &schema).expect("serialize");
        let (decoded_schema, decoded_batches) = decode_ipc(&payload);

        assert_eq!(decoded_schema.fields().len(), 2);
        assert_eq!(decoded_batches.len(), 1);
        let b = &decoded_batches[0];
        assert_eq!(b.num_rows(), 3);
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Carol");
    }

    #[test]
    fn slice_window_spans_multiple_batches() {
        // Two batches of 100 rows each; request rows 50..150 (which straddles
        // the batch boundary). Decoded IPC must contain exactly the right 100
        // rows in the right order.
        let mut batches = VecDeque::new();
        let first_ids: Vec<i64> = (0..100).collect();
        let first_names: Vec<String> = (0..100).map(|i| format!("row_{i}")).collect();
        let first_name_refs: Vec<&str> = first_names.iter().map(String::as_str).collect();
        batches.push_back(sample_batch(first_ids, first_name_refs));

        let second_ids: Vec<i64> = (100..200).collect();
        let second_names: Vec<String> = (100..200).map(|i| format!("row_{i}")).collect();
        let second_name_refs: Vec<&str> = second_names.iter().map(String::as_str).collect();
        batches.push_back(sample_batch(second_ids, second_name_refs));

        let schema = sample_schema();
        let payload =
            serialize_batches_to_ipc_base64(&batches, 50, 100, &schema).expect("serialize");
        let (_, decoded) = decode_ipc(&payload);

        let total_rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);

        // First decoded row corresponds to id=50; last corresponds to id=149.
        let first_ids = decoded[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first_ids.value(0), 50);
        let last = decoded.last().unwrap();
        let last_ids = last
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(last_ids.value(last_ids.len() - 1), 149);
    }

    #[test]
    fn empty_range_yields_schema_only_stream() {
        let mut batches = VecDeque::new();
        batches.push_back(sample_batch(vec![1], vec!["only"]));
        let schema = sample_schema();

        let payload = serialize_batches_to_ipc_base64(&batches, 0, 0, &schema).expect("serialize");
        let (decoded_schema, decoded_batches) = decode_ipc(&payload);
        assert_eq!(decoded_schema.fields().len(), 2);
        assert_eq!(decoded_batches.len(), 0);
    }

    #[test]
    fn canonicalise_result_format_accepts_known_values() {
        assert_eq!(canonicalise_result_format(None).unwrap(), "json");
        assert_eq!(canonicalise_result_format(Some("json")).unwrap(), "json");
        assert_eq!(
            canonicalise_result_format(Some("arrow_ipc")).unwrap(),
            "arrow_ipc"
        );
    }

    #[test]
    fn canonicalise_result_format_rejects_unknown_values() {
        let err = canonicalise_result_format(Some("totally_unknown")).unwrap_err();
        assert!(err.contains("totally_unknown"));
    }

    #[test]
    fn slice_starting_past_end_produces_schema_only_stream() {
        let mut batches = VecDeque::new();
        batches.push_back(sample_batch(vec![1, 2], vec!["a", "b"]));
        let schema = sample_schema();

        let payload = serialize_batches_to_ipc_base64(&batches, 10, 5, &schema).expect("serialize");
        let (_, decoded) = decode_ipc(&payload);
        let total_rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
