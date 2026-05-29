//! Fixed-width text file support.
//!
//! Phase 8 turns the long-reserved `SourceKind::FixedWidth` variant into a
//! working source. Files in this format have no header row — the layout
//! (column name, byte-offset, length, type, nullability) must be supplied
//! out-of-band as a JSON sidecar file. This module owns:
//!
//! - [`FixedWidthLayout`] / [`FixedWidthField`] — the deserialised layout
//!   schema, with validation that overlapping spans, zero/negative lengths,
//!   empty `fields` lists, and unknown SQL type names all fail at load time.
//! - [`FixedWidthLayout::arrow_schema`] — the Arrow `Schema` derived from
//!   the layout. Type names go through [`crate::sql_types::sql_type_to_arrow`]
//!   so the layout file can use any SQL spelling the rest of QuiverSQL
//!   already understands (INTEGER, BIGINT, VARCHAR, DOUBLE, TIMESTAMP, …).
//!
//! The streaming `ExecutionPlan` and `TableProvider` that consume a layout
//! plus a data path land in 8B, in this same module.
//!
//! Layout file shape (`*.layout.json`):
//!
//! ```json
//! {
//!   "fields": [
//!     { "name": "id",     "start": 0,  "length": 6,  "type": "INTEGER", "nullable": false },
//!     { "name": "name",   "start": 6,  "length": 30, "type": "VARCHAR", "nullable": false, "trim": true },
//!     { "name": "salary", "start": 36, "length": 12, "type": "DOUBLE",  "nullable": true }
//!   ]
//! }
//! ```

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{exec_datafusion_err, project_schema};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sql_types::sql_type_to_arrow;

/// One column of a fixed-width record layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedWidthField {
    /// Column name as it appears in the resulting Arrow schema / SQL queries.
    pub name: String,
    /// Zero-based byte offset of the field's first byte within each row.
    pub start: usize,
    /// Field width in bytes. Must be > 0.
    pub length: usize,
    /// SQL type name — anything understood by `sql_type_to_arrow`.
    #[serde(rename = "type")]
    pub sql_type: String,
    /// Whether the field is nullable. When `true` a wholly-whitespace slice
    /// becomes `NULL`; when `false` it is a parse error.
    #[serde(default)]
    pub nullable: bool,
    /// Whether to ASCII-trim string-typed fields. Defaults to `true`.
    /// Non-string fields ignore this flag (they always trim — there's no
    /// way to parse "  42  " as an integer otherwise).
    #[serde(default = "default_trim")]
    pub trim: bool,
}

fn default_trim() -> bool {
    true
}

impl FixedWidthField {
    /// One-past-the-end byte offset of this field (`start + length`).
    pub fn end(&self) -> usize {
        self.start + self.length
    }
}

/// The full deserialised layout — an ordered list of column descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedWidthLayout {
    pub fields: Vec<FixedWidthField>,
}

impl FixedWidthLayout {
    /// Loads a layout from a JSON file on disk and validates it.
    pub fn from_json_path(path: impl AsRef<Path>) -> Result<Self, String> {
        let path_ref = path.as_ref();
        let raw = fs::read_to_string(path_ref).map_err(|e| {
            format!(
                "Failed to read fixed-width layout file '{}': {e}",
                path_ref.display()
            )
        })?;
        Self::from_json_str(&raw)
            .map_err(|e| format!("Layout file '{}' is invalid: {e}", path_ref.display()))
    }

    /// Parses + validates a layout from an in-memory JSON string.
    pub fn from_json_str(s: &str) -> Result<Self, String> {
        let layout: Self =
            serde_json::from_str(s).map_err(|e| format!("Invalid layout JSON: {e}"))?;
        layout.validate()?;
        Ok(layout)
    }

    /// Returns the Arrow `Schema` derived from this layout. Each field's
    /// `sql_type` is run through [`sql_type_to_arrow`] so the layout file
    /// can use any SQL type spelling QuiverSQL already understands.
    pub fn arrow_schema(&self) -> Result<SchemaRef, String> {
        let arrow_fields = self
            .fields
            .iter()
            .map(|f| {
                let dt = sql_type_to_arrow(&f.sql_type).map_err(|e| {
                    format!(
                        "Field '{}' has unrecognised type '{}': {e}",
                        f.name, f.sql_type
                    )
                })?;
                Ok(Field::new(&f.name, dt, f.nullable))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Arc::new(Schema::new(arrow_fields)))
    }

    /// Returns the minimum row width (bytes) implied by the layout — used
    /// by the stream parser to detect ragged rows.
    pub fn min_row_width(&self) -> usize {
        self.fields.iter().map(|f| f.end()).max().unwrap_or(0)
    }

    fn validate(&self) -> Result<(), String> {
        if self.fields.is_empty() {
            return Err("`fields` list is empty — at least one field is required".to_string());
        }

        // Each field's length must be positive.
        for field in &self.fields {
            if field.length == 0 {
                return Err(format!(
                    "Field '{}' has length 0 — every field must have length > 0",
                    field.name
                ));
            }
        }

        // No two fields may overlap. Compare every pair (O(n²) but n is tiny).
        for i in 0..self.fields.len() {
            for j in (i + 1)..self.fields.len() {
                let a = &self.fields[i];
                let b = &self.fields[j];
                if spans_overlap(a, b) {
                    return Err(format!(
                        "Fields '{}' ({}..{}) and '{}' ({}..{}) have overlapping byte spans",
                        a.name,
                        a.start,
                        a.end(),
                        b.name,
                        b.start,
                        b.end(),
                    ));
                }
            }
        }

        // Validate type names eagerly so a bad layout fails at load time
        // rather than at first scan. arrow_schema() will surface the error.
        let _ = self.arrow_schema()?;

        Ok(())
    }
}

fn spans_overlap(a: &FixedWidthField, b: &FixedWidthField) -> bool {
    a.start < b.end() && b.start < a.end()
}

// ────────────────────────────────────────────────────────────────────────────
// FixedWidthTableProvider — DataFusion TableProvider over a layout + data path
// ────────────────────────────────────────────────────────────────────────────

/// A `TableProvider` that exposes a fixed-width text file as a virtual table.
/// The schema is fully determined by the supplied [`FixedWidthLayout`]; the
/// data file is read lazily via a streaming [`FixedWidthExec`] when DataFusion
/// asks for a scan.
///
/// Filter pushdown is `Unsupported` in v1 — DataFusion wraps the scan in a
/// `FilterExec` instead, which works correctly but does not give us byte-level
/// short-circuiting on disk. A follow-up phase can revisit predicate-aware
/// row skipping if profiling motivates it.
#[derive(Debug)]
pub struct FixedWidthTableProvider {
    layout: Arc<FixedWidthLayout>,
    schema: SchemaRef,
    path: PathBuf,
}

impl FixedWidthTableProvider {
    /// Constructs a new provider, eagerly deriving the Arrow schema so any
    /// layout/type-mapping error surfaces at registration time rather than
    /// at first scan.
    pub fn new(layout: FixedWidthLayout, path: impl Into<PathBuf>) -> Result<Self, String> {
        let schema = layout.arrow_schema()?;
        Ok(Self {
            layout: Arc::new(layout),
            schema,
            path: path.into(),
        })
    }

    pub fn layout(&self) -> &FixedWidthLayout {
        &self.layout
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl TableProvider for FixedWidthTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Phase 8: nothing pushed. DataFusion wraps us in a FilterExec.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(FixedWidthExec::new(
            Arc::clone(&self.layout),
            self.path.clone(),
            projected_schema,
            projection.cloned(),
            limit,
        )))
    }
}

// ────────────────────────────────────────────────────────────────────────────
// FixedWidthExec — ExecutionPlan that streams batches from the file
// ────────────────────────────────────────────────────────────────────────────

/// Streaming `ExecutionPlan` that reads a fixed-width text file in batches of
/// up to [`FixedWidthExec::BATCH_SIZE`] rows. Single partition, `Bounded`,
/// `Incremental` — no parallelism for v1; if the file gets large enough that
/// this matters, a multi-partition split (by byte ranges) is a clean follow-up
/// since each row is independent.
#[derive(Debug)]
pub struct FixedWidthExec {
    layout: Arc<FixedWidthLayout>,
    path: PathBuf,
    /// Schema after applying the projection (if any).
    projected_schema: SchemaRef,
    /// Column indices selected by the query — `None` means "all columns".
    projection: Option<Vec<usize>>,
    /// Optional row cap honoured inside the stream so we never read past it.
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl FixedWidthExec {
    /// Rows per emitted `RecordBatch`. Matches DataFusion's default batch size
    /// so downstream operators don't need to re-batch.
    pub const BATCH_SIZE: usize = 8192;

    pub fn new(
        layout: Arc<FixedWidthLayout>,
        path: PathBuf,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            layout,
            path,
            projected_schema,
            projection,
            limit,
            properties,
        }
    }
}

impl DisplayAs for FixedWidthExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FixedWidthExec: path={} fields={}",
            self.path.display(),
            self.projected_schema.fields().len(),
        )?;
        if let Some(limit) = self.limit {
            write!(f, " limit={limit}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FixedWidthExec {
    fn name(&self) -> &str {
        "FixedWidthExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leaf node — DataFusion may still call this during rewrites; just
        // return ourselves unchanged.
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(exec_datafusion_err!(
                "FixedWidthExec has a single partition; got partition={partition}"
            ));
        }

        let file = File::open(&self.path).map_err(|e| {
            DataFusionError::External(
                format!(
                    "Failed to open fixed-width data file '{}': {e}",
                    self.path.display()
                )
                .into(),
            )
        })?;
        let reader = BufReader::new(file);

        let projected_schema = Arc::clone(&self.projected_schema);
        let layout = Arc::clone(&self.layout);
        let projection = self.projection.clone();
        let limit = self.limit;

        // Stream RecordBatches lazily by reading BATCH_SIZE lines at a time.
        // We use `unfold` so each call to `next()` reads exactly one batch
        // worth of lines — no buffering past the next batch.
        let initial = StreamState {
            reader,
            layout,
            projected_schema: Arc::clone(&projected_schema),
            projection,
            rows_emitted: 0,
            limit,
            done: false,
        };
        let stream = stream::unfold(initial, |mut state| async move {
            state.next_batch().map(|b| (b, state))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Row parser
// ────────────────────────────────────────────────────────────────────────────

struct StreamState {
    reader: BufReader<File>,
    layout: Arc<FixedWidthLayout>,
    projected_schema: SchemaRef,
    /// `None` means "all columns of the layout, in layout order".
    projection: Option<Vec<usize>>,
    rows_emitted: usize,
    limit: Option<usize>,
    done: bool,
}

impl StreamState {
    fn next_batch(&mut self) -> Option<DfResult<RecordBatch>> {
        if self.done {
            return None;
        }
        // Compute how many rows are still allowed before we hit the limit.
        let remaining = match self.limit {
            Some(l) => l.saturating_sub(self.rows_emitted),
            None => usize::MAX,
        };
        if remaining == 0 {
            self.done = true;
            return None;
        }
        let target = remaining.min(FixedWidthExec::BATCH_SIZE);

        // Read up to `target` lines from the BufReader.
        let mut lines: Vec<String> = Vec::with_capacity(target.min(1024));
        let mut buf = String::new();
        while lines.len() < target {
            buf.clear();
            match self.reader.read_line(&mut buf) {
                Ok(0) => {
                    // EOF
                    self.done = true;
                    break;
                }
                Ok(_) => {
                    // Strip trailing newline(s); keep the body bytes for
                    // byte-offset slicing.
                    let trimmed = buf.trim_end_matches(['\r', '\n']).to_string();
                    if trimmed.is_empty() {
                        continue;
                    }
                    lines.push(trimmed);
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(DataFusionError::External(
                        format!("Failed to read fixed-width row: {e}").into(),
                    )));
                }
            }
        }

        if lines.is_empty() {
            return None;
        }

        let row_offset_in_file = self.rows_emitted;
        let batch = build_batch(
            &lines,
            &self.layout,
            &self.projected_schema,
            self.projection.as_deref(),
            row_offset_in_file,
        );
        match batch {
            Ok(b) => {
                self.rows_emitted += lines.len();
                Some(Ok(b))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Builds a single `RecordBatch` from a slice of pre-read lines, applying
/// the projection if one is set.
fn build_batch(
    lines: &[String],
    layout: &FixedWidthLayout,
    projected_schema: &SchemaRef,
    projection: Option<&[usize]>,
    row_offset_in_file: usize,
) -> DfResult<RecordBatch> {
    // For each projected column, build an Arrow array by slicing each row's
    // bytes by (start, length) of the corresponding layout field.
    let selected: Vec<usize> = match projection {
        Some(p) => p.to_vec(),
        None => (0..layout.fields.len()).collect(),
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(selected.len());
    for &layout_idx in &selected {
        let field = &layout.fields[layout_idx];
        let arrow_field = &projected_schema.fields()[columns.len()];
        let array = build_column(lines, field, arrow_field.data_type(), row_offset_in_file)?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::clone(projected_schema), columns)
        .map_err(|e| DataFusionError::External(format!("Failed to build RecordBatch: {e}").into()))
}

/// Builds a single column from `lines` by slicing each line at the given
/// field's byte offsets and coercing to `data_type`. Rejects mid-codepoint
/// splits (treating them as parse errors with the row index in the message)
/// so we never emit corrupted UTF-8.
fn build_column(
    lines: &[String],
    field: &FixedWidthField,
    data_type: &DataType,
    row_offset_in_file: usize,
) -> DfResult<ArrayRef> {
    // Pre-slice each line into the field's substring once so the type-specific
    // builders below can focus on coercion, not byte math.
    let mut slices: Vec<Option<String>> = Vec::with_capacity(lines.len());
    for (i, line) in lines.iter().enumerate() {
        let row_idx = row_offset_in_file + i;
        let bytes = line.as_bytes();
        if bytes.len() < field.end() {
            return Err(DataFusionError::External(
                format!(
                    "Row {row_idx} is shorter than the layout requires (got {} bytes, need {} for field '{}')",
                    bytes.len(),
                    field.end(),
                    field.name
                )
                .into(),
            ));
        }
        let slice = &bytes[field.start..field.end()];
        let s = std::str::from_utf8(slice).map_err(|e| {
            DataFusionError::External(
                format!(
                    "Row {row_idx} field '{}' is not valid UTF-8 at bytes {}..{}: {e} (mid-codepoint split is not allowed)",
                    field.name,
                    field.start,
                    field.end()
                )
                .into(),
            )
        })?;
        let trimmed: &str = if field.trim || !matches!(data_type, DataType::Utf8) {
            s.trim()
        } else {
            s
        };
        if trimmed.is_empty() && field.nullable {
            slices.push(None);
        } else if trimmed.is_empty() {
            return Err(DataFusionError::External(
                format!(
                    "Row {row_idx} field '{}' is empty/whitespace but field is non-nullable",
                    field.name
                )
                .into(),
            ));
        } else {
            slices.push(Some(trimmed.to_string()));
        }
    }

    // Now type-coerce. Each branch builds the appropriate Arrow array.
    let array: ArrayRef = match data_type {
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(slices.len(), slices.len() * field.length);
            for s in &slices {
                match s {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(slices.len());
            for (i, s) in slices.iter().enumerate() {
                match s {
                    Some(v) => {
                        let parsed = match v.to_ascii_lowercase().as_str() {
                            "true" | "t" | "1" | "y" | "yes" => true,
                            "false" | "f" | "0" | "n" | "no" => false,
                            other => {
                                return Err(parse_err(
                                    field,
                                    row_offset_in_file + i,
                                    other,
                                    "BOOLEAN",
                                ))
                            }
                        };
                        b.append_value(parsed);
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int8 => build_integer::<i8, _>(
            field,
            &slices,
            row_offset_in_file,
            "INT8",
            Int8Builder::new(),
        )?,
        DataType::Int16 => build_integer::<i16, _>(
            field,
            &slices,
            row_offset_in_file,
            "INT16",
            Int16Builder::new(),
        )?,
        DataType::Int32 => build_integer::<i32, _>(
            field,
            &slices,
            row_offset_in_file,
            "INT32",
            Int32Builder::new(),
        )?,
        DataType::Int64 => build_integer::<i64, _>(
            field,
            &slices,
            row_offset_in_file,
            "INT64",
            Int64Builder::new(),
        )?,
        DataType::UInt8 => build_integer::<u8, _>(
            field,
            &slices,
            row_offset_in_file,
            "UINT8",
            UInt8Builder::new(),
        )?,
        DataType::UInt16 => build_integer::<u16, _>(
            field,
            &slices,
            row_offset_in_file,
            "UINT16",
            UInt16Builder::new(),
        )?,
        DataType::UInt32 => build_integer::<u32, _>(
            field,
            &slices,
            row_offset_in_file,
            "UINT32",
            UInt32Builder::new(),
        )?,
        DataType::UInt64 => build_integer::<u64, _>(
            field,
            &slices,
            row_offset_in_file,
            "UINT64",
            UInt64Builder::new(),
        )?,
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(slices.len());
            for (i, s) in slices.iter().enumerate() {
                match s {
                    Some(v) => match v.parse::<f32>() {
                        Ok(n) => b.append_value(n),
                        Err(_) => {
                            return Err(parse_err(field, row_offset_in_file + i, v, "FLOAT32"))
                        }
                    },
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(slices.len());
            for (i, s) in slices.iter().enumerate() {
                match s {
                    Some(v) => match v.parse::<f64>() {
                        Ok(n) => b.append_value(n),
                        Err(_) => {
                            return Err(parse_err(field, row_offset_in_file + i, v, "FLOAT64"))
                        }
                    },
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(DataFusionError::External(
                format!(
                    "Field '{}' has Arrow type {other:?} which is not yet supported by FixedWidthExec. \
                     Phase 8 v1 supports: Utf8, Boolean, Int8/16/32/64, UInt8/16/32/64, Float32/64. \
                     Open a follow-up for DATE/TIMESTAMP/BINARY parsing.",
                    field.name
                )
                .into(),
            ));
        }
    };
    Ok(array)
}

/// Generic integer builder helper. The `_marker` type parameter tells the
/// compiler which numeric type to parse into; the builder is passed in
/// pre-allocated to avoid threading the type back out.
fn build_integer<N, B>(
    field: &FixedWidthField,
    slices: &[Option<String>],
    row_offset_in_file: usize,
    type_label: &str,
    mut builder: B,
) -> DfResult<ArrayRef>
where
    N: std::str::FromStr,
    B: IntegerBuilder<N>,
{
    for (i, s) in slices.iter().enumerate() {
        match s {
            Some(v) => match v.parse::<N>() {
                Ok(n) => builder.append_value(n),
                Err(_) => return Err(parse_err(field, row_offset_in_file + i, v, type_label)),
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

/// Builder trait abstraction so the generic helper above can use any of the
/// concrete primitive builders from `arrow::array`.
trait IntegerBuilder<N> {
    fn append_value(&mut self, v: N);
    fn append_null(&mut self);
    fn finish(self) -> ArrayRef;
}

macro_rules! impl_integer_builder {
    ($builder:ty, $native:ty) => {
        impl IntegerBuilder<$native> for $builder {
            fn append_value(&mut self, v: $native) {
                <$builder>::append_value(self, v);
            }
            fn append_null(&mut self) {
                <$builder>::append_null(self);
            }
            fn finish(mut self) -> ArrayRef {
                Arc::new(<$builder>::finish(&mut self))
            }
        }
    };
}

impl_integer_builder!(Int8Builder, i8);
impl_integer_builder!(Int16Builder, i16);
impl_integer_builder!(Int32Builder, i32);
impl_integer_builder!(Int64Builder, i64);
impl_integer_builder!(UInt8Builder, u8);
impl_integer_builder!(UInt16Builder, u16);
impl_integer_builder!(UInt32Builder, u32);
impl_integer_builder!(UInt64Builder, u64);

fn parse_err(
    field: &FixedWidthField,
    row_idx: usize,
    value: &str,
    type_label: &str,
) -> DataFusionError {
    DataFusionError::External(
        format!(
            "Row {row_idx} field '{}' could not parse '{value}' as {type_label}",
            field.name
        )
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    fn sample_layout_json() -> &'static str {
        r#"{
            "fields": [
                { "name": "id",     "start": 0,  "length": 6,  "type": "INTEGER", "nullable": false },
                { "name": "name",   "start": 6,  "length": 30, "type": "VARCHAR", "nullable": false, "trim": true },
                { "name": "salary", "start": 36, "length": 12, "type": "DOUBLE",  "nullable": true }
            ]
        }"#
    }

    #[test]
    fn parses_valid_layout() {
        let layout = FixedWidthLayout::from_json_str(sample_layout_json()).unwrap();
        assert_eq!(layout.fields.len(), 3);
        assert_eq!(layout.fields[0].name, "id");
        assert_eq!(layout.fields[0].start, 0);
        assert_eq!(layout.fields[0].length, 6);
        assert_eq!(layout.fields[0].sql_type, "INTEGER");
        assert!(!layout.fields[0].nullable);
        assert!(layout.fields[0].trim, "trim defaults to true");
        assert!(layout.fields[2].nullable);
    }

    #[test]
    fn arrow_schema_maps_each_type() {
        let layout = FixedWidthLayout::from_json_str(sample_layout_json()).unwrap();
        let schema = layout.arrow_schema().unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn rejects_empty_fields_list() {
        let json = r#"{ "fields": [] }"#;
        let err = FixedWidthLayout::from_json_str(json).unwrap_err();
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn rejects_zero_length_field() {
        let json = r#"{
            "fields": [
                { "name": "id", "start": 0, "length": 0, "type": "INTEGER" }
            ]
        }"#;
        let err = FixedWidthLayout::from_json_str(json).unwrap_err();
        assert!(err.contains("length 0"), "got: {err}");
        assert!(
            err.contains("id"),
            "expected field name in error, got: {err}"
        );
    }

    #[test]
    fn rejects_overlapping_spans() {
        // id occupies 0..6, name accidentally starts at 5 instead of 6.
        let json = r#"{
            "fields": [
                { "name": "id",   "start": 0, "length": 6, "type": "INTEGER" },
                { "name": "name", "start": 5, "length": 10, "type": "VARCHAR" }
            ]
        }"#;
        let err = FixedWidthLayout::from_json_str(json).unwrap_err();
        assert!(err.contains("overlapping"), "got: {err}");
        assert!(err.contains("id"));
        assert!(err.contains("name"));
    }

    #[test]
    fn rejects_unknown_sql_type() {
        let json = r#"{
            "fields": [
                { "name": "x", "start": 0, "length": 4, "type": "MARTIANDATA" }
            ]
        }"#;
        let err = FixedWidthLayout::from_json_str(json).unwrap_err();
        assert!(err.contains("MARTIANDATA"), "got: {err}");
    }

    #[test]
    fn allows_adjacent_non_overlapping_spans() {
        // Touching but not overlapping — id ends at 6, name starts at 6.
        let json = r#"{
            "fields": [
                { "name": "id",   "start": 0, "length": 6,  "type": "INTEGER" },
                { "name": "name", "start": 6, "length": 10, "type": "VARCHAR" }
            ]
        }"#;
        let layout = FixedWidthLayout::from_json_str(json).unwrap();
        assert_eq!(layout.fields.len(), 2);
    }

    #[test]
    fn allows_gaps_between_fields() {
        // Common pattern: filler bytes between fields are simply omitted
        // from the layout.
        let json = r#"{
            "fields": [
                { "name": "id",   "start": 0,  "length": 6, "type": "INTEGER" },
                { "name": "name", "start": 10, "length": 10, "type": "VARCHAR" }
            ]
        }"#;
        let layout = FixedWidthLayout::from_json_str(json).unwrap();
        assert_eq!(layout.fields.len(), 2);
    }

    #[test]
    fn min_row_width_returns_max_end_offset() {
        let layout = FixedWidthLayout::from_json_str(sample_layout_json()).unwrap();
        assert_eq!(layout.min_row_width(), 48); // 36 + 12
    }

    #[test]
    fn trim_defaults_to_true_when_omitted() {
        let json = r#"{
            "fields": [
                { "name": "id", "start": 0, "length": 4, "type": "INTEGER" }
            ]
        }"#;
        let layout = FixedWidthLayout::from_json_str(json).unwrap();
        assert!(layout.fields[0].trim);
    }

    #[test]
    fn from_json_str_rejects_malformed_json() {
        let err = FixedWidthLayout::from_json_str("{ not valid json").unwrap_err();
        assert!(err.contains("Invalid layout JSON"), "got: {err}");
    }

    #[test]
    fn from_json_path_surfaces_io_error_with_path() {
        let err = FixedWidthLayout::from_json_path("/nonexistent/path/layout.json").unwrap_err();
        assert!(err.contains("layout.json"), "got: {err}");
    }

    #[test]
    fn nullable_defaults_to_false_when_omitted() {
        let json = r#"{
            "fields": [
                { "name": "id", "start": 0, "length": 4, "type": "INTEGER" }
            ]
        }"#;
        let layout = FixedWidthLayout::from_json_str(json).unwrap();
        assert!(!layout.fields[0].nullable, "nullable defaults to false");
    }
}
