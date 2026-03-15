use crate::core::codec::JsonArrayZstdCodec;
use crate::core::file_layout::{read_json_file, read_json_lines};
use crate::core::interface::{
    ChunkCodec, ColumnDescriptor, ColumnVector, EncodedColumnChunk, GranuleManifest, GranuleReader,
    GranuleRef, ScanBatch, ScanPlan, ScanPlanner, ScanRequest,
};
use crate::core::scan::{MaterializedRow, build_scan_plan, materialize_row_entries};
use crate::pg::catalog::{load_scan_granules, load_visible_insert_buffers};
use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(crate) struct MetadataScanPlanner {
    granules: Vec<GranuleRef>,
}

impl MetadataScanPlanner {
    pub(crate) fn new(granules: Vec<GranuleRef>) -> Self {
        Self { granules }
    }
}

impl ScanPlanner for MetadataScanPlanner {
    fn plan_scan(&self, request: &ScanRequest) -> Result<ScanPlan> {
        build_scan_plan(request, &self.granules)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FileGranuleReader;

impl GranuleReader for FileGranuleReader {
    fn read_granule(&self, plan: &ScanPlan, granule: &GranuleRef) -> Result<ScanBatch> {
        let required_columns = required_columns(plan);
        let manifest = load_granule_manifest(plan, granule)?;
        let chunk_map = manifest
            .chunks
            .into_iter()
            .map(|chunk| (chunk.column.name.clone(), chunk))
            .collect::<HashMap<_, _>>();

        let codec = JsonArrayZstdCodec;
        let mut columns = Vec::with_capacity(required_columns.len());
        for column in required_columns {
            let chunk = chunk_map.get(&column.name).ok_or_else(|| {
                anyhow!(
                    "missing chunk for column {} in granule generation {}",
                    column.name,
                    granule.generation
                )
            })?;
            let payload_path = PathBuf::from(&plan.table.storage_root).join(&chunk.storage_path);
            let payload = fs::read(&payload_path)
                .with_context(|| format!("failed to read chunk file {}", payload_path.display()))?;
            let encoded = EncodedColumnChunk {
                column: chunk.column.clone(),
                codec: chunk.codec.clone(),
                row_count: chunk.row_count,
                uncompressed_bytes: chunk.uncompressed_bytes,
                compressed_bytes: chunk.compressed_bytes,
                payload,
            };
            let values = codec.decode(&encoded)?;
            if values.len() != usize::try_from(chunk.row_count.max(0)).unwrap_or_default() {
                bail!(
                    "decoded row count mismatch for column {} in granule generation {}",
                    column.name,
                    granule.generation
                );
            }
            columns.push(ColumnVector {
                column: encoded.column,
                values,
            });
        }

        Ok(ScanBatch {
            granule: granule.clone(),
            columns,
        })
    }
}

pub(crate) fn plan_scan(request: &ScanRequest) -> Result<ScanPlan> {
    let granules = load_scan_granules(request.table.table_oid, request.snapshot_generation)?;
    MetadataScanPlanner::new(granules).plan_scan(request)
}

pub(crate) fn execute_scan(plan: &ScanPlan) -> Result<Vec<serde_json::Value>> {
    let reader = FileGranuleReader;
    let mut batches = Vec::with_capacity(plan.granules.len());
    for granule in &plan.granules {
        batches.push(reader.read_granule(plan, granule)?);
    }

    let mut rows = materialize_row_entries(plan, &batches)?;
    rows.extend(load_buffered_rows(plan)?);
    rows.sort_by(|left, right| left.pk_text.cmp(&right.pk_text));

    if let Some(limit) = plan.limit {
        rows.truncate(limit);
    }

    Ok(rows.into_iter().map(|row| row.row).collect())
}

fn required_columns(plan: &ScanPlan) -> Vec<ColumnDescriptor> {
    let mut columns = plan.projection.clone();
    if !columns.iter().any(|column| column.name == plan.table.pk_column) {
        columns.push(ColumnDescriptor {
            ordinal: 0,
            name: plan.table.pk_column.clone(),
        });
    }
    columns
}

fn load_granule_manifest(plan: &ScanPlan, granule: &GranuleRef) -> Result<GranuleManifest> {
    let manifest_path = PathBuf::from(&plan.table.storage_root).join(&granule.manifest_path);
    let manifest = read_json_file::<GranuleManifest>(&manifest_path)?;
    if manifest.table_oid != granule.table_oid || manifest.generation != granule.generation {
        bail!(
            "manifest {} does not match granule identity {}:{}",
            manifest_path.display(),
            granule.table_oid,
            granule.generation
        );
    }
    Ok(manifest)
}

#[derive(Debug, Deserialize)]
struct BufferedInsertRecord {
    pk_text: String,
    row_json: Value,
}

fn load_buffered_rows(plan: &ScanPlan) -> Result<Vec<MaterializedRow>> {
    let buffers = load_visible_insert_buffers(plan.table.table_oid)?;
    let mut rows = Vec::new();

    for buffer in buffers {
        let path = PathBuf::from(&plan.table.storage_root).join(&buffer.storage_path);
        let staged_rows = read_json_lines::<BufferedInsertRecord>(&path)
            .with_context(|| format!("failed to read insert buffer {}", path.display()))?;
        for staged_row in staged_rows {
            if !pk_matches_bounds(
                &staged_row.pk_text,
                plan.pk_min.as_deref(),
                plan.pk_max.as_deref(),
            ) {
                continue;
            }
            rows.push(MaterializedRow {
                pk_text: Some(staged_row.pk_text),
                row: project_row(&staged_row.row_json, &plan.projection),
            });
        }
    }

    Ok(rows)
}

fn project_row(row_json: &Value, projection: &[ColumnDescriptor]) -> Value {
    match row_json {
        Value::Object(object) => {
            let mut projected = Map::with_capacity(projection.len());
            for column in projection {
                let value = object.get(&column.name).cloned().unwrap_or(Value::Null);
                projected.insert(column.name.clone(), value);
            }
            Value::Object(projected)
        }
        _ => Value::Object(Map::new()),
    }
}

fn pk_matches_bounds(value: &str, pk_min: Option<&str>, pk_max: Option<&str>) -> bool {
    if pk_min.is_some_and(|bound| value < bound) {
        return false;
    }

    if pk_max.is_some_and(|bound| value > bound) {
        return false;
    }

    true
}
