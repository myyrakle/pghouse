use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub table_oid: i64,
    pub schema_name: String,
    pub table_name: String,
    pub pk_column: String,
    pub granule_rows: usize,
    pub compression: String,
    pub storage_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub ordinal: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowMutation {
    pub sequence: i64,
    pub kind: MutationKind,
    pub pk_text: String,
    pub row_json: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowVersion {
    pub pk_text: String,
    pub row_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkEncodeRequest {
    pub table: TableDescriptor,
    pub column: ColumnDescriptor,
    pub rows: Vec<RowVersion>,
    pub codec: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedColumnChunk {
    pub column: ColumnDescriptor,
    pub codec: String,
    pub row_count: i32,
    pub uncompressed_bytes: i32,
    pub compressed_bytes: i32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleSpan {
    pub generation: i64,
    pub row_count: i32,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub merge_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleWriteBatch {
    pub span: GranuleSpan,
    pub chunks: Vec<EncodedColumnChunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleWriteRequest {
    pub table: TableDescriptor,
    pub granules: Vec<GranuleWriteBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleWriteResult {
    pub granules_written: usize,
    pub rows_written: usize,
    pub first_generation: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleRef {
    pub table_oid: i64,
    pub generation: i64,
    pub row_count: i32,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub manifest_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkManifestEntry {
    pub column: ColumnDescriptor,
    pub codec: String,
    pub row_count: i32,
    pub uncompressed_bytes: i32,
    pub compressed_bytes: i32,
    pub storage_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleManifest {
    pub table_oid: i64,
    pub generation: i64,
    pub row_count: i32,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub chunks: Vec<ChunkManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    pub table: TableDescriptor,
    pub projection: Vec<ColumnDescriptor>,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub limit: Option<usize>,
    pub snapshot_generation: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPlan {
    pub table: TableDescriptor,
    pub projection: Vec<ColumnDescriptor>,
    pub granules: Vec<GranuleRef>,
    pub pk_min: Option<String>,
    pub pk_max: Option<String>,
    pub limit: Option<usize>,
    pub row_estimate: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnVector {
    pub column: ColumnDescriptor,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanBatch {
    pub granule: GranuleRef,
    pub columns: Vec<ColumnVector>,
}

pub trait ChunkCodec {
    fn encode(&self, request: &ColumnChunkEncodeRequest) -> Result<EncodedColumnChunk>;
    fn decode(&self, chunk: &EncodedColumnChunk) -> Result<Vec<Value>>;
}

pub trait SnapshotWriter {
    fn replace_snapshot(&self, request: &GranuleWriteRequest) -> Result<GranuleWriteResult>;
}

pub trait ScanPlanner {
    fn plan_scan(&self, request: &ScanRequest) -> Result<ScanPlan>;
}

pub trait GranuleReader {
    fn read_granule(&self, plan: &ScanPlan, granule: &GranuleRef) -> Result<ScanBatch>;
}
