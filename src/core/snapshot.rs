use crate::core::interface::{
    ChunkCodec, ColumnChunkEncodeRequest, ColumnDescriptor, GranuleSpan, GranuleWriteBatch,
    GranuleWriteRequest, RowVersion, TableDescriptor,
};
use anyhow::Result;

pub(crate) fn build_snapshot_write_request<C: ChunkCodec>(
    table: TableDescriptor,
    columns: &[ColumnDescriptor],
    live_rows: &[RowVersion],
    base_generation: i64,
    merge_reason: &str,
    codec: &C,
) -> Result<GranuleWriteRequest> {
    if live_rows.is_empty() {
        return Ok(GranuleWriteRequest {
            table,
            granules: Vec::new(),
        });
    }

    let granule_size = table.granule_rows.max(1);
    let mut granules = Vec::new();

    for (index, chunk) in live_rows.chunks(granule_size).enumerate() {
        let generation = base_generation + i64::try_from(index).unwrap_or_default();
        let pk_min = chunk.first().map(|row| row.pk_text.clone());
        let pk_max = chunk.last().map(|row| row.pk_text.clone());

        let mut encoded_chunks = Vec::with_capacity(columns.len());
        for column in columns {
            let request = ColumnChunkEncodeRequest {
                table: table.clone(),
                column: column.clone(),
                rows: chunk.to_vec(),
                codec: table.compression.clone(),
            };
            encoded_chunks.push(codec.encode(&request)?);
        }

        granules.push(GranuleWriteBatch {
            span: GranuleSpan {
                generation,
                row_count: i32::try_from(chunk.len()).unwrap_or(i32::MAX),
                pk_min,
                pk_max,
                merge_reason: merge_reason.to_string(),
            },
            chunks: encoded_chunks,
        });
    }

    Ok(GranuleWriteRequest { table, granules })
}
