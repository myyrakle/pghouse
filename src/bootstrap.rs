use crate::catalog::{
    delete_stale_granules, insert_column_chunk, insert_granule, mark_merge_success,
};
use crate::interface::{GranuleWriteRequest, GranuleWriteResult, SnapshotWriter};
use anyhow::Result;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct BootstrapSnapshotWriter;

impl SnapshotWriter for BootstrapSnapshotWriter {
    fn replace_snapshot(&self, request: &GranuleWriteRequest) -> Result<GranuleWriteResult> {
        let first_generation = request
            .granules
            .first()
            .map(|granule| granule.span.generation);

        for granule in &request.granules {
            let granule_id = insert_granule(
                request.table.table_oid,
                granule.span.generation,
                granule.span.row_count,
                granule.span.pk_min.as_deref(),
                granule.span.pk_max.as_deref(),
                &request.table.compression,
                &granule.span.merge_reason,
            )?;

            for chunk in &granule.chunks {
                insert_column_chunk(
                    granule_id,
                    &chunk.column.name,
                    chunk.column.ordinal,
                    &chunk.codec,
                    chunk.row_count,
                    chunk.uncompressed_bytes,
                    chunk.compressed_bytes,
                    chunk.payload.clone(),
                )?;
            }
        }

        let min_generation_to_keep = first_generation.unwrap_or(1);
        mark_merge_success(request.table.table_oid)?;
        delete_stale_granules(request.table.table_oid, min_generation_to_keep)?;

        Ok(GranuleWriteResult {
            granules_written: request.granules.len(),
            rows_written: request
                .granules
                .iter()
                .map(|granule| usize::try_from(granule.span.row_count.max(0)).unwrap_or_default())
                .sum(),
            first_generation,
        })
    }
}
