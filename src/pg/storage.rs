use crate::core::file_layout::{
    chunk_relative_path, cleanup_generation_dirs, reset_storage_root, write_chunk_file,
};
use crate::core::interface::{GranuleWriteRequest, GranuleWriteResult, SnapshotWriter};
use crate::pg::catalog::{
    delete_all_granules, delete_stale_granules, insert_column_chunk, insert_granule,
    mark_merge_success,
};
use anyhow::{Context, Result, bail};
use pgrx::pg_sys;
use std::ffi::CStr;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FileSnapshotWriter;

impl SnapshotWriter for FileSnapshotWriter {
    fn replace_snapshot(&self, request: &GranuleWriteRequest) -> Result<GranuleWriteResult> {
        let table_root = PathBuf::from(&request.table.storage_root);
        fs::create_dir_all(&table_root).with_context(|| {
            format!(
                "failed to create table storage root {}",
                table_root.display()
            )
        })?;

        let first_generation = request
            .granules
            .first()
            .map(|granule| granule.span.generation);

        if request.granules.is_empty() {
            cleanup_generation_dirs(&table_root, None)?;
            delete_all_granules(request.table.table_oid)?;
            mark_merge_success(request.table.table_oid)?;
            return Ok(GranuleWriteResult {
                granules_written: 0,
                rows_written: 0,
                first_generation: None,
            });
        }

        for granule in &request.granules {
            let granule_dir = table_root.join(format!("g{:020}", granule.span.generation));
            if granule_dir.exists() {
                fs::remove_dir_all(&granule_dir).with_context(|| {
                    format!(
                        "failed to clear granule directory {}",
                        granule_dir.display()
                    )
                })?;
            }
            fs::create_dir_all(&granule_dir).with_context(|| {
                format!(
                    "failed to create granule directory {}",
                    granule_dir.display()
                )
            })?;

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
                let relative_path = chunk_relative_path(
                    granule.span.generation,
                    chunk.column.ordinal,
                    &chunk.column.name,
                    &chunk.codec,
                );
                let absolute_path = table_root.join(&relative_path);
                write_chunk_file(&absolute_path, &chunk.payload)?;

                insert_column_chunk(
                    granule_id,
                    &chunk.column.name,
                    chunk.column.ordinal,
                    &chunk.codec,
                    chunk.row_count,
                    chunk.uncompressed_bytes,
                    chunk.compressed_bytes,
                    &relative_path.to_string_lossy(),
                )?;
            }
        }

        cleanup_generation_dirs(&table_root, first_generation)?;
        delete_stale_granules(request.table.table_oid, first_generation)?;
        mark_merge_success(request.table.table_oid)?;

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

pub(crate) fn resolve_storage_root(table_oid: i64, requested_path: &str) -> Result<String> {
    let requested_path = requested_path.trim();
    let base_path = if requested_path.is_empty() {
        default_storage_base_path()?
    } else {
        let requested = PathBuf::from(requested_path);
        if requested.is_absolute() {
            requested
        } else {
            PathBuf::from(pg_data_dir()?).join(requested)
        }
    };
    let path = table_storage_root(base_path, table_oid)?;

    Ok(path.to_string_lossy().into_owned())
}

pub(crate) fn reset_table_storage(storage_root: &str) -> Result<()> {
    reset_storage_root(storage_root)
}

fn default_storage_base_path() -> Result<PathBuf> {
    Ok(PathBuf::from(pg_data_dir()?).join("pghouse"))
}

fn table_storage_root(base_path: PathBuf, table_oid: i64) -> Result<PathBuf> {
    Ok(base_path
        .join(format!("db_{}", current_database_oid()?))
        .join(format!("rel_{table_oid}")))
}

fn current_database_oid() -> Result<u32> {
    let database_oid = unsafe { pg_sys::MyDatabaseId.to_u32() };
    if database_oid == 0 {
        bail!("MyDatabaseId is not initialized")
    }
    Ok(database_oid)
}

fn pg_data_dir() -> Result<String> {
    unsafe {
        if pg_sys::DataDir.is_null() {
            bail!("PostgreSQL DataDir is not initialized");
        }
        Ok(CStr::from_ptr(pg_sys::DataDir)
            .to_str()
            .context("DataDir is not valid UTF-8")?
            .to_string())
    }
}
