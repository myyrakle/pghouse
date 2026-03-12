use crate::catalog::{
    delete_all_granules, delete_stale_granules, insert_column_chunk, insert_granule,
    mark_merge_success,
};
use crate::interface::{GranuleWriteRequest, GranuleWriteResult, SnapshotWriter};
use anyhow::{Context, Result, bail};
use pgrx::pg_sys;
use std::ffi::CStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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
            let granule_dir = table_root.join(granule_dir_name(granule.span.generation));
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
                let file_name =
                    chunk_file_name(chunk.column.ordinal, &chunk.column.name, &chunk.codec);
                let relative_path =
                    PathBuf::from(granule_dir_name(granule.span.generation)).join(file_name);
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
    let root = Path::new(storage_root);
    if root.exists() {
        fs::remove_dir_all(root)
            .with_context(|| format!("failed to remove storage root {}", root.display()))?;
    }
    fs::create_dir_all(root)
        .with_context(|| format!("failed to create storage root {}", root.display()))?;
    Ok(())
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

fn cleanup_generation_dirs(table_root: &Path, min_generation_to_keep: Option<i64>) -> Result<()> {
    if !table_root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(table_root)
        .with_context(|| format!("failed to list storage root {}", table_root.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(generation) = parse_generation_dir_name(name) else {
            continue;
        };

        let should_delete = match min_generation_to_keep {
            Some(min_generation) => generation < min_generation,
            None => true,
        };

        if should_delete {
            fs::remove_dir_all(entry.path()).with_context(|| {
                format!(
                    "failed to remove stale granule directory {}",
                    entry.path().display()
                )
            })?;
        }
    }

    Ok(())
}

fn write_chunk_file(path: &Path, payload: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp_path)
        .with_context(|| format!("failed to create chunk temp file {}", tmp_path.display()))?;
    file.write_all(payload)
        .with_context(|| format!("failed to write chunk temp file {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync chunk temp file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to move chunk temp file {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn granule_dir_name(generation: i64) -> String {
    format!("g{generation:020}")
}

fn parse_generation_dir_name(name: &str) -> Option<i64> {
    name.strip_prefix('g')?.parse::<i64>().ok()
}

fn chunk_file_name(column_ordinal: i32, column_name: &str, codec: &str) -> String {
    let safe_name = sanitize_path_component(column_name);
    format!("c{column_ordinal:04}_{safe_name}.{codec}.bin")
}

fn sanitize_path_component(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "column".to_string()
    } else {
        output
    }
}
