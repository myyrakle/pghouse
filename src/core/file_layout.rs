use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub(crate) fn reset_storage_root(storage_root: &str) -> Result<()> {
    let root = Path::new(storage_root);
    if root.exists() {
        fs::remove_dir_all(root)
            .with_context(|| format!("failed to remove storage root {}", root.display()))?;
    }
    fs::create_dir_all(root)
        .with_context(|| format!("failed to create storage root {}", root.display()))?;
    Ok(())
}

pub(crate) fn cleanup_generation_dirs(
    table_root: &Path,
    min_generation_to_keep: Option<i64>,
) -> Result<()> {
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

pub(crate) fn write_chunk_file(path: &Path, payload: &[u8]) -> Result<()> {
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

pub(crate) fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let payload = serde_json::to_vec_pretty(value)
        .with_context(|| format!("failed to serialize json file {}", path.display()))?;
    write_chunk_file(path, &payload)
}

pub(crate) fn read_json_file<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let payload =
        fs::read(path).with_context(|| format!("failed to read json file {}", path.display()))?;
    serde_json::from_slice(&payload)
        .with_context(|| format!("failed to decode json file {}", path.display()))
}

pub(crate) fn granule_dir_name(generation: i64) -> String {
    format!("g{generation:020}")
}

pub(crate) fn chunk_file_name(column_ordinal: i32, column_name: &str, codec: &str) -> String {
    let safe_name = sanitize_path_component(column_name);
    format!("c{column_ordinal:04}_{safe_name}.{codec}.bin")
}

pub(crate) fn chunk_relative_path(
    generation: i64,
    column_ordinal: i32,
    column_name: &str,
    codec: &str,
) -> PathBuf {
    PathBuf::from(granule_dir_name(generation)).join(chunk_file_name(
        column_ordinal,
        column_name,
        codec,
    ))
}

pub(crate) fn manifest_file_name() -> &'static str {
    "manifest.json"
}

pub(crate) fn manifest_relative_path(generation: i64) -> PathBuf {
    PathBuf::from(granule_dir_name(generation)).join(manifest_file_name())
}

pub(crate) fn incoming_dir_name() -> &'static str {
    "incoming"
}

pub(crate) fn append_json_line<T: Serialize>(path: &Path, value: &T) -> Result<usize> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let mut payload = serde_json::to_vec(value)
        .with_context(|| format!("failed to serialize json line {}", path.display()))?;
    payload.push(b'\n');

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open append file {}", path.display()))?;
    file.write_all(&payload)
        .with_context(|| format!("failed to append file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync append file {}", path.display()))?;

    Ok(payload.len())
}

pub(crate) fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let file = fs::File::open(path)
        .with_context(|| format!("failed to open jsonl file {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut values = Vec::new();
    for line in reader.lines() {
        let line =
            line.with_context(|| format!("failed to read jsonl line from {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        values.push(serde_json::from_str::<T>(&line).with_context(|| {
            format!("failed to decode jsonl line from {}", path.display())
        })?);
    }

    Ok(values)
}

fn parse_generation_dir_name(name: &str) -> Option<i64> {
    name.strip_prefix('g')?.parse::<i64>().ok()
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
