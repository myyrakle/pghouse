use crate::core::interface::{ChunkCodec, ColumnChunkEncodeRequest, EncodedColumnChunk};
use anyhow::{Context, Result, bail};
use std::io::Cursor;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct JsonArrayZstdCodec;

impl ChunkCodec for JsonArrayZstdCodec {
    fn encode(&self, request: &ColumnChunkEncodeRequest) -> Result<EncodedColumnChunk> {
        let values = request
            .rows
            .iter()
            .map(|row| {
                row.row_json
                    .get(&request.column.name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            })
            .collect::<Vec<_>>();
        let raw = serde_json::to_vec(&values).context("failed to serialize column values")?;
        let compressed = match request.codec.as_str() {
            "zstd" => zstd::stream::encode_all(Cursor::new(&raw), 3)
                .context("failed to zstd-compress column chunk")?,
            other => bail!("unsupported compression codec {other}"),
        };

        Ok(EncodedColumnChunk {
            column: request.column.clone(),
            codec: request.codec.clone(),
            row_count: i32::try_from(values.len()).unwrap_or(i32::MAX),
            uncompressed_bytes: i32::try_from(raw.len()).unwrap_or(i32::MAX),
            compressed_bytes: i32::try_from(compressed.len()).unwrap_or(i32::MAX),
            payload: compressed,
        })
    }

    fn decode(&self, chunk: &EncodedColumnChunk) -> Result<Vec<serde_json::Value>> {
        let raw = match chunk.codec.as_str() {
            "zstd" => zstd::stream::decode_all(Cursor::new(&chunk.payload))
                .context("failed to zstd-decompress column chunk")?,
            other => bail!("unsupported compression codec {other}"),
        };

        serde_json::from_slice(&raw).context("failed to decode column values")
    }
}
