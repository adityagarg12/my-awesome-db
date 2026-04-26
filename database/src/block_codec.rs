use anyhow::{Result, bail, Context};
use crate::row::{Row, decode_row, encode_row, decode_value, skip_value_at};
use crate::catalog::ColumnCatalog;

/// Decode a single block into a vector of rows.
///
/// Reads the row_count from the last 2 bytes (u16 little-endian),
/// then decodes rows sequentially from byte 0 until row_count rows are read.
pub fn decode_block(
    block_bytes: &[u8],
    block_size: u64,
    columns: &[ColumnCatalog],
) -> Result<Vec<Row>> {
    if block_bytes.len() as u64 != block_size {
        bail!(
            "Block size mismatch: expected {} bytes, got {}",
            block_size,
            block_bytes.len()
        );
    }

    // Read row_count from last 2 bytes
    let row_count_offset = block_size as usize - 2;
    if row_count_offset < 2 {
        bail!("Block size too small to contain row_count footer");
    }

    let row_count = u16::from_le_bytes([
        block_bytes[row_count_offset],
        block_bytes[row_count_offset + 1],
    ]) as usize;

    // Decode rows sequentially from byte 0
    let mut rows = Vec::new();
    let mut offset = 0;

    for i in 0..row_count {
        if offset >= row_count_offset {
            bail!(
                "Row {} starts at offset {} but usable block space ends at {}",
                i,
                offset,
                row_count_offset
            );
        }

        let row = decode_row(&block_bytes[..row_count_offset], &mut offset, columns)
            .context(format!("Failed to decode row {}", i))?;

        rows.push(row);
    }

    Ok(rows)
}

/// Decode a block, emitting only the columns at `needed_indices` (sorted ascending).
/// Columns not in `needed_indices` are skipped without allocation.
/// Output rows have values in the same order as `needed_indices`.
pub fn decode_block_projected(
    block_bytes: &[u8],
    block_size: u64,
    all_columns: &[ColumnCatalog],
    needed_indices: &[usize],
) -> Result<Vec<Row>> {
    if block_bytes.len() as u64 != block_size {
        bail!("Block size mismatch: expected {} bytes, got {}", block_size, block_bytes.len());
    }

    let row_count_offset = block_size as usize - 2;
    if row_count_offset < 2 {
        bail!("Block size too small to contain row_count footer");
    }

    let row_count = u16::from_le_bytes([
        block_bytes[row_count_offset],
        block_bytes[row_count_offset + 1],
    ]) as usize;

    let usable = &block_bytes[..row_count_offset];
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0usize;

    for i in 0..row_count {
        if offset >= row_count_offset {
            bail!(
                "Row {} starts at offset {} but usable block space ends at {}",
                i, offset, row_count_offset
            );
        }

        let mut values = Vec::with_capacity(needed_indices.len());
        let mut need_pos = 0usize;

        for (col_idx, col) in all_columns.iter().enumerate() {
            if need_pos < needed_indices.len() && needed_indices[need_pos] == col_idx {
                let (value, bytes_read) = decode_value(usable, offset, &col.data_type)
                    .with_context(|| format!("Failed to decode column '{}'", col.name))?;
                offset += bytes_read;
                values.push(value);
                need_pos += 1;
            } else {
                skip_value_at(usable, &mut offset, &col.data_type)
                    .with_context(|| format!("Failed to skip column '{}'", col.name))?;
            }
        }
        rows.push(Row::new(values));
    }

    Ok(rows)
}

/// Encode rows into block-aligned bytes.
///
/// Packs rows into blocks, ensuring each row fits entirely within a block.
/// Last 2 bytes of each block contain row_count as u16 little-endian.
/// Remaining space is zero-padded.
pub fn encode_rows_to_blocks(
    rows: &[Row],
    block_size: u64,
    columns: &[ColumnCatalog],
) -> Result<Vec<u8>> {
    let usable_space = block_size as usize - 2;

    if usable_space < 2 {
        bail!("Block size too small to encode any rows");
    }

    let mut result = Vec::new();
    let mut current_block = vec![0u8; block_size as usize];
    let mut current_offset = 0;
    let mut row_count = 0u16;

    for row in rows {
        let encoded_row =
            encode_row(row, columns).context("Failed to encode row")?;

        // Check if row fits in current block's usable space
        if current_offset + encoded_row.len() > usable_space {
            // Current block is full, finalize and start a new one
            if row_count > 0 {
                // Write row_count to last 2 bytes
                let row_count_offset = current_block.len() - 2;
                current_block[row_count_offset..].copy_from_slice(&row_count.to_le_bytes());

                result.extend_from_slice(&current_block);
            }

            // Start new block
            current_block = vec![0u8; block_size as usize];
            current_offset = 0;
            row_count = 0;
        }

        // Check if row itself is too large to fit in any block
        if encoded_row.len() > usable_space {
            bail!(
                "Row is {} bytes but max row size is {} bytes",
                encoded_row.len(),
                usable_space
            );
        }

        // Write row to current block
        current_block[current_offset..current_offset + encoded_row.len()]
            .copy_from_slice(&encoded_row);
        current_offset += encoded_row.len();
        row_count += 1;
    }

    // Finalize last block (even if empty)
    let row_count_offset = current_block.len() - 2;
    current_block[row_count_offset..].copy_from_slice(&row_count.to_le_bytes());
    result.extend_from_slice(&current_block);

    Ok(result)
}
