use anyhow::{Context, Result, bail};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Debug, Clone, Copy)]
pub struct FileLayout {
    pub start_block_id: u64,
    pub num_blocks: u64,
}

// FIFO block cache: holds up to `capacity` blocks keyed by block ID.
// Writes are write-through (disk + cache). Reads check cache first.
struct BlockCache {
    cache: HashMap<u64, Vec<u8>>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl BlockCache {
    fn new(capacity: usize) -> Self {
        BlockCache {
            cache: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&self, block_id: u64) -> Option<&Vec<u8>> {
        self.cache.get(&block_id)
    }

    fn insert(&mut self, block_id: u64, data: Vec<u8>) {
        if self.cache.contains_key(&block_id) {
            *self.cache.get_mut(&block_id).unwrap() = data;
            return;
        }
        if self.cache.len() >= self.capacity {
            if let Some(evict_id) = self.order.pop_front() {
                self.cache.remove(&evict_id);
            }
        }
        self.cache.insert(block_id, data);
        self.order.push_back(block_id);
    }
}

pub struct DiskAccessLayer<R: Read, W: Write> {
    reader: BufReader<R>,
    writer: W,
    block_size_cache: Option<u64>,
    cache: BlockCache,
}

impl<R: Read, W: Write> DiskAccessLayer<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            block_size_cache: None,
            // 512 blocks × 4 KB = 2 MB.  Doubles the window for cache hits:
            // B-NLJ right sides ≤ 2 MB are fully served from cache on re-scans,
            // and more sort-run blocks survive between write and merge-read.
            // BUFFER_POOL_BLOCKS in main.rs is updated to match this value.
            cache: BlockCache::new(512),
        }
    }

    pub fn get_block_size(&mut self) -> Result<u64> {
        if let Some(block_size) = self.block_size_cache {
            return Ok(block_size);
        }

        self.send_command_line("get block-size\n")?;
        let block_size = self.read_u64_line().context("Failed to read block size")?;
        self.block_size_cache = Some(block_size);
        Ok(block_size)
    }

    pub fn get_file_start_block(&mut self, file_id: &str) -> Result<u64> {
        self.send_command_line(&format!("get file start-block {}\n", file_id))?;
        self.read_u64_line().context("Failed to read file start block")
    }

    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        self.send_command_line(&format!("get file num-blocks {}\n", file_id))?;
        self.read_u64_line().context("Failed to read file block count")
    }

    pub fn get_anon_start_block(&mut self) -> Result<u64> {
        self.send_command_line("get anon-start-block\n")?;
        self.read_u64_line()
            .context("Failed to read anonymous region start block")
    }

    pub fn get_file_layout(&mut self, file_id: &str) -> Result<FileLayout> {
        let start_block_id = self.get_file_start_block(file_id)?;
        let num_blocks = self.get_file_num_blocks(file_id)?;
        Ok(FileLayout {
            start_block_id,
            num_blocks,
        })
    }

    /// Read blocks from disk without consulting the cache.
    fn raw_read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        let block_size = self.get_block_size()?;
        let response_size = num_blocks
            .checked_mul(block_size)
            .context("Requested read size overflowed u64")?;
        self.send_command_line(&format!("get block {} {}\n", start_block_id, num_blocks))?;
        let mut bytes = vec![0u8; response_size as usize];
        self.reader
            .read_exact(&mut bytes)
            .context("Failed to read raw block bytes")?;
        Ok(bytes)
    }

    /// Read blocks, serving single-block requests from the FIFO cache when possible.
    /// Multi-block reads (e.g., ScanOperator prefetch) always go to disk and populate
    /// the cache so subsequent single-block re-reads of those IDs are free.
    pub fn read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        let block_size = self.get_block_size()? as usize;

        if num_blocks == 1 {
            if let Some(data) = self.cache.get(start_block_id) {
                return Ok(data.clone());
            }
        }

        let bytes = self.raw_read_blocks(start_block_id, num_blocks)?;

        // Populate cache with each fetched block individually.
        for i in 0u64..num_blocks {
            let offset = (i as usize) * block_size;
            self.cache.insert(start_block_id + i, bytes[offset..offset + block_size].to_vec());
        }

        Ok(bytes)
    }

    /// Read a single block, using the cache.  On a cache miss, fetches up to
    /// `prefetch_count` sequential blocks in one IPC call and fills the cache
    /// for the extras — so the next sequential single-block calls are free.
    pub fn read_block_with_prefetch(&mut self, block_id: u64, prefetch_count: u64) -> Result<Vec<u8>> {
        if let Some(data) = self.cache.get(block_id) {
            return Ok(data.clone());
        }
        let fetch = prefetch_count.max(1);
        let bytes = self.raw_read_blocks(block_id, fetch)?;
        let block_size = self.get_block_size()? as usize;
        for i in 0..fetch as usize {
            self.cache.insert(block_id + i as u64, bytes[i * block_size..(i + 1) * block_size].to_vec());
        }
        Ok(bytes[..block_size].to_vec())
    }

    /// Write blocks to disk and immediately update the cache (write-through).
    /// This ensures any subsequent read of a just-written block is a cache hit.
    pub fn write_blocks(&mut self, start_block_id: u64, num_blocks: u64, bytes: &[u8]) -> Result<()> {
        let block_size = self.get_block_size()?;
        let expected_size = num_blocks
            .checked_mul(block_size)
            .context("Requested write size overflowed u64")?;

        if bytes.len() as u64 != expected_size {
            bail!(
                "Invalid write payload size: expected {} bytes for {} blocks, found {}",
                expected_size,
                num_blocks,
                bytes.len()
            );
        }

        self.send_command_line(&format!("put block {} {}\n", start_block_id, num_blocks))?;
        self.writer
            .write_all(bytes)
            .context("Failed to write raw block bytes")?;
        self.writer.flush().context("Failed to flush write payload")?;

        let bs = block_size as usize;
        for i in 0u64..num_blocks {
            let offset = (i as usize) * bs;
            self.cache.insert(start_block_id + i, bytes[offset..offset + bs].to_vec());
        }

        Ok(())
    }

    fn send_command_line(&mut self, command: &str) -> Result<()> {
        self.writer
            .write_all(command.as_bytes())
            .context("Failed to write disk command")?;
        self.writer.flush().context("Failed to flush disk command")?;
        Ok(())
    }

    fn read_u64_line(&mut self) -> Result<u64> {
        let mut line = String::new();
        self.reader
            .read_line(&mut line)
            .context("Failed to read response line")?;
        line.trim()
            .parse::<u64>()
            .context("Failed to parse u64 response")
    }
}