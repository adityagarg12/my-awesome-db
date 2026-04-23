use anyhow::{Context, Result, bail};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Debug, Clone, Copy)]
pub struct FileLayout {
    pub start_block_id: u64,
    pub num_blocks: u64,
}

pub struct DiskAccessLayer<R: Read, W: Write> {
    reader: BufReader<R>,
    writer: W,
    block_size_cache: Option<u64>,
    cache: Option<BlockCache>,
}

struct BlockCache {
    max_blocks: usize,
    block_size: usize,
    map: HashMap<u64, Vec<u8>>,
    lru: VecDeque<u64>,
}

impl BlockCache {
    fn new(max_bytes: usize, block_size: usize) -> Self {
        let max_blocks = (max_bytes / block_size.max(1)).max(1);
        Self {
            max_blocks,
            block_size,
            map: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn touch(&mut self, block_id: u64) {
        if let Some(pos) = self.lru.iter().position(|&id| id == block_id) {
            self.lru.remove(pos);
        }
        self.lru.push_back(block_id);
    }

    fn contains(&self, block_id: u64) -> bool {
        self.map.contains_key(&block_id)
    }

    fn get_copy_into(&mut self, block_id: u64, dst: &mut [u8]) -> bool {
        let Some(data) = self.map.get(&block_id) else { return false; };
        if data.len() != dst.len() {
            return false;
        }
        dst.copy_from_slice(data);
        self.touch(block_id);
        true
    }

    fn put(&mut self, block_id: u64, data: Vec<u8>) {
        if data.len() != self.block_size {
            return;
        }

        if self.map.contains_key(&block_id) {
            self.map.insert(block_id, data);
            self.touch(block_id);
            return;
        }

        if self.map.len() >= self.max_blocks {
            if let Some(evict_id) = self.lru.pop_front() {
                self.map.remove(&evict_id);
            }
        }

        self.map.insert(block_id, data);
        self.lru.push_back(block_id);
    }
}

impl<R: Read, W: Write> DiskAccessLayer<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            block_size_cache: None,
            cache: None,
        }
    }

    pub fn configure_buffer_pool(&mut self, max_bytes: u64) -> Result<()> {
        let block_size = self.get_block_size()? as usize;
        let max_bytes = max_bytes as usize;
        if max_bytes < block_size {
            self.cache = None;
            return Ok(());
        }
        self.cache = Some(BlockCache::new(max_bytes, block_size));
        Ok(())
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

    pub fn read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        let block_size = self.get_block_size()?;
        let response_size = num_blocks
            .checked_mul(block_size)
            .context("Requested read size overflowed u64")?;

        if num_blocks == 0 {
            return Ok(Vec::new());
        }

        if self.cache.is_none() {
            return self.read_blocks_uncached(start_block_id, num_blocks);
        }

        let mut bytes = vec![0u8; response_size as usize];
        let bs = block_size as usize;
        let mut i = 0u64;

        while i < num_blocks {
            let block_id = start_block_id + i;

            if let Some(cache) = self.cache.as_mut() {
                let dst = (i as usize) * bs;
                if cache.get_copy_into(block_id, &mut bytes[dst..dst + bs]) {
                    i += 1;
                    continue;
                }
            }

            let mut run_len = 1u64;
            while i + run_len < num_blocks {
                let next_id = start_block_id + i + run_len;
                let cached = self.cache.as_ref().is_some_and(|c| c.contains(next_id));
                if cached {
                    break;
                }
                run_len += 1;
            }

            let fetched = self.read_blocks_uncached(block_id, run_len)?;
            for off in 0..run_len {
                let src = (off as usize) * bs;
                let dst = ((i + off) as usize) * bs;
                let block = fetched[src..src + bs].to_vec();
                bytes[dst..dst + bs].copy_from_slice(&block);
                if let Some(cache) = self.cache.as_mut() {
                    cache.put(block_id + off, block);
                }
            }

            i += run_len;
        }

        Ok(bytes)
    }

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

        if let Some(cache) = self.cache.as_mut() {
            let bs = block_size as usize;
            for off in 0..num_blocks {
                let src = (off as usize) * bs;
                cache.put(start_block_id + off, bytes[src..src + bs].to_vec());
            }
        }

        Ok(())
    }

    fn read_blocks_uncached(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        self.send_command_line(&format!("get block {} {}\n", start_block_id, num_blocks))?;

        let block_size = self.get_block_size()?;
        let response_size = num_blocks
            .checked_mul(block_size)
            .context("Requested read size overflowed u64")?;

        let mut bytes = vec![0u8; response_size as usize];
        self.reader
            .read_exact(&mut bytes)
            .context("Failed to read raw block bytes")?;
        Ok(bytes)
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