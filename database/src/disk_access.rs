use anyhow::{Context, Result, bail};
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
}

impl<R: Read, W: Write> DiskAccessLayer<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            block_size_cache: None,
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

    pub fn read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
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