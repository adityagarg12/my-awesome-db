use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

use crate::{
    catalog::Catalog,
    cli::CliOptions,
    disk_access::DiskAccessLayer,
    executor::{AnonBlockAllocator, build_operator},
    io_setup::{setup_disk_io, setup_monitor_io},
    row::format_row_for_output,
};

mod block_codec;
mod catalog;
mod cli;
mod disk_access;
mod executor;
mod io_setup;
mod row;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    let catalog = Catalog::from_db_context(&ctx)?;

    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let disk_access = Arc::new(Mutex::new(DiskAccessLayer::new(disk_in, disk_out)));
    let mut monitor_reader = BufReader::new(monitor_in);
    let mut line = String::new();

    // 1. Receive query
    monitor_reader.read_line(&mut line)?;
    let query: Query = serde_json::from_str(line.trim())
        .context("Failed to parse query JSON")?;

    // 2. Request memory limit
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    line.clear();
    monitor_reader.read_line(&mut line)?;
    let memory_limit_mb: u64 = line.trim().parse().context("Failed to parse memory limit")?;
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;

    // 3. Get block size and anonymous-region start from disk
    let block_size = {
        let mut disk = disk_access.lock().unwrap();
        disk.get_block_size()?
    };
    let anon_start = {
        let mut disk = disk_access.lock().unwrap();
        disk.get_anon_start_block()?
    };
    let anon_allocator = AnonBlockAllocator::new(anon_start);

    // 4. Build operator tree
    let mut root_op = build_operator(
        &query.root,
        &catalog,
        disk_access,
        block_size,
        memory_limit_bytes,
        &anon_allocator,
    )?;

    // 5. Stream results to monitor
    monitor_out.write_all(b"validate\n")?;
    while let Some(row) = root_op.next()? {
        let mut out = format_row_for_output(&row);
        out.push('\n');
        monitor_out.write_all(out.as_bytes())?;
    }
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
