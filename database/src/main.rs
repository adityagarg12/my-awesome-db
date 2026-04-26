use anyhow::{Context, Result};
use clap::Parser;
use common::query::{Query, QueryOp};
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

use crate::{
    catalog::Catalog,
    cli::CliOptions,
    disk_access::DiskAccessLayer,
    executor::{AnonBlockAllocator, build_operator},
    io_setup::{setup_disk_io, setup_monitor_io},
    optimizer::optimize,
    row::format_row_for_output,
};

mod block_codec;
mod catalog;
mod cli;
mod disk_access;
mod executor;
mod io_setup;
mod optimizer;
mod row;

fn get_operator_weight_units(op: &QueryOp) -> u64 {
    match op {
        // Sort keeps merge heap + run-reader state while parent operators pull.
        QueryOp::Sort(d) => 3 + get_operator_weight_units(&d.underlying),
        // Joins/Crosses tend to hold the highest concurrent row-buffer pressure.
        QueryOp::Cross(d) => 10 + get_operator_weight_units(&d.left) + get_operator_weight_units(&d.right),
        QueryOp::Join(d) => 10 + get_operator_weight_units(&d.left) + get_operator_weight_units(&d.right),
        QueryOp::Filter(d) => get_operator_weight_units(&d.underlying),
        QueryOp::Project(d) => get_operator_weight_units(&d.underlying),
        QueryOp::Scan(_) => 0,
    }
}

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    let catalog = Catalog::from_db_context(&ctx)?;

    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, monitor_out) = setup_monitor_io();
    // Batch monitor writes to avoid one syscall per output row.
    // The explicit flush() calls already present at "get_memory_limit" (line below)
    // and at the end of the result-streaming loop ensure correct protocol ordering.
    let mut monitor_out = std::io::BufWriter::with_capacity(64 * 1024, monitor_out);

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

    // Reserve memory for the shared buffer pool and per-ScanOperator prefetch buffers
    // so operators receive a reduced limit that keeps total allocation under RLIMIT_AS.
    //
    // BUFFER_POOL_BLOCKS must match BlockCache::new(N) in DiskAccessLayer::new().
    // PREFETCH_BLOCKS_PER_SCAN must match PREFETCH_SIZE in executor.rs.
    const BUFFER_POOL_BLOCKS: u64 = 512;      // 512 × 4 KB = 2 MB cache
    const PREFETCH_BLOCKS_PER_SCAN: u64 = 32; // 32 × 4 KB = 128 KB per scan prefetch
    const MAX_CONCURRENT_SCANS: u64 = 2;
    let buffer_reserved = (BUFFER_POOL_BLOCKS + PREFETCH_BLOCKS_PER_SCAN * MAX_CONCURRENT_SCANS)
        .saturating_mul(block_size);
    let operator_memory_limit = memory_limit_bytes.saturating_sub(buffer_reserved);

    // 4. Optimize query tree, then build operator tree
    let optimized = optimize(query.root, &catalog);
    let total_weight_units = get_operator_weight_units(&optimized).max(1);
    let bytes_per_weight_unit = (operator_memory_limit / total_weight_units).max(block_size);
    let mut root_op = build_operator(
        &optimized,
        &catalog,
        disk_access,
        block_size,
        bytes_per_weight_unit,
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
