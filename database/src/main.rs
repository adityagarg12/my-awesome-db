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

fn is_debug_enabled() -> bool {
    matches!(
        std::env::var("DATABASE_DEBUG").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn debug_log(message: &str) {
    if is_debug_enabled() {
        eprintln!("[database-debug] {message}");
    }
}

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();
    debug_log(&format!(
        "loading db config {}",
        cli_options.get_config_path().to_string_lossy()
    ));

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    let catalog = Catalog::from_db_context(&ctx)?;
    debug_log("catalog loaded");

    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let disk_access = Arc::new(Mutex::new(DiskAccessLayer::new(disk_in, disk_out)));
    let mut monitor_reader = BufReader::new(monitor_in);
    let mut line = String::new();

    // 1. Receive query
    monitor_reader.read_line(&mut line)?;
    let query: Query = serde_json::from_str(line.trim())
        .context("Failed to parse query JSON")?;
    debug_log("query JSON received and parsed");

    // 2. Request memory limit
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    debug_log("requested memory limit from monitor");
    line.clear();
    monitor_reader.read_line(&mut line)?;
    let memory_limit_mb: u64 = line.trim().parse().context("Failed to parse memory limit")?;
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;
    debug_log(&format!("received memory limit: {} MB", memory_limit_mb));

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
    debug_log(&format!(
        "disk block_size={}, anon_start_block={}",
        block_size, anon_start
    ));

    // 4. Build operator tree
    let mut root_op = build_operator(
        &query.root,
        &catalog,
        disk_access,
        block_size,
        memory_limit_bytes,
        &anon_allocator,
    )?;
    debug_log("operator tree built");

    // 5. Stream results to monitor
    monitor_out.write_all(b"validate\n")?;
    debug_log("sent validate command to monitor");
    let mut row_count: usize = 0;
    while let Some(row) = root_op.next()? {
        let mut out = format_row_for_output(&row);
        out.push('\n');
        monitor_out.write_all(out.as_bytes())?;
        row_count += 1;
        if is_debug_enabled() && row_count % 10000 == 0 {
            eprintln!("[database-debug] streamed {row_count} rows");
        }
    }
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;
    debug_log(&format!("finished streaming {} rows", row_count));

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
