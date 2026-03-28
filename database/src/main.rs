use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::{
    catalog::Catalog,
    cli::CliOptions,
    disk_access::DiskAccessLayer,
    io_setup::{setup_disk_io, setup_monitor_io},
};

mod block_codec;
mod catalog;
mod cli;
mod disk_access;
mod io_setup;
mod row;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Build metadata layer (table/column/type lookup) from db config
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    let catalog = Catalog::from_db_context(&ctx)?;

    for table in catalog.tables() {
        println!("Table: {}", table.name);
        println!("File id: {}", table.file_id);
        for column in table.columns() {
            println!("\tColumn[{}]: {} ({:?})", column.index, column.name, column.data_type);
        }
        println!();
    }

    if let Some(nation_file_id) = catalog.get_file_id("nation") {
        println!("Lookup example: file id for table nation is {}", nation_file_id);
    }

    if let Some(column_index) = catalog.get_column_index("nation", "n_name") {
        println!("Lookup example: nation.n_name is at column index {}", column_index);
    }

    // Setups and provides handler to talk with disk and monitor
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();
    let mut disk_access_layer = DiskAccessLayer::new(disk_in, disk_out);

    // Use buffered reader to read lines easier
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    // Temporary variable to read a line of input
    let mut input_line = String::new();

    // Read query form monitor
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line).unwrap();
    println!("Input query is: {:#?}", query);

    // Interacting with with Disk

    let block_size = disk_access_layer.get_block_size()?;

    println!("block size is {}", block_size);

    let buf = disk_access_layer.read_blocks(0, 1)?;
    let preview_len = (buf.len()).min(50);

    println!(
        "First few bytes of block 0 contains {:?}",
        String::from_utf8_lossy(&buf[..preview_len])
    );

    // Get memory limit from monitor
    input_line.clear();
    monitor_out.write_all("get_memory_limit\n".as_bytes())?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    println!("Memory limit is set to {} MB", memory_limit_mb);

    // Send result of query to monitor for validation
    /*
    monitor_out.write_all("validate\n".as_bytes())?;
    monitor_out.write_all("1|hello|DBMS|\n".as_bytes())?;
    monitor_out.write_all("!\n".as_bytes())?;
    monitor_out.flush()?;
    */

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
