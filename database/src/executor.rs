use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use anyhow::{Result, anyhow};
use common::{Data, DataType};
use common::query::{ComparisionValue, Predicate, QueryOp, SortSpec};
use crate::catalog::{Catalog, ColumnCatalog, TableCatalog};
use crate::disk_access::DiskAccessLayer;
use crate::row::{Row, data_compare, encode_row};
use crate::block_codec::{decode_block, encode_rows_to_blocks};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Runtime schema: ordered column list with name→index lookup.
#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<ColumnCatalog>,
    name_to_index: HashMap<String, usize>,
}

impl Schema {
    /// Build from an ordered column list. The *position* in `columns` is used
    /// as the row-value index (not `ColumnCatalog::index`).
    pub fn new(columns: Vec<ColumnCatalog>) -> Self {
        let mut name_to_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            name_to_index.insert(col.name.clone(), i);
        }
        Self { columns, name_to_index }
    }

    pub fn from_table(table: &TableCatalog) -> Self {
        Self::new(table.columns().to_vec())
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn get_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Concatenate two schemas (for Cross product).
    pub fn merge(left: &Schema, right: &Schema) -> Self {
        let mut cols = Vec::with_capacity(left.len() + right.len());
        for (i, c) in left.columns.iter().enumerate() {
            cols.push(ColumnCatalog { name: c.name.clone(), index: i, data_type: c.data_type.clone() });
        }
        let offset = left.len();
        for (i, c) in right.columns.iter().enumerate() {
            cols.push(ColumnCatalog { name: c.name.clone(), index: offset + i, data_type: c.data_type.clone() });
        }
        Self::new(cols)
    }

    /// Build the projected schema and return (projected_schema, src_indices).
    /// `column_map` is [(from_name, to_name)].
    pub fn project(input: &Schema, column_map: &[(String, String)]) -> Result<(Self, Vec<usize>)> {
        let mut cols = Vec::new();
        let mut src_indices = Vec::new();
        for (i, (from, to)) in column_map.iter().enumerate() {
            let src_idx = input.get_index(from)
                .ok_or_else(|| anyhow!("Column '{}' not found in schema", from))?;
            let src_col = &input.columns[src_idx];
            cols.push(ColumnCatalog { name: to.clone(), index: i, data_type: src_col.data_type.clone() });
            src_indices.push(src_idx);
        }
        Ok((Self::new(cols), src_indices))
    }
}

// ---------------------------------------------------------------------------
// Operator trait
// ---------------------------------------------------------------------------

pub trait Operator {
    fn schema(&self) -> &Schema;
    /// Return the next row, or None if exhausted.
    fn next(&mut self) -> Result<Option<Row>>;
}

// ---------------------------------------------------------------------------
// Anonymous-block allocator (sparse disk scratch space)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AnonBlockAllocator {
    next_block: Arc<Mutex<u64>>,
}

impl AnonBlockAllocator {
    pub fn new(start: u64) -> Self {
        Self { next_block: Arc::new(Mutex::new(start)) }
    }
    /// Atomically allocate `count` consecutive anonymous blocks.
    /// Returns the starting block ID.
    pub fn allocate(&self, count: u64) -> u64 {
        let mut next = self.next_block.lock().unwrap();
        let start = *next;
        *next += count;
        start
    }
}

// ---------------------------------------------------------------------------
// ScanOperator
// ---------------------------------------------------------------------------

pub struct ScanOperator<R: Read, W: Write> {
    disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
    schema: Schema,
    file_id: String,
    block_size: u64,
    // Populated on first next()
    start_block: u64,
    num_blocks: u64,
    current_block_offset: u64,
    current_rows: Vec<Row>,
    current_row_idx: usize,
    initialized: bool,
}

impl<R: Read, W: Write> ScanOperator<R, W> {
    pub fn new(
        disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
        table: &TableCatalog,
        block_size: u64,
    ) -> Self {
        ScanOperator {
            disk_access,
            schema: Schema::from_table(table),
            file_id: table.file_id.clone(),
            block_size,
            start_block: 0,
            num_blocks: 0,
            current_block_offset: 0,
            current_rows: Vec::new(),
            current_row_idx: 0,
            initialized: false,
        }
    }

    fn initialize(&mut self) -> Result<()> {
        let mut disk = self.disk_access.lock().unwrap();
        self.start_block = disk.get_file_start_block(&self.file_id)?;
        self.num_blocks = disk.get_file_num_blocks(&self.file_id)?;
        self.initialized = true;
        Ok(())
    }

    /// Load the next block from disk into `current_rows`. Returns false when
    /// all blocks have been read.
    fn load_next_block(&mut self) -> Result<bool> {
        if self.current_block_offset >= self.num_blocks {
            return Ok(false);
        }
        let block_id = self.start_block + self.current_block_offset;
        let block_data = {
            let mut disk = self.disk_access.lock().unwrap();
            disk.read_blocks(block_id, 1)?
        };
        self.current_rows = decode_block(&block_data, self.block_size, self.schema.columns())?;
        self.current_row_idx = 0;
        self.current_block_offset += 1;
        Ok(true)
    }
}

impl<R: Read, W: Write> Operator for ScanOperator<R, W> {
    fn schema(&self) -> &Schema { &self.schema }

    fn next(&mut self) -> Result<Option<Row>> {
        if !self.initialized {
            self.initialize()?;
        }
        loop {
            if self.current_row_idx < self.current_rows.len() {
                let row = self.current_rows[self.current_row_idx].clone();
                self.current_row_idx += 1;
                return Ok(Some(row));
            }
            if !self.load_next_block()? {
                return Ok(None);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FilterOperator
// ---------------------------------------------------------------------------

pub struct FilterOperator {
    child: Box<dyn Operator>,
    predicates: Vec<Predicate>,
    schema: Schema,
}

impl FilterOperator {
    pub fn new(child: Box<dyn Operator>, predicates: Vec<Predicate>) -> Self {
        let schema = child.schema().clone();
        FilterOperator { child, predicates, schema }
    }

    fn row_passes(&self, row: &Row) -> Result<bool> {
        for pred in &self.predicates {
            let col_idx = self.schema.get_index(&pred.column_name)
                .ok_or_else(|| anyhow!("Filter: column '{}' not found", pred.column_name))?;
            let left_val = row.get(col_idx)
                .ok_or_else(|| anyhow!("Filter: row has no value at index {}", col_idx))?;

            let right_val: Data = match &pred.value {
                ComparisionValue::Column(name) => {
                    let idx = self.schema.get_index(name)
                        .ok_or_else(|| anyhow!("Filter: column '{}' not found", name))?;
                    row.get(idx)
                        .ok_or_else(|| anyhow!("Filter: row has no value at index {}", idx))?
                        .clone()
                }
                ComparisionValue::I32(v)    => Data::Int32(*v),
                ComparisionValue::I64(v)    => Data::Int64(*v),
                ComparisionValue::F32(v)    => Data::Float32(*v),
                ComparisionValue::F64(v)    => Data::Float64(*v),
                ComparisionValue::String(v) => Data::String(v.clone()),
            };

            if !data_compare(left_val, &right_val, &pred.operator)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl Operator for FilterOperator {
    fn schema(&self) -> &Schema { &self.schema }

    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            match self.child.next()? {
                None      => return Ok(None),
                Some(row) => {
                    if self.row_passes(&row)? {
                        return Ok(Some(row));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ProjectOperator
// ---------------------------------------------------------------------------

pub struct ProjectOperator {
    child: Box<dyn Operator>,
    src_indices: Vec<usize>,
    schema: Schema,
}

impl ProjectOperator {
    pub fn new(child: Box<dyn Operator>, column_name_map: Vec<(String, String)>) -> Result<Self> {
        let (schema, src_indices) = Schema::project(child.schema(), &column_name_map)?;
        Ok(ProjectOperator { child, src_indices, schema })
    }
}

impl Operator for ProjectOperator {
    fn schema(&self) -> &Schema { &self.schema }

    fn next(&mut self) -> Result<Option<Row>> {
        match self.child.next()? {
            None      => Ok(None),
            Some(row) => {
                let values: Vec<Data> = self.src_indices.iter()
                    .map(|&idx| row.get(idx).expect("project: missing column").clone())
                    .collect();
                Ok(Some(Row::new(values)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CrossOperator  — Block Nested-Loop Join (B-NLJ)
//
// Phase 1  (init, one-time):
//   Consume right child row-by-row; pack each row into a block-sized byte
//   buffer and flush to anonymous disk when the buffer is full.
//   At most one block of encoded bytes lives in RAM at any moment.
//
// Phase 2  (next, B-NLJ proper):
//   outer  : left_chunk  — fill RAM with as many left rows as the memory
//            budget allows; re-fill from the left child when exhausted.
//   middle : right_blocks — for each left_chunk, scan every right block
//            from disk exactly once (1 block in RAM at a time).
//   inner  : emit left_chunk[i] ⊗ right_block_row[j] for all i,j.
//
// Total right-disk reads = ceil(|left_rows| / left_chunk_capacity) × |right_blocks|
// instead of |left_rows| × |right_blocks| if we looped per left row.
// ---------------------------------------------------------------------------

pub struct CrossOperator<R: Read, W: Write> {
    left: Box<dyn Operator>,
    right: Option<Box<dyn Operator>>,   // consumed once during init
    schema: Schema,
    disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
    block_size: u64,
    anon_allocator: AnonBlockAllocator,
    right_cols: Vec<ColumnCatalog>,
    memory_limit_bytes: u64,

    // Populated after spill
    initialized: bool,
    right_is_empty: bool,
    right_start_block: u64,
    right_num_blocks: u64,

    // B-NLJ iteration state
    left_chunk: Vec<Row>,           // buffered left rows for current outer pass
    left_exhausted: bool,
    right_block_offset: u64,        // which right block (0-based) we are scanning
    right_block_rows: Vec<Row>,     // decoded rows from current right block
    right_row_idx: usize,           // which row inside right_block_rows
    left_chunk_idx: usize,          // which left_chunk row to pair with current right row
}

impl<R: Read, W: Write> CrossOperator<R, W> {
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
        block_size: u64,
        memory_limit_bytes: u64,
        anon_allocator: AnonBlockAllocator,
    ) -> Self {
        let schema     = Schema::merge(left.schema(), right.schema());
        let right_cols = right.schema().columns().to_vec();
        CrossOperator {
            left, right: Some(right), schema,
            disk_access, block_size, anon_allocator, right_cols,
            memory_limit_bytes,
            initialized: false, right_is_empty: false,
            right_start_block: 0, right_num_blocks: 0,
            left_chunk: Vec::new(), left_exhausted: false,
            right_block_offset: 0,
            right_block_rows: Vec::new(),
            right_row_idx: 0, left_chunk_idx: 0,
        }
    }

    /// Estimate in-memory size of one left row (same logic as SortOperator).
    fn left_row_size(&self) -> usize {
        let mut s = 24usize;
        for col in self.left.schema().columns() {
            s += match col.data_type { DataType::String => 64, _ => 32 };
        }
        s
    }

    /// How many left rows to buffer per chunk.
    ///
    /// Unlike SortOperator there is no double-allocation proportional to
    /// chunk size: the right spill uses a fixed block_buf (~4 KB) that is
    /// freed before B-NLJ begins, and the B-NLJ phase only holds the left
    /// chunk + one decoded right block simultaneously.
    ///
    /// Process overhead (release binary ~12 MB, debug ~14 MB):
    ///   text + data      : ~1.2 MB release / 2.7 MB debug
    ///   shared libs      : ~2.5 MB (libc, libgcc)
    ///   stack reservation: ~8 MB  (Linux default)
    ///   heap metadata    : ~0.5 MB
    ///   right block buf  : 4 × block_size (~16 KB, negligible)
    ///
    /// Reserve 16 MB for all overhead; give everything else to the left chunk.
    /// 64 MB limit  → 48 MB available (75 %)
    /// 256 MB limit → 240 MB available (94 %)
    /// 
    fn left_chunk_capacity(&self) -> usize {
        // TO BE CHECK , we can adjust the PROCESS_OVERHEAD estimate if needed based on actual measurements, which came around 14mb at startup in debug mode and 12mb in release mode. We want to be conservative to avoid OOM, but not too much to underutilize memory.
        const PROCESS_OVERHEAD: u64 = 32 * 1024 * 1024; // 32 MB
        let right_buf = self.block_size * 4;
        let available = self.memory_limit_bytes
            .saturating_sub(PROCESS_OVERHEAD)
            .saturating_sub(right_buf);
        (available as usize / self.left_row_size()).max(64)
    }

    // ------------------------------------------------------------------
    // Phase 1: spill right to anonymous disk, one block at a time
    // ------------------------------------------------------------------
    fn spill_right(&mut self) -> Result<()> {
        let mut right_child = self.right.take().unwrap();
        let usable = self.block_size as usize - 2;

        let mut block_buf      = vec![0u8; self.block_size as usize];
        let mut buf_offset     = 0usize;
        let mut rows_in_block  = 0u16;
        let mut total_blocks   = 0u64;
        let mut first_block    = 0u64;
        let mut got_first      = false;
        let mut has_any_row    = false;

        while let Some(row) = right_child.next()? {
            has_any_row = true;
            let encoded = encode_row(&row, &self.right_cols)?;
            if encoded.len() > usable {
                anyhow::bail!(
                    "CrossOperator: right row ({} B) larger than usable block space ({} B)",
                    encoded.len(), usable
                );
            }

            if buf_offset + encoded.len() > usable {
                // Flush current block to disk
                let footer = block_buf.len() - 2;
                block_buf[footer..].copy_from_slice(&rows_in_block.to_le_bytes());
                let id = self.anon_allocator.allocate(1);
                if !got_first { first_block = id; got_first = true; }
                self.disk_access.lock().unwrap().write_blocks(id, 1, &block_buf)?;
                total_blocks += 1;
                block_buf.fill(0); buf_offset = 0; rows_in_block = 0;
            }

            block_buf[buf_offset..buf_offset + encoded.len()].copy_from_slice(&encoded);
            buf_offset    += encoded.len();
            rows_in_block += 1;
        }

        if !has_any_row {
            self.right_is_empty = true;
            return Ok(());
        }

        // Flush the final (possibly partial) block
        let footer = block_buf.len() - 2;
        block_buf[footer..].copy_from_slice(&rows_in_block.to_le_bytes());
        let id = self.anon_allocator.allocate(1);
        if !got_first { first_block = id; }
        self.disk_access.lock().unwrap().write_blocks(id, 1, &block_buf)?;
        total_blocks += 1;

        self.right_start_block = first_block;
        self.right_num_blocks  = total_blocks;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Phase 2 helpers
    // ------------------------------------------------------------------

    /// Fill left_chunk with up to left_chunk_capacity rows from the left child.
    fn fill_left_chunk(&mut self) -> Result<()> {
        self.left_chunk.clear();
        let cap = self.left_chunk_capacity();
        while self.left_chunk.len() < cap {
            match self.left.next()? {
                Some(row) => self.left_chunk.push(row),
                None      => { self.left_exhausted = true; break; }
            }
        }
        Ok(())
    }

    /// Load right block at `offset` into right_block_rows; reset right_row_idx.
    fn load_right_block(&mut self, offset: u64) -> Result<()> {
        let id   = self.right_start_block + offset;
        let data = { self.disk_access.lock().unwrap().read_blocks(id, 1)? };
        self.right_block_rows = decode_block(&data, self.block_size, &self.right_cols)?;
        self.right_row_idx    = 0;
        Ok(())
    }

    fn initialize(&mut self) -> Result<()> {
        self.spill_right()?;
        if !self.right_is_empty {
            // Load first left chunk and first right block
            self.fill_left_chunk()?;
            if !self.left_chunk.is_empty() {
                self.left_chunk_idx    = 0;
                self.right_block_offset = 0;
                self.load_right_block(0)?;
            }
        }
        self.initialized = true;
        Ok(())
    }
}

impl<R: Read, W: Write> Operator for CrossOperator<R, W> {
    fn schema(&self) -> &Schema { &self.schema }

    fn next(&mut self) -> Result<Option<Row>> {
        if !self.initialized {
            self.initialize()?;
        }

        if self.right_is_empty || self.left_chunk.is_empty() {
            return Ok(None);
        }

        loop {
            // --- inner loop: pair current right row with remaining left_chunk rows ---
            if self.right_row_idx < self.right_block_rows.len() {
                if self.left_chunk_idx < self.left_chunk.len() {
                    let left_row  = &self.left_chunk[self.left_chunk_idx];
                    let right_row = &self.right_block_rows[self.right_row_idx];
                    let mut values = left_row.values().to_vec();
                    values.extend_from_slice(right_row.values());
                    self.left_chunk_idx += 1;
                    return Ok(Some(Row::new(values)));
                }
                // All left_chunk rows paired with this right row → advance right row
                self.left_chunk_idx = 0;
                self.right_row_idx += 1;
                continue;
            }

            // --- right block exhausted: move to next right block ---
            self.right_block_offset += 1;
            if self.right_block_offset < self.right_num_blocks {
                self.load_right_block(self.right_block_offset)?;
                self.left_chunk_idx = 0;
                continue;
            }

            // --- all right blocks done for this left chunk: load next left chunk ---
            self.fill_left_chunk()?;
            if self.left_chunk.is_empty() {
                return Ok(None); // left fully exhausted
            }
            // Reset right scan for the new left chunk
            self.right_block_offset = 0;
            self.load_right_block(0)?;
            self.left_chunk_idx = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// SortOperator  (external merge sort via anonymous disk blocks)
// ---------------------------------------------------------------------------

/// Reads one sorted run back from disk, one block at a time.
struct RunReader<R: Read, W: Write> {
    disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
    columns: Vec<ColumnCatalog>,
    block_size: u64,
    start_block: u64,
    num_blocks: u64,
    blocks_read: u64,
    current_rows: Vec<Row>,
    row_idx: usize,
}

impl<R: Read, W: Write> RunReader<R, W> {
    fn new(
        disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
        columns: Vec<ColumnCatalog>,
        block_size: u64,
        start_block: u64,
        num_blocks: u64,
    ) -> Self {
        RunReader { disk_access, columns, block_size, start_block, num_blocks,
                    blocks_read: 0, current_rows: Vec::new(), row_idx: 0 }
    }

    fn next_row(&mut self) -> Result<Option<Row>> {
        loop {
            if self.row_idx < self.current_rows.len() {
                let row = self.current_rows[self.row_idx].clone();
                self.row_idx += 1;
                return Ok(Some(row));
            }
            if self.blocks_read >= self.num_blocks {
                return Ok(None);
            }
            let block_id = self.start_block + self.blocks_read;
            let data = {
                let mut disk = self.disk_access.lock().unwrap();
                disk.read_blocks(block_id, 1)?
            };
            self.current_rows = decode_block(&data, self.block_size, &self.columns)?;
            self.row_idx = 0;
            self.blocks_read += 1;
        }
    }
}

/// Entry in the k-way merge heap. BinaryHeap is max-heap; we reverse the
/// comparison so the *smallest* sorted row is popped first.
struct HeapEntry {
    /// (value, ascending) for each sort key, in priority order.
    key: Vec<(Data, bool)>,
    row: Row,
    run_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == std::cmp::Ordering::Equal }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for ((sv, asc), (ov, _)) in self.key.iter().zip(other.key.iter()) {
            let ord = sv.partial_cmp(ov).unwrap_or(std::cmp::Ordering::Equal);
            // If ascending: smaller sv should pop first → reverse for max-heap
            // If descending: larger sv should pop first → natural order then reverse for max-heap
            let ord = if *asc { ord } else { ord.reverse() };
            let ord = ord.reverse(); // flip for max-heap → min-heap behaviour
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}

pub struct SortOperator<R: Read, W: Write> {
    child: Option<Box<dyn Operator>>,
    sort_specs: Vec<SortSpec>,
    schema: Schema,
    memory_limit_bytes: u64,
    disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
    block_size: u64,
    anon_allocator: AnonBlockAllocator,
    initialized: bool,
    // In-memory path
    in_memory_rows: Vec<Row>,
    in_memory_idx: usize,
    // External-sort path
    run_readers: Vec<RunReader<R, W>>,
    merge_heap: BinaryHeap<HeapEntry>,
}

impl<R: Read, W: Write> SortOperator<R, W> {
    pub fn new(
        child: Box<dyn Operator>,
        sort_specs: Vec<SortSpec>,
        disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
        block_size: u64,
        memory_limit_bytes: u64,
        anon_allocator: AnonBlockAllocator,
    ) -> Self {
        let schema = child.schema().clone();
        SortOperator {
            child: Some(child), sort_specs, schema,
            memory_limit_bytes, disk_access, block_size, anon_allocator,
            initialized: false,
            in_memory_rows: Vec::new(), in_memory_idx: 0,
            run_readers: Vec::new(), merge_heap: BinaryHeap::new(),
        }
    }

    /// Conservative in-memory size estimate per row.
    fn row_mem_estimate(&self) -> usize {
        let mut size = 24usize; // Vec<Data> header
        for col in self.schema.columns() {
            size += match col.data_type {
                DataType::String => 64, // Data enum (32) + heap string estimate (32)
                _                => 32, // Data enum size (largest variant)
            };
        }
        size
    }

    fn sort_slice(rows: &mut Vec<Row>, specs: &[SortSpec], schema: &Schema) {
        rows.sort_unstable_by(|a, b| {
            for spec in specs {
                let idx = schema.get_index(&spec.column_name).unwrap_or(0);
                if let (Some(av), Some(bv)) = (a.get(idx), b.get(idx)) {
                    let ord = av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal);
                    let ord = if spec.ascending { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn write_run(&self, rows: &[Row]) -> Result<(u64, u64)> {
        let encoded = encode_rows_to_blocks(rows, self.block_size, self.schema.columns())?;
        let num_blocks = (encoded.len() as u64 + self.block_size - 1) / self.block_size;
        // encode_rows_to_blocks always returns a multiple of block_size
        let start = self.anon_allocator.allocate(num_blocks);
        let mut disk = self.disk_access.lock().unwrap();
        disk.write_blocks(start, num_blocks, &encoded)?;
        Ok((start, num_blocks))
    }

    fn make_heap_key(&self, row: &Row) -> Vec<(Data, bool)> {
        self.sort_specs.iter().map(|spec| {
            let idx = self.schema.get_index(&spec.column_name).unwrap_or(0);
            let val = row.get(idx).cloned().unwrap_or(Data::Int32(0));
            (val, spec.ascending)
        }).collect()
    }

    fn initialize(&mut self) -> Result<()> {
        let row_size = self.row_mem_estimate();
        // Use 40% of the memory budget for one sorting chunk
        let chunk_capacity = ((self.memory_limit_bytes as usize * 40 / 100) / row_size).max(128);

        let mut child = self.child.take().unwrap();
        let mut chunk: Vec<Row> = Vec::with_capacity(chunk_capacity);
        let mut runs: Vec<(u64, u64)> = Vec::new();

        loop {
            match child.next()? {
                Some(row) => {
                    chunk.push(row);
                    if chunk.len() >= chunk_capacity {
                        Self::sort_slice(&mut chunk, &self.sort_specs, &self.schema);
                        runs.push(self.write_run(&chunk)?);
                        chunk.clear();
                    }
                }
                None => break,
            }
        }

        if runs.is_empty() {
            // Everything fit in one chunk → pure in-memory sort
            Self::sort_slice(&mut chunk, &self.sort_specs, &self.schema);
            self.in_memory_rows = chunk;
        } else {
            // Write the tail chunk (if non-empty) as the last run
            if !chunk.is_empty() {
                Self::sort_slice(&mut chunk, &self.sort_specs, &self.schema);
                runs.push(self.write_run(&chunk)?);
            }

            // Set up RunReaders and prime the merge heap
            let cols = self.schema.columns().to_vec();
            for (start, n) in runs {
                self.run_readers.push(RunReader::new(
                    self.disk_access.clone(), cols.clone(), self.block_size, start, n,
                ));
            }
            for run_idx in 0..self.run_readers.len() {
                if let Some(row) = self.run_readers[run_idx].next_row()? {
                    let key = self.make_heap_key(&row);
                    self.merge_heap.push(HeapEntry { key, row, run_idx });
                }
            }
        }

        self.initialized = true;
        Ok(())
    }
}

impl<R: Read, W: Write> Operator for SortOperator<R, W> {
    fn schema(&self) -> &Schema { &self.schema }

    fn next(&mut self) -> Result<Option<Row>> {
        if !self.initialized {
            self.initialize()?;
        }

        if !self.run_readers.is_empty() {
            // External merge path
            match self.merge_heap.pop() {
                None        => Ok(None),
                Some(entry) => {
                    let run_idx = entry.run_idx;
                    if let Some(next_row) = self.run_readers[run_idx].next_row()? {
                        let key = self.make_heap_key(&next_row);
                        self.merge_heap.push(HeapEntry { key, row: next_row, run_idx });
                    }
                    Ok(Some(entry.row))
                }
            }
        } else {
            // In-memory path
            if self.in_memory_idx < self.in_memory_rows.len() {
                let row = self.in_memory_rows[self.in_memory_idx].clone();
                self.in_memory_idx += 1;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// build_operator  —  recursively turn a QueryOp tree into Operator boxes
// ---------------------------------------------------------------------------

pub fn build_operator<R: Read + 'static, W: Write + 'static>(
    op: &QueryOp,
    catalog: &Catalog,
    disk_access: Arc<Mutex<DiskAccessLayer<R, W>>>,
    block_size: u64,
    memory_limit_bytes: u64,
    anon_allocator: &AnonBlockAllocator,
) -> Result<Box<dyn Operator>> {
    match op {
        QueryOp::Scan(d) => {
            let table = catalog.get_table(&d.table_id)
                .ok_or_else(|| anyhow!("Scan: table '{}' not found", d.table_id))?;
            Ok(Box::new(ScanOperator::new(disk_access, table, block_size)))
        }

        QueryOp::Filter(d) => {
            let child = build_operator(
                &d.underlying, catalog, disk_access, block_size, memory_limit_bytes, anon_allocator,
            )?;
            Ok(Box::new(FilterOperator::new(child, d.predicates.clone())))
        }

        QueryOp::Project(d) => {
            let child = build_operator(
                &d.underlying, catalog, disk_access, block_size, memory_limit_bytes, anon_allocator,
            )?;
            Ok(Box::new(ProjectOperator::new(child, d.column_name_map.clone())?))
        }

        QueryOp::Sort(d) => {
            let child = build_operator(
                &d.underlying, catalog, disk_access.clone(), block_size, memory_limit_bytes, anon_allocator,
            )?;
            Ok(Box::new(SortOperator::new(
                child, d.sort_specs.clone(),
                disk_access, block_size, memory_limit_bytes,
                anon_allocator.clone(),
            )))
        }

        QueryOp::Cross(d) => {
            let left  = build_operator(&d.left,  catalog, disk_access.clone(), block_size, memory_limit_bytes, anon_allocator)?;
            let right = build_operator(&d.right, catalog, disk_access.clone(), block_size, memory_limit_bytes, anon_allocator)?;
            Ok(Box::new(CrossOperator::new(left, right, disk_access, block_size, memory_limit_bytes, anon_allocator.clone())))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_is_object_safe() {
        let _: Option<Box<dyn Operator>> = None;
    }
}
