# my-awesome-db: Environment Setup

This project uses a dedicated Conda environment with:
- Rust toolchain (`rustc`, `cargo`)
- SQLite command-line tool (`sqlite3`)

## 1) Install Conda (if not installed)

If Conda is already installed, skip this step.

### Ubuntu/Linux (Miniconda)

```bash
mkdir -p "$HOME/miniconda3"
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "$HOME/miniconda3/miniconda.sh"
bash "$HOME/miniconda3/miniconda.sh" -b -u -p "$HOME/miniconda3"
rm -f "$HOME/miniconda3/miniconda.sh"
"$HOME/miniconda3/bin/conda" init bash
```

Restart your terminal after `conda init bash`.

## 2) Create the environment

From the project directory, run:

```bash
conda create -y -n myawesome_db_env -c conda-forge rust sqlite
```

## 3) Activate the environment

```bash
conda activate myawesome_db_env
```

## 4) Verify installation

```bash
rustc --version
cargo --version
sqlite3 --version
```

Expected tools inside this environment:
- `rustc` (Rust compiler)
- `cargo` (Rust package/build tool)
- `sqlite3` (SQLite CLI)

## 5) Deactivate when done

```bash
conda deactivate
```

## Optional: Remove the environment

```bash
conda env remove -n myawesome_db_env
```

## Optional: Run commands without activating

```bash
conda run -n myawesome_db_env rustc --version
conda run -n myawesome_db_env cargo --version
conda run -n myawesome_db_env sqlite3 --version
```

---

# Database Workflow: Export, Build, and Run

This section covers the end-to-end workflow for exporting expected query results, building the database binary, and running validation tests.

## Step 1: Export Expected Query Results from SQLite

Generate expected output files for monitor validation using explicit formatting.

### Create a SQL script (e.g., `q1.sql`)

```sql
.mode list
.separator |
.headers off
SELECT n_nationkey, n_name, n_regionkey, n_comment, '' FROM nation;
```

**Important notes:**
- `.mode list` sets output to list (pipe-delimited) format
- `.separator |` explicitly sets the column separator to `|` (required for portability)
- `.headers off` suppresses column headers
- `''` (empty string column) adds a trailing `|` to each row, as required by the monitor

### Run the export in the conda environment

First activate the environment:

```bash
conda activate myawesome_db_env
```

Then run the export:

```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < q1.sql > scratch/runtimes/tpch/expected_1.csv
```

Verify the output format (should be pipe-delimited with trailing `|`):

```bash
head -5 scratch/runtimes/tpch/expected_1.csv
```

Expected output:
```
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
...
```

## Step 2: Build the Project in Release Mode

Compile all binaries (disk, database, monitor, generator) for optimal performance.

First activate the environment:

```bash
conda activate myawesome_db_env
```

Then build:

```bash
cargo build -r
```

This creates release binaries in `target/release/`:
- `database` — the query execution engine
- `disk` — the disk simulator
- `monitor` — the validation orchestrator
- `generator` — dataset/config generation tool

## Step 3: Run Monitor for Validation

Execute a full test with the monitor, which orchestrates disk, database, and expected output comparison.

First activate the environment:

```bash
conda activate myawesome_db_env
```

Then run the monitor:

```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

### Expected behavior

The monitor will:
1. Start the disk simulator with the disk config
2. Launch the database with the database config and memory limit
3. Send the query JSON to the database
4. Stream the database output row-by-row to the monitor
5. Compare each row with expected output
6. Print success/failure

### Success output

```
--------------------------------------------------------------------------------
Validation success! for Simple Scan
```

### Debugging failed runs

If validation fails:
1. Check expected output format: `head scratch/runtimes/tpch/expected_1.csv`
2. Verify pipe separator and trailing `|` are present
3. Check database output directly (see [database/src/main.rs](database/src/main.rs))
4. Ensure all queries use pipe-delimited format with trailing empty column

---