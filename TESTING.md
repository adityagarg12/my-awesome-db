# My Awesome DB: Build and Test Guide

This README explains how to compile the project from scratch and run tests.

## 1. Prerequisites

Install the following tools:
- Rust toolchain (`cargo`, `rustc`)
- Python 3
- Conda
- sqlite3 CLI (used by `tests_gen`)

Optional but useful:
- `ripgrep` (`rg`) for fast searching

## 2. Get Starter Data

1. Download the `tpch_scratch.tar.gz` starter from [datasets](https://drive.google.com/drive/folders/1NsbGUAsfacgNLeDUC6KrM_HruTTPfe8v?usp=sharing).
2. Place `tpch_scratch.tar.gz` in the repository root.
3. Extract in the root directory:

```bash
tar -xf tpch_scratch.tar.gz
```

## 3. Activate Environment

```bash
conda activate myawesome_db_env
```

If the env does not exist yet, create it first (example):

```bash
conda create -n myawesome_db_env -y
conda activate myawesome_db_env
```

## 4. Compile Everything From Start

From repository root:

```bash
cargo clean
cargo build -r --workspace
```

This compiles all workspace crates in release mode.

## 5. Generate Runtime Artifacts (Fresh)

The system relies on generated `.bin` files and runtime configs.

### 5.1 Single command (recommended)

Yes, this command is sufficient to generate all required artifacts in one shot:

```bash
cargo run -r --bin generator -- all \
  -d scratch/datasets/tpch \
  -c scratch/compiled_datasets/tpch \
  -r scratch/runtimes/tpch \
  -b target/release \
  -s 4096
```

What it generates:
- Compiled dataset binary files
- Disk simulation config
- Database config (`db_config.json`)
- sqlite reference database
- Monitor config scaffold

After this, still run `tests_gen` to regenerate expected outputs and test query list.

### 5.2 Step-by-step generation (optional)

Use this when you only want to regenerate a subset.

#### 5.2.1 Disk files (`.bin`) + disk config

```bash
cargo run -r --bin generator -- disk \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch \
  --block-size 4096
```

#### 5.2.2 Database config (`db_config.json`)

```bash
cargo run -r --bin generator -- database \
  --dataset-folder scratch/datasets/tpch \
  --runtime-folder scratch/runtimes/tpch
```

#### 5.2.3 sqlite reference DB (for expected outputs)

```bash
cargo run -r --bin generator -- sqlite \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch
```

#### 5.2.4 Generate expected outputs + monitor queries

```bash
cargo run -r --bin tests_gen -- \
  -c scratch/compiled_datasets/tpch \
  -r scratch/runtimes/tpch
```

This updates `scratch/runtimes/tpch/monitor_config.json` and expected output CSV files.

## 6. Run Full Test Suite (via monitor)

```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

## 7. Run One Specific Test Case

Example: run only test #39.

```bash
python3 - <<'PY'
import json
src = 'scratch/runtimes/tpch/monitor_config.json'
dst = 'scratch/runtimes/tpch/monitor_config_test52.json'
with open(src) as f:
    cfg = json.load(f)
for i, q in enumerate(cfg['query_configs'], start=1):
    q['disabled'] = (i != 52)
with open(dst, 'w') as f:
    json.dump(cfg, f, indent=2)
print('Enabled query #52:', cfg['query_configs'][51]['execution_name'])
print('Wrote', dst)
PY

cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config_test39.json
```

## 8. Debugging Hangs or Slow Tests

### 7.1 Monitor-side debug logs

```bash
MONITOR_DEBUG=1 cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

### 7.2 Monitor + database debug logs

```bash
MONITOR_DEBUG=1 DATABASE_DEBUG=1 cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

Debug logs are printed to stderr and are protocol-safe.

## 9. Type-Mismatch Note For Local Runs

If, while running generated testcases, you hit a type mismatch like `Cannot compare Int64 with Int32`, that can happen because local generated tests may include comparisons that are not present in the official grading tests.

For local debugging, it is acceptable to:
- Adjust the relevant schema datatype(s) to align the compared columns/literals.
- Regenerate artifacts (`generator`) and expected outputs (`tests_gen`) again.

Then rerun monitor.

## 10. Recommended End-to-End Command Sequence

When schema changes are made, run this full sequence:

```bash
conda activate myawesome_db_env
cargo clean
cargo build -r --workspace

cargo run -r --bin generator -- disk \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch \
  --block-size 4096

cargo run -r --bin generator -- database \
  --dataset-folder scratch/datasets/tpch \
  --runtime-folder scratch/runtimes/tpch

cargo run -r --bin generator -- sqlite \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch

cargo run -r --bin tests_gen -- \
  -c scratch/compiled_datasets/tpch \
  -r scratch/runtimes/tpch

cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

If anything fails after schema updates, regenerate artifacts again before debugging runtime code.
