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
