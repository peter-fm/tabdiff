# 🤖 tabdiff: A Snapshot-Based Structured Data Diff Tool

**tabdiff** is a command-line tool for detecting **schema**, **column-level**, and **row-level** changes between versions of structured datasets such as **Parquet**, **CSV**, and **Hive-style directories**, both local and remote (e.g., **S3**).

## 🚀 Features

- **Fast and memory-efficient** data processing using DuckDB
- **Multiple file format support**: CSV, Parquet, JSON, TSV
- **Git-friendly workflow** with lightweight JSON summaries
- **Compressed archives** for full snapshot data (DVC-compatible)
- **Flexible sampling strategies** for large datasets
- **Schema, column, and row-level diffing**
- **Progress reporting** for long-running operations

## 📦 Installation

### Quick Start

#### Option 1: System DuckDB (Default)

**Prerequisites:**
- **macOS**: `brew install duckdb`
- **Ubuntu/Debian**: `sudo apt install libduckdb-dev`
- **RHEL/CentOS**: `sudo yum install duckdb-devel`
- **Windows**: Download from [duckdb.org](https://duckdb.org/docs/installation/)

```bash
git clone <repository-url>
cd tabdiff
cargo build --release
./target/release/tabdiff --help
```

#### Option 2: Bundled DuckDB (Zero Dependencies)
```bash
cargo build --release --features bundled
```
- ✅ Zero configuration required
- ✅ Works on all platforms
- ✅ No external dependencies
- ⚠️ Larger binary size (~50MB)

#### Option 3: Custom DuckDB Path
```bash
export DUCKDB_LIB_PATH=/custom/path/to/duckdb/lib
cargo build --release
```

### Installation Verification

Test your installation:
```bash
./target/release/tabdiff --version
./target/release/tabdiff init
```

If you encounter issues, run with verbose logging:
```bash
./target/release/tabdiff --verbose init
```

## 🎯 Quick Start

### 1. Initialize a workspace

```bash
tabdiff init
```

This creates a `.tabdiff/` directory in your project (similar to `.git/`).

### 2. Create your first snapshot

```bash
tabdiff snapshot data.csv --name baseline
```

This creates:
- `.tabdiff/baseline.tabdiff` (compressed archive - Git ignored)
- `.tabdiff/baseline.json` (lightweight summary - Git tracked)

### 3. Make changes to your data and create another snapshot

```bash
tabdiff snapshot data_updated.csv --name v2
```

### 4. Compare snapshots

```bash
tabdiff diff baseline v2
```

### 5. Check current data status

```bash
tabdiff status data.csv --compare-to baseline
```

## 📋 Command Reference

### `tabdiff init`
Initialize a tabdiff workspace in the current directory.

```bash
tabdiff init [--force]
```

### `tabdiff snapshot`
Create a snapshot of structured data.

```bash
tabdiff snapshot <input> --name <snapshot_name> [options]
```

**Options:**
- `--sample <strategy>`: Sampling strategy
  - `full`: Hash all rows (default)
  - `N%`: Random percentage (e.g., `10%`)
  - `N`: Exact count (e.g., `1000`)
- `--batch-size <size>`: Processing batch size (default: 10000)

**Examples:**
```bash
# Full snapshot
tabdiff snapshot data.csv --name v1

# Sample 10% of rows
tabdiff snapshot large_data.parquet --name v1 --sample 10%

# Sample exactly 1000 rows
tabdiff snapshot data.csv --name v1 --sample 1000
```

### `tabdiff diff`
Compare two snapshots.

```bash
tabdiff diff <snapshot1> <snapshot2> [options]
```

**Options:**
- `--mode <mode>`: Diff mode (`quick`, `detailed`, `auto`)
- `--output <file>`: Custom output file

**Examples:**
```bash
# Quick comparison using JSON summaries
tabdiff diff v1 v2 --mode quick

# Detailed comparison using full archives
tabdiff diff v1 v2 --mode detailed

# Auto mode (quick first, detailed if needed)
tabdiff diff v1 v2
```

### `tabdiff show`
Display snapshot information.

```bash
tabdiff show <snapshot> [options]
```

**Options:**
- `--detailed`: Show detailed information from archive
- `--format <format>`: Output format (`pretty`, `json`)

### `tabdiff status`
Check current data against a snapshot.

```bash
tabdiff status <input> [options]
```

**Options:**
- `--compare-to <snapshot>`: Specific snapshot (defaults to latest)
- `--sample <strategy>`: Sampling strategy
- `--quiet`: Machine-readable output
- `--json`: JSON output

### `tabdiff list`
List all available snapshots.

```bash
tabdiff list [--format <format>]
```

## 📁 Directory Structure

```
.tabdiff/
├── v1.tabdiff         # ❌ Git-ignored: Full snapshot archive
├── v2.tabdiff         # ❌ Git-ignored: Full snapshot archive  
├── v1.json            # ✅ Git-tracked: Lightweight summary
├── v2.json            # ✅ Git-tracked: Lightweight summary
├── diffs/
│   └── v1-v2.json     # ✅ Git-tracked: Diff results
└── config.json        # ✅ Git-tracked: Workspace config
```

## 🔧 Configuration

### .gitignore Integration

tabdiff automatically adds the following to your `.gitignore`:

```gitignore
# Ignore compressed snapshot archives
.tabdiff/*.tabdiff
```

### DVC Integration (Optional)

For tracking large snapshot archives with DVC:

```bash
dvc add .tabdiff/*.tabdiff
git add .tabdiff/*.tabdiff.dvc .gitignore
git commit -m "Track tabdiff archives with DVC"
```

## 🏗️ Architecture

### Core Components

- **Workspace Management**: `.tabdiff/` directory handling
- **Data Processing**: DuckDB-powered format-agnostic data loading
- **Hashing**: Blake3-based schema, column, and row hashing
- **Archiving**: Tar + Zstandard compression for snapshots
- **Diffing**: Multi-level comparison (schema → columns → rows)

### File Formats

**Snapshot JSON (`.tabdiff/name.json`)**:
```json
{
  "format_version": "1.0.0",
  "name": "v1",
  "created": "2025-07-01T12:00:00Z",
  "source": "data.csv",
  "row_count": 1000000,
  "column_count": 15,
  "schema_hash": "abc123...",
  "columns": {
    "price": "hash1...",
    "status": "hash2..."
  },
  "sampling": {
    "strategy": "full",
    "rows_hashed": 1000000
  }
}
```

**Archive Contents (`.tabdiff/name.tabdiff`)**:
```
name.tabdiff (tar.zst):
├── metadata.json      # Extended metadata
├── schema.json        # Schema + column hashes  
└── rows.json          # Row hashes
```

## 🧪 Examples

### Basic Workflow

```bash
# Initialize workspace
cd my-data-project/
tabdiff init

# Create baseline snapshot
tabdiff snapshot data.csv --name baseline

# Work with your data...
# Create updated snapshot
tabdiff snapshot data.csv --name $(date +%Y%m%d)

# Compare changes
tabdiff diff baseline 20250701

# Check current status
tabdiff status data.csv

# List all snapshots
tabdiff list
```

### CI/CD Integration

```bash
# In your CI pipeline
tabdiff status data.csv --json > status.json

# Check if data changed
if [ "$(jq -r '.rows_changed' status.json)" != "0" ]; then
  echo "Data has changed!"
  tabdiff snapshot data.csv --name "ci-$(date +%Y%m%d-%H%M%S)"
fi
```

### Large Dataset Handling

```bash
# For large datasets, use sampling
tabdiff snapshot large_data.parquet --name v1 --sample 1%

# Quick status check with sampling
tabdiff status large_data.parquet --sample 1000

# Full comparison when needed
tabdiff status large_data.parquet --sample full
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **DuckDB** for powerful data processing
- **Blake3** for fast hashing
- **Zstandard** for efficient compression
- **Rust ecosystem** for excellent tooling
