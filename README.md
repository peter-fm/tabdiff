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
- **🆕 Comprehensive change detection** with before/after values
- **🆕 Rollback functionality** to restore files to previous states
- **🆕 Detailed change analysis** with cell-level precision
- **🆕 Enhanced snapshot caching** with delta chains for space efficiency
- **🆕 Smart cleanup system** to manage storage while preserving rollback capability

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

### 2. Create your first snapshot with full data

```bash
tabdiff snapshot data.csv --name baseline --full-data
```

This creates:
- `.tabdiff/baseline.tabdiff` (compressed archive with full data - Git ignored)
- `.tabdiff/baseline.json` (lightweight summary - Git tracked)

### 3. Make changes to your data and check status

```bash
# Edit your data file...
tabdiff status data.csv --compare-to baseline
```

### 4. See detailed changes with before/after values

```bash
tabdiff status data.csv --compare-to baseline --json
```

### 5. Rollback changes if needed

```bash
# Preview what would be rolled back
tabdiff rollback data.csv --to baseline --dry-run

# Actually rollback (creates backup automatically)
tabdiff rollback data.csv --to baseline
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
- `--full-data`: Store complete row data for comprehensive change detection

**Examples:**
```bash
# Full snapshot with comprehensive change detection
tabdiff snapshot data.csv --name v1 --full-data

# Hash-only snapshot (smaller, basic change detection)
tabdiff snapshot data.csv --name v1

# Sample 10% of rows with full data
tabdiff snapshot large_data.parquet --name v1 --sample 10% --full-data
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
Check current data against a snapshot with comprehensive change detection.

```bash
tabdiff status <input> [options]
```

**Options:**
- `--compare-to <snapshot>`: Specific snapshot (defaults to latest)
- `--sample <strategy>`: Sampling strategy
- `--quiet`: Machine-readable output
- `--json`: JSON output with detailed before/after values

**Example Output:**
```bash
# Pretty output
📊 tabdiff status
├─ ✅ Schema: unchanged
├─ ❌ Rows changed: 2
│  ├─ Modified rows: 2
│  │  ├─ Row 0: 1 columns changed
│  │     └─ rating: '4.5' → '4.7'
│  │  └─ Row 1: 2 columns changed
│        ├─ rating: '3.8' → '3.9'
│        └─ count: '75' → '80'
│  ├─ Added rows: 1
│  │  └─ Indices: 5
└─ Total rollback operations: 3
```

```json
// JSON output with detailed changes
{
  "schema_changes": {
    "column_order": null,
    "columns_added": [],
    "columns_removed": [],
    "type_changes": []
  },
  "row_changes": {
    "modified": [
      {
        "row_index": 0,
        "changes": {
          "rating": {
            "before": "4.5",
            "after": "4.7"
          }
        }
      }
    ],
    "added": [
      {
        "row_index": 5,
        "data": {
          "product_id": "6",
          "rating": "5.0",
          "count": "25",
          "category": "gadgets"
        }
      }
    ],
    "removed": []
  },
  "rollback_operations": [
    {
      "operation_type": "RemoveRow",
      "parameters": {
        "row_index": 5
      }
    },
    {
      "operation_type": "UpdateCell",
      "parameters": {
        "row_index": 0,
        "column": "rating",
        "value": "4.5"
      }
    }
  ]
}
```

### `tabdiff rollback` 🆕
Rollback a file to a previous snapshot state.

```bash
tabdiff rollback <input> --to <snapshot_name> [options]
```

**Options:**
- `--dry-run`: Show what would be changed without applying
- `--force`: Skip confirmation prompts
- `--backup`: Create backup before rollback (default: true)

**Examples:**
```bash
# Preview rollback changes
tabdiff rollback data.csv --to baseline --dry-run

# Interactive rollback with confirmation
tabdiff rollback data.csv --to baseline

# Automated rollback (no prompts)
tabdiff rollback data.csv --to baseline --force

# Rollback without creating backup
tabdiff rollback data.csv --to baseline --no-backup
```

**Rollback Process:**
1. **Analysis**: Compares current file with target snapshot
2. **Preview**: Shows exactly what will change (if not using `--force`)
3. **Backup**: Creates `.backup` file automatically
4. **Execution**: Applies changes to restore file to snapshot state
5. **Verification**: File now matches the target snapshot

### `tabdiff list`
List all available snapshots.

```bash
tabdiff list [--format <format>]
```

### `tabdiff chain` 🆕
Show snapshot chain and relationships.

```bash
tabdiff chain [--format <format>]
```

**Example Output:**
```bash
🔗 Snapshot Chain
Chain structure:
🌱 baseline (seq: 0)
   └─ Archive size: 1114 bytes

├─ v2 (seq: 1)
   └─ Parent: baseline
   └─ Can reconstruct parent: ✅
   └─ Delta size: 536 bytes
   └─ Archive size: 1505 bytes

├─ v3 (seq: 2)
   └─ Parent: v2
   └─ Can reconstruct parent: ✅
   └─ Delta size: 698 bytes
   └─ Archive size: 1604 bytes

Head: v3
```

### `tabdiff cleanup` 🆕
Smart cleanup system to manage storage while preserving rollback capability.

```bash
tabdiff cleanup [options]
```

**Options:**
- `--keep-full <N>`: Number of snapshots to keep full data for (default: 1)
- `--dry-run`: Show what would be cleaned without applying
- `--force`: Skip confirmation prompts

**How It Works:**
- **Keeps full data** for the most recent N snapshots (fast rollback)
- **Removes `data.parquet`** from older snapshots (space savings)
- **Preserves deltas** for reconstruction (maintains rollback capability)
- **Never breaks** the ability to rollback to any snapshot

**Examples:**
```bash
# Default: Keep full data for 1 snapshot (aggressive space savings)
tabdiff cleanup --dry-run
# Output: Would clean baseline and v2, keep v3 with full data

# Conservative: Keep full data for 2 snapshots
tabdiff cleanup --keep-full 2 --dry-run
# Output: Would clean baseline only, keep v2 and v3 with full data

# Apply cleanup
tabdiff cleanup --force
```

**Space Savings Example:**
```bash
📊 Cleanup analysis:
   • Snapshots for data cleanup: 2
   • Estimated space savings: 1832 bytes (≈70% reduction)
   • Archives will retain deltas for reconstruction

🔍 Snapshots that would have data cleaned up:
   • v2 (seq: 1, estimated savings: 1053 bytes)
   • baseline (seq: 0, estimated savings: 779 bytes)
```

## 🔍 Change Detection Features

### Schema Changes
- **Column additions/removals**
- **Column reordering** 
- **Data type changes**
- **Column renames** (detected via content analysis)

### Row Changes
- **Modified cells** with before/after values
- **Added rows** with complete data
- **Removed rows** with original data
- **Content-based comparison** (immune to row reordering)

### Rollback Operations
- **Cell updates** to restore original values
- **Row additions** to restore deleted data
- **Row removals** to eliminate added data
- **Schema restoration** for structural changes

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
- **Change Detection**: Comprehensive before/after analysis
- **Rollback Engine**: Safe file restoration with backups

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
  },
  "has_full_data": true
}
```

**Enhanced Archive Contents (`.tabdiff/name.tabdiff`)**:
```
name.tabdiff (tar.zst):
├── metadata.json      # Extended metadata with chain info
├── schema.json        # Schema + column hashes  
├── rows.json          # Row hashes only
├── data.parquet       # Full dataset (removable during cleanup)
└── delta.parquet      # Changes from parent (always preserved)
```

### Enhanced Snapshot Caching System 🆕

**Delta Chain Architecture:**
- Each snapshot stores both **full data** and **changes from parent**
- Cleanup removes `data.parquet` but preserves `delta.parquet`
- Any snapshot can be reconstructed by walking the delta chain
- Provides optimal balance between speed and storage efficiency

**Reconstruction Process:**
1. **Fast Path**: Use `data.parquet` if available (recent snapshots)
2. **Delta Path**: Reconstruct from chain if `data.parquet` was cleaned up
   - Start from nearest snapshot with full data
   - Apply delta operations in sequence
   - Rebuild target snapshot state

**Space vs Speed Trade-offs:**
- `--keep-full 1`: Maximum space savings, delta reconstruction for older snapshots
- `--keep-full 3`: Balanced approach, fast access to recent snapshots
- `--keep-full 10`: Conservative, prioritizes speed over storage

## 🧪 Examples

### Basic Workflow with Change Detection

```bash
# Initialize workspace
cd my-data-project/
tabdiff init

# Create baseline snapshot with full data
tabdiff snapshot data.csv --name baseline --full-data

# Work with your data...
# Check what changed
tabdiff status data.csv --compare-to baseline

# See detailed changes in JSON
tabdiff status data.csv --compare-to baseline --json

# Rollback if needed
tabdiff rollback data.csv --to baseline --dry-run
tabdiff rollback data.csv --to baseline
```

### Data Quality Workflow

```bash
# Create snapshot before data processing
tabdiff snapshot raw_data.csv --name before_cleaning --full-data

# Process your data...
python clean_data.py

# Check what the cleaning changed
tabdiff status clean_data.csv --compare-to before_cleaning --json > changes.json

# Verify changes are expected
cat changes.json | jq '.row_changes.modified | length'

# Create snapshot of cleaned data
tabdiff snapshot clean_data.csv --name after_cleaning --full-data
```

### CI/CD Integration with Rollback

```bash
# In your CI pipeline
tabdiff status data.csv --json > status.json

# Check if data changed unexpectedly
if [ "$(jq -r '.row_changes.modified | length' status.json)" -gt "0" ]; then
  echo "Unexpected data changes detected!"
  
  # Show what changed
  tabdiff status data.csv
  
  # Optionally rollback
  if [ "$AUTO_ROLLBACK" = "true" ]; then
    tabdiff rollback data.csv --to baseline --force
  fi
  
  exit 1
fi
```

### Large Dataset Handling

```bash
# For large datasets, use sampling for quick checks
tabdiff snapshot large_data.parquet --name v1 --sample 1% --full-data

# Quick status check with sampling
tabdiff status large_data.parquet --sample 1000

# Full comparison when needed
tabdiff status large_data.parquet --sample full
```

### Enhanced Snapshot Caching Workflow 🆕

```bash
# Create a series of snapshots with delta chains
tabdiff snapshot employees.csv --name baseline --full-data
# Edit data: Alice gets raise, add Bob
tabdiff snapshot employees.csv --name v2 --full-data  
# Edit data: Bob gets raise, add Carol
tabdiff snapshot employees.csv --name v3 --full-data

# View the snapshot chain
tabdiff chain
# Output: Shows baseline → v2 → v3 with deltas

# Check space usage before cleanup
ls -la .tabdiff/*.tabdiff
# Output: 3 archives, ~4KB total

# Aggressive cleanup (keep full data for 1 snapshot only)
tabdiff cleanup --dry-run
# Output: Would clean baseline and v2, save ~70% space

# Apply cleanup
tabdiff cleanup --force
# Output: Cleaned 2 snapshots, saved 1832 bytes

# Verify rollback still works after cleanup
tabdiff rollback employees.csv --to baseline --dry-run
# Output: Shows exact changes needed (using delta reconstruction)

# Rollback works perfectly even after cleanup!
tabdiff rollback employees.csv --to baseline --force
```

### Rollback Safety Examples

```bash
# Always preview first
tabdiff rollback data.csv --to baseline --dry-run

# Interactive rollback with confirmation
tabdiff rollback data.csv --to baseline

# Automated rollback for scripts
tabdiff rollback data.csv --to baseline --force

# Check backup was created
ls -la data.csv.backup

# Restore from backup if needed
cp data.csv.backup data.csv
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
