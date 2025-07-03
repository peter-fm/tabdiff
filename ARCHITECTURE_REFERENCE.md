# 🤖 tabdiff Architecture Reference

> **Purpose**: This document serves as a comprehensive reference for understanding how the tabdiff application works, its architecture, and key concepts. Use this when you need to quickly remember how the system operates.

## 📋 Table of Contents

1. [Application Overview](#application-overview)
2. [Core Architecture](#core-architecture)
3. [Command Reference](#command-reference)
4. [Data Processing Pipeline](#data-processing-pipeline)
5. [Snapshot System](#snapshot-system)
6. [Change Detection Engine](#change-detection-engine)
7. [Workspace Management](#workspace-management)
8. [Key Data Structures](#key-data-structures)
9. [Advanced Features](#advanced-features)
10. [File Formats & Storage](#file-formats--storage)

---

## 🎯 Application Overview

**tabdiff** is a snapshot-based structured data diff tool that detects schema, column-level, and row-level changes between versions of structured datasets.

### Core Value Proposition
- **Git-friendly workflow** with lightweight JSON summaries (tracked) + compressed archives (ignored)
- **Comprehensive change detection** with before/after values for precise rollbacks
- **Memory-efficient processing** using DuckDB for large datasets
- **Delta chains** for space-efficient snapshot storage with reconstruction capability

### Key Use Cases
1. **Data Quality Monitoring**: Track changes in datasets over time
2. **Rollback Capability**: Restore files to previous states with cell-level precision
3. **CI/CD Integration**: Detect unexpected data changes in pipelines
4. **Data Lineage**: Understand how datasets evolve through processing steps

---

## 🏗️ Core Architecture

### Module Structure
```
src/
├── lib.rs              # Public API and constants
├── main.rs             # CLI entry point
├── cli.rs              # Command-line interface definitions
├── commands.rs         # Command implementations
├── workspace.rs        # .tabdiff directory management
├── snapshot.rs         # Snapshot creation and chain management
├── change_detection.rs # Comprehensive change analysis
├── data.rs             # Data loading and processing
├── hash.rs             # Blake3-based hashing system
├── archive.rs          # Tar+Zstandard compression
├── resolver.rs         # Snapshot resolution and references
├── output.rs           # Pretty printing and JSON formatting
├── progress.rs         # Progress reporting for long operations
├── git.rs              # Git integration utilities
├── error.rs            # Error handling and types
└── duckdb_config.rs    # DuckDB configuration and setup
```

### Data Flow
```
Input File → DataProcessor → HashComputer → SnapshotCreator → Archive + JSON
                ↓
         ChangeDetector ← Previous Snapshot
                ↓
         RollbackOperations → Commands (status/rollback)
```

### Key Design Patterns
- **Builder Pattern**: SnapshotCreator, DataProcessor configuration
- **Strategy Pattern**: Different diff modes (quick/detailed/auto)
- **Chain of Responsibility**: Snapshot parent-child relationships
- **Observer Pattern**: Progress reporting throughout operations

---

## 📝 Command Reference

### Core Commands

#### `tabdiff init [--force]`
- **Purpose**: Initialize .tabdiff workspace
- **Creates**: `.tabdiff/` directory, `config.json`, updates `.gitignore`
- **Implementation**: `init_command()` in `commands.rs`

#### `tabdiff snapshot <input> --name <name> [--full-data] [--batch-size N]`
- **Purpose**: Create snapshot with optional delta chain
- **Process**: Load data → Hash → Create archive → Update chain
- **Implementation**: `snapshot_command()` → `SnapshotCreator::create_snapshot_with_workspace()`

#### `tabdiff status <input> [--compare-to <snapshot>] [--json]`
- **Purpose**: Compare current data against snapshot with detailed changes
- **Output**: Schema changes + row changes with before/after values
- **Implementation**: `status_command()` → `ChangeDetector::detect_changes()`

#### `tabdiff rollback <input> --to <snapshot> [--dry-run] [--force]`
- **Purpose**: Restore file to previous snapshot state
- **Safety**: Automatic backups, dry-run preview, confirmation prompts
- **Implementation**: `rollback_command()` → Change detection → File rewrite

#### `tabdiff diff <snap1> <snap2> [--mode auto|quick|detailed]`
- **Purpose**: Compare two snapshots
- **Modes**: Quick (JSON only), Detailed (full archive), Auto (adaptive)
- **Implementation**: `diff_command()` → Metadata comparison

#### `tabdiff cleanup [--keep-full N] [--dry-run]`
- **Purpose**: Smart space management preserving rollback capability
- **Strategy**: Remove `data.parquet` but keep `delta.parquet` for reconstruction
- **Implementation**: `cleanup_command()` → `SnapshotChain::find_data_cleanup_candidates()`

### Advanced Commands

#### `tabdiff chain [--format pretty|json]`
- **Purpose**: Visualize snapshot relationships and delta chains
- **Shows**: Parent-child links, sequence numbers, reconstruction capability

#### `tabdiff list [--format pretty|json]`
- **Purpose**: List all snapshots with metadata

#### `tabdiff show <snapshot> [--detailed] [--format pretty|json]`
- **Purpose**: Display snapshot information and optionally archive contents

---

## 🔄 Data Processing Pipeline

### 1. Data Loading (`data.rs`)
```rust
DataProcessor::new() → load_file() → DataInfo
```
- **Supported Formats**: CSV, Parquet, JSON, TSV
- **DuckDB Integration**: Format-agnostic SQL queries
- **Memory Management**: Chunked processing for large files

### 2. Schema Analysis
```rust
DataInfo {
    source: PathBuf,
    columns: Vec<ColumnInfo>,  // name, data_type, nullable
    row_count: u64,
}
```

### 3. Hash Computation (`hash.rs`)
```rust
HashComputer::new(batch_size) → {
    hash_schema() → SchemaHash,
    hash_columns() → Vec<ColumnHash>,
    hash_rows() → Vec<RowHash>,
}
```
- **Algorithm**: Blake3 for speed and security
- **Deterministic**: Consistent ordering for reliable comparison
- **Parallel**: Batch processing for performance

### 4. Change Detection (`change_detection.rs`)
```rust
ChangeDetector::detect_changes(baseline, current) → ChangeDetectionResult {
    schema_changes: SchemaChanges,
    row_changes: RowChanges,
    rollback_operations: Vec<RollbackOperation>,
}
```

---

## 📸 Snapshot System

### Snapshot Creation Flow
1. **Load Data**: Parse input file using DuckDB
2. **Compute Hashes**: Schema, columns, and rows
3. **Find Parent**: Locate previous snapshot for same source file
4. **Compute Delta**: If parent exists, calculate changes
5. **Create Archive**: Store full data + delta + metadata
6. **Update Chain**: Link to parent and update sequence numbers

### Archive Structure (`.tabdiff/name.tabdiff`)
```
name.tabdiff (tar.zst compressed):
├── metadata.json      # Extended metadata with chain info
├── schema.json        # Schema + column hashes  
├── rows.json          # Row hashes only
├── data.parquet       # Full dataset (removable during cleanup)
└── delta.parquet      # Changes from parent (always preserved)
```

### JSON Metadata (`.tabdiff/name.json`)
```json
{
  "format_version": "1.0.0",
  "name": "snapshot_name",
  "created": "2025-07-01T12:00:00Z",
  "source": "data.csv",
  "source_path": "/full/path/to/data.csv",
  "row_count": 1000000,
  "column_count": 15,
  "schema_hash": "blake3_hash",
  "columns": {"col1": "hash1", "col2": "hash2"},
  "has_full_data": true,
  "parent_snapshot": "previous_snapshot",
  "sequence_number": 1,
  "delta_from_parent": {
    "parent_name": "previous_snapshot",
    "changes": {...},
    "compressed_size": 1024
  },
  "can_reconstruct_parent": true
}
```

### Delta Chain Architecture
- **Forward Deltas**: Each snapshot stores changes FROM parent
- **Reconstruction**: Walk chain backwards to rebuild any snapshot
- **Space Efficiency**: Remove `data.parquet` but keep `delta.parquet`
- **Source Isolation**: Separate chains per source file

---

## 🔍 Change Detection Engine

### Schema Changes
```rust
SchemaChanges {
    column_order: Option<ColumnOrderChange>,
    columns_added: Vec<ColumnAddition>,
    columns_removed: Vec<ColumnRemoval>,
    columns_renamed: Vec<ColumnRename>,
    type_changes: Vec<TypeChange>,
}
```

### Row Changes with Intelligence
```rust
RowChanges {
    modified: Vec<RowModification>,  // Cell-level before/after
    added: Vec<RowAddition>,         // Complete new row data
    removed: Vec<RowRemoval>,        // Complete original row data
}
```

### Change Detection Strategy
1. **Fast Hash Filtering**: Identify changed rows using Blake3 hashes
2. **Intelligent Classification**: Distinguish modifications from add/remove
3. **Content Matching**: Use position + content heuristics for row pairing
4. **Parallel Analysis**: Cell-level comparison for modifications only

### Rollback Operations
```rust
RollbackOperation {
    operation_type: UpdateCell | RestoreRow | RemoveRow | ...,
    parameters: HashMap<String, Value>,
}
```
- **Reverse Order**: Undo changes in reverse chronological order
- **Atomic Operations**: Each operation is self-contained
- **Safety Checks**: Validate before applying changes

---

## 🗂️ Workspace Management

### Directory Structure
```
project_root/
├── .tabdiff/
│   ├── config.json           # Workspace configuration
│   ├── snapshot1.json        # ✅ Git tracked
│   ├── snapshot1.tabdiff     # ❌ Git ignored
│   ├── snapshot2.json        # ✅ Git tracked
│   ├── snapshot2.tabdiff     # ❌ Git ignored
│   └── diffs/
│       └── snap1-snap2.json  # ✅ Git tracked
├── .gitignore                # Auto-updated
└── data.csv                  # Your data files
```

### Workspace Discovery
- **Search Strategy**: Walk up directory tree looking for `.tabdiff/`
- **Git Integration**: Also check for `.git/` as project root hint
- **Fallback**: Create in current directory if not found

### Source File Tracking
- **Canonical Paths**: Resolve symlinks and relative paths
- **Source Fingerprints**: Combine path + content hash for identity
- **Chain Isolation**: Separate snapshot chains per source file

---

## 📊 Key Data Structures

### SnapshotMetadata
- **Core Identity**: name, created timestamp, source file
- **Content Hashes**: schema_hash, column hashes, row count
- **Chain Links**: parent_snapshot, sequence_number, delta_from_parent
- **Capabilities**: has_full_data, can_reconstruct_parent

### ChangeDetectionResult
- **Schema Level**: Column additions/removals/renames/type changes
- **Row Level**: Modifications with before/after, additions, removals
- **Rollback**: Ordered operations to undo all changes

### SnapshotChain
- **Relationships**: Parent-child links between snapshots
- **Validation**: Check chain integrity and sequence consistency
- **Cleanup Logic**: Identify safe deletion candidates
- **Reconstruction**: Find paths between snapshots

### DataInfo
- **Source**: Original file path and metadata
- **Schema**: Column definitions with types and nullability
- **Statistics**: Row count, column count, data characteristics

---

## 🚀 Advanced Features

### Smart Cleanup System
- **Strategy**: Keep full data for recent N snapshots, deltas for older ones
- **Safety**: Never break rollback capability
- **Efficiency**: 60-80% space savings while preserving functionality
- **Validation**: Check reconstruction paths before cleanup

### Rollback Safety
- **Dry Run**: Preview all changes before applying
- **Automatic Backups**: Create `.backup` files before modification
- **Confirmation**: Interactive prompts unless `--force` used
- **Verification**: Ensure file matches target snapshot after rollback

### Progress Reporting
- **Multi-Phase**: Schema analysis, row hashing, column hashing, archiving
- **Adaptive**: Different progress bars for different operation types
- **Informative**: Show current operation and estimated completion

### Parallel Processing
- **Row Hashing**: Batch processing with configurable batch sizes
- **Change Detection**: Parallel row comparison and classification
- **Content Matching**: Parallel similarity scoring for row pairing

---

## 💾 File Formats & Storage

### Archive Compression
- **Format**: Tar + Zstandard (`.tabdiff` extension)
- **Rationale**: Good compression ratio, streaming support, wide compatibility
- **Contents**: JSON files + Parquet data (placeholder format currently)

### Data Serialization
- **Current**: JSON placeholder for Parquet files
- **Future**: Native Parquet for better performance and smaller size
- **Schema**: Consistent column ordering for reliable comparison

### Git Integration
- **Tracked**: JSON metadata files, diff results, configuration
- **Ignored**: Compressed archives (large binary files)
- **Auto-Setup**: Automatically updates `.gitignore` on init

---

## 🔧 Configuration & Constants

### Default Values
```rust
FORMAT_VERSION: "1.0.0"
DEFAULT_BATCH_SIZE: 10000
DEFAULT_SAMPLE_SIZE: 1000
```

### DuckDB Configuration
- **Memory Management**: Optimized for large dataset processing
- **Extensions**: Support for various file formats
- **SQL Generation**: Format-agnostic queries for data extraction

---

## 🧪 Testing Strategy

### Test Organization
```
tests/
├── unit/           # Individual component tests
├── integration/    # Cross-component workflow tests
├── functional/     # End-to-end user scenarios
├── edge_cases/     # Error conditions and boundary cases
├── performance/    # Large dataset and timing tests
└── common/         # Shared test utilities
```

### Key Test Scenarios
- **Snapshot Creation**: Various file formats and sizes
- **Change Detection**: All types of schema and row changes
- **Rollback Operations**: Safety and correctness verification
- **Chain Management**: Parent-child relationships and reconstruction
- **Cleanup Operations**: Space savings without breaking functionality

---

## 🚨 Common Pitfalls & Solutions

### Data Consistency
- **Problem**: Row ordering differences causing false changes
- **Solution**: Deterministic SQL ordering in data extraction

### Memory Usage
- **Problem**: Large files causing OOM errors
- **Solution**: Chunked processing with configurable batch sizes

### Chain Integrity
- **Problem**: Broken parent-child relationships
- **Solution**: Validation checks and repair mechanisms

### Source File Tracking
- **Problem**: Same filename in different locations creating conflicts
- **Solution**: Canonical path resolution and source fingerprinting

---

## 📚 Quick Reference Commands

```bash
# Initialize workspace
tabdiff init

# Create snapshot with full data
tabdiff snapshot data.csv --name v1 --full-data

# Check current status
tabdiff status data.csv --compare-to v1 --json

# Rollback with safety
tabdiff rollback data.csv --to v1 --dry-run

# View snapshot chain
tabdiff chain

# Clean up old data
tabdiff cleanup --keep-full 2 --dry-run
```

---

*This reference document captures the essential architecture and functionality of tabdiff. Use it to quickly understand how the system works and make informed decisions when modifying or extending the codebase.*
