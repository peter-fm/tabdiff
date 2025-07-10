# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Building the Project
```bash
cargo build --release
```

### Building with Bundled DuckDB (zero dependencies)
```bash
cargo build --release --features bundled
```

### Running Tests
```bash
cargo test
```

### Running Specific Tests
```bash
cargo test <test_name>
```

### Running Tests with Output
```bash
cargo test -- --nocapture
```

### Development Build
```bash
cargo build
```

### Quick functionality tests
```bash
cargo run --release
```

### Running with Verbose Logging
```bash
RUST_LOG=debug ./target/release/tabdiff <command>
```

## Architecture Overview

tabdiff is a Rust-based snapshot-based structured data diff tool built around these core components:

## CRITICAL: Data Storage Architecture

**IMPORTANT**: The snapshot data storage follows this strict pattern:

### Archive File Structure:
- **`metadata.json`**: Snapshot metadata (name, timestamps, row counts, etc.)
- **`schema.json`**: Column definitions and schema information  
- **`data.parquet`**: **THE ONLY SOURCE OF ROW DATA** - contains actual full row data when `--full-data` is used
- **`delta.parquet`**: Delta changes from parent snapshot (optional)

### ❌ NEVER USE:
- **`rows.json`**: This file should NEVER exist. It causes confusion and bugs.

### ✅ ALWAYS USE:
- **`data.parquet`**: For all full row data storage and retrieval

**Rationale**: Using `rows.json` creates inconsistency and leads to bugs where:
1. Snapshot creation stores data in one location
2. Data loading looks in a different location  
3. Diff and rollback functionality fails because they can't find the actual row data

Engineers should always use `data.parquet` for row data to maintain consistency across the codebase.

## Default Behavior Changes

**IMPORTANT**: As of the latest version:

### ✅ New Defaults (Better UX):
- **`--full-data` is now the default** for snapshot creation
- Users get comprehensive change detection and rollback capabilities by default
- No need to remember to add `--full-data` flag for proper functionality

### 🔄 Performance Options:
- **`--hash-only`**: For large files (>1GB) - lightweight tracking, no rollback/detailed diff
- **Smart warnings**: Automatic file size detection with recommendations
  - Files >100MB: Info message about using `--hash-only` 
  - Files >1GB: Warning with strong recommendation for `--hash-only`

### 📊 File Size Thresholds:
- **< 100MB**: Full data mode (default) - no warnings
- **100MB - 1GB**: Info message about `--hash-only` option  
- **> 1GB**: Warning suggesting `--hash-only` for performance

This ensures users get the best experience by default while providing clear guidance for large file handling.

### Core Module Structure
- **`lib.rs`**: Public API and constants
- **`main.rs`**: CLI entry point 
- **`cli.rs`**: Command-line interface definitions using clap
- **`commands.rs`**: All command implementations (init, snapshot, status, rollback, etc.)
- **`workspace.rs`**: `.tabdiff/` directory management and workspace discovery
- **`snapshot.rs`**: Snapshot creation, chain management, and delta handling
- **`change_detection.rs`**: Comprehensive change analysis with before/after values
- **`data.rs`**: Data loading and processing using DuckDB
- **`hash.rs`**: Blake3-based hashing for schema, columns, and rows
- **`archive.rs`**: Tar+Zstandard compression for snapshot storage
- **`resolver.rs`**: Snapshot resolution and reference handling
- **`output.rs`**: Pretty printing and JSON formatting
- **`progress.rs`**: Progress reporting for long-running operations

### Key Dependencies
- **DuckDB**: Data processing engine for format-agnostic SQL queries
- **Blake3**: Fast cryptographic hashing for data fingerprinting
- **Zstandard + Tar**: Compression for snapshot archives
- **clap**: Command-line argument parsing
- **serde**: JSON serialization for metadata

### Data Flow
1. Input files → DuckDB → DataProcessor → structured data
2. HashComputer → Blake3 hashes for schema/columns/rows
3. SnapshotCreator → compressed archive + JSON metadata
4. ChangeDetector → detailed before/after analysis
5. RollbackOperations → precise file restoration

## Key Architecture Concepts

### Snapshot System
- **Dual Storage**: Git-tracked JSON metadata (`.json`) + Git-ignored compressed archives (`.tabdiff`)
- **Delta Chains**: Each snapshot stores changes from parent for space efficiency
- **Full Data vs Hash-Only**: `--full-data` enables comprehensive change detection and rollback
- **Smart Cleanup**: Remove `data.parquet` but preserve `delta.parquet` for reconstruction

### Change Detection
- **Schema Changes**: Column additions/removals/renames/type changes
- **Row Changes**: Cell-level modifications with before/after values
- **Rollback Operations**: Atomic operations to restore previous state
- **Content-based Matching**: Position + content heuristics for row pairing

### Workspace Management
- **Directory Structure**: `.tabdiff/` workspace similar to `.git/`
- **Source Tracking**: Canonical paths and source fingerprints
- **Chain Isolation**: Separate snapshot chains per source file
- **Git Integration**: Automatic `.gitignore` updates

## Testing Structure

The test suite is focused on validating core functionality with accurate assertions:
- **`unit/`**: Essential CLI parsing tests
- **`integration/`**: Core snapshot creation tests with validation
- **`functional/`**: 
  - `comprehensive_change_detection_tests.rs` - Cell-level accuracy validation
  - `table_changes_tests.rs` - Basic change scenarios with proper validation
  - `workflow_tests.rs` - End-to-end user scenarios with result verification
- **`common/`**: Test utilities with accuracy validation helpers

## Key Testing Principles

### Test Quality Over Quantity
- Tests validate **results accuracy**, not just command success
- Each test verifies **specific expected outcomes**
- Tests use **precise assertions** with before/after value validation
- Focus on **core functionality** that makes tabdiff unique

### Core Test Categories
1. **Cell-Level Change Detection**: Validates exact before/after values
2. **Rollback Accuracy**: Verifies restored files match original exactly
3. **End-to-End Workflows**: Tests complete user scenarios with validation
4. **Schema Change Detection**: Validates structural changes

### Test Validation Helpers
- `assert_cell_change_detected()` - Validates specific cell changes
- `assert_row_addition_detected()` - Validates added rows with exact data
- `assert_row_removal_detected()` - Validates removed rows with exact data
- `assert_rollback_operations_valid()` - Validates rollback operation correctness
- `assert_files_equal()` - Validates exact file content matching

## Development Notes

### File Format Support
Supports CSV, Parquet, JSON, TSV, and SQL files through DuckDB's SQL interface.

### SQL Database Support
tabdiff now supports tracking changes in SQL database query results through `.sql` files:

#### SQL File Format
SQL files should contain:
1. **Connection String Comment**: A comment line with DuckDB ATTACH statement
2. **SQL Query**: The actual query to execute

Example:
```sql
-- ATTACH 'host=localhost user={MYSQL_USER} password={MYSQL_PASSWORD} port=3306 database=mydatabase' AS mysql_db (TYPE mysql);
SELECT 
    product_id,
    product_name,
    quantity,
    last_updated
FROM products 
WHERE last_updated > '2025-01-01';
```

#### Environment Variables
- Create a `.env` file in your project root for database credentials
- Use `{VARIABLE_NAME}` syntax in connection strings for substitution
- See `.env.sample` for examples

#### Supported Databases
- **MySQL**: `TYPE mysql`
- **PostgreSQL**: `TYPE postgres`
- **SQLite**: `TYPE sqlite`
- Any database supported by DuckDB

#### Usage
```bash
# Create snapshot from SQL query
tabdiff snapshot query.sql --name baseline

# Track changes over time
tabdiff snapshot query.sql --name current
tabdiff diff baseline current

# Rollback is not supported for SQL queries (read-only)
```

### Memory Management
Uses chunked processing with configurable batch sizes (default: 10,000 rows) for large datasets.

### Error Handling
Uses `anyhow` for error propagation and `thiserror` for custom error types.

### Parallel Processing
Leverages `rayon` for parallel row hashing and change detection operations.

### Progress Reporting
Multi-phase progress bars using `indicatif` for schema analysis, hashing, and archiving.