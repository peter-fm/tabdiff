# Tabdiff Test Suite

This directory contains a comprehensive test suite for the tabdiff CLI tool, designed to ensure all functionality works correctly and to catch edge cases that might break the application.

## Test Organization

The test suite is organized into several categories:

### 📁 Directory Structure

```
tests/
├── README.md              # This file
├── lib.rs                 # Test library organization
├── test_runner.rs         # Custom test runner with reporting
├── common/                # Shared test utilities
│   └── mod.rs            # Test fixtures, helpers, and sample data
├── unit/                  # Unit tests for individual modules
│   ├── cli_tests.rs      # CLI argument parsing and validation
│   └── workspace_tests.rs # Workspace management functionality
├── integration/           # End-to-end command testing
│   ├── init_tests.rs     # Init command integration tests
│   └── snapshot_tests.rs # Snapshot command integration tests
├── edge_cases/           # Edge cases and error conditions
│   ├── filesystem_tests.rs # File system edge cases
│   └── data_edge_cases.rs  # Data format edge cases
├── functional/           # Real-world workflow testing
│   └── workflow_tests.rs # Complete user workflows
└── fixtures/             # Test data files
    ├── csv/
    ├── parquet/
    ├── json/
    └── corrupted/
```

## Test Categories

### 🧪 Unit Tests (`tests/unit/`)

Test individual modules and functions in isolation:

- **CLI Tests**: Argument parsing, validation, help messages, error handling
- **Workspace Tests**: Directory creation, configuration, snapshot management
- **Error Tests**: Error type conversions, error message formatting

**Run with**: `cargo test unit::`

### 🔗 Integration Tests (`tests/integration/`)

Test complete command workflows end-to-end:

- **Init Command**: Workspace initialization, force flag, .gitignore handling
- **Snapshot Command**: File processing, sampling strategies, metadata generation
- **Diff Command**: Snapshot comparison, output formats, diff modes
- **Show Command**: Snapshot display, detailed views, format options
- **Status Command**: Current data comparison, sampling, output modes
- **List Command**: Snapshot listing, format options

**Run with**: `cargo test integration::`

### ⚠️ Edge Case Tests (`tests/edge_cases/`)

Test potential failure scenarios and boundary conditions:

#### Filesystem Edge Cases
- Non-existent files and directories
- Permission denied scenarios
- Very long filenames and paths
- Special characters and Unicode in filenames
- Symlinks and broken symlinks
- Concurrent file access
- Disk space issues
- Corrupted workspace files

#### Data Edge Cases
- Malformed CSV files (quotes, delimiters, encoding)
- Empty files and files with only headers
- Very large files and datasets
- Unicode data and BOM handling
- Different line endings (Unix, Windows, Mac)
- JSON parsing errors and edge cases
- Binary files with text extensions
- Numeric precision and special float values

**Run with**: `cargo test edge_cases::`

### 🚀 Functional Tests (`tests/functional/`)

Test real-world usage scenarios and complete workflows:

- **Basic Workflow**: Init → Snapshot → Diff → Status cycle
- **Schema Evolution**: Detecting and handling schema changes
- **Sampling Strategies**: Different sampling approaches and comparisons
- **Multiple Formats**: CSV, JSON, Parquet file handling
- **Versioning**: Multiple snapshot management
- **CI/CD Integration**: Automated testing workflows
- **Data Quality Monitoring**: Daily snapshot comparisons
- **Large Dataset Handling**: Performance with big files
- **Error Recovery**: Graceful failure and recovery scenarios

**Run with**: `cargo test functional::`

## Running Tests

### Quick Start

```bash
# Run all tests
cargo test

# Run specific test category
cargo test unit::
cargo test integration::
cargo test edge_cases::
cargo test functional::

# Run with verbose output
cargo test -- --nocapture

# Run tests sequentially (useful for debugging)
cargo test -- --test-threads=1
```

### Using the Custom Test Runner

The test suite includes a custom test runner with enhanced reporting:

```bash
# Run all test categories with summary
cargo run --bin test_runner

# Run specific category
cargo run --bin test_runner --unit
cargo run --bin test_runner --integration
cargo run --bin test_runner --edge-cases
cargo run --bin test_runner --functional

# With verbose output
cargo run --bin test_runner --verbose

# Sequential execution
cargo run --bin test_runner --no-parallel

# Don't capture output (for debugging)
cargo run --bin test_runner --no-capture

# Get help
cargo run --bin test_runner --help
```

### Test Output Example

```
🚀 Running comprehensive tabdiff test suite...

🧪 Running Unit tests...
✅ Unit tests completed successfully in 2.3s
   Passed: 45, Failed: 0, Ignored: 0

🧪 Running Integration tests...
✅ Integration tests completed successfully in 8.7s
   Passed: 32, Failed: 0, Ignored: 0

🧪 Running EdgeCases tests...
✅ EdgeCases tests completed successfully in 12.1s
   Passed: 67, Failed: 0, Ignored: 3

🧪 Running Functional tests...
✅ Functional tests completed successfully in 15.4s
   Passed: 28, Failed: 0, Ignored: 0

📊 Test Suite Summary
═══════════════════════════════════════
✅ PASS Unit: 45 passed, 0 failed, 0 ignored (2.3s)
✅ PASS Integration: 32 passed, 0 failed, 0 ignored (8.7s)
✅ PASS EdgeCases: 67 passed, 0 failed, 3 ignored (12.1s)
✅ PASS Functional: 28 passed, 0 failed, 0 ignored (15.4s)
═══════════════════════════════════════
Total: 172 passed, 0 failed, 3 ignored
Categories: 4/4 passed
Duration: 38.5s

🎉 All tests passed! The tabdiff implementation is working correctly.
```

## Test Utilities

### Common Test Helpers (`tests/common/`)

The test suite provides several utilities to make writing tests easier:

#### TestFixture
Creates temporary test environments with initialized workspaces:

```rust
use crate::common::TestFixture;

let fixture = TestFixture::new()?; // With initialized workspace
let fixture = TestFixture::new_empty()?; // Without workspace

// Create test data
let csv_path = fixture.create_csv("test.csv", &sample_data::simple_csv_data())?;
let json_path = fixture.create_json("test.json", &sample_data::simple_json_data())?;

// Assertions
fixture.assert_snapshot_exists("test_snapshot");
fixture.assert_snapshot_not_exists("missing_snapshot");
```

#### CliTestRunner
Runs CLI commands in test environments:

```rust
use crate::common::CliTestRunner;

let runner = CliTestRunner::new()?;

// Expect success
runner.expect_success(&["init"]);
runner.expect_success(&["snapshot", "data.csv", "--name", "test"]);

// Expect failure
let error = runner.expect_failure(&["snapshot", "nonexistent.csv", "--name", "test"]);
assert!(error.to_string().contains("not found"));
```

#### Sample Data Generators
Pre-defined test data for consistent testing:

```rust
use crate::common::sample_data;

// CSV data
let simple_data = sample_data::simple_csv_data();
let updated_data = sample_data::updated_csv_data();
let schema_changed_data = sample_data::schema_changed_csv_data();

// JSON data
let simple_json = sample_data::simple_json_data();
let nested_json = sample_data::nested_json_data();
```

#### Assertion Helpers
Common assertions for file and data validation:

```rust
use crate::common::assertions;

assertions::assert_file_exists_and_not_empty(&path);
assertions::assert_dir_exists(&directory);
assertions::assert_json_contains_keys(&json_file, &["key1", "key2"])?;
assertions::assert_files_equal(&file1, &file2)?;
```

## Writing New Tests

### Guidelines

1. **Use descriptive test names** that clearly indicate what is being tested
2. **Test one thing at a time** - each test should have a single, clear purpose
3. **Use the common utilities** to reduce boilerplate and ensure consistency
4. **Include both positive and negative test cases**
5. **Test edge cases and boundary conditions**
6. **Clean up after tests** (handled automatically by TestFixture)

### Example Test

```rust
#[test]
fn test_snapshot_with_unicode_data() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create test data with Unicode characters
    let unicode_csv = runner.fixture().create_unicode_csv("unicode.csv").unwrap();
    
    // Should handle Unicode data correctly
    runner.expect_success(&[
        "snapshot", 
        unicode_csv.to_str().unwrap(), 
        "--name", "unicode_test"
    ]);
    
    // Verify snapshot was created
    runner.fixture().assert_snapshot_exists("unicode_test");
    
    // Verify metadata contains expected information
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("unicode_test");
    assertions::assert_json_contains_keys(&json_path, &[
        "name", "row_count", "column_count", "schema_hash"
    ]).unwrap();
}
```

## Test Data

### Fixtures Directory

The `tests/fixtures/` directory contains pre-created test files for scenarios where generating data programmatically is not practical:

- **CSV files**: Various formats, encodings, and edge cases
- **Parquet files**: Different schemas and compression settings
- **JSON files**: Nested structures, arrays, mixed types
- **Corrupted files**: Invalid formats for error testing

### Dynamic Test Data

Most tests use dynamically generated data through the sample data generators, which ensures:

- **Consistency** across test runs
- **Flexibility** to modify data for specific test scenarios
- **Maintainability** - changes to test data are centralized

## Performance Considerations

### Test Execution Time

- **Unit tests**: Fast (< 5 seconds total)
- **Integration tests**: Medium (5-15 seconds)
- **Edge case tests**: Medium-slow (10-30 seconds)
- **Functional tests**: Slow (15-60 seconds)

### Optimization Strategies

1. **Use sampling** for large dataset tests to reduce execution time
2. **Run tests in parallel** when possible (default behavior)
3. **Use smaller test datasets** while still covering edge cases
4. **Mock external dependencies** where appropriate

## Continuous Integration

### CI Pipeline Integration

The test suite is designed to work well in CI environments:

```yaml
# Example GitHub Actions workflow
- name: Run comprehensive tests
  run: cargo run --bin test_runner --verbose

# Or run specific categories
- name: Run unit tests
  run: cargo test unit::
  
- name: Run integration tests  
  run: cargo test integration::
```

### Test Reporting

The custom test runner provides structured output that can be parsed by CI systems for:

- **Test result reporting**
- **Performance tracking**
- **Failure analysis**
- **Coverage reporting**

## Debugging Tests

### Common Issues

1. **Test isolation**: Ensure tests don't interfere with each other
2. **Temporary file cleanup**: Use TestFixture to handle cleanup automatically
3. **Platform differences**: Some tests may behave differently on different OS
4. **Timing issues**: Use appropriate timeouts for async operations

### Debugging Techniques

```bash
# Run single test with output
cargo test test_name -- --nocapture

# Run tests sequentially to avoid race conditions
cargo test -- --test-threads=1

# Use the test runner with no capture for debugging
cargo run --bin test_runner --no-capture --verbose
```

## Contributing

When adding new functionality to tabdiff:

1. **Write tests first** (TDD approach recommended)
2. **Add tests to appropriate category** (unit, integration, edge cases, functional)
3. **Update this README** if adding new test patterns or utilities
4. **Ensure all tests pass** before submitting changes
5. **Consider performance impact** of new tests

### Test Coverage Goals

- **Unit tests**: 100% coverage of public APIs
- **Integration tests**: All CLI commands and major workflows
- **Edge cases**: All identified failure modes and boundary conditions
- **Functional tests**: All documented user workflows

This comprehensive test suite ensures that tabdiff works reliably across different environments, data formats, and usage patterns, giving users confidence in the tool's correctness and robustness.
