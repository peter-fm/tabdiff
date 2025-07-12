//! Common test utilities and helpers

use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tabdiff::{TabdiffWorkspace, Result};

/// Test fixture manager for creating temporary test environments
pub struct TestFixture {
    pub temp_dir: TempDir,
    pub workspace: TabdiffWorkspace,
}

impl TestFixture {
    /// Create a new test fixture with initialized workspace
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf())?;
        
        Ok(Self {
            temp_dir,
            workspace,
        })
    }

    /// Create a new test fixture without initializing workspace
    pub fn new_empty() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        // Create workspace structure manually without calling create_new
        let root = temp_dir.path().to_path_buf();
        let tabdiff_dir = root.join(".tabdiff");
        let diffs_dir = tabdiff_dir.join("diffs");
        
        let workspace = TabdiffWorkspace {
            root,
            tabdiff_dir,
            diffs_dir,
        };
        
        Ok(Self {
            temp_dir,
            workspace,
        })
    }

    /// Get the root path of the test fixture
    pub fn root(&self) -> &Path {
        self.temp_dir.path()
    }

    /// Create a test CSV file with sample data
    pub fn create_csv(&self, name: &str, data: &[Vec<&str>]) -> Result<PathBuf> {
        let path = self.root().join(name);
        let mut content = String::new();
        
        for row in data {
            content.push_str(&row.join(","));
            content.push('\n');
        }
        
        fs::write(&path, content)?;
        Ok(path)
    }

    /// Create a test CSV file with raw string content
    pub fn create_csv_raw(&self, name: &str, content: &str) -> Result<PathBuf> {
        let path = self.root().join(name);
        fs::write(&path, content)?;
        Ok(path)
    }

    /// Create a test JSON file with sample data
    pub fn create_json(&self, name: &str, data: &serde_json::Value) -> Result<PathBuf> {
        let path = self.root().join(name);
        let content = serde_json::to_string_pretty(data)?;
        fs::write(&path, content)?;
        Ok(path)
    }

    /// Create a corrupted file for testing error handling
    pub fn create_corrupted_file(&self, name: &str) -> Result<PathBuf> {
        let path = self.root().join(name);
        fs::write(&path, b"\x00\x01\x02\x03invalid_data\xff\xfe")?;
        Ok(path)
    }

    /// Create a large CSV file for performance testing
    pub fn create_large_csv(&self, name: &str, rows: usize, cols: usize) -> Result<PathBuf> {
        let path = self.root().join(name);
        let mut content = String::new();
        
        // Header
        for i in 0..cols {
            if i > 0 { content.push(','); }
            content.push_str(&format!("col_{}", i));
        }
        content.push('\n');
        
        // Data rows
        for row in 0..rows {
            for col in 0..cols {
                if col > 0 { content.push(','); }
                content.push_str(&format!("value_{}_{}", row, col));
            }
            content.push('\n');
        }
        
        fs::write(&path, content)?;
        Ok(path)
    }

    /// Update a CSV file with new data
    pub fn update_csv(&self, name: &str, data: &[Vec<&str>]) -> Result<PathBuf> {
        let path = self.root().join(name);
        let mut content = String::new();
        
        for row in data {
            content.push_str(&row.join(","));
            content.push('\n');
        }
        
        fs::write(&path, content)?;
        Ok(path)
    }

    /// Create a CSV with various data types for edge case testing
    pub fn create_mixed_types_csv(&self, name: &str) -> Result<PathBuf> {
        let data = vec![
            vec!["id", "name", "price", "active", "created_at", "notes"],
            vec!["1", "Product A", "19.99", "true", "2023-01-01", "Normal product"],
            vec!["2", "Product B", "", "false", "2023-01-02", ""],
            vec!["3", "Product \"C\"", "-5.50", "true", "", "Has quotes"],
            vec!["4", "Product,D", "0", "false", "2023-01-04", "Has comma"],
            vec!["5", "", "999.99", "", "2023-01-05", "Missing name"],
            vec!["", "Product F", "abc", "maybe", "invalid-date", "Invalid data"],
        ];
        
        self.create_csv(name, &data)
    }

    /// Create a CSV with Unicode characters
    pub fn create_unicode_csv(&self, name: &str) -> Result<PathBuf> {
        let data = vec![
            vec!["id", "name", "description"],
            vec!["1", "Café", "Delicious café ☕"],
            vec!["2", "Naïve", "Naïve approach 🤔"],
            vec!["3", "北京", "Beijing in Chinese 中文"],
            vec!["4", "🚀", "Rocket emoji as name"],
        ];
        
        self.create_csv(name, &data)
    }

    /// Assert that a snapshot exists
    pub fn assert_snapshot_exists(&self, name: &str) {
        assert!(self.workspace.snapshot_exists(name), "Snapshot '{}' should exist", name);
    }

    /// Assert that a snapshot does not exist
    pub fn assert_snapshot_not_exists(&self, name: &str) {
        assert!(!self.workspace.snapshot_exists(name), "Snapshot '{}' should not exist", name);
    }
}

/// Helper for running CLI commands in tests
pub struct CliTestRunner {
    fixture: TestFixture,
}

impl CliTestRunner {
    pub fn new() -> Result<Self> {
        Ok(Self {
            fixture: TestFixture::new()?,
        })
    }

    pub fn fixture(&self) -> &TestFixture {
        &self.fixture
    }

    /// Run a tabdiff command and return the result
    pub fn run_command(&self, args: &[&str]) -> Result<()> {
        use tabdiff::cli::Cli;
        use tabdiff::commands::execute_command;
        use clap::Parser;

        // Build command line arguments
        let mut cmd_args = vec!["tabdiff"];
        cmd_args.extend(args);

        // Parse CLI arguments
        let cli = Cli::try_parse_from(cmd_args)
            .map_err(|e| tabdiff::TabdiffError::invalid_input(e.to_string()))?;

        // Execute command with the workspace path from CLI (if any)
        // If no --workspace flag was provided, use the fixture root as default
        let workspace_path = cli.workspace.as_deref().or(Some(self.fixture.root()));
        execute_command(cli.command, workspace_path)
    }

    /// Run a command and expect it to succeed
    pub fn expect_success(&self, args: &[&str]) {
        self.run_command(args).expect("Command should succeed");
    }

    /// Run a command and expect it to succeed, returning output
    pub fn expect_success_with_output(&self, args: &[&str]) -> String {
        // This is a simplified version - in real implementation would capture output
        self.run_command(args).expect("Command should succeed");
        // For now, return empty string - this should be implemented to capture actual output
        String::new()
    }

    /// Run a command and expect it to fail
    pub fn expect_failure(&self, args: &[&str]) -> tabdiff::TabdiffError {
        self.run_command(args).expect_err("Command should fail")
    }
}

/// Sample data generators for testing
pub mod sample_data {
    use serde_json::json;

    pub fn simple_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["1", "Apple", "1.50"],
            vec!["2", "Banana", "0.75"],
            vec!["3", "Cherry", "2.00"],
        ]
    }

    pub fn updated_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["1", "Apple", "1.60"], // Price changed
            vec!["2", "Banana", "0.75"],
            vec!["4", "Date", "3.00"],   // New row, Cherry removed
        ]
    }

    pub fn schema_changed_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price", "category"], // New column
            vec!["1", "Apple", "1.50", "Fruit"],
            vec!["2", "Banana", "0.75", "Fruit"],
            vec!["3", "Cherry", "2.00", "Fruit"],
        ]
    }

    // Table change test data generators
    pub fn rows_added_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["1", "Apple", "1.50"],
            vec!["2", "Banana", "0.75"],
            vec!["3", "Cherry", "2.00"],
            vec!["4", "Date", "3.00"],    // New row
            vec!["5", "Elderberry", "4.50"], // Another new row
        ]
    }

    pub fn rows_deleted_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["2", "Banana", "0.75"], // Only middle row remains (Cherry and Apple deleted)
        ]
    }

    pub fn values_changed_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["1", "Green Apple", "1.75"], // Name and price changed
            vec!["2", "Banana", "0.75"],      // Unchanged
            vec!["3", "Cherry", "2.50"],      // Price changed
        ]
    }

    pub fn columns_reordered_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["price", "id", "name"], // Columns reordered
            vec!["1.50", "1", "Apple"],
            vec!["0.75", "2", "Banana"],
            vec!["2.00", "3", "Cherry"],
        ]
    }

    pub fn columns_renamed_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["product_id", "product_name", "unit_price"], // All columns renamed
            vec!["1", "Apple", "1.50"],
            vec!["2", "Banana", "0.75"],
            vec!["3", "Cherry", "2.00"],
        ]
    }

    pub fn column_types_changed_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["001", "Apple", "$1.50"],    // id now has leading zeros, price has currency symbol
            vec!["002", "Banana", "$0.75"],
            vec!["003", "Cherry", "$2.00"],
        ]
    }

    pub fn multiple_rows_added_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["0", "Avocado", "2.50"],     // New row at beginning
            vec!["1", "Apple", "1.50"],
            vec!["1.5", "Apricot", "3.00"],   // New row in middle
            vec!["2", "Banana", "0.75"],
            vec!["3", "Cherry", "2.00"],
            vec!["4", "Date", "3.50"],        // New row at end
        ]
    }

    pub fn multiple_rows_deleted_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"],
            vec!["2", "Banana", "0.75"], // Only one row remains
        ]
    }

    pub fn empty_table_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price"], // Headers only, no data rows
        ]
    }

    pub fn mixed_changes_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["id", "name", "price", "category"], // New column added
            vec!["1", "Green Apple", "1.75", "Fruit"], // Value changed + new column
            vec!["2", "Banana", "0.75", "Fruit"],      // New column only
            vec!["4", "Date", "3.00", "Fruit"],        // New row (id=3 Cherry deleted)
            vec!["5", "Elderberry", "4.50", "Berry"],  // Another new row
        ]
    }

    pub fn column_order_and_rename_csv_data() -> Vec<Vec<&'static str>> {
        vec![
            vec!["unit_price", "product_id", "product_name"], // Reordered AND renamed
            vec!["1.50", "1", "Apple"],
            vec!["0.75", "2", "Banana"],
            vec!["2.00", "3", "Cherry"],
        ]
    }

    pub fn simple_json_data() -> serde_json::Value {
        json!([
            {"id": 1, "name": "Apple", "price": 1.50},
            {"id": 2, "name": "Banana", "price": 0.75},
            {"id": 3, "name": "Cherry", "price": 2.00}
        ])
    }

    pub fn nested_json_data() -> serde_json::Value {
        json!([
            {
                "id": 1,
                "product": {
                    "name": "Apple",
                    "details": {
                        "price": 1.50,
                        "category": "Fruit"
                    }
                },
                "tags": ["fresh", "organic"]
            },
            {
                "id": 2,
                "product": {
                    "name": "Banana",
                    "details": {
                        "price": 0.75,
                        "category": "Fruit"
                    }
                },
                "tags": ["tropical"]
            }
        ])
    }
}

/// Assertion helpers for test validation
pub mod assertions {
    use std::path::Path;
    use tabdiff::Result;

    /// Assert that a file exists and is not empty
    pub fn assert_file_exists_and_not_empty(path: &Path) {
        assert!(path.exists(), "File should exist: {}", path.display());
        let metadata = std::fs::metadata(path).expect("Should be able to read file metadata");
        assert!(metadata.len() > 0, "File should not be empty: {}", path.display());
    }

    /// Assert that a directory exists
    pub fn assert_dir_exists(path: &Path) {
        assert!(path.exists(), "Directory should exist: {}", path.display());
        assert!(path.is_dir(), "Path should be a directory: {}", path.display());
    }

    /// Assert that a JSON file contains expected keys
    pub fn assert_json_contains_keys(path: &Path, keys: &[&str]) -> Result<()> {
        let content = std::fs::read_to_string(path)?;
        let json: serde_json::Value = serde_json::from_str(&content)?;
        
        for key in keys {
            assert!(json.get(key).is_some(), "JSON should contain key '{}': {}", key, path.display());
        }
        
        Ok(())
    }

    /// Assert that two files have the same content
    pub fn assert_files_equal(path1: &Path, path2: &Path) -> Result<()> {
        let content1 = std::fs::read(path1)?;
        let content2 = std::fs::read(path2)?;
        assert_eq!(content1, content2, "Files should have identical content");
        Ok(())
    }

    /// Assert that a specific cell change was detected correctly
    pub fn assert_cell_change_detected(
        json: &serde_json::Value, 
        row_index: usize, 
        column: &str, 
        before: &str, 
        after: &str
    ) {
        let modified_rows = json["row_changes"]["modified"].as_array()
            .expect("Should have modified rows array");
        
        let row_change = modified_rows.iter()
            .find(|row| row["row_index"] == row_index)
            .expect(&format!("Should find modified row at index {}", row_index));
        
        let changes = &row_change["changes"];
        assert_eq!(changes[column]["before"], before, 
                  "Before value should match for column {}", column);
        assert_eq!(changes[column]["after"], after, 
                  "After value should match for column {}", column);
    }

    /// Assert that an added row was detected correctly
    pub fn assert_row_addition_detected(
        json: &serde_json::Value,
        row_index: usize,
        expected_data: &std::collections::HashMap<&str, &str>
    ) {
        let added_rows = json["row_changes"]["added"].as_array()
            .expect("Should have added rows array");
        
        let added_row = added_rows.iter()
            .find(|row| row["row_index"] == row_index)
            .expect(&format!("Should find added row at index {}", row_index));
        
        for (column, expected_value) in expected_data {
            assert_eq!(added_row["data"][column], *expected_value,
                      "Added row data should match for column {}", column);
        }
    }

    /// Assert that a removed row was detected correctly
    pub fn assert_row_removal_detected(
        json: &serde_json::Value,
        expected_data: &std::collections::HashMap<&str, &str>
    ) {
        let removed_rows = json["row_changes"]["removed"].as_array()
            .expect("Should have removed rows array");
        
        let found_match = removed_rows.iter().any(|row| {
            expected_data.iter().all(|(column, expected_value)| {
                row["data"][column] == *expected_value
            })
        });
        
        assert!(found_match, "Should find matching removed row data");
    }

}
