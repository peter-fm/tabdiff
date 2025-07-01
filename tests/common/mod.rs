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
}
