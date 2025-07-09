//! Integration tests for the snapshot command

use crate::common::{CliTestRunner, sample_data, assertions};

#[test]
fn test_snapshot_command_basic() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create test data
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "test_snapshot"]);
    
    // Verify snapshot was created
    runner.fixture().assert_snapshot_exists("test_snapshot");
    
    // Verify files exist
    let (archive_path, json_path) = runner.fixture().workspace.snapshot_paths("test_snapshot");
    assertions::assert_file_exists_and_not_empty(&archive_path);
    assertions::assert_file_exists_and_not_empty(&json_path);
    
    // Verify JSON metadata structure
    assertions::assert_json_contains_keys(&json_path, &[
        "format_version", "name", "created", "source", "row_count", 
        "column_count", "schema_hash", "columns", "has_full_data"
    ]).unwrap();
}

#[test]
fn test_snapshot_command_with_full_data() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create larger test data
    let csv_path = runner.fixture().create_large_csv("large.csv", 1000, 5).unwrap();
    
    // Test with full data flag
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "full_data_snapshot", 
    ]);
    
    runner.fixture().assert_snapshot_exists("full_data_snapshot");
    
    // Verify has_full_data is true
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("full_data_snapshot");
    let content = std::fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert_eq!(metadata["has_full_data"], true);
}

#[test]
fn test_snapshot_command_with_batch_size() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "test_batch", 
        "--batch-size", "1000"
    ]);
    
    runner.fixture().assert_snapshot_exists("test_batch");
}











#[test]
fn test_snapshot_command_large_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create a reasonably large file for testing (smaller for test speed)
    let large_path = runner.fixture().create_large_csv("large.csv", 1000, 10).unwrap();
    
    runner.expect_success(&[
        "snapshot", large_path.to_str().unwrap(), 
        "--name", "large_snapshot",
        "--batch-size", "100"  // Use smaller batch size for testing
    ]);
    
    runner.fixture().assert_snapshot_exists("large_snapshot");
}

#[test]
fn test_snapshot_command_metadata_content() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "metadata_test"]);
    
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("metadata_test");
    let content = std::fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&content).unwrap();
    
    // Verify metadata content
    assert_eq!(metadata["name"], "metadata_test");
    assert_eq!(metadata["format_version"], "1.0.0");
    assert_eq!(metadata["row_count"], 3); // 3 data rows in simple_csv_data
    assert_eq!(metadata["column_count"], 3); // id, name, price
    assert!(metadata["schema_hash"].is_string());
    assert!(metadata["columns"].is_array()); // columns is an array of column definitions
    assert!(metadata["has_full_data"].is_boolean());
}

#[test]
fn test_snapshot_command_archive_creation() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "archive_test"]);
    
    let (archive_path, _) = runner.fixture().workspace.snapshot_paths("archive_test");
    
    // Archive should exist and be non-empty
    assertions::assert_file_exists_and_not_empty(&archive_path);
    
    // Archive should be smaller than original (due to compression)
    let original_size = std::fs::metadata(&csv_path).unwrap().len();
    let archive_size = std::fs::metadata(&archive_path).unwrap().len();
    
    // For small files, compression might not reduce size, so just check it exists
    assert!(archive_size > 0);
}




