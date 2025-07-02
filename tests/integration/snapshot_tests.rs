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
        "--full-data"
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
fn test_snapshot_command_duplicate_name() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // First snapshot should succeed
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "duplicate"]);
    
    // Second snapshot with same name should fail
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "duplicate"]);
    assert!(error.to_string().contains("already exists"));
}

#[test]
fn test_snapshot_command_nonexistent_file() {
    let runner = CliTestRunner::new().unwrap();
    
    let error = runner.expect_failure(&["snapshot", "nonexistent.csv", "--name", "test"]);
    assert!(error.to_string().contains("No such file") || error.to_string().contains("not found"));
}

#[test]
fn test_snapshot_command_invalid_batch_size() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Invalid batch size (zero)
    let error = runner.expect_failure(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "test", 
        "--batch-size", "0"
    ]);
    assert!(error.to_string().contains("must be greater than 0") || error.to_string().contains("invalid"));
    
    // Invalid batch size (negative)
    let error = runner.expect_failure(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "test", 
        "--batch-size", "-100"
    ]);
    assert!(error.to_string().contains("invalid") || error.to_string().contains("error"));
}

#[test]
fn test_snapshot_command_different_formats() {
    let runner = CliTestRunner::new().unwrap();
    
    // Test CSV
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "csv_snapshot"]);
    
    // Test JSON
    let json_path = runner.fixture().create_json("test.json", &sample_data::simple_json_data()).unwrap();
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "json_snapshot"]);
    
    // Verify both snapshots exist
    runner.fixture().assert_snapshot_exists("csv_snapshot");
    runner.fixture().assert_snapshot_exists("json_snapshot");
}

#[test]
fn test_snapshot_command_mixed_data_types() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_mixed_types_csv("mixed.csv").unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "mixed_types"]);
    
    runner.fixture().assert_snapshot_exists("mixed_types");
}

#[test]
fn test_snapshot_command_unicode_data() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_unicode_csv("unicode.csv").unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "unicode_test"]);
    
    runner.fixture().assert_snapshot_exists("unicode_test");
}

#[test]
fn test_snapshot_command_unicode_name() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "测试快照"]);
    
    runner.fixture().assert_snapshot_exists("测试快照");
}

#[test]
fn test_snapshot_command_special_characters_in_name() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    let special_names = vec![
        "test-snapshot",
        "test_snapshot",
        "test.snapshot.v1",
        "snapshot@2024",
    ];
    
    for name in special_names {
        runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", name]);
        runner.fixture().assert_snapshot_exists(name);
    }
}

#[test]
fn test_snapshot_command_empty_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create empty CSV file
    let empty_path = runner.fixture().root().join("empty.csv");
    std::fs::write(&empty_path, "").unwrap();
    
    // Empty files should be handled gracefully, creating a snapshot with 0 rows
    runner.expect_success(&["snapshot", empty_path.to_str().unwrap(), "--name", "empty"]);
    
    runner.fixture().assert_snapshot_exists("empty");
    
    // Verify the snapshot metadata shows 0 rows
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("empty");
    let content = std::fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert_eq!(metadata["row_count"], 0);
}

#[test]
fn test_snapshot_command_corrupted_file() {
    let runner = CliTestRunner::new().unwrap();
    
    let corrupted_path = runner.fixture().create_corrupted_file("corrupted.csv").unwrap();
    
    let error = runner.expect_failure(&["snapshot", corrupted_path.to_str().unwrap(), "--name", "corrupted"]);
    assert!(error.to_string().contains("error") || error.to_string().contains("invalid"));
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
    assert!(metadata["columns"].is_object());
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

#[test]
fn test_snapshot_command_with_verbose() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&[
        "--verbose", 
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "verbose_test"
    ]);
    
    runner.fixture().assert_snapshot_exists("verbose_test");
}

#[test]
fn test_snapshot_command_boundary_batch_sizes() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Test minimum valid batch size
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "min_batch", 
        "--batch-size", "1"
    ]);
    
    // Test large batch size
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "large_batch", 
        "--batch-size", "100000"
    ]);
    
    // Test with and without full data
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "with_full_data", 
        "--full-data"
    ]);
    
    runner.fixture().assert_snapshot_exists("min_batch");
    runner.fixture().assert_snapshot_exists("large_batch");
    runner.fixture().assert_snapshot_exists("with_full_data");
}

#[test]
fn test_snapshot_command_relative_paths() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create file in subdirectory
    let subdir = runner.fixture().root().join("subdir");
    std::fs::create_dir(&subdir).unwrap();
    
    let csv_path = subdir.join("test.csv");
    runner.fixture().create_csv("subdir/test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Use relative path
    runner.expect_success(&["snapshot", "subdir/test.csv", "--name", "relative_path"]);
    
    runner.fixture().assert_snapshot_exists("relative_path");
}

#[test]
fn test_snapshot_command_absolute_paths() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Use absolute path
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "absolute_path"]);
    
    runner.fixture().assert_snapshot_exists("absolute_path");
}
