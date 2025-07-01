//! Comprehensive tests for enhanced change detection and rollback functionality
//!
//! This module tests the new comprehensive change detection system that provides:
//! - Detailed before/after values for changed cells
//! - Rollback operations generation
//! - Full data snapshots with --full-data flag
//! - Enhanced status command with detailed output
//! - Rollback command functionality

use crate::common::{CliTestRunner, sample_data, assertions};
use std::fs;
use serde_json::Value;

#[test]
fn test_full_data_snapshot_creation() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
        vec!["3", "4.2", "200", "clothing"],
    ];
    
    let csv_file = runner.fixture().create_csv("test_data.csv", &csv_data).unwrap();
    
    // Create snapshot with --full-data flag
    runner.expect_success(&[
        "snapshot", 
        csv_file.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Verify snapshot exists and has full data
    runner.fixture().assert_snapshot_exists("baseline");
    
    // Check metadata indicates full data
    let metadata_path = runner.fixture().workspace.root.join(".tabdiff/baseline.json");
    let metadata_content = fs::read_to_string(&metadata_path).unwrap();
    let metadata: Value = serde_json::from_str(&metadata_content).unwrap();
    
    assert_eq!(metadata["has_full_data"], true, "Snapshot should indicate it has full data");
    assert_eq!(metadata["row_count"], 3, "Should have correct row count");
    assert_eq!(metadata["column_count"], 4, "Should have correct column count");
}

#[test]
fn test_comprehensive_status_with_cell_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline data
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
        vec!["3", "4.2", "200", "clothing"],
    ];
    
    // Create modified data with specific changes
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.7", "100", "electronics"],     // rating changed: 4.5 → 4.7
        vec!["2", "3.9", "80", "books"],            // rating: 3.8 → 3.9, count: 75 → 80
        vec!["3", "4.2", "200", "clothing"],        // no changes
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    // Create baseline snapshot with full data
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Test status command with JSON output
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline", 
        "--json"
    ]);
    
    // Parse and verify the comprehensive change detection output
    let status_json: Value = serde_json::from_str(&output).unwrap();
    
    // Verify schema changes section
    assert!(status_json["schema_changes"].is_object(), "Should have schema_changes section");
    assert_eq!(status_json["schema_changes"]["columns_added"].as_array().unwrap().len(), 0);
    assert_eq!(status_json["schema_changes"]["columns_removed"].as_array().unwrap().len(), 0);
    
    // Verify row changes section
    assert!(status_json["row_changes"].is_object(), "Should have row_changes section");
    
    let modified_rows = status_json["row_changes"]["modified"].as_array().unwrap();
    assert_eq!(modified_rows.len(), 2, "Should detect 2 modified rows");
    
    // Verify first modified row (row 0)
    let row_0_changes = &modified_rows[0];
    assert_eq!(row_0_changes["row_index"], 0);
    let row_0_change_details = &row_0_changes["changes"];
    assert_eq!(row_0_change_details["rating"]["before"], "4.5");
    assert_eq!(row_0_change_details["rating"]["after"], "4.7");
    
    // Verify second modified row (row 1)
    let row_1_changes = &modified_rows[1];
    assert_eq!(row_1_changes["row_index"], 1);
    let row_1_change_details = &row_1_changes["changes"];
    assert_eq!(row_1_change_details["rating"]["before"], "3.8");
    assert_eq!(row_1_change_details["rating"]["after"], "3.9");
    assert_eq!(row_1_change_details["count"]["before"], "75");
    assert_eq!(row_1_change_details["count"]["after"], "80");
    
    // Verify rollback operations
    assert!(status_json["rollback_operations"].is_array(), "Should have rollback_operations");
    let rollback_ops = status_json["rollback_operations"].as_array().unwrap();
    assert!(rollback_ops.len() > 0, "Should have rollback operations");
}

#[test]
fn test_comprehensive_status_with_added_rows() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
        vec!["3", "4.2", "200", "clothing"],        // added row
        vec!["4", "5.0", "25", "gadgets"],          // added row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline", 
        "--json"
    ]);
    
    let status_json: Value = serde_json::from_str(&output).unwrap();
    
    // Verify added rows
    let added_rows = status_json["row_changes"]["added"].as_array().unwrap();
    assert_eq!(added_rows.len(), 2, "Should detect 2 added rows");
    
    // Verify first added row
    let added_row_0 = &added_rows[0];
    assert_eq!(added_row_0["row_index"], 2);
    assert_eq!(added_row_0["data"]["product_id"], "3");
    assert_eq!(added_row_0["data"]["category"], "clothing");
    
    // Verify second added row
    let added_row_1 = &added_rows[1];
    assert_eq!(added_row_1["row_index"], 3);
    assert_eq!(added_row_1["data"]["product_id"], "4");
    assert_eq!(added_row_1["data"]["category"], "gadgets");
}

#[test]
fn test_comprehensive_status_with_removed_rows() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
        vec!["3", "4.2", "200", "clothing"],
        vec!["4", "5.0", "25", "gadgets"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["3", "4.2", "200", "clothing"],        // rows 2 and 4 removed
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline", 
        "--json"
    ]);
    
    let status_json: Value = serde_json::from_str(&output).unwrap();
    
    // Verify removed rows
    let removed_rows = status_json["row_changes"]["removed"].as_array().unwrap();
    assert_eq!(removed_rows.len(), 2, "Should detect 2 removed rows");
}

#[test]
fn test_rollback_dry_run() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.7", "100", "electronics"],     // rating changed
        vec!["2", "3.9", "80", "books"],            // rating and count changed
        vec!["3", "5.0", "25", "gadgets"],          // added row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Test rollback dry run
    let output = runner.expect_success_with_output(&[
        "rollback", 
        modified_csv.to_str().unwrap(), 
        "--to", "baseline", 
        "--dry-run"
    ]);
    
    // Verify dry run output shows what would be changed
    assert!(output.contains("Dry run"), "Should indicate dry run mode");
    assert!(output.contains("rating: '4.7' → '4.5'"), "Should show rating change");
    assert!(output.contains("count: '80' → '75'"), "Should show count change");
    assert!(output.contains("Removed rows: 1"), "Should show row removal");
    
    // Verify file was not actually changed
    let current_content = fs::read_to_string(&modified_csv).unwrap();
    assert!(current_content.contains("4.7"), "File should still contain modified values");
    assert!(current_content.contains("gadgets"), "File should still contain added row");
}

#[test]
fn test_rollback_execution_with_backup() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.7", "100", "electronics"],     // rating changed
        vec!["2", "3.9", "80", "books"],            // rating and count changed
        vec!["3", "5.0", "25", "gadgets"],          // added row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Execute rollback with force flag
    let output = runner.expect_success_with_output(&[
        "rollback", 
        modified_csv.to_str().unwrap(), 
        "--to", "baseline", 
        "--force"
    ]);
    
    // Verify rollback success message
    assert!(output.contains("Rollback completed successfully"), "Should show success message");
    assert!(output.contains("Backup created"), "Should create backup");
    
    // Verify file was rolled back correctly
    let rolled_back_content = fs::read_to_string(&modified_csv).unwrap();
    assert!(rolled_back_content.contains("4.5"), "Should contain original rating");
    assert!(rolled_back_content.contains("75"), "Should contain original count");
    assert!(!rolled_back_content.contains("gadgets"), "Should not contain added row");
    
    // Verify backup was created
    let backup_path = format!("{}.backup", modified_csv.to_str().unwrap());
    assert!(std::path::Path::new(&backup_path).exists(), "Backup file should exist");
    
    let backup_content = fs::read_to_string(&backup_path).unwrap();
    assert!(backup_content.contains("4.7"), "Backup should contain modified values");
    assert!(backup_content.contains("gadgets"), "Backup should contain added row");
}

#[test]
fn test_rollback_no_changes_needed() {
    let runner = CliTestRunner::new().unwrap();
    
    let data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
    ];
    
    let csv_file = runner.fixture().create_csv("test_data.csv", &data).unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&[
        "snapshot", 
        csv_file.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Try to rollback to same state
    let output = runner.expect_success_with_output(&[
        "rollback", 
        csv_file.to_str().unwrap(), 
        "--to", "baseline", 
        "--force"
    ]);
    
    // Should indicate no rollback needed
    assert!(output.contains("already at the target snapshot state"), "Should indicate no changes needed");
    assert!(output.contains("No rollback needed"), "Should indicate no rollback needed");
}

#[test]
fn test_status_pretty_output_format() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
        vec!["2", "3.8", "75", "books"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.7", "100", "electronics"],     // rating changed
        vec!["2", "3.9", "80", "books"],            // rating and count changed
        vec!["3", "5.0", "25", "gadgets"],          // added row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Test pretty output format (default)
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline"
    ]);
    
    // Verify pretty output format
    assert!(output.contains("📊 tabdiff status"), "Should have status header");
    assert!(output.contains("✅ Schema: unchanged"), "Should show schema status");
    assert!(output.contains("❌ Rows changed:"), "Should show row changes");
    assert!(output.contains("Modified rows:"), "Should show modified rows section");
    assert!(output.contains("rating: '4.5' → '4.7'"), "Should show before/after values");
    assert!(output.contains("Added rows:"), "Should show added rows section");
    assert!(output.contains("Total rollback operations:"), "Should show rollback operations count");
}

#[test]
fn test_column_order_preservation() {
    let runner = CliTestRunner::new().unwrap();
    
    // Test that column order is preserved correctly
    let baseline_data = vec![
        vec!["product_id", "rating", "count", "category"],
        vec!["1", "4.5", "100", "electronics"],
    ];
    
    let csv_file = runner.fixture().create_csv("test_data.csv", &baseline_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        csv_file.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Verify snapshot preserves column order
    let output = runner.expect_success_with_output(&[
        "show", 
        "baseline", 
        "--detailed", 
        "--format", "json"
    ]);
    
    let show_json: Value = serde_json::from_str(&output).unwrap();
    let columns = show_json["archive_data"]["schema"]["columns"].as_array().unwrap();
    
    // Verify column order is preserved
    assert_eq!(columns[0]["name"], "product_id");
    assert_eq!(columns[1]["name"], "rating");
    assert_eq!(columns[2]["name"], "count");
    assert_eq!(columns[3]["name"], "category");
}

#[test]
fn test_mixed_changes_comprehensive_detection() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["id", "name", "price", "category"],
        vec!["1", "Apple", "1.50", "fruit"],
        vec!["2", "Banana", "0.75", "fruit"],
        vec!["3", "Carrot", "1.00", "vegetable"],
        vec!["4", "Date", "3.00", "fruit"],
    ];
    
    let modified_data = vec![
        vec!["id", "name", "price", "category"],
        vec!["1", "Green Apple", "1.75", "organic"],    // name, price, category changed
        vec!["2", "Banana", "0.75", "fruit"],           // no changes
        // row 3 (Carrot) removed
        vec!["4", "Date", "3.00", "fruit"],             // no changes
        vec!["5", "Elderberry", "4.50", "berry"],       // new row
        vec!["6", "Fig", "2.25", "fruit"],              // new row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline", 
        "--json"
    ]);
    
    let status_json: Value = serde_json::from_str(&output).unwrap();
    
    // Verify comprehensive detection of mixed changes
    let modified_rows = status_json["row_changes"]["modified"].as_array().unwrap();
    assert_eq!(modified_rows.len(), 1, "Should detect 1 modified row");
    
    // Verify the modified row has multiple changes
    let row_changes = &modified_rows[0]["changes"];
    assert!(row_changes["name"].is_object(), "Should detect name change");
    assert!(row_changes["price"].is_object(), "Should detect price change");
    assert!(row_changes["category"].is_object(), "Should detect category change");
    
    let added_rows = status_json["row_changes"]["added"].as_array().unwrap();
    assert_eq!(added_rows.len(), 2, "Should detect 2 added rows");
    
    let removed_rows = status_json["row_changes"]["removed"].as_array().unwrap();
    assert_eq!(removed_rows.len(), 1, "Should detect 1 removed row");
    
    // Verify rollback operations include all necessary changes
    let rollback_ops = status_json["rollback_operations"].as_array().unwrap();
    assert!(rollback_ops.len() >= 5, "Should have multiple rollback operations for all changes");
}

#[test]
fn test_error_handling_missing_snapshot() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_data = vec![
        vec!["id", "name"],
        vec!["1", "test"],
    ];
    
    let csv_file = runner.fixture().create_csv("test_data.csv", &csv_data).unwrap();
    
    // Try to use status with non-existent snapshot
    runner.expect_failure(&[
        "status", 
        csv_file.to_str().unwrap(), 
        "--compare-to", "nonexistent"
    ]);
    
    // Try to rollback to non-existent snapshot
    runner.expect_failure(&[
        "rollback", 
        csv_file.to_str().unwrap(), 
        "--to", "nonexistent"
    ]);
}

#[test]
fn test_error_handling_non_full_data_snapshot() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_data = vec![
        vec!["id", "name"],
        vec!["1", "test"],
    ];
    
    let csv_file = runner.fixture().create_csv("test_data.csv", &csv_data).unwrap();
    
    // Create snapshot without --full-data
    runner.expect_success(&[
        "snapshot", 
        csv_file.to_str().unwrap(), 
        "--name", "hash_only"
    ]);
    
    // Try to rollback to hash-only snapshot (should fail)
    runner.expect_failure(&[
        "rollback", 
        csv_file.to_str().unwrap(), 
        "--to", "hash_only"
    ]);
}

#[test]
fn test_rollback_with_no_backup() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["id", "value"],
        vec!["1", "original"],
    ];
    
    let modified_data = vec![
        vec!["id", "value"],
        vec!["1", "modified"],
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data"
    ]);
    
    // Execute rollback without backup
    runner.expect_success(&[
        "rollback", 
        modified_csv.to_str().unwrap(), 
        "--to", "baseline", 
        "--force",
        "--no-backup"
    ]);
    
    // Verify no backup was created
    let backup_path = format!("{}.backup", modified_csv.to_str().unwrap());
    assert!(!std::path::Path::new(&backup_path).exists(), "Backup file should not exist");
    
    // Verify file was still rolled back
    let content = fs::read_to_string(&modified_csv).unwrap();
    assert!(content.contains("original"), "Should contain original value");
}

#[test]
fn test_large_dataset_comprehensive_detection() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create larger dataset for testing performance
    let baseline_csv = runner.fixture().create_large_csv("baseline.csv", 100, 5).unwrap();
    let modified_csv = runner.fixture().create_large_csv("modified.csv", 110, 5).unwrap(); // 10 more rows
    
    runner.expect_success(&[
        "snapshot", 
        baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--full-data",
        "--sample", "full"
    ]);
    
    // Test comprehensive status on larger dataset
    let output = runner.expect_success_with_output(&[
        "status", 
        modified_csv.to_str().unwrap(), 
        "--compare-to", "baseline", 
        "--json"
    ]);
    
    let status_json: Value = serde_json::from_str(&output).unwrap();
    
    // Should detect the added rows
    let added_rows = status_json["row_changes"]["added"].as_array().unwrap();
    assert!(added_rows.len() > 0, "Should detect added rows in larger dataset");
    
    // Should have rollback operations
    let rollback_ops = status_json["rollback_operations"].as_array().unwrap();
    assert!(rollback_ops.len() > 0, "Should have rollback operations for larger dataset");
}
