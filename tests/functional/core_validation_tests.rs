//! Core functionality validation tests
//!
//! These tests validate the essential features that make tabdiff unique:
//! - Diff functionality actually detects changes between snapshots
//! - Rollback functionality actually restores full data, not just headers
//! - Cell-level change detection works properly

use crate::common::CliTestRunner;
use std::fs;

#[test]
fn test_diff_detects_price_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline data
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    // Create modified data with price changes
    let modified_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.75"],  // Price changed from 1.50 to 1.75
        vec!["2", "Banana", "0.75"], // Unchanged
        vec!["3", "Cherry", "2.25"],  // Price changed from 2.00 to 2.25
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline", "--full-data"
    ]);
    runner.expect_success(&[
        "snapshot", modified_csv.to_str().unwrap(), "--name", "modified", "--full-data"
    ]);
    
    // Generate diff
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    // Read and validate diff results
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    println!("Diff content: {}", serde_json::to_string_pretty(&diff_json).unwrap());
    
    // CRITICAL: Diff should detect changes, not report "rows_changed: 0"
    assert!(diff_json["rows_changed"].as_i64().unwrap() > 0, 
           "Diff should detect that rows were changed");
    
    // Should NOT report schema changes since only values changed
    assert_eq!(diff_json["schema_changed"], false, 
              "Schema should be unchanged for value-only changes");
    
    // Should detect specific changes
    assert!(diff_json.get("sample_changes").is_some(), 
           "Diff should include sample changes");
}

#[test]
fn test_diff_detects_row_additions() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline data
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
    ];
    
    // Create data with additional rows
    let expanded_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],  // New row
        vec!["4", "Date", "3.00"],    // Another new row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let expanded_csv = runner.fixture().create_csv("expanded.csv", &expanded_data).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline", "--full-data"
    ]);
    runner.expect_success(&[
        "snapshot", expanded_csv.to_str().unwrap(), "--name", "expanded", "--full-data"
    ]);
    
    // Generate diff
    runner.expect_success(&["diff", "baseline", "expanded"]);
    
    // Read and validate diff results
    let diff_path = runner.fixture().workspace.diff_path("baseline", "expanded");
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    println!("Diff content: {}", serde_json::to_string_pretty(&diff_json).unwrap());
    
    // CRITICAL: Should detect row count difference
    assert_eq!(diff_json["row_count"], 4, "Should show updated row count");
    
    // Should detect changes
    assert!(diff_json["rows_changed"].as_i64().unwrap() > 0, 
           "Should detect row additions as changes");
}

#[test]
fn test_diff_detects_schema_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline data
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
    ];
    
    // Create data with schema change (new column)
    let schema_changed_data = vec![
        vec!["id", "name", "price", "category"],  // New column
        vec!["1", "Apple", "1.50", "Fruit"],
        vec!["2", "Banana", "0.75", "Fruit"],
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let schema_csv = runner.fixture().create_csv("schema.csv", &schema_changed_data).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline", "--full-data"
    ]);
    runner.expect_success(&[
        "snapshot", schema_csv.to_str().unwrap(), "--name", "schema_changed", "--full-data"
    ]);
    
    // Generate diff
    runner.expect_success(&["diff", "baseline", "schema_changed"]);
    
    // Read and validate diff results
    let diff_path = runner.fixture().workspace.diff_path("baseline", "schema_changed");
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    println!("Diff content: {}", serde_json::to_string_pretty(&diff_json).unwrap());
    
    // CRITICAL: Should detect schema changes
    assert_eq!(diff_json["schema_changed"], true, 
              "Should detect schema changes when columns are added");
    
    // Should list the column changes
    assert!(diff_json["columns_changed"].as_array().unwrap().len() > 0, 
           "Should list specific column changes");
}

#[test]
fn test_rollback_restores_full_data_not_just_headers() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline data
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    // Create modified data 
    let modified_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Green Apple", "1.75"],  // Changed
        vec!["4", "Date", "3.00"],         // New row, others deleted
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let target_file = runner.fixture().create_csv("target.csv", &modified_data).unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline", "--full-data"
    ]);
    
    // Execute rollback
    runner.expect_success(&[
        "rollback", target_file.to_str().unwrap(), "--to", "baseline", "--force"
    ]);
    
    // CRITICAL: Read the rolled back file and verify it contains FULL DATA
    let restored_content = fs::read_to_string(&target_file).unwrap();
    
    println!("Restored content: '{}'", restored_content);
    
    // Should contain the header
    assert!(restored_content.contains("id,name,price"), 
           "Should contain header");
    
    // CRITICAL: Should contain ALL the original data rows, not just header
    assert!(restored_content.contains("1,Apple,1.5"), 
           "Should restore Apple row with original price");
    assert!(restored_content.contains("2,Banana,0.75"), 
           "Should restore Banana row");
    assert!(restored_content.contains("3,Cherry,2"), 
           "Should restore Cherry row");
    
    // Should NOT contain the modified data
    assert!(!restored_content.contains("Green Apple"), 
           "Should not contain modified data");
    assert!(!restored_content.contains("Date"), 
           "Should not contain new rows from modified data");
    
    // Verify backup was created with modified data
    let backup_path = format!("{}.backup", target_file.to_str().unwrap());
    assert!(std::path::Path::new(&backup_path).exists(), 
           "Backup should be created");
    
    let backup_content = fs::read_to_string(&backup_path).unwrap();
    assert!(backup_content.contains("Green Apple"), 
           "Backup should contain original modified data");
}

#[test]
fn test_rollback_preserves_exact_data_format() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline with specific formatting
    let baseline_data = vec![
        vec!["product_id", "name", "unit_price", "in_stock"],
        vec!["SKU001", "Red Apple", "1.25", "true"],
        vec!["SKU002", "Yellow Banana", "0.85", "false"],
        vec!["SKU003", "Fresh Cherry", "2.15", "true"],
    ];
    
    let modified_data = vec![
        vec!["product_id", "name", "unit_price", "in_stock"],
        vec!["SKU999", "Modified Item", "999.99", "false"],
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    let target_file = runner.fixture().create_csv("target.csv", &modified_data).unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline", "--full-data"
    ]);
    
    // Execute rollback
    runner.expect_success(&[
        "rollback", target_file.to_str().unwrap(), "--to", "baseline", "--force"
    ]);
    
    // Verify exact content restoration
    let restored_content = fs::read_to_string(&target_file).unwrap();
    
    println!("Restored content: '{}'", restored_content);
    
    // Should restore exact values (allowing for minor formatting differences)
    assert!(restored_content.contains("SKU001,Red Apple,1.25,true"), 
           "Should restore exact row 1");
    assert!(restored_content.contains("SKU002,Yellow Banana,0.85,false"), 
           "Should restore exact row 2"); 
    assert!(restored_content.contains("SKU003,Fresh Cherry,2.15,true"), 
           "Should restore exact row 3");
    
    // Should have correct line count (header + 3 data rows)
    let lines: Vec<&str> = restored_content.trim().lines().collect();
    assert_eq!(lines.len(), 4, "Should have header + 3 data rows");
}

#[test]
fn test_end_to_end_diff_and_rollback_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create initial data
    let v1_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
    ];
    
    // Create v2 with changes
    let v2_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.60"],     // Price change
        vec!["2", "Banana", "0.75"],    // Unchanged
        vec!["3", "Cherry", "2.00"],    // New row
    ];
    
    // Create v3 with more changes
    let v3_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Green Apple", "1.75"], // Name and price change
        vec!["3", "Cherry", "2.25"],       // Price change, Banana removed
        vec!["4", "Date", "3.00"],         // New row
    ];
    
    let v1_csv = runner.fixture().create_csv("v1.csv", &v1_data).unwrap();
    let v2_csv = runner.fixture().create_csv("v2.csv", &v2_data).unwrap();
    let current_file = runner.fixture().create_csv("current.csv", &v3_data).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot", v1_csv.to_str().unwrap(), "--name", "v1", "--full-data"
    ]);
    runner.expect_success(&[
        "snapshot", v2_csv.to_str().unwrap(), "--name", "v2", "--full-data"
    ]);
    runner.expect_success(&[
        "snapshot", current_file.to_str().unwrap(), "--name", "v3", "--full-data"
    ]);
    
    // Test diff v1 -> v2
    runner.expect_success(&["diff", "v1", "v2"]);
    let diff_path = runner.fixture().workspace.diff_path("v1", "v2");
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect changes between v1 and v2
    assert!(diff_json["rows_changed"].as_i64().unwrap() > 0, 
           "Should detect changes from v1 to v2");
    
    // Test diff v2 -> v3
    runner.expect_success(&["diff", "v2", "v3"]);
    let diff_path = runner.fixture().workspace.diff_path("v2", "v3");
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect changes between v2 and v3
    assert!(diff_json["rows_changed"].as_i64().unwrap() > 0, 
           "Should detect changes from v2 to v3");
    
    // Test rollback current -> v1
    runner.expect_success(&[
        "rollback", current_file.to_str().unwrap(), "--to", "v1", "--force"
    ]);
    
    // Verify rollback restored v1 data exactly
    let restored_content = fs::read_to_string(&current_file).unwrap();
    assert!(restored_content.contains("1,Apple,1.5"), 
           "Should restore v1 Apple data");
    assert!(restored_content.contains("2,Banana,0.75"), 
           "Should restore v1 Banana data");
    assert!(!restored_content.contains("Cherry"), 
           "Should not contain v2/v3 data");
    assert!(!restored_content.contains("Green Apple"), 
           "Should not contain v3 data");
}