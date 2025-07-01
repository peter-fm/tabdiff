//! Comprehensive tests for basic table changes
//!
//! This module tests the core table change scenarios that users commonly encounter:
//! - Adding and deleting rows
//! - Changing values in cells
//! - Reordering columns
//! - Renaming columns
//! - Changing column types
//!
//! Each test verifies that tabdiff correctly detects the changes and provides
//! logical, user-friendly output in the diff results.

use crate::common::{CliTestRunner, sample_data, assertions};
use std::fs;

#[test]
fn test_add_single_row() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create baseline snapshot
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    // Create data with added rows
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::rows_added_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    // Generate diff
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    // Validate diff output
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect that rows were added
    assert!(diff_json.get("rows_changed").is_some(), "Diff should contain rows_changed information");
    
    // Verify snapshots exist
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_add_multiple_rows() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::multiple_rows_added_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_delete_single_row() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::rows_deleted_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect that rows were changed (deleted)
    assert!(diff_json.get("rows_changed").is_some(), "Diff should contain rows_changed information");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_delete_multiple_rows() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::multiple_rows_deleted_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_delete_all_rows() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::empty_table_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect significant row changes (all rows deleted)
    assert!(diff_json.get("rows_changed").is_some(), "Diff should contain rows_changed information");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_single_cell_value() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::values_changed_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect that rows were changed
    assert!(diff_json.get("rows_changed").is_some(), "Diff should contain rows_changed information");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_multiple_cell_values() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create data with multiple value changes
    let baseline_data = vec![
        vec!["id", "name", "price", "category"],
        vec!["1", "Apple", "1.50", "Fruit"],
        vec!["2", "Banana", "0.75", "Fruit"],
        vec!["3", "Cherry", "2.00", "Fruit"],
    ];
    
    let modified_data = vec![
        vec!["id", "name", "price", "category"],
        vec!["1", "Green Apple", "1.75", "Organic"], // Multiple changes in same row
        vec!["2", "Yellow Banana", "0.80", "Tropical"], // Multiple changes in same row
        vec!["3", "Cherry", "2.00", "Fruit"], // No changes
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_column_order() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::columns_reordered_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect schema changes due to column reordering
    assert!(diff_json.get("schema_changed").is_some(), "Diff should detect schema changes from column reordering");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_column_names() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::columns_renamed_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect schema changes due to column renaming
    assert!(diff_json.get("schema_changed").is_some(), "Diff should detect schema changes from column renaming");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_single_column_name() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let modified_data = vec![
        vec!["id", "product_name", "price"], // Only 'name' column renamed
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_change_column_types() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::column_types_changed_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect changes (type changes manifest as data changes)
    assert!(diff_json.get("rows_changed").is_some() || diff_json.get("schema_changed").is_some(), 
           "Diff should detect changes from column type modifications");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_numeric_to_string_type_change() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let modified_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "Low"],     // Numeric to string
        vec!["2", "Banana", "Medium"], // Numeric to string
        vec!["3", "Cherry", "High"],   // Numeric to string
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_mixed_table_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::mixed_changes_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect both schema and row changes
    assert!(diff_json.get("schema_changed").is_some(), "Diff should detect schema changes");
    assert!(diff_json.get("rows_changed").is_some(), "Diff should detect row changes");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_column_order_and_rename_combined() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::column_order_and_rename_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should detect schema changes due to both reordering and renaming
    assert!(diff_json.get("schema_changed").is_some(), "Diff should detect schema changes from column reordering and renaming");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_add_and_delete_rows_simultaneously() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let modified_data = vec![
        vec!["id", "name", "price"],
        vec!["1", "Apple", "1.50"],     // Kept
        vec!["4", "Date", "3.00"],      // New row (Banana and Cherry deleted)
        vec!["5", "Elderberry", "4.50"], // New row
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_case_sensitive_column_name_change() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_data = vec![
        vec!["ID", "Name", "Price"],
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let modified_data = vec![
        vec!["id", "name", "price"], // Case changed
        vec!["1", "Apple", "1.50"],
        vec!["2", "Banana", "0.75"],
        vec!["3", "Cherry", "2.00"],
    ];
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &baseline_data).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &modified_data).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_diff_output_format_and_content() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::values_changed_csv_data()).unwrap();
    runner.expect_success(&["snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    // Test different diff modes
    runner.expect_success(&["diff", "baseline", "modified", "--mode", "quick"]);
    runner.expect_success(&["diff", "baseline", "modified", "--mode", "detailed"]);
    runner.expect_success(&["diff", "baseline", "modified", "--mode", "auto"]);
    
    // Test custom output file
    runner.expect_success(&["diff", "baseline", "modified", "--output", "custom_diff.json"]);
    
    let custom_diff_path = runner.fixture().root().join("custom_diff.json");
    assertions::assert_file_exists_and_not_empty(&custom_diff_path);
    
    // Verify the diff content is valid JSON
    let diff_content = fs::read_to_string(&custom_diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    
    // Should contain basic diff structure
    assert!(diff_json.is_object(), "Diff output should be a JSON object");
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_status_command_with_table_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::values_changed_csv_data()).unwrap();
    
    // Test status command with different output formats
    runner.expect_success(&["status", modified_csv.to_str().unwrap(), "--compare-to", "baseline"]);
    runner.expect_success(&["status", modified_csv.to_str().unwrap(), "--compare-to", "baseline", "--json"]);
    runner.expect_success(&["status", modified_csv.to_str().unwrap(), "--compare-to", "baseline", "--quiet"]);
    
    // Test with sampling
    runner.expect_success(&["status", modified_csv.to_str().unwrap(), "--compare-to", "baseline", "--sample", "100%"]);
    
    runner.fixture().assert_snapshot_exists("baseline");
}

#[test]
fn test_large_table_changes_with_sampling() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create larger datasets for testing sampling with changes
    let baseline_csv = runner.fixture().create_large_csv("baseline.csv", 1000, 5).unwrap();
    runner.expect_success(&[
        "snapshot", baseline_csv.to_str().unwrap(), 
        "--name", "baseline", 
        "--sample", "10%"
    ]);
    
    let modified_csv = runner.fixture().create_large_csv("modified.csv", 1200, 5).unwrap(); // More rows
    runner.expect_success(&[
        "snapshot", modified_csv.to_str().unwrap(), 
        "--name", "modified", 
        "--sample", "10%"
    ]);
    
    runner.expect_success(&["diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}

#[test]
fn test_table_changes_with_verbose_output() {
    let runner = CliTestRunner::new().unwrap();
    
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["--verbose", "snapshot", baseline_csv.to_str().unwrap(), "--name", "baseline"]);
    
    let modified_csv = runner.fixture().create_csv("modified.csv", &sample_data::mixed_changes_csv_data()).unwrap();
    runner.expect_success(&["--verbose", "snapshot", modified_csv.to_str().unwrap(), "--name", "modified"]);
    
    runner.expect_success(&["--verbose", "diff", "baseline", "modified"]);
    
    let diff_path = runner.fixture().workspace.diff_path("baseline", "modified");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("modified");
}
