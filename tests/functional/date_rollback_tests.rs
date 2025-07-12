//! Tests for date-based rollback functionality

use crate::common::CliTestRunner;
use std::fs;

#[test]
fn test_rollback_by_date_basic_functionality() {
    let runner = CliTestRunner::new().unwrap();
    let csv_path = runner.fixture().create_csv_raw("test.csv", "id,value\n1,100").unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "baseline"]);
    
    // Modify file
    fs::write(&csv_path, "id,value\n1,200").unwrap();
    
    // Test rollback with date format (future date should find the baseline snapshot)
    let future_date = chrono::Utc::now() + chrono::Duration::hours(1);
    let date_str = future_date.format("%Y-%m-%d %H:%M:%S").to_string();
    
    runner.expect_success(&["rollback", csv_path.to_str().unwrap(), "--to-date", &date_str, "--force"]);
    
    // Verify rollback worked
    let content = fs::read_to_string(&csv_path).unwrap();
    assert!(content.contains("1,100"), "Should rollback to original state");
}

#[test]
fn test_rollback_by_date_with_date_only_format() {
    let runner = CliTestRunner::new().unwrap();
    let csv_path = runner.fixture().create_csv_raw("test.csv", "id,value\n1,100").unwrap();
    
    // Create snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "baseline"]);
    
    // Modify file
    fs::write(&csv_path, "id,value\n1,200").unwrap();
    
    // Test rollback with date-only format (should default to start of day)
    let tomorrow = chrono::Utc::now() + chrono::Duration::days(1);
    let date_str = tomorrow.format("%Y-%m-%d").to_string();
    
    runner.expect_success(&["rollback", csv_path.to_str().unwrap(), "--to-date", &date_str, "--force"]);
    
    // Verify rollback worked
    let content = fs::read_to_string(&csv_path).unwrap();
    assert!(content.contains("1,100"), "Should rollback to original state");
}

#[test]
fn test_rollback_by_date_no_snapshots_before_date() {
    let runner = CliTestRunner::new().unwrap();
    let csv_path = runner.fixture().create_csv_raw("test.csv", "id,value\n1,100").unwrap();
    
    // Create snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "recent"]);
    
    // Try to rollback to a date in the past (before any snapshots)
    let past_date = "2020-01-01";
    
    // This should fail because no snapshots exist before the target date
    assert!(runner.run_command(&["rollback", csv_path.to_str().unwrap(), "--to-date", past_date, "--force"]).is_err());
}

#[test]
fn test_rollback_by_date_invalid_format() {
    let runner = CliTestRunner::new().unwrap();
    let csv_path = runner.fixture().create_csv_raw("test.csv", "id,value\n1,100").unwrap();
    
    // Try to rollback with invalid date format - should fail
    assert!(runner.run_command(&["rollback", csv_path.to_str().unwrap(), "--to-date", "invalid-date", "--force"]).is_err());
}

#[test]
fn test_rollback_dry_run_with_date() {
    let runner = CliTestRunner::new().unwrap();
    let csv_path = runner.fixture().create_csv_raw("test.csv", "id,name\n1,Alice\n2,Bob").unwrap();
    
    // Create baseline snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "baseline"]);
    
    // Modify file
    fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n3,Charlie").unwrap();
    let modified_content = fs::read_to_string(&csv_path).unwrap();
    
    // Test dry run rollback by date
    let future_date = chrono::Utc::now() + chrono::Duration::hours(1);
    let date_str = future_date.format("%Y-%m-%d %H:%M:%S").to_string();
    
    runner.expect_success(&["rollback", csv_path.to_str().unwrap(), "--to-date", &date_str, "--dry-run"]);
    
    // File should not be modified
    let unchanged_content = fs::read_to_string(&csv_path).unwrap();
    assert_eq!(modified_content, unchanged_content, "File should not be modified in dry run");
}