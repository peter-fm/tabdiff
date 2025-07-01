//! Functional tests for real-world tabdiff workflows

use crate::common::{CliTestRunner, sample_data, assertions};
use std::fs;

#[test]
fn test_basic_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    // 1. Initialize workspace
    runner.expect_success(&["init"]);
    
    // 2. Create initial snapshot
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "baseline"]);
    
    // 3. List snapshots
    runner.expect_success(&["list"]);
    
    // 4. Show snapshot info
    runner.expect_success(&["show", "baseline"]);
    
    // 5. Create updated data
    let updated_csv = runner.fixture().create_csv("data_v2.csv", &sample_data::updated_csv_data()).unwrap();
    runner.expect_success(&["snapshot", updated_csv.to_str().unwrap(), "--name", "v2"]);
    
    // 6. Compare snapshots
    runner.expect_success(&["diff", "baseline", "v2"]);
    
    // 7. Check status
    runner.expect_success(&["status", updated_csv.to_str().unwrap(), "--compare-to", "baseline"]);
    
    // Verify all snapshots exist
    runner.fixture().assert_snapshot_exists("baseline");
    runner.fixture().assert_snapshot_exists("v2");
}

#[test]
fn test_schema_evolution_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Original schema
    let original_csv = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", original_csv.to_str().unwrap(), "--name", "v1"]);
    
    // Schema change - add column
    let schema_changed_csv = runner.fixture().create_csv("data_v2.csv", &sample_data::schema_changed_csv_data()).unwrap();
    runner.expect_success(&["snapshot", schema_changed_csv.to_str().unwrap(), "--name", "v2"]);
    
    // Compare to detect schema changes
    runner.expect_success(&["diff", "v1", "v2"]);
    
    // Verify diff file was created
    let diff_path = runner.fixture().workspace.diff_path("v1", "v2");
    assertions::assert_file_exists_and_not_empty(&diff_path);
    
    // Check diff content
    let diff_content = fs::read_to_string(&diff_path).unwrap();
    let diff_json: serde_json::Value = serde_json::from_str(&diff_content).unwrap();
    assert_eq!(diff_json["schema_changed"], true);
}

#[test]
fn test_sampling_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Create moderately large dataset (reduced from 10k to 1k rows for speed)
    let large_csv = runner.fixture().create_large_csv("large_data.csv", 1000, 5).unwrap();
    
    // Full snapshot
    runner.expect_success(&[
        "snapshot", large_csv.to_str().unwrap(), 
        "--name", "full", 
        "--sample", "full"
    ]);
    
    // Percentage sampling
    runner.expect_success(&[
        "snapshot", large_csv.to_str().unwrap(), 
        "--name", "sampled_10pct", 
        "--sample", "10%"
    ]);
    
    // Count sampling
    runner.expect_success(&[
        "snapshot", large_csv.to_str().unwrap(), 
        "--name", "sampled_100", 
        "--sample", "100"
    ]);
    
    // Compare different sampling strategies
    runner.expect_success(&["diff", "full", "sampled_10pct"]);
    runner.expect_success(&["diff", "sampled_10pct", "sampled_100"]);
    
    // Status check with sampling
    runner.expect_success(&[
        "status", large_csv.to_str().unwrap(), 
        "--compare-to", "full", 
        "--sample", "5%"
    ]);
}

#[test]
fn test_multiple_format_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Create data in different formats
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    let json_path = runner.fixture().create_json("data.json", &sample_data::simple_json_data()).unwrap();
    
    // Snapshot different formats
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "csv_version"]);
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "json_version"]);
    
    // Compare across formats
    runner.expect_success(&["diff", "csv_version", "json_version"]);
    
    // Show both formats
    runner.expect_success(&["show", "csv_version", "--format", "json"]);
    runner.expect_success(&["show", "json_version", "--format", "pretty"]);
}

#[test]
fn test_versioning_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Create multiple versions
    let versions = vec!["v1.0", "v1.1", "v1.2", "v2.0"];
    for version in &versions {
        runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", version]);
    }
    
    // List all versions
    runner.expect_success(&["list", "--format", "json"]);
    
    // Compare specific versions
    runner.expect_success(&["diff", "v1.0", "v2.0"]);
    runner.expect_success(&["diff", "v1.1", "v1.2"]);
    
    // Show detailed info for latest
    runner.expect_success(&["show", "v2.0", "--detailed"]);
}

#[test]
fn test_ci_cd_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Baseline snapshot (production data)
    let baseline_csv = runner.fixture().create_csv("baseline.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", baseline_csv.to_str().unwrap(), "--name", "production"]);
    
    // New data (from development)
    let dev_csv = runner.fixture().create_csv("dev.csv", &sample_data::updated_csv_data()).unwrap();
    
    // Quick status check (CI pipeline)
    runner.expect_success(&[
        "status", dev_csv.to_str().unwrap(), 
        "--compare-to", "production", 
        "--sample", "1000",
        "--json"
    ]);
    
    // Create development snapshot
    runner.expect_success(&["snapshot", dev_csv.to_str().unwrap(), "--name", "dev_build_123"]);
    
    // Generate diff report
    runner.expect_success(&[
        "diff", "production", "dev_build_123", 
        "--mode", "detailed",
        "--output", "ci_diff_report.json"
    ]);
    
    // Verify report was created
    let report_path = runner.fixture().root().join("ci_diff_report.json");
    assertions::assert_file_exists_and_not_empty(&report_path);
}

#[test]
fn test_data_quality_monitoring_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Daily snapshots
    let dates = vec!["2024-01-01", "2024-01-02", "2024-01-03"];
    for date in &dates {
        let daily_csv = runner.fixture().create_csv(
            &format!("data_{}.csv", date), 
            &sample_data::simple_csv_data()
        ).unwrap();
        
        runner.expect_success(&[
            "snapshot", daily_csv.to_str().unwrap(), 
            "--name", &format!("daily_{}", date)
        ]);
    }
    
    // Compare consecutive days
    runner.expect_success(&["diff", "daily_2024-01-01", "daily_2024-01-02"]);
    runner.expect_success(&["diff", "daily_2024-01-02", "daily_2024-01-03"]);
    
    // Weekly comparison
    runner.expect_success(&["diff", "daily_2024-01-01", "daily_2024-01-03"]);
    
    // List all daily snapshots
    runner.expect_success(&["list", "--format", "json"]);
}

#[test]
fn test_large_dataset_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Create moderately large dataset (reduced from 50k to 1k rows for speed)
    let large_csv = runner.fixture().create_large_csv("large.csv", 1000, 10).unwrap();
    
    // Initial snapshot with sampling
    runner.expect_success(&[
        "snapshot", large_csv.to_str().unwrap(), 
        "--name", "large_baseline", 
        "--sample", "10%",
        "--batch-size", "500"
    ]);
    
    // Quick status checks
    runner.expect_success(&[
        "status", large_csv.to_str().unwrap(), 
        "--compare-to", "large_baseline", 
        "--sample", "5%",
        "--quiet"
    ]);
    
    // Show summary
    runner.expect_success(&["show", "large_baseline"]);
}

#[test]
fn test_error_recovery_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Create valid snapshot
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "valid"]);
    
    // Try to create snapshot with same name (should fail)
    let _error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "valid"]);
    
    // Try to diff with non-existent snapshot (should fail)
    let _error = runner.expect_failure(&["diff", "valid", "nonexistent"]);
    
    // Try to show non-existent snapshot (should fail)
    let _error = runner.expect_failure(&["show", "nonexistent"]);
    
    // Valid operations should still work
    runner.expect_success(&["list"]);
    runner.expect_success(&["show", "valid"]);
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "valid2"]);
}

#[test]
fn test_mixed_data_types_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Create data with mixed types
    let mixed_csv = runner.fixture().create_mixed_types_csv("mixed.csv").unwrap();
    runner.expect_success(&["snapshot", mixed_csv.to_str().unwrap(), "--name", "mixed_baseline"]);
    
    // Create unicode data
    let unicode_csv = runner.fixture().create_unicode_csv("unicode.csv").unwrap();
    runner.expect_success(&["snapshot", unicode_csv.to_str().unwrap(), "--name", "unicode_test"]);
    
    // Compare different data types
    runner.expect_success(&["diff", "mixed_baseline", "unicode_test"]);
    
    // Show detailed info
    runner.expect_success(&["show", "mixed_baseline", "--detailed", "--format", "json"]);
}

#[test]
fn test_workspace_management_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    // Initialize with force
    runner.expect_success(&["init", "--force"]);
    
    // Create several snapshots
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    for i in 1..=5 {
        runner.expect_success(&[
            "snapshot", csv_path.to_str().unwrap(), 
            "--name", &format!("snapshot_{}", i)
        ]);
    }
    
    // List all snapshots
    runner.expect_success(&["list"]);
    
    // Verify workspace structure
    assertions::assert_dir_exists(&runner.fixture().workspace.tabdiff_dir);
    assertions::assert_dir_exists(&runner.fixture().workspace.diffs_dir);
    
    // Check that config exists
    let config_path = runner.fixture().workspace.tabdiff_dir.join("config.json");
    assertions::assert_file_exists_and_not_empty(&config_path);
}

#[test]
fn test_output_format_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "test"]);
    
    // Test different output formats
    runner.expect_success(&["list", "--format", "pretty"]);
    runner.expect_success(&["list", "--format", "json"]);
    
    runner.expect_success(&["show", "test", "--format", "pretty"]);
    runner.expect_success(&["show", "test", "--format", "json"]);
    
    runner.expect_success(&["status", csv_path.to_str().unwrap(), "--json"]);
    runner.expect_success(&["status", csv_path.to_str().unwrap(), "--quiet"]);
}

#[test]
fn test_custom_workspace_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create custom workspace directory
    let custom_workspace = runner.fixture().root().join("custom_workspace");
    fs::create_dir(&custom_workspace).unwrap();
    
    // Initialize with custom workspace
    runner.expect_success(&[
        "--workspace", custom_workspace.to_str().unwrap(), 
        "init"
    ]);
    
    // Create snapshot in custom workspace
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&[
        "--workspace", custom_workspace.to_str().unwrap(),
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "custom_test"
    ]);
    
    // List snapshots in custom workspace
    runner.expect_success(&[
        "--workspace", custom_workspace.to_str().unwrap(),
        "list"
    ]);
    
    // Verify custom workspace structure
    let custom_tabdiff = custom_workspace.join(".tabdiff");
    assertions::assert_dir_exists(&custom_tabdiff);
}

#[test]
fn test_verbose_logging_workflow() {
    let runner = CliTestRunner::new().unwrap();
    
    // All operations with verbose logging
    runner.expect_success(&["--verbose", "init"]);
    
    let csv_path = runner.fixture().create_csv("data.csv", &sample_data::simple_csv_data()).unwrap();
    runner.expect_success(&[
        "--verbose", 
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "verbose_test"
    ]);
    
    runner.expect_success(&["--verbose", "list"]);
    runner.expect_success(&["--verbose", "show", "verbose_test"]);
    
    let updated_csv = runner.fixture().create_csv("data_v2.csv", &sample_data::updated_csv_data()).unwrap();
    runner.expect_success(&[
        "--verbose",
        "snapshot", updated_csv.to_str().unwrap(), 
        "--name", "verbose_test_v2"
    ]);
    
    runner.expect_success(&["--verbose", "diff", "verbose_test", "verbose_test_v2"]);
}
