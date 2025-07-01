//! Edge case tests for filesystem-related scenarios

use crate::common::{CliTestRunner, sample_data};
use std::fs;
use std::os::unix::fs::PermissionsExt;

#[test]
fn test_nonexistent_input_file() {
    let runner = CliTestRunner::new().unwrap();
    
    let error = runner.expect_failure(&["snapshot", "/nonexistent/path/file.csv", "--name", "test"]);
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("no such file") || 
        error_msg.contains("not found") || 
        error_msg.contains("does not exist"),
        "Expected file not found error, got: {}", error
    );
}

#[test]
fn test_empty_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create empty file
    let empty_path = runner.fixture().root().join("empty.csv");
    fs::write(&empty_path, "").unwrap();
    
    // DuckDB actually handles empty files gracefully, so we expect success or a specific error
    let result = runner.run_command(&["snapshot", empty_path.to_str().unwrap(), "--name", "empty"]);
    
    match result {
        Ok(_) => {
            // If it succeeds, verify snapshot was created
            runner.fixture().assert_snapshot_exists("empty");
        }
        Err(error) => {
            // If it fails, should have clear error message about empty file
            assert!(error.to_string().contains("empty") || error.to_string().contains("no data") || error.to_string().contains("schema"));
        }
    }
}

#[test]
fn test_file_with_only_header() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create file with only header
    let header_only_path = runner.fixture().root().join("header_only.csv");
    fs::write(&header_only_path, "id,name,price\n").unwrap();
    
    // DuckDB might handle header-only files gracefully, so we expect success or a specific error
    let result = runner.run_command(&["snapshot", header_only_path.to_str().unwrap(), "--name", "header_only"]);
    
    match result {
        Ok(_) => {
            // If it succeeds, verify snapshot was created
            runner.fixture().assert_snapshot_exists("header_only");
        }
        Err(error) => {
            // If it fails, should have clear error message about no data
            assert!(error.to_string().contains("no data") || error.to_string().contains("empty") || error.to_string().contains("rows"));
        }
    }
}

#[test]
#[cfg(unix)]
fn test_permission_denied_input_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create file and remove read permissions
    let restricted_path = runner.fixture().root().join("restricted.csv");
    runner.fixture().create_csv("restricted.csv", &sample_data::simple_csv_data()).unwrap();
    
    let mut perms = fs::metadata(&restricted_path).unwrap().permissions();
    perms.set_mode(0o000); // No permissions
    fs::set_permissions(&restricted_path, perms).unwrap();
    
    let error = runner.expect_failure(&["snapshot", restricted_path.to_str().unwrap(), "--name", "restricted"]);
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("permission denied") || 
        error_msg.contains("access") || 
        error_msg.contains("forbidden"),
        "Expected permission error, got: {}", error
    );
    
    // Restore permissions for cleanup
    let mut perms = fs::metadata(&restricted_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&restricted_path, perms).unwrap();
}

#[test]
#[cfg(unix)]
fn test_permission_denied_workspace() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create the test file first
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Initialize workspace first so .tabdiff directory exists
    runner.expect_success(&["init"]);
    
    // Remove write permissions from .tabdiff directory specifically
    let tabdiff_dir = runner.fixture().root().join(".tabdiff");
    let mut perms = fs::metadata(&tabdiff_dir).unwrap().permissions();
    perms.set_mode(0o555); // Read and execute only
    fs::set_permissions(&tabdiff_dir, perms).unwrap();
    
    // This should fail because we can't write to the .tabdiff directory
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "test"]);
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("permission denied") || 
        error_msg.contains("access") || 
        error_msg.contains("forbidden"),
        "Expected permission error, got: {}", error
    );
    
    // Restore permissions for cleanup
    let mut perms = fs::metadata(&tabdiff_dir).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&tabdiff_dir, perms).unwrap();
}

#[test]
fn test_very_long_filename() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create file with very long name (but within filesystem limits)
    let long_name = "a".repeat(200) + ".csv";
    let long_path = runner.fixture().root().join(&long_name);
    runner.fixture().create_csv(&long_name, &sample_data::simple_csv_data()).unwrap();
    
    runner.expect_success(&["snapshot", long_path.to_str().unwrap(), "--name", "long_filename"]);
    runner.fixture().assert_snapshot_exists("long_filename");
}

#[test]
fn test_very_long_snapshot_name() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Very long snapshot name
    let long_name = "snapshot_".to_string() + &"a".repeat(200);
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", &long_name]);
    runner.fixture().assert_snapshot_exists(&long_name);
}

#[test]
fn test_special_characters_in_filename() {
    let runner = CliTestRunner::new().unwrap();
    
    let special_files = vec![
        "file with spaces.csv",
        "file-with-dashes.csv",
        "file_with_underscores.csv",
        "file.with.dots.csv",
        "file@with@symbols.csv",
    ];
    
    for filename in special_files {
        let file_path = runner.fixture().create_csv(filename, &sample_data::simple_csv_data()).unwrap();
        let snapshot_name = filename.replace(".csv", "_snapshot");
        
        runner.expect_success(&["snapshot", file_path.to_str().unwrap(), "--name", &snapshot_name]);
        runner.fixture().assert_snapshot_exists(&snapshot_name);
    }
}

#[test]
fn test_unicode_filename() {
    let runner = CliTestRunner::new().unwrap();
    
    let unicode_files = vec![
        "测试文件.csv",
        "файл.csv", 
        "ファイル.csv",
        "café.csv",
    ];
    
    for filename in unicode_files {
        let file_path = runner.fixture().create_csv(filename, &sample_data::simple_csv_data()).unwrap();
        let snapshot_name = filename.replace(".csv", "_snapshot");
        
        runner.expect_success(&["snapshot", file_path.to_str().unwrap(), "--name", &snapshot_name]);
        runner.fixture().assert_snapshot_exists(&snapshot_name);
    }
}

#[test]
fn test_deeply_nested_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create deeply nested directory structure
    let deep_path = runner.fixture().root().join("a/b/c/d/e/f/g");
    fs::create_dir_all(&deep_path).unwrap();
    
    let csv_path = deep_path.join("deep.csv");
    fs::write(&csv_path, "id,name\n1,test\n").unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "deep_nested"]);
    runner.fixture().assert_snapshot_exists("deep_nested");
}

#[test]
fn test_symlink_to_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create original file
    let original_path = runner.fixture().create_csv("original.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Create symlink
    let symlink_path = runner.fixture().root().join("symlink.csv");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&original_path, &symlink_path).unwrap();
        
        runner.expect_success(&["snapshot", symlink_path.to_str().unwrap(), "--name", "symlink_test"]);
        runner.fixture().assert_snapshot_exists("symlink_test");
    }
}

#[test]
fn test_broken_symlink() {
    let runner = CliTestRunner::new().unwrap();
    
    #[cfg(unix)]
    {
        // Create symlink to nonexistent file
        let broken_symlink = runner.fixture().root().join("broken.csv");
        std::os::unix::fs::symlink("/nonexistent/file.csv", &broken_symlink).unwrap();
        
        let error = runner.expect_failure(&["snapshot", broken_symlink.to_str().unwrap(), "--name", "broken"]);
        let error_msg = error.to_string().to_lowercase();
        assert!(
            error_msg.contains("no such file") || 
            error_msg.contains("not found") || 
            error_msg.contains("does not exist"),
            "Expected file not found error, got: {}", error
        );
    }
}

#[test]
fn test_file_removed_before_processing() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("temp.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Verify file exists initially
    assert!(csv_path.exists());
    
    // Remove file before attempting snapshot
    fs::remove_file(&csv_path).unwrap();
    
    // Should fail with file not found error
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "removed"]);
    let error_msg = error.to_string().to_lowercase();
    assert!(error_msg.contains("no such file") || error_msg.contains("not found") || error_msg.contains("does not exist"));
}


#[test]
fn test_concurrent_access_to_same_file() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("concurrent.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Open file for writing to simulate concurrent access
    let _file_handle = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(&csv_path)
        .unwrap();
    
    // Should still be able to read the file for snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "concurrent"]);
    runner.fixture().assert_snapshot_exists("concurrent");
}

#[test]
fn test_case_sensitive_filesystem_issues() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create files with different cases
    let lower_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    let upper_path = runner.fixture().create_csv("TEST.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Both should work (on case-sensitive filesystems they're different files)
    runner.expect_success(&["snapshot", lower_path.to_str().unwrap(), "--name", "lower_case"]);
    runner.expect_success(&["snapshot", upper_path.to_str().unwrap(), "--name", "upper_case"]);
    
    runner.fixture().assert_snapshot_exists("lower_case");
    runner.fixture().assert_snapshot_exists("upper_case");
}

#[test]
fn test_workspace_corruption_recovery() {
    let runner = CliTestRunner::new().unwrap();
    
    // Initialize workspace first
    runner.expect_success(&["init"]);
    
    // Corrupt the workspace config after initialization
    let config_path = runner.fixture().workspace.tabdiff_dir.join("config.json");
    fs::write(&config_path, "invalid json content").unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Should handle corrupted config gracefully - either succeed by recreating config
    // or fail with a clear error message
    let result = runner.run_command(&["snapshot", csv_path.to_str().unwrap(), "--name", "test"]);
    match result {
        Ok(_) => {
            // If it succeeds, it should have recreated the config and created the snapshot
            runner.fixture().assert_snapshot_exists("test");
        }
        Err(error) => {
            // If it fails, should have clear error message about config corruption
            let error_msg = error.to_string().to_lowercase();
            assert!(
                error_msg.contains("json") || 
                error_msg.contains("config") || 
                error_msg.contains("invalid") ||
                error_msg.contains("parse"),
                "Expected JSON/config error, got: {}", error
            );
        }
    }
}

#[test]
fn test_partial_snapshot_cleanup() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Create partial snapshot files manually
    let (archive_path, json_path) = runner.fixture().workspace.snapshot_paths("partial");
    fs::write(&json_path, "{}").unwrap(); // Create JSON but not archive
    
    // Should detect existing snapshot
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "partial"]);
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("already exists") || 
        error_msg.contains("exists") ||
        error_msg.contains("duplicate"),
        "Expected snapshot exists error, got: {}", error
    );
}
