//! Edge case tests for filesystem-related scenarios

use crate::common::{CliTestRunner, sample_data};
use std::fs;
use std::os::unix::fs::PermissionsExt;

#[test]
fn test_nonexistent_input_file() {
    let runner = CliTestRunner::new().unwrap();
    
    let error = runner.expect_failure(&["snapshot", "/nonexistent/path/file.csv", "--name", "test"]);
    assert!(error.to_string().contains("No such file") || error.to_string().contains("not found"));
}

#[test]
fn test_directory_as_input_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create a directory
    let dir_path = runner.fixture().root().join("test_dir");
    fs::create_dir(&dir_path).unwrap();
    
    let error = runner.expect_failure(&["snapshot", dir_path.to_str().unwrap(), "--name", "test"]);
    assert!(error.to_string().contains("directory") || error.to_string().contains("not a file"));
}

#[test]
fn test_empty_file() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create empty file
    let empty_path = runner.fixture().root().join("empty.csv");
    fs::write(&empty_path, "").unwrap();
    
    let error = runner.expect_failure(&["snapshot", empty_path.to_str().unwrap(), "--name", "empty"]);
    assert!(error.to_string().contains("empty") || error.to_string().contains("no data"));
}

#[test]
fn test_file_with_only_header() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create file with only header
    let header_only_path = runner.fixture().root().join("header_only.csv");
    fs::write(&header_only_path, "id,name,price\n").unwrap();
    
    let error = runner.expect_failure(&["snapshot", header_only_path.to_str().unwrap(), "--name", "header_only"]);
    assert!(error.to_string().contains("no data") || error.to_string().contains("empty"));
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
    assert!(error.to_string().contains("Permission denied") || error.to_string().contains("access"));
    
    // Restore permissions for cleanup
    let mut perms = fs::metadata(&restricted_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&restricted_path, perms).unwrap();
}

#[test]
#[cfg(unix)]
fn test_permission_denied_workspace() {
    let runner = CliTestRunner::new().unwrap();
    
    // Remove write permissions from workspace directory
    let mut perms = fs::metadata(runner.fixture().root()).unwrap().permissions();
    perms.set_mode(0o555); // Read and execute only
    fs::set_permissions(runner.fixture().root(), perms).unwrap();
    
    let csv_path = runner.fixture().root().join("test.csv");
    // This will fail because we can't write to the directory
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "test"]);
    assert!(error.to_string().contains("Permission denied") || error.to_string().contains("access"));
    
    // Restore permissions for cleanup
    let mut perms = fs::metadata(runner.fixture().root()).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(runner.fixture().root(), perms).unwrap();
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
        assert!(error.to_string().contains("No such file") || error.to_string().contains("not found"));
    }
}

#[test]
fn test_file_disappears_during_processing() {
    let runner = CliTestRunner::new().unwrap();
    
    let csv_path = runner.fixture().create_csv("disappearing.csv", &sample_data::simple_csv_data()).unwrap();
    
    // This is hard to test reliably, but we can at least verify the file exists initially
    assert!(csv_path.exists());
    
    // Remove file before snapshot (simulating file disappearing)
    fs::remove_file(&csv_path).unwrap();
    
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "disappeared"]);
    assert!(error.to_string().contains("No such file") || error.to_string().contains("not found"));
}

#[test]
fn test_workspace_in_readonly_filesystem() {
    // This test is platform-specific and hard to implement reliably
    // We'll skip it for now but it's an important edge case to consider
}

#[test]
fn test_disk_space_exhaustion() {
    // This test would require creating a very large file or filling up disk
    // We'll skip it for now but it's an important edge case to consider
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
fn test_invalid_utf8_in_filename() {
    // This test is complex to implement cross-platform
    // Different filesystems handle invalid UTF-8 differently
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
fn test_network_filesystem_issues() {
    // This would test NFS, SMB, etc. mounted filesystems
    // Skip for now as it requires specific setup
}

#[test]
fn test_file_locked_by_another_process() {
    // This is platform-specific and hard to test reliably
    // On Unix, files can usually be read even if locked for writing
    // On Windows, file locking is more restrictive
}

#[test]
fn test_workspace_corruption_recovery() {
    let runner = CliTestRunner::new().unwrap();
    
    // Corrupt the workspace config
    let config_path = runner.fixture().workspace.tabdiff_dir.join("config.json");
    fs::write(&config_path, "invalid json content").unwrap();
    
    let csv_path = runner.fixture().create_csv("test.csv", &sample_data::simple_csv_data()).unwrap();
    
    // Should handle corrupted config gracefully
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "test"]);
    assert!(error.to_string().contains("JSON") || error.to_string().contains("config") || error.to_string().contains("invalid"));
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
    assert!(error.to_string().contains("already exists"));
}
