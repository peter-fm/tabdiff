//! Unit tests for workspace management functionality

use std::fs;
use tempfile::TempDir;
use tabdiff::workspace::{TabdiffWorkspace, WorkspaceStats, CleanupStats};

#[test]
fn test_workspace_creation() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Check that directories were created
    assert!(workspace.tabdiff_dir.exists());
    assert!(workspace.diffs_dir.exists());
    
    // Check that config file was created
    let config_path = workspace.tabdiff_dir.join("config.json");
    assert!(config_path.exists());
    
    // Check that .gitignore was created/updated
    let gitignore_path = workspace.root.join(".gitignore");
    assert!(gitignore_path.exists());
    
    let gitignore_content = fs::read_to_string(&gitignore_path).unwrap();
    assert!(gitignore_content.contains(".tabdiff/*.tabdiff"));
}

#[test]
fn test_workspace_from_root() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    assert_eq!(workspace.root, temp_dir.path());
    assert_eq!(workspace.tabdiff_dir, temp_dir.path().join(".tabdiff"));
    assert_eq!(workspace.diffs_dir, temp_dir.path().join(".tabdiff").join("diffs"));
}

#[test]
fn test_workspace_find_existing() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create workspace in parent directory
    let parent_workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Create subdirectory
    let sub_dir = temp_dir.path().join("subdir");
    fs::create_dir(&sub_dir).unwrap();
    
    // Should find existing workspace when starting from subdirectory
    let found_workspace = TabdiffWorkspace::find_or_create(Some(&sub_dir)).unwrap();
    assert_eq!(found_workspace.root, parent_workspace.root);
}

#[test]
fn test_workspace_find_or_create_new() {
    let temp_dir = TempDir::new().unwrap();
    
    // Should create new workspace when none exists
    let workspace = TabdiffWorkspace::find_or_create(Some(temp_dir.path())).unwrap();
    assert!(workspace.tabdiff_dir.exists());
}

#[test]
fn test_snapshot_paths() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let (archive_path, json_path) = workspace.snapshot_paths("test_snapshot");
    
    assert_eq!(archive_path.file_name().unwrap(), "test_snapshot.tabdiff");
    assert_eq!(json_path.file_name().unwrap(), "test_snapshot.json");
    assert_eq!(archive_path.parent().unwrap(), workspace.tabdiff_dir);
    assert_eq!(json_path.parent().unwrap(), workspace.tabdiff_dir);
}

#[test]
fn test_diff_path() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let diff_path = workspace.diff_path("snap1", "snap2");
    
    assert_eq!(diff_path.file_name().unwrap(), "snap1-snap2.json");
    assert_eq!(diff_path.parent().unwrap(), workspace.diffs_dir);
}

#[test]
fn test_snapshot_exists() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Initially no snapshots exist
    assert!(!workspace.snapshot_exists("test"));
    
    // Create a snapshot JSON file
    let (_, json_path) = workspace.snapshot_paths("test");
    fs::write(&json_path, "{}").unwrap();
    
    // Now it should exist
    assert!(workspace.snapshot_exists("test"));
}

#[test]
fn test_list_snapshots_empty() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let snapshots = workspace.list_snapshots().unwrap();
    // Should be empty when no user snapshots exist (config is not a snapshot)
    assert_eq!(snapshots, Vec::<String>::new());
}

#[test]
fn test_list_snapshots_with_data() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Create some snapshot JSON files
    let names = vec!["snapshot1", "snapshot2", "snapshot3"];
    for name in &names {
        let (_, json_path) = workspace.snapshot_paths(name);
        fs::write(&json_path, "{}").unwrap();
    }
    
    let mut snapshots = workspace.list_snapshots().unwrap();
    snapshots.sort();
    
    // Should only include user snapshots, not config
    let mut expected: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    expected.sort();
    
    assert_eq!(snapshots, expected);
}

#[test]
fn test_list_snapshots_ignores_non_json() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Create JSON file
    let (_, json_path) = workspace.snapshot_paths("valid");
    fs::write(&json_path, "{}").unwrap();
    
    // Create non-JSON files that should be ignored
    fs::write(workspace.tabdiff_dir.join("invalid.txt"), "text").unwrap();
    fs::write(workspace.tabdiff_dir.join("test.tabdiff"), "archive").unwrap();
    
    let mut snapshots = workspace.list_snapshots().unwrap();
    snapshots.sort();
    // Should only include user snapshots, not config or non-JSON files
    assert_eq!(snapshots, vec!["valid"]);
}

#[test]
fn test_latest_snapshot_empty() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let latest = workspace.latest_snapshot().unwrap();
    // With no user snapshots, latest should be None (config files should be filtered out)
    assert!(latest.is_none(), "Latest snapshot should be None when no user snapshots exist");
}

#[test]
fn test_latest_snapshot_single() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let (_, json_path) = workspace.snapshot_paths("test");
    fs::write(&json_path, "{}").unwrap();
    
    let latest = workspace.latest_snapshot().unwrap();
    assert_eq!(latest, Some("test".to_string()));
}

#[test]
fn test_gitignore_creation() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let gitignore_path = workspace.root.join(".gitignore");
    assert!(gitignore_path.exists());
    
    let content = fs::read_to_string(&gitignore_path).unwrap();
    assert!(content.contains("# Ignore compressed snapshot archives"));
    assert!(content.contains(".tabdiff/*.tabdiff"));
}

#[test]
fn test_gitignore_update_existing() {
    let temp_dir = TempDir::new().unwrap();
    let gitignore_path = temp_dir.path().join(".gitignore");
    
    // Create existing .gitignore
    fs::write(&gitignore_path, "# Existing content\n*.log\n").unwrap();
    
    let _workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let content = fs::read_to_string(&gitignore_path).unwrap();
    assert!(content.contains("# Existing content"));
    assert!(content.contains("*.log"));
    assert!(content.contains(".tabdiff/*.tabdiff"));
}

#[test]
fn test_gitignore_no_duplicate_entries() {
    let temp_dir = TempDir::new().unwrap();
    let gitignore_path = temp_dir.path().join(".gitignore");
    
    // Create existing .gitignore with tabdiff entries
    fs::write(&gitignore_path, ".tabdiff/*.tabdiff\n").unwrap();
    
    let _workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let content = fs::read_to_string(&gitignore_path).unwrap();
    let count = content.matches(".tabdiff/*.tabdiff").count();
    assert_eq!(count, 1, "Should not duplicate gitignore entries");
}

#[test]
fn test_workspace_stats_empty() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let stats = workspace.stats().unwrap();
    // No user snapshots exist, only config file (which is not counted as a snapshot)
    assert_eq!(stats.snapshot_count, 0);
    assert_eq!(stats.diff_count, 0);
    assert_eq!(stats.total_archive_size, 0);
    assert_eq!(stats.total_json_size, 0); // Only user snapshots are counted
    assert_eq!(stats.total_diff_size, 0);
}

#[test]
fn test_workspace_stats_with_data() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Create snapshot files
    let (archive_path, json_path) = workspace.snapshot_paths("test");
    fs::write(&archive_path, "archive_content").unwrap();
    fs::write(&json_path, "{}").unwrap();
    
    // Create diff file
    let diff_path = workspace.diff_path("snap1", "snap2");
    fs::create_dir_all(diff_path.parent().unwrap()).unwrap();
    fs::write(&diff_path, "diff_content").unwrap();
    
    let stats = workspace.stats().unwrap();
    // Should count only user snapshots (test snapshot = 1, config is not counted)
    assert_eq!(stats.snapshot_count, 1);
    assert_eq!(stats.diff_count, 1);
    assert!(stats.total_archive_size > 0);
    assert!(stats.total_json_size > 0);
    assert!(stats.total_diff_size > 0);
}

#[test]
fn test_cleanup_no_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let stats = workspace.cleanup(5).unwrap();
    assert_eq!(stats.archives_removed, 0);
    assert_eq!(stats.bytes_freed, 0);
}

#[test]
fn test_cleanup_keep_all() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Create 3 snapshots
    for i in 1..=3 {
        let (archive_path, json_path) = workspace.snapshot_paths(&format!("snap{}", i));
        fs::write(&archive_path, format!("archive{}", i)).unwrap();
        fs::write(&json_path, "{}").unwrap();
    }
    
    // Keep 5 (more than we have)
    let stats = workspace.cleanup(5).unwrap();
    assert_eq!(stats.archives_removed, 0);
    assert_eq!(stats.bytes_freed, 0);
}

#[test]
fn test_config_file_creation() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let config_path = workspace.tabdiff_dir.join("config.json");
    assert!(config_path.exists());
    
    let content = fs::read_to_string(&config_path).unwrap();
    let config: serde_json::Value = serde_json::from_str(&content).unwrap();
    
    assert!(config.get("version").is_some());
    assert!(config.get("created").is_some());
    assert!(config.get("default_batch_size").is_some());
    assert!(config.get("default_sample_size").is_some());
}

#[test]
fn test_config_file_not_overwritten() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let config_path = workspace.tabdiff_dir.join("config.json");
    
    // Modify config
    fs::write(&config_path, r#"{"custom": "value"}"#).unwrap();
    
    // Create workspace again (should not overwrite)
    let _workspace2 = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let content = fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("custom"));
    assert!(content.contains("value"));
}

#[test]
fn test_workspace_with_git_directory() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create .git directory
    let git_dir = temp_dir.path().join(".git");
    fs::create_dir(&git_dir).unwrap();
    
    // Create subdirectory
    let sub_dir = temp_dir.path().join("subdir");
    fs::create_dir(&sub_dir).unwrap();
    
    // The current implementation creates workspace in the specified directory,
    // not necessarily at the git root. This test verifies the actual behavior.
    let workspace = TabdiffWorkspace::find_or_create(Some(&sub_dir)).unwrap();
    // Workspace should be created in the subdirectory since no existing workspace was found
    assert_eq!(workspace.root, sub_dir);
}

#[test]
fn test_special_characters_in_snapshot_names() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    let special_names = vec![
        "test-snapshot",
        "test_snapshot",
        "test.snapshot",
        "test snapshot", // Space
        "测试快照",      // Unicode
    ];
    
    for name in special_names {
        let (archive_path, json_path) = workspace.snapshot_paths(name);
        
        // Should be able to create paths
        assert!(archive_path.to_str().is_some());
        assert!(json_path.to_str().is_some());
        
        // Should contain the name
        assert!(archive_path.to_str().unwrap().contains(name));
        assert!(json_path.to_str().unwrap().contains(name));
    }
}

#[test]
fn test_workspace_error_handling() {
    // Test with invalid path
    let invalid_path = std::path::PathBuf::from("/invalid/nonexistent/path");
    let result = TabdiffWorkspace::create_new(invalid_path);
    assert!(result.is_err());
}

#[test]
fn test_workspace_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Test that we can read/write to workspace directories
    let test_file = workspace.tabdiff_dir.join("test.txt");
    fs::write(&test_file, "test content").unwrap();
    
    let content = fs::read_to_string(&test_file).unwrap();
    assert_eq!(content, "test content");
}

#[test]
fn test_workspace_directory_structure() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
    
    // Check directory structure
    assert!(workspace.root.is_dir());
    assert!(workspace.tabdiff_dir.is_dir());
    assert!(workspace.diffs_dir.is_dir());
    
    // Check relative paths
    assert_eq!(workspace.tabdiff_dir, workspace.root.join(".tabdiff"));
    assert_eq!(workspace.diffs_dir, workspace.tabdiff_dir.join("diffs"));
}

#[test]
fn test_workspace_stats_default() {
    let stats = WorkspaceStats::default();
    assert_eq!(stats.snapshot_count, 0);
    assert_eq!(stats.diff_count, 0);
    assert_eq!(stats.total_archive_size, 0);
    assert_eq!(stats.total_json_size, 0);
    assert_eq!(stats.total_diff_size, 0);
}

#[test]
fn test_cleanup_stats_default() {
    let stats = CleanupStats::default();
    assert_eq!(stats.archives_removed, 0);
    assert_eq!(stats.bytes_freed, 0);
}
