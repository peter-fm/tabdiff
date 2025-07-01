//! Integration tests for the init command

use crate::common::{CliTestRunner, assertions};

#[test]
fn test_init_command_success() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    // Verify workspace was created
    let fixture = runner.fixture();
    assertions::assert_dir_exists(&fixture.workspace.tabdiff_dir);
    assertions::assert_dir_exists(&fixture.workspace.diffs_dir);
    
    // Verify config file was created
    let config_path = fixture.workspace.tabdiff_dir.join("config.json");
    assertions::assert_file_exists_and_not_empty(&config_path);
    assertions::assert_json_contains_keys(&config_path, &[
        "version", "created", "default_batch_size", "default_sample_size"
    ]).unwrap();
    
    // Verify .gitignore was created
    let gitignore_path = fixture.workspace.root.join(".gitignore");
    assertions::assert_file_exists_and_not_empty(&gitignore_path);
}

#[test]
fn test_init_command_already_exists() {
    let runner = CliTestRunner::new().unwrap();
    
    // First init should succeed
    runner.expect_success(&["init"]);
    
    // Second init without force should still succeed (idempotent)
    runner.expect_success(&["init"]);
}

#[test]
fn test_init_command_with_force() {
    let runner = CliTestRunner::new().unwrap();
    
    // First init
    runner.expect_success(&["init"]);
    
    // Modify config file
    let config_path = runner.fixture().workspace.tabdiff_dir.join("config.json");
    std::fs::write(&config_path, r#"{"modified": true}"#).unwrap();
    
    // Init with force should recreate
    runner.expect_success(&["init", "--force"]);
    
    // Config should be reset
    assertions::assert_json_contains_keys(&config_path, &[
        "version", "created", "default_batch_size", "default_sample_size"
    ]).unwrap();
}

#[test]
fn test_init_preserves_existing_gitignore() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create existing .gitignore
    let gitignore_path = runner.fixture().root().join(".gitignore");
    std::fs::write(&gitignore_path, "# Existing content\n*.log\n").unwrap();
    
    runner.expect_success(&["init"]);
    
    // Should preserve existing content and add tabdiff entries
    let content = std::fs::read_to_string(&gitignore_path).unwrap();
    assert!(content.contains("# Existing content"));
    assert!(content.contains("*.log"));
    assert!(content.contains(".tabdiff/*.tabdiff"));
}

#[test]
fn test_init_with_verbose_flag() {
    let runner = CliTestRunner::new().unwrap();
    
    // Should succeed with verbose flag
    runner.expect_success(&["--verbose", "init"]);
    
    // Verify workspace was created
    assertions::assert_dir_exists(&runner.fixture().workspace.tabdiff_dir);
}

#[test]
fn test_init_with_custom_workspace() {
    let runner = CliTestRunner::new().unwrap();
    let custom_path = runner.fixture().root().join("custom");
    std::fs::create_dir(&custom_path).unwrap();
    
    runner.expect_success(&["--workspace", custom_path.to_str().unwrap(), "init"]);
    
    // Verify workspace was created in custom location
    let custom_tabdiff = custom_path.join(".tabdiff");
    assertions::assert_dir_exists(&custom_tabdiff);
}

#[test]
fn test_init_creates_proper_directory_structure() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let fixture = runner.fixture();
    let tabdiff_dir = &fixture.workspace.tabdiff_dir;
    let diffs_dir = &fixture.workspace.diffs_dir;
    
    // Check main directories
    assert!(tabdiff_dir.exists() && tabdiff_dir.is_dir());
    assert!(diffs_dir.exists() && diffs_dir.is_dir());
    
    // Check that directories are properly nested
    assert_eq!(diffs_dir.parent().unwrap(), tabdiff_dir);
    assert_eq!(tabdiff_dir.parent().unwrap(), fixture.root());
}

#[test]
fn test_init_config_file_format() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let config_path = runner.fixture().workspace.tabdiff_dir.join("config.json");
    let content = std::fs::read_to_string(&config_path).unwrap();
    let config: serde_json::Value = serde_json::from_str(&content).unwrap();
    
    // Verify config structure
    assert_eq!(config["version"], "1.0.0");
    assert!(config["created"].is_string());
    assert_eq!(config["default_batch_size"], 10000);
    assert_eq!(config["default_sample_size"], 1000);
}

#[test]
fn test_init_gitignore_format() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let gitignore_path = runner.fixture().root().join(".gitignore");
    let content = std::fs::read_to_string(&gitignore_path).unwrap();
    
    // Should contain proper gitignore entries
    assert!(content.contains("# Ignore compressed snapshot archives"));
    assert!(content.contains(".tabdiff/*.tabdiff"));
    
    // Should be properly formatted
    assert!(content.ends_with('\n'));
}

#[test]
fn test_init_idempotent_behavior() {
    let runner = CliTestRunner::new().unwrap();
    
    // Run init multiple times
    for _ in 0..3 {
        runner.expect_success(&["init"]);
    }
    
    // Should still have valid workspace
    let fixture = runner.fixture();
    assertions::assert_dir_exists(&fixture.workspace.tabdiff_dir);
    assertions::assert_dir_exists(&fixture.workspace.diffs_dir);
    
    let config_path = fixture.workspace.tabdiff_dir.join("config.json");
    assertions::assert_file_exists_and_not_empty(&config_path);
}

#[test]
fn test_init_with_existing_files() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create some existing files in the directory
    std::fs::write(runner.fixture().root().join("data.csv"), "id,name\n1,test").unwrap();
    std::fs::write(runner.fixture().root().join("README.md"), "# Test Project").unwrap();
    
    runner.expect_success(&["init"]);
    
    // Should not affect existing files
    assert!(runner.fixture().root().join("data.csv").exists());
    assert!(runner.fixture().root().join("README.md").exists());
    
    // Should still create workspace
    assertions::assert_dir_exists(&runner.fixture().workspace.tabdiff_dir);
}

#[test]
fn test_init_permissions() {
    let runner = CliTestRunner::new().unwrap();
    
    runner.expect_success(&["init"]);
    
    let fixture = runner.fixture();
    
    // Test that we can write to created directories
    let test_file = fixture.workspace.tabdiff_dir.join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();
    
    let content = std::fs::read_to_string(&test_file).unwrap();
    assert_eq!(content, "test content");
}
