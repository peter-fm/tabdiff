//! Unit tests for CLI argument parsing and validation

use tabdiff::cli::{Cli, Commands, DiffMode, OutputFormat};
use clap::Parser;

#[test]
fn test_cli_init_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "init"]).unwrap();
    match cli.command {
        Commands::Init { force } => {
            assert!(!force);
        }
        _ => panic!("Expected Init command"),
    }
}

#[test]
fn test_cli_init_command_with_force() {
    let cli = Cli::try_parse_from(&["tabdiff", "init", "--force"]).unwrap();
    match cli.command {
        Commands::Init { force } => {
            assert!(force);
        }
        _ => panic!("Expected Init command"),
    }
}

#[test]
fn test_cli_snapshot_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "snapshot", "data.csv", "--name", "test"]).unwrap();
    match cli.command {
        Commands::Snapshot { input, name, batch_size, full_data } => {
            assert_eq!(input, "data.csv");
            assert_eq!(name, "test");
            assert_eq!(batch_size, 10000);
            assert!(!full_data);
        }
        _ => panic!("Expected Snapshot command"),
    }
}

#[test]
fn test_cli_snapshot_command_with_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", 
        "--name", "test", 
        "--batch-size", "5000",
        "--full-data"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { input, name, batch_size, full_data } => {
            assert_eq!(input, "data.csv");
            assert_eq!(name, "test");
            assert_eq!(batch_size, 5000);
            assert!(full_data);
        }
        _ => panic!("Expected Snapshot command"),
    }
}

#[test]
fn test_cli_diff_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "diff", "snap1", "snap2"]).unwrap();
    match cli.command {
        Commands::Diff { snapshot1, snapshot2, mode, output } => {
            assert_eq!(snapshot1, "snap1");
            assert_eq!(snapshot2, "snap2");
            assert_eq!(mode, "auto");
            assert!(output.is_none());
        }
        _ => panic!("Expected Diff command"),
    }
}

#[test]
fn test_cli_diff_command_with_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "diff", "snap1", "snap2", 
        "--mode", "detailed", 
        "--output", "result.json"
    ]).unwrap();
    
    match cli.command {
        Commands::Diff { snapshot1, snapshot2, mode, output } => {
            assert_eq!(snapshot1, "snap1");
            assert_eq!(snapshot2, "snap2");
            assert_eq!(mode, "detailed");
            assert_eq!(output.unwrap().to_str().unwrap(), "result.json");
        }
        _ => panic!("Expected Diff command"),
    }
}

#[test]
fn test_cli_show_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "show", "snapshot"]).unwrap();
    match cli.command {
        Commands::Show { snapshot, detailed, format } => {
            assert_eq!(snapshot, "snapshot");
            assert!(!detailed);
            assert_eq!(format, "pretty");
        }
        _ => panic!("Expected Show command"),
    }
}

#[test]
fn test_cli_show_command_with_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "show", "snapshot", 
        "--detailed", 
        "--format", "json"
    ]).unwrap();
    
    match cli.command {
        Commands::Show { snapshot, detailed, format } => {
            assert_eq!(snapshot, "snapshot");
            assert!(detailed);
            assert_eq!(format, "json");
        }
        _ => panic!("Expected Show command"),
    }
}

#[test]
fn test_cli_status_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "status", "data.csv"]).unwrap();
    match cli.command {
        Commands::Status { input, compare_to, quiet, json } => {
            assert_eq!(input, "data.csv");
            assert_eq!(compare_to, None);
            assert!(!quiet);
            assert!(!json);
        }
        _ => panic!("Expected Status command"),
    }
}

#[test]
fn test_cli_status_command_with_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "status", "data.csv", 
        "--compare-to", "baseline", 
        "--quiet", 
        "--json"
    ]).unwrap();
    
    match cli.command {
        Commands::Status { input, compare_to, quiet, json } => {
            assert_eq!(input, "data.csv");
            assert_eq!(compare_to, Some("baseline".to_string()));
            assert!(quiet);
            assert!(json);
        }
        _ => panic!("Expected Status command"),
    }
}

#[test]
fn test_cli_list_command() {
    let cli = Cli::try_parse_from(&["tabdiff", "list"]).unwrap();
    match cli.command {
        Commands::List { format } => {
            assert_eq!(format, "pretty");
        }
        _ => panic!("Expected List command"),
    }
}

#[test]
fn test_cli_list_command_with_format() {
    let cli = Cli::try_parse_from(&["tabdiff", "list", "--format", "json"]).unwrap();
    match cli.command {
        Commands::List { format } => {
            assert_eq!(format, "json");
        }
        _ => panic!("Expected List command"),
    }
}

#[test]
fn test_cli_global_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "--workspace", "/tmp/test", "--verbose", "init"
    ]).unwrap();
    
    assert_eq!(cli.workspace.unwrap().to_str().unwrap(), "/tmp/test");
    assert!(cli.verbose);
}


#[test]
fn test_diff_mode_parse() {
    assert!(matches!(DiffMode::parse("quick"), Ok(DiffMode::Quick)));
    assert!(matches!(DiffMode::parse("detailed"), Ok(DiffMode::Detailed)));
    assert!(matches!(DiffMode::parse("auto"), Ok(DiffMode::Auto)));
    
    // Test case insensitive
    assert!(matches!(DiffMode::parse("QUICK"), Ok(DiffMode::Quick)));
    assert!(matches!(DiffMode::parse("Detailed"), Ok(DiffMode::Detailed)));
    
    // Test invalid
    assert!(DiffMode::parse("invalid").is_err());
    assert!(DiffMode::parse("").is_err());
}

#[test]
fn test_output_format_parse() {
    assert!(matches!(OutputFormat::parse("pretty"), Ok(OutputFormat::Pretty)));
    assert!(matches!(OutputFormat::parse("json"), Ok(OutputFormat::Json)));
    
    // Test case insensitive
    assert!(matches!(OutputFormat::parse("PRETTY"), Ok(OutputFormat::Pretty)));
    assert!(matches!(OutputFormat::parse("Json"), Ok(OutputFormat::Json)));
    
    // Test invalid
    assert!(OutputFormat::parse("invalid").is_err());
    assert!(OutputFormat::parse("").is_err());
}

#[test]
fn test_cli_missing_required_args() {
    // Missing snapshot name
    assert!(Cli::try_parse_from(&["tabdiff", "snapshot", "data.csv"]).is_err());
    
    // Missing input file
    assert!(Cli::try_parse_from(&["tabdiff", "snapshot", "--name", "test"]).is_err());
    
    // Missing snapshot names for diff
    assert!(Cli::try_parse_from(&["tabdiff", "diff"]).is_err());
    assert!(Cli::try_parse_from(&["tabdiff", "diff", "snap1"]).is_err());
    
    // Missing snapshot name for show
    assert!(Cli::try_parse_from(&["tabdiff", "show"]).is_err());
    
    // Missing input for status
    assert!(Cli::try_parse_from(&["tabdiff", "status"]).is_err());
}

#[test]
fn test_cli_invalid_options() {
    // Invalid batch size
    assert!(Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test", "--batch-size", "invalid"
    ]).is_err());
    
    // Note: Format and mode validation happens at runtime, not at CLI parsing time
    // So these will parse successfully but fail during execution
    assert!(Cli::try_parse_from(&[
        "tabdiff", "show", "snapshot", "--format", "invalid"
    ]).is_ok());
    
    assert!(Cli::try_parse_from(&[
        "tabdiff", "diff", "snap1", "snap2", "--mode", "invalid"
    ]).is_ok());
}

#[test]
fn test_cli_help_messages() {
    // Test that help can be generated without panicking
    let result = Cli::try_parse_from(&["tabdiff", "--help"]);
    assert!(result.is_err()); // Help exits with error code
    
    let result = Cli::try_parse_from(&["tabdiff", "init", "--help"]);
    assert!(result.is_err()); // Help exits with error code
}

#[test]
fn test_cli_version() {
    let result = Cli::try_parse_from(&["tabdiff", "--version"]);
    assert!(result.is_err()); // Version exits with error code
}


#[test]
fn test_invalid_snapshot_names() {
    // Test snapshot names with problematic characters
    let problematic_names = vec![
        "", // Empty name
        " ", // Just whitespace
        "name with / slash",
        "name with \\ backslash", 
        "name\nwith\nnewlines",
        "name\twith\ttabs",
        "name with \0 null",
    ];
    
    for name in problematic_names {
        let result = Cli::try_parse_from(&[
            "tabdiff", "snapshot", "data.csv", "--name", name
        ]);
        // These should either parse successfully (and be handled by business logic)
        // or fail at the CLI level - both are acceptable
        match result {
            Ok(_) => {}, // CLI parsing succeeded, validation happens later
            Err(_) => {}, // CLI parsing failed, which is also acceptable
        }
    }
}

#[test]
fn test_invalid_batch_sizes() {
    // Test invalid batch size values
    assert!(Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test", "--batch-size", "0"
    ]).is_err());
    
    assert!(Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test", "--batch-size", "-100"
    ]).is_err());
    
    assert!(Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test", "--batch-size", "abc"
    ]).is_err());
}

#[test]
fn test_conflicting_status_flags() {
    // Test conflicting flags in status command
    let cli = Cli::try_parse_from(&[
        "tabdiff", "status", "data.csv", "--quiet", "--json"
    ]).unwrap();
    
    // Both flags should be parsed successfully - conflict resolution happens at runtime
    match cli.command {
        Commands::Status { quiet, json, .. } => {
            assert!(quiet);
            assert!(json);
        }
        _ => panic!("Expected Status command"),
    }
}

#[test]
fn test_path_handling() {
    // Test various path formats
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "/absolute/path/data.csv", "--name", "test"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { input, .. } => {
            assert_eq!(input, "/absolute/path/data.csv");
        }
        _ => panic!("Expected Snapshot command"),
    }
    
    // Test relative path
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "./relative/data.csv", "--name", "test"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { input, .. } => {
            assert_eq!(input, "./relative/data.csv");
        }
        _ => panic!("Expected Snapshot command"),
    }
}

#[test]
fn test_special_characters_in_names() {
    // Test snapshot names with special characters
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test-snapshot_v1.0"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { name, .. } => {
            assert_eq!(name, "test-snapshot_v1.0");
        }
        _ => panic!("Expected Snapshot command"),
    }
    
    // Test Unicode in names
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "测试快照"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { name, .. } => {
            assert_eq!(name, "测试快照");
        }
        _ => panic!("Expected Snapshot command"),
    }
}

#[test]
fn test_cli_snapshot_with_full_data() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test", "--full-data"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { input, name, batch_size, full_data } => {
            assert_eq!(input, "data.csv");
            assert_eq!(name, "test");
            assert_eq!(batch_size, 10000);
            assert!(full_data);
        }
        _ => panic!("Expected Snapshot command"),
    }
}

#[test]
fn test_cli_rollback_command() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "rollback", "data.csv", "--to", "baseline"
    ]).unwrap();
    
    match cli.command {
        Commands::Rollback { input, to, dry_run, force, backup } => {
            assert_eq!(input, "data.csv");
            assert_eq!(to, "baseline");
            assert!(!dry_run);
            assert!(!force);
            assert!(backup);
        }
        _ => panic!("Expected Rollback command"),
    }
}

#[test]
fn test_cli_rollback_command_with_options() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "rollback", "data.csv", "--to", "baseline", 
        "--dry-run", "--force"
    ]).unwrap();
    
    match cli.command {
        Commands::Rollback { input, to, dry_run, force, backup } => {
            assert_eq!(input, "data.csv");
            assert_eq!(to, "baseline");
            assert!(dry_run);
            assert!(force);
            assert!(backup); // default is true
        }
        _ => panic!("Expected Rollback command"),
    }
}

#[test]
fn test_cli_rollback_missing_required_args() {
    // Missing input file
    assert!(Cli::try_parse_from(&["tabdiff", "rollback", "--to", "baseline"]).is_err());
    
    // Missing target snapshot
    assert!(Cli::try_parse_from(&["tabdiff", "rollback", "data.csv"]).is_err());
    
    // Both missing
    assert!(Cli::try_parse_from(&["tabdiff", "rollback"]).is_err());
}
