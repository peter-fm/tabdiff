//! Unit tests for CLI argument parsing - focused on core functionality

use tabdiff::cli::{Cli, Commands};
use clap::Parser;

#[test]
fn test_cli_snapshot_command_with_full_data() {
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
fn test_cli_status_command_with_json() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "status", "data.csv", "--compare-to", "baseline", "--json"
    ]).unwrap();
    
    match cli.command {
        Commands::Status { input, compare_to, json, .. } => {
            assert_eq!(input, "data.csv");
            assert_eq!(compare_to, Some("baseline".to_string()));
            assert!(json);
        }
        _ => panic!("Expected Status command"),
    }
}

#[test]
fn test_cli_missing_required_args() {
    // Missing snapshot name
    assert!(Cli::try_parse_from(&["tabdiff", "snapshot", "data.csv"]).is_err());
    
    // Missing rollback target
    assert!(Cli::try_parse_from(&["tabdiff", "rollback", "data.csv"]).is_err());
    
    // Missing input for status
    assert!(Cli::try_parse_from(&["tabdiff", "status"]).is_err());
}
