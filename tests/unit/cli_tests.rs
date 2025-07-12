//! Unit tests for CLI argument parsing - focused on core functionality

use tabdiff::cli::{Cli, Commands};
use clap::Parser;

#[test]
fn test_cli_snapshot_command_with_full_data() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "snapshot", "data.csv", "--name", "test"
    ]).unwrap();
    
    match cli.command {
        Commands::Snapshot { input, name, batch_size, full_data, hash_only } => {
            assert_eq!(input, "data.csv");
            assert_eq!(name, "test");
            assert_eq!(batch_size, 10000);
            assert!(full_data);
            assert!(!hash_only);
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
        Commands::Rollback { input, to, to_date, dry_run, force, backup } => {
            assert_eq!(input, "data.csv");
            assert_eq!(to, Some("baseline".to_string()));
            assert_eq!(to_date, None);
            assert!(!dry_run);
            assert!(!force);
            assert!(backup);
        }
        _ => panic!("Expected Rollback command"),
    }
}

#[test]
fn test_cli_rollback_command_with_date() {
    let cli = Cli::try_parse_from(&[
        "tabdiff", "rollback", "data.csv", "--to-date", "2025-01-01 15:00:00"
    ]).unwrap();
    
    match cli.command {
        Commands::Rollback { input, to, to_date, dry_run, force, backup } => {
            assert_eq!(input, "data.csv");
            assert_eq!(to, None);
            assert_eq!(to_date, Some("2025-01-01 15:00:00".to_string()));
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
    
    // Missing rollback target (neither --to nor --to-date)
    assert!(Cli::try_parse_from(&["tabdiff", "rollback", "data.csv"]).is_ok()); // CLI parsing succeeds but validation fails in command
    
    // Missing input for status
    assert!(Cli::try_parse_from(&["tabdiff", "status"]).is_err());
}
