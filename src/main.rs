//! Main entry point for tabdiff CLI

use clap::Parser;
use tabdiff::cli::Cli;
use tabdiff::commands::execute_command;
use tabdiff::duckdb_config;

fn main() {
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Parse command line arguments
    let cli = Cli::parse();

    // Set up verbose logging if requested
    if cli.verbose {
        log::set_max_level(log::LevelFilter::Debug);
    }

    // Initialize and validate DuckDB configuration
    if let Err(e) = duckdb_config::init_duckdb() {
        eprintln!("{}", e);
        std::process::exit(1);
    }

    // Execute the command
    if let Err(e) = execute_command(cli.command, cli.workspace.as_deref()) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
