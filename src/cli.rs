//! Command-line interface for tabdiff

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "tabdiff")]
#[command(about = "A snapshot-based structured data diff tool")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    
    /// Override workspace location
    #[arg(long, global = true)]
    pub workspace: Option<PathBuf>,
    
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize tabdiff workspace
    Init {
        /// Force initialization even if workspace exists
        #[arg(long)]
        force: bool,
    },
    
    /// Create a snapshot of structured data
    Snapshot {
        /// Input file or directory path
        input: String,
        
        /// Name for the snapshot
        #[arg(long)]
        name: String,
        
        /// Sampling strategy: "full", "N%" (e.g., "10%"), or "N" (e.g., "1000")
        #[arg(long, default_value = "full")]
        sample: String,
        
        /// Batch size for processing rows (must be > 0)
        #[arg(long, default_value = "10000", value_parser = validate_batch_size)]
        batch_size: usize,
        
        /// Store full data for comprehensive change detection (larger snapshots)
        #[arg(long)]
        full_data: bool,
    },
    
    /// Compare two snapshots
    Diff {
        /// First snapshot name
        snapshot1: String,
        
        /// Second snapshot name
        snapshot2: String,
        
        /// Diff mode: "quick", "detailed", or "auto"
        #[arg(long, default_value = "auto")]
        mode: String,
        
        /// Custom output file for diff results
        #[arg(long)]
        output: Option<PathBuf>,
    },
    
    /// Show snapshot information
    Show {
        /// Snapshot name to display
        snapshot: String,
        
        /// Show detailed information from archive
        #[arg(long)]
        detailed: bool,
        
        /// Output format: "pretty", "json"
        #[arg(long, default_value = "pretty")]
        format: String,
    },
    
    /// Check current data against a snapshot
    Status {
        /// Input file or directory path
        input: String,
        
        /// Snapshot to compare against (defaults to latest)
        #[arg(long)]
        compare_to: Option<String>,
        
        /// Sampling strategy: "full", "N%" (e.g., "10%"), or "N" (e.g., "1000")
        #[arg(long, default_value = "1000")]
        sample: String,
        
        /// Quiet output (machine-readable)
        #[arg(long)]
        quiet: bool,
        
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// List all snapshots
    List {
        /// Output format: "pretty", "json"
        #[arg(long, default_value = "pretty")]
        format: String,
    },
}

/// Parse sampling strategy string into structured format
#[derive(Debug, Clone)]
pub enum SamplingStrategy {
    Full,
    Percentage(f64),  // 0.0 to 1.0
    Count(usize),
}

impl SamplingStrategy {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "full" => Ok(Self::Full),
            s if s.ends_with('%') => {
                let pct_str = &s[..s.len() - 1];
                let pct: f64 = pct_str.parse()
                    .map_err(|_| format!("Invalid percentage: {}", s))?;
                if pct < 0.0 || pct > 100.0 {
                    return Err(format!("Percentage must be between 0 and 100: {}", pct));
                }
                Ok(Self::Percentage(pct / 100.0))
            }
            s => {
                let count: usize = s.parse()
                    .map_err(|_| format!("Invalid count: {}", s))?;
                Ok(Self::Count(count))
            }
        }
    }
}

/// Parse diff mode string
#[derive(Debug, Clone)]
pub enum DiffMode {
    Quick,
    Detailed,
    Auto,
}

impl DiffMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "quick" => Ok(Self::Quick),
            "detailed" => Ok(Self::Detailed),
            "auto" => Ok(Self::Auto),
            _ => Err(format!("Invalid diff mode: {}. Use 'quick', 'detailed', or 'auto'", s)),
        }
    }
}

/// Parse output format string
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Pretty,
    Json,
}

impl OutputFormat {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            _ => Err(format!("Invalid output format: {}. Use 'pretty' or 'json'", s)),
        }
    }
}

/// Validate that batch size is greater than 0
fn validate_batch_size(s: &str) -> Result<usize, String> {
    let batch_size: usize = s.parse()
        .map_err(|_| format!("Invalid batch size: '{}'. Must be a positive integer.", s))?;
    
    if batch_size == 0 {
        return Err("Batch size must be greater than 0".to_string());
    }
    
    Ok(batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampling_strategy_parse() {
        assert!(matches!(SamplingStrategy::parse("full"), Ok(SamplingStrategy::Full)));
        assert!(matches!(SamplingStrategy::parse("10%"), Ok(SamplingStrategy::Percentage(p)) if (p - 0.1).abs() < f64::EPSILON));
        assert!(matches!(SamplingStrategy::parse("1000"), Ok(SamplingStrategy::Count(1000))));
        assert!(SamplingStrategy::parse("invalid").is_err());
        assert!(SamplingStrategy::parse("150%").is_err());
    }

    #[test]
    fn test_diff_mode_parse() {
        assert!(matches!(DiffMode::parse("quick"), Ok(DiffMode::Quick)));
        assert!(matches!(DiffMode::parse("detailed"), Ok(DiffMode::Detailed)));
        assert!(matches!(DiffMode::parse("auto"), Ok(DiffMode::Auto)));
        assert!(DiffMode::parse("invalid").is_err());
    }
}
