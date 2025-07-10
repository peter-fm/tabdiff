//! # tabdiff
//! 
//! A snapshot-based structured data diff tool for detecting schema, column-level,
//! and row-level changes between versions of structured datasets.

pub mod cli;
pub mod error;
pub mod workspace;
pub mod resolver;
pub mod hash;
pub mod data;
pub mod duckdb_config;
pub mod snapshot;
pub mod archive;
pub mod commands;
pub mod output;
pub mod progress;
pub mod git;
pub mod change_detection;
pub mod sql;

pub use error::{Result, TabdiffError};
pub use workspace::TabdiffWorkspace;
pub use resolver::SnapshotResolver;

/// Current format version for tabdiff files
pub const FORMAT_VERSION: &str = "1.0.0";

/// Default batch size for processing rows
pub const DEFAULT_BATCH_SIZE: usize = 10000;

/// Default sample size for status checks
pub const DEFAULT_SAMPLE_SIZE: usize = 1000;
