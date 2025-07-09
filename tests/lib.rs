//! Test library for tabdiff
//! 
//! This module provides common test utilities and organizes all test modules.

pub mod common;

// Unit tests
pub mod unit {
    pub mod cli_tests;
    pub mod workspace_tests;
}

// Integration tests
pub mod integration {
    pub mod init_tests;
    pub mod snapshot_tests;
}

// Edge case tests
pub mod edge_cases {
    pub mod filesystem_tests;
    pub mod data_edge_cases;
}

// Functional tests
pub mod functional {
    pub mod basic_functionality_tests;
    pub mod core_validation_tests;
}

// Re-export common utilities for easy access
pub use common::*;
