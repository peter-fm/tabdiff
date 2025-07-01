//! Progress reporting utilities

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

/// Progress reporter for tabdiff operations
#[derive(Debug)]
pub struct ProgressReporter {
    pub schema_pb: Option<ProgressBar>,
    pub rows_pb: Option<ProgressBar>,
    pub columns_pb: Option<ProgressBar>,
    pub archive_pb: Option<ProgressBar>,
}

impl ProgressReporter {
    /// Create progress reporter for snapshot creation
    pub fn new_for_snapshot(estimated_rows: u64) -> Self {
        let schema_pb = create_spinner("Analyzing schema...");
        let rows_pb = create_progress_bar(estimated_rows, "Hashing rows");
        let columns_pb = create_spinner("Computing column hashes...");
        let archive_pb = create_spinner("Creating archive...");

        Self {
            schema_pb: Some(schema_pb),
            rows_pb: Some(rows_pb),
            columns_pb: Some(columns_pb),
            archive_pb: Some(archive_pb),
        }
    }

    /// Create progress reporter for diff operations
    pub fn new_for_diff() -> Self {
        let schema_pb = create_spinner("Comparing schemas...");
        let rows_pb = create_spinner("Comparing row hashes...");
        let columns_pb = create_spinner("Comparing column hashes...");

        Self {
            schema_pb: Some(schema_pb),
            rows_pb: Some(rows_pb),
            columns_pb: Some(columns_pb),
            archive_pb: None,
        }
    }

    /// Create minimal progress reporter (no progress bars)
    pub fn new_minimal() -> Self {
        Self {
            schema_pb: None,
            rows_pb: None,
            columns_pb: None,
            archive_pb: None,
        }
    }

    /// Finish schema analysis
    pub fn finish_schema(&mut self, message: &str) {
        if let Some(pb) = self.schema_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Update row progress
    pub fn update_rows(&self, processed: u64) {
        if let Some(pb) = &self.rows_pb {
            pb.set_position(processed);
        }
    }

    /// Finish row processing
    pub fn finish_rows(&mut self, message: &str) {
        if let Some(pb) = self.rows_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Finish column processing
    pub fn finish_columns(&mut self, message: &str) {
        if let Some(pb) = self.columns_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Finish archive creation
    pub fn finish_archive(&mut self, message: &str) {
        if let Some(pb) = self.archive_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Finish all progress bars
    pub fn finish_all(&mut self, message: &str) {
        self.finish_schema(message);
        self.finish_rows(message);
        self.finish_columns(message);
        self.finish_archive(message);
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        // Ensure all progress bars are cleaned up
        self.finish_all("Completed");
    }
}

/// Create a spinner progress bar
fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
            .template("{spinner:.green} {msg}")
            .expect("Invalid progress template"),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Create a progress bar with known total
fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
            .expect("Invalid progress template")
            .progress_chars("#>-"),
    );
    pb.set_message(message.to_string());
    pb
}

/// Create a simple progress bar for file operations
pub fn create_file_progress(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes:>7}/{total_bytes:7} {msg}")
            .expect("Invalid progress template")
            .progress_chars("#>-"),
    );
    pb.set_message(message.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_reporter_creation() {
        let reporter = ProgressReporter::new_for_snapshot(1000);
        assert!(reporter.schema_pb.is_some());
        assert!(reporter.rows_pb.is_some());
        assert!(reporter.columns_pb.is_some());
        assert!(reporter.archive_pb.is_some());
    }

    #[test]
    fn test_minimal_progress_reporter() {
        let reporter = ProgressReporter::new_minimal();
        assert!(reporter.schema_pb.is_none());
        assert!(reporter.rows_pb.is_none());
        assert!(reporter.columns_pb.is_none());
        assert!(reporter.archive_pb.is_none());
    }
}
