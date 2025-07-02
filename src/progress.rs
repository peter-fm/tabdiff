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
    estimated_rows: u64,
    show_progress: bool,
    start_time: std::time::Instant,
}

impl ProgressReporter {
    /// Create progress reporter for snapshot creation
    pub fn new_for_snapshot(estimated_rows: u64) -> Self {
        // Only create the first progress bar (schema analysis)
        let schema_pb = create_spinner("Analyzing schema...");

        Self {
            schema_pb: Some(schema_pb),
            rows_pb: None,
            columns_pb: None,
            archive_pb: None,
            estimated_rows,
            show_progress: true,
            start_time: std::time::Instant::now(),
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
            estimated_rows: 0,
            show_progress: true,
            start_time: std::time::Instant::now(),
        }
    }

    /// Create minimal progress reporter (no progress bars)
    pub fn new_minimal() -> Self {
        Self {
            schema_pb: None,
            rows_pb: None,
            columns_pb: None,
            archive_pb: None,
            estimated_rows: 0,
            show_progress: false,
            start_time: std::time::Instant::now(),
        }
    }

    /// Update estimated rows (useful when actual count is known after loading)
    pub fn update_estimated_rows(&mut self, new_count: u64) {
        self.estimated_rows = new_count;
        // If rows progress bar already exists, update its length
        if let Some(pb) = &self.rows_pb {
            pb.set_length(new_count);
        }
    }

    /// Lazily create rows progress bar when needed
    fn ensure_rows_pb(&mut self) {
        if self.show_progress && self.rows_pb.is_none() {
            self.rows_pb = Some(create_progress_bar(self.estimated_rows, "Hashing rows"));
        }
    }

    /// Lazily create columns progress bar when needed
    fn ensure_columns_pb(&mut self) {
        if self.show_progress && self.columns_pb.is_none() {
            self.columns_pb = Some(create_spinner("Computing column hashes..."));
        }
    }

    /// Lazily create archive progress bar when needed
    fn ensure_archive_pb(&mut self) {
        if self.show_progress && self.archive_pb.is_none() {
            self.archive_pb = Some(create_spinner("Creating archive..."));
        }
    }

    /// Finish schema analysis and prepare for row processing
    pub fn finish_schema(&mut self, message: &str) {
        if let Some(pb) = self.schema_pb.take() {
            pb.finish_with_message(message.to_string());
        }
        // Immediately create the rows progress bar for large datasets
        self.ensure_rows_pb();
    }

    /// Update row progress
    pub fn update_rows(&mut self, processed: u64) {
        self.ensure_rows_pb();
        if let Some(pb) = &self.rows_pb {
            pb.set_position(processed);
            // Force immediate flush to terminal for real-time updates
            pb.tick();
            use std::io::Write;
            let _ = std::io::stdout().flush();
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
        self.ensure_columns_pb();
        if let Some(pb) = self.columns_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Update archive progress message without finishing
    pub fn update_archive(&mut self, message: &str) {
        self.ensure_archive_pb();
        if let Some(pb) = &self.archive_pb {
            pb.set_message(message.to_string());
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
        // Ensure all progress bars are cleaned up silently
        if let Some(pb) = self.schema_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.rows_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.columns_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.archive_pb.take() {
            pb.finish_and_clear();
        }
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
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({per_sec}) {eta} {msg}")
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
        // These are now created lazily, so they start as None
        assert!(reporter.rows_pb.is_none());
        assert!(reporter.columns_pb.is_none());
        assert!(reporter.archive_pb.is_none());
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
