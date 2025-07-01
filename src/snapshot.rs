//! Snapshot creation and management

use crate::archive::ArchiveManager;
use crate::cli::SamplingStrategy;
use crate::data::{DataInfo, DataProcessor};
use crate::error::{Result, TabdiffError};
use crate::hash::{ColumnHash, HashComputer, RowHash, SchemaHash};
use crate::progress::ProgressReporter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Snapshot metadata stored in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub format_version: String,
    pub name: String,
    pub created: DateTime<Utc>,
    pub source: String,
    pub source_hash: String,
    pub row_count: u64,
    pub column_count: usize,
    pub schema_hash: String,
    pub columns: HashMap<String, String>,
    pub sampling: SamplingInfo,
    pub archive_size: Option<u64>,
    pub has_full_data: bool,
}

/// Sampling information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingInfo {
    pub strategy: String,
    pub rows_hashed: u64,
    pub total_rows: u64,
}

/// Snapshot creator
pub struct SnapshotCreator {
    hash_computer: HashComputer,
    progress: ProgressReporter,
}

impl SnapshotCreator {
    pub fn new(batch_size: usize, show_progress: bool) -> Self {
        let hash_computer = HashComputer::new(batch_size);
        let progress = if show_progress {
            ProgressReporter::new_for_snapshot(0) // Will update with actual count
        } else {
            ProgressReporter::new_minimal()
        };

        Self {
            hash_computer,
            progress,
        }
    }

    /// Create a snapshot from input file
    pub fn create_snapshot(
        &mut self,
        input_path: &Path,
        name: &str,
        sampling: &SamplingStrategy,
        archive_path: &Path,
        json_path: &Path,
    ) -> Result<SnapshotMetadata> {
        // Load data
        let data_processor = DataProcessor::new()?;
        
        // Only check format for files, not directories (which can contain supported files)
        if input_path.is_file() && !DataProcessor::is_supported_format(input_path) {
            return Err(TabdiffError::invalid_input(format!(
                "Unsupported file format: {}",
                input_path.display()
            )));
        }

        self.progress.finish_schema("Loading data...");
        let data_info = data_processor.load_file(input_path)?;
        
        // Update progress with actual row count
        if let Some(pb) = &self.progress.rows_pb {
            pb.set_length(data_info.row_count);
        }

        // Compute schema hash
        self.progress.finish_schema("Computing schema hash...");
        let schema_hash = self.hash_computer.hash_schema(&data_info.columns)?;

        // Extract data for hashing
        let row_data = data_processor.extract_all_data()?;
        let column_data = data_processor.extract_column_data()?;

        // Compute row hashes
        self.progress.finish_schema("Computing row hashes...");
        let row_hashes = self.hash_computer.hash_rows(&row_data, sampling)?;
        self.progress.finish_rows(&format!("Hashed {} rows", row_hashes.len()));

        // Compute column hashes
        self.progress.finish_columns("Computing column hashes...");
        let column_hashes = self.hash_computer.hash_columns(&column_data)?;
        self.progress.finish_columns(&format!("Hashed {} columns", column_hashes.len()));

        // Create archive files
        self.progress.finish_archive("Creating archive...");
        let archive_files = self.create_archive_files(
            &data_info,
            &schema_hash,
            &row_hashes,
            &column_hashes,
            name,
            sampling,
        )?;

        // Create compressed archive
        ArchiveManager::create_archive(archive_path, &archive_files)?;
        
        // Get archive size
        let archive_size = std::fs::metadata(archive_path)?.len();

        // Create metadata
        let metadata = SnapshotMetadata {
            format_version: crate::FORMAT_VERSION.to_string(),
            name: name.to_string(),
            created: Utc::now(),
            source: input_path.to_string_lossy().to_string(),
            source_hash: self.hash_computer.hash_value(&std::fs::read_to_string(input_path).unwrap_or_default()),
            row_count: data_info.row_count,
            column_count: data_info.column_count(),
            schema_hash: schema_hash.hash.clone(),
            columns: column_hashes
                .iter()
                .map(|ch| (ch.column_name.clone(), ch.hash.clone()))
                .collect(),
            sampling: SamplingInfo {
                strategy: format!("{:?}", sampling),
                rows_hashed: row_hashes.len() as u64,
                total_rows: data_info.row_count,
            },
            archive_size: Some(archive_size),
            has_full_data: true,
        };

        // Save JSON metadata
        let json_content = serde_json::to_string_pretty(&metadata)?;
        std::fs::write(json_path, json_content)?;

        self.progress.finish_archive("Snapshot created successfully");

        Ok(metadata)
    }

    /// Create files for the archive
    fn create_archive_files(
        &self,
        data_info: &DataInfo,
        schema_hash: &SchemaHash,
        row_hashes: &[RowHash],
        column_hashes: &[ColumnHash],
        name: &str,
        sampling: &SamplingStrategy,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();

        // Create metadata.json
        let metadata = serde_json::json!({
            "name": name,
            "created": Utc::now(),
            "source": data_info.source.to_string_lossy(),
            "row_count": data_info.row_count,
            "column_count": data_info.column_count(),
            "schema_hash": schema_hash.hash,
            "sampling": {
                "strategy": format!("{:?}", sampling),
                "rows_hashed": row_hashes.len(),
                "total_rows": data_info.row_count
            }
        });
        files.push((
            "metadata.json".to_string(),
            serde_json::to_string_pretty(&metadata)?.into_bytes(),
        ));

        // Create schema.json (simplified for now - would use Parquet in full implementation)
        let schema_data = serde_json::json!({
            "hash": schema_hash.hash,
            "columns": schema_hash.columns,
            "column_hashes": column_hashes
        });
        files.push((
            "schema.json".to_string(),
            serde_json::to_string_pretty(&schema_data)?.into_bytes(),
        ));

        // Create rows.json (simplified for now - would use Parquet in full implementation)
        let rows_data = serde_json::json!({
            "row_hashes": row_hashes,
            "sampling": format!("{:?}", sampling)
        });
        files.push((
            "rows.json".to_string(),
            serde_json::to_string_pretty(&rows_data)?.into_bytes(),
        ));

        Ok(files)
    }
}

/// Snapshot loader for reading existing snapshots
pub struct SnapshotLoader;

impl SnapshotLoader {
    /// Load snapshot metadata from JSON file
    pub fn load_metadata<P: AsRef<Path>>(json_path: P) -> Result<SnapshotMetadata> {
        let content = std::fs::read_to_string(json_path)?;
        let metadata: SnapshotMetadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    /// Load full snapshot data from archive
    pub fn load_full_snapshot<P: AsRef<Path>>(
        archive_path: P,
    ) -> Result<FullSnapshotData> {
        let files = ArchiveManager::extract_archive(archive_path)?;
        
        let mut metadata = None;
        let mut schema_data = None;
        let mut row_data = None;

        for (filename, content) in files {
            match filename.as_str() {
                "metadata.json" => {
                    let content_str = String::from_utf8(content)?;
                    metadata = Some(serde_json::from_str(&content_str)?);
                }
                "schema.json" => {
                    let content_str = String::from_utf8(content)?;
                    schema_data = Some(serde_json::from_str(&content_str)?);
                }
                "rows.json" => {
                    let content_str = String::from_utf8(content)?;
                    row_data = Some(serde_json::from_str(&content_str)?);
                }
                _ => {
                    // Unknown file, skip
                }
            }
        }

        Ok(FullSnapshotData {
            metadata: metadata.ok_or_else(|| TabdiffError::archive("Missing metadata.json"))?,
            schema_data: schema_data.ok_or_else(|| TabdiffError::archive("Missing schema.json"))?,
            row_data: row_data.ok_or_else(|| TabdiffError::archive("Missing rows.json"))?,
        })
    }

    /// Check if snapshot has full archive data
    pub fn has_archive<P: AsRef<Path>>(archive_path: P) -> bool {
        archive_path.as_ref().exists()
    }
}

/// Full snapshot data loaded from archive
#[derive(Debug)]
pub struct FullSnapshotData {
    pub metadata: serde_json::Value,
    pub schema_data: serde_json::Value,
    pub row_data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_snapshot_metadata_serialization() {
        let metadata = SnapshotMetadata {
            format_version: "1.0.0".to_string(),
            name: "test".to_string(),
            created: Utc::now(),
            source: "test.csv".to_string(),
            source_hash: "abc123".to_string(),
            row_count: 100,
            column_count: 3,
            schema_hash: "def456".to_string(),
            columns: HashMap::new(),
            sampling: SamplingInfo {
                strategy: "Full".to_string(),
                rows_hashed: 100,
                total_rows: 100,
            },
            archive_size: Some(1024),
            has_full_data: true,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: SnapshotMetadata = serde_json::from_str(&json).unwrap();
        
        assert_eq!(metadata.name, deserialized.name);
        assert_eq!(metadata.row_count, deserialized.row_count);
    }

    #[test]
    fn test_snapshot_loader() {
        let temp_dir = TempDir::new().unwrap();
        let json_path = temp_dir.path().join("test.json");
        
        let metadata = SnapshotMetadata {
            format_version: "1.0.0".to_string(),
            name: "test".to_string(),
            created: Utc::now(),
            source: "test.csv".to_string(),
            source_hash: "abc123".to_string(),
            row_count: 100,
            column_count: 3,
            schema_hash: "def456".to_string(),
            columns: HashMap::new(),
            sampling: SamplingInfo {
                strategy: "Full".to_string(),
                rows_hashed: 100,
                total_rows: 100,
            },
            archive_size: Some(1024),
            has_full_data: true,
        };

        let json_content = serde_json::to_string_pretty(&metadata).unwrap();
        fs::write(&json_path, json_content).unwrap();

        let loaded = SnapshotLoader::load_metadata(&json_path).unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.row_count, 100);
    }
}
