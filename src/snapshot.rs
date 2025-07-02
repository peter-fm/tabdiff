//! Snapshot creation and management

use crate::archive::ArchiveManager;
use crate::cli::SamplingStrategy;
use crate::data::{DataInfo, DataProcessor};
use crate::error::{Result, TabdiffError};
use crate::hash::{ColumnHash, HashComputer, RowHash, SchemaHash};
use crate::progress::ProgressReporter;
use crate::change_detection::ChangeDetectionResult;
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
    // Enhanced snapshot chain fields (with defaults for backward compatibility)
    #[serde(default)]
    pub parent_snapshot: Option<String>,
    #[serde(default)]
    pub sequence_number: u64,
    #[serde(default)]
    pub delta_from_parent: Option<DeltaInfo>,
    #[serde(default)]
    pub can_reconstruct_parent: bool,
}

/// Information about delta changes from parent snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaInfo {
    pub parent_name: String,
    pub changes: ChangeDetectionResult,
    pub compressed_size: u64,
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

    /// Create a snapshot from input file with enhanced chain management
    pub fn create_snapshot(
        &mut self,
        input_path: &Path,
        name: &str,
        sampling: &SamplingStrategy,
        archive_path: &Path,
        json_path: &Path,
        full_data: bool,
    ) -> Result<SnapshotMetadata> {
        self.create_snapshot_with_workspace(input_path, name, sampling, archive_path, json_path, full_data, None)
    }

    /// Create a snapshot with workspace context for chain management
    pub fn create_snapshot_with_workspace(
        &mut self,
        input_path: &Path,
        name: &str,
        sampling: &SamplingStrategy,
        archive_path: &Path,
        json_path: &Path,
        full_data: bool,
        workspace: Option<&crate::workspace::TabdiffWorkspace>,
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

        // Find parent snapshot and compute delta if workspace is provided
        let (parent_snapshot, sequence_number, delta_from_parent) = if let Some(ws) = workspace {
            self.progress.finish_schema("Finding parent snapshot...");
            self.find_parent_and_compute_delta(ws, &data_info, &data_processor)?
        } else {
            (None, 0, None)
        };

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

        // Create archive files (including delta if available)
        self.progress.finish_archive("Creating archive...");
        let archive_files = self.create_archive_files_with_delta(
            &data_info,
            &schema_hash,
            &row_hashes,
            &column_hashes,
            name,
            sampling,
            full_data,
            &delta_from_parent,
        )?;

        // Create compressed archive
        ArchiveManager::create_archive(archive_path, &archive_files)?;
        
        // Get archive size
        let archive_size = std::fs::metadata(archive_path)?.len();

        // Create metadata
        let mut metadata = SnapshotMetadata {
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
            parent_snapshot,
            sequence_number,
            delta_from_parent,
            can_reconstruct_parent: false,
        };

        // Set can_reconstruct_parent flag if this snapshot has a delta
        self.update_current_reconstruct_flag(&mut metadata);

        // Save JSON metadata
        let json_content = serde_json::to_string_pretty(&metadata)?;
        std::fs::write(json_path, json_content)?;

        // Update parent's can_reconstruct_parent flag if we have a delta
        if let (Some(ws), Some(parent_name)) = (workspace, &metadata.parent_snapshot) {
            if metadata.delta_from_parent.is_some() {
                self.update_parent_reconstruct_flag(ws, parent_name)?;
            }
        }

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
        full_data: bool,
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
        // We need to extract the actual row data for comprehensive change detection
        let data_processor = DataProcessor::new()?;
        data_processor.load_file(&data_info.source)?;
        let actual_row_data = data_processor.extract_all_data()?;
        
        let rows_data = serde_json::json!({
            "row_hashes": row_hashes,
            "rows": actual_row_data,
            "sampling": format!("{:?}", sampling)
        });
        files.push((
            "rows.json".to_string(),
            serde_json::to_string_pretty(&rows_data)?.into_bytes(),
        ));

        Ok(files)
    }

    /// Create data.parquet file with full dataset
    fn create_data_parquet(
        &self,
        row_data: &[Vec<String>],
        columns: &[crate::hash::ColumnInfo],
    ) -> Result<Vec<u8>> {
        // For now, serialize as JSON until we implement proper Parquet support
        // TODO: Replace with actual Parquet serialization
        let data_structure = serde_json::json!({
            "format": "parquet_placeholder",
            "columns": columns,
            "rows": row_data
        });
        
        Ok(serde_json::to_vec_pretty(&data_structure)?)
    }

    /// Create delta.parquet file with change operations
    fn create_delta_parquet(&self, delta_info: &DeltaInfo) -> Result<Vec<u8>> {
        // For now, serialize as JSON until we implement proper Parquet support
        // TODO: Replace with actual Parquet serialization for delta operations
        let delta_structure = serde_json::json!({
            "format": "parquet_placeholder",
            "parent_name": delta_info.parent_name,
            "changes": delta_info.changes,
            "compressed_size": delta_info.compressed_size
        });
        
        Ok(serde_json::to_vec_pretty(&delta_structure)?)
    }

    /// Create archive files with delta support
    fn create_archive_files_with_delta(
        &self,
        data_info: &DataInfo,
        schema_hash: &SchemaHash,
        row_hashes: &[RowHash],
        column_hashes: &[ColumnHash],
        name: &str,
        sampling: &SamplingStrategy,
        _full_data: bool,
        delta_from_parent: &Option<DeltaInfo>,
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

        // Create schema.json
        let schema_data = serde_json::json!({
            "hash": schema_hash.hash,
            "columns": schema_hash.columns,
            "column_hashes": column_hashes
        });
        files.push((
            "schema.json".to_string(),
            serde_json::to_string_pretty(&schema_data)?.into_bytes(),
        ));

        // Create rows.json
        let data_processor = DataProcessor::new()?;
        data_processor.load_file(&data_info.source)?;
        let actual_row_data = data_processor.extract_all_data()?;
        
        let rows_data = serde_json::json!({
            "row_hashes": row_hashes,
            "rows": actual_row_data,
            "sampling": format!("{:?}", sampling)
        });
        files.push((
            "rows.json".to_string(),
            serde_json::to_string_pretty(&rows_data)?.into_bytes(),
        ));

        // Create data.parquet with full dataset
        let data_parquet = self.create_data_parquet(&actual_row_data, &data_info.columns)?;
        files.push((
            "data.parquet".to_string(),
            data_parquet,
        ));

        // Create delta.parquet if we have delta information
        if let Some(delta_info) = delta_from_parent {
            let delta_parquet = self.create_delta_parquet(delta_info)?;
            files.push((
                "delta.parquet".to_string(),
                delta_parquet,
            ));
        }

        Ok(files)
    }

    /// Find parent snapshot and compute delta
    fn find_parent_and_compute_delta(
        &self,
        workspace: &crate::workspace::TabdiffWorkspace,
        current_data_info: &DataInfo,
        current_data_processor: &DataProcessor,
    ) -> Result<(Option<String>, u64, Option<DeltaInfo>)> {
        // Build snapshot chain to find the latest snapshot
        let chain = SnapshotChain::build_chain(workspace)?;
        
        if let Some(head_name) = &chain.head {
            // Load parent snapshot data
            let (parent_archive_path, parent_json_path) = workspace.snapshot_paths(head_name);
            
            if parent_json_path.exists() {
                let parent_metadata = SnapshotLoader::load_metadata(&parent_json_path)?;
                
                // Check if parent has archive data for comparison
                if parent_archive_path.exists() {
                    let parent_data = SnapshotLoader::load_full_snapshot(&parent_archive_path)?;
                    
                    // Extract parent schema and row data
                    let parent_schema = self.extract_schema_from_archive(&parent_data)?;
                    let parent_row_data = self.extract_row_data_from_archive(&parent_data)?;
                    
                    // Extract current row data
                    let current_row_data = current_data_processor.extract_all_data()?;
                    
                    // Compute changes from parent to current
                    let changes = crate::change_detection::ChangeDetector::detect_changes(
                        &parent_schema,
                        &parent_row_data,
                        &current_data_info.columns,
                        &current_row_data,
                    )?;
                    
                    // Calculate compressed size (estimate based on JSON serialization)
                    let changes_json = serde_json::to_string(&changes)?;
                    let compressed_size = changes_json.len() as u64;
                    
                    let delta_info = DeltaInfo {
                        parent_name: head_name.clone(),
                        changes,
                        compressed_size,
                    };
                    
                    let sequence_number = parent_metadata.sequence_number + 1;
                    
                    return Ok((Some(head_name.clone()), sequence_number, Some(delta_info)));
                }
                
                // Parent exists but no archive data - still create chain link
                let sequence_number = parent_metadata.sequence_number + 1;
                return Ok((Some(head_name.clone()), sequence_number, None));
            }
        }
        
        // No parent found - this is the first snapshot
        Ok((None, 0, None))
    }

    /// Extract schema from archive data
    fn extract_schema_from_archive(&self, archive_data: &FullSnapshotData) -> Result<Vec<crate::hash::ColumnInfo>> {
        if let Some(schema_data) = archive_data.schema_data.get("columns") {
            if let Some(columns_array) = schema_data.as_array() {
                let mut columns = Vec::new();
                for col_value in columns_array {
                    if let (Some(name), Some(data_type), Some(nullable)) = (
                        col_value.get("name").and_then(|v| v.as_str()),
                        col_value.get("data_type").and_then(|v| v.as_str()),
                        col_value.get("nullable").and_then(|v| v.as_bool())
                    ) {
                        columns.push(crate::hash::ColumnInfo {
                            name: name.to_string(),
                            data_type: data_type.to_string(),
                            nullable,
                        });
                    }
                }
                return Ok(columns);
            }
        }
        Ok(Vec::new())
    }

    /// Extract row data from archive data
    fn extract_row_data_from_archive(&self, archive_data: &FullSnapshotData) -> Result<Vec<Vec<String>>> {
        if let Some(rows_data) = archive_data.row_data.get("rows") {
            if let Some(rows_array) = rows_data.as_array() {
                let mut rows = Vec::new();
                for row_value in rows_array {
                    if let Some(row_array) = row_value.as_array() {
                        let row: Vec<String> = row_array
                            .iter()
                            .map(|v| v.as_str().unwrap_or("").to_string())
                            .collect();
                        rows.push(row);
                    }
                }
                return Ok(rows);
            }
        }
        Ok(Vec::new())
    }

    /// Update parent's can_reconstruct_parent flag
    fn update_parent_reconstruct_flag(
        &self,
        workspace: &crate::workspace::TabdiffWorkspace,
        parent_name: &str,
    ) -> Result<()> {
        // This method name is misleading - we're actually updating the CURRENT snapshot
        // to indicate it can reconstruct its parent, not updating the parent itself
        // The parent doesn't need to know it can be reconstructed - the child does
        Ok(())
    }

    /// Update current snapshot to indicate it can reconstruct its parent
    fn update_current_reconstruct_flag(
        &self,
        current_metadata: &mut SnapshotMetadata,
    ) {
        // If this snapshot has a delta from parent, it can reconstruct the parent
        if current_metadata.delta_from_parent.is_some() {
            current_metadata.can_reconstruct_parent = true;
        }
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
        let mut delta_data = None;

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
                "delta.json" => {
                    let content_str = String::from_utf8(content)?;
                    delta_data = Some(serde_json::from_str(&content_str)?);
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
            delta_data,
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
    pub delta_data: Option<serde_json::Value>,
}

/// Snapshot chain management for tracking relationships between snapshots
#[derive(Debug, Clone)]
pub struct SnapshotChain {
    pub snapshots: Vec<SnapshotMetadata>,
    pub head: Option<String>,
}

impl SnapshotChain {
    /// Build snapshot chain from workspace
    pub fn build_chain(workspace: &crate::workspace::TabdiffWorkspace) -> Result<Self> {
        let snapshot_names = workspace.list_snapshots()?;
        let mut snapshots = Vec::new();
        
        for name in snapshot_names {
            let (_, json_path) = workspace.snapshot_paths(&name);
            if json_path.exists() {
                let metadata = SnapshotLoader::load_metadata(&json_path)?;
                snapshots.push(metadata);
            }
        }
        
        // Sort by sequence number and creation time
        snapshots.sort_by(|a, b| {
            a.sequence_number.cmp(&b.sequence_number)
                .then_with(|| a.created.cmp(&b.created))
        });
        
        // Find head (latest snapshot)
        let head = snapshots.last().map(|s| s.name.clone());
        
        Ok(Self { snapshots, head })
    }
    
    /// Find path from one snapshot to another
    pub fn find_path_to_snapshot(&self, target: &str) -> Option<Vec<String>> {
        // Find target snapshot
        let target_snapshot = self.snapshots.iter().find(|s| s.name == target)?;
        
        // Build path by following parent chain backwards
        let mut path = vec![target.to_string()];
        let mut current = target_snapshot;
        
        while let Some(parent_name) = &current.parent_snapshot {
            path.push(parent_name.clone());
            current = self.snapshots.iter().find(|s| s.name == *parent_name)?;
        }
        
        path.reverse();
        Some(path)
    }
    
    /// Check if a snapshot can be safely deleted (has child that can reconstruct it)
    pub fn can_safely_delete(&self, snapshot: &str) -> bool {
        // Find children of this snapshot
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot && child.can_reconstruct_parent {
                    return true;
                }
            }
        }
        false
    }
    
    /// Get children of a snapshot
    pub fn get_children(&self, snapshot: &str) -> Vec<&SnapshotMetadata> {
        self.snapshots
            .iter()
            .filter(|s| s.parent_snapshot.as_ref() == Some(&snapshot.to_string()))
            .collect()
    }
    
    /// Get parent of a snapshot
    pub fn get_parent(&self, snapshot: &str) -> Option<&SnapshotMetadata> {
        let snapshot_meta = self.snapshots.iter().find(|s| s.name == snapshot)?;
        let parent_name = snapshot_meta.parent_snapshot.as_ref()?;
        self.snapshots.iter().find(|s| s.name == *parent_name)
    }
    
    /// Validate chain integrity
    pub fn validate(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();
        
        for snapshot in &self.snapshots {
            // Check parent exists if specified
            if let Some(parent_name) = &snapshot.parent_snapshot {
                if !self.snapshots.iter().any(|s| s.name == *parent_name) {
                    issues.push(format!("Snapshot '{}' references missing parent '{}'", 
                                      snapshot.name, parent_name));
                }
            }
            
            // Check sequence number consistency
            if let Some(parent) = self.get_parent(&snapshot.name) {
                if snapshot.sequence_number <= parent.sequence_number {
                    issues.push(format!("Snapshot '{}' has invalid sequence number", 
                                      snapshot.name));
                }
            }
        }
        
        Ok(issues)
    }
    
    /// Find snapshots that can be safely deleted using smart chain-aware logic
    pub fn find_safe_deletion_candidates(
        &self,
        keep_full: usize,
        workspace: &crate::workspace::TabdiffWorkspace,
    ) -> Result<Vec<&SnapshotMetadata>> {
        let mut candidates = Vec::new();
        
        // Count total archives
        let mut archives_with_files = Vec::new();
        for snapshot in &self.snapshots {
            let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
            if archive_path.exists() {
                archives_with_files.push(snapshot);
            }
        }
        
        // If we don't have more archives than the minimum, nothing to delete
        if archives_with_files.len() <= keep_full {
            return Ok(candidates);
        }
        
        // Smart deletion strategy:
        // 1. Always keep the head (latest snapshot)
        // 2. Keep snapshots that are needed for reconstruction chains
        // 3. Delete from oldest to newest, but only if safe
        
        let head_name = self.head.as_ref();
        let mut essential_snapshots = std::collections::HashSet::new();
        
        // Mark head as essential
        if let Some(head) = head_name {
            essential_snapshots.insert(head.clone());
        }
        
        // Mark snapshots needed for reconstruction chains as essential
        for snapshot in &self.snapshots {
            if self.is_needed_for_reconstruction(&snapshot.name) {
                essential_snapshots.insert(snapshot.name.clone());
            }
        }
        
        // Find candidates for deletion (oldest first)
        let mut sorted_archives = archives_with_files.clone();
        sorted_archives.sort_by_key(|s| s.sequence_number);
        
        let mut archives_to_keep = archives_with_files.len();
        
        for snapshot in sorted_archives {
            // Don't delete if it's essential
            if essential_snapshots.contains(&snapshot.name) {
                continue;
            }
            
            // Don't delete if it would leave us with too few archives
            if archives_to_keep <= keep_full {
                break;
            }
            
            // Check if this snapshot can be safely deleted
            if self.can_safely_delete(&snapshot.name) {
                candidates.push(snapshot);
                archives_to_keep -= 1;
            }
        }
        
        Ok(candidates)
    }

    /// Find snapshots that can have their full data cleaned up (selective cleanup)
    pub fn find_data_cleanup_candidates(
        &self,
        keep_full: usize,
        workspace: &crate::workspace::TabdiffWorkspace,
    ) -> Result<Vec<&SnapshotMetadata>> {
        let mut candidates = Vec::new();
        
        // Count total archives
        let mut archives_with_files = Vec::new();
        for snapshot in &self.snapshots {
            let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
            if archive_path.exists() {
                archives_with_files.push(snapshot);
            }
        }
        
        // Data cleanup strategy:
        // 1. Always keep full data for the most recent N snapshots (head + keep_full-1)
        // 2. Remove full data from ALL other snapshots but preserve deltas
        // 3. Can reconstruct any snapshot through delta chains from head
        
        // Sort archives by sequence number (newest first, so head is first)
        let mut sorted_archives = archives_with_files.clone();
        sorted_archives.sort_by_key(|s| std::cmp::Reverse(s.sequence_number));
        
        // Keep full data for the most recent keep_full snapshots
        for (index, snapshot) in sorted_archives.iter().enumerate() {
            // Clean up all snapshots except the most recent keep_full
            if index >= keep_full {
                // Check if this snapshot can be reconstructed from the chain
                if self.can_be_reconstructed(&snapshot.name) {
                    candidates.push(*snapshot);
                }
            }
        }
        
        Ok(candidates)
    }
    
    /// Check if a snapshot is needed for reconstruction of other snapshots
    fn is_needed_for_reconstruction(&self, snapshot_name: &str) -> bool {
        // A snapshot is needed if:
        // 1. It's the head (latest)
        // 2. It has children that depend on it for reconstruction
        // 3. It's part of a critical reconstruction path
        
        if let Some(head) = &self.head {
            if head == snapshot_name {
                return true;
            }
        }
        
        // Check if any children need this snapshot for reconstruction
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot_name {
                    // This snapshot has children - check if they can reconstruct it
                    if !child.can_reconstruct_parent {
                        // Child cannot reconstruct this parent, so parent is essential
                        return true;
                    }
                }
            }
        }
        
        false
    }

    /// Check if a snapshot can be reconstructed from the chain
    fn can_be_reconstructed(&self, snapshot_name: &str) -> bool {
        // A snapshot can be reconstructed if:
        // 1. There's a path from the head to this snapshot through deltas
        // 2. OR it has a child that can reconstruct it
        
        // Check if any child can reconstruct this snapshot
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot_name && child.can_reconstruct_parent {
                    return true;
                }
            }
        }
        
        // Check if we can trace a path from head to this snapshot
        if let Some(head) = &self.head {
            if let Some(path) = self.find_path_to_snapshot(snapshot_name) {
                // If there's a path and it's not just the snapshot itself, it can be reconstructed
                return path.len() > 1 || head == snapshot_name;
            }
        }
        
        false
    }
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
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: SnapshotMetadata = serde_json::from_str(&json).unwrap();
        
        assert_eq!(metadata.name, deserialized.name);
        assert_eq!(metadata.row_count, deserialized.row_count);
        assert_eq!(metadata.sequence_number, deserialized.sequence_number);
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
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
        };

        let json_content = serde_json::to_string_pretty(&metadata).unwrap();
        fs::write(&json_path, json_content).unwrap();

        let loaded = SnapshotLoader::load_metadata(&json_path).unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.row_count, 100);
    }
}
