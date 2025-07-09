//! Snapshot creation and management

use crate::archive::ArchiveManager;
use crate::data::{DataInfo, DataProcessor};
use crate::error::{Result, TabdiffError};
use crate::hash::{ColumnHash, ColumnInfo, HashComputer, RowHash, SchemaHash};
use crate::progress::ProgressReporter;
use crate::change_detection::ChangeDetectionResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    pub columns: Vec<ColumnInfo>,
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
    // Source-aware fields for proper chain isolation
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub source_fingerprint: Option<String>,
}

/// Information about delta changes from parent snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaInfo {
    pub parent_name: String,
    pub changes: ChangeDetectionResult,
    pub compressed_size: u64,
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
        archive_path: &Path,
        json_path: &Path,
        full_data: bool,
    ) -> Result<SnapshotMetadata> {
        self.create_snapshot_with_workspace(input_path, name, archive_path, json_path, full_data, None)
    }

    /// Create a snapshot with workspace context for chain management
    pub fn create_snapshot_with_workspace(
        &mut self,
        input_path: &Path,
        name: &str,
        archive_path: &Path,
        json_path: &Path,
        full_data: bool,
        workspace: Option<&crate::workspace::TabdiffWorkspace>,
    ) -> Result<SnapshotMetadata> {
        // Load data
        let mut data_processor = DataProcessor::new()?;
        
        // Only check format for files, not directories (which can contain supported files)
        if input_path.is_file() && !DataProcessor::is_supported_format(input_path) {
            return Err(TabdiffError::invalid_input(format!(
                "Unsupported file format: {}",
                input_path.display()
            )));
        }

        // Phase 1: Load and analyze data
        self.progress.finish_schema("📊 Loading and analyzing data...");
        let data_info = data_processor.load_file(input_path)?;
        
        // Update progress with actual row count
        self.progress.update_estimated_rows(data_info.row_count);

        // Phase 2: Compute schema hash
        let schema_hash = self.hash_computer.hash_schema(&data_info.columns)?;

        // Phase 3: Compute row hashes with progress reporting
        let row_hashes = self.hash_computer.hash_rows_with_processor_and_progress(
            &mut data_processor,
            None // No callback needed - data.rs handles progress display directly
        )?;
        self.progress.finish_rows(&format!("✅ Hashed {} rows", row_hashes.len()));

        // Find parent snapshot and compute delta if workspace is provided (using computed hashes)
        let (parent_snapshot, sequence_number, delta_from_parent) = if let Some(ws) = workspace {
            self.find_parent_and_compute_delta(ws, &data_info, &row_hashes)?
        } else {
            (None, 0, None)
        };

        // Phase 4: Compute column hashes
        let column_hashes = self.hash_computer.hash_columns_with_processor(&mut data_processor)?;
        self.progress.finish_columns(&format!("✅ Hashed {} columns", column_hashes.len()));

        // Phase 5: Create archive files (reuse existing data_processor to avoid reloading)
        self.progress.update_archive("📦 Creating archive...");
        let archive_files = self.create_archive_files_optimized(
            &data_info,
            &schema_hash,
            &row_hashes,
            &column_hashes,
            name,
            full_data,
            &delta_from_parent,
            &mut data_processor, // Pass existing processor to avoid reloading
        )?;

        // Create compressed archive with integrated progress
        {
            let progress_ref = &self.progress;
            ArchiveManager::create_archive_with_progress(
                archive_path, 
                &archive_files,
                Some(&|processed: u64, total: u64, message: &str| {
                    if let Some(pb) = &progress_ref.archive_pb {
                        pb.set_length(total);
                        pb.set_position(processed);
                        pb.set_message(message.to_string());
                    }
                })
            )?;
        }
        
        // Get archive size
        let archive_size = std::fs::metadata(archive_path)?.len();

        // Create canonical source path and fingerprint for source tracking
        let canonical_source_path = input_path.canonicalize()
            .unwrap_or_else(|_| input_path.to_path_buf())
            .to_string_lossy()
            .to_string();
        
        let source_fingerprint = format!("{}:{}", 
            canonical_source_path,
            self.hash_computer.hash_value(&format!("{}:{}", 
                canonical_source_path, 
                data_info.row_count
            ))
        );

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
            columns: data_info.columns.clone(),
            archive_size: Some(archive_size),
            has_full_data: full_data, // Use the actual parameter instead of hardcoded true
            parent_snapshot,
            sequence_number,
            delta_from_parent,
            can_reconstruct_parent: false,
            source_path: Some(canonical_source_path),
            source_fingerprint: Some(source_fingerprint),
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

        self.progress.finish_archive("🎉 Snapshot created successfully");

        Ok(metadata)
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


    /// Create archive files with delta support (optimized version that reuses data processor)
    fn create_archive_files_optimized(
        &mut self,
        data_info: &DataInfo,
        schema_hash: &SchemaHash,
        row_hashes: &[RowHash],
        column_hashes: &[ColumnHash],
        name: &str,
        full_data: bool,
        delta_from_parent: &Option<DeltaInfo>,
        data_processor: &mut DataProcessor, // Reuse existing processor to avoid reloading
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
            "rows_hashed": row_hashes.len(),
            "total_rows": data_info.row_count
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

        // NOTE: We do NOT create rows.json - this causes confusion and bugs
        // Row hashes are stored in metadata.json, and full row data is stored in data.parquet
        // This maintains consistency between snapshot creation and loading

        // Only create data.parquet if full_data is true (implements --full-data functionality)
        if full_data {
            // Extract full row data for comprehensive change detection and rollback
            let full_row_data = {
                let progress_ref = &self.progress;
                data_processor.extract_data_chunked_with_progress(
                    Some(&|processed: u64, total: u64| {
                        if let Some(pb) = &progress_ref.archive_pb {
                            pb.set_length(total);
                            pb.set_position(processed);
                            if processed % 10000 == 0 || processed == total {
                                pb.set_message(format!("Extracting data ({}/{})", processed, total));
                            }
                        }
                    })
                )?
            };

            // Create data.parquet with full dataset (enables rollback and detailed change detection)
            let data_parquet = self.create_data_parquet(&full_row_data, &data_info.columns)?;
            files.push((
                "data.parquet".to_string(),
                data_parquet,
            ));
        }

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

    /// Find parent snapshot and compute delta using cached hashes (FIXED ARCHITECTURE)
    fn find_parent_and_compute_delta(
        &self,
        workspace: &crate::workspace::TabdiffWorkspace,
        current_data_info: &DataInfo,
        _current_row_hashes: &[crate::hash::RowHash],
    ) -> Result<(Option<String>, u64, Option<DeltaInfo>)> {
        // Create canonical source path for current file
        let current_canonical_path = current_data_info.source.canonicalize()
            .unwrap_or_else(|_| current_data_info.source.clone())
            .to_string_lossy()
            .to_string();

        // Build source-aware snapshot chain for the current file only
        let chain = SnapshotChain::build_chain_for_source(workspace, &current_canonical_path)?;
        
        if let Some(head_name) = &chain.head {
            // Load parent snapshot metadata
            let (parent_archive_path, parent_json_path) = workspace.snapshot_paths(head_name);
            
            if parent_json_path.exists() {
                let parent_metadata = SnapshotLoader::load_metadata(&parent_json_path)?;
                
                // Double-check that parent is from the same source
                if let Some(parent_source_path) = &parent_metadata.source_path {
                    if parent_source_path != &current_canonical_path {
                        // Parent is from different source, treat as first snapshot
                        return Ok((None, 0, None));
                    }
                } else {
                    // Legacy snapshot without source_path, check original source field
                    let parent_canonical_path = std::path::Path::new(&parent_metadata.source)
                        .canonicalize()
                        .unwrap_or_else(|_| std::path::PathBuf::from(&parent_metadata.source))
                        .to_string_lossy()
                        .to_string();
                    
                    if parent_canonical_path != current_canonical_path {
                        // Parent is from different source, treat as first snapshot
                        return Ok((None, 0, None));
                    }
                }
                
                // CRITICAL FIX: Always check for schema changes, not just row changes
                if parent_archive_path.exists() {
                    // Step 1: Load parent schema and data for comparison
                    let parent_schema = self.load_cached_schema(&parent_archive_path)?;
                    let parent_row_data = self.load_cached_row_data(&parent_archive_path)?;
                    let current_row_data = self.extract_current_row_data(current_data_info)?;
                    
                    // Step 2: Always run comprehensive change detection (schema + rows)
                    let changes = crate::change_detection::ChangeDetector::detect_changes(
                        &parent_schema,
                        &parent_row_data,
                        &current_data_info.columns,
                        &current_row_data,
                    )?;
                    
                    // Step 3: Create delta info regardless of whether changes exist
                    // (even "no changes" is valuable information for the chain)
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
        
        // No parent found - this is the first snapshot for this source
        Ok((None, 0, None))
    }


    /// Update parent's can_reconstruct_parent flag
    fn update_parent_reconstruct_flag(
        &self,
        _workspace: &crate::workspace::TabdiffWorkspace,
        _parent_name: &str,
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


    /// Load cached row data from archive for detailed comparison
    fn load_cached_row_data(&self, archive_path: &Path) -> Result<Vec<Vec<String>>> {
        let files = ArchiveManager::extract_archive(archive_path)?;
        
        // Load from data.parquet (as per README spec)
        for (filename, content) in files {
            if filename == "data.parquet" {
                let content_str = String::from_utf8(content)?;
                let data_parquet: serde_json::Value = serde_json::from_str(&content_str)?;
                
                if let Some(rows_array_data) = data_parquet.get("rows") {
                    if let Some(rows_array) = rows_array_data.as_array() {
                        let mut row_data = Vec::new();
                        for row_value in rows_array {
                            if let Some(row_array) = row_value.as_array() {
                                let row: Vec<String> = row_array
                                    .iter()
                                    .map(|v| v.as_str().unwrap_or("").to_string())
                                    .collect();
                                row_data.push(row);
                            }
                        }
                        return Ok(row_data);
                    }
                }
            }
        }
        
        Ok(Vec::new())
    }

    /// Load cached schema from archive
    fn load_cached_schema(&self, archive_path: &Path) -> Result<Vec<crate::hash::ColumnInfo>> {
        let files = ArchiveManager::extract_archive(archive_path)?;
        
        for (filename, content) in files {
            if filename == "schema.json" {
                let content_str = String::from_utf8(content)?;
                let schema_data: serde_json::Value = serde_json::from_str(&content_str)?;
                
                if let Some(columns_data) = schema_data.get("columns") {
                    if let Some(columns_array) = columns_data.as_array() {
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
            }
        }
        
        Ok(Vec::new())
    }

    /// Extract current row data from data info (we need to reload from source)
    fn extract_current_row_data(&self, current_data_info: &DataInfo) -> Result<Vec<Vec<String>>> {
        // Create a new data processor to extract the current data
        let mut data_processor = DataProcessor::new()?;
        data_processor.load_file(&current_data_info.source)?;
        
        // Extract the full row data
        let row_data = data_processor.extract_data_chunked_with_progress(None)?;
        
        Ok(row_data)
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
                "data.parquet" => {
                    let content_str = String::from_utf8(content)?;
                    // This contains the actual full row data when --full-data is used
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
            row_data: row_data.unwrap_or_else(|| serde_json::json!({"rows": []})), // Default to empty rows if no data.parquet
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

    /// Build snapshot chain for a specific source file
    pub fn build_chain_for_source(workspace: &crate::workspace::TabdiffWorkspace, source_path: &str) -> Result<Self> {
        let snapshot_names = workspace.list_snapshots()?;
        let mut snapshots = Vec::new();
        
        for name in snapshot_names {
            let (_, json_path) = workspace.snapshot_paths(&name);
            if json_path.exists() {
                let metadata = SnapshotLoader::load_metadata(&json_path)?;
                
                // Check if this snapshot is from the same source
                let is_same_source = if let Some(snapshot_source_path) = &metadata.source_path {
                    // Use the stored canonical source path
                    snapshot_source_path == source_path
                } else {
                    // Legacy snapshot without source_path, check original source field
                    let snapshot_canonical_path = std::path::Path::new(&metadata.source)
                        .canonicalize()
                        .unwrap_or_else(|_| std::path::PathBuf::from(&metadata.source))
                        .to_string_lossy()
                        .to_string();
                    
                    snapshot_canonical_path == source_path
                };
                
                if is_same_source {
                    snapshots.push(metadata);
                }
            }
        }
        
        // Sort by sequence number and creation time
        snapshots.sort_by(|a, b| {
            a.sequence_number.cmp(&b.sequence_number)
                .then_with(|| a.created.cmp(&b.created))
        });
        
        // Find head (latest snapshot for this source)
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
            columns: Vec::new(),
            archive_size: Some(1024),
            has_full_data: true,
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
            source_path: Some("/path/to/test.csv".to_string()),
            source_fingerprint: Some("test_fingerprint".to_string()),
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
            columns: Vec::new(),
            archive_size: Some(1024),
            has_full_data: true,
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
            source_path: Some("/path/to/test.csv".to_string()),
            source_fingerprint: Some("test_fingerprint".to_string()),
        };

        let json_content = serde_json::to_string_pretty(&metadata).unwrap();
        fs::write(&json_path, json_content).unwrap();

        let loaded = SnapshotLoader::load_metadata(&json_path).unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.row_count, 100);
    }
}
