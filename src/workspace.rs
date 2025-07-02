//! Workspace management for tabdiff operations

use crate::error::Result;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Manages the .tabdiff workspace directory
#[derive(Debug, Clone)]
pub struct TabdiffWorkspace {
    /// Project root directory (where .tabdiff/ lives)
    pub root: PathBuf,
    /// .tabdiff/ directory path
    pub tabdiff_dir: PathBuf,
    /// .tabdiff/diffs/ directory path
    pub diffs_dir: PathBuf,
}

impl TabdiffWorkspace {
    /// Find existing workspace or create a new one
    pub fn find_or_create(start_dir: Option<&Path>) -> Result<Self> {
        let current_dir = std::env::current_dir()?;
        let start = start_dir.unwrap_or(&current_dir);
        
        // First try to find existing .tabdiff directory
        if let Some(workspace) = Self::find_existing(start)? {
            return Ok(workspace);
        }
        
        // If not found, create in current directory or specified directory
        let root = start.to_path_buf();
        Self::create_new(root)
    }
    
    /// Find existing .tabdiff workspace by walking up directory tree
    fn find_existing(start_dir: &Path) -> Result<Option<Self>> {
        let mut current = start_dir;
        
        loop {
            let tabdiff_dir = current.join(".tabdiff");
            if tabdiff_dir.exists() && tabdiff_dir.is_dir() {
                return Ok(Some(Self::from_root(current.to_path_buf())?));
            }
            
            // Also check for .git directory as a hint for project root
            let git_dir = current.join(".git");
            if git_dir.exists() {
                // Found git repo but no .tabdiff, could create here
                break;
            }
            
            match current.parent() {
                Some(parent) => current = parent,
                None => break, // Reached filesystem root
            }
        }
        
        Ok(None)
    }
    
    /// Create a new workspace in the specified root directory
    pub fn create_new(root: PathBuf) -> Result<Self> {
        let workspace = Self::from_root(root)?;
        
        // Create directories
        fs::create_dir_all(&workspace.tabdiff_dir)?;
        fs::create_dir_all(&workspace.diffs_dir)?;
        
        // Create initial config file
        workspace.create_config()?;
        
        // Update .gitignore
        workspace.ensure_gitignore()?;
        
        log::info!("Created tabdiff workspace at: {}", workspace.root.display());
        
        Ok(workspace)
    }
    
    /// Create workspace from root directory path
    pub fn from_root(root: PathBuf) -> Result<Self> {
        let tabdiff_dir = root.join(".tabdiff");
        let diffs_dir = tabdiff_dir.join("diffs");
        
        Ok(Self {
            root,
            tabdiff_dir,
            diffs_dir,
        })
    }
    
    /// Get paths for a snapshot (archive and JSON)
    pub fn snapshot_paths(&self, name: &str) -> (PathBuf, PathBuf) {
        let archive_path = self.tabdiff_dir.join(format!("{}.tabdiff", name));
        let json_path = self.tabdiff_dir.join(format!("{}.json", name));
        (archive_path, json_path)
    }
    
    /// Get path for a diff result
    pub fn diff_path(&self, name1: &str, name2: &str) -> PathBuf {
        self.diffs_dir.join(format!("{}-{}.json", name1, name2))
    }
    
    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        let mut snapshots = Vec::new();
        
        if !self.tabdiff_dir.exists() {
            return Ok(snapshots);
        }
        
        for entry in fs::read_dir(&self.tabdiff_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "json" {
                    if let Some(stem) = path.file_stem() {
                        if let Some(name) = stem.to_str() {
                            // Filter out config file - only include user snapshots
                            if name != "config" {
                                snapshots.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
        
        snapshots.sort();
        Ok(snapshots)
    }

    /// List snapshots for a specific source file
    pub fn list_snapshots_for_source(&self, source_path: &str) -> Result<Vec<String>> {
        let all_snapshots = self.list_snapshots()?;
        let mut source_snapshots = Vec::new();
        
        for snapshot_name in all_snapshots {
            let (_, json_path) = self.snapshot_paths(&snapshot_name);
            if json_path.exists() {
                // Load metadata to check source
                let content = fs::read_to_string(&json_path)?;
                let metadata: serde_json::Value = serde_json::from_str(&content)?;
                
                // Check if this snapshot is from the same source
                let is_same_source = if let Some(snapshot_source_path) = metadata.get("source_path").and_then(|v| v.as_str()) {
                    // Use the stored canonical source path
                    snapshot_source_path == source_path
                } else if let Some(snapshot_source) = metadata.get("source").and_then(|v| v.as_str()) {
                    // Legacy snapshot without source_path, check original source field
                    let snapshot_canonical_path = std::path::Path::new(snapshot_source)
                        .canonicalize()
                        .unwrap_or_else(|_| std::path::PathBuf::from(snapshot_source))
                        .to_string_lossy()
                        .to_string();
                    
                    snapshot_canonical_path == source_path
                } else {
                    false
                };
                
                if is_same_source {
                    source_snapshots.push(snapshot_name);
                }
            }
        }
        
        source_snapshots.sort();
        Ok(source_snapshots)
    }

    /// Find the most recent snapshot for a specific source file
    pub fn latest_snapshot_for_source(&self, source_path: &str) -> Result<Option<String>> {
        let source_snapshots = self.list_snapshots_for_source(source_path)?;
        
        if source_snapshots.is_empty() {
            return Ok(None);
        }
        
        // Read creation times from JSON files and find latest
        let mut latest_time = None;
        let mut latest_name = None;
        
        for name in source_snapshots {
            let (_, json_path) = self.snapshot_paths(&name);
            if json_path.exists() {
                if let Ok(metadata) = fs::metadata(&json_path) {
                    if let Ok(created) = metadata.created() {
                        if latest_time.is_none() || Some(created) > latest_time {
                            latest_time = Some(created);
                            latest_name = Some(name);
                        }
                    }
                }
            }
        }
        
        Ok(latest_name)
    }
    
    /// Find the most recent snapshot by creation time
    pub fn latest_snapshot(&self) -> Result<Option<String>> {
        let snapshots = self.list_snapshots()?;
        
        // Filter out config file - only consider user snapshots
        let user_snapshots: Vec<String> = snapshots.into_iter()
            .filter(|name| name != "config")
            .collect();
        
        if user_snapshots.is_empty() {
            return Ok(None);
        }
        
        // Read creation times from JSON files and find latest
        let mut latest_time = None;
        let mut latest_name = None;
        
        for name in user_snapshots {
            let (_, json_path) = self.snapshot_paths(&name);
            if json_path.exists() {
                if let Ok(metadata) = fs::metadata(&json_path) {
                    if let Ok(created) = metadata.created() {
                        if latest_time.is_none() || Some(created) > latest_time {
                            latest_time = Some(created);
                            latest_name = Some(name);
                        }
                    }
                }
            }
        }
        
        Ok(latest_name)
    }
    
    /// Check if a snapshot exists
    pub fn snapshot_exists(&self, name: &str) -> bool {
        let (_, json_path) = self.snapshot_paths(name);
        json_path.exists()
    }
    
    /// Create initial configuration file
    fn create_config(&self) -> Result<()> {
        self.create_config_with_force(false)
    }
    
    /// Create configuration file with optional force overwrite
    pub fn create_config_with_force(&self, force: bool) -> Result<()> {
        let config_path = self.tabdiff_dir.join("config.json");
        
        if config_path.exists() && !force {
            return Ok(()); // Don't overwrite existing config unless forced
        }
        
        let config = serde_json::json!({
            "version": crate::FORMAT_VERSION,
            "created": chrono::Utc::now(),
            "default_batch_size": crate::DEFAULT_BATCH_SIZE,
            "default_sample_size": crate::DEFAULT_SAMPLE_SIZE
        });
        
        fs::write(config_path, serde_json::to_string_pretty(&config)?)?;
        Ok(())
    }
    
    /// Ensure .gitignore contains tabdiff entries
    pub fn ensure_gitignore(&self) -> Result<()> {
        let gitignore_path = self.root.join(".gitignore");
        let tabdiff_ignore = "# Ignore compressed snapshot archives\n.tabdiff/*.tabdiff\n";
        
        if gitignore_path.exists() {
            let content = fs::read_to_string(&gitignore_path)?;
            if !content.contains(".tabdiff/*.tabdiff") {
                let new_content = if content.ends_with('\n') {
                    format!("{}\n{}", content, tabdiff_ignore)
                } else {
                    format!("{}\n\n{}", content, tabdiff_ignore)
                };
                fs::write(gitignore_path, new_content)?;
                log::info!("Updated .gitignore with tabdiff entries");
            }
        } else {
            fs::write(gitignore_path, tabdiff_ignore)?;
            log::info!("Created .gitignore with tabdiff entries");
        }
        
        Ok(())
    }
    
    /// Get workspace statistics
    pub fn stats(&self) -> Result<WorkspaceStats> {
        let snapshots = self.list_snapshots()?;
        let mut total_archive_size = 0u64;
        let mut total_json_size = 0u64;
        
        for name in &snapshots {
            let (archive_path, json_path) = self.snapshot_paths(name);
            
            if archive_path.exists() {
                if let Ok(metadata) = fs::metadata(&archive_path) {
                    total_archive_size += metadata.len();
                }
            }
            
            if json_path.exists() {
                if let Ok(metadata) = fs::metadata(&json_path) {
                    total_json_size += metadata.len();
                }
            }
        }
        
        // Count diff files
        let mut diff_count = 0;
        let mut total_diff_size = 0u64;
        
        if self.diffs_dir.exists() {
            for entry in WalkDir::new(&self.diffs_dir) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    diff_count += 1;
                    total_diff_size += entry.metadata()?.len();
                }
            }
        }
        
        Ok(WorkspaceStats {
            snapshot_count: snapshots.len(),
            diff_count,
            total_archive_size,
            total_json_size,
            total_diff_size,
        })
    }
    
    /// Clean up old or unused files
    pub fn cleanup(&self, keep_latest: usize) -> Result<CleanupStats> {
        let snapshots = self.list_snapshots()?;
        
        if snapshots.len() <= keep_latest {
            return Ok(CleanupStats::default());
        }
        
        // Sort by creation time and remove oldest
        let mut snapshots_with_time = Vec::new();
        
        for name in snapshots {
            let (_, json_path) = self.snapshot_paths(&name);
            if let Ok(metadata) = fs::metadata(&json_path) {
                if let Ok(created) = metadata.created() {
                    snapshots_with_time.push((name, created));
                }
            }
        }
        
        snapshots_with_time.sort_by_key(|(_, time)| *time);
        
        let mut stats = CleanupStats::default();
        
        // Remove oldest snapshots beyond keep_latest
        for (name, _) in snapshots_with_time.iter().take(snapshots_with_time.len().saturating_sub(keep_latest)) {
            let (archive_path, _json_path) = self.snapshot_paths(name);
            
            if archive_path.exists() {
                if let Ok(metadata) = fs::metadata(&archive_path) {
                    stats.archives_removed += 1;
                    stats.bytes_freed += metadata.len();
                }
                fs::remove_file(archive_path)?;
            }
            
            // Note: We keep JSON files for Git history
            log::info!("Removed old snapshot archive: {}", name);
        }
        
        Ok(stats)
    }
}

/// Statistics about the workspace
#[derive(Debug, Default)]
pub struct WorkspaceStats {
    pub snapshot_count: usize,
    pub diff_count: usize,
    pub total_archive_size: u64,
    pub total_json_size: u64,
    pub total_diff_size: u64,
}

/// Statistics about cleanup operations
#[derive(Debug, Default)]
pub struct CleanupStats {
    pub archives_removed: usize,
    pub bytes_freed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_workspace_creation() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
        
        assert!(workspace.tabdiff_dir.exists());
        assert!(workspace.diffs_dir.exists());
        assert!(workspace.root.join(".gitignore").exists());
    }

    #[test]
    fn test_snapshot_paths() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = TabdiffWorkspace::from_root(temp_dir.path().to_path_buf()).unwrap();
        
        let (archive, json) = workspace.snapshot_paths("test");
        assert_eq!(archive.file_name().unwrap(), "test.tabdiff");
        assert_eq!(json.file_name().unwrap(), "test.json");
    }
}
