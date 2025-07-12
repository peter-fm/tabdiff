//! Snapshot name resolution and management

use crate::error::{Result, TabdiffError};
use crate::workspace::TabdiffWorkspace;
use crate::snapshot::SnapshotLoader;
use chrono::{DateTime, Utc, NaiveDateTime, TimeZone};
use std::path::{Path, PathBuf};

/// Reference to a snapshot (by name or path)
#[derive(Debug, Clone)]
pub enum SnapshotRef {
    /// Snapshot name (e.g., "v1")
    Name(String),
    /// Direct path to .tabdiff or .json file
    Path(PathBuf),
}

impl SnapshotRef {
    pub fn from_string(s: String) -> Self {
        let path = Path::new(&s);
        if path.exists() || s.contains('/') || s.contains('\\') {
            Self::Path(PathBuf::from(s))
        } else {
            Self::Name(s)
        }
    }
}

/// Resolves snapshot references to actual file paths
#[derive(Debug)]
pub struct SnapshotResolver {
    workspace: TabdiffWorkspace,
}

impl SnapshotResolver {
    pub fn new(workspace: TabdiffWorkspace) -> Self {
        Self { workspace }
    }

    /// Resolve a snapshot reference to archive and JSON paths
    pub fn resolve(&self, snapshot_ref: &SnapshotRef) -> Result<ResolvedSnapshot> {
        match snapshot_ref {
            SnapshotRef::Name(name) => self.resolve_by_name(name),
            SnapshotRef::Path(path) => self.resolve_by_path(path),
        }
    }

    /// Resolve snapshot by name
    fn resolve_by_name(&self, name: &str) -> Result<ResolvedSnapshot> {
        let (archive_path, json_path) = self.workspace.snapshot_paths(name);
        
        // Check if snapshot exists
        if !json_path.exists() {
            return Err(TabdiffError::SnapshotNotFound {
                name: name.to_string(),
            });
        }

        Ok(ResolvedSnapshot {
            name: name.to_string(),
            archive_path: if archive_path.exists() {
                Some(archive_path)
            } else {
                None
            },
            json_path,
        })
    }

    /// Resolve snapshot by direct path
    fn resolve_by_path(&self, path: &Path) -> Result<ResolvedSnapshot> {
        if !path.exists() {
            return Err(TabdiffError::InvalidSnapshot {
                path: path.to_path_buf(),
            });
        }

        let extension = path.extension().and_then(|s| s.to_str());
        
        match extension {
            Some("tabdiff") => {
                // Archive file - find corresponding JSON
                let json_path = path.with_extension("json");
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                Ok(ResolvedSnapshot {
                    name,
                    archive_path: Some(path.to_path_buf()),
                    json_path: if json_path.exists() {
                        json_path
                    } else {
                        // If no JSON exists, we'll need to extract metadata from archive
                        path.to_path_buf()
                    },
                })
            }
            Some("json") => {
                // JSON file - find corresponding archive
                let archive_path = path.with_extension("tabdiff");
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                Ok(ResolvedSnapshot {
                    name,
                    archive_path: if archive_path.exists() {
                        Some(archive_path)
                    } else {
                        None
                    },
                    json_path: path.to_path_buf(),
                })
            }
            _ => Err(TabdiffError::InvalidSnapshot {
                path: path.to_path_buf(),
            }),
        }
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        self.workspace.list_snapshots()
    }

    /// Find the latest snapshot
    pub fn latest_snapshot(&self) -> Result<Option<String>> {
        self.workspace.latest_snapshot()
    }

    /// Check if a snapshot exists
    pub fn snapshot_exists(&self, name: &str) -> bool {
        self.workspace.snapshot_exists(name)
    }

    /// Resolve latest snapshot if no specific snapshot is provided
    pub fn resolve_latest(&self) -> Result<Option<ResolvedSnapshot>> {
        if let Some(latest_name) = self.latest_snapshot()? {
            Ok(Some(self.resolve_by_name(&latest_name)?))
        } else {
            Ok(None)
        }
    }

    /// Resolve snapshot with fallback to latest
    pub fn resolve_or_latest(&self, snapshot_ref: Option<&SnapshotRef>) -> Result<ResolvedSnapshot> {
        match snapshot_ref {
            Some(snapshot_ref) => self.resolve(snapshot_ref),
            None => {
                self.resolve_latest()?
                    .ok_or_else(|| TabdiffError::workspace("No snapshots found in workspace"))
            }
        }
    }

    /// Get workspace reference
    pub fn workspace(&self) -> &TabdiffWorkspace {
        &self.workspace
    }

    /// Parse a date string and resolve to the latest snapshot before that time
    pub fn resolve_by_date(&self, date_str: &str) -> Result<ResolvedSnapshot> {
        let target_date = parse_date_string(date_str)?;
        
        // Get all snapshots
        let snapshot_names = self.list_snapshots()?;
        
        let mut best_snapshot: Option<(String, DateTime<Utc>)> = None;
        
        // Find the latest snapshot before the target date
        for name in snapshot_names {
            let (_, json_path) = self.workspace.snapshot_paths(&name);
            if !json_path.exists() {
                continue;
            }
            
            // Load metadata to get creation time
            match SnapshotLoader::load_metadata(&json_path) {
                Ok(metadata) => {
                    // Only consider snapshots created before the target date
                    if metadata.created <= target_date {
                        match &best_snapshot {
                            None => {
                                best_snapshot = Some((name, metadata.created));
                            }
                            Some((_, best_date)) => {
                                if metadata.created > *best_date {
                                    best_snapshot = Some((name, metadata.created));
                                }
                            }
                        }
                    }
                }
                Err(_) => continue, // Skip snapshots with invalid metadata
            }
        }
        
        match best_snapshot {
            Some((name, created)) => {
                println!("🕒 Found snapshot '{}' created at {}", name, created.format("%Y-%m-%d %H:%M:%S UTC"));
                self.resolve_by_name(&name)
            }
            None => Err(TabdiffError::SnapshotNotFound {
                name: format!("No snapshots found before {}", target_date.format("%Y-%m-%d %H:%M:%S UTC")),
            }),
        }
    }
}

/// Parse a date string in various formats
fn parse_date_string(date_str: &str) -> Result<DateTime<Utc>> {
    // Try different date formats
    
    // Format 1: "2025-01-01 15:00:00" (date and time)
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&naive_dt));
    }
    
    // Format 2: "2025-01-01" (date only, defaults to start of day)
    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        let naive_dt = naive_date.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Utc.from_utc_datetime(&naive_dt));
    }
    
    // Format 3: ISO 8601 with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
        return Ok(dt.with_timezone(&Utc));
    }
    
    Err(TabdiffError::invalid_input(format!(
        "Invalid date format: '{}'. Supported formats: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', or ISO 8601",
        date_str
    )))
}

/// A resolved snapshot with all relevant paths
#[derive(Debug, Clone)]
pub struct ResolvedSnapshot {
    /// Snapshot name
    pub name: String,
    /// Path to archive file (if exists)
    pub archive_path: Option<PathBuf>,
    /// Path to JSON metadata file
    pub json_path: PathBuf,
}

impl ResolvedSnapshot {
    /// Check if the snapshot has a full archive
    pub fn has_archive(&self) -> bool {
        self.archive_path.is_some()
    }

    /// Get the archive path, returning error if not available
    pub fn require_archive(&self) -> Result<&PathBuf> {
        self.archive_path
            .as_ref()
            .ok_or_else(|| TabdiffError::archive(format!("Archive not found for snapshot '{}'", self.name)))
    }

    /// Get the best available path (prefer JSON for quick operations)
    pub fn best_path(&self) -> &PathBuf {
        &self.json_path
    }

    /// Get display name for the snapshot
    pub fn display_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_snapshot_ref_from_string() {
        // Name-like strings
        let ref1 = SnapshotRef::from_string("v1".to_string());
        assert!(matches!(ref1, SnapshotRef::Name(_)));

        // Path-like strings
        let ref2 = SnapshotRef::from_string("/path/to/file.json".to_string());
        assert!(matches!(ref2, SnapshotRef::Path(_)));

        let ref3 = SnapshotRef::from_string("./file.tabdiff".to_string());
        assert!(matches!(ref3, SnapshotRef::Path(_)));
    }

    #[test]
    fn test_resolver_with_workspace() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = TabdiffWorkspace::create_new(temp_dir.path().to_path_buf()).unwrap();
        let resolver = SnapshotResolver::new(workspace);

        // Test non-existent snapshot
        let result = resolver.resolve(&SnapshotRef::Name("nonexistent".to_string()));
        assert!(result.is_err());

        // Create a mock snapshot
        let (_, json_path) = resolver.workspace.snapshot_paths("test");
        fs::write(&json_path, "{}").unwrap();

        // Test existing snapshot
        let resolved = resolver.resolve(&SnapshotRef::Name("test".to_string())).unwrap();
        assert_eq!(resolved.name, "test");
        assert!(!resolved.has_archive());
    }
}
