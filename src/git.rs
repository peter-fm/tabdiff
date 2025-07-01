//! Git integration utilities

use crate::error::{Result, TabdiffError};
use std::path::Path;

/// Git integration helper
pub struct GitHelper;

impl GitHelper {
    /// Check if current directory is in a git repository
    pub fn is_git_repo(path: &Path) -> bool {
        let mut current = path;
        loop {
            if current.join(".git").exists() {
                return true;
            }
            match current.parent() {
                Some(parent) => current = parent,
                None => return false,
            }
        }
    }

    /// Get git repository root
    pub fn find_git_root(path: &Path) -> Option<std::path::PathBuf> {
        let mut current = path;
        loop {
            if current.join(".git").exists() {
                return Some(current.to_path_buf());
            }
            match current.parent() {
                Some(parent) => current = parent,
                None => return None,
            }
        }
    }

    /// Suggest DVC commands for tracking large files
    pub fn suggest_dvc_commands() -> Vec<String> {
        vec![
            "dvc add .tabdiff/*.tabdiff".to_string(),
            "git add .tabdiff/*.tabdiff.dvc .gitignore".to_string(),
            "git commit -m 'Track tabdiff archives with DVC'".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_git_repo_detection() {
        let temp_dir = TempDir::new().unwrap();
        
        // Not a git repo initially
        assert!(!GitHelper::is_git_repo(temp_dir.path()));
        
        // Create .git directory
        fs::create_dir(temp_dir.path().join(".git")).unwrap();
        assert!(GitHelper::is_git_repo(temp_dir.path()));
    }
}
