//! Archive management for tabdiff snapshots

use crate::error::{Result, TabdiffError};
use crate::progress::create_file_progress;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use tar::{Archive, Builder};
use zstd::{Decoder, Encoder};

/// Archive manager for creating and extracting .tabdiff files
pub struct ArchiveManager;

impl ArchiveManager {
    /// Create a compressed archive from multiple files
    pub fn create_archive<P: AsRef<Path>>(
        archive_path: P,
        files: &[(String, Vec<u8>)], // (filename, content)
    ) -> Result<()> {
        let archive_file = File::create(archive_path)?;
        
        // Calculate total size for progress
        let total_size: u64 = files.iter().map(|(_, content)| content.len() as u64).sum();
        let progress = create_file_progress(total_size, "Creating archive");
        
        // Create zstd encoder
        let mut encoder = Encoder::new(archive_file, 3)?; // Compression level 3
        
        // Create tar builder
        {
            let mut tar_builder = Builder::new(&mut encoder);
            
            let mut processed = 0u64;
            
            for (filename, content) in files {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                
                tar_builder.append_data(&mut header, filename, content.as_slice())?;
                
                processed += content.len() as u64;
                progress.set_position(processed);
            }
            
            tar_builder.finish()?;
        } // tar_builder is dropped here, releasing the borrow
        
        encoder.finish()?;
        
        progress.finish_with_message("Archive created");
        
        Ok(())
    }
    
    /// Extract files from a compressed archive
    pub fn extract_archive<P: AsRef<Path>>(
        archive_path: P,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let archive_file = File::open(archive_path)?;
        
        // Get file size for progress
        let file_size = archive_file.metadata()?.len();
        let progress = create_file_progress(file_size, "Extracting archive");
        
        // Create zstd decoder
        let mut decoder = Decoder::new(archive_file)?;
        
        // Create tar archive
        let mut archive = Archive::new(&mut decoder);
        
        let mut files = Vec::new();
        let mut processed = 0u64;
        
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.to_string_lossy().to_string();
            
            let mut content = Vec::new();
            entry.read_to_end(&mut content)?;
            
            files.push((path, content));
            
            processed += entry.header().size()?;
            progress.set_position(processed.min(file_size));
        }
        
        progress.finish_with_message("Archive extracted");
        
        Ok(files)
    }
    
    /// List files in an archive without extracting
    pub fn list_archive_contents<P: AsRef<Path>>(
        archive_path: P,
    ) -> Result<Vec<ArchiveEntry>> {
        let archive_file = File::open(archive_path)?;
        let mut decoder = Decoder::new(archive_file)?;
        let mut archive = Archive::new(&mut decoder);
        
        let mut entries = Vec::new();
        
        for entry in archive.entries()? {
            let entry = entry?;
            let path = entry.path()?.to_string_lossy().to_string();
            let size = entry.header().size()?;
            let modified = entry.header().mtime().unwrap_or(0);
            
            entries.push(ArchiveEntry {
                path,
                size,
                modified,
            });
        }
        
        Ok(entries)
    }
    
    /// Extract a single file from archive
    pub fn extract_file<P: AsRef<Path>>(
        archive_path: P,
        filename: &str,
    ) -> Result<Option<Vec<u8>>> {
        let archive_file = File::open(archive_path)?;
        let mut decoder = Decoder::new(archive_file)?;
        let mut archive = Archive::new(&mut decoder);
        
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path_buf = entry.path()?.to_path_buf();
            let path = path_buf.to_string_lossy();
            
            if path == filename {
                let mut content = Vec::new();
                entry.read_to_end(&mut content)?;
                return Ok(Some(content));
            }
        }
        
        Ok(None)
    }
    
    /// Check if archive exists and is valid
    pub fn validate_archive<P: AsRef<Path>>(archive_path: P) -> Result<bool> {
        if !archive_path.as_ref().exists() {
            return Ok(false);
        }
        
        // Try to open and read the archive
        match Self::list_archive_contents(archive_path) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// Get archive size and compression ratio
    pub fn get_archive_stats<P: AsRef<Path>>(
        archive_path: P,
    ) -> Result<ArchiveStats> {
        let archive_file = File::open(&archive_path)?;
        let compressed_size = archive_file.metadata()?.len();
        
        let entries = Self::list_archive_contents(&archive_path)?;
        let uncompressed_size: u64 = entries.iter().map(|e| e.size).sum();
        
        let compression_ratio = if uncompressed_size > 0 {
            compressed_size as f64 / uncompressed_size as f64
        } else {
            1.0
        };
        
        Ok(ArchiveStats {
            compressed_size,
            uncompressed_size,
            compression_ratio,
            file_count: entries.len(),
        })
    }
}

/// Information about a file in an archive
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    pub path: String,
    pub size: u64,
    pub modified: u64,
}

/// Statistics about an archive
#[derive(Debug, Clone)]
pub struct ArchiveStats {
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub compression_ratio: f64,
    pub file_count: usize,
}

impl ArchiveStats {
    pub fn compression_percentage(&self) -> f64 {
        (1.0 - self.compression_ratio) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_extract_archive() {
        let temp_dir = TempDir::new().unwrap();
        let archive_path = temp_dir.path().join("test.tar.zst");
        
        // Create test files
        let files = vec![
            ("file1.txt".to_string(), b"Hello, world!".to_vec()),
            ("file2.json".to_string(), b"{\"test\": true}".to_vec()),
        ];
        
        // Create archive
        ArchiveManager::create_archive(&archive_path, &files).unwrap();
        assert!(archive_path.exists());
        
        // Extract archive
        let extracted = ArchiveManager::extract_archive(&archive_path).unwrap();
        assert_eq!(extracted.len(), 2);
        
        // Check contents
        let file1 = extracted.iter().find(|(name, _)| name == "file1.txt").unwrap();
        assert_eq!(file1.1, b"Hello, world!");
        
        let file2 = extracted.iter().find(|(name, _)| name == "file2.json").unwrap();
        assert_eq!(file2.1, b"{\"test\": true}");
    }
    
    #[test]
    fn test_list_archive_contents() {
        let temp_dir = TempDir::new().unwrap();
        let archive_path = temp_dir.path().join("test.tar.zst");
        
        let files = vec![
            ("metadata.json".to_string(), b"{}".to_vec()),
            ("data.parquet".to_string(), b"fake parquet data".to_vec()),
        ];
        
        ArchiveManager::create_archive(&archive_path, &files).unwrap();
        
        let entries = ArchiveManager::list_archive_contents(&archive_path).unwrap();
        assert_eq!(entries.len(), 2);
        
        let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(names.contains(&"metadata.json"));
        assert!(names.contains(&"data.parquet"));
    }
    
    #[test]
    fn test_extract_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let archive_path = temp_dir.path().join("test.tar.zst");
        
        let files = vec![
            ("wanted.txt".to_string(), b"This is the file I want".to_vec()),
            ("unwanted.txt".to_string(), b"This is not the file I want".to_vec()),
        ];
        
        ArchiveManager::create_archive(&archive_path, &files).unwrap();
        
        let content = ArchiveManager::extract_file(&archive_path, "wanted.txt").unwrap();
        assert!(content.is_some());
        assert_eq!(content.unwrap(), b"This is the file I want");
        
        let missing = ArchiveManager::extract_file(&archive_path, "missing.txt").unwrap();
        assert!(missing.is_none());
    }
}
