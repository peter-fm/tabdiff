//! Data processing utilities using DuckDB

use crate::error::Result;
use crate::hash::ColumnInfo;
use duckdb::Connection;
use std::collections::HashMap;
use std::path::Path;

/// Data processor for various file formats
pub struct DataProcessor {
    connection: Connection,
}

impl DataProcessor {
    /// Create a new data processor
    pub fn new() -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        Ok(Self { connection })
    }

    /// Load data from file and return basic info
    pub fn load_file(&self, file_path: &Path) -> Result<DataInfo> {
        // Validate file exists and is readable
        if !file_path.exists() {
            return Err(crate::error::TabdiffError::invalid_input(
                format!("File not found: {}", file_path.display())
            ));
        }

        if !file_path.is_file() && !file_path.is_dir() {
            return Err(crate::error::TabdiffError::invalid_input(
                format!("Path is neither a file nor a directory: {}", file_path.display())
            ));
        }

        let path_str = file_path.to_string_lossy();
        
        // Create a view of the file with proper error handling
        let create_view_sql = format!(
            "CREATE OR REPLACE VIEW data_view AS SELECT * FROM '{}'",
            path_str
        );
        
        self.connection.execute(&create_view_sql, [])
            .map_err(|e| self.convert_duckdb_error(e, file_path))?;
        
        // Get row count with error handling
        let row_count: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare row count query: {}", e)
            ))?
            .query_row([], |row| row.get(0))
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to get row count: {}", e)
            ))?;
        
        // Get column information
        let columns = self.get_column_info()?;
        
        Ok(DataInfo {
            source: file_path.to_path_buf(),
            row_count,
            columns,
        })
    }

    /// Convert DuckDB errors to appropriate TabdiffError types
    fn convert_duckdb_error(&self, error: duckdb::Error, file_path: &Path) -> crate::error::TabdiffError {
        let error_msg = error.to_string();
        
        // Detect common file format issues
        if error_msg.contains("CSV Error") || 
           error_msg.contains("Could not convert") ||
           error_msg.contains("Invalid CSV") ||
           error_msg.contains("Unterminated quoted field") {
            crate::error::TabdiffError::invalid_input(
                format!("Malformed CSV file '{}': {}", file_path.display(), error_msg)
            )
        } else if error_msg.contains("JSON") || error_msg.contains("Malformed JSON") {
            crate::error::TabdiffError::invalid_input(
                format!("Malformed JSON file '{}': {}", file_path.display(), error_msg)
            )
        } else if error_msg.contains("No files found") || error_msg.contains("does not exist") {
            crate::error::TabdiffError::invalid_input(
                format!("File not found: {}", file_path.display())
            )
        } else if error_msg.contains("Permission denied") {
            crate::error::TabdiffError::invalid_input(
                format!("Permission denied accessing file: {}", file_path.display())
            )
        } else if error_msg.contains("UTF-8") || error_msg.contains("encoding") {
            crate::error::TabdiffError::invalid_input(
                format!("File encoding error '{}': {}", file_path.display(), error_msg)
            )
        } else {
            // For other DuckDB errors, pass through the original error message
            crate::error::TabdiffError::DuckDb(error)
        }
    }

    /// Get column information from the current view
    fn get_column_info(&self) -> Result<Vec<ColumnInfo>> {
        let mut stmt = self.connection.prepare("PRAGMA table_info(data_view)")
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare column info query: {}", e)
            ))?;
            
        let rows = stmt.query_map([], |row| {
            // Try to get the nullable value as different types
            let nullable = match row.get_ref(3) {
                Ok(duckdb::types::ValueRef::Int(val)) => val == 0,
                Ok(duckdb::types::ValueRef::Boolean(val)) => !val, // If it's boolean, true means NOT NULL
                _ => false, // Default to nullable if we can't determine
            };
            
            Ok(ColumnInfo {
                name: row.get::<_, String>(1)?,
                data_type: row.get::<_, String>(2)?,
                nullable,
            })
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to query column info: {}", e)
        ))?;
        
        let mut columns = Vec::new();
        for row in rows {
            columns.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process column info row: {}", e)
            ))?);
        }
        
        Ok(columns)
    }

    /// Extract all data as rows of strings
    pub fn extract_all_data(&self) -> Result<Vec<Vec<String>>> {
        // First, get column information to determine the number of columns safely
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new()); // No columns, return empty data
        }
        
        let mut stmt = self.connection.prepare("SELECT * FROM data_view")
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare data extraction query: {}", e)
            ))?;
        
        let rows = stmt.query_map([], |row| {
            let mut string_row = Vec::new();
            for i in 0..column_count {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => b.to_string(),
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => format!("{:?}", d),
                    duckdb::types::ValueRef::Time64(t, _) => format!("{:?}", t),
                    duckdb::types::ValueRef::Timestamp(ts, _) => format!("{:?}", ts),
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok(string_row)
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to extract data rows: {}", e)
        ))?;
        
        let mut data = Vec::new();
        for row in rows {
            data.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process data row: {}", e)
            ))?);
        }
        
        Ok(data)
    }

    /// Extract data by columns
    pub fn extract_column_data(&self) -> Result<HashMap<String, Vec<String>>> {
        let columns = self.get_column_info()?;
        let mut column_data = HashMap::new();
        
        for column in &columns {
            let sql = format!("SELECT \"{}\" FROM data_view", column.name);
            let mut stmt = self.connection.prepare(&sql)
                .map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to prepare column query for '{}': {}", column.name, e)
                ))?;
            
            let rows = stmt.query_map([], |row| {
                let value: String = match row.get_ref(0)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => b.to_string(),
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => format!("{:?}", d),
                    duckdb::types::ValueRef::Time64(t, _) => format!("{:?}", t),
                    duckdb::types::ValueRef::Timestamp(ts, _) => format!("{:?}", ts),
                    _ => "<unknown>".to_string(),
                };
                Ok(value)
            }).map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to extract column data for '{}': {}", column.name, e)
            ))?;
            
            let mut values = Vec::new();
            for row in rows {
                values.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to process column data row for '{}': {}", column.name, e)
                ))?);
            }
            
            column_data.insert(column.name.clone(), values);
        }
        
        Ok(column_data)
    }

    /// Get estimated row count (for progress reporting)
    pub fn estimate_row_count(&self, file_path: &Path) -> Result<u64> {
        // For now, just load and count - could be optimized for large files
        self.load_file(file_path)?;
        let count: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Check if file format is supported
    pub fn is_supported_format(file_path: &Path) -> bool {
        if let Some(extension) = file_path.extension().and_then(|s| s.to_str()) {
            matches!(extension.to_lowercase().as_str(), 
                     "csv" | "parquet" | "json" | "jsonl" | "tsv")
        } else {
            false
        }
    }
}

/// Information about loaded data
#[derive(Debug, Clone)]
pub struct DataInfo {
    pub source: std::path::PathBuf,
    pub row_count: u64,
    pub columns: Vec<ColumnInfo>,
}

impl DataInfo {
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_data_processor_creation() {
        let _processor = DataProcessor::new().unwrap();
        // Just test that we can create a processor
        assert!(true);
    }

    #[test]
    fn test_supported_formats() {
        assert!(DataProcessor::is_supported_format(Path::new("test.csv")));
        assert!(DataProcessor::is_supported_format(Path::new("test.parquet")));
        assert!(DataProcessor::is_supported_format(Path::new("test.json")));
        assert!(!DataProcessor::is_supported_format(Path::new("test.txt")));
        assert!(!DataProcessor::is_supported_format(Path::new("test")));
    }

    #[test]
    fn test_csv_loading() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("test.csv");
        
        // Create a simple CSV file
        let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        fs::write(&csv_path, csv_content).unwrap();
        
        let processor = DataProcessor::new().unwrap();
        let data_info = processor.load_file(&csv_path).unwrap();
        
        assert_eq!(data_info.row_count, 2);
        assert_eq!(data_info.column_count(), 3);
        assert_eq!(data_info.column_names(), vec!["name", "age", "city"]);
    }
}
