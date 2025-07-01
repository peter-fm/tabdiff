//! Data processing utilities using DuckDB

use crate::error::{Result, TabdiffError};
use crate::hash::{ColumnInfo, HashComputer};
use duckdb::{Connection, Result as DuckResult};
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
        let path_str = file_path.to_string_lossy();
        
        // Create a view of the file
        let create_view_sql = format!(
            "CREATE OR REPLACE VIEW data_view AS SELECT * FROM '{}'",
            path_str
        );
        
        self.connection.execute(&create_view_sql, [])?;
        
        // Get row count
        let row_count: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;
        
        // Get column information
        let columns = self.get_column_info()?;
        
        Ok(DataInfo {
            source: file_path.to_path_buf(),
            row_count,
            columns,
        })
    }

    /// Get column information from the current view
    fn get_column_info(&self) -> Result<Vec<ColumnInfo>> {
        let mut stmt = self.connection.prepare("PRAGMA table_info(data_view)")?;
        let rows = stmt.query_map([], |row| {
            // Try to get the nullable value as different types
            let nullable = match row.get_ref(3)? {
                duckdb::types::ValueRef::Int(val) => val == 0,
                duckdb::types::ValueRef::Boolean(val) => !val, // If it's boolean, true means NOT NULL
                _ => false, // Default to nullable if we can't determine
            };
            
            Ok(ColumnInfo {
                name: row.get::<_, String>(1)?,
                data_type: row.get::<_, String>(2)?,
                nullable,
            })
        })?;
        
        let mut columns = Vec::new();
        for row in rows {
            columns.push(row?);
        }
        
        Ok(columns)
    }

    /// Extract all data as rows of strings
    pub fn extract_all_data(&self) -> Result<Vec<Vec<String>>> {
        let mut stmt = self.connection.prepare("SELECT * FROM data_view")?;
        let column_count = stmt.column_count();
        
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
        })?;
        
        let mut data = Vec::new();
        for row in rows {
            data.push(row?);
        }
        
        Ok(data)
    }

    /// Extract data by columns
    pub fn extract_column_data(&self) -> Result<HashMap<String, Vec<String>>> {
        let columns = self.get_column_info()?;
        let mut column_data = HashMap::new();
        
        for column in &columns {
            let sql = format!("SELECT \"{}\" FROM data_view", column.name);
            let mut stmt = self.connection.prepare(&sql)?;
            
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
            })?;
            
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
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
        let processor = DataProcessor::new().unwrap();
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
