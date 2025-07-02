//! Data processing utilities using DuckDB

use crate::error::Result;
use crate::hash::ColumnInfo;
use duckdb::Connection;
use num_bigint::BigUint;
use num_traits::Num;
use std::collections::HashMap;
use std::path::Path;

/// Data processor for various file formats
pub struct DataProcessor {
    connection: Connection,
    chunk_size: usize,
}

impl DataProcessor {
    /// Create a new data processor with default settings
    pub fn new() -> Result<Self> {
        Self::new_with_config(10000) // Default chunk size of 10K rows
    }

    /// Create a new data processor with custom configuration
    pub fn new_with_config(chunk_size: usize) -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        
        // Optimize DuckDB for large datasets and performance
        connection.execute("SET memory_limit='4GB'", [])?;
        // Use all available CPU cores (DuckDB auto-detects if not specified)
        connection.execute("SET enable_progress_bar=false", [])?; // Disable for performance
        connection.execute("SET preserve_insertion_order=false", [])?; // Allow reordering for performance
        connection.execute("SET enable_object_cache=true", [])?; // Enable object caching
        
        Ok(Self { connection, chunk_size })
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
        // First, get column names in their original order using DESCRIBE
        let mut stmt = self.connection.prepare("DESCRIBE data_view")
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare describe query: {}", e)
            ))?;
            
        let rows = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get::<_, String>(0)?,           // column_name
                data_type: row.get::<_, String>(1)?,      // column_type
                nullable: true, // DESCRIBE doesn't provide nullable info, default to true
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


    /// Safe hash conversion using num-bigint to handle large integers
    fn safe_hash_conversion(&self, hash_hex: &str) -> String {
        // Parse the hex string as BigUint
        let big_hash = BigUint::from_str_radix(hash_hex, 16)
            .unwrap_or_else(|_| BigUint::from(0u64));
        
        // Convert to a consistent 64-bit representation
        let hash_u64 = if big_hash > BigUint::from(u64::MAX) {
            // For very large hashes, use modulo to fit in u64
            let modulo_result = &big_hash % BigUint::from(u64::MAX);
            modulo_result.to_string().parse::<u64>().unwrap_or(0)
        } else {
            big_hash.to_string().parse::<u64>().unwrap_or(0)
        };
        
        // Return as consistent hex format
        format!("{:016x}", hash_u64)
    }

    /// Robust hash extraction from DuckDB row with proper type handling
    fn robust_hash_extraction(&self, row: &duckdb::Row, index: usize) -> Result<String> {
        match row.get_ref(index).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to get value at index {}: {}", index, e)
        ))? {
            duckdb::types::ValueRef::Text(s) => {
                let hex_str = String::from_utf8_lossy(s);
                Ok(self.safe_hash_conversion(&hex_str))
            },
            duckdb::types::ValueRef::BigInt(i) => {
                Ok(format!("{:016x}", i.abs() as u64))
            },
            duckdb::types::ValueRef::HugeInt(i) => {
                // Handle 128-bit integers safely using num-bigint
                let big_int = BigUint::from(i.abs() as u128);
                Ok(self.safe_hash_conversion(&format!("{:x}", big_int)))
            },
            duckdb::types::ValueRef::UBigInt(i) => {
                Ok(format!("{:016x}", i))
            },
            duckdb::types::ValueRef::Int(i) => {
                Ok(format!("{:016x}", i.abs() as u64))
            },
            _ => {
                // Fallback for any other type
                Ok("0000000000000000".to_string())
            }
        }
    }

    /// Compute row hashes directly in DuckDB for maximum performance (robust version)
    pub fn compute_row_hashes_sql(&self) -> Result<Vec<crate::hash::RowHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Build column concatenation for hashing
        let column_concat = columns.iter()
            .map(|col| format!("COALESCE(CAST(\"{}\" AS VARCHAR), '')", col.name))
            .collect::<Vec<_>>()
            .join(", '|', ");

        // Use DuckDB's hash function but return as hex string to avoid overflow
        let hash_sql = format!(
            "SELECT ROW_NUMBER() OVER () as row_index,
                    printf('%x', hash(concat({}))) as row_hash_hex
             FROM data_view
             ORDER BY row_index",
            column_concat
        );

        let mut stmt = self.connection.prepare(&hash_sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare hash query: {}", e)
            ))?;

        let rows = stmt.query_map([], |row| {
            // Get row index safely
            let row_idx = match row.get::<_, i64>(0) {
                Ok(idx) => idx.max(0) as u64, // Ensure non-negative
                Err(_) => 0u64, // Fallback to 0 if conversion fails
            };
            
            // Get hash as hex string - this avoids overflow issues entirely
            let hash_hex: String = match row.get::<_, String>(1) {
                Ok(hex_str) => self.safe_hash_conversion(&hex_str),
                Err(_) => {
                    // Fallback: try to extract using robust method
                    self.robust_hash_extraction(row, 1).unwrap_or_else(|_| "0000000000000000".to_string())
                }
            };
            
            Ok(crate::hash::RowHash {
                row_index: row_idx,
                hash: hash_hex,
            })
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to compute row hashes: {}", e)
        ))?;

        let mut hashes = Vec::new();
        for row in rows {
            hashes.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process hash row: {}", e)
            ))?);
        }

        Ok(hashes)
    }

    /// Compute column hashes directly in DuckDB using batch processing for performance
    pub fn compute_column_hashes_sql(&self) -> Result<Vec<crate::hash::ColumnHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Try batch processing first for better performance
        match self.compute_column_hashes_batch(&columns) {
            Ok(hashes) => Ok(hashes),
            Err(_) => {
                // Fallback to individual processing if batch fails
                self.compute_column_hashes_individual(&columns)
            }
        }
    }

    /// Batch column hash computation - processes all columns in a single query
    fn compute_column_hashes_batch(&self, columns: &[ColumnInfo]) -> Result<Vec<crate::hash::ColumnHash>> {
        // Build a single SQL query that computes all column hashes at once
        let column_selects: Vec<String> = columns.iter()
            .map(|col| format!(
                "printf('%x', hash(string_agg(COALESCE(CAST(\"{}\" AS VARCHAR), ''), '|'))) as \"{}\"",
                col.name, col.name
            ))
            .collect();

        let batch_sql = format!(
            "SELECT {} FROM data_view",
            column_selects.join(", ")
        );

        let mut stmt = self.connection.prepare(&batch_sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare batch column hash query: {}", e)
            ))?;

        let result_row = stmt.query_row([], |row| {
            let mut hashes = Vec::new();
            for (i, column) in columns.iter().enumerate() {
                let hash_hex: String = row.get(i)?;
                let processed_hash = self.safe_hash_conversion(&hash_hex);
                
                hashes.push(crate::hash::ColumnHash {
                    column_name: column.name.clone(),
                    column_type: column.data_type.clone(),
                    hash: processed_hash,
                });
            }
            Ok(hashes)
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to execute batch column hash query: {}", e)
        ))?;

        let mut column_hashes = result_row;
        // Sort by column name for consistency
        column_hashes.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        Ok(column_hashes)
    }

    /// Individual column hash computation - fallback for when batch processing fails
    fn compute_column_hashes_individual(&self, columns: &[ColumnInfo]) -> Result<Vec<crate::hash::ColumnHash>> {
        let mut column_hashes = Vec::new();

        for column in columns {
            // Use printf to get hash as hex string, avoiding overflow issues
            let hash_sql = format!(
                "SELECT printf('%x', hash(string_agg(COALESCE(CAST(\"{}\" AS VARCHAR), ''), '|'))) as col_hash
                 FROM data_view",
                column.name
            );

            let hash_hex: String = self.connection
                .prepare(&hash_sql)
                .map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to prepare column hash query for '{}': {}", column.name, e)
                ))?
                .query_row([], |row| row.get(0))
                .map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to compute column hash for '{}': {}", column.name, e)
                ))?;

            // Use safe hash conversion to ensure consistent format
            let processed_hash = self.safe_hash_conversion(&hash_hex);

            column_hashes.push(crate::hash::ColumnHash {
                column_name: column.name.clone(),
                column_type: column.data_type.clone(),
                hash: processed_hash,
            });
        }

        // Sort by column name for consistency
        column_hashes.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        Ok(column_hashes)
    }

    /// Helper method to extract value as string from DuckDB row
    fn extract_value_as_string(&self, row: &duckdb::Row, index: usize) -> Result<String> {
        let value: String = match row.get_ref(index)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to get value at index {}: {}", index, e)
            ))? {
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
