//! Data processing utilities using DuckDB

use crate::error::Result;
use crate::hash::ColumnInfo;
use blake3;
use duckdb::Connection;
use num_bigint::BigUint;
use num_traits::Num;
use std::collections::HashMap;
use std::path::Path;

/// Data processor for various file formats
pub struct DataProcessor {
    connection: Connection,
    chunk_size: usize,
    cached_columns: Option<Vec<ColumnInfo>>,
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
        
        Ok(Self { 
            connection, 
            chunk_size, 
            cached_columns: None 
        })
    }

    /// Load data from file and return basic info
    pub fn load_file(&mut self, file_path: &Path) -> Result<DataInfo> {
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

    /// Get column information from the current view (cached to avoid repeated calls)
    fn get_column_info(&mut self) -> Result<Vec<ColumnInfo>> {
        // Return cached columns if available
        if let Some(ref columns) = self.cached_columns {
            return Ok(columns.clone());
        }

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
        
        // Cache the columns for future calls
        self.cached_columns = Some(columns.clone());
        
        Ok(columns)
    }

    /// Extract all data as rows of strings
    pub fn extract_all_data(&mut self) -> Result<Vec<Vec<String>>> {
        self.extract_data_chunked_with_progress(None)
    }

    /// Extract data in chunks with progress reporting for better memory efficiency
    pub fn extract_data_chunked_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<Vec<String>>> {
        // First, get column information to determine the number of columns safely
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new()); // No columns, return empty data
        }

        // Get total row count for progress reporting
        let total_rows: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_data = Vec::new();
        let mut processed_rows = 0u64;

        // Use adaptive chunk size based on total rows
        let chunk_size = if total_rows > 1_000_000 {
            50_000 // Large datasets: 50K rows per chunk
        } else if total_rows > 100_000 {
            25_000 // Medium datasets: 25K rows per chunk
        } else {
            self.chunk_size.min(total_rows as usize) // Small datasets: use configured chunk size
        };

        while processed_rows < total_rows {
            let current_chunk_size = chunk_size.min((total_rows - processed_rows) as usize);
            
            let chunk_sql = format!(
                "SELECT * FROM data_view LIMIT {} OFFSET {}",
                current_chunk_size,
                processed_rows
            );

            let mut stmt = self.connection.prepare(&chunk_sql)
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
            
            let mut chunk_data = Vec::new();
            for row in rows {
                chunk_data.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to process data row: {}", e)
                ))?);
            }

            processed_rows += chunk_data.len() as u64;
            all_data.extend(chunk_data);

            // Report progress if callback provided
            if let Some(callback) = progress_callback {
                callback(processed_rows, total_rows);
            }
        }
        
        Ok(all_data)
    }

    /// Extract data by columns
    pub fn extract_column_data(&mut self) -> Result<HashMap<String, Vec<String>>> {
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
    pub fn estimate_row_count(&mut self, file_path: &Path) -> Result<u64> {
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
    pub fn compute_row_hashes_sql(&mut self) -> Result<Vec<crate::hash::RowHash>> {
        self.compute_row_hashes_with_progress(None)
    }

    /// Compute row hashes with streaming row-by-row processing (revolutionary approach)
    pub fn compute_row_hashes_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Get total row count for progress reporting
        let total_rows: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        // REVOLUTIONARY APPROACH: Simple streaming without complex SQL
        // Use basic SELECT * and process row-by-row in Rust
        eprintln!("Starting streaming processing of {} rows...", total_rows);
        
        let mut all_hashes = Vec::new();
        let mut processed_rows = 0u64;
        let start_time = std::time::Instant::now();
        
        // Simple SQL - just get all data without complex operations
        let simple_sql = "SELECT * FROM data_view";
        
        let mut stmt = self.connection.prepare(simple_sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare simple streaming query: {}", e)
            ))?;

        eprintln!("Query prepared, starting row iteration...");
        
        let rows = stmt.query_map([], |row| {
            // Extract all column values for this row
            let mut row_values = Vec::new();
            for i in 0..columns.len() {
                let value: String = match row.get_ref(i) {
                    Ok(duckdb::types::ValueRef::Null) => String::new(),
                    Ok(duckdb::types::ValueRef::Boolean(b)) => b.to_string(),
                    Ok(duckdb::types::ValueRef::TinyInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::SmallInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::Int(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::BigInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::HugeInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::UTinyInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::USmallInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::UInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::UBigInt(i)) => i.to_string(),
                    Ok(duckdb::types::ValueRef::Float(f)) => f.to_string(),
                    Ok(duckdb::types::ValueRef::Double(f)) => f.to_string(),
                    Ok(duckdb::types::ValueRef::Decimal(d)) => d.to_string(),
                    Ok(duckdb::types::ValueRef::Text(s)) => String::from_utf8_lossy(s).to_string(),
                    Ok(duckdb::types::ValueRef::Blob(b)) => format!("<blob:{} bytes>", b.len()),
                    Ok(duckdb::types::ValueRef::Date32(d)) => format!("{:?}", d),
                    Ok(duckdb::types::ValueRef::Time64(t, _)) => format!("{:?}", t),
                    Ok(duckdb::types::ValueRef::Timestamp(ts, _)) => format!("{:?}", ts),
                    _ => String::new(), // Handle any other types or errors
                };
                row_values.push(value);
            }
            
            Ok(row_values)
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to create row iterator: {}", e)
        ))?;

        eprintln!("Row iterator created, processing rows...");
        
        // Process each row individually with immediate progress updates
        for (row_index, row_result) in rows.enumerate() {
            let row_values = row_result.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process row {}: {}", row_index, e)
            ))?;
            
            // Hash the row values using Blake3
            let row_content = row_values.join("|");
            let hash = blake3::hash(row_content.as_bytes());
            let hash_hex = format!("{:016x}", 
                hash.as_bytes()[0..8]
                    .iter()
                    .fold(0u64, |acc, &b| (acc << 8) | b as u64)
            );
            
            all_hashes.push(crate::hash::RowHash {
                row_index: row_index as u64,
                hash: hash_hex,
            });
            
            processed_rows += 1;
            
            // Real-time progress updates - every 1000 rows for large files, every 100 for smaller
            let update_frequency = if total_rows > 1_000_000 { 1000 } else { 100 };
            
            if processed_rows % update_frequency == 0 || processed_rows == total_rows {
                // Always print to stderr for immediate feedback
                let elapsed = start_time.elapsed().as_secs_f64();
                let rate = processed_rows as f64 / elapsed;
                let percent = (processed_rows as f64 / total_rows as f64) * 100.0;
                
                use std::io::Write;
                eprint!("\rProcessed: {}/{} rows ({:.1}%) - {:.0} rows/sec", 
                       processed_rows, total_rows, percent, rate);
                let _ = std::io::stderr().flush();
                
                // Also call the progress callback if provided
                if let Some(callback) = progress_callback {
                    callback(processed_rows, total_rows);
                }
            }
        }
        
        // Final newline after progress
        eprintln!("\nStreaming processing completed!");
        
        Ok(all_hashes)
    }

    /// Compute column hashes efficiently - just hash column metadata, not all data
    pub fn compute_column_hashes_sql(&mut self) -> Result<Vec<crate::hash::ColumnHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Fast column hashing - just hash the column metadata, not all the data
        // This is much more efficient and still provides change detection for schema changes
        self.compute_column_metadata_hashes(&columns)
    }

    /// Efficient column hash computation - hash only metadata, not data content
    fn compute_column_metadata_hashes(&self, columns: &[ColumnInfo]) -> Result<Vec<crate::hash::ColumnHash>> {
        let mut column_hashes = Vec::new();

        for column in columns {
            // Hash just the column metadata (name + type + nullable flag)
            // This is much faster than hashing all column data
            let metadata_string = format!("{}|{}|{}", 
                column.name, 
                column.data_type, 
                if column.nullable { "nullable" } else { "not_null" }
            );
            
            // Use a simple hash of the metadata
            let hash_hex = format!("{:016x}", 
                blake3::hash(metadata_string.as_bytes()).as_bytes()[0..8]
                    .iter()
                    .fold(0u64, |acc, &b| (acc << 8) | b as u64)
            );

            column_hashes.push(crate::hash::ColumnHash {
                column_name: column.name.clone(),
                column_type: column.data_type.clone(),
                hash: hash_hex,
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
        
        let mut processor = DataProcessor::new().unwrap();
        let data_info = processor.load_file(&csv_path).unwrap();
        
        assert_eq!(data_info.row_count, 2);
        assert_eq!(data_info.column_count(), 3);
        assert_eq!(data_info.column_names(), vec!["name", "age", "city"]);
    }
}
