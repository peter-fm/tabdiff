//! Data processing utilities using DuckDB

use crate::error::Result;
use crate::hash::ColumnInfo;
use crate::sql;
use blake3;
use duckdb::Connection;
use std::collections::HashMap;
use std::path::Path;

fn get_duckdb_install_instructions() -> String {
    if cfg!(target_os = "windows") {
        r#"  Windows:
    1. Download DuckDB from: https://duckdb.org/docs/installation/
    2. Extract to C:\Program Files\DuckDB\ or C:\duckdb\
    3. Add the lib directory to your PATH environment variable
    
  Or use Windows Package Manager:
    winget install DuckDB.cli"#.to_string()
    } else if cfg!(target_os = "macos") {
        r#"  macOS:
    Unbundled builds are not available for macOS due to cross-compilation complexity.
    Please use the bundled version or build from source:
    
    # Use bundled version (recommended):
    curl -L -o tabdiff "https://github.com/peter-fm/tabdiff/releases/latest/download/tabdiff-macos-arm64-bundled"
    
    # Or build from source:
    cargo build --release --features bundled"#.to_string()
    } else {
        r#"  Linux:
    # Ubuntu/Debian
    sudo apt update
    sudo apt install duckdb
    
    # Or manually install from GitHub:
    wget https://github.com/duckdb/duckdb/releases/latest/download/libduckdb-linux-amd64.zip
    unzip libduckdb-linux-amd64.zip
    sudo cp libduckdb.so /usr/local/lib/
    sudo ldconfig
    
    # RHEL/CentOS/Fedora
    sudo yum install duckdb
    
    # Or using snap
    sudo snap install duckdb"#.to_string()
    }
}

/// Data processor for various file formats
pub struct DataProcessor {
    connection: Connection,
    chunk_size: usize,
    cached_columns: Option<Vec<ColumnInfo>>,
    streaming_query: Option<String>,
}

impl DataProcessor {
    /// Create a new data processor with default settings
    pub fn new() -> Result<Self> {
        Self::new_with_config(10000) // Default chunk size of 10K rows
    }

    /// Create a new data processor with custom configuration
    pub fn new_with_config(chunk_size: usize) -> Result<Self> {
        let connection = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(e) => {
                // Check if this is a DuckDB library loading error
                let error_msg = e.to_string();
                if error_msg.contains("libduckdb") || error_msg.contains("duckdb.dll") || error_msg.contains("cannot open shared object") {
                    let install_instructions = get_duckdb_install_instructions();
                    eprintln!("❌ DuckDB library not found!");
                    eprintln!();
                    eprintln!("This version of tabdiff requires DuckDB to be installed on your system.");
                    eprintln!();
                    eprintln!("📦 Install DuckDB:");
                    eprintln!("{}", install_instructions);
                    eprintln!();
                    eprintln!("💡 Alternatively, download the bundled version that includes DuckDB:");
                    eprintln!("   Visit: https://github.com/peter-fm/tabdiff/releases/latest");
                    eprintln!();
                    eprintln!("   For your platform, download the file ending with '-bundled' instead.");
                    eprintln!();
                    eprintln!("Original error: {}", error_msg);
                    std::process::exit(1);
                }
                return Err(e.into());
            }
        };
        
        // Optimize DuckDB for large datasets and performance
        connection.execute("SET memory_limit='4GB'", [])?;
        // Use all available CPU cores (DuckDB auto-detects if not specified)
        connection.execute("SET enable_progress_bar=false", [])?; // Disable for performance
        connection.execute("SET preserve_insertion_order=false", [])?; // Allow reordering for performance
        connection.execute("SET enable_object_cache=true", [])?; // Enable object caching
        
        Ok(Self { 
            connection, 
            chunk_size, 
            cached_columns: None,
            streaming_query: None,
        })
    }

    /// Load data from file and return basic info
    pub fn load_file(&mut self, file_path: &Path) -> Result<DataInfo> {
        // Check if this is a SQL file
        if sql::is_sql_file(file_path) {
            return self.load_sql_file(file_path);
        }
        
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

    /// Load data from SQL file with database connection
    pub fn load_sql_file(&mut self, file_path: &Path) -> Result<DataInfo> {
        // Load environment variables
        sql::load_env_file()?;
        
        // Parse the SQL file
        let sql_file = sql::parse_sql_file(file_path)?;
        
        // Substitute environment variables in the connection string
        let connection_string = sql::substitute_env_vars(&sql_file.connection_string)?;
        
        // Execute the connection string to attach the database (if provided)
        if !connection_string.is_empty() {
            self.connection.execute(&connection_string, [])
                .map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to execute connection string '{}': {}", connection_string, e)
                ))?;
        }
        
        // Parse the content again to get setup statements and the SELECT query
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| crate::error::TabdiffError::invalid_input(
                format!("Failed to read SQL file '{}': {}", file_path.display(), e)
            ))?;
        
        // Split by semicolons to get individual statements
        let statements: Vec<&str> = content.split(';').collect();
        let mut setup_statements = Vec::new();
        let mut select_query = String::new();
        
        for statement in statements {
            let trimmed = statement.trim();
            
            // Skip empty statements
            if trimmed.is_empty() {
                continue;
            }
            
            // Remove any comment lines from the statement
            let cleaned_statement = trimmed.lines()
                .filter(|line| !line.trim().starts_with("--") && !line.trim().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();
            
            if cleaned_statement.is_empty() {
                continue;
            }
            
            // Check if this is a SELECT query or CTE (Common Table Expression)
            let upper_statement = cleaned_statement.to_uppercase();
            if (upper_statement.starts_with("SELECT") || upper_statement.starts_with("WITH")) && 
               !upper_statement.contains("CREATE TABLE") {
                select_query = cleaned_statement;
            } else {
                setup_statements.push(cleaned_statement);
            }
        }
        
        // Execute setup statements first
        for statement in setup_statements {
            if !statement.is_empty() {
                self.connection.execute(&statement, [])
                    .map_err(|e| crate::error::TabdiffError::data_processing(
                        format!("Failed to execute setup statement '{}': {}", statement, e)
                    ))?;
            }
        }
        
        // For SQL queries, use streaming approach to handle large datasets efficiently
        if select_query.trim().is_empty() {
            return Err(crate::error::TabdiffError::invalid_input(
                format!("No SELECT query found in SQL file '{}'", file_path.display())
            ));
        }
        
        // First, get the row count and column info without materializing all data
        let count_query = format!("SELECT COUNT(*) FROM ({})", select_query.trim());
        let row_count: u64 = self.connection
            .prepare(&count_query)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare row count query: {}", e)
            ))?
            .query_row([], |row| row.get(0))
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to get row count: {}", e)
            ))?;
        
        // Get column information by creating a temporary view with LIMIT 0
        let temp_view_sql = format!(
            "CREATE OR REPLACE VIEW temp_schema_view AS SELECT * FROM ({}) AS query_result LIMIT 0",
            select_query.trim()
        );
        
        self.connection.execute(&temp_view_sql, [])
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to create temporary view for schema: {}", e)
            ))?;
        
        // Get column information from the temporary view and cache it
        let columns = self.get_column_info_from_view("temp_schema_view")?;
        
        // Cache the columns for streaming queries since we won't have data_view
        self.cached_columns = Some(columns.clone());
        
        // Clean up temporary view
        self.connection.execute("DROP VIEW IF EXISTS temp_schema_view", [])
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to drop temporary view: {}", e)
            ))?;
        
        // Store the original SELECT query for streaming use
        self.streaming_query = Some(select_query.trim().to_string());
        
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
        self.get_column_info_from_view("data_view")
    }

    /// Get column information from a specific view (cached to avoid repeated calls)
    fn get_column_info_from_view(&mut self, view_name: &str) -> Result<Vec<ColumnInfo>> {
        // Return cached columns if available (for data_view only)
        if view_name == "data_view" {
            if let Some(ref columns) = self.cached_columns {
                return Ok(columns.clone());
            }
        }

        // First, get column names in their original order using DESCRIBE
        let describe_sql = format!("DESCRIBE {}", view_name);
        let mut stmt = self.connection.prepare(&describe_sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare describe query for '{}': {}", view_name, e)
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
        
        // Cache the columns for future calls (only for data_view)
        if view_name == "data_view" {
            self.cached_columns = Some(columns.clone());
        }
        
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
        // Check if this is a streaming SQL query
        if let Some(ref query) = self.streaming_query.clone() {
            return self.extract_streaming_data_with_progress(query, progress_callback);
        }

        // Regular file-based extraction
        self.extract_regular_data_with_progress(progress_callback)
    }

    /// Extract data from regular files (CSV, Parquet, etc.) with chunking
    fn extract_regular_data_with_progress(
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

            let chunk_data = self.execute_chunk_query(&chunk_sql, column_count)?;
            processed_rows += chunk_data.len() as u64;
            all_data.extend(chunk_data);

            // Report progress if callback provided
            if let Some(callback) = progress_callback {
                callback(processed_rows, total_rows);
            }
        }
        
        Ok(all_data)
    }

    /// Extract data from streaming SQL queries with chunking for memory efficiency
    fn extract_streaming_data_with_progress(
        &mut self,
        query: &str,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<Vec<String>>> {
        // Get column information
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new());
        }

        // Get total row count without materializing data
        let count_query = format!("SELECT COUNT(*) FROM ({})", query);
        let total_rows: u64 = self.connection
            .prepare(&count_query)?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_data = Vec::new();
        let mut processed_rows = 0u64;

        // Use larger chunk sizes for streaming SQL queries since database can handle them efficiently
        let chunk_size = if total_rows > 10_000_000 {
            100_000 // Very large datasets: 100K rows per chunk
        } else if total_rows > 1_000_000 {
            50_000 // Large datasets: 50K rows per chunk
        } else if total_rows > 100_000 {
            25_000 // Medium datasets: 25K rows per chunk
        } else {
            self.chunk_size.min(total_rows as usize)
        };

        while processed_rows < total_rows {
            let current_chunk_size = chunk_size.min((total_rows - processed_rows) as usize);
            
            // Stream directly from the original query using subquery with LIMIT/OFFSET
            let chunk_sql = format!(
                "SELECT * FROM ({}) LIMIT {} OFFSET {}",
                query,
                current_chunk_size,
                processed_rows
            );

            let chunk_data = self.execute_chunk_query(&chunk_sql, column_count)?;
            processed_rows += chunk_data.len() as u64;
            all_data.extend(chunk_data);

            // Report progress if callback provided
            if let Some(callback) = progress_callback {
                callback(processed_rows, total_rows);
            }
        }
        
        Ok(all_data)
    }

    /// Execute a chunk query and return the results as string vectors
    fn execute_chunk_query(&mut self, sql: &str, column_count: usize) -> Result<Vec<Vec<String>>> {
        let mut stmt = self.connection.prepare(sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare chunk query: {}", e)
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
            format!("Failed to extract chunk data: {}", e)
        ))?;
        
        let mut chunk_data = Vec::new();
        for row in rows {
            chunk_data.push(row.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process chunk row: {}", e)
            ))?);
        }

        Ok(chunk_data)
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



    /// Compute row hashes directly in DuckDB for maximum performance (robust version)
    pub fn compute_row_hashes_sql(&mut self) -> Result<Vec<crate::hash::RowHash>> {
        self.compute_row_hashes_with_progress(None)
    }

    /// Compute row hashes with deterministic ordering and full hash precision
    pub fn compute_row_hashes_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        // Check if this is a streaming SQL query
        if let Some(ref query) = self.streaming_query.clone() {
            return self.compute_streaming_row_hashes_with_progress(query, progress_callback);
        }

        // Regular file-based row hashing
        self.compute_regular_row_hashes_with_progress(progress_callback)
    }

    /// Compute row hashes for regular files with chunking
    fn compute_regular_row_hashes_with_progress(
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

        let mut all_hashes = Vec::new();
        let mut processed_rows = 0u64;
        let start_time = std::time::Instant::now();
        
        // Use natural file order - no ORDER BY clause needed
        // DuckDB preserves the original row order from CSV files
        let column_list = columns.iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        
        let natural_order_sql = format!(
            "SELECT {} FROM data_view",
            column_list
        );
        
        let mut stmt = self.connection.prepare(&natural_order_sql)
            .map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to prepare natural order query: {}", e)
            ))?;

        let rows = stmt.query_map([], |row| {
            self.extract_row_values_for_hashing(row, &columns)
        }).map_err(|e| crate::error::TabdiffError::data_processing(
            format!("Failed to create row iterator: {}", e)
        ))?;

        // Process each row individually with immediate progress updates
        for (row_index, row_result) in rows.enumerate() {
            let row_values = row_result.map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to process row {}: {}", row_index, e)
            ))?;
            
            let hash_hex = self.compute_row_hash(&row_values);
            
            all_hashes.push(crate::hash::RowHash {
                row_index: row_index as u64,
                hash: hash_hex,
            });
            
            processed_rows += 1;
            
            self.report_hash_progress(processed_rows, total_rows, start_time, &progress_callback);
        }
        
        // Final newline after progress
        eprintln!();
        
        Ok(all_hashes)
    }

    /// Compute row hashes for streaming SQL queries with chunking for memory efficiency
    fn compute_streaming_row_hashes_with_progress(
        &mut self,
        query: &str,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Get total row count without materializing data
        let count_query = format!("SELECT COUNT(*) FROM ({})", query);
        let total_rows: u64 = self.connection
            .prepare(&count_query)?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_hashes = Vec::new();
        let mut processed_rows = 0u64;
        let start_time = std::time::Instant::now();

        // Use larger chunk sizes for streaming SQL queries
        let chunk_size = if total_rows > 10_000_000 {
            100_000 // Very large datasets: 100K rows per chunk
        } else if total_rows > 1_000_000 {
            50_000 // Large datasets: 50K rows per chunk
        } else if total_rows > 100_000 {
            25_000 // Medium datasets: 25K rows per chunk
        } else {
            self.chunk_size.min(total_rows as usize)
        };

        let column_list = columns.iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");

        while processed_rows < total_rows {
            let current_chunk_size = chunk_size.min((total_rows - processed_rows) as usize);
            
            // Stream directly from the original query using subquery with LIMIT/OFFSET
            let chunk_sql = format!(
                "SELECT {} FROM ({}) LIMIT {} OFFSET {}",
                column_list,
                query,
                current_chunk_size,
                processed_rows
            );

            let mut stmt = self.connection.prepare(&chunk_sql)
                .map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to prepare streaming hash query: {}", e)
                ))?;

            let rows = stmt.query_map([], |row| {
                self.extract_row_values_for_hashing(row, &columns)
            }).map_err(|e| crate::error::TabdiffError::data_processing(
                format!("Failed to create streaming row iterator: {}", e)
            ))?;

            // Process each row in the chunk
            for (chunk_row_index, row_result) in rows.enumerate() {
                let row_values = row_result.map_err(|e| crate::error::TabdiffError::data_processing(
                    format!("Failed to process streaming row {}: {}", chunk_row_index, e)
                ))?;
                
                let hash_hex = self.compute_row_hash(&row_values);
                
                all_hashes.push(crate::hash::RowHash {
                    row_index: processed_rows + chunk_row_index as u64,
                    hash: hash_hex,
                });
            }

            processed_rows += current_chunk_size as u64;
            self.report_hash_progress(processed_rows, total_rows, start_time, &progress_callback);
        }
        
        // Final newline after progress
        eprintln!();
        
        Ok(all_hashes)
    }

    /// Extract row values for hashing with consistent formatting
    fn extract_row_values_for_hashing(&self, row: &duckdb::Row, columns: &[ColumnInfo]) -> duckdb::Result<Vec<String>> {
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
                Ok(duckdb::types::ValueRef::Float(f)) => {
                    // Use consistent float formatting to avoid precision issues
                    format!("{:.10}", f)
                },
                Ok(duckdb::types::ValueRef::Double(f)) => {
                    // Use consistent double formatting to avoid precision issues
                    format!("{:.15}", f)
                },
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
    }

    /// Compute hash for a row's values
    fn compute_row_hash(&self, row_values: &[String]) -> String {
        // Hash the row values using Blake3 with consistent separator
        let row_content = row_values.join("||"); // Use || to avoid conflicts with | in data
        let hash = blake3::hash(row_content.as_bytes());
        
        // Use full Blake3 hash for maximum collision resistance
        hash.to_hex().to_string()
    }

    /// Report progress for hash computation
    fn report_hash_progress(
        &self,
        processed_rows: u64,
        total_rows: u64,
        start_time: std::time::Instant,
        progress_callback: &Option<&dyn Fn(u64, u64)>,
    ) {
        // Real-time progress updates - every 10000 rows for large files, every 1000 for smaller
        let update_frequency = if total_rows > 1_000_000 { 10000 } else { 1000 };
        
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

        // Preserve original column order - don't sort alphabetically
        Ok(column_hashes)
    }



    /// Check if file format is supported
    pub fn is_supported_format(file_path: &Path) -> bool {
        if let Some(extension) = file_path.extension().and_then(|s| s.to_str()) {
            matches!(extension.to_lowercase().as_str(), 
                     "csv" | "parquet" | "json" | "jsonl" | "tsv" | "sql")
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
        assert!(DataProcessor::is_supported_format(Path::new("test.sql")));
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
