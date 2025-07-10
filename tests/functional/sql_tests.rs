use crate::common::CliTestRunner;
use std::fs;
use std::path::Path;

#[test]
fn test_sql_file_basic_functionality() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create a basic SQL file with test data
    let sql_content = r#"
-- Test SQL file with in-memory data
CREATE TABLE test_products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    category VARCHAR(50)
);

INSERT INTO test_products (id, name, price, category) VALUES
    (1, 'Widget A', 19.99, 'Widgets'),
    (2, 'Gadget B', 29.99, 'Gadgets'),
    (3, 'Tool C', 39.99, 'Tools');

SELECT id, name, price, category 
FROM test_products 
ORDER BY id;
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("test_products.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "sql_baseline"
    ]);
    
    // Verify metadata file exists and has correct content
    // Verify snapshot exists
    runner.fixture().assert_snapshot_exists("sql_baseline");
    
    // Verify metadata file exists and has correct content
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("sql_baseline");
    let metadata_content = fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&metadata_content).unwrap();
    
    assert_eq!(metadata["name"], "sql_baseline");
    assert_eq!(metadata["row_count"], 3);
    assert_eq!(metadata["column_count"], 4);
}

#[test]
fn test_sql_file_with_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create initial SQL file
    let sql_v1_content = r#"
CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INTEGER,
    last_updated DATE
);

INSERT INTO inventory (product_id, product_name, quantity, last_updated) VALUES
    (1, 'Widget A', 25, '2025-01-15'),
    (2, 'Widget B', 50, '2025-01-16'),
    (3, 'Gadget X', 75, '2025-01-17');

SELECT product_id, product_name, quantity, last_updated 
FROM inventory 
ORDER BY product_id;
"#;
    
    let sql_v1_path = runner.fixture().temp_dir.path().join("inventory_v1.sql");
    fs::write(&sql_v1_path, sql_v1_content).unwrap();
    
    // Create modified SQL file with quantity changes and new product
    let sql_v2_content = r#"
CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INTEGER,
    last_updated DATE
);

INSERT INTO inventory (product_id, product_name, quantity, last_updated) VALUES
    (1, 'Widget A', 35, '2025-01-15'),    -- quantity changed: 25 -> 35
    (2, 'Widget B', 45, '2025-01-16'),    -- quantity changed: 50 -> 45
    (3, 'Gadget X', 75, '2025-01-17'),    -- unchanged
    (4, 'New Product', 60, '2025-01-20'); -- new product

SELECT product_id, product_name, quantity, last_updated 
FROM inventory 
ORDER BY product_id;
"#;
    
    let sql_v2_path = runner.fixture().temp_dir.path().join("inventory_v2.sql");
    fs::write(&sql_v2_path, sql_v2_content).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot",
        sql_v1_path.to_str().unwrap(),
        "--name",
        "inventory_baseline"
    ]);
    
    runner.expect_success(&[
        "snapshot",
        sql_v2_path.to_str().unwrap(),
        "--name",
        "inventory_updated"
    ]);
    
    // Run diff
    runner.expect_success(&[
        "diff",
        "inventory_baseline",
        "inventory_updated"
    ]);
    
    // Verify both snapshots exist
    runner.fixture().assert_snapshot_exists("inventory_baseline");
    runner.fixture().assert_snapshot_exists("inventory_updated");
}

#[test]
fn test_sql_file_schema_changes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create initial SQL file with 3 columns
    let sql_v1_content = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

INSERT INTO users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com');

SELECT id, name, email FROM users ORDER BY id;
"#;
    
    let sql_v1_path = runner.fixture().temp_dir.path().join("users_v1.sql");
    fs::write(&sql_v1_path, sql_v1_content).unwrap();
    
    // Create modified SQL file with additional column
    let sql_v2_content = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    created_at DATE
);

INSERT INTO users (id, name, email, created_at) VALUES
    (1, 'Alice', 'alice@example.com', '2025-01-01'),
    (2, 'Bob', 'bob@example.com', '2025-01-02');

SELECT id, name, email, created_at FROM users ORDER BY id;
"#;
    
    let sql_v2_path = runner.fixture().temp_dir.path().join("users_v2.sql");
    fs::write(&sql_v2_path, sql_v2_content).unwrap();
    
    // Create snapshots
    runner.expect_success(&[
        "snapshot",
        sql_v1_path.to_str().unwrap(),
        "--name",
        "users_schema_v1"
    ]);
    
    runner.expect_success(&[
        "snapshot",
        sql_v2_path.to_str().unwrap(),
        "--name",
        "users_schema_v2"
    ]);
    
    // Run diff to detect schema changes
    runner.expect_success(&[
        "diff",
        "users_schema_v1",
        "users_schema_v2"
    ]);
    
    // Verify snapshots exist
    runner.fixture().assert_snapshot_exists("users_schema_v1");
    runner.fixture().assert_snapshot_exists("users_schema_v2");
}

#[test]
fn test_sql_file_with_complex_query() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with complex query including JOINs and aggregations
    let sql_content = r#"
-- Create multiple related tables
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    category_id INTEGER,
    price DECIMAL(10,2)
);

INSERT INTO categories (id, name) VALUES
    (1, 'Electronics'),
    (2, 'Books');

INSERT INTO products (id, name, category_id, price) VALUES
    (1, 'Laptop', 1, 999.99),
    (2, 'Mouse', 1, 29.99),
    (3, 'Novel', 2, 15.99),
    (4, 'Textbook', 2, 89.99);

-- Complex query with JOIN and aggregation
SELECT 
    c.name as category,
    COUNT(p.id) as product_count,
    AVG(p.price) as avg_price,
    MIN(p.price) as min_price,
    MAX(p.price) as max_price
FROM categories c
LEFT JOIN products p ON c.id = p.category_id
GROUP BY c.id, c.name
ORDER BY c.name;
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("complex_query.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "complex_query_snapshot"
    ]);
    
    // Verify snapshot exists
    runner.fixture().assert_snapshot_exists("complex_query_snapshot");
}

#[test]
fn test_sql_file_empty_result() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file that returns no rows
    let sql_content = r#"
CREATE TABLE empty_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

-- Query empty table
SELECT id, name FROM empty_table WHERE id > 0;
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("empty_result.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "empty_snapshot"
    ]);
    
    // Verify snapshot exists
    runner.fixture().assert_snapshot_exists("empty_snapshot");
}

#[test]
fn test_sql_file_error_handling() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with syntax error
    let invalid_sql_content = r#"
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

-- Invalid SQL query
SELECT FROM test_table; -- Missing column list
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("invalid.sql");
    fs::write(&sql_path, invalid_sql_content).unwrap();
    
    // Attempt to create snapshot - should fail
    let error = runner.expect_failure(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "invalid_snapshot"
    ]);
    
    // Should contain error about syntax
    let error_msg = error.to_string();
    assert!(error_msg.contains("Error") || error_msg.contains("syntax error") || error_msg.contains("Parser Error"));
}

#[test]
fn test_sql_file_no_select_query() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with only setup statements, no SELECT
    let sql_content = r#"
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO test_table (id, name) VALUES (1, 'Test');
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("no_select.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Attempt to create snapshot - should fail
    let error = runner.expect_failure(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "no_select_snapshot"
    ]);
    
    // Should contain error about missing SELECT query
    let error_msg = error.to_string();
    assert!(error_msg.contains("No SELECT query found"));
}

#[test]
fn test_sql_file_with_comments() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with extensive comments
    let sql_content = r#"
-- This is a test SQL file
-- Testing comment handling

/* Multi-line comment
   across multiple lines */

CREATE TABLE products (
    id INTEGER PRIMARY KEY, -- Product ID
    name VARCHAR(100), -- Product name
    price DECIMAL(10,2) -- Product price
);

-- Insert some test data
INSERT INTO products (id, name, price) VALUES
    (1, 'Widget', 19.99), -- First product
    (2, 'Gadget', 29.99); -- Second product

-- Final query to get the data
SELECT 
    id,     -- Product identifier
    name,   -- Product name  
    price   -- Product price
FROM products 
ORDER BY id; -- Sort by ID
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("with_comments.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "comments_snapshot"
    ]);
    
    // Verify snapshot exists
    runner.fixture().assert_snapshot_exists("comments_snapshot");
}

#[test]
fn test_sql_supported_format() {
    use tabdiff::data::DataProcessor;
    
    // Test that .sql files are recognized as supported format
    assert!(DataProcessor::is_supported_format(Path::new("test.sql")));
    assert!(DataProcessor::is_supported_format(Path::new("query.SQL")));
    assert!(DataProcessor::is_supported_format(Path::new("data.sql")));
    
    // Test that other formats still work
    assert!(DataProcessor::is_supported_format(Path::new("test.csv")));
    assert!(DataProcessor::is_supported_format(Path::new("test.parquet")));
    
    // Test unsupported formats
    assert!(!DataProcessor::is_supported_format(Path::new("test.txt")));
    assert!(!DataProcessor::is_supported_format(Path::new("test")));
}

#[test]
fn test_sql_streaming_large_dataset() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with large dataset using CTE and generate_series
    let sql_content = r#"
-- Test streaming with larger dataset to trigger chunking behavior
WITH large_dataset AS (
    SELECT 
        row_number() OVER () as id,
        'Product_' || (row_number() OVER ()) as name,
        (random() * 100 + 10)::INT as quantity,
        '2025-01-' || (1 + (row_number() OVER () % 28))::VARCHAR as date_field
    FROM generate_series(1, 30000)
)
SELECT id, name, quantity, date_field 
FROM large_dataset 
ORDER BY id;
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("large_streaming.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot - should use streaming internally for large dataset
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "large_streaming_snapshot"
    ]);
    
    // Verify snapshot exists and has correct metadata
    runner.fixture().assert_snapshot_exists("large_streaming_snapshot");
    
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("large_streaming_snapshot");
    let metadata_content = fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&metadata_content).unwrap();
    
    assert_eq!(metadata["name"], "large_streaming_snapshot");
    assert_eq!(metadata["row_count"], 30000);
    assert_eq!(metadata["column_count"], 4);
}

#[test]
fn test_sql_cte_support() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create SQL file with Common Table Expression (CTE)
    let sql_content = r#"
-- Test CTE support in SQL parsing
WITH product_categories AS (
    SELECT * FROM VALUES 
        (1, 'Electronics'),
        (2, 'Books'),
        (3, 'Clothing')
    AS t(category_id, category_name)
),
products AS (
    SELECT * FROM VALUES 
        (1, 'Laptop', 1, 999.99),
        (2, 'Mouse', 1, 29.99),
        (3, 'Novel', 2, 15.99)
    AS t(product_id, product_name, category_id, price)
)
SELECT 
    p.product_id,
    p.product_name,
    c.category_name,
    p.price
FROM products p
JOIN product_categories c ON p.category_id = c.category_id
ORDER BY p.product_id;
"#;
    
    let sql_path = runner.fixture().temp_dir.path().join("cte_test.sql");
    fs::write(&sql_path, sql_content).unwrap();
    
    // Create snapshot
    runner.expect_success(&[
        "snapshot",
        sql_path.to_str().unwrap(),
        "--name",
        "cte_snapshot"
    ]);
    
    // Verify snapshot exists
    runner.fixture().assert_snapshot_exists("cte_snapshot");
    
    // Verify metadata
    let (_, json_path) = runner.fixture().workspace.snapshot_paths("cte_snapshot");
    let metadata_content = fs::read_to_string(&json_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&metadata_content).unwrap();
    
    assert_eq!(metadata["name"], "cte_snapshot");
    assert_eq!(metadata["row_count"], 3);
    assert_eq!(metadata["column_count"], 4);
}