//! Edge case tests for data-related scenarios

use crate::common::{CliTestRunner, sample_data};
use std::fs;

#[test]
fn test_csv_with_malformed_quotes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with malformed quotes
    let malformed_csv = r#"id,name,description
1,"Product A","Good product"
2,"Product B,"Missing closing quote
3,Product C,"Normal product"
"#;
    
    let csv_path = runner.fixture().root().join("malformed.csv");
    fs::write(&csv_path, malformed_csv).unwrap();
    
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "malformed"]);
    assert!(error.to_string().contains("parse") || error.to_string().contains("CSV") || error.to_string().contains("quote"));
}

#[test]
fn test_csv_with_inconsistent_columns() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with inconsistent number of columns
    let inconsistent_csv = r#"id,name,price
1,Product A,19.99
2,Product B,29.99,Extra Column
3,Product C
4,Product D,39.99,Extra,Even More
"#;
    
    let csv_path = runner.fixture().root().join("inconsistent.csv");
    fs::write(&csv_path, inconsistent_csv).unwrap();
    
    // Should handle inconsistent columns gracefully or fail with clear error
    let result = runner.run_command(&["snapshot", csv_path.to_str().unwrap(), "--name", "inconsistent"]);
    
    match result {
        Ok(_) => {
            // If it succeeds, verify snapshot was created
            runner.fixture().assert_snapshot_exists("inconsistent");
        }
        Err(error) => {
            // If it fails, should have clear error message
            assert!(error.to_string().contains("column") || error.to_string().contains("field") || error.to_string().contains("inconsistent"));
        }
    }
}

#[test]
fn test_csv_with_null_bytes() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with null bytes
    let null_csv = "id,name\n1,Product\x00A\n2,Product B\n";
    
    let csv_path = runner.fixture().root().join("null_bytes.csv");
    fs::write(&csv_path, null_csv.as_bytes()).unwrap();
    
    let error = runner.expect_failure(&["snapshot", csv_path.to_str().unwrap(), "--name", "null_bytes"]);
    assert!(error.to_string().contains("null") || error.to_string().contains("invalid") || error.to_string().contains("UTF-8"));
}

#[test]
fn test_csv_with_very_long_lines() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with very long line
    let long_value = "x".repeat(100000);
    let long_csv = format!("id,name,description\n1,Product A,{}\n2,Product B,Short description\n", long_value);
    
    let csv_path = runner.fixture().root().join("long_lines.csv");
    fs::write(&csv_path, long_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "long_lines"]);
    runner.fixture().assert_snapshot_exists("long_lines");
}

#[test]
fn test_csv_with_many_columns() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with many columns (1000 columns)
    let mut headers = Vec::new();
    let mut values = Vec::new();
    
    for i in 0..1000 {
        headers.push(format!("col_{}", i));
        values.push(format!("val_{}", i));
    }
    
    let many_cols_csv = format!("{}\n{}\n", headers.join(","), values.join(","));
    
    let csv_path = runner.fixture().root().join("many_columns.csv");
    fs::write(&csv_path, many_cols_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "many_columns"]);
    runner.fixture().assert_snapshot_exists("many_columns");
}

#[test]
fn test_csv_with_unicode_bom() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with UTF-8 BOM
    let bom_csv = "\u{FEFF}id,name,price\n1,Product A,19.99\n2,Product B,29.99\n";
    
    let csv_path = runner.fixture().root().join("bom.csv");
    fs::write(&csv_path, bom_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "bom_test"]);
    runner.fixture().assert_snapshot_exists("bom_test");
}

#[test]
fn test_csv_with_different_line_endings() {
    let runner = CliTestRunner::new().unwrap();
    
    // Test different line endings
    let line_ending_variants = vec![
        ("unix.csv", "id,name\n1,Product A\n2,Product B\n"),           // Unix (LF)
        ("windows.csv", "id,name\r\n1,Product A\r\n2,Product B\r\n"), // Windows (CRLF)
        ("mac.csv", "id,name\r1,Product A\r2,Product B\r"),           // Old Mac (CR)
        ("mixed.csv", "id,name\n1,Product A\r\n2,Product B\r"),       // Mixed
    ];
    
    for (filename, content) in line_ending_variants {
        let csv_path = runner.fixture().root().join(filename);
        fs::write(&csv_path, content).unwrap();
        
        let snapshot_name = filename.replace(".csv", "_snapshot");
        runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", &snapshot_name]);
        runner.fixture().assert_snapshot_exists(&snapshot_name);
    }
}

#[test]
fn test_csv_with_different_delimiters() {
    let runner = CliTestRunner::new().unwrap();
    
    // Test different delimiters (though tabdiff might not support all)
    let delimiter_variants = vec![
        ("comma.csv", "id,name,price\n1,Product A,19.99\n"),
        ("semicolon.csv", "id;name;price\n1;Product A;19.99\n"),
        ("tab.tsv", "id\tname\tprice\n1\tProduct A\t19.99\n"),
        ("pipe.csv", "id|name|price\n1|Product A|19.99\n"),
    ];
    
    for (filename, content) in delimiter_variants {
        let csv_path = runner.fixture().root().join(filename);
        fs::write(&csv_path, content).unwrap();
        
        let snapshot_name = filename.replace(".csv", "_snapshot").replace(".tsv", "_snapshot");
        
        // Some delimiters might not be supported
        let result = runner.run_command(&["snapshot", csv_path.to_str().unwrap(), "--name", &snapshot_name]);
        
        match result {
            Ok(_) => runner.fixture().assert_snapshot_exists(&snapshot_name),
            Err(_) => {
                // Some delimiters might not be auto-detected
                // This is expected behavior
            }
        }
    }
}

#[test]
fn test_json_with_nested_structures() {
    let runner = CliTestRunner::new().unwrap();
    
    let json_path = runner.fixture().create_json("nested.json", &sample_data::nested_json_data()).unwrap();
    
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "nested_json"]);
    runner.fixture().assert_snapshot_exists("nested_json");
}

#[test]
fn test_json_with_arrays() {
    let runner = CliTestRunner::new().unwrap();
    
    let array_json = serde_json::json!([
        {"id": 1, "tags": ["tag1", "tag2", "tag3"]},
        {"id": 2, "tags": ["tag4"]},
        {"id": 3, "tags": []}
    ]);
    
    let json_path = runner.fixture().create_json("arrays.json", &array_json).unwrap();
    
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "array_json"]);
    runner.fixture().assert_snapshot_exists("array_json");
}

#[test]
fn test_json_with_mixed_types() {
    let runner = CliTestRunner::new().unwrap();
    
    let mixed_json = serde_json::json!([
        {"id": 1, "value": "string", "active": true, "score": 95.5},
        {"id": 2, "value": 42, "active": false, "score": null},
        {"id": 3, "value": null, "active": null, "score": "invalid"}
    ]);
    
    let json_path = runner.fixture().create_json("mixed_types.json", &mixed_json).unwrap();
    
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "mixed_json"]);
    runner.fixture().assert_snapshot_exists("mixed_json");
}

#[test]
fn test_malformed_json() {
    let runner = CliTestRunner::new().unwrap();
    
    let malformed_json = r#"{"id": 1, "name": "Product A", "price": 19.99,}"#; // Trailing comma
    
    let json_path = runner.fixture().root().join("malformed.json");
    fs::write(&json_path, malformed_json).unwrap();
    
    let error = runner.expect_failure(&["snapshot", json_path.to_str().unwrap(), "--name", "malformed_json"]);
    assert!(error.to_string().contains("JSON") || error.to_string().contains("parse") || error.to_string().contains("invalid"));
}

#[test]
fn test_very_large_json_values() {
    let runner = CliTestRunner::new().unwrap();
    
    let large_value = "x".repeat(100000);
    let large_json = serde_json::json!([
        {"id": 1, "data": large_value},
        {"id": 2, "data": "small"}
    ]);
    
    let json_path = runner.fixture().create_json("large_values.json", &large_json).unwrap();
    
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "large_json"]);
    runner.fixture().assert_snapshot_exists("large_json");
}

#[test]
fn test_deeply_nested_json() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create deeply nested JSON structure
    let mut nested = serde_json::json!({"value": "deep"});
    for i in 0..100 {
        nested = serde_json::json!({"level": i, "nested": nested});
    }
    
    let deep_json = serde_json::json!([nested]);
    
    let json_path = runner.fixture().create_json("deep_nested.json", &deep_json).unwrap();
    
    runner.expect_success(&["snapshot", json_path.to_str().unwrap(), "--name", "deep_json"]);
    runner.fixture().assert_snapshot_exists("deep_json");
}

#[test]
fn test_csv_with_all_null_values() {
    let runner = CliTestRunner::new().unwrap();
    
    let null_csv = "id,name,price\n,,,\n,,,\n,,,\n";
    
    let csv_path = runner.fixture().root().join("all_nulls.csv");
    fs::write(&csv_path, null_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "all_nulls"]);
    runner.fixture().assert_snapshot_exists("all_nulls");
}

#[test]
fn test_csv_with_duplicate_headers() {
    let runner = CliTestRunner::new().unwrap();
    
    let duplicate_headers_csv = "id,name,name,price\n1,Product A,Product A,19.99\n2,Product B,Product B,29.99\n";
    
    let csv_path = runner.fixture().root().join("duplicate_headers.csv");
    fs::write(&csv_path, duplicate_headers_csv).unwrap();
    
    // Should handle duplicate headers gracefully or fail with clear error
    let result = runner.run_command(&["snapshot", csv_path.to_str().unwrap(), "--name", "duplicate_headers"]);
    
    match result {
        Ok(_) => runner.fixture().assert_snapshot_exists("duplicate_headers"),
        Err(error) => {
            assert!(error.to_string().contains("duplicate") || error.to_string().contains("header") || error.to_string().contains("column"));
        }
    }
}

#[test]
fn test_csv_with_numeric_precision() {
    let runner = CliTestRunner::new().unwrap();
    
    let precision_csv = r#"id,small_decimal,large_decimal,scientific
1,0.000000001,123456789.987654321,1.23e-10
2,0.1,999999999.999999999,9.99e+20
3,-0.000001,-123456789.123456789,-1.5e-5
"#;
    
    let csv_path = runner.fixture().root().join("precision.csv");
    fs::write(&csv_path, precision_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "precision_test"]);
    runner.fixture().assert_snapshot_exists("precision_test");
}

#[test]
fn test_csv_with_special_float_values() {
    let runner = CliTestRunner::new().unwrap();
    
    let special_floats_csv = r#"id,value,description
1,inf,Positive infinity
2,-inf,Negative infinity
3,nan,Not a number
4,1.7976931348623157e+308,Max float
5,2.2250738585072014e-308,Min positive float
"#;
    
    let csv_path = runner.fixture().root().join("special_floats.csv");
    fs::write(&csv_path, special_floats_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "special_floats"]);
    runner.fixture().assert_snapshot_exists("special_floats");
}

#[test]
fn test_csv_with_date_formats() {
    let runner = CliTestRunner::new().unwrap();
    
    let date_csv = r#"id,date_iso,date_us,date_eu,timestamp
1,2023-12-25,12/25/2023,25/12/2023,2023-12-25T10:30:00Z
2,2024-01-01,01/01/2024,01/01/2024,2024-01-01T00:00:00.000Z
3,invalid-date,13/32/2023,32/13/2023,not-a-timestamp
"#;
    
    let csv_path = runner.fixture().root().join("dates.csv");
    fs::write(&csv_path, date_csv).unwrap();
    
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "date_formats"]);
    runner.fixture().assert_snapshot_exists("date_formats");
}

#[test]
fn test_binary_file_as_csv() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create a binary file with .csv extension
    let binary_data = vec![0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD];
    let binary_path = runner.fixture().root().join("binary.csv");
    fs::write(&binary_path, binary_data).unwrap();
    
    let error = runner.expect_failure(&["snapshot", binary_path.to_str().unwrap(), "--name", "binary"]);
    assert!(error.to_string().contains("UTF-8") || error.to_string().contains("invalid") || error.to_string().contains("binary"));
}

#[test]
fn test_extremely_large_single_row() {
    let runner = CliTestRunner::new().unwrap();
    
    // Create CSV with one extremely large row
    let mut large_row = vec!["id".to_string()];
    let mut large_values = vec!["1".to_string()];
    
    // Add 10000 columns
    for i in 0..10000 {
        large_row.push(format!("col_{}", i));
        large_values.push(format!("value_{}", i));
    }
    
    let large_csv = format!("{}\n{}\n", large_row.join(","), large_values.join(","));
    
    let csv_path = runner.fixture().root().join("large_row.csv");
    fs::write(&csv_path, large_csv).unwrap();
    
    // Use sampling to make this test faster
    runner.expect_success(&[
        "snapshot", csv_path.to_str().unwrap(), 
        "--name", "large_row", 
        "--sample", "100"
    ]);
    runner.fixture().assert_snapshot_exists("large_row");
}
