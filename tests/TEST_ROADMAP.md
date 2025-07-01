# Tabdiff Test Roadmap

This document outlines the comprehensive test coverage roadmap for ensuring tabdiff can handle any possible change to a table structure or data.

## Current Status ✅

### Implemented (Phase 1 - Basic Table Changes)
- ✅ Row operations (add single/multiple, delete single/multiple, delete all)
- ✅ Cell value changes (single cell, multiple cells)
- ✅ Column operations (reorder, rename single/multiple, type changes)
- ✅ Mixed simultaneous changes
- ✅ Case sensitivity handling
- ✅ Large dataset sampling
- ✅ Output format validation
- ✅ Status command integration
- ✅ Verbose output testing

**Total: 20 tests implemented**

---

## Phase 2 - Column Structure Evolution 🚧

### Priority: HIGH
**Target: 15 additional tests**

#### 2.1 Column Addition Patterns
- [ ] `test_add_column_at_beginning()` - New column as first column
- [ ] `test_add_column_in_middle()` - New column between existing columns  
- [ ] `test_add_column_at_end()` - New column as last column
- [ ] `test_add_multiple_columns_scattered()` - Multiple new columns at different positions
- [ ] `test_add_column_with_default_values()` - New column with populated default data

#### 2.2 Column Removal Patterns
- [ ] `test_remove_first_column()` - Remove leftmost column
- [ ] `test_remove_middle_column()` - Remove column from middle
- [ ] `test_remove_last_column()` - Remove rightmost column
- [ ] `test_remove_multiple_columns()` - Remove several columns at once
- [ ] `test_remove_all_but_one_column()` - Extreme column reduction

#### 2.3 Advanced Data Type Conversions
- [ ] `test_string_to_number_conversion()` - Text data becomes numeric
- [ ] `test_number_to_string_conversion()` - Numeric data becomes text
- [ ] `test_date_format_changes()` - Different date/time representations
- [ ] `test_boolean_to_string_conversion()` - Boolean values become text
- [ ] `test_precision_changes()` - Decimal precision modifications

---

## Phase 3 - Data Quality and Integrity 🔍

### Priority: HIGH
**Target: 12 additional tests**

#### 3.1 Null Value Handling
- [ ] `test_introduce_null_values()` - Values become null/empty
- [ ] `test_remove_null_values()` - Null values get populated
- [ ] `test_null_to_value_conversion()` - Specific null→value changes
- [ ] `test_value_to_null_conversion()` - Specific value→null changes

#### 3.2 Data Integrity Changes
- [ ] `test_duplicate_row_introduction()` - Identical rows added
- [ ] `test_duplicate_row_removal()` - Duplicate cleanup
- [ ] `test_data_validation_failures()` - Invalid data introduction
- [ ] `test_data_cleanup_operations()` - Data quality improvements

#### 3.3 Encoding and Character Changes
- [ ] `test_encoding_changes()` - UTF-8↔ASCII conversions
- [ ] `test_special_character_handling()` - Unicode, emojis, symbols
- [ ] `test_control_character_changes()` - Tabs, newlines, special chars
- [ ] `test_case_sensitivity_data_changes()` - Case modifications in data

---

## Phase 4 - Scale and Performance Testing 📊

### Priority: MEDIUM
**Target: 10 additional tests**

#### 4.1 Massive Data Changes
- [ ] `test_10x_data_growth()` - 10x row increase with sampling
- [ ] `test_100x_data_growth()` - 100x row increase with sampling
- [ ] `test_90_percent_data_reduction()` - Massive row deletion
- [ ] `test_wide_table_expansion()` - Adding 50+ columns
- [ ] `test_million_row_changes()` - Large dataset modifications

#### 4.2 Performance Benchmarks
- [ ] `test_diff_performance_baseline()` - Performance baseline measurement
- [ ] `test_memory_usage_large_diffs()` - Memory consumption tracking
- [ ] `test_sampling_strategy_effectiveness()` - Sampling accuracy validation
- [ ] `test_incremental_diff_performance()` - Progressive change detection
- [ ] `test_concurrent_diff_operations()` - Multiple simultaneous diffs

---

## Phase 5 - Schema Evolution Patterns 🔄

### Priority: MEDIUM
**Target: 12 additional tests**

#### 5.1 Structural Transformations
- [ ] `test_table_normalization()` - Denormalized→normalized structure
- [ ] `test_table_denormalization()` - Normalized→denormalized structure
- [ ] `test_pivot_operation_detection()` - Wide→long format transformation
- [ ] `test_unpivot_operation_detection()` - Long→wide format transformation

#### 5.2 Business Schema Changes
- [ ] `test_audit_field_addition()` - Adding created_by, modified_by fields
- [ ] `test_versioning_field_addition()` - Adding version tracking columns
- [ ] `test_soft_delete_pattern()` - Adding is_deleted, deleted_at fields
- [ ] `test_timestamp_field_modifications()` - Timezone, format changes

#### 5.3 Calculated and Derived Fields
- [ ] `test_calculated_field_addition()` - New computed columns
- [ ] `test_calculated_field_logic_change()` - Modified computation logic
- [ ] `test_aggregation_level_changes()` - Daily→hourly, summary→detail
- [ ] `test_derived_field_removal()` - Removing computed columns

---

## Phase 6 - Cross-Format Compatibility 🔀

### Priority: MEDIUM
**Target: 8 additional tests**

#### 6.1 Format Transitions
- [ ] `test_csv_to_json_structure_change()` - Flat→nested structure
- [ ] `test_json_to_parquet_optimization()` - Schema inference changes
- [ ] `test_mixed_format_logical_comparison()` - Same data, different formats
- [ ] `test_format_specific_features()` - Format-unique capabilities

#### 6.2 Schema Inference Changes
- [ ] `test_automatic_type_detection_changes()` - Inference algorithm updates
- [ ] `test_nested_structure_flattening()` - JSON→flat table conversion
- [ ] `test_array_field_handling()` - Array/list column changes
- [ ] `test_object_field_serialization()` - Complex type handling

---

## Phase 7 - Business Logic and Domain Changes 💼

### Priority: LOW-MEDIUM
**Target: 10 additional tests**

#### 7.1 Domain-Specific Changes
- [ ] `test_currency_conversion_changes()` - USD→EUR, currency updates
- [ ] `test_unit_conversion_changes()` - Meters→feet, unit changes
- [ ] `test_category_taxonomy_updates()` - Classification system changes
- [ ] `test_code_standardization_changes()` - Country codes, standards

#### 7.2 Temporal and Versioning
- [ ] `test_timezone_conversion_changes()` - UTC→local time changes
- [ ] `test_date_granularity_changes()` - Daily→hourly→minute precision
- [ ] `test_historical_data_backfill()` - Adding historical records
- [ ] `test_data_retention_policy_changes()` - Removing old data

#### 7.3 Regulatory and Compliance
- [ ] `test_data_anonymization_changes()` - PII removal/masking
- [ ] `test_gdpr_compliance_modifications()` - Privacy-related changes
- [ ] `test_audit_trail_requirements()` - Compliance field additions

---

## Phase 8 - Edge Cases and Boundary Conditions ⚠️

### Priority: LOW
**Target: 15 additional tests**

#### 8.1 Extreme Scenarios
- [ ] `test_empty_to_populated_transition()` - Empty table→data
- [ ] `test_populated_to_empty_transition()` - Data→empty table
- [ ] `test_single_row_table_changes()` - Minimal data modifications
- [ ] `test_single_column_table_changes()` - Minimal structure changes
- [ ] `test_extremely_long_text_fields()` - Large text handling

#### 8.2 Statistical and Distribution Changes
- [ ] `test_normal_to_skewed_distribution()` - Data distribution changes
- [ ] `test_outlier_introduction()` - Statistical anomaly detection
- [ ] `test_outlier_removal()` - Anomaly cleanup detection
- [ ] `test_sampling_bias_changes()` - Different sampling strategies

#### 8.3 Complex Data Patterns
- [ ] `test_hierarchical_data_changes()` - Tree/nested structure changes
- [ ] `test_time_series_pattern_changes()` - Temporal pattern modifications
- [ ] `test_relational_integrity_changes()` - Foreign key-like relationships
- [ ] `test_composite_key_changes()` - Multi-column identifier changes

#### 8.4 Error Handling and Recovery
- [ ] `test_corrupted_data_handling()` - Malformed data detection
- [ ] `test_partial_file_comparison()` - Incomplete data scenarios
- [ ] `test_memory_limit_scenarios()` - Resource constraint handling

---

## Phase 9 - Real-World Migration Scenarios 🌍

### Priority: LOW
**Target: 8 additional tests**

#### 9.1 Database Migration Patterns
- [ ] `test_legacy_to_modern_schema()` - Old→new database schema
- [ ] `test_star_to_snowflake_schema()` - Data warehouse evolution
- [ ] `test_api_version_migration()` - v1→v2 response format changes
- [ ] `test_etl_pipeline_changes()` - Transformation logic updates

#### 9.2 System Integration Changes
- [ ] `test_microservice_data_evolution()` - Service boundary changes
- [ ] `test_data_lake_to_warehouse()` - Architecture migration patterns
- [ ] `test_batch_to_streaming_changes()` - Processing model changes
- [ ] `test_cloud_migration_patterns()` - On-premise→cloud data changes

---

## Implementation Guidelines

### Test Structure Standards
```rust
#[test]
fn test_specific_change_scenario() {
    let runner = CliTestRunner::new().unwrap();
    
    // 1. Create initial data state
    let initial_data = create_initial_test_data();
    let csv_path = runner.fixture().create_csv("test.csv", &initial_data).unwrap();
    
    // 2. Take initial snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "before"]);
    
    // 3. Modify data to represent the change
    let modified_data = apply_specific_change(&initial_data);
    runner.fixture().update_csv("test.csv", &modified_data).unwrap();
    
    // 4. Take second snapshot
    runner.expect_success(&["snapshot", csv_path.to_str().unwrap(), "--name", "after"]);
    
    // 5. Generate and validate diff
    let diff_output = runner.expect_success(&["diff", "before", "after"]);
    
    // 6. Assert expected changes are detected
    assert_contains_expected_changes(&diff_output);
    
    // 7. Validate output format and logical consistency
    validate_diff_output_format(&diff_output);
}
```

### Data Generation Helpers
Each phase should include helper functions for generating test data:
```rust
// Phase 2 helpers
fn create_table_with_extra_column(base_data: &str, column_name: &str, position: usize) -> String
fn remove_column_from_table(data: &str, column_index: usize) -> String
fn change_column_type(data: &str, column_index: usize, new_type: DataType) -> String

// Phase 3 helpers  
fn introduce_null_values(data: &str, percentage: f32) -> String
fn add_duplicate_rows(data: &str, count: usize) -> String
fn change_encoding(data: &str, from: Encoding, to: Encoding) -> String
```

### Validation Helpers
```rust
fn assert_column_addition_detected(diff_output: &str, column_name: &str)
fn assert_column_removal_detected(diff_output: &str, column_name: &str)
fn assert_data_type_change_detected(diff_output: &str, column: &str, old_type: &str, new_type: &str)
fn assert_null_value_changes_detected(diff_output: &str, count: usize)
```

## Success Metrics

### Coverage Goals
- **Phase 1**: ✅ 20/20 tests (100% complete)
- **Phase 2**: 0/15 tests (Column structure)
- **Phase 3**: 0/12 tests (Data quality)
- **Phase 4**: 0/10 tests (Scale/performance)
- **Phase 5**: 0/12 tests (Schema evolution)
- **Phase 6**: 0/8 tests (Cross-format)
- **Phase 7**: 0/10 tests (Business logic)
- **Phase 8**: 0/15 tests (Edge cases)
- **Phase 9**: 0/8 tests (Migration scenarios)

**Total Target**: 110 comprehensive table change tests

### Quality Metrics
- All tests must pass consistently
- Test execution time should remain reasonable (< 5 minutes total)
- Memory usage should be bounded for large dataset tests
- Output validation should be comprehensive and logical
- Edge cases should be properly handled without crashes

## Next Steps

1. **Immediate**: Begin Phase 2 implementation (column structure changes)
2. **Short-term**: Complete Phases 2-4 (high priority scenarios)
3. **Medium-term**: Implement Phases 5-7 (medium priority scenarios)
4. **Long-term**: Complete Phases 8-9 (edge cases and complex scenarios)

This roadmap ensures comprehensive coverage of all possible table changes while maintaining a practical implementation timeline focused on the most common real-world scenarios first.
