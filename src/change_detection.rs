//! Comprehensive change detection and rollback system for tabdiff

use crate::error::Result;
use crate::hash::ColumnInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Comprehensive change detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeDetectionResult {
    pub schema_changes: SchemaChanges,
    pub row_changes: RowChanges,
    pub rollback_operations: Vec<RollbackOperation>,
}

/// Schema-level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChanges {
    pub column_order: Option<ColumnOrderChange>,
    pub columns_added: Vec<ColumnAddition>,
    pub columns_removed: Vec<ColumnRemoval>,
    pub columns_renamed: Vec<ColumnRename>,
    pub type_changes: Vec<TypeChange>,
}

/// Column order change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnOrderChange {
    pub before: Vec<String>,
    pub after: Vec<String>,
}

/// Column addition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAddition {
    pub name: String,
    pub data_type: String,
    pub position: usize,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// Column removal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRemoval {
    pub name: String,
    pub data_type: String,
    pub position: usize,
    pub nullable: bool,
}

/// Column rename
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRename {
    pub from: String,
    pub to: String,
}

/// Type change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeChange {
    pub column: String,
    pub from: String,
    pub to: String,
}

/// Row-level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChanges {
    pub modified: Vec<RowModification>,
    pub added: Vec<RowAddition>,
    pub removed: Vec<RowRemoval>,
}

/// Row modification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowModification {
    pub row_index: u64,
    pub changes: HashMap<String, CellChange>,
}

/// Cell change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellChange {
    pub before: String,
    pub after: String,
}

/// Row addition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowAddition {
    pub row_index: u64,
    pub data: HashMap<String, String>,
}

/// Row removal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowRemoval {
    pub row_index: u64,
    pub data: HashMap<String, String>,
}

/// Rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackOperation {
    pub operation_type: RollbackOperationType,
    pub parameters: HashMap<String, serde_json::Value>,
}

/// Types of rollback operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackOperationType {
    UpdateCell,
    RestoreRow,
    RemoveRow,
    RenameColumn,
    ChangeColumnType,
    AddColumn,
    RemoveColumn,
    ReorderColumns,
}

/// Change detector for comprehensive analysis
pub struct ChangeDetector;

impl ChangeDetector {
    /// Detect all changes between baseline and current data
    pub fn detect_changes(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<ChangeDetectionResult> {
        let schema_changes = Self::detect_schema_changes(baseline_schema, current_schema)?;
        let row_changes = Self::detect_row_changes(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
        )?;
        let rollback_operations = Self::generate_rollback_operations(&schema_changes, &row_changes)?;

        Ok(ChangeDetectionResult {
            schema_changes,
            row_changes,
            rollback_operations,
        })
    }

    /// Detect schema changes using position-based comparison
    fn detect_schema_changes(
        baseline: &[ColumnInfo],
        current: &[ColumnInfo],
    ) -> Result<SchemaChanges> {
        let baseline_names: Vec<String> = baseline.iter().map(|c| c.name.clone()).collect();
        let current_names: Vec<String> = current.iter().map(|c| c.name.clone()).collect();

        // Detect column order changes (if column names are reordered)
        let column_order = if baseline_names != current_names && baseline.len() == current.len() {
            // Check if it's just a reordering (same columns, different order)
            let mut baseline_sorted = baseline_names.clone();
            let mut current_sorted = current_names.clone();
            baseline_sorted.sort();
            current_sorted.sort();
            
            if baseline_sorted == current_sorted {
                Some(ColumnOrderChange {
                    before: baseline_names.clone(),
                    after: current_names.clone(),
                })
            } else {
                None // Not just reordering, there are additions/removals/renames
            }
        } else {
            None
        };

        let mut columns_added = Vec::new();
        let mut columns_removed = Vec::new();
        let mut columns_renamed = Vec::new();
        let mut type_changes = Vec::new();

        // Handle different column counts (additions/removals)
        if baseline.len() != current.len() {
            if current.len() > baseline.len() {
                // Columns were added at the end
                for (pos, col) in current.iter().enumerate().skip(baseline.len()) {
                    columns_added.push(ColumnAddition {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                        default_value: None,
                    });
                }
            } else {
                // Columns were removed from the end
                for (pos, col) in baseline.iter().enumerate().skip(current.len()) {
                    columns_removed.push(ColumnRemoval {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                    });
                }
            }
        }

        // Compare columns position by position (for common length)
        let min_len = baseline.len().min(current.len());
        for pos in 0..min_len {
            let baseline_col = &baseline[pos];
            let current_col = &current[pos];

            // Check for column rename at this position
            if baseline_col.name != current_col.name {
                columns_renamed.push(ColumnRename {
                    from: baseline_col.name.clone(),
                    to: current_col.name.clone(),
                });
            }

            // Check for type change at this position
            if baseline_col.data_type != current_col.data_type {
                type_changes.push(TypeChange {
                    column: current_col.name.clone(), // Use current name in case it was renamed
                    from: baseline_col.data_type.clone(),
                    to: current_col.data_type.clone(),
                });
            }
        }

        Ok(SchemaChanges {
            column_order,
            columns_added,
            columns_removed,
            columns_renamed,
            type_changes,
        })
    }

    /// Detect row changes using optimized hash-based comparison with intelligent modification detection
    fn detect_row_changes(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<RowChanges> {
        // Phase 1: Fast hash-based filtering to identify changed rows
        let hash_computer = crate::hash::HashComputer::new(10000);
        let baseline_hashes = hash_computer.hash_rows(baseline_data)?;
        let current_hashes = hash_computer.hash_rows(current_data)?;
        let comparison = hash_computer.compare_row_hashes(&baseline_hashes, &current_hashes);
        
        // Phase 2: Intelligent row classification for changed subset only
        let (modifications, genuine_additions, genuine_removals) = Self::classify_changed_rows(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
            &comparison.added_rows,
            &comparison.removed_rows,
        )?;
        
        // Phase 3: Parallel cell-level analysis for modifications only
        let detailed_modifications = Self::analyze_modifications_parallel(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
            &modifications,
        )?;
        
        // Convert results to final format
        let added = Self::convert_additions_parallel(current_schema, current_data, &genuine_additions)?;
        let removed = Self::convert_removals_parallel(baseline_schema, baseline_data, &genuine_removals)?;

        Ok(RowChanges {
            modified: detailed_modifications,
            added,
            removed,
        })
    }

    /// Classify changed rows into modifications vs genuine additions/removals
    fn classify_changed_rows(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        added_indices: &[u64],
        removed_indices: &[u64],
    ) -> Result<(Vec<(u64, u64)>, Vec<u64>, Vec<u64>)> {
        use rayon::prelude::*;
        
        // Early exit if no changes
        if added_indices.is_empty() && removed_indices.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        
        // Create column mapping for schema-aware comparison
        let common_columns = Self::find_common_columns(baseline_schema, current_schema);
        
        // Parallel matching: find likely modifications using position and content heuristics
        let mut modifications = Vec::new();
        let mut unmatched_added = added_indices.to_vec();
        let mut unmatched_removed = removed_indices.to_vec();
        
        // Strategy 1: Position-based matching (most common case)
        let position_matches: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&removed_idx| {
                // Look for an added row at the same position
                if let Some(added_pos) = added_indices.iter().position(|&added_idx| added_idx == removed_idx) {
                    Some((removed_idx, added_indices[added_pos]))
                } else {
                    None
                }
            })
            .collect();
        
        // Remove position matches from unmatched lists
        for &(removed_idx, added_idx) in &position_matches {
            modifications.push((removed_idx, added_idx));
            unmatched_removed.retain(|&x| x != removed_idx);
            unmatched_added.retain(|&x| x != added_idx);
        }
        
        // Strategy 2: Content-based matching for remaining rows (using key columns)
        if !unmatched_removed.is_empty() && !unmatched_added.is_empty() && !common_columns.is_empty() {
            let content_matches = Self::find_content_matches_parallel(
                baseline_data,
                current_data,
                &unmatched_removed,
                &unmatched_added,
                &common_columns,
            )?;
            
            for &(removed_idx, added_idx) in &content_matches {
                modifications.push((removed_idx, added_idx));
                unmatched_removed.retain(|&x| x != removed_idx);
                unmatched_added.retain(|&x| x != added_idx);
            }
        }
        
        Ok((modifications, unmatched_added, unmatched_removed))
    }
    
    /// Find common columns between schemas for content matching
    fn find_common_columns(baseline_schema: &[ColumnInfo], current_schema: &[ColumnInfo]) -> Vec<String> {
        let current_names: std::collections::HashSet<_> = current_schema.iter().map(|c| &c.name).collect();
        baseline_schema
            .iter()
            .filter_map(|col| {
                if current_names.contains(&col.name) {
                    Some(col.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Find content-based matches using parallel processing
    fn find_content_matches_parallel(
        baseline_data: &[Vec<String>],
        current_data: &[Vec<String>],
        removed_indices: &[u64],
        added_indices: &[u64],
        common_columns: &[String],
    ) -> Result<Vec<(u64, u64)>> {
        use rayon::prelude::*;
        
        // Create column index mappings
        let baseline_col_map: std::collections::HashMap<String, usize> = common_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        
        // Parallel content matching with similarity scoring
        let matches: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&removed_idx| {
                let removed_row = baseline_data.get(removed_idx as usize)?;
                
                // Find best match among added rows
                let best_match = added_indices
                    .iter()
                    .filter_map(|&added_idx| {
                        let added_row = current_data.get(added_idx as usize)?;
                        let similarity = Self::calculate_row_similarity(
                            removed_row,
                            added_row,
                            common_columns,
                            &baseline_col_map,
                        );
                        Some((added_idx, similarity))
                    })
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                
                // Only consider it a match if similarity is above threshold
                if let Some((added_idx, similarity)) = best_match {
                    if similarity > 0.5 { // At least 50% of key columns match
                        Some((removed_idx, added_idx))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        Ok(matches)
    }
    
    /// Calculate similarity between two rows based on common columns
    fn calculate_row_similarity(
        row1: &[String],
        row2: &[String],
        common_columns: &[String],
        col_map: &std::collections::HashMap<String, usize>,
    ) -> f64 {
        let mut matches = 0;
        let mut total = 0;
        
        for col_name in common_columns {
            if let Some(&col_idx) = col_map.get(col_name) {
                if let (Some(val1), Some(val2)) = (row1.get(col_idx), row2.get(col_idx)) {
                    total += 1;
                    if val1 == val2 {
                        matches += 1;
                    }
                }
            }
        }
        
        if total > 0 {
            matches as f64 / total as f64
        } else {
            0.0
        }
    }
    
    /// Analyze modifications in parallel to detect cell-level changes
    fn analyze_modifications_parallel(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        modifications: &[(u64, u64)],
    ) -> Result<Vec<RowModification>> {
        use rayon::prelude::*;
        
        // Create column mappings for schema-aware comparison
        let baseline_col_map: std::collections::HashMap<String, usize> = baseline_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        let current_col_map: std::collections::HashMap<String, usize> = current_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        // Parallel cell-level analysis
        let detailed_modifications: Vec<_> = modifications
            .par_iter()
            .filter_map(|&(baseline_idx, current_idx)| {
                let baseline_row = baseline_data.get(baseline_idx as usize)?;
                let current_row = current_data.get(current_idx as usize)?;
                
                let changes = Self::compare_rows_schema_aware(
                    baseline_row,
                    current_row,
                    &baseline_col_map,
                    &current_col_map,
                );
                
                if !changes.is_empty() {
                    Some(RowModification {
                        row_index: current_idx, // Use current position as the canonical index
                        changes,
                    })
                } else {
                    None
                }
            })
            .collect();
        
        Ok(detailed_modifications)
    }
    
    /// Compare two rows with schema awareness
    fn compare_rows_schema_aware(
        baseline_row: &[String],
        current_row: &[String],
        baseline_col_map: &std::collections::HashMap<String, usize>,
        current_col_map: &std::collections::HashMap<String, usize>,
    ) -> HashMap<String, CellChange> {
        let mut changes = HashMap::new();
        
        // Compare common columns only
        for col_name in baseline_col_map.keys() {
            if let (Some(&baseline_idx), Some(&current_idx)) = 
                (baseline_col_map.get(col_name), current_col_map.get(col_name)) {
                
                let baseline_value = baseline_row.get(baseline_idx).map(|s| s.as_str()).unwrap_or("");
                let current_value = current_row.get(current_idx).map(|s| s.as_str()).unwrap_or("");
                
                if baseline_value != current_value {
                    changes.insert(col_name.clone(), CellChange {
                        before: baseline_value.to_string(),
                        after: current_value.to_string(),
                    });
                }
            }
        }
        
        changes
    }
    
    /// Convert genuine additions to RowAddition format in parallel
    fn convert_additions_parallel(
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        added_indices: &[u64],
    ) -> Result<Vec<RowAddition>> {
        use rayon::prelude::*;
        
        let additions: Vec<_> = added_indices
            .par_iter()
            .filter_map(|&row_idx| {
                let row_data = current_data.get(row_idx as usize)?;
                let mut data = HashMap::new();
                
                for (col_idx, col) in current_schema.iter().enumerate() {
                    if let Some(value) = row_data.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                
                Some(RowAddition { row_index: row_idx, data })
            })
            .collect();
        
        Ok(additions)
    }
    
    /// Convert genuine removals to RowRemoval format in parallel
    fn convert_removals_parallel(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        removed_indices: &[u64],
    ) -> Result<Vec<RowRemoval>> {
        use rayon::prelude::*;
        
        let removals: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&row_idx| {
                let row_data = baseline_data.get(row_idx as usize)?;
                let mut data = HashMap::new();
                
                for (col_idx, col) in baseline_schema.iter().enumerate() {
                    if let Some(value) = row_data.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                
                Some(RowRemoval { row_index: row_idx, data })
            })
            .collect();
        
        Ok(removals)
    }

    /// Convert positional rows to column-name-based maps
    fn convert_rows_to_maps(
        rows: &[Vec<String>],
        col_map: &HashMap<String, usize>,
    ) -> Vec<HashMap<String, String>> {
        rows.iter()
            .map(|row| {
                col_map
                    .iter()
                    .filter_map(|(col_name, &col_idx)| {
                        row.get(col_idx).map(|value| (col_name.clone(), value.clone()))
                    })
                    .collect()
            })
            .collect()
    }

    /// Create content-based lookup map
    fn create_content_map(
        rows: &[HashMap<String, String>],
        common_columns: &[String],
    ) -> HashMap<String, Vec<usize>> {
        let mut content_map = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let content_key = Self::create_content_key(row, common_columns);
            content_map
                .entry(content_key)
                .or_insert_with(Vec::new)
                .push(row_idx);
        }

        content_map
    }

    /// Create a content key for a row based on common columns
    fn create_content_key(row: &HashMap<String, String>, common_columns: &[String]) -> String {
        let mut key_parts = Vec::new();
        for col in common_columns {
            let value = row.get(col).map(|s| s.as_str()).unwrap_or("");
            key_parts.push(value.to_string());
        }
        key_parts.join("|")
    }

    /// Compare two rows and return the differences
    fn compare_row_content(
        baseline: &HashMap<String, String>,
        current: &HashMap<String, String>,
        common_columns: &[String],
    ) -> HashMap<String, CellChange> {
        let mut changes = HashMap::new();
        let empty_string = String::new();

        for col in common_columns {
            let baseline_value = baseline.get(col).unwrap_or(&empty_string);
            let current_value = current.get(col).unwrap_or(&empty_string);

            if baseline_value != current_value {
                changes.insert(
                    col.clone(),
                    CellChange {
                        before: baseline_value.clone(),
                        after: current_value.clone(),
                    },
                );
            }
        }

        changes
    }

    /// Generate rollback operations in reverse order
    fn generate_rollback_operations(
        schema_changes: &SchemaChanges,
        row_changes: &RowChanges,
    ) -> Result<Vec<RollbackOperation>> {
        let mut operations = Vec::new();

        // Step 1: Undo row changes first (in reverse order)
        
        // Undo row additions (remove them)
        for addition in row_changes.added.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::RemoveRow,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("row_index".to_string(), serde_json::Value::Number(addition.row_index.into()));
                    params
                },
            });
        }

        // Undo row modifications (restore original values)
        for modification in row_changes.modified.iter().rev() {
            for (column, change) in &modification.changes {
                operations.push(RollbackOperation {
                    operation_type: RollbackOperationType::UpdateCell,
                    parameters: {
                        let mut params = HashMap::new();
                        params.insert("row_index".to_string(), serde_json::Value::Number(modification.row_index.into()));
                        params.insert("column".to_string(), serde_json::Value::String(column.clone()));
                        params.insert("value".to_string(), serde_json::Value::String(change.before.clone()));
                        params
                    },
                });
            }
        }

        // Undo row removals (restore them)
        for removal in row_changes.removed.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::RestoreRow,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("row_index".to_string(), serde_json::Value::Number(removal.row_index.into()));
                    params.insert("data".to_string(), serde_json::to_value(&removal.data)?);
                    params
                },
            });
        }

        // Step 2: Undo schema changes (in reverse order)
        
        // Undo type changes
        for type_change in schema_changes.type_changes.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::ChangeColumnType,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("column".to_string(), serde_json::Value::String(type_change.column.clone()));
                    params.insert("to".to_string(), serde_json::Value::String(type_change.from.clone()));
                    params
                },
            });
        }

        // Undo column renames
        for rename in schema_changes.columns_renamed.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::RenameColumn,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("from".to_string(), serde_json::Value::String(rename.to.clone()));
                    params.insert("to".to_string(), serde_json::Value::String(rename.from.clone()));
                    params
                },
            });
        }

        // Undo column additions (remove them)
        for addition in schema_changes.columns_added.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::RemoveColumn,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("name".to_string(), serde_json::Value::String(addition.name.clone()));
                    params
                },
            });
        }

        // Undo column removals (restore them)
        for removal in schema_changes.columns_removed.iter().rev() {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::AddColumn,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("name".to_string(), serde_json::Value::String(removal.name.clone()));
                    params.insert("data_type".to_string(), serde_json::Value::String(removal.data_type.clone()));
                    params.insert("position".to_string(), serde_json::Value::Number(removal.position.into()));
                    params.insert("nullable".to_string(), serde_json::Value::Bool(removal.nullable));
                    params
                },
            });
        }

        // Undo column order changes
        if let Some(order_change) = &schema_changes.column_order {
            operations.push(RollbackOperation {
                operation_type: RollbackOperationType::ReorderColumns,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("order".to_string(), serde_json::to_value(&order_change.before)?);
                    params
                },
            });
        }

        Ok(operations)
    }
}

impl SchemaChanges {
    /// Check if there are any schema changes
    pub fn has_changes(&self) -> bool {
        self.column_order.is_some()
            || !self.columns_added.is_empty()
            || !self.columns_removed.is_empty()
            || !self.columns_renamed.is_empty()
            || !self.type_changes.is_empty()
    }
}

impl RowChanges {
    /// Check if there are any row changes
    pub fn has_changes(&self) -> bool {
        !self.modified.is_empty() || !self.added.is_empty() || !self.removed.is_empty()
    }

    /// Get total number of changed rows
    pub fn total_changes(&self) -> usize {
        self.modified.len() + self.added.len() + self.removed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_detection() {
        let baseline = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let current = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(), // Type changed
                nullable: true,
            },
            ColumnInfo {
                name: "email".to_string(), // Added column
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let changes = ChangeDetector::detect_schema_changes(&baseline, &current).unwrap();

        assert!(changes.has_changes());
        assert_eq!(changes.columns_added.len(), 1);
        assert_eq!(changes.columns_added[0].name, "email");
        assert_eq!(changes.type_changes.len(), 1);
        assert_eq!(changes.type_changes[0].column, "name");
        assert_eq!(changes.type_changes[0].from, "TEXT");
        assert_eq!(changes.type_changes[0].to, "VARCHAR");
    }

    #[test]
    fn test_row_change_detection() {
        let schema = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let baseline_data = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];

        let current_data = vec![
            vec!["1".to_string(), "Alice Smith".to_string()], // Modified
            vec!["2".to_string(), "Bob".to_string()],         // Unchanged
            vec!["3".to_string(), "Charlie".to_string()],     // Added
        ];

        let changes = ChangeDetector::detect_row_changes(&schema, &baseline_data, &schema, &current_data).unwrap();

        assert!(changes.has_changes());
        assert_eq!(changes.modified.len(), 1);
        assert_eq!(changes.modified[0].row_index, 0);
        assert!(changes.modified[0].changes.contains_key("name"));
        assert_eq!(changes.added.len(), 1);
        assert_eq!(changes.added[0].row_index, 2);
        assert_eq!(changes.removed.len(), 0);
    }
}
