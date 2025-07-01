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

    /// Detect schema changes
    fn detect_schema_changes(
        baseline: &[ColumnInfo],
        current: &[ColumnInfo],
    ) -> Result<SchemaChanges> {
        let baseline_names: Vec<String> = baseline.iter().map(|c| c.name.clone()).collect();
        let current_names: Vec<String> = current.iter().map(|c| c.name.clone()).collect();

        // Create maps for efficient lookup
        let baseline_map: HashMap<String, &ColumnInfo> = baseline
            .iter()
            .map(|c| (c.name.clone(), c))
            .collect();
        let current_map: HashMap<String, &ColumnInfo> = current
            .iter()
            .map(|c| (c.name.clone(), c))
            .collect();

        // Detect column order changes
        let column_order = if baseline_names != current_names {
            Some(ColumnOrderChange {
                before: baseline_names.clone(),
                after: current_names.clone(),
            })
        } else {
            None
        };

        // Detect added columns
        let mut columns_added = Vec::new();
        for (pos, col) in current.iter().enumerate() {
            if !baseline_map.contains_key(&col.name) {
                columns_added.push(ColumnAddition {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    position: pos,
                    nullable: col.nullable,
                    default_value: None, // Could be enhanced to detect default values
                });
            }
        }

        // Detect removed columns
        let mut columns_removed = Vec::new();
        for (pos, col) in baseline.iter().enumerate() {
            if !current_map.contains_key(&col.name) {
                columns_removed.push(ColumnRemoval {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    position: pos,
                    nullable: col.nullable,
                });
            }
        }

        // Detect type changes (for columns that exist in both)
        let mut type_changes = Vec::new();
        for col in baseline {
            if let Some(current_col) = current_map.get(&col.name) {
                if col.data_type != current_col.data_type {
                    type_changes.push(TypeChange {
                        column: col.name.clone(),
                        from: col.data_type.clone(),
                        to: current_col.data_type.clone(),
                    });
                }
            }
        }

        // Note: Column renames are complex to detect automatically
        // For now, we treat them as remove + add
        let columns_renamed = Vec::new();

        Ok(SchemaChanges {
            column_order,
            columns_added,
            columns_removed,
            columns_renamed,
            type_changes,
        })
    }

    /// Detect row changes using column-name-based comparison
    fn detect_row_changes(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<RowChanges> {
        // Create column name to index mappings
        let baseline_col_map: HashMap<String, usize> = baseline_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        let current_col_map: HashMap<String, usize> = current_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        // Find common columns (columns that exist in both schemas)
        let common_columns: Vec<String> = baseline_schema
            .iter()
            .filter_map(|col| {
                if current_col_map.contains_key(&col.name) {
                    Some(col.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Convert rows to column-name-based maps for comparison
        let baseline_rows = Self::convert_rows_to_maps(baseline_data, &baseline_col_map);
        let current_rows = Self::convert_rows_to_maps(current_data, &current_col_map);

        // Create content-based lookup for efficient comparison
        let _baseline_content_map = Self::create_content_map(&baseline_rows, &common_columns);
        let _current_content_map = Self::create_content_map(&current_rows, &common_columns);

        let mut modified = Vec::new();
        let mut added = Vec::new();
        let mut removed = Vec::new();

        // Compare rows by position first, then by content
        let max_rows = std::cmp::max(baseline_data.len(), current_data.len());

        for row_idx in 0..max_rows {
            let baseline_row = baseline_rows.get(row_idx);
            let current_row = current_rows.get(row_idx);

            match (baseline_row, current_row) {
                (Some(baseline), Some(current)) => {
                    // Both rows exist - check for modifications
                    let changes = Self::compare_row_content(baseline, current, &common_columns);
                    if !changes.is_empty() {
                        modified.push(RowModification {
                            row_index: row_idx as u64,
                            changes,
                        });
                    }
                }
                (None, Some(current)) => {
                    // Row was added
                    added.push(RowAddition {
                        row_index: row_idx as u64,
                        data: current.clone(),
                    });
                }
                (Some(baseline), None) => {
                    // Row was removed
                    removed.push(RowRemoval {
                        row_index: row_idx as u64,
                        data: baseline.clone(),
                    });
                }
                (None, None) => {
                    // This shouldn't happen in our loop
                    break;
                }
            }
        }

        Ok(RowChanges {
            modified,
            added,
            removed,
        })
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
