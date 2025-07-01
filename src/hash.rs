//! Hashing utilities for tabdiff operations

use crate::cli::SamplingStrategy;
use crate::error::Result;
use blake3::Hasher;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A hash value represented as a hex string
pub type HashValue = String;

/// Row hash with index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowHash {
    pub row_index: u64,
    pub hash: HashValue,
}

/// Column hash information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnHash {
    pub column_name: String,
    pub column_type: String,
    pub hash: HashValue,
}

/// Schema hash information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHash {
    pub hash: HashValue,
    pub column_count: usize,
    pub columns: Vec<ColumnInfo>,
}

/// Column information for schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Hash computer for various data structures
pub struct HashComputer {
    #[allow(dead_code)] // Used for potential future batching optimizations
    batch_size: usize,
}

impl HashComputer {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }

    /// Compute hash for a single value
    pub fn hash_value(&self, value: &str) -> HashValue {
        let mut hasher = Hasher::new();
        hasher.update(value.as_bytes());
        hasher.finalize().to_hex().to_string()
    }

    /// Compute hash for multiple values (e.g., a row)
    pub fn hash_values(&self, values: &[String]) -> HashValue {
        let mut hasher = Hasher::new();
        for value in values {
            hasher.update(value.as_bytes());
            hasher.update(b"|"); // Separator to avoid hash collisions
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Compute schema hash from column information
    pub fn hash_schema(&self, columns: &[ColumnInfo]) -> Result<SchemaHash> {
        let mut hasher = Hasher::new();
        
        // Sort columns by name for deterministic hashing
        let mut sorted_columns = columns.to_vec();
        sorted_columns.sort_by(|a, b| a.name.cmp(&b.name));
        
        for col in &sorted_columns {
            hasher.update(col.name.as_bytes());
            hasher.update(b"|");
            hasher.update(col.data_type.as_bytes());
            hasher.update(b"|");
            hasher.update(if col.nullable { b"1" } else { b"0" });
            hasher.update(b"||");
        }
        
        let hash = hasher.finalize().to_hex().to_string();
        
        Ok(SchemaHash {
            hash,
            column_count: columns.len(),
            columns: sorted_columns,
        })
    }

    /// Compute column hashes from data
    pub fn hash_columns(&self, column_data: &HashMap<String, Vec<String>>) -> Result<Vec<ColumnHash>> {
        let mut column_hashes = Vec::new();
        
        for (column_name, values) in column_data {
            let mut hasher = Hasher::new();
            
            // Hash all values in the column
            for value in values {
                hasher.update(value.as_bytes());
                hasher.update(b"|");
            }
            
            let hash = hasher.finalize().to_hex().to_string();
            
            // Infer column type from first non-empty value
            let column_type = self.infer_column_type(values);
            
            column_hashes.push(ColumnHash {
                column_name: column_name.clone(),
                column_type,
                hash,
            });
        }
        
        // Sort by column name for consistency
        column_hashes.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        
        Ok(column_hashes)
    }

    /// Compute row hashes with sampling strategy
    pub fn hash_rows(
        &self,
        row_data: &[Vec<String>],
        sampling: &SamplingStrategy,
    ) -> Result<Vec<RowHash>> {
        let total_rows = row_data.len();
        
        if total_rows == 0 {
            return Ok(Vec::new());
        }
        
        let indices_to_hash = match sampling {
            SamplingStrategy::Full => (0..total_rows).collect(),
            SamplingStrategy::Count(n) => {
                if *n >= total_rows {
                    (0..total_rows).collect()
                } else {
                    self.sample_indices(total_rows, *n)
                }
            }
            SamplingStrategy::Percentage(pct) => {
                let count = ((total_rows as f64) * pct).ceil() as usize;
                if count >= total_rows {
                    (0..total_rows).collect()
                } else {
                    self.sample_indices(total_rows, count)
                }
            }
        };

        // Compute hashes in parallel
        let row_hashes: Vec<RowHash> = indices_to_hash
            .par_iter()
            .map(|&idx| {
                let row = &row_data[idx];
                let hash = self.hash_values(row);
                RowHash {
                    row_index: idx as u64,
                    hash,
                }
            })
            .collect();

        Ok(row_hashes)
    }

    /// Sample random indices
    fn sample_indices(&self, total: usize, count: usize) -> Vec<usize> {
        use std::collections::HashSet;
        
        if count >= total {
            return (0..total).collect();
        }
        
        let mut indices = HashSet::new();
        let mut rng_state = 12345u64; // Simple PRNG for reproducible sampling
        
        while indices.len() < count {
            rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
            let idx = (rng_state % total as u64) as usize;
            indices.insert(idx);
        }
        
        let mut result: Vec<usize> = indices.into_iter().collect();
        result.sort();
        result
    }

    /// Infer column type from values
    fn infer_column_type(&self, values: &[String]) -> String {
        for value in values {
            if !value.is_empty() {
                // Simple type inference
                if value.parse::<i64>().is_ok() {
                    return "INTEGER".to_string();
                } else if value.parse::<f64>().is_ok() {
                    return "FLOAT".to_string();
                } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                    return "BOOLEAN".to_string();
                } else {
                    return "TEXT".to_string();
                }
            }
        }
        "TEXT".to_string() // Default
    }

    /// Compare two sets of row hashes
    pub fn compare_row_hashes(
        &self,
        base_hashes: &[RowHash],
        compare_hashes: &[RowHash],
    ) -> RowHashComparison {
        let base_map: HashMap<u64, &str> = base_hashes
            .iter()
            .map(|rh| (rh.row_index, rh.hash.as_str()))
            .collect();
        
        let compare_map: HashMap<u64, &str> = compare_hashes
            .iter()
            .map(|rh| (rh.row_index, rh.hash.as_str()))
            .collect();
        
        let mut changed_rows = Vec::new();
        let mut added_rows = Vec::new();
        let mut removed_rows = Vec::new();
        
        // Find changed and removed rows
        for (idx, hash) in &base_map {
            match compare_map.get(idx) {
                Some(compare_hash) => {
                    if hash != compare_hash {
                        changed_rows.push(*idx);
                    }
                }
                None => removed_rows.push(*idx),
            }
        }
        
        // Find added rows
        for idx in compare_map.keys() {
            if !base_map.contains_key(idx) {
                added_rows.push(*idx);
            }
        }
        
        RowHashComparison {
            changed_rows,
            added_rows,
            removed_rows,
            total_base: base_hashes.len(),
            total_compare: compare_hashes.len(),
        }
    }
}

/// Result of comparing row hashes
#[derive(Debug, Clone)]
pub struct RowHashComparison {
    pub changed_rows: Vec<u64>,
    pub added_rows: Vec<u64>,
    pub removed_rows: Vec<u64>,
    pub total_base: usize,
    pub total_compare: usize,
}

impl RowHashComparison {
    pub fn has_changes(&self) -> bool {
        !self.changed_rows.is_empty() || !self.added_rows.is_empty() || !self.removed_rows.is_empty()
    }
    
    pub fn total_changes(&self) -> usize {
        self.changed_rows.len() + self.added_rows.len() + self.removed_rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_value() {
        let computer = HashComputer::new(1000);
        let hash1 = computer.hash_value("test");
        let hash2 = computer.hash_value("test");
        let hash3 = computer.hash_value("different");
        
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_values() {
        let computer = HashComputer::new(1000);
        let values1 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let values2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let values3 = vec!["a".to_string(), "c".to_string(), "b".to_string()];
        
        let hash1 = computer.hash_values(&values1);
        let hash2 = computer.hash_values(&values2);
        let hash3 = computer.hash_values(&values3);
        
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3); // Order matters
    }

    #[test]
    fn test_sampling_strategy() {
        let computer = HashComputer::new(1000);
        let row_data = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["4".to_string(), "d".to_string()],
        ];
        
        // Test full sampling
        let full_hashes = computer.hash_rows(&row_data, &SamplingStrategy::Full).unwrap();
        assert_eq!(full_hashes.len(), 4);
        
        // Test count sampling
        let count_hashes = computer.hash_rows(&row_data, &SamplingStrategy::Count(2)).unwrap();
        assert_eq!(count_hashes.len(), 2);
        
        // Test percentage sampling
        let pct_hashes = computer.hash_rows(&row_data, &SamplingStrategy::Percentage(0.5)).unwrap();
        assert_eq!(pct_hashes.len(), 2);
    }
}
