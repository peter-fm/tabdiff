//! Output formatting utilities

use crate::cli::OutputFormat;
use crate::error::Result;
use crate::hash::RowHashComparison;
use crate::workspace::WorkspaceStats;
use serde_json::Value;

/// Pretty printer for tabdiff output
pub struct PrettyPrinter;

impl PrettyPrinter {
    /// Print workspace statistics
    pub fn print_workspace_stats(stats: &WorkspaceStats) {
        println!("📊 Tabdiff Workspace Statistics");
        println!("├─ Snapshots: {}", stats.snapshot_count);
        println!("├─ Diffs: {}", stats.diff_count);
        println!("├─ Archive size: {}", format_bytes(stats.total_archive_size));
        println!("├─ JSON size: {}", format_bytes(stats.total_json_size));
        println!("└─ Diff size: {}", format_bytes(stats.total_diff_size));
    }

    /// Print snapshot list
    pub fn print_snapshot_list(snapshots: &[String]) {
        if snapshots.is_empty() {
            println!("No snapshots found.");
            return;
        }

        println!("📸 Available Snapshots:");
        for (i, snapshot) in snapshots.iter().enumerate() {
            let prefix = if i == snapshots.len() - 1 { "└─" } else { "├─" };
            println!("{} {}", prefix, snapshot);
        }
    }

    /// Print snapshot metadata
    pub fn print_snapshot_metadata(metadata: &Value, detailed: bool) {
        println!("📸 Snapshot: {}", metadata.get("name").unwrap_or(&Value::Null));
        println!("├─ Created: {}", metadata.get("created").unwrap_or(&Value::Null));
        println!("├─ Source: {}", metadata.get("source").unwrap_or(&Value::Null));
        println!("├─ Rows: {}", metadata.get("row_count").unwrap_or(&Value::Null));
        println!("├─ Columns: {}", metadata.get("column_count").unwrap_or(&Value::Null));
        
        if let Some(sampling) = metadata.get("sampling") {
            println!("├─ Sampling: {}", sampling.get("strategy").unwrap_or(&Value::Null));
        }
        
        if detailed {
            if let Some(columns) = metadata.get("columns").and_then(|c| c.as_object()) {
                println!("└─ Column Hashes:");
                for (i, (name, hash)) in columns.iter().enumerate() {
                    let prefix = if i == columns.len() - 1 { "   └─" } else { "   ├─" };
                    println!("{} {}: {}", prefix, name, hash.as_str().unwrap_or(""));
                }
            }
        } else {
            println!("└─ Schema Hash: {}", metadata.get("schema_hash").unwrap_or(&Value::Null));
        }
    }

    /// Print diff results
    pub fn print_diff_results(diff: &Value) {
        println!("🔍 Diff Results: {} → {}", 
                 diff.get("base").unwrap_or(&Value::Null),
                 diff.get("compare").unwrap_or(&Value::Null));
        
        let schema_changed = diff.get("schema_changed").and_then(|v| v.as_bool()).unwrap_or(false);
        let rows_changed = diff.get("rows_changed").and_then(|v| v.as_u64()).unwrap_or(0);
        
        if schema_changed {
            println!("├─ ❌ Schema: CHANGED");
            if let Some(columns) = diff.get("columns_changed").and_then(|v| v.as_array()) {
                println!("│  └─ Changed columns: {}", 
                         columns.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(", "));
            }
        } else {
            println!("├─ ✅ Schema: unchanged");
        }
        
        if rows_changed > 0 {
            println!("├─ ❌ Rows: {} changed", rows_changed);
            if let Some(samples) = diff.get("sample_changes").and_then(|v| v.as_array()) {
                let sample_str = samples.iter()
                                       .filter_map(|v| v.as_u64())
                                       .map(|n| n.to_string())
                                       .collect::<Vec<_>>()
                                       .join(", ");
                println!("│  └─ Sample indices: {}", sample_str);
            }
        } else {
            println!("├─ ✅ Rows: unchanged");
        }
        
        println!("└─ Total rows: {}", diff.get("row_count").unwrap_or(&Value::Null));
    }

    /// Print status check results
    pub fn print_status_results(
        schema_changed: bool,
        columns_changed: &[String],
        row_comparison: &RowHashComparison,
        quiet: bool,
    ) {
        if quiet {
            // Machine-readable output
            println!("schema_changed={}", schema_changed);
            println!("columns_changed={}", columns_changed.len());
            println!("rows_changed={}", row_comparison.total_changes());
            return;
        }

        println!("📊 tabdiff status");
        
        if schema_changed {
            println!("├─ ❌ Schema: CHANGED");
        } else {
            println!("├─ ✅ Schema: unchanged");
        }
        
        if columns_changed.is_empty() {
            println!("├─ ✅ Columns: all matched");
        } else {
            println!("├─ ❌ Columns changed: {}", columns_changed.len());
            println!("│  └─ {}", columns_changed.join(", "));
        }
        
        if row_comparison.has_changes() {
            println!("├─ ❌ Rows changed: {}", row_comparison.total_changes());
            if !row_comparison.changed_rows.is_empty() {
                let sample: Vec<String> = row_comparison.changed_rows
                    .iter()
                    .take(5)
                    .map(|n| n.to_string())
                    .collect();
                println!("│  └─ Changed row indices (sample): {}", sample.join(", "));
            }
        } else {
            println!("├─ ✅ Rows: unchanged");
        }
        
        println!("└─ Total rows checked: {}", row_comparison.total_compare);
        
        if row_comparison.has_changes() || schema_changed || !columns_changed.is_empty() {
            println!();
            println!("🟡 You may want to run:");
            println!("  tabdiff snapshot <input> --name <new_version>");
        }
    }
}

/// JSON formatter for machine-readable output
pub struct JsonFormatter;

impl JsonFormatter {
    /// Format any serializable data as JSON
    pub fn format<T: serde::Serialize + ?Sized>(data: &T) -> Result<String> {
        Ok(serde_json::to_string_pretty(data)?)
    }

    /// Format workspace stats as JSON
    pub fn format_workspace_stats(stats: &WorkspaceStats) -> Result<String> {
        let json = serde_json::json!({
            "snapshot_count": stats.snapshot_count,
            "diff_count": stats.diff_count,
            "total_archive_size": stats.total_archive_size,
            "total_json_size": stats.total_json_size,
            "total_diff_size": stats.total_diff_size
        });
        Ok(serde_json::to_string_pretty(&json)?)
    }

    /// Format status results as JSON
    pub fn format_status_results(
        schema_changed: bool,
        columns_changed: &[String],
        row_comparison: &RowHashComparison,
    ) -> Result<String> {
        let json = serde_json::json!({
            "schema_changed": schema_changed,
            "columns_changed": columns_changed,
            "rows_changed": row_comparison.total_changes(),
            "row_details": {
                "changed": row_comparison.changed_rows,
                "added": row_comparison.added_rows,
                "removed": row_comparison.removed_rows,
                "total_base": row_comparison.total_base,
                "total_compare": row_comparison.total_compare
            }
        });
        Ok(serde_json::to_string_pretty(&json)?)
    }
}

/// Output manager that handles different formats
pub struct OutputManager {
    format: OutputFormat,
}

impl OutputManager {
    pub fn new(format: OutputFormat) -> Self {
        Self { format }
    }

    /// Output data in the configured format
    pub fn output<T: serde::Serialize>(&self, data: &T) -> Result<()> {
        match self.format {
            OutputFormat::Json => {
                println!("{}", JsonFormatter::format(data)?);
            }
            OutputFormat::Pretty => {
                // For pretty format, we need specific handling per data type
                // This is a fallback to JSON for unknown types
                println!("{}", JsonFormatter::format(data)?);
            }
        }
        Ok(())
    }

    /// Output workspace stats
    pub fn output_workspace_stats(&self, stats: &WorkspaceStats) -> Result<()> {
        match self.format {
            OutputFormat::Json => {
                println!("{}", JsonFormatter::format_workspace_stats(stats)?);
            }
            OutputFormat::Pretty => {
                PrettyPrinter::print_workspace_stats(stats);
            }
        }
        Ok(())
    }

    /// Output snapshot list
    pub fn output_snapshot_list(&self, snapshots: &[String]) -> Result<()> {
        match self.format {
            OutputFormat::Json => {
                println!("{}", JsonFormatter::format(snapshots)?);
            }
            OutputFormat::Pretty => {
                PrettyPrinter::print_snapshot_list(snapshots);
            }
        }
        Ok(())
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
    }

    #[test]
    fn test_json_formatter() {
        let data = serde_json::json!({"test": "value"});
        let result = JsonFormatter::format(&data).unwrap();
        assert!(result.contains("test"));
        assert!(result.contains("value"));
    }
}
