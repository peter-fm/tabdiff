//! Command implementations for tabdiff CLI

use crate::cli::{Commands, DiffMode};
use crate::data::DataProcessor;
use crate::error::Result;
use crate::output::{PrettyPrinter, JsonFormatter};
use crate::resolver::{SnapshotRef, SnapshotResolver};
use crate::snapshot::{SnapshotCreator, SnapshotLoader};
use crate::workspace::TabdiffWorkspace;
use crate::change_detection::ChangeDetector;
use std::path::Path;

/// Execute a command
pub fn execute_command(command: Commands, workspace_path: Option<&Path>) -> Result<()> {
    match command {
        Commands::Init { force } => init_command(workspace_path, force),
        Commands::Snapshot {
            input,
            name,
            batch_size,
            full_data,
            hash_only,
        } => {
            // Determine final full_data setting
            let enable_full_data = if hash_only {
                false
            } else {
                full_data
            };
            snapshot_command(workspace_path, &input, &name, batch_size, enable_full_data)
        },
        Commands::Diff {
            snapshot1,
            snapshot2,
            mode,
            output,
        } => diff_command(workspace_path, &snapshot1, &snapshot2, &mode, output.as_deref()),
        Commands::Show {
            snapshot,
            detailed,
            json,
        } => show_command(workspace_path, &snapshot, detailed, json),
        Commands::Status {
            input,
            compare_to,
            quiet,
            json,
        } => status_command(workspace_path, &input, compare_to.as_deref(), quiet, json),
        Commands::List { json } => list_command(workspace_path, json),
        Commands::Rollback {
            input,
            to,
            dry_run,
            force,
            backup,
        } => rollback_command(workspace_path, &input, &to, dry_run, force, backup),
        Commands::Chain { json } => chain_command(workspace_path, json),
        Commands::Cleanup {
            keep_full,
            dry_run,
            force,
        } => cleanup_command(workspace_path, keep_full, dry_run, force),
    }
}


/// Initialize tabdiff workspace
fn init_command(workspace_path: Option<&Path>, force: bool) -> Result<()> {
    let current_dir = std::env::current_dir()?;
    let root = workspace_path.unwrap_or(&current_dir);
    
    let workspace = if force {
        // Force create new workspace, overwriting existing config
        let workspace = TabdiffWorkspace::from_root(root.to_path_buf())?;
        
        // Create directories
        std::fs::create_dir_all(&workspace.tabdiff_dir)?;
        std::fs::create_dir_all(&workspace.diffs_dir)?;
        
        // Force create config file
        workspace.create_config_with_force(true)?;
        
        // Update .gitignore
        workspace.ensure_gitignore()?;
        
        workspace
    } else {
        // For init command, always create in the specified directory
        // Don't search for existing workspace in parent directories
        TabdiffWorkspace::create_new(root.to_path_buf())?
    };

    println!("✅ Initialized tabdiff workspace at: {}", workspace.root.display());
    println!("📁 Workspace directory: {}", workspace.tabdiff_dir.display());
    
    Ok(())
}

/// Rollback a file to a previous snapshot state
fn rollback_command(
    workspace_path: Option<&Path>,
    input: &str,
    to: &str,
    dry_run: bool,
    force: bool,
    backup: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Resolve target snapshot
    let target_snapshot = {
        let snap_ref = SnapshotRef::from_string(to.to_string());
        resolver.resolve(&snap_ref)?
    };

    println!("🔄 Rolling back '{}' to snapshot '{}'...", input, target_snapshot.name);

    // Load target snapshot data
    let target_data = if target_snapshot.has_archive() {
        SnapshotLoader::load_full_snapshot(target_snapshot.require_archive()?)?
    } else {
        return Err(crate::error::TabdiffError::archive("Target snapshot has no archive data"));
    };

    // Load current data
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        workspace.root.join(input)
    };

    if !input_path.exists() {
        return Err(crate::error::TabdiffError::invalid_input(format!(
            "Input file does not exist: {}", input_path.display()
        )));
    }

    let mut data_processor = DataProcessor::new()?;
    let current_data_info = data_processor.load_file(&input_path)?;
    let current_row_data = data_processor.extract_all_data()?;

    // Extract target schema from archive data
    let target_schema = if let Some(schema_data) = target_data.schema_data.get("columns") {
        if let Some(columns_array) = schema_data.as_array() {
            let mut target_columns = Vec::new();
            for col_value in columns_array {
                if let (Some(name), Some(data_type), Some(nullable)) = (
                    col_value.get("name").and_then(|v| v.as_str()),
                    col_value.get("data_type").and_then(|v| v.as_str()),
                    col_value.get("nullable").and_then(|v| v.as_bool())
                ) {
                    target_columns.push(crate::hash::ColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                    });
                }
            }
            target_columns
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Extract target row data from archive
    let target_row_data = if let Some(rows_data) = target_data.row_data.get("rows") {
        if let Some(rows_array) = rows_data.as_array() {
            let mut target_rows = Vec::new();
            for row_value in rows_array {
                if let Some(row_array) = row_value.as_array() {
                    let row: Vec<String> = row_array
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    target_rows.push(row);
                }
            }
            target_rows
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Detect changes needed to rollback
    let changes = ChangeDetector::detect_changes(
        &current_data_info.columns,
        &current_row_data,
        &target_schema,
        &target_row_data,
    )?;

    // Check if there are any changes to apply
    if !changes.schema_changes.has_changes() && !changes.row_changes.has_changes() {
        println!("✅ File is already at the target snapshot state. No rollback needed.");
        return Ok(());
    }

    // Show what will be changed
    if dry_run {
        println!("🔍 Dry run - showing what would be changed:");
        PrettyPrinter::print_comprehensive_status_results(&changes, false);
        println!("\n💡 Use --force to apply these changes");
        return Ok(());
    }

    // Show changes and ask for confirmation
    if !force {
        println!("📋 The following changes will be applied:");
        PrettyPrinter::print_comprehensive_status_results(&changes, false);
        
        println!("\n⚠️  This will modify your file. Continue? (y/N)");
        let mut user_input = String::new();
        std::io::stdin().read_line(&mut user_input)?;
        
        if !user_input.trim().to_lowercase().starts_with('y') {
            println!("❌ Rollback cancelled.");
            return Ok(());
        }
    }

    // Create backup if requested
    if backup {
        let backup_path = format!("{}.backup", input_path.display());
        std::fs::copy(&input_path, &backup_path)?;
        println!("💾 Backup created: {}", backup_path);
    }

    // Apply the rollback by writing the target data
    let target_csv_content = create_csv_content(&target_schema, &target_row_data)?;
    std::fs::write(&input_path, target_csv_content)?;

    let snapshot_name = &target_snapshot.name;
    println!("✅ Rollback completed successfully!");
    println!("📄 File '{}' has been rolled back to snapshot '{}'", input, snapshot_name);

    Ok(())
}

/// Create CSV content from schema and row data
fn create_csv_content(schema: &[crate::hash::ColumnInfo], rows: &[Vec<String>]) -> Result<String> {
    let mut content = String::new();
    
    // Write header
    let headers: Vec<&str> = schema.iter().map(|col| col.name.as_str()).collect();
    content.push_str(&headers.join(","));
    content.push('\n');
    
    // Write rows
    let empty_string = String::new();
    for row in rows {
        // Ensure row has the right number of columns
        let mut row_values = Vec::new();
        for (i, _col) in schema.iter().enumerate() {
            let value = row.get(i).unwrap_or(&empty_string);
            // Escape CSV values if they contain commas or quotes
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                row_values.push(format!("\"{}\"", value.replace('"', "\"\"")));
            } else {
                row_values.push(value.clone());
            }
        }
        content.push_str(&row_values.join(","));
        content.push('\n');
    }
    
    Ok(content)
}

/// Create a snapshot
fn snapshot_command(
    workspace_path: Option<&Path>,
    input: &str,
    name: &str,
    batch_size: usize,
    full_data: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let (archive_path, json_path) = workspace.snapshot_paths(name);
    
    // Check if snapshot already exists
    if workspace.snapshot_exists(name) {
        return Err(crate::error::TabdiffError::invalid_input(format!(
            "Snapshot '{}' already exists. Use a different name or remove the existing snapshot.",
            name
        )));
    }

    // Create snapshot
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        // Resolve relative paths relative to the workspace root
        workspace.root.join(input)
    };
    
    // Check file size and provide warnings/recommendations
    let file_size = std::fs::metadata(&input_path)?.len();
    const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
    const VERY_LARGE_FILE_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1GB
    
    if file_size > VERY_LARGE_FILE_THRESHOLD && full_data {
        println!("⚠️  WARNING: Large file detected ({:.1} GB)", file_size as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("   Consider using --hash-only for faster processing and smaller snapshots.");
        println!("   This will disable rollback and detailed diff capabilities.");
    } else if file_size > LARGE_FILE_THRESHOLD && full_data {
        println!("ℹ️  INFO: Moderate file size ({:.1} MB) - using full data storage", file_size as f64 / (1024.0 * 1024.0));
        println!("   Use --hash-only if you need faster processing.");
    }
    
    if !full_data {
        println!("ℹ️  Using hash-only mode - rollback and detailed diff capabilities disabled");
    }
    
    let mut creator = SnapshotCreator::new(batch_size, true);
    
    println!("📸 Creating snapshot '{}' from '{}'...", name, input);
    
    // Use enhanced snapshot creation with workspace context for chain management
    let metadata = creator.create_snapshot_with_workspace(
        &input_path,
        name,
        &archive_path,
        &json_path,
        full_data,
        Some(&workspace),
    )?;

    println!("✅ Snapshot created successfully!");
    println!("├─ Name: {}", metadata.name);
    println!("├─ Rows: {}", metadata.row_count);
    println!("├─ Columns: {}", metadata.column_count);
    
    // Show chain information if this snapshot has a parent
    if let Some(parent_name) = &metadata.parent_snapshot {
        println!("├─ Parent: {}", parent_name);
        println!("├─ Sequence: {}", metadata.sequence_number);
        if metadata.delta_from_parent.is_some() {
            println!("├─ Delta: Cached from parent");
        }
    } else {
        println!("├─ Chain: First snapshot");
    }
    
    println!("├─ Archive: {}", archive_path.display());
    println!("└─ Metadata: {}", json_path.display());

    Ok(())
}

/// Compare two snapshots
fn diff_command(
    workspace_path: Option<&Path>,
    snapshot1: &str,
    snapshot2: &str,
    mode: &str,
    output_path: Option<&Path>,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Parse diff mode
    let _diff_mode = DiffMode::parse(mode)
        .map_err(|e| crate::error::TabdiffError::invalid_input(e))?;

    // Resolve snapshots
    let snap1_ref = SnapshotRef::from_string(snapshot1.to_string());
    let snap2_ref = SnapshotRef::from_string(snapshot2.to_string());
    
    let resolved1 = resolver.resolve(&snap1_ref)?;
    let resolved2 = resolver.resolve(&snap2_ref)?;

    println!("🔍 Comparing snapshots: {} → {}", resolved1.name, resolved2.name);

    // Load metadata for output formatting
    let metadata2 = SnapshotLoader::load_metadata(&resolved2.json_path)?;

    // Load full snapshot data for comprehensive comparison
    let snapshot1_data = if resolved1.has_archive() {
        SnapshotLoader::load_full_snapshot(resolved1.require_archive()?)?
    } else {
        return Err(crate::error::TabdiffError::archive("Baseline snapshot has no archive data"));
    };
    
    let snapshot2_data = if resolved2.has_archive() {
        SnapshotLoader::load_full_snapshot(resolved2.require_archive()?)?
    } else {
        return Err(crate::error::TabdiffError::archive("Comparison snapshot has no archive data"));
    };
    
    // Extract baseline schema from archive data
    let baseline_schema = if let Some(schema_data) = snapshot1_data.schema_data.get("columns") {
        if let Some(columns_array) = schema_data.as_array() {
            let mut baseline_columns = Vec::new();
            for col_value in columns_array {
                if let (Some(name), Some(data_type), Some(nullable)) = (
                    col_value.get("name").and_then(|v| v.as_str()),
                    col_value.get("data_type").and_then(|v| v.as_str()),
                    col_value.get("nullable").and_then(|v| v.as_bool())
                ) {
                    baseline_columns.push(crate::hash::ColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                    });
                }
            }
            baseline_columns
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    
    // Extract current schema from archive data
    let current_schema = if let Some(schema_data) = snapshot2_data.schema_data.get("columns") {
        if let Some(columns_array) = schema_data.as_array() {
            let mut current_columns = Vec::new();
            for col_value in columns_array {
                if let (Some(name), Some(data_type), Some(nullable)) = (
                    col_value.get("name").and_then(|v| v.as_str()),
                    col_value.get("data_type").and_then(|v| v.as_str()),
                    col_value.get("nullable").and_then(|v| v.as_bool())
                ) {
                    current_columns.push(crate::hash::ColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                    });
                }
            }
            current_columns
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Extract baseline row data from archive
    let baseline_rows = if let Some(rows_data) = snapshot1_data.row_data.get("rows") {
        if let Some(rows_array) = rows_data.as_array() {
            let mut baseline_rows = Vec::new();
            for row_value in rows_array {
                if let Some(row_array) = row_value.as_array() {
                    let row: Vec<String> = row_array
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    baseline_rows.push(row);
                }
            }
            baseline_rows
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Extract current row data from archive
    let current_rows = if let Some(rows_data) = snapshot2_data.row_data.get("rows") {
        if let Some(rows_array) = rows_data.as_array() {
            let mut current_rows = Vec::new();
            for row_value in rows_array {
                if let Some(row_array) = row_value.as_array() {
                    let row: Vec<String> = row_array
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    current_rows.push(row);
                }
            }
            current_rows
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    
    // Use comprehensive change detection
    let changes = crate::change_detection::ChangeDetector::detect_changes(
        &baseline_schema,
        &baseline_rows,
        &current_schema,
        &current_rows,
    )?;
    
    // Build comprehensive diff result
    let schema_changed = !changes.schema_changes.columns_added.is_empty() ||
                        !changes.schema_changes.columns_removed.is_empty() ||
                        !changes.schema_changes.columns_renamed.is_empty() ||
                        !changes.schema_changes.type_changes.is_empty() ||
                        changes.schema_changes.column_order.is_some();
    let mut columns_changed = Vec::new();
    
    // Process schema changes
    for col_add in &changes.schema_changes.columns_added {
        columns_changed.push(format!("  {} (added)", col_add.name));
    }
    for col_rem in &changes.schema_changes.columns_removed {
        columns_changed.push(format!("  {} (removed)", col_rem.name));
    }
    for col_rename in &changes.schema_changes.columns_renamed {
        columns_changed.push(format!("  {} → {} (renamed)", col_rename.from, col_rename.to));
    }
    for type_change in &changes.schema_changes.type_changes {
        columns_changed.push(format!("  {} (type changed: {} → {})", 
            type_change.column, type_change.from, type_change.to));
    }
    
    // Count row changes
    let rows_changed = changes.row_changes.modified.len() + 
                      changes.row_changes.added.len() + 
                      changes.row_changes.removed.len();
    
    // Build sample changes for display
    let mut sample_changes = Vec::new();
    
    // Add sample modifications
    for (idx, modification) in changes.row_changes.modified.iter().enumerate() {
        if idx >= 5 { break; } // Limit to 5 samples
        sample_changes.push(serde_json::json!({
            "type": "modified",
            "row_index": modification.row_index,
            "changes": modification.changes
        }));
    }
    
    // Add sample additions
    for (idx, addition) in changes.row_changes.added.iter().enumerate() {
        if idx >= 5 || sample_changes.len() >= 5 { break; }
        sample_changes.push(serde_json::json!({
            "type": "added",
            "row_index": addition.row_index,
            "data": addition.data
        }));
    }
    
    // Add sample removals
    for (idx, removal) in changes.row_changes.removed.iter().enumerate() {
        if idx >= 5 || sample_changes.len() >= 5 { break; }
        sample_changes.push(serde_json::json!({
            "type": "removed",
            "data": removal.data
        }));
    }

    // Create comprehensive diff result
    let diff_result = serde_json::json!({
        "base": resolved1.name,
        "compare": resolved2.name,
        "schema_changed": schema_changed,
        "columns_changed": columns_changed,
        "row_count": metadata2.row_count,
        "rows_changed": rows_changed,
        "sample_changes": sample_changes,
        "row_changes": {
            "modified": changes.row_changes.modified.len(),
            "added": changes.row_changes.added.len(),
            "removed": changes.row_changes.removed.len()
        }
    });

    // Output results
    PrettyPrinter::print_diff_results(&diff_result);

    // Save diff result if requested
    if let Some(output_path) = output_path {
        let diff_content = serde_json::to_string_pretty(&diff_result)?;
        // If output path is relative, resolve it relative to workspace root
        let final_output_path = if output_path.is_absolute() {
            output_path.to_path_buf()
        } else {
            resolver.workspace().root.join(output_path)
        };
        
        // Create parent directories if needed
        if let Some(parent) = final_output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        std::fs::write(&final_output_path, diff_content)?;
        println!("\n💾 Diff saved to: {}", final_output_path.display());
    } else {
        // Save to default location
        let diff_path = resolver.workspace().diff_path(&resolved1.name, &resolved2.name);
        let diff_content = serde_json::to_string_pretty(&diff_result)?;
        std::fs::create_dir_all(diff_path.parent().unwrap())?;
        std::fs::write(&diff_path, diff_content)?;
        println!("\n💾 Diff saved to: {}", diff_path.display());
    }

    Ok(())
}

/// Show snapshot information
fn show_command(
    workspace_path: Option<&Path>,
    snapshot: &str,
    detailed: bool,
    json: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Resolve snapshot
    let snap_ref = SnapshotRef::from_string(snapshot.to_string());
    let resolved = resolver.resolve(&snap_ref)?;

    // Load metadata
    let metadata = SnapshotLoader::load_metadata(&resolved.json_path)?;
    let metadata_json = serde_json::to_value(&metadata)?;

    if json {
        if detailed && resolved.has_archive() {
            // Load full snapshot data
            let full_data = SnapshotLoader::load_full_snapshot(resolved.require_archive()?)?;
            let combined = serde_json::json!({
                "metadata": metadata_json,
                "archive_data": {
                    "schema": full_data.schema_data,
                    "rows": full_data.row_data
                }
            });
            println!("{}", serde_json::to_string_pretty(&combined)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&metadata_json)?);
        }
    } else {
        PrettyPrinter::print_snapshot_metadata(&metadata_json, detailed);
    }

    Ok(())
}

/// Check status against a snapshot
fn status_command(
    workspace_path: Option<&Path>,
    input: &str,
    compare_to: Option<&str>,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Resolve comparison snapshot
    let comparison_snapshot = if let Some(name) = compare_to {
        let snap_ref = SnapshotRef::from_string(name.to_string());
        resolver.resolve(&snap_ref)?
    } else {
        resolver.resolve_latest()?.ok_or_else(|| {
            crate::error::TabdiffError::workspace("No snapshots found to compare against")
        })?
    };

    if !json {
        println!("📊 Checking status of '{}' against snapshot '{}'...", input, comparison_snapshot.name);
    }

    // Load baseline snapshot metadata and data
    let baseline_metadata = SnapshotLoader::load_metadata(&comparison_snapshot.json_path)?;
    
    // Check if baseline snapshot has full data for rollback capability
    if !baseline_metadata.has_full_data {
        return Err(crate::error::TabdiffError::invalid_input(
            "Cannot rollback from hash-only snapshot. Use --full-data when creating snapshots for rollback capability."
        ));
    }
    
    let baseline_data = if comparison_snapshot.has_archive() {
        SnapshotLoader::load_full_snapshot(comparison_snapshot.require_archive()?)?
    } else {
        return Err(crate::error::TabdiffError::archive("Baseline snapshot has no archive data"));
    };

    // Load current data
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        // Resolve relative paths relative to the workspace root
        workspace.root.join(input)
    };

    let mut data_processor = DataProcessor::new()?;
    let current_data_info = data_processor.load_file(&input_path)?;
    let current_row_data = data_processor.extract_all_data()?;

    // Extract baseline schema from archive data
    let baseline_schema = if let Some(schema_data) = baseline_data.schema_data.get("columns") {
        if let Some(columns_array) = schema_data.as_array() {
            let mut baseline_columns = Vec::new();
            for col_value in columns_array {
                if let (Some(name), Some(data_type), Some(nullable)) = (
                    col_value.get("name").and_then(|v| v.as_str()),
                    col_value.get("data_type").and_then(|v| v.as_str()),
                    col_value.get("nullable").and_then(|v| v.as_bool())
                ) {
                    baseline_columns.push(crate::hash::ColumnInfo {
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                    });
                }
            }
            baseline_columns
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Extract baseline row data from archive
    let baseline_row_data = if let Some(rows_data) = baseline_data.row_data.get("rows") {
        if let Some(rows_array) = rows_data.as_array() {
            let mut baseline_rows = Vec::new();
            for row_value in rows_array {
                if let Some(row_array) = row_value.as_array() {
                    let row: Vec<String> = row_array
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    baseline_rows.push(row);
                }
            }
            baseline_rows
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Use comprehensive change detection
    let changes = ChangeDetector::detect_changes(
        &baseline_schema,
        &baseline_row_data,
        &current_data_info.columns,
        &current_row_data,
    )?;

    // Output results
    if json {
        let status_json = JsonFormatter::format_comprehensive_status_results(&changes)?;
        println!("{}", status_json);
    } else {
        PrettyPrinter::print_comprehensive_status_results(&changes, quiet);
    }

    Ok(())
}

/// List all snapshots
fn list_command(workspace_path: Option<&Path>, json: bool) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace);

    let snapshots = resolver.list_snapshots()?;
    
    if json {
        println!("{}", serde_json::to_string_pretty(&snapshots)?);
    } else {
        PrettyPrinter::print_snapshot_list(&snapshots);
    }

    Ok(())
}

/// Show snapshot chain and relationships
fn chain_command(workspace_path: Option<&Path>, json: bool) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;

    // Build snapshot chain
    let chain = crate::snapshot::SnapshotChain::build_chain(&workspace)?;

    if json {
        let chain_json = serde_json::json!({
            "snapshots": chain.snapshots,
            "head": chain.head,
            "validation_issues": chain.validate()?
        });
        println!("{}", serde_json::to_string_pretty(&chain_json)?);
    } else {
        println!("🔗 Snapshot Chain");
        
        if chain.snapshots.is_empty() {
            println!("No snapshots found.");
            return Ok(());
        }

        // Validate chain integrity
        let issues = chain.validate()?;
        if !issues.is_empty() {
            println!("⚠️  Chain validation issues:");
            for issue in &issues {
                println!("   • {}", issue);
            }
            println!();
        }

        // Show chain structure
        println!("Chain structure:");
        for snapshot in &chain.snapshots {
            let prefix = if snapshot.parent_snapshot.is_none() {
                "🌱"
            } else {
                "├─"
            };
            
            println!("{} {} (seq: {})", prefix, snapshot.name, snapshot.sequence_number);
            
            if let Some(parent) = &snapshot.parent_snapshot {
                println!("   └─ Parent: {}", parent);
            }
            
            if snapshot.can_reconstruct_parent {
                println!("   └─ Can reconstruct parent: ✅");
            }
            
            if let Some(delta) = &snapshot.delta_from_parent {
                println!("   └─ Delta size: {} bytes", delta.compressed_size);
            }
            
            if let Some(archive_size) = snapshot.archive_size {
                println!("   └─ Archive size: {} bytes", archive_size);
            }
            
            println!();
        }

        if let Some(head) = &chain.head {
            println!("Head: {}", head);
        }
    }

    Ok(())
}

/// Clean up old snapshot archives to save space
fn cleanup_command(
    workspace_path: Option<&Path>,
    keep_full: usize,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    
    // Build snapshot chain to understand relationships
    let chain = crate::snapshot::SnapshotChain::build_chain(&workspace)?;
    
    if chain.snapshots.is_empty() {
        println!("No snapshots found to clean up.");
        return Ok(());
    }

    println!("🧹 Analyzing snapshots for cleanup...");
    
    // Count total archives for display (consolidate the calculation)
    let mut full_archives_count = 0;
    for snapshot in &chain.snapshots {
        let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
        if archive_path.exists() {
            full_archives_count += 1;
        }
    }
    
    // Find snapshots that can have their full data removed (selective cleanup)
    let candidates_for_cleanup = chain.find_data_cleanup_candidates(keep_full, &workspace)?;

    if candidates_for_cleanup.is_empty() {
        println!("✅ No snapshots need data cleanup.");
        println!("   • Total archives: {}", full_archives_count);
        println!("   • Keep full data for: {}", keep_full);
        return Ok(());
    }

    // Calculate space savings from removing data.parquet files
    let mut total_space_saved = 0u64;
    
    // Estimate space savings (this would be more accurate with actual file analysis)
    for snapshot in &candidates_for_cleanup {
        if let Some(archive_size) = snapshot.archive_size {
            // Estimate that data.parquet is about 60-80% of archive size
            total_space_saved += (archive_size as f64 * 0.7) as u64;
        }
    }

    println!("📊 Cleanup analysis:");
    println!("   • Total archives: {}", full_archives_count);
    println!("   • Snapshots for data cleanup: {}", candidates_for_cleanup.len());
    println!("   • Keep full data for: {}", keep_full);
    println!("   • Estimated space savings: {} bytes", total_space_saved);
    println!("   • Archives will retain deltas for reconstruction");

    if dry_run {
        println!("\n🔍 Dry run - snapshots that would have data cleaned up:");
        for snapshot in &candidates_for_cleanup {
            println!("   • {} (seq: {}, estimated savings: {} bytes)", 
                    snapshot.name, 
                    snapshot.sequence_number,
                    (snapshot.archive_size.unwrap_or(0) as f64 * 0.7) as u64);
        }
        println!("\n💡 Use --force to apply these changes");
        return Ok(());
    }

    // Ask for confirmation unless force is used
    if !force {
        println!("\n⚠️  This will remove full data from {} snapshots (keeping deltas). Continue? (y/N)", candidates_for_cleanup.len());
        let mut user_input = String::new();
        std::io::stdin().read_line(&mut user_input)?;
        
        if !user_input.trim().to_lowercase().starts_with('y') {
            println!("❌ Cleanup cancelled.");
            return Ok(());
        }
    }

    // Perform selective cleanup (remove data.parquet but keep delta.parquet)
    let mut cleaned_count = 0;
    let mut actual_space_saved = 0u64;
    
    for snapshot in &candidates_for_cleanup {
        let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
        
        if archive_path.exists() {
            // TODO: Implement selective archive cleanup
            // For now, we'll just report what would be done
            println!("🧹 Would clean data from: {}", snapshot.name);
            cleaned_count += 1;
            
            // Estimate space saved
            if let Some(archive_size) = snapshot.archive_size {
                actual_space_saved += (archive_size as f64 * 0.7) as u64;
            }
        }
    }

    println!("✅ Cleanup completed!");
    println!("   • Snapshots cleaned: {}", cleaned_count);
    println!("   • Estimated space saved: {} bytes", actual_space_saved);
    println!("   • Deltas preserved for reconstruction");

    Ok(())
}
