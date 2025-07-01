//! Command implementations for tabdiff CLI

use crate::cli::{Commands, DiffMode, OutputFormat, SamplingStrategy};
use crate::data::DataProcessor;
use crate::error::Result;
use crate::output::{OutputManager, PrettyPrinter, JsonFormatter};
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
            sample,
            batch_size,
            full_data,
        } => snapshot_command(workspace_path, &input, &name, &sample, batch_size, full_data),
        Commands::Diff {
            snapshot1,
            snapshot2,
            mode,
            output,
        } => diff_command(workspace_path, &snapshot1, &snapshot2, &mode, output.as_deref()),
        Commands::Show {
            snapshot,
            detailed,
            format,
        } => show_command(workspace_path, &snapshot, detailed, &format),
        Commands::Status {
            input,
            compare_to,
            sample,
            quiet,
            json,
        } => status_command(workspace_path, &input, compare_to.as_deref(), &sample, quiet, json),
        Commands::List { format } => list_command(workspace_path, &format),
        Commands::Rollback {
            input,
            to,
            dry_run,
            force,
            backup,
        } => rollback_command(workspace_path, &input, &to, dry_run, force, backup),
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

    let data_processor = DataProcessor::new()?;
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
    sample: &str,
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

    // Parse sampling strategy
    let sampling = SamplingStrategy::parse(sample)
        .map_err(|e| crate::error::TabdiffError::invalid_sampling(e))?;

    // Create snapshot
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        // Resolve relative paths relative to the workspace root
        workspace.root.join(input)
    };
    
    let mut creator = SnapshotCreator::new(batch_size, true);
    
    println!("📸 Creating snapshot '{}' from '{}'...", name, input);
    
    let metadata = creator.create_snapshot(
        &input_path,
        name,
        &sampling,
        &archive_path,
        &json_path,
        full_data,
    )?;

    println!("✅ Snapshot created successfully!");
    println!("├─ Name: {}", metadata.name);
    println!("├─ Rows: {}", metadata.row_count);
    println!("├─ Columns: {}", metadata.column_count);
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

    // Load metadata for quick comparison
    let metadata1 = SnapshotLoader::load_metadata(&resolved1.json_path)?;
    let metadata2 = SnapshotLoader::load_metadata(&resolved2.json_path)?;

    // Simple diff implementation
    let schema_changed = metadata1.schema_hash != metadata2.schema_hash;
    let mut columns_changed = Vec::new();
    
    for (col_name, hash1) in &metadata1.columns {
        if let Some(hash2) = metadata2.columns.get(col_name) {
            if hash1 != hash2 {
                columns_changed.push(col_name.clone());
            }
        } else {
            columns_changed.push(format!("{} (removed)", col_name));
        }
    }
    
    for col_name in metadata2.columns.keys() {
        if !metadata1.columns.contains_key(col_name) {
            columns_changed.push(format!("{} (added)", col_name));
        }
    }

    // Create diff result
    let diff_result = serde_json::json!({
        "base": resolved1.name,
        "compare": resolved2.name,
        "schema_changed": schema_changed,
        "columns_changed": columns_changed,
        "row_count": metadata2.row_count,
        "rows_changed": 0, // Would need full comparison for this
        "sample_changes": []
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
    format: &str,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    let output_format = OutputFormat::parse(format)
        .map_err(|e| crate::error::TabdiffError::invalid_input(e))?;

    // Resolve snapshot
    let snap_ref = SnapshotRef::from_string(snapshot.to_string());
    let resolved = resolver.resolve(&snap_ref)?;

    // Load metadata
    let metadata = SnapshotLoader::load_metadata(&resolved.json_path)?;
    let metadata_json = serde_json::to_value(&metadata)?;

    match output_format {
        OutputFormat::Pretty => {
            PrettyPrinter::print_snapshot_metadata(&metadata_json, detailed);
        }
        OutputFormat::Json => {
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
        }
    }

    Ok(())
}

/// Check status against a snapshot
fn status_command(
    workspace_path: Option<&Path>,
    input: &str,
    compare_to: Option<&str>,
    sample: &str,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Parse sampling strategy
    let _sampling = SamplingStrategy::parse(sample)
        .map_err(|e| crate::error::TabdiffError::invalid_sampling(e))?;

    // Resolve comparison snapshot
    let comparison_snapshot = if let Some(name) = compare_to {
        let snap_ref = SnapshotRef::from_string(name.to_string());
        resolver.resolve(&snap_ref)?
    } else {
        resolver.resolve_latest()?.ok_or_else(|| {
            crate::error::TabdiffError::workspace("No snapshots found to compare against")
        })?
    };

    println!("📊 Checking status of '{}' against snapshot '{}'...", input, comparison_snapshot.name);

    // Load baseline snapshot metadata and data
    let _baseline_metadata = SnapshotLoader::load_metadata(&comparison_snapshot.json_path)?;
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

    let data_processor = DataProcessor::new()?;
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
fn list_command(workspace_path: Option<&Path>, format: &str) -> Result<()> {
    let workspace = TabdiffWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace);

    let output_format = OutputFormat::parse(format)
        .map_err(|e| crate::error::TabdiffError::invalid_input(e))?;

    let snapshots = resolver.list_snapshots()?;
    
    let output_manager = OutputManager::new(output_format);
    output_manager.output_snapshot_list(&snapshots)?;

    Ok(())
}
