//! Command implementations for tabdiff CLI

use crate::cli::{Commands, DiffMode, OutputFormat, SamplingStrategy};
use crate::data::DataProcessor;
use crate::error::Result;
use crate::hash::HashComputer;
use crate::output::{OutputManager, PrettyPrinter};
use crate::resolver::{SnapshotRef, SnapshotResolver};
use crate::snapshot::{SnapshotCreator, SnapshotLoader};
use crate::workspace::TabdiffWorkspace;
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
        } => snapshot_command(workspace_path, &input, &name, &sample, batch_size),
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

/// Create a snapshot
fn snapshot_command(
    workspace_path: Option<&Path>,
    input: &str,
    name: &str,
    sample: &str,
    batch_size: usize,
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
    let sampling = SamplingStrategy::parse(sample)
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
    let baseline_metadata = SnapshotLoader::load_metadata(&comparison_snapshot.json_path)?;
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

    // Initialize hash computer
    let hash_computer = HashComputer::new(1000);

    // Compare schemas
    let current_schema_hash = hash_computer.hash_schema(&current_data_info.columns)?;
    let schema_changed = current_schema_hash.hash != baseline_metadata.schema_hash;

    // Compare column schemas (not data content)
    let mut columns_changed = Vec::new();
    
    // Get baseline schema from metadata
    let baseline_schema_data = if let Some(schema_data) = baseline_data.schema_data.get("columns") {
        schema_data.as_array().unwrap_or(&Vec::new()).clone()
    } else {
        Vec::new()
    };
    
    // Create maps for easy comparison
    let mut baseline_columns = std::collections::HashMap::new();
    for col_value in &baseline_schema_data {
        if let (Some(name), Some(data_type), Some(nullable)) = (
            col_value.get("name").and_then(|v| v.as_str()),
            col_value.get("data_type").and_then(|v| v.as_str()),
            col_value.get("nullable").and_then(|v| v.as_bool())
        ) {
            baseline_columns.insert(name.to_string(), (data_type.to_string(), nullable));
        }
    }
    
    let mut current_columns = std::collections::HashMap::new();
    for col in &current_data_info.columns {
        current_columns.insert(col.name.clone(), (col.data_type.clone(), col.nullable));
    }
    
    // Check for changed or removed columns
    for (col_name, (baseline_type, baseline_nullable)) in &baseline_columns {
        if let Some((current_type, current_nullable)) = current_columns.get(col_name) {
            if baseline_type != current_type || baseline_nullable != current_nullable {
                columns_changed.push(col_name.clone());
            }
        } else {
            columns_changed.push(format!("{} (removed)", col_name));
        }
    }
    
    // Check for added columns
    for col_name in current_columns.keys() {
        if !baseline_columns.contains_key(col_name) {
            columns_changed.push(format!("{} (added)", col_name));
        }
    }

    // Compare rows
    let current_row_hashes = hash_computer.hash_rows(&current_row_data, &sampling)?;
    
    // Extract baseline row hashes from archive data
    let baseline_row_hashes = if let Some(rows_data) = baseline_data.row_data.get("row_hashes") {
        if let Some(row_hashes_array) = rows_data.as_array() {
            let mut baseline_hashes = Vec::new();
            for row_hash_value in row_hashes_array {
                if let (Some(row_index), Some(hash)) = (
                    row_hash_value.get("row_index").and_then(|v| v.as_u64()),
                    row_hash_value.get("hash").and_then(|v| v.as_str())
                ) {
                    baseline_hashes.push(crate::hash::RowHash {
                        row_index,
                        hash: hash.to_string(),
                    });
                }
            }
            baseline_hashes
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let row_comparison = hash_computer.compare_row_hashes(&baseline_row_hashes, &current_row_hashes);

    if json {
        let status_json = crate::output::JsonFormatter::format_status_results(
            schema_changed,
            &columns_changed,
            &row_comparison,
        )?;
        println!("{}", status_json);
    } else {
        PrettyPrinter::print_status_results(
            schema_changed,
            &columns_changed,
            &row_comparison,
            quiet,
        );
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
