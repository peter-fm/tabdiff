//! Command implementations for tabdiff CLI

use crate::cli::{Commands, DiffMode, OutputFormat, SamplingStrategy};
use crate::error::Result;
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
    let workspace = if force {
        let current_dir = std::env::current_dir()?;
        let root = workspace_path.unwrap_or(&current_dir);
        TabdiffWorkspace::create_new(root.to_path_buf())?
    } else {
        TabdiffWorkspace::find_or_create(workspace_path)?
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
    let input_path = Path::new(input);
    let mut creator = SnapshotCreator::new(batch_size, true);
    
    println!("📸 Creating snapshot '{}' from '{}'...", name, input);
    
    let metadata = creator.create_snapshot(
        input_path,
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
    let resolver = SnapshotResolver::new(workspace);

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
        std::fs::write(output_path, diff_content)?;
        println!("\n💾 Diff saved to: {}", output_path.display());
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
    let resolver = SnapshotResolver::new(workspace);

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
    let resolver = SnapshotResolver::new(workspace);

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

    // For now, just show a simple status
    // In a full implementation, this would load the current data and compare
    let schema_changed = false;
    let columns_changed: Vec<String> = Vec::new();
    let row_comparison = crate::hash::RowHashComparison {
        changed_rows: Vec::new(),
        added_rows: Vec::new(),
        removed_rows: Vec::new(),
        total_base: 0,
        total_compare: 0,
    };

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
