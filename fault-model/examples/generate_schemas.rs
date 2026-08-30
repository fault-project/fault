use std::fs;
use std::path::Path;

#[path = "../schema_artifacts.rs"]
mod schema_artifacts;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("fault-model is inside the workspace");
    let output = workspace.join("docs").join("schemas");
    fs::create_dir_all(&output)?;

    let schemas = schema_artifacts::schemas();
    for (name, schema) in &schemas {
        write_schema(output.join(name), schema)?;
    }
    fs::write(
        workspace.join("docs").join("reference.html"),
        schema_artifacts::reference_html(&schemas),
    )?;
    Ok(())
}

fn write_schema(
    path: impl AsRef<Path>,
    schema: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut json = serde_json::to_string_pretty(schema)?;
    json.push('\n');
    fs::write(path, json)?;
    Ok(())
}
