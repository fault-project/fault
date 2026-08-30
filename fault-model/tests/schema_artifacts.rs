#[path = "../schema_artifacts.rs"]
mod schema_artifacts;

#[test]
fn committed_schemas_match_the_model() {
    for (name, generated) in schema_artifacts::schemas() {
        let committed = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("docs/schemas")
                .join(name),
        )
        .unwrap();
        assert_schema(&committed, &generated);
    }
}

#[test]
fn committed_reference_matches_the_model() {
    let schemas = schema_artifacts::schemas();
    assert_eq!(
        include_str!("../../docs/reference.html"),
        schema_artifacts::reference_html(&schemas),
        "run `cargo run -p fault-model --example generate_schemas` to refresh the reference"
    );
}

fn assert_schema(committed: &str, generated: &impl serde::Serialize) {
    let committed: serde_json::Value = serde_json::from_str(committed).unwrap();
    let generated = serde_json::to_value(generated).unwrap();
    assert_eq!(
        committed, generated,
        "run `cargo run -p fault-model --example generate_schemas` to refresh the schemas"
    );
}
