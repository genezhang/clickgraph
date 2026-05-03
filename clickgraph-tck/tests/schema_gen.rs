//! Scans openCypher TCK feature files to build a universal ClickGraph schema.
//!
//! Because chdb supports only one session per process, we create a single
//! `Database` at startup with a schema that covers **all** labels and
//! relationship types seen across the selected feature files.  Between
//! scenarios, we truncate the tables rather than recreating the database.
//!
//! The generated YAML uses the writable (no `source:`) schema format so
//! that ClickGraph auto-creates `ReplacingMergeTree` tables on startup.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::create_parser::{parse_create_block, PropValue};

// ---------------------------------------------------------------------------
// Type inference
// ---------------------------------------------------------------------------

/// Inferred ClickHouse-compatible type for a property column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferredType {
    Integer,
    Float,
    Boolean,
    String, // default / fallback
}

impl InferredType {
    /// Promote `self` to accommodate a new observed type hint.
    fn merge(&self, hint: &str) -> Self {
        match (self, hint) {
            (Self::Integer, "int") => Self::Integer,
            (Self::Integer, "float") => Self::Float,
            (Self::Float, "int") | (Self::Float, "float") => Self::Float,
            (Self::Boolean, "bool") => Self::Boolean,
            // Any string value collapses to String
            _ => Self::String,
        }
    }

    /// Schema YAML type string.
    pub fn yaml_type(&self) -> Option<&'static str> {
        match self {
            Self::Integer => Some("integer"),
            Self::Float => Some("float"),
            Self::Boolean => Some("boolean"),
            Self::String => None, // default; no need to specify
        }
    }
}

// ---------------------------------------------------------------------------
// Schema catalog
// ---------------------------------------------------------------------------

/// A catalog of all labels and relationship types observed during the feature scan.
#[derive(Debug, Default)]
pub struct SchemaCatalog {
    /// label → property_name → inferred type
    pub nodes: BTreeMap<String, BTreeMap<String, InferredType>>,
    /// (rel_type, from_label, to_label) → property_name → inferred type
    pub edges: BTreeMap<(String, String, String), BTreeMap<String, InferredType>>,
}

impl SchemaCatalog {
    /// Ingest the output of `parse_create_block` into the catalog.
    /// `var_labels` maps variable names to their labels (built from the parsed nodes).
    pub fn ingest_parsed(
        &mut self,
        nodes: &[crate::create_parser::ParsedNode],
        edges: &[crate::create_parser::ParsedEdge],
    ) {
        // Build a local var→label map so edges can look up endpoint labels.
        let mut var_to_label: BTreeMap<String, String> = BTreeMap::new();
        for node in nodes {
            if let (Some(var), Some(label)) = (&node.var, &node.label) {
                var_to_label.insert(var.clone(), label.clone());
            }
        }

        // Ingest nodes
        for node in nodes {
            let label = match &node.label {
                Some(l) => l.clone(),
                None => "__Unlabeled".to_string(),
            };
            let entry = self.nodes.entry(label).or_default();
            for (prop, val) in &node.props {
                let hint = val.type_hint();
                let typ = entry.entry(prop.clone()).or_insert(type_from_hint(hint));
                *typ = typ.merge(hint);
            }
        }

        // Ingest edges
        for edge in edges {
            let from_label = var_to_label
                .get(&edge.from_var)
                .cloned()
                .unwrap_or_else(|| "__Unlabeled".to_string());
            let to_label = var_to_label
                .get(&edge.to_var)
                .cloned()
                .unwrap_or_else(|| "__Unlabeled".to_string());
            let key = (edge.rel_type.clone(), from_label, to_label);
            let entry = self.edges.entry(key).or_default();
            for (prop, val) in &edge.props {
                let hint = val.type_hint();
                let typ = entry.entry(prop.clone()).or_insert(type_from_hint(hint));
                *typ = typ.merge(hint);
            }
        }
    }

    /// Return all ClickHouse table names managed by this schema.
    pub fn all_table_names(&self) -> Vec<String> {
        let mut tables: Vec<String> = self.nodes.keys().map(|l| node_table_name(l)).collect();
        for (key, _) in &self.edges {
            tables.push(edge_table_name(&key.0, &key.1, &key.2));
        }
        tables
    }
}

fn type_from_hint(hint: &str) -> InferredType {
    match hint {
        "int" => InferredType::Integer,
        "float" => InferredType::Float,
        "bool" => InferredType::Boolean,
        _ => InferredType::String,
    }
}

// ---------------------------------------------------------------------------
// Feature file scanning
// ---------------------------------------------------------------------------

/// Scan all `.feature` files under `features_dir` and build a `SchemaCatalog`.
///
/// Files with a Feature-level `@wip` / `@skip` / `@fails` / `@crash` tag are
/// excluded from schema inference too — otherwise unrun feature files
/// (typically ones imported pending harness extensions) would inflate the
/// universal schema with labels and properties that don't appear in any
/// running scenario.
pub fn scan_features(features_dir: &str) -> SchemaCatalog {
    let mut catalog = SchemaCatalog::default();

    let walker = walkdir_feature_files(features_dir);
    for path in walker {
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if feature_is_filtered(&content) {
            continue;
        }
        scan_feature_content(&content, &mut catalog);
    }

    // Always ensure we have at least a dummy label so the schema is non-empty.
    // Some feature files only use unlabeled nodes.
    catalog.nodes.entry("__Unlabeled".to_string()).or_default();

    catalog
}

/// True if the file carries a Feature-level filter tag (`@wip`, `@skip`,
/// `@fails`, `@crash`) — same set the cucumber harness skips. We look at
/// any standalone tag line that appears *before* the `Feature:` keyword.
fn feature_is_filtered(content: &str) -> bool {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("Feature:") {
            return false;
        }
        if let Some(rest) = trimmed.strip_prefix('@') {
            // Only the bare tag (whitespace-separated tokens). Match the
            // tags the cucumber filter recognises.
            for tok in rest.split_whitespace() {
                let tok = tok.trim_start_matches('@');
                if matches!(tok, "wip" | "skip" | "fails" | "crash" | "NegativeTests") {
                    return true;
                }
            }
        }
    }
    false
}

/// Recursively collect `.feature` file paths under `dir`.
fn walkdir_feature_files(dir: &str) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    collect_feature_files(Path::new(dir), &mut result);
    result
}

fn collect_feature_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_feature_files(&path, out);
        } else if path.extension().map(|e| e == "feature").unwrap_or(false) {
            out.push(path);
        }
    }
}

/// Extract CREATE blocks from a feature file and ingest them into the catalog.
fn scan_feature_content(content: &str, catalog: &mut SchemaCatalog) {
    // Find all docstring blocks following "having executed:" steps.
    // Docstrings are delimited by triple-quote lines (`"""`).
    let mut in_docstring = false;
    let mut docstring_buf = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == "\"\"\"" {
            if in_docstring {
                // End of docstring — check if it contains CREATE
                if docstring_buf.to_uppercase().contains("CREATE") {
                    let mut dummy_var_map = std::collections::HashMap::new();
                    let parsed = parse_create_block(&docstring_buf, &mut dummy_var_map);
                    catalog.ingest_parsed(&parsed.nodes, &parsed.edges);
                }
                docstring_buf.clear();
                in_docstring = false;
            } else {
                in_docstring = true;
            }
            continue;
        }
        if in_docstring {
            docstring_buf.push_str(line);
            docstring_buf.push('\n');
        }
    }
}

// ---------------------------------------------------------------------------
// YAML generation
// ---------------------------------------------------------------------------

/// Generate a ClickGraph-compatible YAML schema string from a `SchemaCatalog`.
pub fn generate_yaml(catalog: &SchemaCatalog) -> String {
    let mut yaml = String::from("name: tck\ngraph_schema:\n  nodes:\n");

    for (label, props) in &catalog.nodes {
        let table = node_table_name(label);
        yaml.push_str(&format!("    - label: {label}\n"));
        yaml.push_str("      database: default\n");
        yaml.push_str(&format!("      table: {table}\n"));
        yaml.push_str("      node_id: _tck_id\n");
        yaml.push_str("      type: string\n");
        yaml.push_str("      property_mappings:\n");
        yaml.push_str("        _tck_id: _tck_id\n");

        let mut sorted_props: Vec<_> = props.keys().collect();
        sorted_props.sort();
        for prop in &sorted_props {
            let col = sanitize_col(prop);
            yaml.push_str(&format!("        {prop}: {col}\n"));
        }

        // property_types for non-String properties
        let typed: Vec<_> = sorted_props
            .iter()
            .filter_map(|p| props[*p].yaml_type().map(|t| (p, t)))
            .collect();
        if !typed.is_empty() {
            yaml.push_str("      property_types:\n");
            for (prop, typ) in typed {
                yaml.push_str(&format!("        {prop}: {typ}\n"));
            }
        }
    }

    yaml.push_str("  edges:\n");
    for ((rel_type, from_label, to_label), props) in &catalog.edges {
        let table = edge_table_name(rel_type, from_label, to_label);
        yaml.push_str(&format!("    - type: {rel_type}\n"));
        yaml.push_str("      database: default\n");
        yaml.push_str(&format!("      table: {table}\n"));
        yaml.push_str(&format!("      from_node: {from_label}\n"));
        yaml.push_str(&format!("      to_node: {to_label}\n"));
        yaml.push_str("      from_id: from_id\n");
        yaml.push_str("      to_id: to_id\n");

        if !props.is_empty() {
            yaml.push_str("      property_mappings:\n");
            let mut sorted_props: Vec<_> = props.keys().collect();
            sorted_props.sort();
            for prop in &sorted_props {
                let col = sanitize_col(prop);
                yaml.push_str(&format!("        {prop}: {col}\n"));
            }
            let typed: Vec<_> = sorted_props
                .iter()
                .filter_map(|p| props[*p].yaml_type().map(|t| (p, t)))
                .collect();
            if !typed.is_empty() {
                yaml.push_str("      property_types:\n");
                for (prop, typ) in typed {
                    yaml.push_str(&format!("        {prop}: {typ}\n"));
                }
            }
        }
    }

    yaml
}

// ---------------------------------------------------------------------------
// Naming helpers
// ---------------------------------------------------------------------------

/// ClickHouse table name for a node label.
/// e.g. "Person" → "tck_n_person", "__Unlabeled" → "tck_n_unlabeled"
pub fn node_table_name(label: &str) -> String {
    let sanitized = label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("tck_n_{sanitized}")
}

/// ClickHouse table name for an edge.
/// e.g. ("KNOWS", "A", "B") → "tck_e_knows_a_b"
pub fn edge_table_name(rel_type: &str, from_label: &str, to_label: &str) -> String {
    let rt = rel_type
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let fl = from_label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let tl = to_label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("tck_e_{rt}_{fl}_{tl}")
}

/// Make a property name safe as a ClickHouse column name.
fn sanitize_col(prop: &str) -> String {
    prop.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_table_name() {
        assert_eq!(node_table_name("Person"), "tck_n_person");
        assert_eq!(node_table_name("__Unlabeled"), "tck_n___unlabeled");
    }

    #[test]
    fn test_edge_table_name() {
        assert_eq!(edge_table_name("KNOWS", "A", "B"), "tck_e_knows_a_b");
    }

    #[test]
    fn test_type_merge() {
        let t = InferredType::Integer;
        assert_eq!(t.merge("int"), InferredType::Integer);
        assert_eq!(t.merge("float"), InferredType::Float);
        assert_eq!(t.merge("string"), InferredType::String);
    }

    #[test]
    fn test_feature_is_filtered_recognises_filter_tags() {
        // Feature-level filter tag → excluded from schema inference.
        let wip_feature = "@wip\nFeature: Foo\n  Scenario: bar\n    Given x\n";
        assert!(feature_is_filtered(wip_feature));

        let skip_feature = "@skip\nFeature: Foo\n";
        assert!(feature_is_filtered(skip_feature));

        let fails_feature = "@fails\nFeature: Foo\n";
        assert!(feature_is_filtered(fails_feature));

        // Non-filter tags don't trigger exclusion.
        let plain = "@SomeAnnotation\nFeature: Foo\n";
        assert!(!feature_is_filtered(plain));

        // Tag line below the Feature: line is per-scenario, not feature-level.
        let scenario_tag = "Feature: Foo\n  @wip\n  Scenario: bar\n";
        assert!(!feature_is_filtered(scenario_tag));

        // No tags at all.
        assert!(!feature_is_filtered("Feature: Foo\n  Scenario: bar\n"));
    }

    #[test]
    fn test_thelabel_schema() {
        let catalog = scan_features("tests/features");
        let yaml = generate_yaml(&catalog);
        let thelabel_section: String = yaml
            .lines()
            .skip_while(|l| !l.contains("label: TheLabel"))
            .take(20)
            .collect::<Vec<_>>()
            .join("\n");
        eprintln!("TheLabel schema:\n{}", thelabel_section);
        assert!(
            thelabel_section.contains("id: id"),
            "Expected 'id: id' mapping in TheLabel schema, got:\n{}",
            thelabel_section
        );
    }
}
