//! Compact, agent-friendly schema formatter.
//!
//! Produces a text representation optimised for LLM consumption:
//! Cypher-native notation so the model already knows the query syntax.

use clickgraph::graph_catalog::graph_schema::GraphSchema;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Format a GraphSchema as compact text for agents/LLMs.
pub fn format_text(schema: &GraphSchema) -> String {
    let mut out = String::new();

    out.push_str(&format!("Graph: {}\n", schema.database()));
    out.push('\n');

    // --- Node Labels ---
    out.push_str("Node Labels:\n");
    for (label, node) in schema.all_node_schemas() {
        // Collect Cypher property names with their ClickHouse types
        let mut props: Vec<String> = node
            .property_mappings
            .keys()
            .map(|k| {
                // Try to get property type from property_types map
                let dtype = node
                    .property_types
                    .get(k)
                    .map(|t| format!("{:?}", t))
                    .unwrap_or_else(|| "String".to_string());
                format!("{}: {}", k, dtype)
            })
            .collect();
        props.sort();

        // Add a note if this is a polymorphic/denormalized node
        let mut notes = vec![];
        if node.is_denormalized {
            notes.push("denormalized");
        }
        if node.label_column.is_some() {
            notes.push("polymorphic");
        }
        let note_str = if notes.is_empty() {
            String::new()
        } else {
            format!("  # {}", notes.join(", "))
        };

        if props.is_empty() {
            out.push_str(&format!("  {:12} {{}}{}  \n", label, note_str));
        } else {
            out.push_str(&format!("  {:12} {{{}}}{}  \n", label, props.join(", "), note_str));
        }
    }

    out.push('\n');

    // --- Relationships ---
    // Group by base type name (before "::" composite key separator)
    // and collect (from, to) pairs for multi-variant relationships
    out.push_str("Relationships:\n");
    let mut rel_groups: BTreeMap<String, Vec<(&str, &str, bool)>> = BTreeMap::new();

    for (rel_key, rel) in schema.get_relationships_schemas() {
        let base_type = rel_key.split("::").next().unwrap_or(rel_key.as_str());
        let is_undirected = is_undirected(rel_key, schema);
        rel_groups
            .entry(base_type.to_string())
            .or_default()
            .push((&rel.from_node, &rel.to_node, is_undirected));
    }

    for (rel_type, variants) in &rel_groups {
        // Deduplicate (from, to, directed) tuples
        let mut seen: BTreeSet<(&str, &str, bool)> = BTreeSet::new();
        let mut unique: Vec<(&str, &str, bool)> = Vec::new();
        for v in variants {
            if seen.insert(*v) {
                unique.push(*v);
            }
        }

        // Group from_nodes and to_nodes
        let froms: BTreeSet<&str> = unique.iter().map(|(f, _, _)| *f).collect();
        let tos: BTreeSet<&str> = unique.iter().map(|(_, t, _)| *t).collect();
        let undirected = unique.iter().any(|(_, _, u)| *u);

        let from_str = node_set_str(&froms);
        let to_str = node_set_str(&tos);
        let arrow = if undirected { "-" } else { "->" };

        out.push_str(&format!(
            "  ({})-[:{}]{}({})\n",
            from_str, rel_type, arrow, to_str
        ));
    }

    out.push('\n');

    // --- Notes ---
    let undirected_rels: Vec<&str> = schema
        .get_relationships_schemas()
        .iter()
        .filter(|(k, _)| is_undirected(k, schema))
        .map(|(k, _)| k.split("::").next().unwrap_or(k.as_str()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    out.push_str("Notes:\n");
    out.push_str("  - Property names in Cypher may differ from ClickHouse column names\n");
    out.push_str("  - Queries are read-only (MATCH/RETURN only; no writes)\n");
    if !undirected_rels.is_empty() {
        out.push_str(&format!(
            "  - Undirected relationships (use -[]-): {}\n",
            undirected_rels.join(", ")
        ));
    }

    out
}

/// Format a GraphSchema as a JSON object for machine consumption.
pub fn format_json(schema: &GraphSchema) -> Value {
    let nodes: Vec<Value> = schema
        .all_node_schemas()
        .iter()
        .map(|(label, node)| {
            let mut props: Vec<Value> = node
                .property_mappings
                .keys()
                .map(|k| {
                    let column = node
                        .property_mappings
                        .get(k)
                        .map(|v| v.raw().to_string())
                        .unwrap_or_else(|| k.clone());
                    let dtype = node
                        .property_types
                        .get(k)
                        .map(|t| format!("{:?}", t))
                        .unwrap_or_else(|| "String".to_string());
                    json!({ "name": k, "column": column, "type": dtype })
                })
                .collect();
            props.sort_by_key(|p| p["name"].as_str().unwrap_or("").to_string());
            json!({ "label": label, "properties": props })
        })
        .collect();

    let mut rel_groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for (rel_key, rel) in schema.get_relationships_schemas() {
        let base_type = rel_key.split("::").next().unwrap_or(rel_key.as_str());
        let undirected = is_undirected(rel_key, schema);
        rel_groups
            .entry(base_type.to_string())
            .or_default()
            .push(json!({
                "from": rel.from_node,
                "to": rel.to_node,
                "undirected": undirected
            }));
    }

    let relationships: Vec<Value> = rel_groups
        .into_iter()
        .map(|(rel_type, variants)| {
            json!({ "type": rel_type, "variants": variants })
        })
        .collect();

    json!({
        "database": schema.database(),
        "nodes": nodes,
        "relationships": relationships
    })
}

fn node_set_str(nodes: &BTreeSet<&str>) -> String {
    if nodes.len() == 1 {
        format!(":{}", nodes.iter().next().unwrap())
    } else {
        let labels: Vec<&str> = nodes.iter().copied().collect();
        format!(":{}", labels.join("|:"))
    }
}

/// Heuristic: a relationship is undirected if its rel_key has no "::" suffix
/// and the type name conventionally represents symmetric relationships.
/// For now we detect by checking if there are two variants with swapped from/to.
fn is_undirected(rel_key: &str, schema: &GraphSchema) -> bool {
    let rel = match schema.get_relationships_schemas().get(rel_key) {
        Some(r) => r,
        None => return false,
    };
    // Check if a variant with swapped from/to exists (bidirectional == undirected in YAML)
    let base_type = rel_key.split("::").next().unwrap_or(rel_key);
    schema
        .get_relationships_schemas()
        .iter()
        .filter(|(k, _)| k.split("::").next().unwrap_or(k.as_str()) == base_type)
        .any(|(_, other)| other.from_node == rel.to_node && other.to_node == rel.from_node)
}
