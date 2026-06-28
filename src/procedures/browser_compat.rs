//! Compatibility procedures the Neo4j Browser probes on connect / sidebar
//! refresh. Recent `neo4j` Browser builds call several read-only system
//! procedures that ClickGraph did not register, so the registry returned an
//! `Unknown procedure` FAILURE. A FAILURE on these probes does more than blank
//! the "Database Information" sidebar — it can abort the Browser's metadata +
//! graph-styling initialization, which then renders query-result nodes/edges
//! without captions or property display. Returning a valid (possibly empty)
//! result instead keeps that init path alive.
//!
//! These derive what they can from the graph schema. Per-entity row counts
//! require executing a query (no executor is available at the procedure layer),
//! so count fields are reported as 0; the Browser's separate
//! `count(n)`/`count(r)` probe (special-cased in the Bolt handler) supplies the
//! real totals.

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::procedures::ProcedureResult;
use std::collections::{HashMap, HashSet};

/// Base node labels (deduped, schema-key suffix stripped).
fn node_labels(schema: &GraphSchema) -> Vec<String> {
    let mut labels: Vec<String> = schema
        .all_node_schemas()
        .keys()
        .map(|k| k.rsplit("::").next().unwrap_or(k).to_string())
        .collect();
    labels.sort();
    labels.dedup();
    labels
}

/// Base relationship types (deduped, schema-key prefix taken).
fn rel_types(schema: &GraphSchema) -> Vec<String> {
    let mut types: Vec<String> = schema
        .get_relationships_schemas()
        .keys()
        .filter_map(|k| {
            k.split("::")
                .next()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
        .collect();
    types.sort();
    types.dedup();
    types
}

fn property_keys(schema: &GraphSchema) -> HashSet<String> {
    let mut keys = HashSet::new();
    for ns in schema.all_node_schemas().values() {
        for p in ns.property_mappings.keys() {
            keys.insert(p.clone());
        }
    }
    for rs in schema.get_relationships_schemas().values() {
        for p in rs.property_mappings.keys() {
            keys.insert(p.clone());
        }
    }
    keys
}

/// `db.schema.visualization()` — Neo4j Browser renders this as the schema
/// diagram. The result is a single record with `nodes` and `relationships`
/// columns, each a list of graph objects. The procedure layer can only emit
/// plain JSON (not Bolt Node/Relationship structures), so we return empty lists
/// here: a valid shape that stops the FAILURE cascade. (A richer virtual-graph
/// implementation belongs in the Bolt handler, where graph objects can be
/// packstream-encoded.)
pub fn db_schema_visualization(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![HashMap::from([
        ("nodes".to_string(), serde_json::json!([])),
        ("relationships".to_string(), serde_json::json!([])),
    ])])
}

/// `db.indexes()` — ClickGraph manages no indexes; empty (valid) result.
pub fn db_indexes(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![])
}

/// `dbms.licenseAgreementDetails()` — Browser probes license-acceptance state
/// on connect; we have no license gate, so an empty result is correct.
pub fn dbms_license_agreement_details(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![])
}

/// `apoc.meta.stats()` — Browser uses this to populate the "Database
/// Information" counts. We derive label / relationship-type / property-key
/// counts from the schema; per-entity row counts need a query and are reported
/// as 0 (the Browser's separate count probe supplies real totals).
pub fn apoc_meta_stats(schema: &GraphSchema) -> ProcedureResult {
    let labels = node_labels(schema);
    let types = rel_types(schema);
    let keys = property_keys(schema);

    let labels_map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|l| (l.clone(), serde_json::json!(0)))
        .collect();
    let rel_types_map: serde_json::Map<String, serde_json::Value> = types
        .iter()
        .map(|t| (format!("()-[:{}]->()", t), serde_json::json!(0)))
        .collect();

    let record = HashMap::from([
        ("labelCount".to_string(), serde_json::json!(labels.len())),
        ("relTypeCount".to_string(), serde_json::json!(types.len())),
        (
            "propertyKeyCount".to_string(),
            serde_json::json!(keys.len()),
        ),
        ("nodeCount".to_string(), serde_json::json!(0)),
        ("relCount".to_string(), serde_json::json!(0)),
        ("labels".to_string(), serde_json::Value::Object(labels_map)),
        (
            "relTypes".to_string(),
            serde_json::Value::Object(rel_types_map),
        ),
        ("stats".to_string(), serde_json::json!({})),
    ]);
    Ok(vec![record])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> GraphSchema {
        GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn schema_visualization_returns_valid_empty_shape() {
        let r = db_schema_visualization(&empty_schema()).expect("ok");
        assert_eq!(r.len(), 1);
        assert!(r[0].get("nodes").unwrap().is_array());
        assert!(r[0].get("relationships").unwrap().is_array());
    }

    #[test]
    fn indexes_and_license_are_empty() {
        assert!(db_indexes(&empty_schema()).expect("ok").is_empty());
        assert!(dbms_license_agreement_details(&empty_schema())
            .expect("ok")
            .is_empty());
    }

    #[test]
    fn meta_stats_has_count_fields() {
        let r = apoc_meta_stats(&empty_schema()).expect("ok");
        assert_eq!(r.len(), 1);
        for f in [
            "labelCount",
            "relTypeCount",
            "propertyKeyCount",
            "labels",
            "relTypes",
        ] {
            assert!(r[0].contains_key(f), "missing field {f}");
        }
    }
}
