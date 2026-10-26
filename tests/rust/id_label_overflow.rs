//! ELEMENT_IDENTITY.md I4b-a: the numbers of nodes of more than 63 labels.
//!
//! Its own test target (its own process): the label codes are process-wide,
//! and 70 labels would push the labels of every other test past code 63.

use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::server::bolt_protocol::id_mapper::{resolve_node_id, IdMapper};

fn schema_of(labels: &[String]) -> clickgraph::graph_catalog::graph_schema::GraphSchema {
    let nodes: String = labels
        .iter()
        .map(|l| {
            format!(
                "    - {{label: {l}, database: d, table: t_{l}, node_id: id, \
                 property_mappings: {{id: id}}}}\n"
            )
        })
        .collect();
    GraphSchemaConfig::from_yaml_str(&format!(
        "graph_schema:\n  name: overflow\n  nodes:\n{nodes}"
    ))
    .expect("parse schema")
    .to_graph_schema()
    .expect("schema")
}

#[test]
fn labels_past_the_63rd_have_numbers_of_their_own() {
    let labels: Vec<String> = (0..70).map(|i| format!("Ov{i:02}")).collect();
    let schema = schema_of(&labels);
    // The same key (1) in every label: 70 numbers.
    let element_ids: Vec<String> = labels.iter().map(|l| format!("{l}:1-")).collect();
    let numbers: Vec<i64> = element_ids
        .iter()
        .map(|e| IdMapper::compute_deterministic_id(e))
        .collect();
    let distinct: std::collections::BTreeSet<i64> = numbers.iter().copied().collect();
    assert_eq!(distinct.len(), 70, "{numbers:?}");
    // The first 63 labels (codes 1 to 63) hold their keys' bits; the rest
    // (codes past 63) are kept in the session.
    let bits: Vec<bool> = element_ids
        .iter()
        .map(|e| IdMapper::holds_key_bits(&schema, e))
        .collect();
    assert!(bits[..63].iter().all(|b| *b), "{bits:?}");
    assert!(bits[63..].iter().all(|b| !*b), "{bits:?}");
    // The 63rd label's number is its code over the key, as before.
    assert_eq!(numbers[62], (63_i64 << 47) | 1);
    // ... and each reads back to its own node: the bits' from the number
    // alone (a fresh mapper), the others from the session that gave them.
    let fresh = IdMapper::new();
    for (i, (label, element_id)) in labels.iter().zip(&element_ids).enumerate().take(63) {
        assert_eq!(
            resolve_node_id(&fresh, Some(&schema), numbers[i]),
            Some((label.clone(), "1".to_string())),
            "{label}"
        );
        let _ = element_id;
    }
    let mut ids = IdMapper::new();
    let mut kept = Vec::new();
    for (label, element_id) in labels.iter().zip(&element_ids).skip(63) {
        let n = ids.get_or_assign(element_id);
        assert_eq!(
            resolve_node_id(&ids, Some(&schema), n),
            Some((label.clone(), "1".to_string())),
            "{label}"
        );
        kept.push((label.clone(), n));
    }
    // Not from the number alone: once the session that gave them is gone.
    drop(ids);
    for (label, n) in kept {
        assert_ne!(
            resolve_node_id(&fresh, Some(&schema), n),
            Some((label.clone(), "1".to_string())),
            "{label}"
        );
    }
}
