//! Graph output transformation for HTTP API
//!
//! Transforms ClickHouse query result rows into structured graph objects (nodes and edges)
//! by reusing the Bolt protocol's result_transformer logic.

use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    server::{
        bolt_protocol::result_transformer::{
            transform_to_node, transform_to_relationship, ReturnItemMetadata, ReturnItemType,
        },
        models::{GraphEdge, GraphNode},
    },
};

/// Transform flat JSON result rows into deduplicated graph nodes and edges.
///
/// The result shape (`ReadTranslation::return_metadata`) says which return
/// items are nodes vs relationships; the Bolt transform functions build the
/// graph objects. Scalars and paths are skipped (paths could be added in the
/// future).
pub fn transform_to_graph(
    rows: &[Value],
    metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut seen_nodes = HashSet::new();
    let mut seen_edges = HashSet::new();

    for row_value in rows {
        let row_map: HashMap<String, Value> = match row_value {
            Value::Object(obj) => obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            _ => continue,
        };

        for meta in metadata {
            match &meta.item_type {
                ReturnItemType::Node { labels } => {
                    match transform_to_node(
                        &meta.entity_row(&row_map),
                        &meta.field_name,
                        labels,
                        schema,
                    ) {
                        Ok(node) => {
                            let gn = node.to_graph_node();
                            if seen_nodes.insert(gn.element_id.clone()) {
                                nodes.push(gn);
                            }
                        }
                        Err(e) => {
                            log::debug!(
                                "Skipping node '{}' in graph output: {}",
                                meta.field_name,
                                e
                            );
                        }
                    }
                }
                ReturnItemType::Relationship {
                    rel_types,
                    from_label,
                    to_label,
                    ..
                } => {
                    match transform_to_relationship(
                        &meta.entity_row(&row_map),
                        &meta.field_name,
                        rel_types,
                        from_label.as_deref(),
                        to_label.as_deref(),
                        schema,
                    ) {
                        Ok(rel) => {
                            let ge = rel.to_graph_edge();
                            if seen_edges.insert(ge.element_id.clone()) {
                                edges.push(ge);
                            }
                        }
                        Err(e) => {
                            log::debug!(
                                "Skipping relationship '{}' in graph output: {}",
                                meta.field_name,
                                e
                            );
                        }
                    }
                }
                // Scalars, paths, id functions — skip for graph output
                _ => {}
            }
        }
    }

    (nodes, edges)
}
