//! Graph output types for embedded mode.
//!
//! Provides `GraphNode`, `GraphEdge`, and `GraphResult` for structured graph
//! output from `Connection::query_graph()`. The transformation logic mirrors
//! the server's `transform_to_graph()` but uses the embedded `Value` type.

use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use clickgraph::graph_catalog::element_id::{
    generate_node_element_id, generate_relationship_element_id,
};
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph::query_planner::logical_plan::LogicalPlan;
use clickgraph::query_planner::plan_ctx::PlanCtx;
use clickgraph::server::bolt_protocol::result_transformer::{
    extract_return_metadata, ReturnItemType,
};

use super::value::Value;

/// Statistics returned by `Connection::store_subgraph()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStats {
    /// Number of nodes written to local tables.
    pub nodes_stored: usize,
    /// Number of edges written to local tables.
    pub edges_stored: usize,
}

/// Parse an element ID string like `"Label:raw_id"` into `("Label", "raw_id")`.
///
/// Returns `None` if the string does not contain a `:` separator.
pub fn parse_element_id(element_id: &str) -> Option<(&str, &str)> {
    element_id.split_once(':')
}

/// A graph node with an element ID, labels, and properties.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphNode {
    /// Unique element identifier (e.g., `"User:42"`).
    pub id: String,
    /// Node labels (e.g., `["Person"]`).
    pub labels: Vec<String>,
    /// Property key-value pairs using the embedded `Value` type.
    pub properties: HashMap<String, Value>,
}

/// A graph edge (relationship) with endpoints and properties.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphEdge {
    /// Unique element identifier for this edge.
    pub id: String,
    /// Relationship type (e.g., `"FOLLOWS"`).
    pub type_name: String,
    /// Element ID of the source node.
    pub from_id: String,
    /// Element ID of the target node.
    pub to_id: String,
    /// Property key-value pairs using the embedded `Value` type.
    pub properties: HashMap<String, Value>,
}

/// Structured graph result containing deduplicated nodes and edges.
#[derive(Debug, Clone)]
pub struct GraphResult {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

impl GraphResult {
    /// Return the list of nodes.
    pub fn nodes(&self) -> &[GraphNode] {
        &self.nodes
    }

    /// Return the list of edges.
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }

    /// Return the number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Return the number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Create an empty `GraphResult` with no nodes or edges.
    pub fn empty() -> Self {
        GraphResult {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

/// Builder for constructing a `GraphResult` with deduplication.
pub struct GraphResultBuilder {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
    seen_nodes: HashSet<String>,
    seen_edges: HashSet<String>,
}

impl GraphResultBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            seen_nodes: HashSet::new(),
            seen_edges: HashSet::new(),
        }
    }

    /// Add a node, skipping if its element ID has already been seen.
    pub fn add_node(&mut self, node: GraphNode) {
        if self.seen_nodes.insert(node.id.clone()) {
            self.nodes.push(node);
        }
    }

    /// Add an edge, skipping if its element ID has already been seen.
    pub fn add_edge(&mut self, edge: GraphEdge) {
        if self.seen_edges.insert(edge.id.clone()) {
            self.edges.push(edge);
        }
    }

    /// Consume the builder and produce the final `GraphResult`.
    pub fn build(self) -> GraphResult {
        GraphResult {
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

/// Transform flat JSON result rows into a structured `GraphResult`.
///
/// Uses `extract_return_metadata` from the core crate to classify return items
/// as Node, Relationship, or Scalar, then extracts properties from each row.
pub fn transform_rows_to_graph(
    rows: &[JsonValue],
    logical_plan: &LogicalPlan,
    plan_ctx: &PlanCtx,
    schema: &GraphSchema,
) -> Result<GraphResult, String> {
    let metadata = extract_return_metadata(logical_plan, plan_ctx)
        .map_err(|e| format!("Failed to extract return metadata: {}", e))?;

    let mut builder = GraphResultBuilder::new();

    for row_value in rows {
        let row_map: HashMap<String, JsonValue> = match row_value {
            JsonValue::Object(obj) => obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            _ => continue,
        };

        for meta in &metadata {
            match &meta.item_type {
                ReturnItemType::Node { labels } => {
                    if let Some(node) =
                        extract_node_from_row(&row_map, &meta.field_name, labels, schema)
                    {
                        builder.add_node(node);
                    }
                }
                ReturnItemType::Relationship {
                    rel_types,
                    from_label,
                    to_label,
                    ..
                } => {
                    if let Some(edge) = extract_edge_from_row(
                        &row_map,
                        &meta.field_name,
                        rel_types,
                        from_label.as_deref(),
                        to_label.as_deref(),
                        schema,
                    ) {
                        builder.add_edge(edge);
                    }
                }
                // Scalars, paths, id functions -- skip for graph output
                _ => {}
            }
        }
    }

    Ok(builder.build())
}

/// Extract a `GraphNode` from a result row.
///
/// Looks for columns prefixed with `{var_name}.` to build the property map.
/// Determines the label from `__label__` columns or metadata.
/// Generates an element ID from the schema's node ID columns.
fn extract_node_from_row(
    row: &HashMap<String, JsonValue>,
    var_name: &str,
    labels: &[String],
    schema: &GraphSchema,
) -> Option<GraphNode> {
    let prefix = format!("{}.", var_name);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            // Skip internal metadata columns
            if prop_name == "__label__"
                || prop_name == "_label__"
                || prop_name == "id"
                || prop_name == "properties"
            {
                continue;
            }
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    // Handle VLP CTE JSON properties blob
    let vlp_props_col = format!("{}.properties", var_name);
    if let Some(JsonValue::String(json_str)) = row.get(&vlp_props_col) {
        if let Ok(parsed) = serde_json::from_str::<HashMap<String, JsonValue>>(json_str) {
            for (k, v) in parsed {
                let clean_key = if k.contains('.') {
                    k.split('.').next_back().unwrap_or(&k).to_string()
                } else {
                    k
                };
                properties.entry(clean_key).or_insert(v);
            }
        }
    }

    // Resolve label: per-row __label__ > metadata labels > infer from schema
    let label = resolve_node_label(row, var_name, labels, &properties, schema)?;

    let node_schema = schema.node_schema_opt(&label)?;
    let id_columns = node_schema.node_id.id.columns();

    let id_values: Vec<String> = id_columns
        .iter()
        .filter_map(|col_name| properties.get(*col_name).and_then(json_value_to_string))
        .collect();

    if id_values.len() != id_columns.len() {
        return None;
    }

    let id_value_refs: Vec<&str> = id_values.iter().map(|s| s.as_str()).collect();
    let element_id = generate_node_element_id(&label, &id_value_refs);

    // Convert JSON properties to embedded Value properties
    let embedded_properties: HashMap<String, Value> = properties
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect();

    Some(GraphNode {
        id: element_id,
        labels: vec![label],
        properties: embedded_properties,
    })
}

/// Resolve the label for a node from row data and metadata.
fn resolve_node_label(
    row: &HashMap<String, JsonValue>,
    var_name: &str,
    labels: &[String],
    properties: &HashMap<String, JsonValue>,
    schema: &GraphSchema,
) -> Option<String> {
    // Priority 1: per-row {alias}.__label__
    if let Some(label_value) = row.get(&format!("{}.__label__", var_name)) {
        if let Some(label_str) = json_value_to_string(label_value) {
            if label_str != "Unknown" {
                return Some(label_str);
            }
        }
    }

    // Priority 2: metadata labels
    if let Some(l) = labels.first() {
        return Some(l.clone());
    }

    // Priority 3: global __label__
    if let Some(label_value) = row.get("__label__") {
        if let Some(label_str) = json_value_to_string(label_value) {
            if label_str != "Unknown" {
                return Some(label_str);
            }
        }
    }

    // Priority 4: infer from schema by matching ID columns in properties
    infer_node_label(properties, schema)
}

/// Infer a node label by matching schema ID columns against available properties.
fn infer_node_label(
    properties: &HashMap<String, JsonValue>,
    schema: &GraphSchema,
) -> Option<String> {
    for (label, node_schema) in schema.all_node_schemas() {
        let id_columns = node_schema.node_id.id.columns();
        let has_id = id_columns
            .iter()
            .any(|col| properties.get(*col).is_some_and(|v| !v.is_null()));
        if has_id {
            return Some(label.clone());
        }
    }
    None
}

/// Extract a `GraphEdge` from a result row.
fn extract_edge_from_row(
    row: &HashMap<String, JsonValue>,
    var_name: &str,
    rel_types: &[String],
    from_label: Option<&str>,
    to_label: Option<&str>,
    schema: &GraphSchema,
) -> Option<GraphEdge> {
    let prefix = format!("{}.", var_name);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    // Get the relationship type
    let rel_type = rel_types.first()?.clone();

    // Find a matching relationship schema from the available schemas for this type.
    // Prefer schemas that match from/to labels if provided.
    let rel_schemas = schema.rel_schemas_for_type(&rel_type);
    let rel_schema = if rel_schemas.is_empty() {
        return None;
    } else if let (Some(fl), Some(tl)) = (from_label, to_label) {
        rel_schemas
            .iter()
            .find(|rs| rs.from_node == fl && rs.to_node == tl)
            .or_else(|| rel_schemas.first())
            .copied()?
    } else {
        rel_schemas.first().copied()?
    };

    let from_id_cols = rel_schema.from_id.columns();
    let to_id_cols = rel_schema.to_id.columns();

    // Extract from/to ID values from properties.
    // The SQL generator may normalize FK columns to `from_id`/`to_id` aliases,
    // so we also try those as fallbacks if the schema column names don't match.
    let from_id = from_id_cols
        .iter()
        .filter_map(|col| properties.get(*col).and_then(json_value_to_string))
        .next()
        .or_else(|| properties.get("from_id").and_then(json_value_to_string))?;

    let to_id = to_id_cols
        .iter()
        .filter_map(|col| properties.get(*col).and_then(json_value_to_string))
        .next()
        .or_else(|| properties.get("to_id").and_then(json_value_to_string))?;

    // Determine from/to labels for element IDs
    let from_label_str = from_label
        .map(|s| s.to_string())
        .unwrap_or_else(|| rel_schema.from_node.clone());
    let to_label_str = to_label
        .map(|s| s.to_string())
        .unwrap_or_else(|| rel_schema.to_node.clone());

    let from_element_id = generate_node_element_id(&from_label_str, &[&from_id]);
    let to_element_id = generate_node_element_id(&to_label_str, &[&to_id]);

    let element_id = generate_relationship_element_id(&rel_type, &from_id, &to_id);

    // Remove from/to ID columns from edge properties (they are structural, not properties)
    for col in from_id_cols.iter().chain(to_id_cols.iter()) {
        properties.remove(*col);
    }
    // Also remove the normalized aliases if present.
    // Note: this assumes `from_id`/`to_id` are structural FK aliases, not user
    // properties. Graph data rarely has properties with these exact names.
    properties.remove("from_id");
    properties.remove("to_id");

    let embedded_properties: HashMap<String, Value> = properties
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect();

    Some(GraphEdge {
        id: element_id,
        type_name: rel_type,
        from_id: from_element_id,
        to_id: to_element_id,
        properties: embedded_properties,
    })
}

/// Convert a `serde_json::Value` to a `String` for ID extraction.
fn json_value_to_string(v: &JsonValue) -> Option<String> {
    match v {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_node_construction() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int64(30));

        let node = GraphNode {
            id: "Person:42".to_string(),
            labels: vec!["Person".to_string()],
            properties: props,
        };

        assert_eq!(node.id, "Person:42");
        assert_eq!(node.labels, vec!["Person"]);
        assert_eq!(
            node.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(node.properties.get("age"), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_graph_edge_construction() {
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int64(2020));

        let edge = GraphEdge {
            id: "FOLLOWS:1:2".to_string(),
            type_name: "FOLLOWS".to_string(),
            from_id: "User:1".to_string(),
            to_id: "User:2".to_string(),
            properties: props,
        };

        assert_eq!(edge.id, "FOLLOWS:1:2");
        assert_eq!(edge.type_name, "FOLLOWS");
        assert_eq!(edge.from_id, "User:1");
        assert_eq!(edge.to_id, "User:2");
        assert_eq!(edge.properties.get("since"), Some(&Value::Int64(2020)));
    }

    #[test]
    fn test_graph_result_deduplication() {
        let mut builder = GraphResultBuilder::new();

        let node1 = GraphNode {
            id: "Person:1".to_string(),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        let node1_dup = node1.clone();
        let node2 = GraphNode {
            id: "Person:2".to_string(),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };

        builder.add_node(node1);
        builder.add_node(node1_dup);
        builder.add_node(node2);

        let edge1 = GraphEdge {
            id: "KNOWS:1:2".to_string(),
            type_name: "KNOWS".to_string(),
            from_id: "Person:1".to_string(),
            to_id: "Person:2".to_string(),
            properties: HashMap::new(),
        };
        let edge1_dup = edge1.clone();

        builder.add_edge(edge1);
        builder.add_edge(edge1_dup);

        let result = builder.build();
        assert_eq!(result.node_count(), 2, "duplicate node should be skipped");
        assert_eq!(result.edge_count(), 1, "duplicate edge should be skipped");
    }

    #[test]
    fn test_graph_result_empty() {
        let builder = GraphResultBuilder::new();
        let result = builder.build();
        assert_eq!(result.node_count(), 0);
        assert_eq!(result.edge_count(), 0);
        assert!(result.nodes().is_empty());
        assert!(result.edges().is_empty());
    }

    #[test]
    fn test_parse_element_id_node() {
        let (label, id) = parse_element_id("User:42").unwrap();
        assert_eq!(label, "User");
        assert_eq!(id, "42");
    }

    #[test]
    fn test_parse_element_id_string_id() {
        let (label, id) = parse_element_id("Person:abc-def").unwrap();
        assert_eq!(label, "Person");
        assert_eq!(id, "abc-def");
    }

    #[test]
    fn test_parse_element_id_no_colon() {
        assert!(parse_element_id("nocolon").is_none());
    }

    #[test]
    fn test_parse_element_id_empty() {
        assert!(parse_element_id("").is_none());
    }

    #[test]
    fn test_store_stats_default() {
        let stats = StoreStats {
            nodes_stored: 5,
            edges_stored: 10,
        };
        assert_eq!(stats.nodes_stored, 5);
        assert_eq!(stats.edges_stored, 10);
    }
}
