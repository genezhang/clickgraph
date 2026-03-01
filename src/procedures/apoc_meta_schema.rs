//! apoc.meta.schema() procedure
//!
//! Returns schema metadata in the APOC format used by Neo4j MCP servers
//! for schema introspection. This enables zero-config compatibility with
//! the official Neo4j Go MCP server and the Labs Python MCP server.
//!
//! Two execution modes:
//! - `execute()` — returns a single record with `{"value": <schema_map>}` for simple CALL
//! - `execute_unwound()` — returns pre-unwound records for the MCP query pattern that
//!   uses UNWIND + map indexing + map projection

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;

/// Execute apoc.meta.schema() — returns single record with full schema map.
///
/// Output: `[{"value": { "User": {...}, "FOLLOWS": {...}, ... }}]`
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let schema_map = build_schema_map(schema);

    let mut record = HashMap::new();
    record.insert("value".to_string(), schema_map);

    Ok(vec![record])
}

/// Execute apoc.meta.schema() in unwound format for MCP server compatibility.
///
/// The MCP Go server sends:
/// ```cypher
/// CALL apoc.meta.schema({sample: $sampleSize}) YIELD value
/// UNWIND keys(value) AS key
/// WITH key, value[key] AS value
/// RETURN key, value { .properties, .type, .relationships } AS value
/// ```
///
/// Since the procedure executor cannot handle UNWIND + map indexing + map projection,
/// this function returns the already-unwound result: one record per schema entry,
/// each with `key` and `value` (containing only .properties, .type, .relationships).
pub fn execute_unwound(
    schema: &GraphSchema,
) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let schema_map = build_schema_map(schema);

    let mut records = Vec::new();

    if let serde_json::Value::Object(map) = schema_map {
        // Sort keys for deterministic output
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();

        for key in keys {
            let entry = &map[key];
            // Apply map projection: keep only .properties, .type, .relationships
            let projected = serde_json::json!({
                "properties": entry.get("properties").cloned().unwrap_or(serde_json::json!({})),
                "type": entry.get("type").cloned().unwrap_or(serde_json::json!("node")),
                "relationships": entry.get("relationships").cloned().unwrap_or(serde_json::json!({})),
            });

            let mut record = HashMap::new();
            record.insert("key".to_string(), serde_json::json!(key));
            record.insert("value".to_string(), projected);
            records.push(record);
        }
    }

    Ok(records)
}

/// Build the full APOC-format schema map from a GraphSchema.
///
/// Returns a JSON object with entries for each node label and relationship type:
/// ```json
/// {
///   "User": {
///     "type": "node",
///     "count": -1,
///     "properties": { "name": {"type": "STRING", ...}, ... },
///     "relationships": { "FOLLOWS": {"direction": "out", ...}, ... }
///   },
///   "FOLLOWS": {
///     "type": "relationship",
///     "count": -1,
///     "properties": { "since": {"type": "STRING", ...}, ... }
///   }
/// }
/// ```
fn build_schema_map(schema: &GraphSchema) -> serde_json::Value {
    let mut schema_map = serde_json::Map::new();

    // Build node entries
    for (label, node_schema) in schema.all_node_schemas() {
        let properties = build_property_metadata(&node_schema.property_mappings);

        // Build relationships section for this node
        let relationships = build_node_relationships(schema, label);

        schema_map.insert(
            label.clone(),
            serde_json::json!({
                "type": "node",
                "count": -1,
                "properties": properties,
                "relationships": relationships,
            }),
        );
    }

    // Build relationship entries using rel_type_index for unique base type names.
    // The relationships BTreeMap may only contain composite keys (e.g., "FOLLOWS::User::User"),
    // so get_unique_relationship_types() can return empty. The rel_type_index always maps
    // base type names to their composite keys.
    for (rel_type, composite_keys) in schema.get_rel_type_index() {
        // Get properties from the first matching relationship schema
        let properties = composite_keys
            .first()
            .and_then(|key| schema.get_relationships_schemas().get(key))
            .map(|rs| build_property_metadata(&rs.property_mappings))
            .unwrap_or_else(|| serde_json::json!({}));

        schema_map.insert(
            rel_type.clone(),
            serde_json::json!({
                "type": "relationship",
                "count": -1,
                "properties": properties,
            }),
        );
    }

    serde_json::Value::Object(schema_map)
}

/// Build property metadata in APOC format.
///
/// Each property becomes: `{"type": "STRING", "indexed": false, "unique": false, "existence": false}`
fn build_property_metadata(
    property_mappings: &HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>,
) -> serde_json::Value {
    let mut props = serde_json::Map::new();
    for prop_name in property_mappings.keys() {
        props.insert(
            prop_name.clone(),
            serde_json::json!({
                "type": "STRING",
                "indexed": false,
                "unique": false,
                "existence": false,
            }),
        );
    }
    serde_json::Value::Object(props)
}

/// Build the relationships section for a node label.
///
/// Scans all relationship schemas to find edges originating from or targeting this node.
fn build_node_relationships(schema: &GraphSchema, node_label: &str) -> serde_json::Value {
    let mut rels = serde_json::Map::new();

    for (key, rel_schema) in schema.get_relationships_schemas() {
        // Skip composite keys — only process simple type keys or composite keys
        // We need the from_node/to_node info which is on the schema
        let rel_type = if key.contains("::") {
            // Composite key like "FOLLOWS::User::User" — extract base type
            key.split("::").next().unwrap_or(key).to_string()
        } else {
            key.clone()
        };

        if rel_schema.from_node == node_label {
            // Outgoing relationship
            let entry = rels
                .entry(rel_type.clone())
                .or_insert_with(|| {
                    serde_json::json!({
                        "direction": "out",
                        "count": -1,
                        "labels": [],
                        "properties": {},
                    })
                })
                .as_object_mut()
                .unwrap();

            // Add target label to labels array if not already present
            if let Some(labels) = entry.get_mut("labels").and_then(|v| v.as_array_mut()) {
                let target = serde_json::Value::String(rel_schema.to_node.clone());
                if !labels.contains(&target) {
                    labels.push(target);
                }
            }

            // Set properties from the relationship schema
            entry.insert(
                "properties".to_string(),
                build_property_metadata(&rel_schema.property_mappings),
            );
        }

        if rel_schema.to_node == node_label {
            // Incoming relationship — use a separate key to avoid overwriting outgoing
            let in_key = format!("{}_in", rel_type);

            // Only add if we haven't already added an outgoing for the same type to this node
            // (self-referential relationships like User-FOLLOWS->User have both)
            if !rels.contains_key(&in_key) {
                rels.insert(
                    in_key,
                    serde_json::json!({
                        "direction": "in",
                        "count": -1,
                        "labels": [&rel_schema.from_node],
                        "properties": {},
                    }),
                );
            }
        }
    }

    serde_json::Value::Object(rels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::graph_catalog::graph_schema::{
        GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema,
    };
    use crate::graph_catalog::schema_types::SchemaType;

    fn create_empty_schema() -> GraphSchema {
        GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new())
    }

    fn make_node(table: &str, id_col: &str, props: HashMap<String, PropertyValue>) -> NodeSchema {
        NodeSchema {
            database: "test".to_string(),
            table_name: table.to_string(),
            column_names: vec![id_col.to_string()],
            primary_keys: id_col.to_string(),
            node_id: NodeIdSchema::single(id_col.to_string(), SchemaType::Integer),
            property_mappings: props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        }
    }

    fn make_rel(
        table: &str,
        from_node: &str,
        to_node: &str,
        from_id_col: &str,
        to_id_col: &str,
        props: HashMap<String, PropertyValue>,
    ) -> RelationshipSchema {
        RelationshipSchema {
            database: "test".to_string(),
            table_name: table.to_string(),
            column_names: vec![from_id_col.to_string(), to_id_col.to_string()],
            from_node: from_node.to_string(),
            to_node: to_node.to_string(),
            from_node_table: format!("{}s", from_node.to_lowercase()),
            to_node_table: format!("{}s", to_node.to_lowercase()),
            from_id: Identifier::Single(from_id_col.to_string()),
            to_id: Identifier::Single(to_id_col.to_string()),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        }
    }

    fn create_schema_with_nodes_and_rels() -> GraphSchema {
        let mut nodes = HashMap::new();
        let mut user_props = HashMap::new();
        user_props.insert(
            "name".to_string(),
            PropertyValue::Column("full_name".to_string()),
        );
        user_props.insert("age".to_string(), PropertyValue::Column("age".to_string()));
        nodes.insert("User".to_string(), make_node("users", "id", user_props));

        let mut post_props = HashMap::new();
        post_props.insert(
            "title".to_string(),
            PropertyValue::Column("title".to_string()),
        );
        nodes.insert("Post".to_string(), make_node("posts", "id", post_props));

        let mut rels = HashMap::new();
        let mut follows_props = HashMap::new();
        follows_props.insert(
            "since".to_string(),
            PropertyValue::Column("since".to_string()),
        );
        rels.insert(
            "FOLLOWS".to_string(),
            make_rel("follows", "User", "User", "from_id", "to_id", follows_props),
        );

        rels.insert(
            "LIKES".to_string(),
            make_rel(
                "likes",
                "User",
                "Post",
                "user_id",
                "post_id",
                HashMap::new(),
            ),
        );

        GraphSchema::build(1, "test".to_string(), nodes, rels)
    }

    #[test]
    fn test_execute_empty_schema() {
        let schema = create_empty_schema();
        let results = execute(&schema).unwrap();
        assert_eq!(results.len(), 1);
        let value = &results[0]["value"];
        assert!(value.is_object());
        assert_eq!(value.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_execute_with_nodes_and_relationships() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute(&schema).unwrap();
        assert_eq!(results.len(), 1);

        let value = &results[0]["value"];
        let map = value.as_object().unwrap();

        // Should have node entries and relationship entries
        assert!(map.contains_key("User"));
        assert!(map.contains_key("Post"));
        assert!(map.contains_key("FOLLOWS"));
        assert!(map.contains_key("LIKES"));

        // Check node type
        assert_eq!(map["User"]["type"], "node");
        assert_eq!(map["FOLLOWS"]["type"], "relationship");
    }

    #[test]
    fn test_node_properties_format() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute(&schema).unwrap();
        let value = &results[0]["value"];
        let user = &value["User"];

        // Check properties structure
        let props = user["properties"].as_object().unwrap();
        assert!(props.contains_key("name"));
        assert!(props.contains_key("age"));

        // Check individual property metadata
        let name_prop = &props["name"];
        assert_eq!(name_prop["type"], "STRING");
        assert_eq!(name_prop["indexed"], false);
        assert_eq!(name_prop["unique"], false);
        assert_eq!(name_prop["existence"], false);
    }

    #[test]
    fn test_relationship_direction() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute(&schema).unwrap();
        let value = &results[0]["value"];

        // User should have outgoing FOLLOWS and LIKES relationships
        let user_rels = value["User"]["relationships"].as_object().unwrap();
        assert!(user_rels.contains_key("FOLLOWS"));
        assert_eq!(user_rels["FOLLOWS"]["direction"], "out");

        // FOLLOWS targets User
        let follows_labels = user_rels["FOLLOWS"]["labels"].as_array().unwrap();
        assert!(follows_labels.contains(&serde_json::json!("User")));

        // LIKES targets Post
        assert!(user_rels.contains_key("LIKES"));
        assert_eq!(user_rels["LIKES"]["direction"], "out");
        let likes_labels = user_rels["LIKES"]["labels"].as_array().unwrap();
        assert!(likes_labels.contains(&serde_json::json!("Post")));
    }

    #[test]
    fn test_relationship_properties() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute(&schema).unwrap();
        let value = &results[0]["value"];

        // FOLLOWS relationship should have "since" property
        let follows = &value["FOLLOWS"];
        let props = follows["properties"].as_object().unwrap();
        assert!(props.contains_key("since"));
    }

    #[test]
    fn test_execute_unwound_format() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute_unwound(&schema).unwrap();

        // Should have one record per schema entry (nodes + relationship types)
        assert!(results.len() >= 4); // User, Post, FOLLOWS, LIKES

        // Each record should have "key" and "value"
        for record in &results {
            assert!(record.contains_key("key"));
            assert!(record.contains_key("value"));

            let value = &record["value"];
            // Projected value should only have properties, type, relationships
            let obj = value.as_object().unwrap();
            assert!(obj.contains_key("properties"));
            assert!(obj.contains_key("type"));
            assert!(obj.contains_key("relationships"));
        }

        // Check that keys are sorted
        let keys: Vec<&str> = results.iter().map(|r| r["key"].as_str().unwrap()).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
    }

    #[test]
    fn test_unwound_projected_value() {
        let schema = create_schema_with_nodes_and_rels();
        let results = execute_unwound(&schema).unwrap();

        // Find the User record
        let user_record = results
            .iter()
            .find(|r| r["key"].as_str() == Some("User"))
            .expect("User record should exist");

        let value = &user_record["value"];
        assert_eq!(value["type"], "node");
        assert!(value["properties"]
            .as_object()
            .unwrap()
            .contains_key("name"));
        assert!(value["relationships"].is_object());

        // Should NOT contain "count" — that's excluded by map projection
        assert!(!value.as_object().unwrap().contains_key("count"));
    }
}
