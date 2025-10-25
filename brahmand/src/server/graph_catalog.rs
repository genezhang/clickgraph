use std::time::Duration;
use std::collections::HashMap;

use clickhouse::Client;
use tokio::{sync::RwLock, time::interval};

use crate::graph_catalog::{
    graph_schema::{GraphSchema, GraphSchemaElement, NodeSchema, RelationshipSchema, RelationshipIndexSchema},
    config::GraphViewConfig,
    SchemaValidator,
};

use super::{GLOBAL_GRAPH_SCHEMA, GLOBAL_VIEW_CONFIG, models::GraphCatalog};

/// Test basic ClickHouse connectivity
async fn test_clickhouse_connection(client: Client) -> Result<(), String> {
    client
        .query("SELECT 1")
        .fetch_one::<u8>()
        .await
        .map(|_| ())
        .map_err(|e| format!("ClickHouse connection test failed: {}", e))
}

pub async fn initialize_global_schema(clickhouse_client: Option<Client>, validate_schema: bool) -> Result<(), String> {
    println!("Initializing ClickGraph schema...");
    
    // Try to load from YAML configuration first (preferred approach)
    if let Ok(yaml_config_path) = std::env::var("GRAPH_CONFIG_PATH") {
        println!("Found GRAPH_CONFIG_PATH: {}", yaml_config_path);
        
        match load_schema_and_config_from_yaml(&yaml_config_path).await {
            Ok((schema, config)) => {
                println!("✓ Successfully loaded schema from YAML config: {}", yaml_config_path);
                
                // Validate schema against ClickHouse if requested
                if validate_schema {
                    if let Some(client) = clickhouse_client.as_ref() {
                        println!("  Validating schema against ClickHouse...");
                        match config.validate_schema(&mut crate::graph_catalog::SchemaValidator::new(client.clone())).await {
                            Ok(_) => println!("  ✓ Schema validation passed"),
                            Err(e) => {
                                eprintln!("  ✗ Schema validation failed: {}", e);
                                return Err(format!("Schema validation failed: {}", e));
                            }
                        }
                    } else {
                        eprintln!("  ⚠ Schema validation requested but no ClickHouse client available");
                        eprintln!("    Skipping validation - some queries may fail at runtime");
                    }
                }
                
                // Set global state - these should not fail in normal circumstances
                GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema))
                    .map_err(|_| "Failed to initialize global graph schema")?;
                GLOBAL_VIEW_CONFIG.set(RwLock::new(config))
                    .map_err(|_| "Failed to initialize global view config")?;
                
                println!("✓ Schema initialization complete (YAML mode)");
                return Ok(());
            }
            Err(e) => {
                eprintln!("✗ Failed to load YAML config {}: {}", yaml_config_path, e);
                eprintln!("  Falling back to database schema loading...");
            }
        }
    } else {
        println!("No GRAPH_CONFIG_PATH environment variable found, using database schema");
    }
    
    // Fallback to database approach (original Brahmand behavior)
    if let Some(client) = clickhouse_client.as_ref() {
        match get_graph_catalog(client.clone()).await {
            Ok(schema) => {
                println!("✓ Successfully loaded schema from database");
                
                let nodes_map = schema.get_nodes_schemas();
                let rels_map = schema.get_relationships_schemas();
                
                if nodes_map.is_empty() && rels_map.is_empty() {
                    println!("  Warning: Database schema is empty - this is normal for new installations");
                    println!("  You can add graph schema using CREATE NODE/RELATIONSHIP statements");
                } else {
                    println!("  - Loaded {} node types from database", nodes_map.len());
                    println!("  - Loaded {} relationship types from database", rels_map.len());
                }
                
                GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema))
                    .map_err(|_| "Failed to initialize global graph schema")?;
                    
                println!("✓ Schema initialization complete (database mode)");
                Ok(())
            }
            Err(e) => {
                eprintln!("✗ Failed to load schema from database: {}", e);
                
                // Try to test ClickHouse connectivity
                match test_clickhouse_connection(client.clone()).await {
                    Ok(_) => {
                        println!("✓ ClickHouse connection is working");
                        println!("  Creating empty schema for new installation...");
                        
                        // Initialize with empty but valid schema
                        let empty_schema = GraphSchema::build(
                            1,
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                        );
                        
                        GLOBAL_GRAPH_SCHEMA.set(RwLock::new(empty_schema))
                            .map_err(|_| "Failed to initialize global graph schema")?;
                            
                        println!("✓ Empty schema initialized successfully");
                        println!("  ClickGraph is ready to use. Add schema via YAML config or CREATE statements.");
                        Ok(())
                    }
                    Err(conn_err) => {
                        Err(format!(
                            "Cannot initialize ClickGraph: ClickHouse connection failed: {}. \
                            Please check your CLICKHOUSE_URL, credentials, and ensure ClickHouse is running.", 
                            conn_err
                        ))
                    }
                }
            }
        }
    } else {
        // No ClickHouse client provided at all
        println!("⚠ No ClickHouse client configuration available");
        println!("  Please provide YAML config via GRAPH_CONFIG_PATH or ClickHouse environment variables");
        
        let empty_schema = GraphSchema::build(
            1,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );
        
        GLOBAL_GRAPH_SCHEMA.set(RwLock::new(empty_schema))
            .map_err(|_| "Failed to initialize global graph schema")?;
            
        println!("✓ Minimal schema initialized - server ready for YAML configuration");
        Ok(())
    }
}

pub async fn refresh_global_schema(clickhouse_client: Client) -> Result<(), String> {
    let new_schema = get_graph_catalog(clickhouse_client).await?;
    // Acquire a write lock asynchronously.
    let global_schema_lock = GLOBAL_GRAPH_SCHEMA
        .get()
        .ok_or("Global schema not initialized")?;
    let mut schema_guard = global_schema_lock.write().await;
    *schema_guard = new_schema;
    println!("Global schema refreshed");
    Ok(())
}

pub async fn get_graph_schema() -> GraphSchema {
    let schema_guard = GLOBAL_GRAPH_SCHEMA
        .get()
        .expect("Global schema not initialized")
        .read()
        .await;
    schema_guard.clone()
}

pub async fn get_view_config() -> Option<GraphViewConfig> {
    if let Some(config_guard) = GLOBAL_VIEW_CONFIG.get() {
        let config = config_guard.read().await;
        Some((*config).clone())
    } else {
        None
    }
}

pub async fn get_graph_catalog(clickhouse_client: Client) -> Result<GraphSchema, String> {
    let graph_catalog_query = "SELECT id, schema_json FROM graph_catalog FINAL";
    let graph_catalog_result = clickhouse_client
        .query(graph_catalog_query)
        .fetch_one::<GraphCatalog>()
        .await;

    match graph_catalog_result {
        Ok(graph_catalog) => {
            let graph_schema: GraphSchema = serde_json::from_str(&graph_catalog.schema_json)
                .map_err(|e| format!("Schema parsing error: {}", e))?;

            Ok(graph_schema)
        }
        Err(err) => {
            // if it is a connection error then send error to the client from server
            // if the graph catalog table is not present then create a one.
            let err_msg = err.to_string();
            // println!("err_msg -> {:?}", err_msg);

            if err_msg.contains("UNKNOWN_TABLE") {
                println!("Creating the graph_catalog table");
                let create_graph_catalog_query = "
                CREATE TABLE graph_catalog (
                    id UInt64,
                    schema_json String 
                ) ENGINE = ReplacingMergeTree()
                ORDER BY id";

                let _ = clickhouse_client
                    .clone()
                    .with_option("wait_end_of_query", "1")
                    .query(create_graph_catalog_query)
                    .execute()
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e));

                let graph_catalog = GraphCatalog {
                    id: 1,
                    schema_json: r#"{"version": 1,"nodes": {},"relationships": {}, "relationships_indexes": {}}"#.to_string(),
                };
                let mut insert = clickhouse_client
                    .insert("graph_catalog")
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;
                insert
                    .write(&graph_catalog)
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;
                insert
                    .end()
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;

                let graph_schema: GraphSchema = serde_json::from_str(&graph_catalog.schema_json)
                    .map_err(|e| format!("Schema parsing error: {}", e))?;

                Ok(graph_schema)
            } else {
                Err(format!("Clickhouse Error: {}", err_msg))
            }
        }
    }
}

pub async fn validate_schema(graph_schema_element: &Vec<GraphSchemaElement>) -> Result<(), String> {
    for element in graph_schema_element {
        if let GraphSchemaElement::Rel(relationship_schema) = element {
            // here check if both from_node and to_node tables are present or not in the schema

            let graph_schema_lock = GLOBAL_GRAPH_SCHEMA
                .get()
                .expect("Schema not initialized")
                .read()
                .await;

            if !graph_schema_lock
                .get_nodes_schemas()
                .contains_key(&relationship_schema.from_node)
                || !graph_schema_lock
                    .get_nodes_schemas()
                    .contains_key(&relationship_schema.to_node)
            {
                return Err("From and To node tables must be present before creating a relationship between them".to_string());
            }
        }
    }

    Ok(())
}

pub async fn add_to_schema(
    clickhouse_client: Client,
    graph_schema_elements: Vec<GraphSchemaElement>,
) -> Result<(), String> {
    let mut graph_schema = GLOBAL_GRAPH_SCHEMA.get().unwrap().write().await;

    for element in graph_schema_elements {
        match element {
            GraphSchemaElement::Node(node_schema) => {
                graph_schema.insert_node_schema(node_schema.table_name.to_string(), node_schema);
                graph_schema.increment_version();
            }
            GraphSchemaElement::Rel(relationship_schema) => {
                graph_schema.insert_rel_schema(
                    relationship_schema.table_name.to_string(),
                    relationship_schema,
                );
                graph_schema.increment_version();
            }
            GraphSchemaElement::RelIndex(relationship_index_schema) => {
                graph_schema.insert_rel_index_schema(
                    relationship_index_schema.table_name.to_string(),
                    relationship_index_schema,
                );
            }
        }
    }

    let schema_json = serde_json::to_string(&*graph_schema)
        .map_err(|e| format!("Schema serialization error: {}", e))?;

    let graph_catalog = GraphCatalog { id: 1, schema_json };

    let mut insert = clickhouse_client
        .insert("graph_catalog")
        .map_err(|e| format!("Clickhouse Error: {}", e))?;
    insert
        .write(&graph_catalog)
        .await
        .map_err(|e| format!("Clickhouse Error: {}", e))?;
    insert
        .end()
        .await
        .map_err(|e| format!("Clickhouse Error: {}", e))?;

    Ok(())
}

// This function periodically checks for schema updates.
// This will be helpful in distributed environment where schema has changed.
// In distributed environment, I think Keeper Map engine makes sense.
pub async fn monitor_schema_updates(ch_client: Client) -> Result<(), String> {
    // TODO Currently checking after every min. Make it an option to set by user.
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        // get in memory data for the graph schema
        let in_mem_schema_guard = GLOBAL_GRAPH_SCHEMA
            .get()
            .expect("Global schema not initialized")
            .read()
            .await;

        let mem_version = in_mem_schema_guard.get_version();

        // Fetch the schema from ClickHouse.
        let remote_schema = match get_graph_catalog(ch_client.clone()).await {
            Ok(schema) => schema,
            Err(err) => {
                eprintln!("Error fetching remote schema: {}", err);
                continue;
            }
        };

        // Compare versions. If they differ, update the global schema.
        if remote_schema.get_version() != mem_version {
            let mut schema_guard = GLOBAL_GRAPH_SCHEMA
                .get()
                .expect("Global schema not initialized")
                .write()
                .await;
            *schema_guard = remote_schema.clone();

            println!(
                "Global schema updated from version {} to {}",
                mem_version,
                remote_schema.get_version()
            );
        }
    }
}

// Load schema from YAML configuration file
async fn load_schema_and_config_from_yaml(config_path: &str) -> Result<(GraphSchema, GraphViewConfig), String> {
    use std::collections::HashMap;
    use crate::graph_catalog::graph_schema::{NodeIdSchema};
    
    let config = GraphViewConfig::from_yaml_file(config_path)
        .map_err(|e| format!("Failed to load YAML config: {}", e))?;
    
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();
    let relationships_indexes = HashMap::new();

    // Convert view config to graph schema
    for view in &config.views {
        for (_key, node_mapping) in &view.nodes {
            let mut column_names = Vec::new();
            column_names.push(node_mapping.id_column.clone());
            for column in node_mapping.property_mappings.values() {
                column_names.push(column.clone());
            }
            
            let node_schema = NodeSchema {
                table_name: node_mapping.source_table.clone(),
                column_names,
                primary_keys: node_mapping.id_column.clone(),
                node_id: NodeIdSchema {
                    column: node_mapping.id_column.clone(),
                    dtype: "UInt32".to_string(), // Default type, could be made configurable
                },
                property_mappings: node_mapping.property_mappings.clone(),
            };
            nodes.insert(node_mapping.label.clone(), node_schema);
        }
        
        for (_rel_key, rel_mapping) in &view.relationships {
            let mut column_names = Vec::new();
            column_names.push(rel_mapping.from_column.clone());
            column_names.push(rel_mapping.to_column.clone());
            for column in rel_mapping.property_mappings.values() {
                column_names.push(column.clone());
            }
            
            // Use from_node_type and to_node_type from the mapping if provided, otherwise use generic fallback
            let from_node = rel_mapping.from_node_type.as_ref()
                .map(|s| s.as_str())
                .unwrap_or("Node");
            let to_node = rel_mapping.to_node_type.as_ref()
                .map(|s| s.as_str())
                .unwrap_or("Node");
            
            let rel_schema = RelationshipSchema {
                table_name: rel_mapping.source_table.clone(),
                column_names,
                from_node: from_node.to_string(),
                to_node: to_node.to_string(),
                from_column: rel_mapping.from_column.clone(),
                to_column: rel_mapping.to_column.clone(),
                from_node_id_dtype: "UInt32".to_string(),
                to_node_id_dtype: "UInt32".to_string(),
                property_mappings: rel_mapping.property_mappings.clone(),
            };
            
            // Use the type_name from the mapping as the schema key (this is what Cypher queries use)
            let schema_key = rel_mapping.type_name.clone();
            relationships.insert(schema_key, rel_schema);
        }
    }
    
    Ok((GraphSchema::build(1, nodes, relationships, relationships_indexes), config))
}
