use std::time::Duration;
use std::collections::HashMap;

use clickhouse::Client;
use tokio::{sync::RwLock, time::interval};

use crate::graph_catalog::{
    graph_schema::{GraphSchema, GraphSchemaElement},
    config::{GraphSchemaConfig, GraphSchemaDefinition},
};

/// Indicates the source from which the schema was loaded
#[derive(Debug, Clone)]
pub enum SchemaSource {
    Yaml,
    Database,
}

use super::{GLOBAL_GRAPH_SCHEMA, GLOBAL_SCHEMA_CONFIG, GLOBAL_SCHEMAS, GLOBAL_SCHEMA_CONFIGS, models::GraphCatalog};

/// Test basic ClickHouse connectivity
async fn test_clickhouse_connection(client: Client) -> Result<(), String> {
    client
        .query("SELECT 1")
        .fetch_one::<u8>()
        .await
        .map(|_| ())
        .map_err(|e| format!("ClickHouse connection test failed: {}", e))
}

/// Load schema and config from YAML file
async fn load_schema_and_config_from_yaml(config_path: &str) -> Result<(GraphSchema, GraphSchemaConfig), String> {
    let config = GraphSchemaConfig::from_yaml_file(config_path)
        .map_err(|e| format!("Failed to load YAML config: {}", e))?;
    
    let schema = config.to_graph_schema()
        .map_err(|e| format!("Failed to create schema from config: {}", e))?;
    Ok((schema, config))
}

pub async fn initialize_global_schema(clickhouse_client: Option<Client>, validate_schema: bool) -> Result<SchemaSource, String> {
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
                GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema.clone()))
                    .map_err(|_| "Failed to initialize global graph schema")?;
                GLOBAL_SCHEMA_CONFIG.set(RwLock::new(config.clone()))
                    .map_err(|_| "Failed to initialize global view config")?;

                // Initialize multi-schema storage with default schema
                // Register with BOTH keys: actual schema name (if provided) + "default"
                let mut schemas = HashMap::new();
                let mut view_configs = HashMap::new();
                
                // Always register as "default"
                schemas.insert("default".to_string(), schema.clone());
                view_configs.insert("default".to_string(), config.clone());
                
                // Also register with schema name if provided in YAML
                if let Some(ref schema_name) = config.name {
                    println!("  Registering schema with name: {}", schema_name);
                    schemas.insert(schema_name.clone(), schema.clone());
                    view_configs.insert(schema_name.clone(), config.clone());
                } else {
                    println!("  Schema name not specified in YAML, using 'default' only");
                }
                
                GLOBAL_SCHEMAS.set(RwLock::new(schemas))
                    .map_err(|_| "Failed to initialize global schemas")?;
                GLOBAL_SCHEMA_CONFIGS.set(RwLock::new(view_configs))
                    .map_err(|_| "Failed to initialize global view configs")?;

                println!("✓ Schema initialization complete (single schema mode)");
                return Ok(SchemaSource::Yaml);
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
                
                GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema.clone()))
                    .map_err(|_| "Failed to initialize global graph schema")?;

                // Initialize multi-schema storage with default schema
                let mut schemas = HashMap::new();
                schemas.insert("default".to_string(), schema);
                GLOBAL_SCHEMAS.set(RwLock::new(schemas))
                    .map_err(|_| "Failed to initialize global schemas")?;

                let mut view_configs = HashMap::new();
                // For database mode, we don't have a view config, so create an empty one
                let empty_config = GraphSchemaConfig {
                    name: None,
                    graph_schema: crate::graph_catalog::config::GraphSchemaDefinition {
                        nodes: Vec::new(),
                        relationships: Vec::new(),
                    },
                };
                view_configs.insert("default".to_string(), empty_config);
                GLOBAL_SCHEMA_CONFIGS.set(RwLock::new(view_configs))
                    .map_err(|_| "Failed to initialize global view configs")?;

                println!("✓ Schema initialization complete (database mode)");
                Ok(SchemaSource::Database)
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
                            "default".to_string(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                        );
                        
                        GLOBAL_GRAPH_SCHEMA.set(RwLock::new(empty_schema.clone()))
                            .map_err(|_| "Failed to initialize global graph schema")?;

                        // Initialize multi-schema storage with empty default schema
                        let mut schemas = HashMap::new();
                        schemas.insert("default".to_string(), empty_schema);
                        GLOBAL_SCHEMAS.set(RwLock::new(schemas))
                            .map_err(|_| "Failed to initialize global schemas")?;

                        let mut view_configs = HashMap::new();
                        let empty_config = GraphSchemaConfig {
                            name: None,
                            graph_schema: GraphSchemaDefinition {
                                nodes: Vec::new(),
                                relationships: Vec::new(),
                            },
                        };
                        view_configs.insert("default".to_string(), empty_config);
                        GLOBAL_SCHEMA_CONFIGS.set(RwLock::new(view_configs))
                            .map_err(|_| "Failed to initialize global view configs")?;

                        println!("✓ Empty schema initialized successfully");
                        println!("  ClickGraph is ready to use. Add schema via YAML config or CREATE statements.");
                        Ok(SchemaSource::Database)
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
            "default".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );

        GLOBAL_GRAPH_SCHEMA.set(RwLock::new(empty_schema.clone()))
            .map_err(|_| "Failed to initialize global graph schema")?;

        // Initialize multi-schema storage with empty default schema
        let mut schemas = HashMap::new();
        schemas.insert("default".to_string(), empty_schema);
        GLOBAL_SCHEMAS.set(RwLock::new(schemas))
            .map_err(|_| "Failed to initialize global schemas")?;

        let mut view_configs = HashMap::new();
        let empty_config = GraphSchemaConfig {
            name: None,
            graph_schema: GraphSchemaDefinition {
                nodes: Vec::new(),
                relationships: Vec::new(),
            },
        };
        view_configs.insert("default".to_string(), empty_config);
        GLOBAL_SCHEMA_CONFIGS.set(RwLock::new(view_configs))
            .map_err(|_| "Failed to initialize global view configs")?;

        println!("✓ Minimal schema initialized - server ready for YAML configuration");
        Ok(SchemaSource::Database)
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

pub async fn get_view_config() -> Option<GraphSchemaConfig> {
    if let Some(config_guard) = GLOBAL_SCHEMA_CONFIG.get() {
        let config = config_guard.read().await;
        Some((*config).clone())
    } else {
        None
    }
}

// Multi-schema support functions - NEW
pub async fn get_graph_schema_by_name(schema_name: &str) -> Result<GraphSchema, String> {
    let schemas_guard = GLOBAL_SCHEMAS
        .get()
        .ok_or("Global schemas not initialized")?
        .read()
        .await;

    schemas_guard
        .get(schema_name)
        .cloned()
        .ok_or(format!("Schema '{}' not found", schema_name))
}

pub async fn get_view_config_by_name(schema_name: &str) -> Result<GraphSchemaConfig, String> {
    let configs_guard = GLOBAL_SCHEMA_CONFIGS
        .get()
        .ok_or("Global view configs not initialized")?
        .read()
        .await;

    configs_guard
        .get(schema_name)
        .cloned()
        .ok_or(format!("View config for schema '{}' not found", schema_name))
}

pub async fn list_available_schemas() -> Vec<String> {
    if let Some(schemas_guard) = GLOBAL_SCHEMAS.get() {
        let schemas = schemas_guard.read().await;
        schemas.keys().cloned().collect()
    } else {
        Vec::new()
    }
}

pub async fn load_schema_by_name(schema_name: &str, config_path: &str, clickhouse_client: Option<Client>, validate_schema: bool) -> Result<(), String> {
    println!("Loading schema '{}' from config: {}", schema_name, config_path);

    match load_schema_and_config_from_yaml(config_path).await {
        Ok((schema, config)) => {
            println!("✓ Successfully loaded schema '{}' from YAML config", schema_name);

            // Validate schema against ClickHouse if requested
            if validate_schema {
                if let Some(client) = clickhouse_client.as_ref() {
                    println!("  Validating schema '{}' against ClickHouse...", schema_name);
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

            // Add to multi-schema storage
            let schemas_lock = GLOBAL_SCHEMAS
                .get()
                .ok_or("Global schemas not initialized")?;
            let mut schemas_guard = schemas_lock.write().await;
            schemas_guard.insert(schema_name.to_string(), schema.clone());

            let configs_lock = GLOBAL_SCHEMA_CONFIGS
                .get()
                .ok_or("Global view configs not initialized")?;
            let mut configs_guard = configs_lock.write().await;
            configs_guard.insert(schema_name.to_string(), config);

            // IMPORTANT: Only update GLOBAL_GRAPH_SCHEMA if it's uninitialized
            // This prevents API-loaded schemas from overwriting the default schema
            // and causing race conditions between concurrent queries
            if let Some(global_schema_lock) = GLOBAL_GRAPH_SCHEMA.get() {
                let current_schema = global_schema_lock.read().await;
                let is_empty = current_schema.get_nodes_schemas().is_empty();
                drop(current_schema);  // Release read lock before acquiring write lock
                
                if is_empty {
                    let mut global_schema_guard = global_schema_lock.write().await;
                    *global_schema_guard = schema;
                    println!("  ✓ Set GLOBAL_GRAPH_SCHEMA (was uninitialized)");
                } else {
                    println!("  ℹ GLOBAL_GRAPH_SCHEMA already set from config, not overwriting");
                    println!("    Schema '{}' available in GLOBAL_SCHEMAS for USE clause", schema_name);
                }
            }

            println!("✓ Schema '{}' loaded successfully", schema_name);
            Ok(())
        }
        Err(e) => {
            Err(format!("Failed to load schema '{}': {}", schema_name, e))
        }
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
    let global_schema = GLOBAL_GRAPH_SCHEMA.get().ok_or_else(|| "Global graph schema not initialized".to_string())?;
    let mut graph_schema = global_schema.write().await;

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
pub async fn monitor_schema_updates(ch_client: Client) {
    // TODO Currently checking after every min. Make it an option to set by user.
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        // Check if global schema is initialized before proceeding
        let global_schema = match GLOBAL_GRAPH_SCHEMA.get() {
            Some(schema) => schema,
            None => {
                eprintln!("Schema monitor: Global schema not initialized, skipping check");
                continue;
            }
        };

        // Get current in-memory schema version
        let mem_version = {
            let in_mem_schema_guard = global_schema.read().await;
            in_mem_schema_guard.get_version()
        };

        // Fetch the schema from ClickHouse
        let remote_schema = match get_graph_catalog(ch_client.clone()).await {
            Ok(schema) => schema,
            Err(err) => {
                eprintln!("Schema monitor: Error fetching remote schema: {}", err);
                continue;
            }
        };

        // Compare versions and update if needed
        if remote_schema.get_version() != mem_version {
            let mut schema_guard = global_schema.write().await;
            *schema_guard = remote_schema.clone();

            println!(
                "✓ Schema monitor: Global schema updated from version {} to {}",
                mem_version,
                remote_schema.get_version()
            );
        }
    }
}

// Load schema from YAML configuration file

