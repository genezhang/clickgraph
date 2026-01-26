use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use clickhouse::Client;
use handlers::{
    get_schema_handler, health_check, list_schemas_handler, load_schema_handler, query_handler,
};
use sql_generation_handler::sql_generation_handler;

use dotenvy::dotenv;
use tokio::net::TcpListener;
#[cfg(windows)]
use tokio::signal;
use tokio::sync::{OnceCell, RwLock};

use crate::config::ServerConfig;
use crate::graph_catalog::graph_schema::GraphSchema;
use bolt_protocol::{BoltConfig, BoltServer};

pub mod bolt_protocol;
mod clickhouse_client;
pub mod connection_pool;
pub mod graph_catalog;
pub mod handlers;
mod models;
mod parameter_substitution;
mod query_cache;
pub mod query_context;
mod sql_generation_handler;

// #[derive(Clone)]
#[derive(Clone)]
pub struct AppState {
    pub clickhouse_client: Client,
    pub connection_pool: Arc<connection_pool::RoleConnectionPool>,
    pub config: ServerConfig,
}

// ==================================================================================
// SCHEMA STORAGE
// ==================================================================================
// Multi-schema registry stores all schemas by name (including "default")
// Schemas are selected via USE clause and passed through the query execution path:
//   1. handlers.rs: get_graph_schema_by_name(schema_name)
//   2. Planning: evaluate_read_query(ast, &graph_schema)
//   3. Rendering: to_render_plan(&graph_schema)
//
// Helper functions in render layer use GLOBAL_SCHEMAS["default"] as fallback
// for contexts where schema isn't directly available.
// ==================================================================================

// Legacy single-schema config support (DEPRECATED - use GLOBAL_SCHEMA_CONFIGS)
pub static GLOBAL_SCHEMA_CONFIG: OnceCell<RwLock<crate::graph_catalog::config::GraphSchemaConfig>> =
    OnceCell::const_new();

// Multi-schema support - all schemas stored by name (including "default")
pub static GLOBAL_SCHEMAS: OnceCell<RwLock<HashMap<String, GraphSchema>>> = OnceCell::const_new();
pub static GLOBAL_SCHEMA_CONFIGS: OnceCell<
    RwLock<HashMap<String, crate::graph_catalog::config::GraphSchemaConfig>>,
> = OnceCell::const_new();

// Query cache for SQL templates
pub static GLOBAL_QUERY_CACHE: OnceCell<query_cache::QueryCache> = OnceCell::const_new();

pub async fn run() {
    dotenv().ok();

    // Load server configuration from environment variables
    let config = match ServerConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            log::error!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    run_with_config(config).await;
}

pub async fn run_with_config(config: ServerConfig) {
    dotenv().ok();

    // Test that logging is working
    log::debug!("=== SERVER STARTING (debug log test) ===");
    log::info!(
        "Server configuration: http={}:{}, bolt={}:{}",
        config.http_host,
        config.http_port,
        config.bolt_host,
        config.bolt_port
    );

    // Try to create ClickHouse client (optional for YAML-only mode)
    let client_opt = clickhouse_client::try_get_client();

    if client_opt.is_some() {
        log::info!("✓ ClickHouse client created successfully");
    } else {
        log::warn!("⚠ ClickHouse client could not be created (missing env vars?)");
    }

    // Create connection pool (uses same env vars as client)
    let connection_pool = match connection_pool::RoleConnectionPool::new() {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            log::error!(
                "Warning: Failed to create connection pool: {}. Using default client.",
                e
            );
            // Create a minimal pool for YAML-only mode
            Arc::new(
                connection_pool::RoleConnectionPool::new().unwrap_or_else(|_| {
                    panic!("Failed to create connection pool even with defaults")
                }),
            )
        }
    };

    let app_state = if let Some(client) = client_opt.as_ref() {
        AppState {
            clickhouse_client: client.clone(),
            connection_pool: connection_pool.clone(),
            config: config.clone(),
        }
    } else {
        // For YAML-only mode, we need a placeholder client
        // This is a limitation we should fix in the future
        log::error!("⚠ No ClickHouse configuration found. Running in YAML-only mode.");
        log::error!(
            "  Note: Some query functionality may be limited without ClickHouse connection."
        );

        // Create a dummy client for now - this is not ideal but allows server to start
        let dummy_client = clickhouse::Client::default().with_url("http://localhost:8123");
        AppState {
            clickhouse_client: dummy_client,
            connection_pool: connection_pool.clone(),
            config: config.clone(),
        }
    };

    // Initialize schema with proper error handling
    let _schema_source =
        match graph_catalog::initialize_global_schema(client_opt.clone(), config.validate_schema)
            .await
        {
            Ok(source) => source,
            Err(e) => {
                log::error!("✗ Failed to initialize ClickGraph: {}", e);
                log::error!("  Server cannot start without proper schema initialization.");
                std::process::exit(1);
            }
        };

    log::debug!(
        "GLOBAL_SCHEMAS initialized: {:?}",
        GLOBAL_SCHEMAS.get().is_some()
    );

    // Initialize query cache
    let cache_config = query_cache::QueryCacheConfig::from_env();
    log::info!(
        "Initializing query cache: enabled={}, max_entries={}, max_size_mb={}",
        cache_config.enabled,
        cache_config.max_entries,
        cache_config.max_size_bytes / (1024 * 1024)
    );
    let _ = GLOBAL_QUERY_CACHE.set(query_cache::QueryCache::new(cache_config));

    // Schema monitoring disabled - our YAML-based schema format differs from upstream Brahmand
    // Re-enable when we implement proper schema versioning in ClickHouse tables
    log::debug!("Schema monitoring disabled: Using in-memory schema management");

    // // Start background schema monitoring (only for database-loaded schemas)
    // if let Some(schema_client) = client_opt {
    //     match schema_source {
    //         SchemaSource::Database => {
    //             tokio::spawn(async move {
    //                 println!("Starting background schema monitoring (checks every 60 seconds)");
    //                 graph_catalog::monitor_schema_updates(schema_client).await;
    //             });
    //         }
    //         SchemaSource::Yaml => {
    //             println!("Schema monitoring disabled: Schema loaded from YAML (static configuration)");
    //         }
    //     }
    // } else {
    //     println!("Schema monitoring disabled: No ClickHouse client available");
    // }

    // Start HTTP server
    let http_bind_address = format!("{}:{}", config.http_host, config.http_port);
    log::info!("Starting HTTP server on {}", http_bind_address);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/query", post(query_handler))
        .route("/query/sql", post(sql_generation_handler))
        .route("/schemas", get(list_schemas_handler))
        .route("/schemas/load", post(load_schema_handler))
        .route("/schemas/{name}", get(get_schema_handler))
        .with_state(Arc::new(app_state.clone()));

    let http_listener = match TcpListener::bind(&http_bind_address).await {
        Ok(listener) => {
            log::info!("Successfully bound HTTP listener to {}", http_bind_address);
            println!(
                "✓ Successfully bound HTTP listener to {}",
                http_bind_address
            );
            listener
        }
        Err(e) => {
            log::error!(
                "Failed to bind HTTP listener to {}: {}",
                http_bind_address,
                e
            );
            log::error!(
                "✗ FATAL: Failed to bind HTTP listener to {}: {}",
                http_bind_address,
                e
            );
            log::error!("  Is another process using port {}?", config.http_port);
            std::process::exit(1);
        }
    };

    let http_server = axum::serve(http_listener, app);

    // Start Bolt server if enabled
    if config.bolt_enabled {
        let bolt_bind_address = format!("{}:{}", config.bolt_host, config.bolt_port);
        println!("Starting Bolt server on {}", bolt_bind_address);

        let bolt_config = BoltConfig {
            max_message_size: 65536,
            connection_timeout: 300,
            enable_auth: false,
            default_user: Some("neo4j".to_string()),
            server_agent: format!("ClickGraph/{}", env!("CARGO_PKG_VERSION")),
        };

        // Clone the ClickHouse client from app_state for Bolt server
        let bolt_clickhouse_client = app_state.clickhouse_client.clone();
        let bolt_server = Arc::new(BoltServer::new(bolt_config, bolt_clickhouse_client));
        let bolt_listener = match TcpListener::bind(&bolt_bind_address).await {
            Ok(listener) => {
                println!("Successfully bound Bolt listener to {}", bolt_bind_address);
                listener
            }
            Err(e) => {
                log::error!(
                    "Failed to bind Bolt listener to {}: {}",
                    bolt_bind_address,
                    e
                );
                return;
            }
        };

        // Spawn Bolt server task
        tokio::spawn(async move {
            println!("Bolt server loop starting, listening for connections...");
            loop {
                match bolt_listener.accept().await {
                    Ok((stream, addr)) => {
                        println!("Accepted Bolt connection from: {}", addr);
                        let addr_str = addr.to_string();
                        let server = bolt_server.clone();

                        // Spawn individual connection handler
                        tokio::spawn(async move {
                            match server.handle_connection(stream, addr_str).await {
                                Ok(_) => {
                                    log::debug!("Bolt connection closed successfully");
                                }
                                Err(e) => {
                                    log::error!("Bolt connection error: {:?}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        log::error!("Failed to accept Bolt connection: {:?}", e);
                        break;
                    }
                }
            }
        });
    }

    println!("ClickGraph server is running");
    println!("  HTTP API: http://{}", http_bind_address);
    if config.bolt_enabled {
        println!(
            "  Bolt Protocol: bolt://{}",
            format!("{}:{}", config.bolt_host, config.bolt_port)
        );
    }

    if config.daemon {
        println!("Running in daemon mode - press Ctrl+C to stop");

        // Run server and signal handler concurrently
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Failed to register SIGTERM handler: {}. Server will run without graceful shutdown.", e);
                    log::error!("This is not fatal, but Ctrl+C may not work properly.");
                    // Continue without signal handling rather than crash
                    if let Err(e) = http_server.await {
                        log::error!("HTTP server error: {:?}", e);
                    }
                    return;
                }
            };
            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Failed to register SIGINT handler: {}. Server will run without graceful shutdown.", e);
                    log::error!("This is not fatal, but Ctrl+C may not work properly.");
                    // Continue without signal handling rather than crash
                    if let Err(e) = http_server.await {
                        log::error!("HTTP server error: {:?}", e);
                    }
                    return;
                }
            };

            tokio::select! {
                result = http_server => {
                    if let Err(e) = result {
                        log::error!("HTTP server error: {:?}", e);
                    }
                }
                _ = sigterm.recv() => println!("Received SIGTERM, shutting down..."),
                _ = sigint.recv() => println!("Received SIGINT, shutting down..."),
            }
        }

        #[cfg(windows)]
        {
            tokio::select! {
                result = http_server => {
                    if let Err(e) = result {
                        log::error!("HTTP server error: {:?}", e);
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("Received shutdown signal, shutting down...");
                }
            }
        }

        println!("Server stopped");
    } else {
        // Run HTTP server (this will block until shutdown)
        if let Err(e) = http_server.await {
            log::error!("HTTP server fatal error: {:?}", e);
            std::process::exit(1);
        }
    }
}
