use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use clickhouse::Client;
use handlers::{
    discover_prompt_handler, draft_handler, get_schema_handler, health_check, introspect_handler,
    list_schemas_handler, load_schema_handler, query_handler,
};
use sql_generation_handler::sql_generation_handler;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::timeout::TimeoutLayer;

use dotenvy::dotenv;
use tokio::net::TcpListener;
#[cfg(windows)]
use tokio::signal;
use tokio::sync::{OnceCell, RwLock, Semaphore};

use crate::config::ServerConfig;
use crate::executor::{remote::RemoteClickHouseExecutor, QueryExecutor};
use crate::graph_catalog::graph_schema::GraphSchema;
use bolt_protocol::{BoltConfig, BoltServer};

pub mod bolt_protocol;
mod clickhouse_client;
pub mod connection_pool;
pub mod graph_catalog;
pub mod graph_output;
pub mod handlers;
pub mod models;
mod parameter_substitution;
mod query_cache;
pub mod query_context;
mod sql_generation_handler;

#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<dyn QueryExecutor>,
    /// Raw ClickHouse client for admin/DDL operations.
    /// `None` in embedded mode — admin endpoints return 501.
    pub clickhouse_client: Option<Client>,
    pub config: ServerConfig,
    /// Semaphore limiting concurrent query processing.
    /// `None` when max_concurrent_queries = 0 (unlimited).
    pub query_semaphore: Option<Arc<Semaphore>>,
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

    // ── Embedded mode: use in-process chdb instead of remote ClickHouse ────────
    #[cfg(feature = "embedded")]
    if config.embedded {
        log::info!("🔌 Embedded mode: using in-process chdb (no ClickHouse server required)");

        // Initialize schema (no ClickHouse client needed)
        let _schema_source =
            match graph_catalog::initialize_global_schema(None, config.validate_schema).await {
                Ok(source) => source,
                Err(e) => {
                    log::error!("✗ Failed to initialize schema: {}", e);
                    std::process::exit(1);
                }
            };

        // Build ChdbExecutor with ephemeral session
        let chdb_executor = match crate::executor::chdb_embedded::ChdbExecutor::new_ephemeral() {
            Ok(e) => e,
            Err(e) => {
                log::error!("✗ Failed to create chdb executor: {}", e);
                std::process::exit(1);
            }
        };

        // Load schema source: VIEWs if any schema entries have source: URIs
        if let Some(schema_lock) = GLOBAL_SCHEMAS.get() {
            if let Ok(schemas) = schema_lock.try_read() {
                for schema in schemas.values() {
                    if let Err(e) =
                        crate::executor::data_loader::load_schema_sources(&chdb_executor, schema)
                    {
                        log::warn!("⚠ Failed to load schema sources: {}", e);
                    }
                }
            }
        }

        let executor: Arc<dyn QueryExecutor> = Arc::new(chdb_executor);
        let app_state = AppState {
            executor,
            clickhouse_client: None,
            query_semaphore: make_query_semaphore(&config),
            config: config.clone(),
        };

        // Initialize query cache
        let cache_config = query_cache::QueryCacheConfig::from_env();
        let _ = GLOBAL_QUERY_CACHE.set(query_cache::QueryCache::new(cache_config));

        return run_server(app_state, config).await;
    }

    #[cfg(not(feature = "embedded"))]
    if config.embedded {
        log::error!("✗ --embedded flag set but the `embedded` feature is not compiled in.");
        log::error!("  Recompile with: cargo build --features embedded");
        std::process::exit(1);
    }

    // ── Remote ClickHouse mode (default) ────────────────────────────────────────
    // Try to create ClickHouse client (optional for YAML-only mode)
    let client_opt = clickhouse_client::try_get_client();

    if client_opt.is_some() {
        log::info!("✓ ClickHouse client created successfully");
    } else {
        log::warn!("⚠ ClickHouse client could not be created (missing env vars?)");
    }

    // Create connection pool (uses same env vars as client)
    // If CLICKHOUSE_CLUSTER is set, this discovers cluster nodes for load balancing
    let connection_pool = match connection_pool::RoleConnectionPool::new(config.max_cte_depth).await
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            log::error!("✗ FATAL: Failed to create connection pool: {}", e);
            log::error!(
                "  Resolution: Ensure ClickHouse environment variables are set (CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)"
            );
            std::process::exit(1);
        }
    };

    let query_semaphore = make_query_semaphore(&config);
    let app_state = if client_opt.is_some() {
        let executor: Arc<dyn QueryExecutor> =
            Arc::new(RemoteClickHouseExecutor::new(connection_pool.clone()));
        AppState {
            executor,
            clickhouse_client: client_opt.clone(),
            query_semaphore: query_semaphore.clone(),
            config: config.clone(),
        }
    } else {
        // For YAML-only mode, we need a placeholder client
        // This is a limitation we should fix in the future
        log::error!("⚠ No ClickHouse configuration found. Running in YAML-only mode.");
        log::error!(
            "  Note: Some query functionality may be limited without ClickHouse connection."
        );

        let executor: Arc<dyn QueryExecutor> =
            Arc::new(RemoteClickHouseExecutor::new(connection_pool.clone()));
        AppState {
            executor,
            clickhouse_client: None,
            query_semaphore,
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

    log::debug!("Schema monitoring disabled: Using in-memory schema management");

    run_server(app_state, config).await;
}

/// Create query concurrency semaphore from config.
fn make_query_semaphore(config: &ServerConfig) -> Option<Arc<Semaphore>> {
    if config.max_concurrent_queries > 0 {
        Some(Arc::new(Semaphore::new(config.max_concurrent_queries)))
    } else {
        None
    }
}

/// Bind and serve HTTP (and optionally Bolt) using the given `app_state`.
async fn run_server(app_state: AppState, config: ServerConfig) {
    let http_bind_address = format!("{}:{}", config.http_host, config.http_port);
    log::info!("Starting HTTP server on {}", http_bind_address);

    let mut app = Router::new()
        .route("/health", get(health_check))
        .route("/query", post(query_handler))
        .route("/query/sql", post(sql_generation_handler))
        .route("/schemas", get(list_schemas_handler))
        .route("/schemas/load", post(load_schema_handler))
        .route("/schemas/{name}", get(get_schema_handler))
        .route("/schemas/introspect", post(introspect_handler))
        .route("/schemas/discover-prompt", post(discover_prompt_handler))
        .route("/schemas/draft", post(draft_handler))
        .with_state(Arc::new(app_state.clone()))
        // Body size limit (default 1 MB, configurable via CLICKGRAPH_MAX_REQUEST_BODY_BYTES)
        .layer(DefaultBodyLimit::max(config.max_request_body_bytes))
        // Catch panics in handlers — return 500 instead of dropping the connection
        .layer(CatchPanicLayer::new());

    // Per-request timeout (covers parsing + planning + execution)
    if config.query_timeout_secs > 0 {
        app = app.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(config.query_timeout_secs),
        ));
        log::info!("HTTP request timeout: {}s", config.query_timeout_secs);
    }

    log::info!(
        "Max request body size: {} bytes",
        config.max_request_body_bytes
    );
    if config.max_concurrent_queries > 0 {
        log::info!("Max concurrent queries: {}", config.max_concurrent_queries);
    }

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
            server_agent: if config.neo4j_compat_mode {
                "Neo4j/5.8.0".to_string() // Masquerade as Neo4j for tool compatibility
            } else {
                format!("ClickGraph/{}", env!("CARGO_PKG_VERSION"))
            },
            host: config.bolt_host.clone(),
            port: config.bolt_port,
        };

        // Clone the executor from app_state for Bolt server
        let bolt_executor = app_state.executor.clone();
        let bolt_server = Arc::new(BoltServer::new(bolt_config, bolt_executor));
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
                        println!("Accepted connection from: {}", addr);
                        let addr_str = addr.to_string();
                        let server = bolt_server.clone();

                        // Spawn individual connection handler
                        tokio::spawn(async move {
                            // Peek at first 4 bytes to detect protocol type
                            let mut peek_buf = [0u8; 4];
                            match stream.peek(&mut peek_buf).await {
                                Ok(n) if n >= 4 => {
                                    // Check if this is an HTTP request (WebSocket upgrade or probe)
                                    let is_http = peek_buf.starts_with(b"GET ")
                                        || peek_buf.starts_with(b"POST")
                                        || peek_buf.starts_with(b"OPTI") // OPTIONS preflight
                                        || peek_buf.starts_with(b"HEAD")
                                        || peek_buf.starts_with(b"PUT ")
                                        || peek_buf.starts_with(b"DELE"); // DELETE
                                    if is_http {
                                        // Only GET may carry a WebSocket upgrade; other HTTP
                                        // methods are browser probes — drop them silently.
                                        if peek_buf.starts_with(b"GET ")
                                            || peek_buf.starts_with(b"POST")
                                        {
                                            log::debug!(
                                                "Detected HTTP/WebSocket probe from {}",
                                                addr_str
                                            );

                                            // Attempt WebSocket upgrade
                                            match bolt_protocol::websocket::WebSocketBoltAdapter::new(
                                                stream,
                                            )
                                            .await
                                            {
                                                Ok(ws_adapter) => {
                                                    match server
                                                        .handle_connection(ws_adapter, addr_str.clone())
                                                        .await
                                                    {
                                                        Ok(_) => {
                                                            log::debug!("WebSocket Bolt connection closed successfully");
                                                        }
                                                        Err(e) => {
                                                            log::debug!("WebSocket Bolt connection closed from {}: {:?}", addr_str, e);
                                                        }
                                                    }
                                                }
                                                Err(_) => {
                                                    // Browser probe without WS upgrade — expected, ignore
                                                    log::debug!(
                                                        "HTTP probe (no WS upgrade) from {} — ignored",
                                                        addr_str
                                                    );
                                                }
                                            }
                                        } else {
                                            // OPTIONS/HEAD/etc: browser CORS preflight — drop silently
                                            log::debug!(
                                                "HTTP {} probe on Bolt port from {} — ignored",
                                                std::str::from_utf8(&peek_buf).unwrap_or("?"),
                                                addr_str
                                            );
                                        }
                                    } else {
                                        log::info!(
                                            "Detected TCP Bolt connection from {}",
                                            addr_str
                                        );

                                        // Handle raw TCP Bolt connection (existing behavior)
                                        match server
                                            .handle_connection(stream, addr_str.clone())
                                            .await
                                        {
                                            Ok(_) => {
                                                log::debug!(
                                                    "TCP Bolt connection closed successfully"
                                                );
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "TCP Bolt connection error from {}: {:?}",
                                                    addr_str,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                Ok(_) => {
                                    log::warn!(
                                        "Connection from {} closed before protocol detection",
                                        addr_str
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to peek connection from {}: {}",
                                        addr_str,
                                        e
                                    );
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
            "  Bolt Protocol: bolt://{}:{}",
            config.bolt_host, config.bolt_port
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
