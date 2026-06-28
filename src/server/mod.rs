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
use connection_pool::RoleConnectionPool;

pub mod bolt_protocol;
mod clickhouse_client;
pub mod connection_pool;
pub mod graph_catalog;
pub mod graph_output;
pub mod handlers;
pub mod metrics;
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
    /// Remote ClickHouse connection pool, exposed so `/stats` and `/metrics`
    /// can report pool stats. `None` in embedded / Databricks modes.
    pub pool: Option<Arc<RoleConnectionPool>>,
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

// Observability registry (aggregate counters, latency histograms, slow-query
// ring). Initialized once in `run_server` before the listener binds.
pub static GLOBAL_SERVER_METRICS: OnceCell<Arc<metrics::ServerMetrics>> = OnceCell::const_new();

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
            pool: None,
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

    // ── DeltaGraph mode: route reads through a Databricks SQL Warehouse ────────
    //
    // Mirrors the embedded branch: when set, we build a Databricks executor
    // instead of the ClickHouse pool and stash it in AppState behind the
    // same QueryExecutor trait the read path already uses. AppState's
    // `clickhouse_client: Option<Client>` stays None, and that None is what
    // makes the server read-only for this dispatch — `query_handler_inner`
    // returns 501 NOT_IMPLEMENTED for any DDL when `clickhouse_client` is
    // absent (same code path the embedded branch already relies on). The
    // planner-level `write_guard` only kicks in for the embedded crate's
    // `Database::new_*` constructors, not the server, so we are not
    // depending on `ExecutorKind` here.
    #[cfg(feature = "databricks")]
    if config.databricks {
        if config.embedded {
            log::error!("✗ --embedded and --databricks are mutually exclusive.");
            std::process::exit(1);
        }
        log::info!("🧱 DeltaGraph mode: routing queries through a Databricks SQL Warehouse");

        let mut dbc_config = match build_databricks_config() {
            Ok(c) => c,
            Err(e) => {
                log::error!("✗ Databricks configuration error: {e}");
                std::process::exit(1);
            }
        };
        // DeltaGraph Phase 3.2: when DATABRICKS_CATALOG was unset, fall
        // back to the optional top-level `catalog:` field in the schema
        // YAML pointed to by `GRAPH_CONFIG_PATH`. Env still wins. Errors
        // are intentionally non-fatal here — `initialize_global_schema`
        // below is the authoritative loader and will surface any real
        // parse problem with full diagnostics; we only need the field.
        if dbc_config.catalog.is_none() {
            if let Some(yaml_catalog) = read_yaml_catalog_for_server() {
                log::info!(
                    "  Using catalog '{yaml_catalog}' from schema YAML \
                     (DATABRICKS_CATALOG was unset)"
                );
                dbc_config.catalog = Some(yaml_catalog);
            }
        }
        let dbc_executor =
            match crate::executor::databricks_sql::DatabricksSqlExecutor::new(dbc_config) {
                Ok(e) => e,
                Err(e) => {
                    log::error!("✗ Failed to create Databricks executor: {e}");
                    std::process::exit(1);
                }
            };

        let _schema_source =
            match graph_catalog::initialize_global_schema(None, config.validate_schema).await {
                Ok(source) => source,
                Err(e) => {
                    log::error!("✗ Failed to initialize schema: {e}");
                    std::process::exit(1);
                }
            };

        let executor: Arc<dyn QueryExecutor> = Arc::new(dbc_executor);

        // Target the Databricks SQL dialect for every server-handled query.
        // Query rendering reads the dialect from the per-query `QueryContext`,
        // which seeds from this process-wide default; without it the server
        // would emit ClickHouse SQL even in `--databricks` mode (e.g. VLP array
        // syntax `[]`/`Array(T)`/`arrayConcat`/`has` instead of Spark's
        // `array()`/`ARRAY<T>`/`concat`/`array_contains`).
        query_context::set_server_dialect(crate::sql_generator::SqlDialect::Databricks);

        let app_state = AppState {
            executor,
            clickhouse_client: None,
            query_semaphore: make_query_semaphore(&config),
            config: config.clone(),
            pool: None,
        };

        let cache_config = query_cache::QueryCacheConfig::from_env();
        let _ = GLOBAL_QUERY_CACHE.set(query_cache::QueryCache::new(cache_config));

        return run_server(app_state, config).await;
    }

    #[cfg(not(feature = "databricks"))]
    if config.databricks {
        log::error!("✗ --databricks flag set but the `databricks` feature is not compiled in.");
        log::error!("  Recompile with: cargo build --features databricks");
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
        let executor: Arc<dyn QueryExecutor> = Arc::new(RemoteClickHouseExecutor::with_ch_summary(
            connection_pool.clone(),
            config.metrics_ch_summary,
        ));
        AppState {
            executor,
            clickhouse_client: client_opt.clone(),
            query_semaphore: query_semaphore.clone(),
            config: config.clone(),
            pool: Some(connection_pool.clone()),
        }
    } else {
        // For YAML-only mode, we need a placeholder client
        // This is a limitation we should fix in the future
        log::error!("⚠ No ClickHouse configuration found. Running in YAML-only mode.");
        log::error!(
            "  Note: Some query functionality may be limited without ClickHouse connection."
        );

        let executor: Arc<dyn QueryExecutor> = Arc::new(RemoteClickHouseExecutor::with_ch_summary(
            connection_pool.clone(),
            config.metrics_ch_summary,
        ));
        AppState {
            executor,
            clickhouse_client: None,
            query_semaphore,
            config: config.clone(),
            pool: Some(connection_pool.clone()),
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

/// Build the fully-layered HTTP router for the given state and config.
///
/// Extracted from `run_server` so it can be exercised directly in tests via
/// `tower::ServiceExt::oneshot` without binding a real listener.
pub fn build_router(app_state: AppState, config: &ServerConfig) -> Router {
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
        // Observability / stats / performance monitoring
        .route("/metrics", get(handlers::metrics_handler))
        .route("/stats", get(handlers::stats_handler))
        .route("/stats/queries", get(handlers::stats_queries_handler))
        .with_state(Arc::new(app_state))
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
    }
    app
}

/// Bind and serve HTTP (and optionally Bolt) using the given `app_state`.
async fn run_server(app_state: AppState, config: ServerConfig) {
    let http_bind_address = format!("{}:{}", config.http_host, config.http_port);
    log::info!("Starting HTTP server on {}", http_bind_address);

    // Initialize the observability registry once, before binding the listener
    // (same ordering guarantee as GLOBAL_QUERY_CACHE).
    let metrics_cfg = metrics::MetricsConfig {
        enabled: config.metrics_enabled,
        slow_query_capacity: config.slow_query_capacity,
        slow_query_threshold_ms: config.slow_query_threshold_ms,
        query_preview: config.metrics_query_preview,
    };
    let _ = GLOBAL_SERVER_METRICS.set(Arc::new(metrics::ServerMetrics::new(metrics_cfg)));

    let app = build_router(app_state.clone(), &config);

    if config.query_timeout_secs > 0 {
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

/// Resolve a DatabricksConfig from `DATABRICKS_*` env vars. Returns a
/// human-readable `String` error (vs a typed one) because the caller
/// just logs it and exits — no upstream needs to discriminate the
/// failure mode. Used by the `--databricks` server path and by the
/// `deltagraph` binary's startup.
#[cfg(feature = "databricks")]
fn build_databricks_config() -> Result<crate::executor::databricks_sql::DatabricksConfig, String> {
    let hostname = std::env::var("DATABRICKS_HOST").map_err(|_| {
        "DATABRICKS_HOST not set — provide the workspace host \
         (e.g. dbc-abc123-def4.cloud.databricks.com)"
            .to_string()
    })?;
    let warehouse_id = std::env::var("DATABRICKS_WAREHOUSE_ID")
        .map_err(|_| "DATABRICKS_WAREHOUSE_ID not set".to_string())?;
    let token = std::env::var("DATABRICKS_TOKEN").map_err(|_| {
        "DATABRICKS_TOKEN not set — provide a personal access token \
         (env-only; never accepted as a CLI flag)"
            .to_string()
    })?;
    let mut cfg =
        crate::executor::databricks_sql::DatabricksConfig::new(hostname, warehouse_id, token);
    cfg.catalog = std::env::var("DATABRICKS_CATALOG").ok();
    cfg.schema = std::env::var("DATABRICKS_SCHEMA").ok();
    // Test-only override for the executor's request base URL. Honored
    // by the executor (`DatabricksConfig.base_url`) and used by the
    // deltagraph subprocess test in tests/rust/bin/ to redirect HTTP
    // at a wiremock URL. Production callers leave this unset — the
    // executor falls back to `https://{hostname}`.
    //
    // Security guardrail: a misconfigured override would otherwise send
    // the PAT in plaintext to whatever endpoint is named. We accept
    // either an `https://` URL (any host, real workspaces) or `http://`
    // restricted to loopback (wiremock-style tests). Anything else is
    // rejected up front, and any successful override produces a single
    // `log::warn!` so a stray production setting can't be silent.
    if let Ok(raw) = std::env::var("DATABRICKS_BASE_URL") {
        validate_databricks_base_url(&raw)?;
        log::warn!(
            "⚠ DATABRICKS_BASE_URL override in use ({raw}); the PAT will be sent there. \
             This env var is intended for tests against a local mock — unset it for production."
        );
        cfg.base_url = Some(raw);
    }
    Ok(cfg)
}

/// Best-effort lookup of the optional top-level `catalog:` field in the
/// schema YAML pointed to by `GRAPH_CONFIG_PATH`. Returns `None` if the
/// env var is unset, the file is missing, or the YAML doesn't parse —
/// `initialize_global_schema` is called immediately after this and is
/// the authoritative loader; it will surface any real parse error with
/// full context, so swallowing the diagnostic here doesn't hide bugs.
/// We only need the catalog field; the rest of the schema is loaded
/// downstream.
#[cfg(feature = "databricks")]
fn read_yaml_catalog_for_server() -> Option<String> {
    let path = std::env::var("GRAPH_CONFIG_PATH").ok()?;
    let content = std::fs::read_to_string(&path).ok()?;
    let cfg: crate::graph_catalog::config::GraphSchemaConfig =
        serde_yaml::from_str(&content).ok()?;
    cfg.catalog
}

/// Reject overrides that would silently leak the PAT to a non-localhost
/// plaintext endpoint. Loopback HTTP is allowed (wiremock); arbitrary
/// HTTP is not. Anything that fails to parse or uses a non-http(s)
/// scheme is rejected with a clear message rather than being passed
/// through.
#[cfg(feature = "databricks")]
fn validate_databricks_base_url(raw: &str) -> Result<(), String> {
    // Minimal parsing — we don't want to pull in the `url` crate just
    // for this check. Match `https://...` unconditionally; `http://`
    // only when the host segment is `localhost` or `127.0.0.1` (with
    // optional port). Anything else is rejected.
    if let Some(rest) = raw.strip_prefix("https://") {
        if rest.is_empty() {
            return Err("DATABRICKS_BASE_URL is empty after https://".to_string());
        }
        return Ok(());
    }
    if let Some(rest) = raw.strip_prefix("http://") {
        let host = rest.split(['/', ':']).next().unwrap_or("");
        if host == "localhost" || host == "127.0.0.1" {
            return Ok(());
        }
        return Err(format!(
            "DATABRICKS_BASE_URL rejected: `http://` is only allowed for loopback (localhost, \
             127.0.0.1) to avoid leaking the PAT in plaintext. For real workspaces use https:// \
             or unset the variable. Got host={host:?}"
        ));
    }
    Err(format!(
        "DATABRICKS_BASE_URL rejected: must start with `https://` (or `http://localhost…` for \
         tests). Got: {raw:?}"
    ))
}

#[cfg(all(test, feature = "databricks"))]
mod databricks_base_url_validation_tests {
    use super::validate_databricks_base_url;

    #[test]
    fn accepts_https_for_real_workspaces() {
        assert!(validate_databricks_base_url("https://dbc-abc-def.cloud.databricks.com").is_ok());
        assert!(
            validate_databricks_base_url("https://dbc-abc-def.cloud.databricks.com/api/").is_ok()
        );
    }

    #[test]
    fn accepts_http_for_loopback_tests() {
        // wiremock + mock server hosts — anything bound to loopback.
        assert!(validate_databricks_base_url("http://127.0.0.1:55555").is_ok());
        assert!(validate_databricks_base_url("http://localhost:55555/api/2.0/sql").is_ok());
    }

    #[test]
    fn rejects_plaintext_http_to_external_hosts() {
        // The whole point of the validator: a typo'd
        // `DATABRICKS_BASE_URL=http://workspace.example.com` would
        // POST the PAT in plaintext. Block it up front.
        let err = validate_databricks_base_url("http://workspace.example.com").unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
        let err = validate_databricks_base_url("http://192.168.1.10:8080").unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_schemes() {
        // ftp/gopher/no-scheme — anything that wouldn't reach the
        // executor's reqwest client cleanly should fail loudly here
        // rather than producing a confusing runtime error later.
        assert!(validate_databricks_base_url("workspace.example.com").is_err());
        assert!(validate_databricks_base_url("ftp://workspace.example.com").is_err());
        assert!(validate_databricks_base_url("").is_err());
        assert!(validate_databricks_base_url("https://").is_err());
    }
}

/// DeltaGraph Phase 3.2 server-side wiring: tests that the optional
/// `catalog:` field in `GRAPH_CONFIG_PATH`'s YAML is picked up by the
/// `--databricks` server branch as a fallback when `DATABRICKS_CATALOG`
/// is unset. The helper itself is intentionally best-effort (returns
/// `None` on any failure); `initialize_global_schema` is the real
/// loader so error surfacing happens there.
///
/// Each test sets / unsets `GRAPH_CONFIG_PATH` and serializes via a
/// mutex — env vars are global to the process and concurrent tests
/// would race.
#[cfg(all(test, feature = "databricks"))]
mod yaml_catalog_for_server_tests {
    use super::read_yaml_catalog_for_server;
    use std::io::Write;
    use std::sync::Mutex;
    use tempfile::NamedTempFile;

    // Serialize env-var manipulation across the tests in this module.
    // `read_yaml_catalog_for_server` reads `GRAPH_CONFIG_PATH` from the
    // process env, so concurrent tests would interleave each other's
    // writes and produce flaky results.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_yaml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(content.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn returns_catalog_when_yaml_has_field() {
        let _guard = ENV_LOCK.lock().unwrap();
        let f = write_yaml(
            r#"
name: server_test
catalog: server_yaml_cat
graph_schema:
  nodes:
    - label: User
      database: graphs
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
"#,
        );
        unsafe { std::env::set_var("GRAPH_CONFIG_PATH", f.path()) };
        let got = read_yaml_catalog_for_server();
        unsafe { std::env::remove_var("GRAPH_CONFIG_PATH") };
        assert_eq!(got.as_deref(), Some("server_yaml_cat"));
    }

    #[test]
    fn returns_none_when_yaml_omits_catalog() {
        let _guard = ENV_LOCK.lock().unwrap();
        let f = write_yaml(
            r#"
name: server_test
graph_schema:
  nodes:
    - label: User
      database: graphs
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
"#,
        );
        unsafe { std::env::set_var("GRAPH_CONFIG_PATH", f.path()) };
        let got = read_yaml_catalog_for_server();
        unsafe { std::env::remove_var("GRAPH_CONFIG_PATH") };
        assert_eq!(got, None);
    }

    #[test]
    fn returns_none_when_env_var_unset() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Make sure no inherited value leaks in.
        unsafe { std::env::remove_var("GRAPH_CONFIG_PATH") };
        assert_eq!(read_yaml_catalog_for_server(), None);
    }

    #[test]
    fn returns_none_on_malformed_yaml() {
        // Best-effort: malformed YAML must not crash or panic. The real
        // parse error is surfaced later by `initialize_global_schema`
        // (the authoritative loader), so silent fallback here is safe.
        let _guard = ENV_LOCK.lock().unwrap();
        let f = write_yaml("this is :::: not valid yaml ::::");
        unsafe { std::env::set_var("GRAPH_CONFIG_PATH", f.path()) };
        let got = read_yaml_catalog_for_server();
        unsafe { std::env::remove_var("GRAPH_CONFIG_PATH") };
        assert_eq!(got, None);
    }
}
