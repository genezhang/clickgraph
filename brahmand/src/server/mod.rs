use std::{env, sync::Arc};

use axum::{Router, routing::post};
use clickhouse::Client;
use handlers::query_handler;

use dotenv::dotenv;
use tokio::sync::{OnceCell, RwLock};
use tokio::net::TcpListener;

use crate::graph_catalog::graph_schema::GraphSchema;
use bolt_protocol::{BoltServer, BoltConfig};

pub mod bolt_protocol;
mod clickhouse_client;
mod graph_catalog;
mod handlers;
mod models;

// Server configuration
#[derive(Clone)]
pub struct ServerConfig {
    pub http_host: String,
    pub http_port: u16,
    pub bolt_host: String,
    pub bolt_port: u16,
    pub bolt_enabled: bool,
}

impl ServerConfig {
    fn from_env() -> Self {
        Self {
            http_host: env::var("BRAHMAND_HOST").unwrap_or("0.0.0.0".to_string()),
            http_port: env::var("BRAHMAND_PORT")
                .unwrap_or("8080".to_string())
                .parse()
                .expect("Invalid HTTP port"),
            bolt_host: env::var("BRAHMAND_BOLT_HOST").unwrap_or("0.0.0.0".to_string()),
            bolt_port: env::var("BRAHMAND_BOLT_PORT")
                .unwrap_or("7687".to_string())
                .parse()
                .expect("Invalid Bolt port"),
            bolt_enabled: env::var("BRAHMAND_BOLT_ENABLED")
                .unwrap_or("true".to_string())
                .parse()
                .unwrap_or(true),
        }
    }
    
    /// Create config from command-line arguments with env variable fallbacks
    pub fn from_args(cli_config: CliConfig) -> Self {
        Self {
            http_host: cli_config.http_host,
            http_port: cli_config.http_port,
            bolt_host: cli_config.bolt_host,
            bolt_port: cli_config.bolt_port,
            bolt_enabled: cli_config.bolt_enabled,
        }
    }
}

/// Configuration from CLI arguments
pub struct CliConfig {
    pub http_host: String,
    pub http_port: u16,
    pub bolt_host: String,
    pub bolt_port: u16,
    pub bolt_enabled: bool,
}

// #[derive(Clone)]
struct AppState {
    clickhouse_client: Client,
}

pub static GLOBAL_GRAPH_SCHEMA: OnceCell<RwLock<GraphSchema>> = OnceCell::const_new();

pub async fn run() {
    dotenv().ok();
    
    // Load server configuration from environment variables
    let config = ServerConfig::from_env();
    
    run_with_config(config).await;
}

pub async fn run_with_config(config: ServerConfig) {
    dotenv().ok();
    
    // Create and configure the ClickHouse client.
    let client = clickhouse_client::get_client();

    let app_state = AppState {
        clickhouse_client: client.clone(),
    };

    graph_catalog::initialize_global_schema(client.clone()).await;

    println!("GLOBAL_GRAPH_SCHEMA {:?}", GLOBAL_GRAPH_SCHEMA.get());

    // Spawn the background task to monitor schema updates.
    let schema_client = client.clone();
    tokio::spawn(async move {
        if let Err(e) = graph_catalog::monitor_schema_updates(schema_client).await {
            eprintln!("Error in schema monitor: {}", e);
        }
    });

    // Start HTTP server
    let http_bind_address = format!("{}:{}", config.http_host, config.http_port);
    println!("Starting HTTP server on {}", http_bind_address);
    
    let app = Router::new()
        .route("/query", post(query_handler))
        .with_state(Arc::new(app_state));

    let http_listener = TcpListener::bind(&http_bind_address).await.unwrap();
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
            server_agent: format!("Brahmand/{}", env!("CARGO_PKG_VERSION")),
        };
        
        let bolt_server = Arc::new(tokio::sync::Mutex::new(BoltServer::new(bolt_config)));
        let bolt_listener = match TcpListener::bind(&bolt_bind_address).await {
            Ok(listener) => {
                println!("Successfully bound Bolt listener to {}", bolt_bind_address);
                listener
            }
            Err(e) => {
                eprintln!("Failed to bind Bolt listener to {}: {}", bolt_bind_address, e);
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
                            let mut server_guard = server.lock().await;
                            if let Err(e) = server_guard.handle_connection(stream, addr_str).await {
                                eprintln!("Bolt connection error: {:?}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept Bolt connection: {:?}", e);
                        break;
                    }
                }
            }
        });
    }
    
    println!("Brahmand server is running");
    println!("  HTTP API: http://{}", http_bind_address);
    if config.bolt_enabled {
        println!("  Bolt Protocol: bolt://{}", format!("{}:{}", config.bolt_host, config.bolt_port));
    }
    
    // Run HTTP server (this will block until shutdown)
    http_server.await.unwrap();
}
