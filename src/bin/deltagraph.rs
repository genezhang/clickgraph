//! DeltaGraph server — read-only Cypher against a Databricks SQL Warehouse.
//!
//! The `deltagraph` binary is a thin sibling of `clickgraph`: same HTTP +
//! Bolt server, same Cypher language, but emits Spark SQL via the dialect
//! routing landed in Phase 1.x and executes against a Databricks SQL
//! Warehouse via the executor from Phase 2.x. The only user-visible
//! defaults that differ are the `--databricks` flag (forced on) and the
//! `DATABRICKS_*` env vars (consumed at startup; see `--help` below).
//!
//! Available only with the `databricks` Cargo feature — `Cargo.toml`'s
//! `[[bin]] required-features` gate keeps the default `cargo build`
//! producing only `clickgraph`. To build:
//!
//!   cargo build --release --features databricks --bin deltagraph
//!
//! The binary deliberately does not implement its own server loop — it
//! re-uses `clickgraph::server::run_with_config` so improvements to the
//! shared server (Bolt fixes, query cache, observability) reach both
//! binaries automatically. Phase 4.3 (Neo4j Browser → DeltaGraph
//! end-to-end) builds on this.

// jemalloc parity with the clickgraph binary — same MSVC guard so the
// Windows build doesn't try to link a crate that isn't compiled in.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use clickgraph::{config, server};

/// DeltaGraph — a Cypher query engine over Databricks SQL Warehouses.
///
/// Reads the workspace host, warehouse ID, and PAT from these env vars:
///
///   DATABRICKS_HOST           e.g. dbc-abc123-def4.cloud.databricks.com
///   DATABRICKS_WAREHOUSE_ID   the target SQL Warehouse ID
///   DATABRICKS_TOKEN          a personal access token (env-only)
///   DATABRICKS_CATALOG        (optional) catalog override
///   DATABRICKS_SCHEMA         (optional) schema override
///
/// The token is never accepted on the command line — it would otherwise
/// leak via `ps` and shell history.
#[derive(Parser)]
// Keep `long_about` enabled — the doc comment below lists the
// DATABRICKS_* env vars and the token-on-CLI policy, and the smoke
// test pins those strings as the user's discovery surface.
#[command(author, version, about, name = "deltagraph")]
struct Cli {
    /// HTTP server host address
    #[arg(long, default_value = "0.0.0.0")]
    http_host: String,

    /// HTTP server port
    #[arg(long, default_value_t = 7475)]
    http_port: u16,

    /// Disable Bolt protocol server (enabled by default)
    #[arg(long)]
    disable_bolt: bool,

    /// Bolt server host address
    #[arg(long, default_value = "0.0.0.0")]
    bolt_host: String,

    /// Bolt server port
    #[arg(long, default_value_t = 7687)]
    bolt_port: u16,

    /// Maximum recursive CTE evaluation depth for variable-length paths
    #[arg(long, default_value_t = 100)]
    max_cte_depth: u32,

    /// Validate YAML schema on startup (no introspection against Databricks
    /// — `validate_schema` is a no-op here because the executor has no
    /// equivalent of ClickHouse's introspection path; the flag is accepted
    /// for parity with the `clickgraph` binary).
    #[arg(long)]
    validate_schema: bool,

    /// Run server in daemon mode (background process)
    #[arg(long)]
    daemon: bool,

    /// Disable Neo4j compatibility mode. By default `deltagraph` runs in
    /// compat mode so Neo4j Browser / NeoDash / graph-notebook can connect
    /// — that's the headline demo. Pass this flag when you want the raw
    /// ClickGraph server identity instead (mirrors `--disable-bolt`).
    #[arg(long)]
    disable_neo4j_compat: bool,

    /// Per-query timeout in seconds (0 = no timeout)
    #[arg(long, default_value_t = 300)]
    query_timeout_secs: u64,

    /// Maximum HTTP request body size in bytes
    #[arg(long, default_value_t = 1_048_576)]
    max_request_body_bytes: usize,

    /// Maximum concurrent queries (0 = unlimited)
    #[arg(long, default_value_t = 64)]
    max_concurrent_queries: usize,

    /// Log level (overridden by RUST_LOG env var)
    #[arg(long, default_value = "info")]
    log_level: String,
}

impl From<Cli> for config::CliConfig {
    fn from(cli: Cli) -> Self {
        config::CliConfig {
            http_host: cli.http_host,
            http_port: cli.http_port,
            bolt_host: cli.bolt_host,
            bolt_port: cli.bolt_port,
            bolt_enabled: !cli.disable_bolt,
            max_cte_depth: cli.max_cte_depth,
            validate_schema: cli.validate_schema,
            daemon: cli.daemon,
            neo4j_compat_mode: !cli.disable_neo4j_compat,
            embedded: false,
            // The whole point of this binary: force the Databricks path
            // on regardless of CLICKGRAPH_DATABRICKS env. Users who want
            // the ClickHouse path should use the `clickgraph` binary.
            databricks: true,
            query_timeout_secs: cli.query_timeout_secs,
            max_request_body_bytes: cli.max_request_body_bytes,
            max_concurrent_queries: cli.max_concurrent_queries,
        }
    }
}

fn main() {
    // Same tokio runtime tuning as `clickgraph` — recursive plan
    // traversal needs the larger thread stack, and the threshold is
    // dialect-independent.
    let stack_mb: usize = std::env::var("CLICKGRAPH_THREAD_STACK_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(stack_mb * 1024 * 1024)
        .build()
        .expect("Failed to create tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    let cli = Cli::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&cli.log_level))
        .init();

    println!("\nDeltaGraph v{}\n", env!("CARGO_PKG_VERSION"));

    let cli_config: config::CliConfig = cli.into();
    let config = match config::ServerConfig::from_cli(cli_config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    server::run_with_config(config).await;
}
