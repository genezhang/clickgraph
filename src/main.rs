// jemalloc is only available on non-MSVC targets — Cargo.toml gates the dep
// with `[target.'cfg(not(target_env = "msvc"))'.dependencies]`. Mirror the
// guard here so Windows MSVC builds use the system allocator. Without this,
// the default `jemalloc` feature is on for Windows but the crate isn't,
// producing E0433 ("unresolved module `tikv_jemallocator`").
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::builder::BoolishValueParser;
use clap::{ArgAction, Parser};
use clickgraph::{config, server};

/// ClickGraph - A graph analysis layer for ClickHouse
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// HTTP server host address
    #[arg(long, env = "CLICKGRAPH_HOST", default_value = "0.0.0.0")]
    http_host: String,

    /// HTTP server port
    #[arg(long, env = "CLICKGRAPH_PORT", default_value_t = 7475)]
    http_port: u16,

    /// Disable Bolt protocol server (enabled by default)
    #[arg(long)]
    disable_bolt: bool,

    /// Enable the Bolt protocol server (`--disable-bolt` wins over this)
    #[arg(
        long,
        env = "CLICKGRAPH_BOLT_ENABLED",
        default_value_t = true,
        action = ArgAction::Set,
        value_parser = BoolishValueParser::new()
    )]
    bolt_enabled: bool,

    /// Bolt server host address
    #[arg(long, env = "CLICKGRAPH_BOLT_HOST", default_value = "0.0.0.0")]
    bolt_host: String,

    /// Bolt server port
    #[arg(long, env = "CLICKGRAPH_BOLT_PORT", default_value_t = 7687)]
    bolt_port: u16,

    /// Maximum recursive CTE evaluation depth for variable-length paths
    #[arg(long, env = "CLICKGRAPH_MAX_CTE_DEPTH", default_value_t = 100)]
    max_cte_depth: u32,

    /// Validate YAML schema against ClickHouse tables on startup
    #[arg(long, env = "CLICKGRAPH_VALIDATE_SCHEMA", value_parser = BoolishValueParser::new())]
    validate_schema: bool,

    /// Run server in daemon mode (background process)
    #[arg(long)]
    daemon: bool,

    /// Neo4j compatibility mode - masquerade as Neo4j server for tool compatibility
    /// Useful for graph-notebook, Neodash, and other Neo4j ecosystem tools
    #[arg(long, env = "CLICKGRAPH_NEO4J_COMPAT_MODE", value_parser = BoolishValueParser::new())]
    neo4j_compat_mode: bool,

    /// Run in embedded mode using in-process chdb (no ClickHouse server required).
    /// Requires the `embedded` Cargo feature.
    #[arg(long, env = "CLICKGRAPH_EMBEDDED", value_parser = BoolishValueParser::new())]
    embedded: bool,

    /// Route queries to a Databricks SQL Warehouse instead of ClickHouse.
    /// Reads DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TOKEN
    /// from the environment. Requires the `databricks` Cargo feature.
    /// Mutually exclusive with --embedded.
    #[arg(long, env = "CLICKGRAPH_DATABRICKS", value_parser = BoolishValueParser::new())]
    databricks: bool,

    /// Per-query timeout in seconds (0 = no timeout)
    #[arg(long, env = "CLICKGRAPH_QUERY_TIMEOUT_SECS", default_value_t = 300)]
    query_timeout_secs: u64,

    /// Maximum HTTP request body size in bytes
    #[arg(
        long,
        env = "CLICKGRAPH_MAX_REQUEST_BODY_BYTES",
        default_value_t = 1_048_576
    )]
    max_request_body_bytes: usize,

    /// Maximum concurrent queries (0 = unlimited)
    #[arg(long, env = "CLICKGRAPH_MAX_CONCURRENT_QUERIES", default_value_t = 64)]
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
            bolt_enabled: cli.bolt_enabled && !cli.disable_bolt, // `--disable-bolt` always wins
            max_cte_depth: cli.max_cte_depth,
            validate_schema: cli.validate_schema,
            daemon: cli.daemon,
            neo4j_compat_mode: cli.neo4j_compat_mode,
            embedded: cli.embedded,
            databricks: cli.databricks,
            query_timeout_secs: cli.query_timeout_secs,
            max_request_body_bytes: cli.max_request_body_bytes,
            max_concurrent_queries: cli.max_concurrent_queries,
        }
    }
}

fn main() {
    // Build tokio runtime with larger worker thread stacks to handle
    // deep recursive logical plan traversal (e.g., bidirectional + WITH chains).
    // Default 128 MB covers all known query patterns; override via CLICKGRAPH_THREAD_STACK_MB.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(clickgraph::translate::translation_stack_bytes())
        .build()
        .expect("Failed to create tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    // Load `.env` before parsing, so the `CLICKGRAPH_*` variables in it reach the
    // flags below the same way the shell's do (a flag still beats a variable).
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    // Initialize logger - use --log-level value as default, RUST_LOG env var overrides
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&cli.log_level))
        .init();

    println!("\nClickGraph v{}\n", env!("CARGO_PKG_VERSION"));

    // Create configuration from CLI args
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// Flags that deliberately have no `CLICKGRAPH_*` variable: `--disable-bolt`
    /// is the inverse of `CLICKGRAPH_BOLT_ENABLED`, `--daemon` is a launch mode
    /// and `--log-level` is overridden by `RUST_LOG`.
    const FLAGS_WITHOUT_A_VARIABLE: &[&str] = &["disable_bolt", "daemon", "log_level"];

    fn clear() {
        for arg in Cli::command().get_arguments() {
            if let Some(var) = arg.get_env() {
                std::env::remove_var(var);
            }
        }
    }

    fn parse(args: &[&str]) -> Result<config::CliConfig, clap::Error> {
        Cli::try_parse_from(std::iter::once("clickgraph").chain(args.iter().copied()))
            .map(config::CliConfig::from)
    }

    /// Every server flag reads `CLICKGRAPH_<NAME>` (`http_port` is `CLICKGRAPH_PORT`,
    /// `http_host` `CLICKGRAPH_HOST`): a flag added without its variable is the
    /// bug of #1592, where the Dockerfile, the compose files and the Helm chart
    /// set variables the binary never read.
    #[test]
    fn every_server_flag_has_its_clickgraph_variable() {
        for arg in Cli::command().get_arguments() {
            let id = arg.get_id().as_str();
            if FLAGS_WITHOUT_A_VARIABLE.contains(&id) {
                assert!(arg.get_env().is_none(), "`{id}` is listed as variable-less");
                continue;
            }
            let expected = format!(
                "CLICKGRAPH_{}",
                id.strip_prefix("http_").unwrap_or(id).to_uppercase()
            );
            assert_eq!(
                arg.get_env().and_then(|v| v.to_str()),
                Some(expected.as_str()),
                "flag `{id}` must read {expected}"
            );
        }
    }

    /// The `CLICKGRAPH_*` variables the docs, the Dockerfile and the Helm chart
    /// set configure the server, and a flag beats them. The `clickgraph` binary
    /// used to read none of them (it parses flags only), so a port set by
    /// variable was silently ignored.
    ///
    /// One test, not several: they all write the process environment.
    #[test]
    fn clickgraph_env_vars_configure_the_server_and_flags_beat_them() {
        clear();

        // No variable, no flag: the defaults.
        let c = parse(&[]).unwrap();
        assert_eq!((c.http_host.as_str(), c.http_port), ("0.0.0.0", 7475));
        assert_eq!((c.bolt_host.as_str(), c.bolt_port), ("0.0.0.0", 7687));
        assert!(c.bolt_enabled && !c.validate_schema && !c.neo4j_compat_mode && !c.embedded);
        assert_eq!(c.max_cte_depth, 100);
        assert_eq!(c.query_timeout_secs, 300);
        assert_eq!(c.max_request_body_bytes, 1_048_576);
        assert_eq!(c.max_concurrent_queries, 64);

        // Variables.
        for (var, value) in [
            ("CLICKGRAPH_HOST", "127.0.0.1"),
            ("CLICKGRAPH_PORT", "7991"),
            ("CLICKGRAPH_BOLT_HOST", "127.0.0.2"),
            ("CLICKGRAPH_BOLT_PORT", "7992"),
            ("CLICKGRAPH_BOLT_ENABLED", "false"),
            ("CLICKGRAPH_MAX_CTE_DEPTH", "500"),
            ("CLICKGRAPH_VALIDATE_SCHEMA", "true"),
            ("CLICKGRAPH_NEO4J_COMPAT_MODE", "true"),
            ("CLICKGRAPH_EMBEDDED", "true"),
            ("CLICKGRAPH_QUERY_TIMEOUT_SECS", "17"),
            ("CLICKGRAPH_MAX_REQUEST_BODY_BYTES", "2048"),
            ("CLICKGRAPH_MAX_CONCURRENT_QUERIES", "8"),
        ] {
            std::env::set_var(var, value);
        }
        let c = parse(&[]).unwrap();
        assert_eq!((c.http_host.as_str(), c.http_port), ("127.0.0.1", 7991));
        assert_eq!((c.bolt_host.as_str(), c.bolt_port), ("127.0.0.2", 7992));
        assert!(!c.bolt_enabled);
        assert_eq!(c.max_cte_depth, 500);
        assert!(c.validate_schema && c.neo4j_compat_mode && c.embedded);
        assert_eq!(c.query_timeout_secs, 17);
        assert_eq!(c.max_request_body_bytes, 2048);
        assert_eq!(c.max_concurrent_queries, 8);

        // A flag beats its variable.
        let c = parse(&["--http-port", "9000", "--max-cte-depth", "200"]).unwrap();
        assert_eq!((c.http_port, c.max_cte_depth), (9000, 200));
        assert_eq!(c.bolt_port, 7992, "a variable without a flag still applies");
        assert!(parse(&["--bolt-enabled", "true"]).unwrap().bolt_enabled);

        // The Dockerfile sets CLICKGRAPH_BOLT_ENABLED=true; `--disable-bolt` must
        // still turn Bolt off for the people who pass it.
        std::env::set_var("CLICKGRAPH_BOLT_ENABLED", "true");
        assert!(parse(&[]).unwrap().bolt_enabled);
        assert!(!parse(&["--disable-bolt"]).unwrap().bolt_enabled);

        // Boolean spellings, like the other opt-in flags.
        for (value, expected) in [
            ("true", true),
            ("1", true),
            ("yes", true),
            ("on", true),
            ("false", false),
            ("0", false),
            ("no", false),
            ("off", false),
        ] {
            std::env::set_var("CLICKGRAPH_VALIDATE_SCHEMA", value);
            std::env::set_var("CLICKGRAPH_BOLT_ENABLED", value);
            let c = parse(&[]).unwrap();
            assert_eq!(
                c.validate_schema, expected,
                "CLICKGRAPH_VALIDATE_SCHEMA={value}"
            );
            assert_eq!(c.bolt_enabled, expected, "CLICKGRAPH_BOLT_ENABLED={value}");
        }

        // A value that is not valid stops the start instead of being ignored.
        std::env::set_var("CLICKGRAPH_VALIDATE_SCHEMA", "maybe");
        assert!(parse(&[]).is_err(), "a boolean variable of `maybe`");
        std::env::set_var("CLICKGRAPH_VALIDATE_SCHEMA", "false");
        std::env::set_var("CLICKGRAPH_PORT", "not-a-port");
        assert!(parse(&[]).is_err(), "a port of `not-a-port`");
        std::env::set_var("CLICKGRAPH_PORT", "70000");
        assert!(parse(&[]).is_err(), "a port above 65535");

        clear();
    }
}
