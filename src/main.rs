use clap::Parser;
use clickgraph::{config, server};

/// ClickGraph - A graph analysis layer for ClickHouse
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// HTTP server host address
    #[arg(long, default_value = "0.0.0.0")]
    http_host: String,

    /// HTTP server port
    #[arg(long, default_value_t = 8080)]
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

    /// Validate YAML schema against ClickHouse tables on startup
    #[arg(long)]
    validate_schema: bool,

    /// Run server in daemon mode (background process)
    #[arg(long)]
    daemon: bool,
}

impl From<Cli> for config::CliConfig {
    fn from(cli: Cli) -> Self {
        config::CliConfig {
            http_host: cli.http_host,
            http_port: cli.http_port,
            bolt_host: cli.bolt_host,
            bolt_port: cli.bolt_port,
            bolt_enabled: !cli.disable_bolt, // Invert the flag
            max_cte_depth: cli.max_cte_depth,
            validate_schema: cli.validate_schema,
            daemon: cli.daemon,
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize logger - defaults to INFO level, can be overridden with RUST_LOG env var
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    let cli = Cli::parse();

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
