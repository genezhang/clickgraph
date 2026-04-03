use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod config;
mod llm;
mod schema_fmt;

use config::CgConfig;

#[derive(Parser)]
#[command(
    name = "cg",
    about = "ClickGraph CLI — translate and run Cypher queries against ClickHouse",
    version
)]
struct Cli {
    /// Path to graph schema YAML file
    #[arg(long, env = "CG_SCHEMA", global = true)]
    schema: Option<String>,

    /// ClickHouse URL for query execution (e.g. http://localhost:8123)
    #[arg(long, env = "CG_CLICKHOUSE_URL", global = true)]
    clickhouse: Option<String>,

    /// ClickHouse user
    #[arg(long, env = "CG_CLICKHOUSE_USER", global = true, default_value = "default")]
    ch_user: String,

    /// ClickHouse password
    #[arg(long, env = "CG_CLICKHOUSE_PASSWORD", global = true, default_value = "")]
    ch_password: String,

    /// ClickHouse database to query
    #[arg(long, env = "CG_CLICKHOUSE_DATABASE", global = true)]
    ch_database: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Translate Cypher to ClickHouse SQL (no execution)
    Sql {
        /// Cypher query string
        query: String,
    },

    /// Validate Cypher syntax and planning against the schema
    Validate {
        /// Cypher query string
        query: String,
    },

    /// Execute a Cypher query (requires --clickhouse URL)
    Query {
        /// Cypher query string
        query: String,

        /// Only translate to SQL, do not execute
        #[arg(long)]
        sql_only: bool,

        /// Output format: table (default), json, pretty
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Translate a natural-language description to Cypher and optionally execute it
    Nl {
        /// Natural language query description
        description: String,

        /// Execute the generated Cypher (requires --clickhouse URL)
        #[arg(long)]
        execute: bool,

        /// Output format for execution: table (default), json
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Schema management subcommands
    Schema {
        #[command(subcommand)]
        action: SchemaCommands,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Show the loaded schema in a compact, agent-friendly format
    Show {
        /// Output format: text (default), json
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Validate a schema YAML file (structural check, no ClickHouse needed)
    Validate {
        /// Path to schema YAML file (uses --schema if not provided)
        file: Option<String>,
    },

    /// Discover schema from an existing ClickHouse database using LLM assistance
    Discover {
        /// ClickHouse database to introspect
        #[arg(long)]
        database: String,

        /// ClickHouse URL (falls back to --clickhouse global flag)
        #[arg(long, env = "CG_CLICKHOUSE_URL")]
        clickhouse: Option<String>,

        /// ClickHouse user
        #[arg(long, env = "CG_CLICKHOUSE_USER", default_value = "default")]
        user: String,

        /// ClickHouse password
        #[arg(long, env = "CG_CLICKHOUSE_PASSWORD", default_value = "")]
        password: String,

        /// Output file for the generated schema YAML (stdout if not provided)
        #[arg(long, short)]
        out: Option<String>,
    },

    /// Show the diff between two schema YAML files
    Diff {
        /// First schema file (old)
        old: String,

        /// Second schema file (new)
        new: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = CgConfig::load(&cli.schema, &cli.clickhouse, &cli.ch_user, &cli.ch_password, &cli.ch_database)?;

    match cli.command {
        Commands::Sql { query } => {
            commands::query::run_sql(&query, &cfg)?;
        }

        Commands::Validate { query } => {
            commands::query::run_validate(&query, &cfg)?;
        }

        Commands::Query { query, sql_only, format } => {
            commands::query::run_query(&query, sql_only, &format, &cfg).await?;
        }

        Commands::Nl { description, execute, format } => {
            commands::nl::run_nl(&description, execute, &format, &cfg).await?;
        }

        Commands::Schema { action } => match action {
            SchemaCommands::Show { format } => {
                commands::schema::run_show(&format, &cfg)?;
            }
            SchemaCommands::Validate { file } => {
                let path = file
                    .or_else(|| cfg.schema_path.clone())
                    .ok_or_else(|| anyhow::anyhow!("No schema file specified. Use --schema or provide a file argument."))?;
                commands::schema::run_validate_schema(&path)?;
            }
            SchemaCommands::Discover { database, clickhouse, user, password, out } => {
                let ch_url = clickhouse
                    .or_else(|| cfg.clickhouse_url.clone())
                    .ok_or_else(|| anyhow::anyhow!("No ClickHouse URL. Use --clickhouse or CG_CLICKHOUSE_URL."))?;
                commands::schema::run_discover(&database, &ch_url, &user, &password, out.as_deref(), &cfg).await?;
            }
            SchemaCommands::Diff { old, new } => {
                commands::schema::run_diff(&old, &new)?;
            }
        },
    }

    Ok(())
}
