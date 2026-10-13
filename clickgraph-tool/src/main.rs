use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

mod commands;
mod config;
mod llm;
mod schema_fmt;

use config::CgConfig;

#[derive(Copy, Clone, Debug, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
pub enum DialectArg {
    #[default]
    Clickhouse,
    Databricks,
}

impl DialectArg {
    pub fn to_sql_dialect(self) -> clickgraph_embedded::SqlDialect {
        match self {
            DialectArg::Clickhouse => clickgraph_embedded::SqlDialect::ClickHouse,
            DialectArg::Databricks => clickgraph_embedded::SqlDialect::Databricks,
        }
    }
}

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
    #[arg(long, env = "CG_CLICKHOUSE_USER", global = true)]
    ch_user: Option<String>,

    /// ClickHouse password
    #[arg(long, env = "CG_CLICKHOUSE_PASSWORD", global = true)]
    ch_password: Option<String>,

    /// ClickHouse database to query
    #[arg(long, env = "CG_CLICKHOUSE_DATABASE", global = true)]
    ch_database: Option<String>,

    /// Target SQL dialect for Cypher translation
    #[arg(long, env = "CG_DIALECT", global = true, value_enum)]
    dialect: Option<DialectArg>,

    /// Fail a query reading a property of a node embedded in edge tables
    /// when the rows of one node id hold several values of it (#531);
    /// without it any one of the values is read
    #[arg(
        long,
        env = "CG_CHECK_NODE_PROPERTIES",
        global = true,
        value_parser = clap::builder::FalseyValueParser::new()
    )]
    check_node_properties: bool,

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

    /// Validate a schema YAML file (structural check, no ClickHouse needed;
    /// `--data` also checks the data in ClickHouse)
    Validate {
        /// Path to schema YAML file (uses --schema if not provided)
        file: Option<String>,

        /// Also check, in ClickHouse (--clickhouse), that the rows of each
        /// node embedded in edge tables hold one value of each property
        /// per node id (a query reads any one of such a property's values)
        #[arg(long)]
        data: bool,

        /// With --data: check one node id in N (all rows of each) …
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
        sample: u64,

        /// … reading at most this many rows of each table
        #[arg(long, default_value_t = 1_000_000, value_parser = clap::value_parser!(u64).range(1..))]
        max_rows: u64,

        /// With --data: check every node id and every row
        #[arg(long)]
        full: bool,
    },

    /// Discover schema via LLM assistance. Default targets ClickHouse;
    /// with `--dialect databricks` targets a Databricks SQL Warehouse
    /// catalog/schema (requires the `databricks` build feature).
    Discover {
        /// Database (ClickHouse) or schema name within a catalog (Databricks)
        #[arg(long)]
        database: String,

        /// Databricks catalog (required for --dialect databricks;
        /// falls back to DATABRICKS_CATALOG / CG_DATABRICKS_CATALOG).
        /// Ignored for ClickHouse.
        #[arg(long)]
        catalog: Option<String>,

        /// ClickHouse URL (falls back to --clickhouse global flag).
        /// Ignored for --dialect databricks.
        #[arg(long, env = "CG_CLICKHOUSE_URL")]
        clickhouse: Option<String>,

        /// ClickHouse user. Ignored for --dialect databricks.
        #[arg(long, env = "CG_CLICKHOUSE_USER", default_value = "default")]
        user: String,

        /// ClickHouse password. Ignored for --dialect databricks.
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
    clickgraph::server::query_context::set_check_node_properties(cli.check_node_properties);
    let cfg = CgConfig::load(
        &cli.schema,
        &cli.clickhouse,
        cli.ch_user.as_deref(),
        cli.ch_password.as_deref(),
        &cli.ch_database,
        cli.dialect,
    )?;

    match cli.command {
        Commands::Sql { query } => {
            commands::query::run_sql(&query, &cfg)?;
        }

        Commands::Validate { query } => {
            commands::query::run_validate(&query, &cfg)?;
        }

        Commands::Query {
            query,
            sql_only,
            format,
        } => {
            commands::query::run_query(&query, sql_only, &format, &cfg).await?;
        }

        Commands::Nl {
            description,
            execute,
            format,
        } => {
            commands::nl::run_nl(&description, execute, &format, &cfg).await?;
        }

        Commands::Schema { action } => match action {
            SchemaCommands::Show { format } => {
                commands::schema::run_show(&format, &cfg)?;
            }
            SchemaCommands::Validate {
                file,
                data,
                sample,
                max_rows,
                full,
            } => {
                let path = file.or_else(|| cfg.schema_path.clone()).ok_or_else(|| {
                    anyhow::anyhow!(
                        "No schema file specified. Use --schema or provide a file argument."
                    )
                })?;
                commands::schema::run_validate_schema(&path)?;
                if data {
                    let sampling = match full {
                        true => None,
                        false => Some(clickgraph::graph_catalog::node_property_check::Sampling {
                            one_in: sample,
                            max_rows,
                        }),
                    };
                    commands::schema::run_validate_data(&path, sampling, &cfg).await?;
                }
            }
            SchemaCommands::Discover {
                database,
                catalog,
                clickhouse,
                user,
                password,
                out,
            } => {
                // For ClickHouse the URL is required; for Databricks it's
                // unused (the executor reads DATABRICKS_HOST etc.). Resolve
                // it lazily so a missing CG_CLICKHOUSE_URL doesn't block
                // the Databricks path.
                let ch_url = match cfg.dialect {
                    DialectArg::Clickhouse => clickhouse
                        .or_else(|| cfg.clickhouse_url.clone())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "No ClickHouse URL. Use --clickhouse or CG_CLICKHOUSE_URL."
                            )
                        })?,
                    DialectArg::Databricks => String::new(),
                };
                commands::schema::run_discover(
                    &database,
                    catalog.as_deref(),
                    &ch_url,
                    &user,
                    &password,
                    out.as_deref(),
                    &cfg,
                )
                .await?;
            }
            SchemaCommands::Diff { old, new } => {
                commands::schema::run_diff(&old, &new)?;
            }
        },
    }

    Ok(())
}
