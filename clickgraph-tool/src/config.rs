use anyhow::Result;
use serde::Deserialize;

use crate::DialectArg;

/// Runtime configuration for cg, resolved from (priority order):
/// 1. CLI flags / env vars
/// 2. ~/.config/cg/config.toml
/// 3. Compiled-in defaults
#[derive(Debug, Default)]
pub struct CgConfig {
    pub schema_path: Option<String>,
    pub clickhouse_url: Option<String>,
    pub ch_user: String,
    pub ch_password: String,
    pub ch_database: Option<String>,
    pub dialect: DialectArg,
    pub llm: LlmConfig,
    // Only read by the `databricks`-feature execution path; populated
    // unconditionally so users see the same warnings/parsing regardless
    // of which build they have, and the resolved struct is identical
    // across feature combinations.
    #[cfg_attr(not(feature = "databricks"), allow(dead_code))]
    pub databricks: DatabricksClientConfig,
}

#[derive(Debug, Default, Clone)]
pub struct LlmConfig {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub max_tokens: Option<u32>,
}

/// Databricks SQL Warehouse credentials and overrides for
/// `cg query --dialect databricks` (the actual executor lives in
/// `clickgraph-embedded` behind the `databricks` feature). Resolution
/// precedence is `CG_DATABRICKS_*` > `DATABRICKS_*` > `[databricks]`
/// section of `~/.config/cg/config.toml`, mirroring the way other
/// fields (LLM, ClickHouse) are sourced.
#[cfg_attr(not(feature = "databricks"), allow(dead_code))]
#[derive(Debug, Default, Clone)]
pub struct DatabricksClientConfig {
    pub hostname: Option<String>,
    pub warehouse_id: Option<String>,
    pub token: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    /// Override the request base URL. Production users leave this
    /// unset (the executor sends to `https://{hostname}`); integration
    /// tests point this at a `wiremock` URL so the same code paths run
    /// against a localhost mock.
    pub base_url: Option<String>,
    /// OAuth M2M (service-principal) credentials. When both are set, OAuth
    /// is used instead of the PAT `token`. From `CG_DATABRICKS_CLIENT_ID` /
    /// `CG_DATABRICKS_CLIENT_SECRET` (or the `DATABRICKS_*` forms).
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

/// Config file format (~/.config/cg/config.toml)
#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    #[serde(default)]
    schema: FileSchemaSection,
    #[serde(default)]
    clickhouse: FileClickHouseSection,
    #[serde(default)]
    dialect: Option<String>,
    #[serde(default)]
    llm: FileLlmSection,
    #[serde(default)]
    databricks: FileDatabricksSection,
}

#[derive(Debug, Deserialize, Default)]
struct FileSchemaSection {
    path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct FileClickHouseSection {
    url: Option<String>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct FileLlmSection {
    provider: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
    base_url: Option<String>,
    max_tokens: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct FileDatabricksSection {
    hostname: Option<String>,
    warehouse_id: Option<String>,
    token: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    base_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
}

impl CgConfig {
    pub fn load(
        schema: &Option<String>,
        clickhouse: &Option<String>,
        ch_user: Option<&str>,
        ch_password: Option<&str>,
        ch_database: &Option<String>,
        dialect: Option<DialectArg>,
    ) -> Result<Self> {
        let file_cfg = load_file_config();

        // Schema path: CLI flag > env (already handled by clap) > config file
        let schema_path = schema.clone().or_else(|| file_cfg.schema.path.clone());

        // ClickHouse URL: CLI flag > env > config file
        let clickhouse_url = clickhouse
            .clone()
            .or_else(|| file_cfg.clickhouse.url.clone());

        // ClickHouse user: explicit CLI/env > config file > "default"
        let ch_user = ch_user
            .or(file_cfg.clickhouse.user.as_deref())
            .unwrap_or("default")
            .to_string();

        // ClickHouse password: explicit CLI/env > config file > ""
        let ch_password = ch_password
            .or(file_cfg.clickhouse.password.as_deref())
            .unwrap_or("")
            .to_string();

        let ch_database = ch_database
            .clone()
            .or_else(|| file_cfg.clickhouse.database.clone());

        // LLM config: env vars > config file > defaults
        let llm = LlmConfig {
            provider: std::env::var("CG_LLM_PROVIDER")
                .ok()
                .or_else(|| std::env::var("CLICKGRAPH_LLM_PROVIDER").ok())
                .or(file_cfg.llm.provider),
            model: std::env::var("CG_LLM_MODEL")
                .ok()
                .or_else(|| std::env::var("CLICKGRAPH_LLM_MODEL").ok())
                .or(file_cfg.llm.model),
            api_key: std::env::var("CG_LLM_API_KEY")
                .ok()
                .or(file_cfg.llm.api_key),
            base_url: std::env::var("CG_LLM_BASE_URL")
                .ok()
                .or_else(|| std::env::var("CLICKGRAPH_LLM_API_URL").ok())
                .or(file_cfg.llm.base_url),
            max_tokens: std::env::var("CG_LLM_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .or(file_cfg.llm.max_tokens),
        };

        // Dialect resolution: CLI flag/env wins, then config file. A
        // present-but-invalid config-file value warns to stderr and falls
        // back to the default — silent ignore was masking typos.
        let dialect = match dialect {
            Some(d) => d,
            None => match file_cfg.dialect.as_deref() {
                Some(s) => parse_dialect(s).unwrap_or_else(|| {
                    eprintln!(
                        "Warning: ignoring unknown `dialect = \"{s}\"` in config.toml \
                         (expected `clickhouse` or `databricks`); using default."
                    );
                    DialectArg::default()
                }),
                None => DialectArg::default(),
            },
        };

        // Databricks resolution: CG_DATABRICKS_* > DATABRICKS_* > config
        // file. The bare `DATABRICKS_*` names match the databricks-cli /
        // databricks-sql-python convention so users can reuse the same
        // env vars they already have set; CG_-prefixed variants exist for
        // callers who want to scope the override to cg alone.
        let databricks = DatabricksClientConfig {
            hostname: std::env::var("CG_DATABRICKS_HOST")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_HOST").ok())
                .or(file_cfg.databricks.hostname),
            warehouse_id: std::env::var("CG_DATABRICKS_WAREHOUSE_ID")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_WAREHOUSE_ID").ok())
                .or(file_cfg.databricks.warehouse_id),
            token: std::env::var("CG_DATABRICKS_TOKEN")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_TOKEN").ok())
                .or(file_cfg.databricks.token),
            catalog: std::env::var("CG_DATABRICKS_CATALOG")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_CATALOG").ok())
                .or(file_cfg.databricks.catalog),
            schema: std::env::var("CG_DATABRICKS_SCHEMA")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_SCHEMA").ok())
                .or(file_cfg.databricks.schema),
            base_url: std::env::var("CG_DATABRICKS_BASE_URL")
                .ok()
                .or(file_cfg.databricks.base_url),
            client_id: std::env::var("CG_DATABRICKS_CLIENT_ID")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_CLIENT_ID").ok())
                .or(file_cfg.databricks.client_id),
            client_secret: std::env::var("CG_DATABRICKS_CLIENT_SECRET")
                .ok()
                .or_else(|| std::env::var("DATABRICKS_CLIENT_SECRET").ok())
                .or(file_cfg.databricks.client_secret),
        };

        Ok(CgConfig {
            schema_path,
            clickhouse_url,
            ch_user,
            ch_password,
            ch_database,
            dialect,
            llm,
            databricks,
        })
    }

    /// Resolve the schema path, returning an error if not set
    pub fn require_schema(&self) -> Result<&str> {
        self.schema_path.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "No schema file specified. Use --schema <file> or set CG_SCHEMA env var."
            )
        })
    }
}

fn parse_dialect(s: &str) -> Option<DialectArg> {
    match s.trim().to_ascii_lowercase().as_str() {
        "clickhouse" | "ch" => Some(DialectArg::Clickhouse),
        "databricks" | "spark" => Some(DialectArg::Databricks),
        _ => None,
    }
}

fn load_file_config() -> FileConfig {
    let Some(config_dir) = dirs::config_dir() else {
        return FileConfig::default();
    };
    let config_path = config_dir.join("cg").join("config.toml");
    let Ok(content) = std::fs::read_to_string(&config_path) else {
        return FileConfig::default();
    };
    toml::from_str(&content).unwrap_or_else(|e| {
        eprintln!("Warning: could not parse {}: {}", config_path.display(), e);
        FileConfig::default()
    })
}
