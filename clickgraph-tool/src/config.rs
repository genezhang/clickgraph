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
}

#[derive(Debug, Default, Clone)]
pub struct LlmConfig {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub max_tokens: Option<u32>,
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

        Ok(CgConfig {
            schema_path,
            clickhouse_url,
            ch_user,
            ch_password,
            ch_database,
            dialect,
            llm,
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
