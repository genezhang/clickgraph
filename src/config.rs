use serde::{Deserialize, Serialize};
use std::env;
use thiserror::Error;
use validator::Validate;

/// Configuration errors
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),

    #[error("Parse error for {field}: {value} - {source}")]
    Parse {
        field: String,
        value: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Validation error: {0}")]
    Validation(#[from] validator::ValidationErrors),
}

/// Server configuration with validation
#[derive(Clone, Debug, Validate, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP server host address
    #[validate(length(min = 1, message = "HTTP host cannot be empty"))]
    pub http_host: String,

    /// HTTP server port (1-65535)
    #[validate(range(
        min = 1,
        max = 65535,
        message = "HTTP port must be between 1 and 65535"
    ))]
    pub http_port: u16,

    /// Bolt server host address
    #[validate(length(min = 1, message = "Bolt host cannot be empty"))]
    pub bolt_host: String,

    /// Bolt server port (1-65535)
    #[validate(range(
        min = 1,
        max = 65535,
        message = "Bolt port must be between 1 and 65535"
    ))]
    pub bolt_port: u16,

    /// Whether Bolt protocol server is enabled
    pub bolt_enabled: bool,

    /// Maximum recursive CTE evaluation depth for variable-length paths
    #[validate(range(
        min = 1,
        max = 1000,
        message = "Max CTE depth must be between 1 and 1000"
    ))]
    pub max_cte_depth: u32,

    /// Whether to validate YAML schema against ClickHouse tables on startup
    pub validate_schema: bool,

    /// Whether to run server in daemon mode
    pub daemon: bool,

    /// Neo4j compatibility mode - masquerade as Neo4j server for tool compatibility
    /// Useful for graph-notebook, Neodash, and other Neo4j ecosystem tools
    pub neo4j_compat_mode: bool,

    /// Run in embedded mode using in-process chdb instead of a remote ClickHouse server.
    /// When true, `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` are not required.
    /// Requires the `embedded` feature.
    pub embedded: bool,

    /// Execute against a Databricks SQL Warehouse instead of ClickHouse.
    /// When true, the server reads `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`,
    /// `DATABRICKS_TOKEN` env vars and routes queries through `DatabricksSqlExecutor`
    /// with Spark SQL emission. Requires the `databricks` feature.
    /// Mutually exclusive with `embedded`.
    pub databricks: bool,

    /// Per-query timeout in seconds for HTTP requests (covers parsing + planning + execution).
    /// 0 = no timeout. Default: 300 (5 minutes).
    pub query_timeout_secs: u64,

    /// Maximum HTTP request body size in bytes. Default: 1 MB.
    pub max_request_body_bytes: usize,

    /// Maximum concurrent queries. 0 = unlimited. Default: 64.
    pub max_concurrent_queries: usize,

    /// Whether the observability registry and `/metrics` /`/stats` endpoints
    /// are enabled. When false the endpoints return 404 and recording is a
    /// cheap no-op. Default: true.
    pub metrics_enabled: bool,

    /// Capacity of the in-memory slow-query ring buffer surfaced by
    /// `/stats/queries`. Default: 128.
    #[validate(range(
        min = 1,
        max = 10000,
        message = "Slow-query capacity must be between 1 and 10000"
    ))]
    pub slow_query_capacity: usize,

    /// Only queries whose total time is at least this many milliseconds are
    /// pushed into the slow-query ring. 0 = record every query (the ring then
    /// retains the most recent N). Default: 0.
    pub slow_query_threshold_ms: u64,

    /// Capture true ClickHouse-side execution stats (read_rows/read_bytes/
    /// elapsed) by reading the `X-ClickHouse-Summary` response header via a
    /// direct request path. Off by default — the default path uses the
    /// `clickhouse` crate, which does not expose that header. Remote mode only.
    pub metrics_ch_summary: bool,

    /// Include a truncated preview of the Cypher text in the slow-query ring
    /// (JSON only, never a Prometheus label). Off by default for environments
    /// where query text may carry sensitive values.
    pub metrics_query_preview: bool,

    /// S1 stats-informed planning (`docs/design/STATS_PLANNING.md`): fetch
    /// per-table row counts from `system.tables` and let the planner rank
    /// semantically-equivalent anchor/FROM choices by table size. Ordering
    /// only — never row membership (PRIORITIES.md §1.7). Off by default so
    /// sql_only/test paths stay byte-deterministic. Remote ClickHouse mode
    /// only in this slice.
    #[serde(default)]
    pub stats_enabled: bool,

    /// TTL in seconds for the cached table row counts
    /// (`CLICKGRAPH_STATS_TTL_SECS`). Default: 300.
    #[serde(default = "default_stats_ttl_secs")]
    pub stats_ttl_secs: u64,

    /// Whether a query reading a property of a node embedded in edge tables
    /// checks that the rows of each node id hold one value of it, failing
    /// when they hold several (`CLICKGRAPH_CHECK_NODE_PROPERTIES`, #531).
    /// Off by default: any one of the values is read.
    #[serde(default)]
    pub check_node_properties: bool,

    /// Where runtime schema registrations survive restarts
    /// (`CLICKGRAPH_SCHEMA_PERSISTENCE`): `off` (default — this feature
    /// writes nothing anywhere, exactly the pre-feature behavior) or
    /// `clickhouse` (registrations are stored as source rows in a
    /// `clickgraph_schema_registry` table in the configured database and
    /// replayed at boot, config-defined names winning; the Kafka Connect
    /// config-topic pattern). Server modes that run without a ClickHouse
    /// client have none to store to, so `clickhouse` degrades to inert
    /// there.
    #[serde(default)]
    pub schema_persistence: SchemaPersistenceMode,

    /// Boot-replay row budget (`CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS`): the
    /// oldest-first rows restored at most, everything beyond it skipped with
    /// an exact WARN. Bounds how much durable, API-written state one boot
    /// re-plays. Default: 500. 0 restores nothing (the store is still read;
    /// the WARN accounts every row).
    pub schema_replay_max_rows: usize,

    /// Boot-replay payload budget in bytes
    /// (`CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES`): restored rows' cumulative
    /// payload stops before exceeding it (deterministic oldest-first
    /// cutoff, exact WARN about the remainder). Default: 64 MiB.
    pub schema_replay_max_bytes: u64,

    /// OTLP/HTTP base URL for opt-in per-query tracing
    /// (`CLICKGRAPH_OTLP_ENDPOINT`, traces go to `{endpoint}/v1/traces`).
    /// Unset = tracing off (default): no traffic, no per-query cost.
    /// Tracing rides on the observability registry, so it is also off when
    /// `CLICKGRAPH_METRICS_ENABLED=false`.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Include the literals-stripped Cypher text in OTLP spans
    /// (`CLICKGRAPH_OTLP_INCLUDE_QUERY=1`). Off by default: spans carry
    /// shape/timings only, never query text.
    #[serde(default)]
    pub otlp_include_query: bool,

    /// Static auth/tenant headers for OTLP exports, comma-separated
    /// `key=value` (OTEL's `OTEL_EXPORTER_OTLP_HEADERS` form), e.g.
    /// `CLICKGRAPH_OTLP_HEADERS=Authorization=Basic <base64(pk:sk)>`.
    /// Unset = no extra headers (open collectors).
    #[serde(default)]
    pub otlp_headers: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_host: "0.0.0.0".to_string(),
            http_port: 7475,
            bolt_host: "0.0.0.0".to_string(),
            bolt_port: 7687,
            bolt_enabled: true,
            max_cte_depth: 100,
            validate_schema: false,
            daemon: false,
            neo4j_compat_mode: false,
            embedded: false,
            databricks: false,
            query_timeout_secs: 300,
            max_request_body_bytes: 1_048_576, // 1 MB
            max_concurrent_queries: 64,
            metrics_enabled: true,
            slow_query_capacity: 128,
            slow_query_threshold_ms: 0,
            metrics_ch_summary: false,
            metrics_query_preview: false,
            stats_enabled: false,
            stats_ttl_secs: 300,
            check_node_properties: false,
            schema_persistence: SchemaPersistenceMode::Off,
            schema_replay_max_rows: 500,
            schema_replay_max_bytes: 67_108_864, // 64 MiB
            otlp_endpoint: None,
            otlp_include_query: false,
            otlp_headers: String::new(),
        }
    }
}

impl ServerConfig {
    /// Create configuration from environment variables with validation
    pub fn from_env() -> Result<Self, ConfigError> {
        let config = Self {
            http_host: env::var("CLICKGRAPH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            http_port: parse_env_var("CLICKGRAPH_PORT", "7475")?,
            bolt_host: env::var("CLICKGRAPH_BOLT_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            bolt_port: parse_env_var("CLICKGRAPH_BOLT_PORT", "7687")?,
            bolt_enabled: parse_env_var("CLICKGRAPH_BOLT_ENABLED", "true")?,
            max_cte_depth: parse_env_var("CLICKGRAPH_MAX_CTE_DEPTH", "100")?,
            validate_schema: parse_env_var("CLICKGRAPH_VALIDATE_SCHEMA", "false")?,
            daemon: false, // Environment-based config always runs in foreground
            neo4j_compat_mode: parse_env_var("CLICKGRAPH_NEO4J_COMPAT_MODE", "false")?,
            embedded: parse_env_var("CLICKGRAPH_EMBEDDED", "false")?,
            databricks: parse_env_var("CLICKGRAPH_DATABRICKS", "false")?,
            query_timeout_secs: parse_env_var("CLICKGRAPH_QUERY_TIMEOUT_SECS", "300")?,
            max_request_body_bytes: parse_env_var("CLICKGRAPH_MAX_REQUEST_BODY_BYTES", "1048576")?,
            max_concurrent_queries: parse_env_var("CLICKGRAPH_MAX_CONCURRENT_QUERIES", "64")?,
            metrics_enabled: parse_env_var("CLICKGRAPH_METRICS_ENABLED", "true")?,
            slow_query_capacity: parse_env_var("CLICKGRAPH_SLOW_QUERY_CAPACITY", "128")?,
            slow_query_threshold_ms: parse_env_var("CLICKGRAPH_SLOW_QUERY_THRESHOLD_MS", "0")?,
            metrics_ch_summary: parse_env_var("CLICKGRAPH_METRICS_CH_SUMMARY", "false")?,
            metrics_query_preview: parse_env_var("CLICKGRAPH_METRICS_QUERY_PREVIEW", "false")?,
            stats_enabled: parse_env_var("CLICKGRAPH_STATS_ENABLED", "false")?,
            stats_ttl_secs: parse_env_var("CLICKGRAPH_STATS_TTL_SECS", "300")?,
            check_node_properties: parse_env_var("CLICKGRAPH_CHECK_NODE_PROPERTIES", "false")?,
            schema_persistence: parse_env_var("CLICKGRAPH_SCHEMA_PERSISTENCE", "off")?,
            schema_replay_max_rows: parse_env_var("CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS", "500")?,
            schema_replay_max_bytes: parse_env_var(
                "CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES",
                "67108864",
            )?,
            otlp_endpoint: env::var("CLICKGRAPH_OTLP_ENDPOINT")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            otlp_include_query: parse_env_flag("CLICKGRAPH_OTLP_INCLUDE_QUERY", false)?,
            otlp_headers: env::var("CLICKGRAPH_OTLP_HEADERS").unwrap_or_default(),
        };

        config.validate()?;
        Ok(config)
    }

    /// Create configuration from CLI arguments with validation
    pub fn from_cli(cli: CliConfig) -> Result<Self, ConfigError> {
        let config = Self {
            http_host: cli.http_host,
            http_port: cli.http_port,
            bolt_host: cli.bolt_host,
            bolt_port: cli.bolt_port,
            bolt_enabled: cli.bolt_enabled,
            max_cte_depth: cli.max_cte_depth,
            validate_schema: cli.validate_schema,
            neo4j_compat_mode: cli.neo4j_compat_mode,
            daemon: cli.daemon,
            embedded: cli.embedded,
            databricks: cli.databricks,
            query_timeout_secs: cli.query_timeout_secs,
            max_request_body_bytes: cli.max_request_body_bytes,
            max_concurrent_queries: cli.max_concurrent_queries,
            // Metrics knobs are operational and env-only (no CLI flag); read
            // them from the environment so `from_cli` (the live startup path)
            // still honors CLICKGRAPH_METRICS_* / CLICKGRAPH_SLOW_QUERY_*.
            metrics_enabled: parse_env_var("CLICKGRAPH_METRICS_ENABLED", "true")?,
            slow_query_capacity: parse_env_var("CLICKGRAPH_SLOW_QUERY_CAPACITY", "128")?,
            slow_query_threshold_ms: parse_env_var("CLICKGRAPH_SLOW_QUERY_THRESHOLD_MS", "0")?,
            metrics_ch_summary: parse_env_var("CLICKGRAPH_METRICS_CH_SUMMARY", "false")?,
            metrics_query_preview: parse_env_var("CLICKGRAPH_METRICS_QUERY_PREVIEW", "false")?,
            // Stats knobs are operational and env-only, like the metrics knobs.
            stats_enabled: parse_env_var("CLICKGRAPH_STATS_ENABLED", "false")?,
            stats_ttl_secs: parse_env_var("CLICKGRAPH_STATS_TTL_SECS", "300")?,
            check_node_properties: parse_env_var("CLICKGRAPH_CHECK_NODE_PROPERTIES", "false")?,
            schema_persistence: parse_env_var("CLICKGRAPH_SCHEMA_PERSISTENCE", "off")?,
            // Replay budgets are operational and env-only, like the metrics
            // and stats knobs.
            schema_replay_max_rows: parse_env_var("CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS", "500")?,
            schema_replay_max_bytes: parse_env_var(
                "CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES",
                "67108864",
            )?,
            // Tracing knobs are operational and env-only, like the metrics knobs.
            otlp_endpoint: env::var("CLICKGRAPH_OTLP_ENDPOINT")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            otlp_include_query: parse_env_flag("CLICKGRAPH_OTLP_INCLUDE_QUERY", false)?,
            otlp_headers: env::var("CLICKGRAPH_OTLP_HEADERS").unwrap_or_default(),
        };

        config.validate()?;
        Ok(config)
    }

    /// Create configuration from YAML file
    pub fn from_yaml_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Parse {
            field: "yaml_file".to_string(),
            value: "file read failed".to_string(),
            source: Box::new(e),
        })?;

        let config: Self = serde_yaml::from_str(&content).map_err(|e| ConfigError::Parse {
            field: "yaml_content".to_string(),
            value: content,
            source: Box::new(e),
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Merge with another configuration (CLI overrides environment)
    pub fn merge(&mut self, other: Self) {
        self.http_host = other.http_host;
        self.http_port = other.http_port;
        self.bolt_host = other.bolt_host;
        self.bolt_port = other.bolt_port;
        self.bolt_enabled = other.bolt_enabled;
        self.max_cte_depth = other.max_cte_depth;
        self.validate_schema = other.validate_schema;
        self.neo4j_compat_mode = other.neo4j_compat_mode;
        self.daemon = other.daemon;
        self.embedded = other.embedded;
        self.databricks = other.databricks;
        self.query_timeout_secs = other.query_timeout_secs;
        self.max_request_body_bytes = other.max_request_body_bytes;
        self.max_concurrent_queries = other.max_concurrent_queries;
        self.metrics_enabled = other.metrics_enabled;
        self.slow_query_capacity = other.slow_query_capacity;
        self.slow_query_threshold_ms = other.slow_query_threshold_ms;
        self.metrics_ch_summary = other.metrics_ch_summary;
        self.metrics_query_preview = other.metrics_query_preview;
        self.stats_enabled = other.stats_enabled;
        self.stats_ttl_secs = other.stats_ttl_secs;
        self.check_node_properties = other.check_node_properties;
        self.schema_persistence = other.schema_persistence;
        self.schema_replay_max_rows = other.schema_replay_max_rows;
        self.schema_replay_max_bytes = other.schema_replay_max_bytes;
    }
}

/// CLI configuration (parsed from command line arguments)
#[derive(Clone, Debug)]
pub struct CliConfig {
    pub http_host: String,
    pub http_port: u16,
    pub bolt_host: String,
    pub bolt_port: u16,
    pub bolt_enabled: bool,
    pub max_cte_depth: u32,
    pub validate_schema: bool,
    pub neo4j_compat_mode: bool,
    pub daemon: bool,
    pub embedded: bool,
    pub databricks: bool,
    pub query_timeout_secs: u64,
    pub max_request_body_bytes: usize,
    pub max_concurrent_queries: usize,
}

/// serde default for `ServerConfig::stats_ttl_secs` (YAML-file config path).
fn default_stats_ttl_secs() -> u64 {
    300
}

/// Parse an environment variable with a default value
fn parse_env_var<T: std::str::FromStr>(key: &str, default: &str) -> Result<T, ConfigError>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    value.parse().map_err(|e| ConfigError::Parse {
        field: key.to_string(),
        value,
        source: Box::new(e),
    })
}

/// Parse a boolean flag leniently: `true`/`false` plus `1`/`0`, `yes`/`no`,
/// `on`/`off`, case-insensitively. Used for opt-in observability knobs, where
/// `str::parse::<bool>` would reject `1` and — because these are read during
/// startup — take the whole server down over a tracing flag.
fn parse_env_flag(key: &str, default: bool) -> Result<bool, ConfigError> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(ConfigError::Parse {
            field: key.to_string(),
            value: other.to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected one of: true/false, 1/0, yes/no, on/off",
            )),
        }),
    }
}

/// How runtime schema registrations survive restarts
/// (`CLICKGRAPH_SCHEMA_PERSISTENCE`, spec: `docs/design/SCHEMA_PERSISTENCE.md`).
///
/// A word enum parsed *strictly* (via `FromStr`, so a typo fails startup
/// rather than silently leaving persistence off): a durability setting that
/// lies to its operator is worse than a startup error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaPersistenceMode {
    /// Nothing is ever written; HTTP registrations behave exactly as before
    /// the feature existed (lost on restart).
    #[default]
    Off,
    /// Registrations ride in a `clickgraph_schema_registry` table in the
    /// configured ClickHouse database and are replayed at boot, with
    /// config-defined names winning (Kafka Connect's `config.storage.topic`
    /// pattern).
    Clickhouse,
}

impl std::str::FromStr for SchemaPersistenceMode {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "clickhouse" => Ok(Self::Clickhouse),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected one of: off, clickhouse",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.http_port, 7475);
        assert_eq!(config.bolt_port, 7687);
        assert!(config.bolt_enabled);
    }

    #[test]
    fn otlp_include_query_accepts_numeric_and_wordy_flags() {
        // Read at startup, so an unparseable value must not be a reason to
        // refuse to boot.
        for truthy in ["1", "true", "TRUE", "yes", "on"] {
            unsafe { env::set_var("CLICKGRAPH_OTLP_INCLUDE_QUERY", truthy) };
            assert!(
                ServerConfig::from_env().expect("starts").otlp_include_query,
                "{truthy} should enable"
            );
        }
        for falsy in ["0", "false", "no", "off", ""] {
            unsafe { env::set_var("CLICKGRAPH_OTLP_INCLUDE_QUERY", falsy) };
            assert!(
                !ServerConfig::from_env().expect("starts").otlp_include_query,
                "{falsy:?} should disable"
            );
        }
        unsafe { env::set_var("CLICKGRAPH_OTLP_INCLUDE_QUERY", "maybe") };
        assert!(
            ServerConfig::from_env().is_err(),
            "nonsense is reported rather than silently ignored"
        );
        unsafe { env::remove_var("CLICKGRAPH_OTLP_INCLUDE_QUERY") };
    }

    #[test]
    fn schema_persistence_parses_strictly() {
        // Durability has no lenient aliases ("on"/"true" must not quietly
        // mean a different backend — a persistence setting that means
        // something other than it says is a silent data-loss knob), and
        // unset means Off. Tested on FromStr directly rather than through
        // from_env: env-touching tests race each other on the process-global
        // environment (see otlp_include_query_...), and this parse is the
        // exact one parse_env_var delegates to. `str::parse` is inherent, so
        // no FromStr import is needed here.
        assert_eq!(
            "off".parse::<SchemaPersistenceMode>().expect("parses"),
            SchemaPersistenceMode::Off
        );
        assert_eq!(
            "clickhouse"
                .parse::<SchemaPersistenceMode>()
                .expect("parses"),
            SchemaPersistenceMode::Clickhouse
        );
        assert_eq!(
            "ClickHouse"
                .parse::<SchemaPersistenceMode>()
                .expect("case-insensitive enum value"),
            SchemaPersistenceMode::Clickhouse
        );
        assert_eq!(
            " off ".parse::<SchemaPersistenceMode>().expect("trims"),
            SchemaPersistenceMode::Off
        );
        assert!(
            "true".parse::<SchemaPersistenceMode>().is_err(),
            "a truthy flag is not a backend: refuse rather than guess"
        );
        assert!("yes".parse::<SchemaPersistenceMode>().is_err());
        assert!("ON".parse::<SchemaPersistenceMode>().is_err());
        assert!("".parse::<SchemaPersistenceMode>().is_err());
        assert_eq!(SchemaPersistenceMode::default(), SchemaPersistenceMode::Off);
    }

    #[test]
    fn schema_replay_budget_defaults_are_operator_dialable() {
        // The budgets bound how much durable, API-written state one boot
        // re-plays; their defaults are the shipped worst case. Pinned here
        // (not through from_env: env tests race the process-global
        // environment) so a silent change of the default boot cost fails.
        let config = ServerConfig::default();
        assert_eq!(config.schema_replay_max_rows, 500);
        assert_eq!(config.schema_replay_max_bytes, 67_108_864);
    }

    #[test]
    fn test_invalid_port_range() {
        let config = ServerConfig {
            http_port: 0, // Invalid
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_cte_depth() {
        let config = ServerConfig {
            max_cte_depth: 1001, // Invalid (> 1000)
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_host() {
        let config = ServerConfig {
            http_host: "".to_string(), // Invalid
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_metrics_defaults() {
        let config = ServerConfig::default();
        assert!(config.metrics_enabled);
        assert_eq!(config.slow_query_capacity, 128);
        assert_eq!(config.slow_query_threshold_ms, 0);
        assert!(!config.metrics_ch_summary);
        assert!(!config.metrics_query_preview);
    }

    #[test]
    fn test_invalid_slow_query_capacity() {
        let config = ServerConfig {
            slow_query_capacity: 10_001, // Invalid (> 10000)
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
