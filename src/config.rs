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
