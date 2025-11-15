use serde::{Deserialize, Serialize};
use std::env;
use thiserror::Error;
use validator::{Validate, ValidationError};

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_host: "0.0.0.0".to_string(),
            http_port: 8080,
            bolt_host: "0.0.0.0".to_string(),
            bolt_port: 7687,
            bolt_enabled: true,
            max_cte_depth: 100,
            validate_schema: false,
            daemon: false,
        }
    }
}

impl ServerConfig {
    /// Create configuration from environment variables with validation
    pub fn from_env() -> Result<Self, ConfigError> {
        let config = Self {
            http_host: env::var("CLICKGRAPH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            http_port: parse_env_var("CLICKGRAPH_PORT", "8080")?,
            bolt_host: env::var("CLICKGRAPH_BOLT_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            bolt_port: parse_env_var("CLICKGRAPH_BOLT_PORT", "7687")?,
            bolt_enabled: parse_env_var("CLICKGRAPH_BOLT_ENABLED", "true")?,
            max_cte_depth: parse_env_var("CLICKGRAPH_MAX_CTE_DEPTH", "100")?,
            validate_schema: parse_env_var("CLICKGRAPH_VALIDATE_SCHEMA", "false")?,
            daemon: false, // Environment-based config always runs in foreground
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
            daemon: cli.daemon,
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
        self.daemon = other.daemon;
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
    pub daemon: bool,
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
        assert_eq!(config.http_port, 8080);
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
}
