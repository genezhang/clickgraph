//! Role-based ClickHouse connection pool manager
//!
//! Maintains separate connection pools for different roles to avoid
//! SET ROLE overhead and ensure proper role isolation.

use clickhouse::Client;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages multiple connection pools, one per role + default
pub struct RoleConnectionPool {
    default_client: Client,
    role_clients: Arc<RwLock<HashMap<String, Client>>>,
    base_config: ConnectionConfig,
}

#[derive(Clone)]
struct ConnectionConfig {
    url: String,
    user: String,
    password: String,
    database: String,
}

impl RoleConnectionPool {
    /// Create a new role-based connection pool
    pub fn new() -> Result<Self, String> {
        let config = ConnectionConfig::from_env()?;
        let default_client = config.create_client(None);

        Ok(Self {
            default_client,
            role_clients: Arc::new(RwLock::new(HashMap::new())),
            base_config: config,
        })
    }

    /// Get a client for the specified role (or default if None)
    ///
    /// This method:
    /// 1. Returns default pool if role is None
    /// 2. Checks if role pool exists (fast path - read lock)
    /// 3. Creates new role pool if needed (slow path - write lock)
    ///
    /// Role pools are lazy-initialized on first use.
    pub async fn get_client(&self, role: Option<&str>) -> Client {
        let Some(role) = role else {
            return self.default_client.clone();
        };

        // Fast path: check if role pool exists (read lock)
        {
            let pools = self.role_clients.read().await;
            if let Some(client) = pools.get(role) {
                return client.clone();
            }
        }

        // Slow path: create new role pool (write lock)
        let mut pools = self.role_clients.write().await;

        // Double-check after acquiring write lock (another thread might have created it)
        if let Some(client) = pools.get(role) {
            return client.clone();
        }

        // Create new client with role
        log::info!("Creating new connection pool for role: {}", role);
        let client = self.base_config.create_client(Some(role));
        pools.insert(role.to_string(), client.clone());

        client
    }

    /// Get statistics about pool usage
    pub async fn stats(&self) -> PoolStats {
        let pools = self.role_clients.read().await;
        PoolStats {
            total_role_pools: pools.len(),
            roles: pools.keys().cloned().collect(),
        }
    }
}

#[derive(Debug)]
pub struct PoolStats {
    pub total_role_pools: usize,
    pub roles: Vec<String>,
}

impl ConnectionConfig {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            url: env::var("CLICKHOUSE_URL").map_err(|_| "CLICKHOUSE_URL not set".to_string())?,
            user: env::var("CLICKHOUSE_USER").map_err(|_| "CLICKHOUSE_USER not set".to_string())?,
            // Allow empty password for local development
            password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            database: env::var("CLICKHOUSE_DATABASE")
                .map_err(|_| "CLICKHOUSE_DATABASE not set".to_string())?,
        })
    }

    fn create_client(&self, role: Option<&str>) -> Client {
        let mut client = Client::default()
            .with_url(&self.url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database)
            .with_option("join_use_nulls", "1")
            .with_option("allow_experimental_json_type", "1")
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("output_format_binary_write_json_as_string", "1");

        // Set role for this connection pool via ClickHouse option
        // This adds the role parameter to all HTTP requests from this client
        if let Some(role_name) = role {
            log::debug!("Creating connection pool with role: {}", role_name);
            client = client.with_option("role", role_name);
        }

        client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires ClickHouse connection
    async fn test_role_pool_isolation() {
        // Set up environment
        unsafe {
            env::set_var("CLICKHOUSE_URL", "http://localhost:8123");
            env::set_var("CLICKHOUSE_USER", "test_user");
            env::set_var("CLICKHOUSE_PASSWORD", "test_pass");
            env::set_var("CLICKHOUSE_DATABASE", "test_db");
        }

        let pool = RoleConnectionPool::new().unwrap();

        // Get clients for different roles
        let _default = pool.get_client(None).await;
        let _analyst = pool.get_client(Some("analyst")).await;
        let _admin = pool.get_client(Some("admin")).await;

        // Verify pools exist (can't compare Client instances directly)
        // ClickHouse Client doesn't implement Debug or PartialEq

        // Check stats to verify different roles created different pools
        let stats = pool.stats().await;
        assert_eq!(stats.total_role_pools, 2); // analyst + admin
        assert!(stats.roles.contains(&"analyst".to_string()));
        assert!(stats.roles.contains(&"admin".to_string()));
    }
}
