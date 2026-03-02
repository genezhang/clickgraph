//! Role-based ClickHouse connection pool manager
//!
//! Maintains separate connection pools for different roles to avoid
//! SET ROLE overhead and ensure proper role isolation.
//!
//! When `CLICKHOUSE_CLUSTER` is set, discovers cluster nodes from
//! `system.clusters` and round-robins queries across them.

use clickhouse::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages multiple connection pools, one per role + default.
/// When cluster mode is active, round-robins across discovered nodes.
pub struct RoleConnectionPool {
    default_clients: Vec<Client>,
    role_clients: Arc<RwLock<HashMap<String, Vec<Client>>>>,
    base_config: ConnectionConfig,
    round_robin: AtomicUsize,
}

#[derive(Clone)]
struct ConnectionConfig {
    urls: Vec<String>,
    cluster_name: Option<String>,
    user: String,
    password: String,
    database: String,
    max_cte_depth: u32,
}

impl RoleConnectionPool {
    /// Create a new role-based connection pool.
    /// If `CLICKHOUSE_CLUSTER` is set, queries the seed node to discover cluster members.
    pub async fn new(max_cte_depth: u32) -> Result<Self, String> {
        let mut config = ConnectionConfig::from_env(max_cte_depth)?;

        // If cluster mode, discover nodes from the seed URL
        if config.cluster_name.is_some() {
            config.discover_cluster_nodes().await;
        }

        let default_clients: Vec<Client> = config
            .urls
            .iter()
            .map(|url| config.create_client_for_url(url, None))
            .collect();

        let node_count = default_clients.len();
        let cluster_info = if let Some(ref name) = config.cluster_name {
            format!("Cluster mode: {} nodes for cluster '{}'", node_count, name)
        } else {
            "Single-node mode".to_string()
        };
        log::info!("{}", cluster_info);

        Ok(Self {
            default_clients,
            role_clients: Arc::new(RwLock::new(HashMap::new())),
            base_config: config,
            round_robin: AtomicUsize::new(0),
        })
    }

    /// Get a client for the specified role (or default if None).
    /// Round-robins across cluster nodes when multiple URLs are configured.
    ///
    /// This method:
    /// 1. Picks a node index via round-robin
    /// 2. Returns default pool if role is None
    /// 3. Checks if role pool exists (fast path - read lock)
    /// 4. Creates new role pool if needed (slow path - write lock)
    ///
    /// Role pools are lazy-initialized on first use.
    pub async fn get_client(&self, role: Option<&str>) -> Client {
        let idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % self.default_clients.len();

        let Some(role) = role else {
            return self.default_clients[idx].clone();
        };

        // Fast path: check if role pool exists (read lock)
        {
            let pools = self.role_clients.read().await;
            if let Some(clients) = pools.get(role) {
                return clients[idx].clone();
            }
        }

        // Slow path: create new role pools for all URLs (write lock)
        let mut pools = self.role_clients.write().await;

        // Double-check after acquiring write lock (another thread might have created it)
        if let Some(clients) = pools.get(role) {
            return clients[idx].clone();
        }

        // Create clients with role for all URLs
        log::info!("Creating new connection pool for role: {}", role);
        let clients: Vec<Client> = self
            .base_config
            .urls
            .iter()
            .map(|url| self.base_config.create_client_for_url(url, Some(role)))
            .collect();
        let client = clients[idx].clone();
        pools.insert(role.to_string(), clients);

        client
    }

    /// Get statistics about pool usage
    pub async fn stats(&self) -> PoolStats {
        let pools = self.role_clients.read().await;
        PoolStats {
            total_role_pools: pools.len(),
            roles: pools.keys().cloned().collect(),
            node_count: self.default_clients.len(),
            cluster_name: self.base_config.cluster_name.clone(),
        }
    }
}

#[derive(Debug)]
pub struct PoolStats {
    pub total_role_pools: usize,
    pub roles: Vec<String>,
    pub node_count: usize,
    pub cluster_name: Option<String>,
}

impl ConnectionConfig {
    fn from_env(max_cte_depth: u32) -> Result<Self, String> {
        let url = env::var("CLICKHOUSE_URL").map_err(|_| "CLICKHOUSE_URL not set".to_string())?;
        let cluster_name = env::var("CLICKHOUSE_CLUSTER").ok();

        Ok(Self {
            urls: vec![url],
            cluster_name,
            user: env::var("CLICKHOUSE_USER").map_err(|_| "CLICKHOUSE_USER not set".to_string())?,
            // Allow empty password for local development
            password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            // Database is optional - defaults to "default". All queries use fully-qualified table names anyway.
            database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string()),
            max_cte_depth,
        })
    }

    /// Discover cluster nodes from `system.clusters` using the seed URL.
    /// Falls back to the seed URL with a warning if discovery fails or returns empty.
    async fn discover_cluster_nodes(&mut self) {
        let cluster_name = match &self.cluster_name {
            Some(name) => name.clone(),
            None => return,
        };

        let seed_url = &self.urls[0];
        log::info!(
            "Discovering cluster '{}' nodes from seed: {}",
            cluster_name,
            seed_url
        );

        #[derive(Debug, clickhouse::Row, Deserialize)]
        struct ClusterNode {
            host_address: String,
            port: u16,
        }

        let client = Client::default()
            .with_url(seed_url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database);

        let query = format!(
            "SELECT host_address, port FROM system.clusters WHERE cluster = '{}' ORDER BY host_address, port",
            cluster_name.replace('\'', "\\'")
        );

        match client.query(&query).fetch_all::<ClusterNode>().await {
            Ok(rows) => {
                let scheme = if seed_url.starts_with("https://") {
                    "https"
                } else {
                    "http"
                };

                let discovered_urls: Vec<String> = rows
                    .into_iter()
                    .map(|node| format!("{}://{}:{}", scheme, node.host_address, node.port))
                    .collect();

                if discovered_urls.is_empty() {
                    log::warn!(
                        "Cluster '{}' returned no nodes, falling back to seed URL",
                        cluster_name
                    );
                } else {
                    log::info!(
                        "Discovered {} nodes for cluster '{}': {:?}",
                        discovered_urls.len(),
                        cluster_name,
                        discovered_urls
                    );
                    self.urls = discovered_urls;
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to discover cluster '{}' nodes: {}. Falling back to seed URL",
                    cluster_name,
                    e
                );
            }
        }
    }

    fn create_client_for_url(&self, url: &str, role: Option<&str>) -> Client {
        let mut client = Client::default()
            .with_url(url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database)
            .with_option("join_use_nulls", "1")
            .with_option("allow_experimental_json_type", "1")
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("output_format_binary_write_json_as_string", "1")
            .with_option(
                "max_recursive_cte_evaluation_depth",
                &self.max_cte_depth.to_string(),
            );

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

        let pool = RoleConnectionPool::new(100).await.unwrap();

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

    #[tokio::test]
    async fn test_single_node_config() {
        unsafe {
            env::set_var("CLICKHOUSE_URL", "http://localhost:8123");
            env::set_var("CLICKHOUSE_USER", "test_user");
            env::set_var("CLICKHOUSE_PASSWORD", "test_pass");
            env::remove_var("CLICKHOUSE_CLUSTER");
        }

        let config = ConnectionConfig::from_env(100).unwrap();
        assert_eq!(config.urls.len(), 1);
        assert_eq!(config.urls[0], "http://localhost:8123");
        assert!(config.cluster_name.is_none());
    }

    #[tokio::test]
    async fn test_cluster_config_env() {
        unsafe {
            env::set_var("CLICKHOUSE_URL", "http://localhost:8123");
            env::set_var("CLICKHOUSE_USER", "test_user");
            env::set_var("CLICKHOUSE_PASSWORD", "test_pass");
            env::set_var("CLICKHOUSE_CLUSTER", "my_cluster");
        }

        let config = ConnectionConfig::from_env(100).unwrap();
        assert_eq!(config.cluster_name, Some("my_cluster".to_string()));
        // URLs still just seed before discovery
        assert_eq!(config.urls.len(), 1);

        // Clean up
        unsafe {
            env::remove_var("CLICKHOUSE_CLUSTER");
        }
    }

    #[tokio::test]
    async fn test_round_robin_distribution() {
        unsafe {
            env::set_var("CLICKHOUSE_URL", "http://localhost:8123");
            env::set_var("CLICKHOUSE_USER", "test_user");
            env::set_var("CLICKHOUSE_PASSWORD", "test_pass");
            env::remove_var("CLICKHOUSE_CLUSTER");
        }

        let mut config = ConnectionConfig::from_env(100).unwrap();
        // Simulate multi-node by adding URLs manually
        config.urls = vec![
            "http://node1:8123".to_string(),
            "http://node2:8123".to_string(),
            "http://node3:8123".to_string(),
        ];

        let default_clients: Vec<Client> = config
            .urls
            .iter()
            .map(|url| config.create_client_for_url(url, None))
            .collect();

        let pool = RoleConnectionPool {
            default_clients,
            role_clients: Arc::new(RwLock::new(HashMap::new())),
            base_config: config,
            round_robin: AtomicUsize::new(0),
        };

        // Verify round-robin cycles through indices
        // We can't inspect which URL a Client uses, but we can verify
        // the counter increments and wraps correctly
        let stats = pool.stats().await;
        assert_eq!(stats.node_count, 3);

        // Call get_client multiple times and verify counter advances
        for _ in 0..9 {
            let _ = pool.get_client(None).await;
        }
        // After 9 calls, counter should be at 9
        assert_eq!(pool.round_robin.load(Ordering::Relaxed), 9);
    }
}
