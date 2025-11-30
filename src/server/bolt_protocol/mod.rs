//! Neo4j Bolt Protocol v4.4 Implementation
//!
//! This module implements the Neo4j Bolt protocol for compatibility with
//! Neo4j drivers and tools. The Bolt protocol enables binary communication
//! between Neo4j clients and Brahmand, allowing seamless integration with
//! existing Neo4j ecosystem tools.
//!
//! Reference: Neo4j Bolt Protocol Specification v4.4
//! https://7687.org/bolt/bolt-protocol-message-specification-4.html

use clickhouse::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod auth;
pub mod connection;
pub mod errors;
pub mod handler;
pub mod messages;

use errors::BoltError;

/// Bolt protocol version constants
/// Bolt 5.x versions (5.1 introduced LOGON/LOGOFF messages)
pub const BOLT_VERSION_5_8: u32 = 0x00000508;
pub const BOLT_VERSION_5_7: u32 = 0x00000507;
pub const BOLT_VERSION_5_6: u32 = 0x00000506;
pub const BOLT_VERSION_5_4: u32 = 0x00000504;
pub const BOLT_VERSION_5_3: u32 = 0x00000503;
pub const BOLT_VERSION_5_2: u32 = 0x00000502;
pub const BOLT_VERSION_5_1: u32 = 0x00000501;
pub const BOLT_VERSION_5_0: u32 = 0x00000500;

/// Bolt 4.x versions (4.0 introduced multi-database support)
pub const BOLT_VERSION_4_4: u32 = 0x00000404;
pub const BOLT_VERSION_4_3: u32 = 0x00000403;
pub const BOLT_VERSION_4_2: u32 = 0x00000402;
pub const BOLT_VERSION_4_1: u32 = 0x00000401;

/// Supported Bolt protocol versions in order of preference (5.x first, then 4.x)
pub const SUPPORTED_VERSIONS: &[u32] = &[
    BOLT_VERSION_5_8,
    BOLT_VERSION_5_7,
    BOLT_VERSION_5_6,
    BOLT_VERSION_5_4,
    BOLT_VERSION_5_3,
    BOLT_VERSION_5_2,
    BOLT_VERSION_5_1,
    BOLT_VERSION_5_0,
    BOLT_VERSION_4_4,
    BOLT_VERSION_4_3,
    BOLT_VERSION_4_2,
    BOLT_VERSION_4_1,
];

/// Bolt connection state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection established, waiting for version negotiation
    Connected,
    /// Version negotiated, waiting for HELLO message
    Negotiated(u32),
    /// HELLO received (Bolt 5.1+), waiting for LOGON message
    Authentication(u32),
    /// Authentication completed, ready for queries
    Ready,
    /// Connection is streaming results
    Streaming,
    /// Connection failed or closed
    Failed,
    /// Connection interrupted
    Interrupted,
}

/// Bolt connection context
#[derive(Debug, Clone)]
pub struct BoltContext {
    /// Current connection state
    pub state: ConnectionState,
    /// Negotiated protocol version
    pub version: Option<u32>,
    /// Authenticated user information
    pub user: Option<String>,
    /// Connection metadata
    pub metadata: HashMap<String, String>,
    /// Current transaction ID (if in transaction)
    pub tx_id: Option<String>,
    /// Selected graph schema/database name (defaults to "default")
    pub schema_name: Option<String>,
}

impl Default for BoltContext {
    fn default() -> Self {
        BoltContext {
            state: ConnectionState::Connected,
            version: None,
            user: None,
            metadata: HashMap::new(),
            tx_id: None,
            schema_name: None,
        }
    }
}

impl BoltContext {
    /// Create a new Bolt context
    pub fn new() -> Self {
        Self::default()
    }

    /// Update connection state
    pub fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Set negotiated protocol version
    pub fn set_version(&mut self, version: u32) {
        self.version = Some(version);
        self.state = ConnectionState::Negotiated(version);
    }

    /// Set authenticated user
    pub fn set_user(&mut self, user: String) {
        self.user = Some(user);
        self.state = ConnectionState::Ready;
    }

    /// Check if connection is ready for queries
    pub fn is_ready(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Ready | ConnectionState::Streaming
        )
    }

    /// Check if connection is in transaction
    pub fn is_in_transaction(&self) -> bool {
        self.tx_id.is_some()
    }
}

/// Bolt protocol configuration
#[derive(Debug, Clone)]
pub struct BoltConfig {
    /// Maximum message size (bytes)
    pub max_message_size: usize,
    /// Connection timeout (seconds)
    pub connection_timeout: u64,
    /// Enable authentication
    pub enable_auth: bool,
    /// Default user for unauthenticated connections
    pub default_user: Option<String>,
    /// Server user agent string
    pub server_agent: String,
}

impl Default for BoltConfig {
    fn default() -> Self {
        BoltConfig {
            max_message_size: 65536, // 64KB
            connection_timeout: 30,
            enable_auth: false, // Start with auth disabled for development
            default_user: Some("brahmand".to_string()),
            server_agent: format!("Brahmand/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

/// Main Bolt protocol server
#[derive(Clone)]
pub struct BoltServer {
    /// Server configuration
    pub config: Arc<BoltConfig>,
    /// ClickHouse client for query execution
    clickhouse_client: Client,
}

impl BoltServer {
    /// Create a new Bolt server
    pub fn new(config: BoltConfig, clickhouse_client: Client) -> Self {
        BoltServer {
            config: Arc::new(config),
            clickhouse_client,
        }
    }

    /// Handle a new Bolt connection
    pub async fn handle_connection<S>(&self, stream: S, _peer_addr: String) -> Result<(), BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let context = Arc::new(std::sync::Mutex::new(BoltContext::new()));

        let connection = connection::BoltConnection::new(
            stream,
            context.clone(),
            self.config.clone(),
            self.clickhouse_client.clone(),
        );
        connection.handle().await?;

        Ok(())
    }

    /// Get server configuration
    pub fn config(&self) -> &BoltConfig {
        &self.config
    }
}

/// Utility functions for Bolt protocol
pub mod utils {
    use super::*;

    /// Check if a protocol version is supported
    pub fn is_version_supported(version: u32) -> bool {
        SUPPORTED_VERSIONS.contains(&version)
    }

    /// Get the best supported version from a list of client versions
    /// Supports both exact matching and Bolt 4.3+ range format
    pub fn negotiate_version(client_versions: &[u32]) -> Option<u32> {
        for &client_version in client_versions {
            // Bolt 5.x changed version encoding!
            // Bolt 4.x and earlier: [reserved][range][major][minor]
            // Bolt 5.x and later: [reserved][range][minor][major] (SWAPPED!)

            // Try decoding as Bolt 5.x first (swapped bytes)
            let bolt5_major = client_version & 0xFF;
            let bolt5_minor = (client_version >> 8) & 0xFF;
            let range = (client_version >> 16) & 0xFF;

            // Try decoding as Bolt 4.x (original format)
            let bolt4_minor = client_version & 0xFF;
            let bolt4_major = (client_version >> 8) & 0xFF;

            // Heuristic: If bolt5_major is 5-8 and bolt5_minor is reasonable (0-8),
            // interpret as Bolt 5.x. Otherwise, use Bolt 4.x format.
            let (client_major, client_minor) = if bolt5_major >= 5
                && bolt5_major <= 8
                && bolt5_minor <= 8
            {
                log::debug!(
                    "  Checking client version 0x{:08X}: Bolt 5.x format → major={}, minor={}, range={}",
                    client_version,
                    bolt5_major,
                    bolt5_minor,
                    range
                );
                (bolt5_major, bolt5_minor)
            } else {
                log::debug!(
                    "  Checking client version 0x{:08X}: Bolt 4.x format → major={}, minor={}, range={}",
                    client_version,
                    bolt4_major,
                    bolt4_minor,
                    range
                );
                (bolt4_major, bolt4_minor)
            };

            // Check if any of our supported versions match
            for &server_version in SUPPORTED_VERSIONS {
                // Our server versions use Bolt 4.x format: [00][00][major][minor]
                let server_minor = server_version & 0xFF;
                let server_major = (server_version >> 8) & 0xFF;

                // Same major version?
                if client_major == server_major {
                    // Check if server minor is within client's range
                    // Client wants: minor down to (minor - range)
                    // E.g., client 5.8 with range 8 = accepts 5.8 down to 5.0
                    if server_minor <= client_minor
                        && server_minor >= client_minor.saturating_sub(range)
                    {
                        log::info!(
                            "✅ Negotiation match: Client wants {}.{} (±{}), Server has {}.{} → Negotiated {}",
                            client_major,
                            client_minor,
                            range,
                            server_major,
                            server_minor,
                            version_to_string(server_version)
                        );
                        return Some(server_version);
                    }
                }

                // Also support exact match for backward compatibility
                if client_version == server_version {
                    log::info!(
                        "✅ Exact match: {} → {}",
                        version_to_string(client_version),
                        version_to_string(server_version)
                    );
                    return Some(server_version);
                }
            }
            log::debug!(
                "  ❌ No match found for client version {}.{}",
                client_major,
                client_minor
            );
        }
        log::warn!("❌ Negotiation failed: No compatible version found");
        None
    }

    /// Format version as string for logging
    pub fn version_to_string(version: u32) -> String {
        let major = (version >> 8) & 0xFF;
        let minor = version & 0xFF;
        format!("{}.{}", major, minor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bolt_context_creation() {
        let context = BoltContext::new();
        assert_eq!(context.state, ConnectionState::Connected);
        assert!(context.version.is_none());
        assert!(context.user.is_none());
    }

    #[test]
    fn test_version_negotiation() {
        let client_versions = vec![BOLT_VERSION_4_4, BOLT_VERSION_4_3];
        let negotiated = utils::negotiate_version(&client_versions);
        assert_eq!(negotiated, Some(BOLT_VERSION_4_4));
    }

    #[test]
    fn test_version_string_formatting() {
        assert_eq!(utils::version_to_string(BOLT_VERSION_4_4), "4.4");
        assert_eq!(utils::version_to_string(BOLT_VERSION_4_3), "4.3");
    }

    #[test]
    fn test_bolt_server_creation() {
        let config = BoltConfig::default();
        // Create a test ClickHouse client (won't be used in unit tests)
        let clickhouse_client = Client::default().with_url("http://localhost:8123");
        let _server = BoltServer::new(config, clickhouse_client);
        // Just test that we can create the server
        assert!(true);
    }

    #[test]
    fn test_context_state_transitions() {
        let mut context = BoltContext::new();

        // Test version negotiation
        context.set_version(BOLT_VERSION_4_4);
        assert_eq!(context.state, ConnectionState::Negotiated(BOLT_VERSION_4_4));
        assert_eq!(context.version, Some(BOLT_VERSION_4_4));

        // Test authentication
        context.set_user("test_user".to_string());
        assert_eq!(context.state, ConnectionState::Ready);
        assert_eq!(context.user, Some("test_user".to_string()));
        assert!(context.is_ready());
    }
}
