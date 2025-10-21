//! Neo4j Bolt Protocol v4.4 Implementation
//!
//! This module implements the Neo4j Bolt protocol for compatibility with
//! Neo4j drivers and tools. The Bolt protocol enables binary communication
//! between Neo4j clients and Brahmand, allowing seamless integration with
//! existing Neo4j ecosystem tools.
//!
//! Reference: Neo4j Bolt Protocol Specification v4.4
//! https://7687.org/bolt/bolt-protocol-message-specification-4.html

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod connection;
pub mod messages;
pub mod handler;
pub mod auth;
pub mod errors;

use errors::BoltError;

/// Bolt protocol version constants
pub const BOLT_VERSION_4_4: u32 = 0x00000404;
pub const BOLT_VERSION_4_3: u32 = 0x00000403;
pub const BOLT_VERSION_4_2: u32 = 0x00000402;
pub const BOLT_VERSION_4_1: u32 = 0x00000401;

/// Supported Bolt protocol versions in order of preference
pub const SUPPORTED_VERSIONS: &[u32] = &[
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
}

impl Default for BoltContext {
    fn default() -> Self {
        BoltContext {
            state: ConnectionState::Connected,
            version: None,
            user: None,
            metadata: HashMap::new(),
            tx_id: None,
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
        matches!(self.state, ConnectionState::Ready | ConnectionState::Streaming)
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
#[derive(Debug)]
pub struct BoltServer {
    /// Server configuration
    pub config: Arc<BoltConfig>,
    /// Active connections
    connections: HashMap<String, Arc<Mutex<BoltContext>>>,
}

impl BoltServer {
    /// Create a new Bolt server
    pub fn new(config: BoltConfig) -> Self {
        BoltServer {
            config: Arc::new(config),
            connections: HashMap::new(),
        }
    }

    /// Handle a new Bolt connection
    pub async fn handle_connection<S>(&mut self, stream: S, peer_addr: String) -> Result<(), BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let context = Arc::new(std::sync::Mutex::new(BoltContext::new()));
        self.connections.insert(peer_addr.clone(), context.clone());

        let connection = connection::BoltConnection::new(stream, context.clone(), self.config.clone());
        connection.handle().await?;

        self.connections.remove(&peer_addr);
        Ok(())
    }

    /// Get number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.len()
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
    pub fn negotiate_version(client_versions: &[u32]) -> Option<u32> {
        for &server_version in SUPPORTED_VERSIONS {
            if client_versions.contains(&server_version) {
                return Some(server_version);
            }
        }
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
        let server = BoltServer::new(config);
        assert_eq!(server.connection_count(), 0);
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
