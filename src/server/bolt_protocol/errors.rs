//! Bolt Protocol Error Types
//!
//! This module defines error types specific to the Bolt protocol implementation.

use thiserror::Error;

/// Bolt protocol error types
#[derive(Error, Debug)]
pub enum BoltError {
    /// IO errors during communication
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol version negotiation failed
    #[error(
        "Version negotiation failed: client versions {client_versions:?}, server supports {server_versions:?}"
    )]
    VersionNegotiationFailed {
        client_versions: Vec<u32>,
        server_versions: Vec<u32>,
    },

    /// Invalid message format
    #[error("Invalid message format: {message}")]
    InvalidMessage { message: String },

    /// Message too large
    #[error("Message too large: {size} bytes, maximum allowed: {max_size} bytes")]
    MessageTooLarge { size: usize, max_size: usize },

    /// Authentication failed
    #[error("Authentication failed for user: {user}")]
    AuthenticationFailed { user: String },

    /// Authorization failed
    #[error("User {user} is not authorized to perform action: {action}")]
    AuthorizationFailed { user: String, action: String },

    /// Invalid connection state for operation
    #[error("Invalid connection state: {current_state}, expected: {expected_state}")]
    InvalidState {
        current_state: String,
        expected_state: String,
    },

    /// Query execution error
    #[error("Query execution error: {message}")]
    QueryError { message: String },

    /// Transaction error
    #[error("Transaction error: {message}")]
    TransactionError { message: String },

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Connection timeout
    #[error("Connection timeout after {timeout_seconds} seconds")]
    ConnectionTimeout { timeout_seconds: u64 },

    /// Protocol error from Neo4j specification
    #[error("Protocol error: {code} - {message}")]
    ProtocolError { code: String, message: String },

    /// Internal server error
    #[error("Internal server error: {message}")]
    Internal { message: String },

    /// Mutex poisoning error (synchronization failure)
    #[error("Mutex poisoning error: {message}")]
    MutexPoisoned { message: String },

    /// Feature not implemented
    #[error("Feature not implemented: {feature}")]
    NotImplemented { feature: String },
}

/// Result type for Bolt operations
pub type BoltResult<T> = Result<T, BoltError>;

impl BoltError {
    /// Documented GQLSTATUS polyfill default for exceptions without a
    /// specific mapping (#1368). Drivers use this exact value when an old
    /// server omits the GQL object, so emitting it keeps us consistent with
    /// what drivers already tolerate.
    pub const GQLSTATUS_DEFAULT: &'static str = "50N42";

    /// Create a new invalid message error
    pub fn invalid_message<S: Into<String>>(message: S) -> Self {
        BoltError::InvalidMessage {
            message: message.into(),
        }
    }

    /// Create a new query error
    pub fn query_error<S: Into<String>>(message: S) -> Self {
        BoltError::QueryError {
            message: message.into(),
        }
    }

    /// Create a new internal error
    pub fn internal<S: Into<String>>(message: S) -> Self {
        BoltError::Internal {
            message: message.into(),
        }
    }

    /// Create a new not implemented error
    pub fn not_implemented<S: Into<String>>(feature: S) -> Self {
        BoltError::NotImplemented {
            feature: feature.into(),
        }
    }

    /// Create a new mutex poisoned error
    pub fn mutex_poisoned<S: Into<String>>(message: S) -> Self {
        BoltError::MutexPoisoned {
            message: message.into(),
        }
    }

    /// Get the error code for Neo4j compatibility
    pub fn error_code(&self) -> &'static str {
        match self {
            BoltError::Io(_) => "Neo.ClientError.General.ConnectionError",
            BoltError::VersionNegotiationFailed { .. } => "Neo.ClientError.Request.Invalid",
            BoltError::InvalidMessage { .. } => "Neo.ClientError.Request.InvalidFormat",
            BoltError::MessageTooLarge { .. } => "Neo.ClientError.Request.InvalidFormat",
            BoltError::AuthenticationFailed { .. } => "Neo.ClientError.Security.Unauthorized",
            BoltError::AuthorizationFailed { .. } => "Neo.ClientError.Security.Forbidden",
            BoltError::InvalidState { .. } => "Neo.ClientError.Request.Invalid",
            BoltError::QueryError { .. } => "Neo.ClientError.Statement.SyntaxError",
            BoltError::TransactionError { .. } => "Neo.TransientError.Transaction.Terminated",
            BoltError::SerializationError(_) => "Neo.ClientError.Request.InvalidFormat",
            BoltError::ConnectionTimeout { .. } => "Neo.TransientError.General.DatabaseUnavailable",
            BoltError::ProtocolError { .. } => "Neo.ClientError.Request.Invalid",
            BoltError::Internal { .. } => "Neo.DatabaseError.General.UnknownError",
            BoltError::MutexPoisoned { .. } => "Neo.TransientError.General.DatabaseUnavailable",
            BoltError::NotImplemented { .. } => "Neo.ClientError.Statement.FeatureNotSupported",
        }
    }

    /// GQL error classification for the Bolt 5.7+ diagnostic record
    /// (#1368). Derived from the `Neo.*` prefix, matching the documented
    /// CLIENT_ERROR / TRANSIENT_ERROR / DATABASE_ERROR categories.
    /// (Free function: `failure()` only receives code + message strings.)
    pub fn classification_for(code: &str) -> &'static str {
        if code.starts_with("Neo.TransientError") {
            "TRANSIENT_ERROR"
        } else if code.starts_with("Neo.DatabaseError") {
            "DATABASE_ERROR"
        } else {
            "CLIENT_ERROR"
        }
    }

    /// Human-readable status description for a raw message string:
    /// `error: general processing exception - unexpected error. <message>`,
    /// matching Neo4j and the drivers' own fallback wording for `50N42`.
    /// Note: `message` may echo substituted query text from ClickHouse
    /// errors (pre-existing behavior, also visible pre-5.7) — never log or
    /// store it beyond the FAILURE itself.
    pub fn status_description_for(message: &str) -> String {
        format!(
            "error: general processing exception - unexpected error. {}",
            message
        )
    }

    /// Check if the error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            BoltError::ConnectionTimeout { .. } => true,
            BoltError::TransactionError { .. } => true,
            BoltError::MutexPoisoned { .. } => true, // Transient error, client can retry
            BoltError::Internal { .. } => false,
            BoltError::Io(_) => false,
            _ => false,
        }
    }
}

/// Convert various error types to BoltError for convenient error propagation
impl From<&str> for BoltError {
    fn from(message: &str) -> Self {
        BoltError::internal(message)
    }
}

impl From<String> for BoltError {
    fn from(message: String) -> Self {
        BoltError::internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bolt_error_creation() {
        let error = BoltError::invalid_message("Test message");
        assert!(matches!(error, BoltError::InvalidMessage { .. }));
    }

    #[test]
    fn test_error_codes() {
        let auth_error = BoltError::AuthenticationFailed {
            user: "test".to_string(),
        };
        assert_eq!(
            auth_error.error_code(),
            "Neo.ClientError.Security.Unauthorized"
        );

        let query_error = BoltError::query_error("Syntax error");
        assert_eq!(
            query_error.error_code(),
            "Neo.ClientError.Statement.SyntaxError"
        );
    }

    #[test]
    fn test_recoverable_errors() {
        let timeout_error = BoltError::ConnectionTimeout {
            timeout_seconds: 30,
        };
        assert!(timeout_error.is_recoverable());

        let auth_error = BoltError::AuthenticationFailed {
            user: "test".to_string(),
        };
        assert!(!auth_error.is_recoverable());
    }

    #[test]
    fn test_error_conversion() {
        let error: BoltError = "Test error".into();
        assert!(matches!(error, BoltError::Internal { .. }));

        let error: BoltError = "Test error".to_string().into();
        assert!(matches!(error, BoltError::Internal { .. }));
    }

    #[test]
    fn test_1368_gql_classification() {
        // Classification derives from the Neo.* prefix (GQL diagnostics).
        // Free functions: failure() only sees code + message strings.
        assert_eq!(
            BoltError::classification_for("Neo.ClientError.Statement.SyntaxError"),
            "CLIENT_ERROR"
        );
        assert_eq!(
            BoltError::classification_for("Neo.TransientError.General.DatabaseUnavailable"),
            "TRANSIENT_ERROR"
        );
        assert_eq!(
            BoltError::classification_for("Neo.DatabaseError.General.UnknownError"),
            "DATABASE_ERROR"
        );
        // GQLSTATUS default is the documented polyfill value; neo4j_code stays
        // authoritative.
        assert_eq!(BoltError::GQLSTATUS_DEFAULT, "50N42");
        assert_eq!(BoltError::GQLSTATUS_DEFAULT.len(), 5);
        // Description matches Neo4j/drivers fallback wording for 50N42.
        assert_eq!(
            BoltError::status_description_for("boom"),
            "error: general processing exception - unexpected error. boom"
        );
    }
}
