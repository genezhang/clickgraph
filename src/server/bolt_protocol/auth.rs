//! Bolt Protocol Authentication
//!
//! This module handles authentication for Bolt protocol connections,
//! supporting various authentication schemes compatible with Neo4j drivers.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;

use super::errors::{BoltError, BoltResult};

/// Authentication schemes supported by the Bolt protocol
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthScheme {
    /// No authentication required
    None,
    /// Basic username/password authentication
    Basic,
    /// Kerberos authentication (not implemented)
    Kerberos,
    /// Custom authentication scheme
    Custom(String),
}

impl AuthScheme {
    /// Parse authentication scheme from string
    pub fn parse_from_str(scheme: &str) -> Self {
        match scheme.to_lowercase().as_str() {
            "none" => AuthScheme::None,
            "basic" => AuthScheme::Basic,
            "kerberos" => AuthScheme::Kerberos,
            custom => AuthScheme::Custom(custom.to_string()),
        }
    }
}

impl fmt::Display for AuthScheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AuthScheme::None => "none",
                AuthScheme::Basic => "basic",
                AuthScheme::Kerberos => "kerberos",
                AuthScheme::Custom(scheme) => scheme.as_str(),
            }
        )
    }
}

/// Authentication token containing credentials
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// Authentication scheme
    pub scheme: AuthScheme,
    /// Principal (username)
    pub principal: Option<String>,
    /// Credentials (password, token, etc.)
    pub credentials: Option<String>,
    /// Additional parameters
    pub parameters: HashMap<String, String>,
}

impl AuthToken {
    /// Create a new authentication token
    pub fn new(scheme: AuthScheme) -> Self {
        AuthToken {
            scheme,
            principal: None,
            credentials: None,
            parameters: HashMap::new(),
        }
    }

    /// Create a basic authentication token
    pub fn basic(username: String, password: String) -> Self {
        AuthToken {
            scheme: AuthScheme::Basic,
            principal: Some(username),
            credentials: Some(password),
            parameters: HashMap::new(),
        }
    }

    /// Create a no-authentication token
    pub fn none() -> Self {
        AuthToken::new(AuthScheme::None)
    }

    /// Parse authentication token from HELLO message fields
    pub fn from_hello_fields(auth_map: &HashMap<String, Value>) -> BoltResult<Self> {
        let scheme_str = auth_map
            .get("scheme")
            .and_then(|v| v.as_str())
            .unwrap_or("none");

        let scheme = AuthScheme::parse_from_str(scheme_str);

        let principal = auth_map
            .get("principal")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let credentials = auth_map
            .get("credentials")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Extract additional parameters
        let mut parameters = HashMap::new();
        for (key, value) in auth_map {
            if !matches!(key.as_str(), "scheme" | "principal" | "credentials") {
                if let Some(str_value) = value.as_str() {
                    parameters.insert(key.clone(), str_value.to_string());
                }
            }
        }

        Ok(AuthToken {
            scheme,
            principal,
            credentials,
            parameters,
        })
    }

    /// Convert to HashMap for serialization
    pub fn to_map(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        map.insert("scheme".to_string(), Value::String(self.scheme.to_string()));

        if let Some(ref principal) = self.principal {
            map.insert("principal".to_string(), Value::String(principal.clone()));
        }

        if let Some(ref credentials) = self.credentials {
            map.insert(
                "credentials".to_string(),
                Value::String(credentials.clone()),
            );
        }

        for (key, value) in &self.parameters {
            map.insert(key.clone(), Value::String(value.clone()));
        }

        map
    }
}

/// User information after successful authentication
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// Username
    pub username: String,
    /// User roles/permissions
    pub roles: Vec<String>,
    /// Authentication scheme used
    pub auth_scheme: AuthScheme,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl AuthenticatedUser {
    /// Create a new authenticated user
    pub fn new(username: String, auth_scheme: AuthScheme) -> Self {
        AuthenticatedUser {
            username,
            roles: vec!["user".to_string()], // Default role
            auth_scheme,
            metadata: HashMap::new(),
        }
    }

    /// Check if user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(&role.to_string())
    }

    /// Add a role to the user
    pub fn add_role(&mut self, role: String) {
        if !self.roles.contains(&role) {
            self.roles.push(role);
        }
    }
}

/// Authentication manager
#[derive(Debug)]
pub struct Authenticator {
    /// Enable authentication (if false, all connections are allowed)
    pub enabled: bool,
    /// Default user for unauthenticated connections
    pub default_user: Option<String>,
    /// Static user database (username -> password hash)
    users: HashMap<String, String>,
}

impl Authenticator {
    /// Create a new authenticator
    pub fn new(enabled: bool, default_user: Option<String>) -> Self {
        let mut authenticator = Authenticator {
            enabled,
            default_user,
            users: HashMap::new(),
        };

        // Add default users for development
        if !enabled {
            authenticator.add_user("brahmand".to_string(), "password".to_string());
            authenticator.add_user("neo4j".to_string(), "password".to_string());
        }

        authenticator
    }

    /// Add a user to the static user database
    pub fn add_user(&mut self, username: String, password: String) {
        let password_hash = self.hash_password(&password);
        self.users.insert(username, password_hash);
    }

    /// Authenticate a user with the given token
    pub fn authenticate(&self, token: &AuthToken) -> BoltResult<AuthenticatedUser> {
        if !self.enabled {
            // Authentication disabled - allow all connections
            let username = token
                .principal
                .clone()
                .or_else(|| self.default_user.clone())
                .unwrap_or_else(|| "anonymous".to_string());

            return Ok(AuthenticatedUser::new(username, token.scheme.clone()));
        }

        match &token.scheme {
            AuthScheme::None => {
                if let Some(ref default_user) = self.default_user {
                    Ok(AuthenticatedUser::new(
                        default_user.clone(),
                        AuthScheme::None,
                    ))
                } else {
                    Err(BoltError::AuthenticationFailed {
                        user: "anonymous".to_string(),
                    })
                }
            }
            AuthScheme::Basic => {
                let username =
                    token
                        .principal
                        .as_ref()
                        .ok_or_else(|| BoltError::AuthenticationFailed {
                            user: "unknown".to_string(),
                        })?;

                let password =
                    token
                        .credentials
                        .as_ref()
                        .ok_or_else(|| BoltError::AuthenticationFailed {
                            user: username.clone(),
                        })?;

                if self.verify_password(username, password) {
                    Ok(AuthenticatedUser::new(username.clone(), AuthScheme::Basic))
                } else {
                    Err(BoltError::AuthenticationFailed {
                        user: username.clone(),
                    })
                }
            }
            AuthScheme::Kerberos => Err(BoltError::not_implemented("Kerberos authentication")),
            AuthScheme::Custom(scheme) => Err(BoltError::not_implemented(format!(
                "Custom authentication scheme: {}",
                scheme
            ))),
        }
    }

    /// Verify password for a user
    fn verify_password(&self, username: &str, password: &str) -> bool {
        if let Some(stored_hash) = self.users.get(username) {
            let password_hash = self.hash_password(password);
            stored_hash == &password_hash
        } else {
            false
        }
    }

    /// Hash a password using SHA-256
    fn hash_password(&self, password: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let result = hasher.finalize();
        BASE64.encode(result)
    }

    /// Get list of users (for debugging)
    pub fn list_users(&self) -> Vec<String> {
        self.users.keys().cloned().collect()
    }
}

impl Default for Authenticator {
    fn default() -> Self {
        Authenticator::new(false, Some("brahmand".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_scheme_parsing() {
        assert_eq!(AuthScheme::parse_from_str("basic"), AuthScheme::Basic);
        assert_eq!(AuthScheme::parse_from_str("BASIC"), AuthScheme::Basic);
        assert_eq!(AuthScheme::parse_from_str("none"), AuthScheme::None);
        assert_eq!(
            AuthScheme::parse_from_str("custom"),
            AuthScheme::Custom("custom".to_string())
        );
    }

    #[test]
    fn test_basic_auth_token() {
        let token = AuthToken::basic("alice".to_string(), "secret".to_string());
        assert_eq!(token.scheme, AuthScheme::Basic);
        assert_eq!(token.principal, Some("alice".to_string()));
        assert_eq!(token.credentials, Some("secret".to_string()));
    }

    #[test]
    fn test_auth_token_from_hello_fields() {
        let mut auth_map = HashMap::new();
        auth_map.insert("scheme".to_string(), Value::String("basic".to_string()));
        auth_map.insert("principal".to_string(), Value::String("alice".to_string()));
        auth_map.insert(
            "credentials".to_string(),
            Value::String("secret".to_string()),
        );

        let token = AuthToken::from_hello_fields(&auth_map).unwrap();
        assert_eq!(token.scheme, AuthScheme::Basic);
        assert_eq!(token.principal, Some("alice".to_string()));
        assert_eq!(token.credentials, Some("secret".to_string()));
    }

    #[test]
    fn test_authenticator_disabled() {
        let authenticator = Authenticator::new(false, Some("default".to_string()));
        let token = AuthToken::basic("alice".to_string(), "wrong_password".to_string());

        let result = authenticator.authenticate(&token);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().username, "alice");
    }

    #[test]
    fn test_authenticator_enabled() {
        let mut authenticator = Authenticator::new(true, None);
        authenticator.add_user("alice".to_string(), "secret".to_string());

        // Valid credentials
        let token = AuthToken::basic("alice".to_string(), "secret".to_string());
        let result = authenticator.authenticate(&token);
        assert!(result.is_ok());

        // Invalid credentials
        let token = AuthToken::basic("alice".to_string(), "wrong".to_string());
        let result = authenticator.authenticate(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticated_user_roles() {
        let mut user = AuthenticatedUser::new("alice".to_string(), AuthScheme::Basic);
        assert!(user.has_role("user"));
        assert!(!user.has_role("admin"));

        user.add_role("admin".to_string());
        assert!(user.has_role("admin"));
    }

    #[test]
    fn test_password_hashing() {
        let authenticator = Authenticator::new(true, None);
        let hash1 = authenticator.hash_password("secret");
        let hash2 = authenticator.hash_password("secret");
        let hash3 = authenticator.hash_password("different");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
}
