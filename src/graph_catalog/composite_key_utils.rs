//! Composite key utilities for relationship schemas
//!
//! This module consolidates logic for building, parsing, and working with composite
//! relationship keys of the form `TYPE::FROM_NODE::TO_NODE`.
//!
//! # Why Composite Keys?
//!
//! Relationships in ClickGraph can have the same type but different node pairs:
//! - `POST_HAS_TAG::Post::Tag` - One Post to many Tags
//! - `POST_HAS_TAG::Comment::Tag` - One Comment to many Tags  
//! - `COMMENT_HAS_TAG::Post::Tag` - Different relationship for Posts vs Comments
//!
//! Composite keys uniquely identify each variant without ambiguity.
//!
//! # Example
//!
//! ```ignore
//! use crate::graph_catalog::composite_key_utils::{CompositeKey, CompositeKeyError};
//!
//! // Building a composite key
//! let key = CompositeKey::new("FOLLOWS", "User", "User");
//! assert_eq!(key.to_string(), "FOLLOWS::User::User");
//!
//! // Parsing a composite key
//! let parsed = CompositeKey::parse("FOLLOWS::User::User")?;
//! assert_eq!(parsed.rel_type, "FOLLOWS");
//! assert_eq!(parsed.from_node, "User");
//! assert_eq!(parsed.to_node, "User");
//! ```

use std::fmt;

/// Error type for composite key operations
#[derive(Debug, Clone, PartialEq)]
pub enum CompositeKeyError {
    /// Invalid number of parts in composite key
    InvalidFormat {
        key: String,
        reason: String,
    },
    /// Empty component in composite key
    EmptyComponent {
        key: String,
        component: String,
    },
}

impl fmt::Display for CompositeKeyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompositeKeyError::InvalidFormat { key, reason } => {
                write!(
                    f,
                    "Invalid composite key format '{}': {}. Expected 'TYPE::FROM_NODE::TO_NODE'",
                    key, reason
                )
            }
            CompositeKeyError::EmptyComponent { key, component } => {
                write!(
                    f,
                    "Empty {} in composite key '{}'. Expected 'TYPE::FROM_NODE::TO_NODE'",
                    component, key
                )
            }
        }
    }
}

impl std::error::Error for CompositeKeyError {}

/// Composite relationship key: `TYPE::FROM_NODE::TO_NODE`
///
/// # Example
/// ```ignore
/// let key = CompositeKey::new("FOLLOWS", "User", "User");
/// assert_eq!(key.to_string(), "FOLLOWS::User::User");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey {
    /// Relationship type name (e.g., "FOLLOWS")
    pub rel_type: String,
    /// Source node label (e.g., "User")
    pub from_node: String,
    /// Target node label (e.g., "User")
    pub to_node: String,
}

impl CompositeKey {
    /// Create a new composite key
    ///
    /// # Arguments
    /// * `rel_type` - Relationship type (e.g., "FOLLOWS")
    /// * `from_node` - Source node label (e.g., "User")
    /// * `to_node` - Target node label (e.g., "User")
    ///
    /// # Returns
    /// New CompositeKey instance
    ///
    /// # Panics
    /// If any argument is empty
    pub fn new(rel_type: &str, from_node: &str, to_node: &str) -> Self {
        assert!(!rel_type.is_empty(), "rel_type cannot be empty");
        assert!(!from_node.is_empty(), "from_node cannot be empty");
        assert!(!to_node.is_empty(), "to_node cannot be empty");

        CompositeKey {
            rel_type: rel_type.to_string(),
            from_node: from_node.to_string(),
            to_node: to_node.to_string(),
        }
    }

    /// Parse a composite key from a string
    ///
    /// # Arguments
    /// * `key_str` - String representation (e.g., "FOLLOWS::User::User")
    ///
    /// # Returns
    /// `Ok(CompositeKey)` if valid, `Err(CompositeKeyError)` otherwise
    ///
    /// # Example
    /// ```ignore
    /// let key = CompositeKey::parse("FOLLOWS::User::User")?;
    /// assert_eq!(key.rel_type, "FOLLOWS");
    /// ```
    pub fn parse(key_str: &str) -> Result<Self, CompositeKeyError> {
        let parts: Vec<&str> = key_str.split("::").collect();

        if parts.len() != 3 {
            return Err(CompositeKeyError::InvalidFormat {
                key: key_str.to_string(),
                reason: format!("expected 3 parts, found {}", parts.len()),
            });
        }

        let rel_type = parts[0].trim();
        let from_node = parts[1].trim();
        let to_node = parts[2].trim();

        if rel_type.is_empty() {
            return Err(CompositeKeyError::EmptyComponent {
                key: key_str.to_string(),
                component: "type".to_string(),
            });
        }

        if from_node.is_empty() {
            return Err(CompositeKeyError::EmptyComponent {
                key: key_str.to_string(),
                component: "from_node".to_string(),
            });
        }

        if to_node.is_empty() {
            return Err(CompositeKeyError::EmptyComponent {
                key: key_str.to_string(),
                component: "to_node".to_string(),
            });
        }

        Ok(CompositeKey {
            rel_type: rel_type.to_string(),
            from_node: from_node.to_string(),
            to_node: to_node.to_string(),
        })
    }

    /// Convert to string representation
    ///
    /// # Example
    /// ```ignore
    /// let key = CompositeKey::new("FOLLOWS", "User", "User");
    /// assert_eq!(key.to_string(), "FOLLOWS::User::User");
    /// ```
    pub fn to_string(&self) -> String {
        format!("{}::{}::{}", self.rel_type, self.from_node, self.to_node)
    }

    /// Check if this key matches the given relationship spec
    ///
    /// # Arguments
    /// * `rel_type` - Relationship type to match
    /// * `from_node` - Source node label to match
    /// * `to_node` - Target node label to match
    ///
    /// # Returns
    /// `true` if all components match
    pub fn matches(&self, rel_type: &str, from_node: &str, to_node: &str) -> bool {
        self.rel_type == rel_type && self.from_node == from_node && self.to_node == to_node
    }

    /// Check if this key has a given relationship type
    ///
    /// # Arguments
    /// * `rel_type` - Relationship type to check
    ///
    /// # Returns
    /// `true` if relationship type matches
    pub fn has_type(&self, rel_type: &str) -> bool {
        self.rel_type == rel_type
    }

    /// Check if from_node matches
    pub fn has_from_node(&self, from_node: &str) -> bool {
        self.from_node == from_node
    }

    /// Check if to_node matches
    pub fn has_to_node(&self, to_node: &str) -> bool {
        self.to_node == to_node
    }
}

impl fmt::Display for CompositeKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Check if a string is a composite key (contains "::")
///
/// # Example
/// ```ignore
/// assert!(is_composite_key("FOLLOWS::User::User"));
/// assert!(!is_composite_key("FOLLOWS"));
/// ```
pub fn is_composite_key(key: &str) -> bool {
    key.contains("::")
}

/// Extract the type name from a composite key string
///
/// Returns the first part before the first "::", or the whole string if not a composite key.
///
/// # Example
/// ```ignore
/// assert_eq!(extract_type_name("FOLLOWS::User::User"), "FOLLOWS");
/// assert_eq!(extract_type_name("FOLLOWS"), "FOLLOWS");
/// ```
pub fn extract_type_name(key: &str) -> &str {
    key.split("::").next().unwrap_or(key)
}

/// Build a composite key string (convenience function)
///
/// # Arguments
/// * `rel_type` - Relationship type
/// * `from_node` - Source node label
/// * `to_node` - Target node label
///
/// # Returns
/// Composite key string in format `TYPE::FROM::TO`
pub fn build_composite_key(rel_type: &str, from_node: &str, to_node: &str) -> String {
    format!("{}::{}::{}", rel_type, from_node, to_node)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_key_new() {
        let key = CompositeKey::new("FOLLOWS", "User", "User");
        assert_eq!(key.rel_type, "FOLLOWS");
        assert_eq!(key.from_node, "User");
        assert_eq!(key.to_node, "User");
    }

    #[test]
    fn test_composite_key_to_string() {
        let key = CompositeKey::new("FOLLOWS", "User", "User");
        assert_eq!(key.to_string(), "FOLLOWS::User::User");
    }

    #[test]
    fn test_composite_key_parse_valid() {
        let key = CompositeKey::parse("FOLLOWS::User::User").unwrap();
        assert_eq!(key.rel_type, "FOLLOWS");
        assert_eq!(key.from_node, "User");
        assert_eq!(key.to_node, "User");
    }

    #[test]
    fn test_composite_key_parse_with_spaces() {
        let key = CompositeKey::parse("FOLLOWS :: User :: User").unwrap();
        assert_eq!(key.rel_type, "FOLLOWS");
        assert_eq!(key.from_node, "User");
        assert_eq!(key.to_node, "User");
    }

    #[test]
    fn test_composite_key_parse_invalid_parts() {
        let result = CompositeKey::parse("FOLLOWS::User");
        assert!(result.is_err());

        let result = CompositeKey::parse("FOLLOWS::User::User::Extra");
        assert!(result.is_err());
    }

    #[test]
    fn test_composite_key_parse_empty_component() {
        let result = CompositeKey::parse("::User::User");
        assert!(result.is_err());

        let result = CompositeKey::parse("FOLLOWS::::User");
        assert!(result.is_err());
    }

    #[test]
    fn test_composite_key_matches() {
        let key = CompositeKey::new("FOLLOWS", "User", "User");
        assert!(key.matches("FOLLOWS", "User", "User"));
        assert!(!key.matches("FOLLOWS", "User", "Post"));
    }

    #[test]
    fn test_is_composite_key() {
        assert!(is_composite_key("FOLLOWS::User::User"));
        assert!(!is_composite_key("FOLLOWS"));
        assert!(is_composite_key("::"));
    }

    #[test]
    fn test_extract_type_name() {
        assert_eq!(extract_type_name("FOLLOWS::User::User"), "FOLLOWS");
        assert_eq!(extract_type_name("FOLLOWS"), "FOLLOWS");
        assert_eq!(extract_type_name("::User::User"), "");
    }

    #[test]
    fn test_build_composite_key() {
        let key = build_composite_key("FOLLOWS", "User", "User");
        assert_eq!(key, "FOLLOWS::User::User");
    }
}
