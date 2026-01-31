//! Element ID generation and parsing for Neo4j Bolt protocol compatibility.
//!
//! This module provides functions to generate and parse Neo4j-compatible elementId
//! strings that support any ID type (integers, strings, UUIDs, composites).
//!
//! # Format Specification
//!
//! ## Node elementId Format
//! - Single ID: `"Label:id_value"`
//! - Composite ID: `"Label:id1|id2|id3"`
//!
//! Examples:
//! - `"User:123"` (integer ID)
//! - `"User:alice@example.com"` (string ID)
//! - `"User:550e8400-e29b-41d4-a716-446655440000"` (UUID ID)
//! - `"Account:tenant_1|456"` (composite ID: tenant_id + account_id)
//!
//! ## Relationship elementId Format
//! - `"RelType:from_id->to_id"`
//!
//! Examples:
//! - `"FOLLOWS:123->456"` (integer IDs)
//! - `"AUTHORED:alice@example.com->post-uuid-123"` (string IDs)
//! - `"BELONGS_TO:tenant_1|user_456->tenant_1|org_789"` (composite IDs)
//!
//! # Reversibility
//!
//! All elementId strings are 100% reversible using simple string operations:
//! - Split on `:` to extract label/type and ID portion
//! - Split ID portion on `|` for composite IDs
//! - Split relationship IDs on `->` for from/to IDs
//!
//! This enables Neo4j Browser's "Expand" feature which requires parsing
//! `WHERE id(n) = X` or `WHERE elementId(n) = 'X'` back into SQL predicates.

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum ElementIdError {
    InvalidFormat(String),
    MissingLabel,
    MissingId,
    MissingRelationshipType,
    MissingFromId,
    MissingToId,
    InvalidSeparator(String),
}

impl fmt::Display for ElementIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElementIdError::InvalidFormat(msg) => write!(f, "Invalid elementId format: {}", msg),
            ElementIdError::MissingLabel => write!(f, "Missing label in elementId"),
            ElementIdError::MissingId => write!(f, "Missing ID value in elementId"),
            ElementIdError::MissingRelationshipType => {
                write!(f, "Missing relationship type in elementId")
            }
            ElementIdError::MissingFromId => write!(f, "Missing from_id in relationship elementId"),
            ElementIdError::MissingToId => write!(f, "Missing to_id in relationship elementId"),
            ElementIdError::InvalidSeparator(sep) => {
                write!(f, "Invalid separator in elementId: {}", sep)
            }
        }
    }
}

impl std::error::Error for ElementIdError {}

/// Generate a Neo4j-compatible elementId for a node.
///
/// # Arguments
///
/// * `label` - The node label (e.g., "User", "Post")
/// * `id_values` - The ID values as strings (single element or multiple for composite IDs)
///
/// # Returns
///
/// A string in format:
/// - Single ID: `"Label:id_value"`
/// - Composite ID: `"Label:id1|id2|id3"`
///
/// # Examples
///
/// ```
/// use clickgraph::graph_catalog::element_id::generate_node_element_id;
///
/// // Single integer ID
/// let element_id = generate_node_element_id("User", &["123"]);
/// assert_eq!(element_id, "User:123");
///
/// // String ID
/// let element_id = generate_node_element_id("User", &["alice@example.com"]);
/// assert_eq!(element_id, "User:alice@example.com");
///
/// // Composite ID
/// let element_id = generate_node_element_id("Account", &["tenant_1", "456"]);
/// assert_eq!(element_id, "Account:tenant_1|456");
/// ```
pub fn generate_node_element_id(label: &str, id_values: &[&str]) -> String {
    if id_values.len() == 1 {
        // Single ID: "Label:id"
        format!("{}:{}", label, id_values[0])
    } else {
        // Composite ID: "Label:id1|id2|id3"
        format!("{}:{}", label, id_values.join("|"))
    }
}

/// Parse a Neo4j node elementId back into its components.
///
/// # Arguments
///
/// * `element_id` - The elementId string to parse
///
/// # Returns
///
/// A tuple of (label, id_values) where:
/// - label: The node label
/// - id_values: Vector of ID value strings (single or multiple for composite IDs)
///
/// # Errors
///
/// Returns `ElementIdError` if:
/// - Missing colon separator
/// - Missing label
/// - Missing ID values
///
/// # Examples
///
/// ```
/// use clickgraph::graph_catalog::element_id::parse_node_element_id;
///
/// // Single ID
/// let (label, ids) = parse_node_element_id("User:123").unwrap();
/// assert_eq!(label, "User");
/// assert_eq!(ids, vec!["123"]);
///
/// // Composite ID
/// let (label, ids) = parse_node_element_id("Account:tenant_1|456").unwrap();
/// assert_eq!(label, "Account");
/// assert_eq!(ids, vec!["tenant_1", "456"]);
/// ```
pub fn parse_node_element_id(element_id: &str) -> Result<(String, Vec<String>), ElementIdError> {
    // Split on first colon: "Label:id_portion"
    let parts: Vec<&str> = element_id.splitn(2, ':').collect();

    if parts.len() != 2 {
        return Err(ElementIdError::InvalidFormat(
            "Expected format 'Label:id' or 'Label:id1|id2'".to_string(),
        ));
    }

    let label = parts[0].trim();
    let id_portion = parts[1].trim();

    if label.is_empty() {
        return Err(ElementIdError::MissingLabel);
    }

    if id_portion.is_empty() {
        return Err(ElementIdError::MissingId);
    }

    // Split ID portion on pipe for composite IDs
    let id_values: Vec<String> = id_portion.split('|').map(|s| s.to_string()).collect();

    Ok((label.to_string(), id_values))
}

/// Generate a Neo4j-compatible elementId for a relationship.
///
/// # Arguments
///
/// * `rel_type` - The relationship type (e.g., "FOLLOWS", "AUTHORED")
/// * `from_id` - The from node ID (single or composite, already joined with `|`)
/// * `to_id` - The to node ID (single or composite, already joined with `|`)
///
/// # Returns
///
/// A string in format: `"RelType:from_id->to_id"`
///
/// # Examples
///
/// ```
/// use clickgraph::graph_catalog::element_id::generate_relationship_element_id;
///
/// // Simple integer IDs
/// let element_id = generate_relationship_element_id("FOLLOWS", "123", "456");
/// assert_eq!(element_id, "FOLLOWS:123->456");
///
/// // String IDs
/// let element_id = generate_relationship_element_id("AUTHORED", "alice@example.com", "post-uuid-123");
/// assert_eq!(element_id, "AUTHORED:alice@example.com->post-uuid-123");
///
/// // Composite IDs (already joined)
/// let element_id = generate_relationship_element_id("BELONGS_TO", "tenant_1|user_456", "tenant_1|org_789");
/// assert_eq!(element_id, "BELONGS_TO:tenant_1|user_456->tenant_1|org_789");
/// ```
pub fn generate_relationship_element_id(rel_type: &str, from_id: &str, to_id: &str) -> String {
    format!("{}:{}->{}", rel_type, from_id, to_id)
}

/// Parse a Neo4j relationship elementId back into its components.
///
/// # Arguments
///
/// * `element_id` - The relationship elementId string to parse
///
/// # Returns
///
/// A tuple of (rel_type, from_id, to_id) where:
/// - rel_type: The relationship type
/// - from_id: The from node ID (may contain `|` for composite IDs)
/// - to_id: The to node ID (may contain `|` for composite IDs)
///
/// # Errors
///
/// Returns `ElementIdError` if:
/// - Missing colon separator
/// - Missing relationship type
/// - Missing arrow separator (`->`)
/// - Missing from_id or to_id
///
/// # Examples
///
/// ```
/// use clickgraph::graph_catalog::element_id::parse_relationship_element_id;
///
/// // Simple IDs
/// let (rel_type, from_id, to_id) = parse_relationship_element_id("FOLLOWS:123->456").unwrap();
/// assert_eq!(rel_type, "FOLLOWS");
/// assert_eq!(from_id, "123");
/// assert_eq!(to_id, "456");
///
/// // Composite IDs
/// let (rel_type, from_id, to_id) = parse_relationship_element_id("BELONGS_TO:tenant_1|user_456->tenant_1|org_789").unwrap();
/// assert_eq!(rel_type, "BELONGS_TO");
/// assert_eq!(from_id, "tenant_1|user_456");
/// assert_eq!(to_id, "tenant_1|org_789");
/// ```
pub fn parse_relationship_element_id(
    element_id: &str,
) -> Result<(String, String, String), ElementIdError> {
    // Split on first colon: "RelType:from_id->to_id"
    let parts: Vec<&str> = element_id.splitn(2, ':').collect();

    if parts.len() != 2 {
        return Err(ElementIdError::InvalidFormat(
            "Expected format 'RelType:from_id->to_id'".to_string(),
        ));
    }

    let rel_type = parts[0].trim();
    let id_portion = parts[1].trim();

    if rel_type.is_empty() {
        return Err(ElementIdError::MissingRelationshipType);
    }

    // Split ID portion on arrow: "from_id->to_id"
    let id_parts: Vec<&str> = id_portion.split("->").collect();

    if id_parts.len() != 2 {
        return Err(ElementIdError::InvalidFormat(
            "Expected format 'from_id->to_id' in relationship elementId".to_string(),
        ));
    }

    let from_id = id_parts[0].trim();
    let to_id = id_parts[1].trim();

    if from_id.is_empty() {
        return Err(ElementIdError::MissingFromId);
    }

    if to_id.is_empty() {
        return Err(ElementIdError::MissingToId);
    }

    Ok((rel_type.to_string(), from_id.to_string(), to_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Node elementId Tests ==========

    #[test]
    fn test_generate_node_element_id_single_integer() {
        let element_id = generate_node_element_id("User", &["123"]);
        assert_eq!(element_id, "User:123");
    }

    #[test]
    fn test_generate_node_element_id_single_string() {
        let element_id = generate_node_element_id("User", &["alice@example.com"]);
        assert_eq!(element_id, "User:alice@example.com");
    }

    #[test]
    fn test_generate_node_element_id_single_uuid() {
        let element_id =
            generate_node_element_id("Post", &["550e8400-e29b-41d4-a716-446655440000"]);
        assert_eq!(element_id, "Post:550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_generate_node_element_id_composite_two() {
        let element_id = generate_node_element_id("Account", &["tenant_1", "456"]);
        assert_eq!(element_id, "Account:tenant_1|456");
    }

    #[test]
    fn test_generate_node_element_id_composite_three() {
        let element_id = generate_node_element_id("Event", &["2024", "01", "15"]);
        assert_eq!(element_id, "Event:2024|01|15");
    }

    #[test]
    fn test_parse_node_element_id_single_integer() {
        let (label, ids) = parse_node_element_id("User:123").unwrap();
        assert_eq!(label, "User");
        assert_eq!(ids, vec!["123"]);
    }

    #[test]
    fn test_parse_node_element_id_single_string() {
        let (label, ids) = parse_node_element_id("User:alice@example.com").unwrap();
        assert_eq!(label, "User");
        assert_eq!(ids, vec!["alice@example.com"]);
    }

    #[test]
    fn test_parse_node_element_id_single_uuid() {
        let (label, ids) =
            parse_node_element_id("Post:550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(label, "Post");
        assert_eq!(ids, vec!["550e8400-e29b-41d4-a716-446655440000"]);
    }

    #[test]
    fn test_parse_node_element_id_composite_two() {
        let (label, ids) = parse_node_element_id("Account:tenant_1|456").unwrap();
        assert_eq!(label, "Account");
        assert_eq!(ids, vec!["tenant_1", "456"]);
    }

    #[test]
    fn test_parse_node_element_id_composite_three() {
        let (label, ids) = parse_node_element_id("Event:2024|01|15").unwrap();
        assert_eq!(label, "Event");
        assert_eq!(ids, vec!["2024", "01", "15"]);
    }

    #[test]
    fn test_parse_node_element_id_missing_colon() {
        let result = parse_node_element_id("User123");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ElementIdError::InvalidFormat(_)
        ));
    }

    #[test]
    fn test_parse_node_element_id_missing_label() {
        let result = parse_node_element_id(":123");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElementIdError::MissingLabel));
    }

    #[test]
    fn test_parse_node_element_id_missing_id() {
        let result = parse_node_element_id("User:");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElementIdError::MissingId));
    }

    #[test]
    fn test_node_element_id_round_trip_single() {
        let original_label = "User";
        let original_ids = vec!["123"];
        let element_id = generate_node_element_id(original_label, &original_ids);
        let (parsed_label, parsed_ids) = parse_node_element_id(&element_id).unwrap();
        assert_eq!(parsed_label, original_label);
        assert_eq!(parsed_ids, original_ids);
    }

    #[test]
    fn test_node_element_id_round_trip_composite() {
        let original_label = "Account";
        let original_ids = vec!["tenant_1", "456"];
        let element_id = generate_node_element_id(original_label, &original_ids);
        let (parsed_label, parsed_ids) = parse_node_element_id(&element_id).unwrap();
        assert_eq!(parsed_label, original_label);
        assert_eq!(parsed_ids, original_ids);
    }

    // ========== Relationship elementId Tests ==========

    #[test]
    fn test_generate_relationship_element_id_simple() {
        let element_id = generate_relationship_element_id("FOLLOWS", "123", "456");
        assert_eq!(element_id, "FOLLOWS:123->456");
    }

    #[test]
    fn test_generate_relationship_element_id_strings() {
        let element_id =
            generate_relationship_element_id("AUTHORED", "alice@example.com", "post-uuid-123");
        assert_eq!(element_id, "AUTHORED:alice@example.com->post-uuid-123");
    }

    #[test]
    fn test_generate_relationship_element_id_composite() {
        let element_id =
            generate_relationship_element_id("BELONGS_TO", "tenant_1|user_456", "tenant_1|org_789");
        assert_eq!(element_id, "BELONGS_TO:tenant_1|user_456->tenant_1|org_789");
    }

    #[test]
    fn test_parse_relationship_element_id_simple() {
        let (rel_type, from_id, to_id) = parse_relationship_element_id("FOLLOWS:123->456").unwrap();
        assert_eq!(rel_type, "FOLLOWS");
        assert_eq!(from_id, "123");
        assert_eq!(to_id, "456");
    }

    #[test]
    fn test_parse_relationship_element_id_strings() {
        let (rel_type, from_id, to_id) =
            parse_relationship_element_id("AUTHORED:alice@example.com->post-uuid-123").unwrap();
        assert_eq!(rel_type, "AUTHORED");
        assert_eq!(from_id, "alice@example.com");
        assert_eq!(to_id, "post-uuid-123");
    }

    #[test]
    fn test_parse_relationship_element_id_composite() {
        let (rel_type, from_id, to_id) =
            parse_relationship_element_id("BELONGS_TO:tenant_1|user_456->tenant_1|org_789")
                .unwrap();
        assert_eq!(rel_type, "BELONGS_TO");
        assert_eq!(from_id, "tenant_1|user_456");
        assert_eq!(to_id, "tenant_1|org_789");
    }

    #[test]
    fn test_parse_relationship_element_id_missing_colon() {
        let result = parse_relationship_element_id("FOLLOWS123->456");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ElementIdError::InvalidFormat(_)
        ));
    }

    #[test]
    fn test_parse_relationship_element_id_missing_type() {
        let result = parse_relationship_element_id(":123->456");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ElementIdError::MissingRelationshipType
        ));
    }

    #[test]
    fn test_parse_relationship_element_id_missing_arrow() {
        let result = parse_relationship_element_id("FOLLOWS:123456");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ElementIdError::InvalidFormat(_)
        ));
    }

    #[test]
    fn test_parse_relationship_element_id_missing_from_id() {
        let result = parse_relationship_element_id("FOLLOWS:->456");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElementIdError::MissingFromId));
    }

    #[test]
    fn test_parse_relationship_element_id_missing_to_id() {
        let result = parse_relationship_element_id("FOLLOWS:123->");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElementIdError::MissingToId));
    }

    #[test]
    fn test_relationship_element_id_round_trip_simple() {
        let original_type = "FOLLOWS";
        let original_from = "123";
        let original_to = "456";
        let element_id =
            generate_relationship_element_id(original_type, original_from, original_to);
        let (parsed_type, parsed_from, parsed_to) =
            parse_relationship_element_id(&element_id).unwrap();
        assert_eq!(parsed_type, original_type);
        assert_eq!(parsed_from, original_from);
        assert_eq!(parsed_to, original_to);
    }

    #[test]
    fn test_relationship_element_id_round_trip_composite() {
        let original_type = "BELONGS_TO";
        let original_from = "tenant_1|user_456";
        let original_to = "tenant_1|org_789";
        let element_id =
            generate_relationship_element_id(original_type, original_from, original_to);
        let (parsed_type, parsed_from, parsed_to) =
            parse_relationship_element_id(&element_id).unwrap();
        assert_eq!(parsed_type, original_type);
        assert_eq!(parsed_from, original_from);
        assert_eq!(parsed_to, original_to);
    }

    // ========== Edge Cases ==========

    #[test]
    fn test_node_element_id_with_special_characters() {
        // Test that special characters in IDs are preserved
        let element_id = generate_node_element_id("User", &["alice+bob@example.com"]);
        assert_eq!(element_id, "User:alice+bob@example.com");

        let (label, ids) = parse_node_element_id(&element_id).unwrap();
        assert_eq!(label, "User");
        assert_eq!(ids, vec!["alice+bob@example.com"]);
    }

    #[test]
    fn test_relationship_element_id_with_special_characters() {
        // Test that special characters in IDs are preserved
        let element_id = generate_relationship_element_id(
            "SENT_EMAIL",
            "alice+test@example.com",
            "bob@example.com",
        );
        assert_eq!(
            element_id,
            "SENT_EMAIL:alice+test@example.com->bob@example.com"
        );

        let (rel_type, from_id, to_id) = parse_relationship_element_id(&element_id).unwrap();
        assert_eq!(rel_type, "SENT_EMAIL");
        assert_eq!(from_id, "alice+test@example.com");
        assert_eq!(to_id, "bob@example.com");
    }

    #[test]
    fn test_node_element_id_with_colon_in_uuid() {
        // UUID shouldn't have colons, but test robustness with splitn(2)
        let element_id = generate_node_element_id("Post", &["post:123:456"]);
        assert_eq!(element_id, "Post:post:123:456");

        let (label, ids) = parse_node_element_id(&element_id).unwrap();
        assert_eq!(label, "Post");
        // splitn(2, ':') will split only on first colon
        assert_eq!(ids, vec!["post:123:456"]);
    }
}
