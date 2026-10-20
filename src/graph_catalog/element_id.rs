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
//! - `"RelType:edge_id:from_id->to_id"` where the schema declares an
//!   `edge_id` (parallel relationships, #1334): its parts' texts joined by
//!   `|`, each escaped ([`edge_id_text`]), so no two edge ids spell one text
//!
//! Examples:
//! - `"FOLLOWS:123->456"` (integer IDs)
//! - `"AUTHORED:alice@example.com->post-uuid-123"` (string IDs)
//! - `"BELONGS_TO:tenant_1|user_456->tenant_1|org_789"` (composite IDs)
//! - `"FOLLOWS:10:6->7"` (edge_id 10)
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
/// // Single integer ID (note: trailing `-` is a Neo4j-Browser-compat sentinel)
/// let element_id = generate_node_element_id("User", &["123"]);
/// assert_eq!(element_id, "User:123-");
///
/// // String ID
/// let element_id = generate_node_element_id("User", &["alice@example.com"]);
/// assert_eq!(element_id, "User:alice@example.com-");
///
/// // Composite ID
/// let element_id = generate_node_element_id("Account", &["tenant_1", "456"]);
/// assert_eq!(element_id, "Account:tenant_1|456-");
/// ```
pub fn generate_node_element_id(label: &str, id_values: &[&str]) -> String {
    // Trailing `-` is a Neo4j-Browser-compat sentinel: Browser's expand-query
    // generator picks elementId-mode iff the first nodeId string contains `-`,
    // otherwise it falls back to legacy id() mode and tries `parseInt(elementId)`,
    // which yields NaN → 0 for our `Label:id` format and breaks click-to-expand.
    // Parsers strip the trailing `-` so internal callers see the same value.
    // Single ID: "Label:id-"; composite ID: "Label:id1|id2|id3-".
    let (prefix, suffix) = node_element_id_affixes(label);
    format!("{prefix}{}{suffix}", id_values.join("|"))
}

/// The text around a node's id in its element id
/// ([`generate_node_element_id`]): the bound plan spells the same element id
/// in SQL for a node inside a value (a path, `nodes(p)`), so the two agree.
pub fn node_element_id_affixes(label: &str) -> (String, &'static str) {
    let (after_label, suffix) = node_element_id_separators();
    (format!("{label}{after_label}"), suffix)
}

/// The text after a node's label and after its id in its element id
/// ([`generate_node_element_id`]): the bound plan spells the element id of a
/// node whose label is a column (one of several possible labels) from them.
pub fn node_element_id_separators() -> (&'static str, &'static str) {
    (":", "-")
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
    // Strip the Neo4j-Browser-compat trailing `-` sentinel (see generator docs).
    let element_id = element_id.trim_end_matches('-');

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
/// // Simple integer IDs (note: trailing `-` is a Neo4j-Browser-compat sentinel)
/// let element_id = generate_relationship_element_id("FOLLOWS", "123", "456");
/// assert_eq!(element_id, "FOLLOWS:123->456-");
///
/// // String IDs
/// let element_id = generate_relationship_element_id("AUTHORED", "alice@example.com", "post-uuid-123");
/// assert_eq!(element_id, "AUTHORED:alice@example.com->post-uuid-123-");
///
/// // Composite IDs (already joined)
/// let element_id = generate_relationship_element_id("BELONGS_TO", "tenant_1|user_456", "tenant_1|org_789");
/// assert_eq!(element_id, "BELONGS_TO:tenant_1|user_456->tenant_1|org_789-");
/// ```
pub fn generate_relationship_element_id(rel_type: &str, from_id: &str, to_id: &str) -> String {
    relationship_element_id(rel_type, None, from_id, to_id)
}

/// [`generate_relationship_element_id`] of a relationship whose schema
/// declares an `edge_id`, of text `edge_id` ([`edge_id_text`]), else (`None`)
/// of one without: `"TYPE:edge_id:from->to-"`.
///
/// ```
/// use clickgraph::graph_catalog::element_id::{edge_id_text, relationship_element_id};
///
/// let edge = edge_id_text(&[Some("10")]);
/// assert_eq!(relationship_element_id("FOLLOWS", Some(&edge), "6", "7"), "FOLLOWS:10:6->7-");
/// assert_eq!(relationship_element_id("FOLLOWS", None, "6", "7"), "FOLLOWS:6->7-");
/// ```
pub fn relationship_element_id(
    rel_type: &str,
    edge_id: Option<&str>,
    from_id: &str,
    to_id: &str,
) -> String {
    // Trailing `-` is a Neo4j-Browser-compat sentinel; see node generator.
    // The arrow `->` already contains a `-` in practice, but for relationships
    // returned without nodes, Browser still inspects the string. Be consistent.
    let (prefix, between, suffix) = relationship_element_id_affixes(rel_type);
    let edge = match edge_id {
        Some(e) => format!("{e}{}", edge_id_separators().1),
        None => String::new(),
    };
    format!("{prefix}{edge}{from_id}{between}{to_id}{suffix}")
}

/// The texts in an `edge_id`'s part ([`edge_id_text`]) that are escaped, each
/// with its escape: the escape character itself, and the separators between
/// the parts and after the edge id.
pub fn edge_id_escapes() -> [(&'static str, &'static str); 3] {
    [("\\", "\\\\"), (":", "\\:"), ("|", "\\|")]
}

/// The text of a NULL part of an `edge_id` ([`edge_id_text`]): no escaped
/// part's text.
pub const NULL_EDGE_ID_PART: &str = "\\N";

/// The text between an `edge_id`'s parts, and after the edge id, in a
/// relationship's element id ([`relationship_element_id`]).
pub fn edge_id_separators() -> (&'static str, &'static str) {
    ("|", ":")
}

/// The text of an `edge_id` of parts `parts` (`None`: NULL) in a
/// relationship's element id: each part's text escaped ([`edge_id_escapes`]),
/// NULL as [`NULL_EDGE_ID_PART`], joined by `|`. Two edge ids of different
/// parts are different texts, which end at the first unescaped `:`.
pub fn edge_id_text(parts: &[Option<&str>]) -> String {
    let texts: Vec<String> = parts
        .iter()
        .map(|p| match p {
            Some(text) => escape_edge_id_part(text),
            None => NULL_EDGE_ID_PART.to_string(),
        })
        .collect();
    texts.join(edge_id_separators().0)
}

fn escape_edge_id_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        let mut buf = [0u8; 4];
        let c = &*c.encode_utf8(&mut buf);
        match edge_id_escapes().iter().find(|(from, _)| *from == c) {
            Some((_, to)) => out.push_str(to),
            None => out.push_str(c),
        }
    }
    out
}

/// A relationship's element id with an `edge_id` ([`relationship_element_id`])
/// read back: its type, the parts of its edge id (`None`: NULL), and the
/// texts of its ends (`from->to`, which a string id containing `->` makes
/// ambiguous; the edge id is not).
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeElementId {
    pub rel_type: String,
    pub edge_id: Vec<Option<String>>,
    pub ends: String,
}

/// Parse the element id of a relationship whose schema declares an
/// `edge_id` ([`relationship_element_id`]).
///
/// ```
/// use clickgraph::graph_catalog::element_id::{
///     edge_id_text, parse_edge_element_id, relationship_element_id,
/// };
///
/// let id = relationship_element_id("R", Some(&edge_id_text(&[Some("a:b"), None])), "1", "2");
/// let parsed = parse_edge_element_id(&id).unwrap();
/// assert_eq!(parsed.rel_type, "R");
/// assert_eq!(parsed.edge_id, vec![Some("a:b".to_string()), None]);
/// assert_eq!(parsed.ends, "1->2");
/// ```
pub fn parse_edge_element_id(element_id: &str) -> Result<EdgeElementId, ElementIdError> {
    // One trailing `-` is the sentinel; an end's text may end with another.
    let element_id = element_id
        .strip_suffix(relationship_element_id_separators().2)
        .unwrap_or(element_id);
    let (after_type, _, _) = relationship_element_id_separators();
    let (rel_type, rest) = element_id
        .split_once(after_type)
        .ok_or_else(|| ElementIdError::InvalidFormat("Expected 'TYPE:edge_id:ends'".into()))?;
    if rel_type.is_empty() {
        return Err(ElementIdError::MissingRelationshipType);
    }
    let (between_parts, after_edge) = edge_id_separators();
    let mut parts: Vec<Option<String>> = Vec::new();
    // The part being read: its text, or NULL (`\\N`, alone in its part).
    let mut part: Option<String> = Some(String::new());
    let mut chars = rest.char_indices();
    let ends = loop {
        let Some((i, c)) = chars.next() else {
            return Err(ElementIdError::InvalidFormat(
                "an edge id's element id has no ends after its edge id".into(),
            ));
        };
        let mut buf = [0u8; 4];
        let text = &*c.encode_utf8(&mut buf);
        if text == between_parts || text == after_edge {
            parts.push(part.replace(String::new()));
            if text == after_edge {
                break &rest[i + c.len_utf8()..];
            }
            continue;
        }
        let read = match (c, &mut part) {
            ('\\', Some(p)) => match chars.next().map(|(_, e)| e) {
                Some('N') if p.is_empty() => {
                    part = None;
                    continue;
                }
                Some(e) if edge_id_escapes().iter().any(|(f, _)| f.starts_with(e)) => {
                    p.push(e);
                    true
                }
                _ => false,
            },
            (_, Some(p)) => {
                p.push(c);
                true
            }
            // Text after a NULL part's `\\N`.
            (_, None) => false,
        };
        if !read {
            return Err(ElementIdError::InvalidFormat(
                "an edge id's part is not an escaped text".into(),
            ));
        }
    };
    if ends.is_empty() {
        return Err(ElementIdError::MissingFromId);
    }
    Ok(EdgeElementId {
        rel_type: rel_type.to_string(),
        edge_id: parts,
        ends: ends.to_string(),
    })
}

/// The text around a relationship's endpoint ids in its element id
/// ([`generate_relationship_element_id`]): before the `from` id, between the
/// two, after the `to` id.
pub fn relationship_element_id_affixes(rel_type: &str) -> (String, &'static str, &'static str) {
    let (after_type, between, suffix) = relationship_element_id_separators();
    (format!("{rel_type}{after_type}"), between, suffix)
}

/// The text after a relationship's type, between its endpoint ids and after
/// them in its element id ([`generate_relationship_element_id`]): the bound
/// plan spells the element id of a relationship whose type is a column (one
/// of several possible types) from them.
pub fn relationship_element_id_separators() -> (&'static str, &'static str, &'static str) {
    (":", "->", "-")
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
    // Strip the Neo4j-Browser-compat trailing `-` sentinel (see generator docs).
    let element_id = element_id.trim_end_matches('-');

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

    // Note: generated element_ids carry a trailing `-` Browser-compat sentinel.
    // Internal parse functions strip it, so all existing round-trip tests still
    // pass against the same input strings.

    #[test]
    fn test_generate_node_element_id_single_integer() {
        let element_id = generate_node_element_id("User", &["123"]);
        assert_eq!(element_id, "User:123-");
    }

    #[test]
    fn test_generate_node_element_id_single_string() {
        let element_id = generate_node_element_id("User", &["alice@example.com"]);
        assert_eq!(element_id, "User:alice@example.com-");
    }

    #[test]
    fn test_generate_node_element_id_single_uuid() {
        let element_id =
            generate_node_element_id("Post", &["550e8400-e29b-41d4-a716-446655440000"]);
        assert_eq!(element_id, "Post:550e8400-e29b-41d4-a716-446655440000-");
    }

    #[test]
    fn test_generate_node_element_id_composite_two() {
        let element_id = generate_node_element_id("Account", &["tenant_1", "456"]);
        assert_eq!(element_id, "Account:tenant_1|456-");
    }

    #[test]
    fn test_generate_node_element_id_composite_three() {
        let element_id = generate_node_element_id("Event", &["2024", "01", "15"]);
        assert_eq!(element_id, "Event:2024|01|15-");
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
        assert_eq!(element_id, "FOLLOWS:123->456-");
    }

    #[test]
    fn test_generate_relationship_element_id_strings() {
        let element_id =
            generate_relationship_element_id("AUTHORED", "alice@example.com", "post-uuid-123");
        assert_eq!(element_id, "AUTHORED:alice@example.com->post-uuid-123-");
    }

    #[test]
    fn test_generate_relationship_element_id_composite() {
        let element_id =
            generate_relationship_element_id("BELONGS_TO", "tenant_1|user_456", "tenant_1|org_789");
        assert_eq!(
            element_id,
            "BELONGS_TO:tenant_1|user_456->tenant_1|org_789-"
        );
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
        assert_eq!(element_id, "User:alice+bob@example.com-");

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
            "SENT_EMAIL:alice+test@example.com->bob@example.com-"
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
        assert_eq!(element_id, "Post:post:123:456-");

        let (label, ids) = parse_node_element_id(&element_id).unwrap();
        assert_eq!(label, "Post");
        // splitn(2, ':') will split only on first colon
        assert_eq!(ids, vec!["post:123:456"]);
    }

    // ========== Relationship elementId with an edge_id (#1334) ==========

    #[test]
    fn an_edge_id_is_in_its_relationships_element_id() {
        let edge = edge_id_text(&[Some("10")]);
        assert_eq!(edge, "10");
        assert_eq!(
            relationship_element_id("FOLLOWS", Some(&edge), "6", "7"),
            "FOLLOWS:10:6->7-"
        );
        // Parallel relationships: one per edge id.
        assert_ne!(
            relationship_element_id("FOLLOWS", Some(&edge_id_text(&[Some("10")])), "6", "7"),
            relationship_element_id("FOLLOWS", Some(&edge_id_text(&[Some("11")])), "6", "7"),
        );
        // Without one, as before.
        assert_eq!(
            relationship_element_id("FOLLOWS", None, "6", "7"),
            generate_relationship_element_id("FOLLOWS", "6", "7")
        );
    }

    #[test]
    fn an_edge_ids_parts_are_escaped() {
        assert_eq!(edge_id_text(&[Some("a:b")]), "a\\:b");
        assert_eq!(edge_id_text(&[Some("a|b"), Some("c")]), "a\\|b|c");
        assert_eq!(edge_id_text(&[Some("a\\"), None]), "a\\\\|\\N");
        assert_eq!(edge_id_text(&[Some(""), Some("")]), "|");
    }

    /// No two edge ids spell one text, wherever their parts' texts hold the
    /// separators, the escape, or the NULL part's text; each reads back.
    #[test]
    fn edge_ids_spell_texts_one_to_one() {
        let parts: [Option<&str>; 10] = [
            None,
            Some(""),
            Some("N"),
            Some("\\N"),
            Some(":"),
            Some("|"),
            Some("\\"),
            Some("a:b|c"),
            Some("->"),
            Some("x-"),
        ];
        let mut ids: Vec<Vec<Option<&str>>> = parts.iter().map(|p| vec![*p]).collect();
        for a in parts {
            for b in parts {
                ids.push(vec![a, b]);
            }
        }
        let mut texts = std::collections::HashMap::new();
        for id in &ids {
            let element_id = relationship_element_id("R", Some(&edge_id_text(id)), "1:2", "3-");
            if let Some(other) = texts.insert(element_id.clone(), id.clone()) {
                panic!("{id:?} and {other:?} both spell {element_id}");
            }
            let parsed = parse_edge_element_id(&element_id).unwrap();
            let read: Vec<Option<&str>> = parsed.edge_id.iter().map(Option::as_deref).collect();
            assert_eq!(&read, id, "{element_id}");
            assert_eq!(parsed.rel_type, "R");
            assert_eq!(parsed.ends, "1:2->3-", "{element_id}");
        }
    }

    #[test]
    fn an_edge_element_id_that_is_not_one_is_refused() {
        for bad in [
            "R:10",
            "R:1\\x:1->2-",
            "R:\\Nx:1->2-",
            "R:10:-",
            ":10:1->2-",
            "R10",
        ] {
            assert!(parse_edge_element_id(bad).is_err(), "{bad}");
        }
    }
}
