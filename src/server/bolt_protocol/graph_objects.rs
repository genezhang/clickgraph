//! Neo4j Bolt 5.x Graph Objects
//!
//! This module defines Node, Relationship, and Path structures compatible with
//! Neo4j Bolt protocol 5.x. These objects use packstream encoding and include
//! the new STRING-based elementId field.
//!
//! # Packstream Encoding
//!
//! Bolt 5.x uses packstream binary encoding for graph objects:
//!
//! ## Node Structure
//! ```text
//! Signature: 0xB4 (4-field struct) + 0x4E ('N')
//! Fields:
//!   1. id: Integer (legacy, can be 0)
//!   2. labels: List<String>
//!   3. properties: Map<String, Value>
//!   4. elementId: String
//! ```
//!
//! ## Relationship Structure
//! ```text
//! Signature: 0xB8 (8-field struct) + 0x52 ('R')
//! Fields:
//!   1. id: Integer (legacy, can be 0)
//!   2. startNodeId: Integer (legacy)
//!   3. endNodeId: Integer (legacy)
//!   4. type: String
//!   5. properties: Map<String, Value>
//!   6. elementId: String
//!   7. startNodeElementId: String
//!   8. endNodeElementId: String
//! ```
//!
//! # Usage
//!
//! ```rust
//! use clickgraph::server::bolt_protocol::graph_objects::Node;
//! use std::collections::HashMap;
//! use serde_json::Value;
//!
//! let mut properties = HashMap::new();
//! properties.insert("user_id".to_string(), Value::Number(123.into()));
//! properties.insert("name".to_string(), Value::String("Alice".to_string()));
//!
//! let node = Node {
//!     id: 0,  // Legacy ID (unused in modern Neo4j)
//!     labels: vec!["User".to_string()],
//!     properties,
//!     element_id: "User:123".to_string(),
//! };
//!
//! // Encode to packstream bytes
//! let encoded = node.to_packstream();
//! ```

use serde_json::Value;
use std::collections::HashMap;

/// Neo4j Bolt 5.x Node structure
///
/// Represents a graph node with labels, properties, and Neo4j 5.0+ elementId.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// Legacy integer ID (can be 0 or hash for compatibility)
    pub id: i64,
    /// Node labels (e.g., ["User", "Person"])
    pub labels: Vec<String>,
    /// Node properties as JSON values
    pub properties: HashMap<String, Value>,
    /// Neo4j 5.0+ STRING-based element ID
    /// Format: "Label:id" or "Label:id1|id2|id3" for composite IDs
    pub element_id: String,
}

/// Neo4j Bolt 5.x Relationship structure
///
/// Represents a graph relationship with type, properties, and Neo4j 5.0+ elementIds.
#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    /// Legacy integer ID (can be 0 for compatibility)
    pub id: i64,
    /// Legacy start node ID
    pub start_node_id: i64,
    /// Legacy end node ID
    pub end_node_id: i64,
    /// Relationship type (e.g., "FOLLOWS", "AUTHORED")
    pub rel_type: String,
    /// Relationship properties as JSON values
    pub properties: HashMap<String, Value>,
    /// Neo4j 5.0+ STRING-based element ID for this relationship
    /// Format: "Type:from_id->to_id"
    pub element_id: String,
    /// Neo4j 5.0+ STRING-based element ID for start node
    pub start_node_element_id: String,
    /// Neo4j 5.0+ STRING-based element ID for end node
    pub end_node_element_id: String,
}

/// Neo4j Bolt Path structure
///
/// Represents a graph path as a sequence of alternating nodes and relationships.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// Nodes in the path
    pub nodes: Vec<Node>,
    /// Relationships connecting the nodes
    pub relationships: Vec<Relationship>,
    /// Indices describing the path structure
    /// Each pair (node_idx, rel_idx) indicates the path segment
    pub indices: Vec<i64>,
}

impl Node {
    /// Create a new Node
    pub fn new(
        id: i64,
        labels: Vec<String>,
        properties: HashMap<String, Value>,
        element_id: String,
    ) -> Self {
        Node {
            id,
            labels,
            properties,
            element_id,
        }
    }

    /// Encode this Node to packstream format
    ///
    /// Format: 0xB4 (4-field struct) + 0x4E ('N') + [id, labels, properties, elementId]
    ///
    /// # Returns
    ///
    /// A Vec<u8> containing the packstream-encoded bytes
    pub fn to_packstream(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Struct signature: 0xB4 (4 fields) + 0x4E ('N')
        bytes.push(0xB4);
        bytes.push(0x4E);

        // Field 1: id (Integer)
        bytes.extend_from_slice(&encode_integer(self.id));

        // Field 2: labels (List of Strings)
        bytes.extend_from_slice(&encode_string_list(&self.labels));

        // Field 3: properties (Map)
        bytes.extend_from_slice(&encode_properties_map(&self.properties));

        // Field 4: elementId (String)
        bytes.extend_from_slice(&encode_string(&self.element_id));

        bytes
    }
}

impl Relationship {
    /// Create a new Relationship
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        start_node_id: i64,
        end_node_id: i64,
        rel_type: String,
        properties: HashMap<String, Value>,
        element_id: String,
        start_node_element_id: String,
        end_node_element_id: String,
    ) -> Self {
        Relationship {
            id,
            start_node_id,
            end_node_id,
            rel_type,
            properties,
            element_id,
            start_node_element_id,
            end_node_element_id,
        }
    }

    /// Encode this Relationship to packstream format
    ///
    /// Format: 0xB8 (8-field struct) + 0x52 ('R') + [8 fields]
    ///
    /// # Returns
    ///
    /// A Vec<u8> containing the packstream-encoded bytes
    pub fn to_packstream(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Struct signature: 0xB8 (8 fields) + 0x52 ('R')
        bytes.push(0xB8);
        bytes.push(0x52);

        // Field 1: id (Integer)
        bytes.extend_from_slice(&encode_integer(self.id));

        // Field 2: startNodeId (Integer)
        bytes.extend_from_slice(&encode_integer(self.start_node_id));

        // Field 3: endNodeId (Integer)
        bytes.extend_from_slice(&encode_integer(self.end_node_id));

        // Field 4: type (String)
        bytes.extend_from_slice(&encode_string(&self.rel_type));

        // Field 5: properties (Map)
        bytes.extend_from_slice(&encode_properties_map(&self.properties));

        // Field 6: elementId (String)
        bytes.extend_from_slice(&encode_string(&self.element_id));

        // Field 7: startNodeElementId (String)
        bytes.extend_from_slice(&encode_string(&self.start_node_element_id));

        // Field 8: endNodeElementId (String)
        bytes.extend_from_slice(&encode_string(&self.end_node_element_id));

        bytes
    }
}

// ========== Packstream Encoding Helpers ==========

// ========== Packstream Encoding Helpers ==========
//
// Custom Packstream Implementation Rationale:
//
// We implement Packstream encoding directly (vs using crates like `packs` or `neo4j_rust_bolt`)
// for several reasons:
//
// 1. **Precise Control**: Need exact byte-level control for Bolt 5.x graph objects
//    - Custom elementId format: "Label:id1|id2|id3" for composite IDs
//    - Tight integration with ClickGraph's schema type system
//    - Direct control over Node/Relationship structure encoding
//
// 2. **Minimal Dependencies**: ~200 LOC implementation with zero external deps
//    - No version conflicts or breaking changes risk
//    - Faster compilation, smaller binary
//    - Easier to debug and maintain
//
// 3. **Available Crates Limitations**:
//    - `packstream 0.0.0`: Appears unfinished/placeholder
//    - `packs`: General-purpose, would need adapter layer anyway
//    - `raio`/`neo4j_rust_bolt`: Full drivers (overkill for encoding layer)
//
// 4. **Performance**: Zero abstraction overhead - direct byte vector generation
//
// If migrating to external crate in future: benchmark performance, add adapter layer,
// test all schema variations (composite IDs, denormalized, polymorphic, etc.)
//
// Spec reference: https://neo4j.com/docs/bolt/current/packstream/

/// Encode an integer to packstream format
///
/// Packstream integer encoding:
/// - -16..127: Single byte (signed)
/// - Other: 0xC8 + 1 byte, 0xC9 + 2 bytes (BE), 0xCA + 4 bytes (BE), 0xCB + 8 bytes (BE)
fn encode_integer(value: i64) -> Vec<u8> {
    if (-16..=127).contains(&value) {
        // TINY_INT: Single byte
        vec![value as u8]
    } else if i8::MIN as i64 <= value && value <= i8::MAX as i64 {
        // INT_8: 0xC8 + 1 byte
        vec![0xC8, value as u8]
    } else if i16::MIN as i64 <= value && value <= i16::MAX as i64 {
        // INT_16: 0xC9 + 2 bytes (big-endian)
        let bytes = (value as i16).to_be_bytes();
        vec![0xC9, bytes[0], bytes[1]]
    } else if i32::MIN as i64 <= value && value <= i32::MAX as i64 {
        // INT_32: 0xCA + 4 bytes (big-endian)
        let bytes = (value as i32).to_be_bytes();
        vec![0xCA, bytes[0], bytes[1], bytes[2], bytes[3]]
    } else {
        // INT_64: 0xCB + 8 bytes (big-endian)
        let bytes = value.to_be_bytes();
        let mut result = vec![0xCB];
        result.extend_from_slice(&bytes);
        result
    }
}

/// Encode a string to packstream format
///
/// Packstream string encoding:
/// - 0..15 bytes: 0x8X + bytes (where X = length)
/// - 16..255 bytes: 0xD0 + 1-byte length + bytes
/// - 256..65535 bytes: 0xD1 + 2-byte length (BE) + bytes
/// - 65536+: 0xD2 + 4-byte length (BE) + bytes
fn encode_string(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let len = bytes.len();

    let mut result = Vec::new();

    if len < 16 {
        // TINY_STRING: 0x80..0x8F
        result.push(0x80 | (len as u8));
    } else if len <= 255 {
        // STRING_8: 0xD0 + 1-byte length
        result.push(0xD0);
        result.push(len as u8);
    } else if len <= 65535 {
        // STRING_16: 0xD1 + 2-byte length (big-endian)
        result.push(0xD1);
        result.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        // STRING_32: 0xD2 + 4-byte length (big-endian)
        result.push(0xD2);
        result.extend_from_slice(&(len as u32).to_be_bytes());
    }

    result.extend_from_slice(bytes);
    result
}

/// Encode a list of strings to packstream format
fn encode_string_list(strings: &[String]) -> Vec<u8> {
    let mut result = Vec::new();
    let len = strings.len();

    // List header
    if len < 16 {
        // TINY_LIST: 0x90..0x9F
        result.push(0x90 | (len as u8));
    } else if len <= 255 {
        // LIST_8: 0xD4 + 1-byte length
        result.push(0xD4);
        result.push(len as u8);
    } else if len <= 65535 {
        // LIST_16: 0xD5 + 2-byte length
        result.push(0xD5);
        result.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        // LIST_32: 0xD6 + 4-byte length
        result.push(0xD6);
        result.extend_from_slice(&(len as u32).to_be_bytes());
    }

    // List items
    for s in strings {
        result.extend_from_slice(&encode_string(s));
    }

    result
}

/// Encode a properties map to packstream format
fn encode_properties_map(properties: &HashMap<String, Value>) -> Vec<u8> {
    let mut result = Vec::new();
    let len = properties.len();

    // Map header
    if len < 16 {
        // TINY_MAP: 0xA0..0xAF
        result.push(0xA0 | (len as u8));
    } else if len <= 255 {
        // MAP_8: 0xD8 + 1-byte length
        result.push(0xD8);
        result.push(len as u8);
    } else if len <= 65535 {
        // MAP_16: 0xD9 + 2-byte length
        result.push(0xD9);
        result.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        // MAP_32: 0xDA + 4-byte length
        result.push(0xDA);
        result.extend_from_slice(&(len as u32).to_be_bytes());
    }

    // Map entries (key-value pairs)
    for (key, value) in properties {
        result.extend_from_slice(&encode_string(key));
        result.extend_from_slice(&encode_json_value(value));
    }

    result
}

/// Encode a JSON value to packstream format
fn encode_json_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0xC0],        // NULL
        Value::Bool(true) => vec![0xC3],  // TRUE
        Value::Bool(false) => vec![0xC2], // FALSE
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                encode_integer(i)
            } else if let Some(f) = n.as_f64() {
                // FLOAT_64: 0xC1 + 8 bytes (big-endian)
                let mut result = vec![0xC1];
                result.extend_from_slice(&f.to_be_bytes());
                result
            } else {
                // Fallback to 0 if neither i64 nor f64
                encode_integer(0)
            }
        }
        Value::String(s) => encode_string(s),
        Value::Array(_arr) => {
            // TODO: Implement array encoding if needed
            // For now, encode as empty list
            vec![0x90] // TINY_LIST with 0 items
        }
        Value::Object(_obj) => {
            // TODO: Implement nested object encoding if needed
            // For now, encode as empty map
            vec![0xA0] // TINY_MAP with 0 entries
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_integer_tiny() {
        // TINY_INT: -16..127
        assert_eq!(encode_integer(0), vec![0x00]);
        assert_eq!(encode_integer(1), vec![0x01]);
        assert_eq!(encode_integer(127), vec![0x7F]);
        assert_eq!(encode_integer(-1), vec![0xFF]);
        assert_eq!(encode_integer(-16), vec![0xF0]);
    }

    #[test]
    fn test_encode_integer_int8() {
        // INT_8: beyond TINY_INT range
        assert_eq!(encode_integer(-17), vec![0xC8, 0xEF]); // -17 as i8
        assert_eq!(encode_integer(-128), vec![0xC8, 0x80]); // i8::MIN
    }

    #[test]
    fn test_encode_integer_int16() {
        // INT_16
        assert_eq!(encode_integer(128), vec![0xC9, 0x00, 0x80]);
        assert_eq!(encode_integer(32767), vec![0xC9, 0x7F, 0xFF]);
        assert_eq!(encode_integer(-129), vec![0xC9, 0xFF, 0x7F]);
    }

    #[test]
    fn test_encode_integer_int32() {
        // INT_32
        assert_eq!(encode_integer(65536), vec![0xCA, 0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn test_encode_string_tiny() {
        // TINY_STRING: 0..15 bytes
        assert_eq!(encode_string(""), vec![0x80]);
        assert_eq!(encode_string("A"), vec![0x81, b'A']);
        assert_eq!(
            encode_string("Hello"),
            vec![0x85, b'H', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn test_encode_string_8() {
        // STRING_8: 16..255 bytes
        let long_str = "x".repeat(20);
        let encoded = encode_string(&long_str);
        assert_eq!(encoded[0], 0xD0); // STRING_8 marker
        assert_eq!(encoded[1], 20); // Length
        assert_eq!(encoded.len(), 2 + 20);
    }

    #[test]
    fn test_encode_string_list_empty() {
        assert_eq!(encode_string_list(&[]), vec![0x90]); // TINY_LIST with 0 items
    }

    #[test]
    fn test_encode_string_list_single() {
        let list = vec!["User".to_string()];
        let encoded = encode_string_list(&list);
        assert_eq!(encoded[0], 0x91); // TINY_LIST with 1 item
                                      // Followed by encoded "User" string
        assert_eq!(&encoded[1..], &[0x84, b'U', b's', b'e', b'r']);
    }

    #[test]
    fn test_encode_string_list_multiple() {
        let list = vec!["User".to_string(), "Person".to_string()];
        let encoded = encode_string_list(&list);
        assert_eq!(encoded[0], 0x92); // TINY_LIST with 2 items
    }

    #[test]
    fn test_node_packstream_encoding() {
        let mut properties = HashMap::new();
        properties.insert("user_id".to_string(), Value::Number(123.into()));
        properties.insert("name".to_string(), Value::String("Alice".to_string()));

        let node = Node::new(
            0,
            vec!["User".to_string()],
            properties,
            "User:123".to_string(),
        );

        let encoded = node.to_packstream();

        // Verify structure signature
        assert_eq!(encoded[0], 0xB4); // 4-field struct
        assert_eq!(encoded[1], 0x4E); // 'N' for Node

        // Field 1: id = 0
        assert_eq!(encoded[2], 0x00); // TINY_INT 0

        // Field 2: labels = ["User"] starts at byte 3
        assert_eq!(encoded[3], 0x91); // TINY_LIST with 1 item
        assert_eq!(encoded[4], 0x84); // TINY_STRING with 4 bytes
        assert_eq!(&encoded[5..9], b"User");

        // Remaining fields: properties map and elementId string
        // (detailed validation omitted for brevity)
    }

    #[test]
    fn test_relationship_packstream_encoding() {
        let properties = HashMap::new();

        let rel = Relationship::new(
            0,
            0,
            0,
            "FOLLOWS".to_string(),
            properties,
            "FOLLOWS:123->456".to_string(),
            "User:123".to_string(),
            "User:456".to_string(),
        );

        let encoded = rel.to_packstream();

        // Verify structure signature
        assert_eq!(encoded[0], 0xB8); // 8-field struct
        assert_eq!(encoded[1], 0x52); // 'R' for Relationship

        // Field 1: id = 0
        assert_eq!(encoded[2], 0x00); // TINY_INT 0

        // Field 2: startNodeId = 0
        assert_eq!(encoded[3], 0x00);

        // Field 3: endNodeId = 0
        assert_eq!(encoded[4], 0x00);

        // Field 4: type = "FOLLOWS"
        assert_eq!(encoded[5], 0x87); // TINY_STRING with 7 bytes
        assert_eq!(&encoded[6..13], b"FOLLOWS");
    }

    #[test]
    fn test_node_with_composite_element_id() {
        let properties = HashMap::new();

        let node = Node::new(
            0,
            vec!["Account".to_string()],
            properties,
            "Account:tenant_1|456".to_string(), // Composite ID
        );

        let encoded = node.to_packstream();

        // Just verify it encodes without panic
        assert!(encoded.len() > 10);
        assert_eq!(encoded[0], 0xB4);
        assert_eq!(encoded[1], 0x4E);
    }

    #[test]
    fn test_node_with_multiple_labels() {
        let properties = HashMap::new();

        let node = Node::new(
            0,
            vec!["User".to_string(), "Person".to_string()],
            properties,
            "User:123".to_string(),
        );

        let encoded = node.to_packstream();

        // Verify 2 labels
        assert_eq!(encoded[3], 0x92); // TINY_LIST with 2 items
    }
}
