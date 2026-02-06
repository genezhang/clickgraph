//! Neo4j-compatible ID Encoding Utilities
//!
//! This module provides encoding and decoding for Neo4j-style integer IDs.
//! Used for Neo4j Browser compatibility where nodes and relationships are
//! identified by integer IDs rather than element_id strings.
//!
//! ## Encoding Scheme
//!
//! 64-bit ID layout:
//! ```text
//! [8-bit label_code][56-bit id_value]
//! ```
//!
//! - **label_code** (bits 56-63): Identifies the node label or relationship type (1-255)
//! - **id_value** (bits 0-55): The actual ID value (raw for simple IDs, hash for complex)
//!
//! Label codes start at 1 (not 0) so ALL encoded IDs are distinguishable from raw values.
//! A raw value like `1` stays `1`, while an encoded User:1 becomes `(1 << 56) | 1`.
//!
//! ## Usage
//!
//! ```ignore
//! // Encode: label "User" with code 1, raw id 42
//! let encoded = IdEncoding::encode(1, 42);
//! assert_eq!(encoded, 72057594037927978); // (1 << 56) | 42
//!
//! // Decode: extract label_code and id_value
//! let (label_code, id_value) = IdEncoding::decode(72057594037927978);
//! assert_eq!(label_code, 1);
//! assert_eq!(id_value, 42);
//!
//! // Check if a value is encoded (has non-zero high bits)
//! assert!(IdEncoding::is_encoded(72057594037927978));
//! assert!(!IdEncoding::is_encoded(42));
//! ```

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::RwLock;

lazy_static! {
    /// Global registry mapping label codes to label names.
    /// This is the single source of truth for label↔code mappings.
    ///
    /// Populated when nodes/relationships are first seen (during result transformation).
    /// Used for decoding encoded IDs back to labels (during query rewriting).
    pub static ref LABEL_CODE_REGISTRY: RwLock<LabelCodeRegistry> =
        RwLock::new(LabelCodeRegistry::new());
}

/// Registry that assigns unique 8-bit codes to label/type names
#[derive(Debug)]
pub struct LabelCodeRegistry {
    /// Label name → code (1-255)
    label_to_code: HashMap<String, u8>,
    /// Code → label name (reverse lookup)
    code_to_label: HashMap<u8, String>,
    /// Next code to assign
    next_code: u8,
}

impl LabelCodeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        LabelCodeRegistry {
            label_to_code: HashMap::new(),
            code_to_label: HashMap::new(),
            // Start at 1, not 0, so ALL encoded IDs are distinguishable from raw values.
            // With code=0, User:1 would become just 1, defeating the encoding purpose.
            next_code: 1,
        }
    }

    /// Get or assign a code for a label (1-255)
    /// Returns 255 if we've exhausted all codes (overflow)
    pub fn get_or_assign(&mut self, label: &str) -> u8 {
        if let Some(&code) = self.label_to_code.get(label) {
            return code;
        }

        // Assign new code (starts at 1)
        let code = self.next_code;
        if self.next_code < 255 {
            self.next_code += 1;
        }
        // At 255, we stop incrementing (overflow protection)
        self.label_to_code.insert(label.to_string(), code);
        self.code_to_label.insert(code, label.to_string());
        code
    }

    /// Get the code for a label without assigning a new one
    pub fn get_code(&self, label: &str) -> Option<u8> {
        self.label_to_code.get(label).copied()
    }

    /// Get the label name for a given code (reverse lookup)
    pub fn get_label(&self, code: u8) -> Option<String> {
        self.code_to_label.get(&code).cloned()
    }
}

impl Default for LabelCodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// ID encoding/decoding utilities
pub struct IdEncoding;

impl IdEncoding {
    /// Encode a label code and ID value into a single 64-bit integer
    ///
    /// # Arguments
    /// * `label_code` - 8-bit label identifier (1-255)
    /// * `id_value` - 56-bit ID value (raw ID or hash)
    ///
    /// # Returns
    /// Encoded 64-bit ID with label code in high bits
    #[inline]
    pub fn encode(label_code: u8, id_value: i64) -> i64 {
        ((label_code as i64) << 56) | (id_value & 0x00FFFFFFFFFFFFFF)
    }

    /// Decode an encoded ID to extract label code and ID value
    ///
    /// # Arguments
    /// * `encoded_id` - The encoded 64-bit ID
    ///
    /// # Returns
    /// Tuple of (label_code, id_value) where:
    /// - label_code is 1-255 for encoded IDs, 0 for raw values
    /// - id_value is the lower 56 bits
    #[inline]
    pub fn decode(encoded_id: i64) -> (u8, i64) {
        let label_code = ((encoded_id >> 56) & 0xFF) as u8;
        let id_value = encoded_id & 0x00FFFFFFFFFFFFFF;
        (label_code, id_value)
    }

    /// Check if a value appears to be an encoded ID (has label code in high bits)
    ///
    /// Returns true if the value has a non-zero label code (value > 2^56)
    #[inline]
    pub fn is_encoded(value: i64) -> bool {
        value > 0 && (value >> 56) > 0
    }

    /// Get the label name for an encoded ID using the global registry
    ///
    /// # Arguments
    /// * `encoded_id` - The encoded 64-bit ID
    ///
    /// # Returns
    /// Some((label, raw_id)) if the ID is encoded and label is found
    /// None if the ID is not encoded or label not in registry
    pub fn decode_with_label(encoded_id: i64) -> Option<(String, i64)> {
        if !Self::is_encoded(encoded_id) {
            return None;
        }

        let (label_code, id_value) = Self::decode(encoded_id);

        if let Ok(registry) = LABEL_CODE_REGISTRY.read() {
            if let Some(label) = registry.get_label(label_code) {
                return Some((label, id_value));
            }
        }

        None
    }

    /// Register a label and get its code (thread-safe)
    ///
    /// # Arguments
    /// * `label` - The label name to register
    ///
    /// # Returns
    /// The assigned code (1-255)
    pub fn register_label(label: &str) -> u8 {
        if let Ok(mut registry) = LABEL_CODE_REGISTRY.write() {
            registry.get_or_assign(label)
        } else {
            255 // Fallback on lock failure
        }
    }

    /// Get the code for a label without registering (thread-safe)
    pub fn get_label_code(label: &str) -> Option<u8> {
        if let Ok(registry) = LABEL_CODE_REGISTRY.read() {
            registry.get_code(label)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let label_code = 1u8;
        let id_value = 42i64;

        let encoded = IdEncoding::encode(label_code, id_value);
        let (decoded_code, decoded_value) = IdEncoding::decode(encoded);

        assert_eq!(decoded_code, label_code);
        assert_eq!(decoded_value, id_value);
    }

    #[test]
    fn test_is_encoded() {
        // Raw values are not encoded
        assert!(!IdEncoding::is_encoded(0));
        assert!(!IdEncoding::is_encoded(1));
        assert!(!IdEncoding::is_encoded(1000000));

        // Encoded values have label code in high bits
        let encoded = IdEncoding::encode(1, 42);
        assert!(IdEncoding::is_encoded(encoded));
    }

    #[test]
    fn test_label_code_starts_at_1() {
        let mut registry = LabelCodeRegistry::new();

        let code1 = registry.get_or_assign("User");
        let code2 = registry.get_or_assign("Post");

        assert_eq!(code1, 1);
        assert_eq!(code2, 2);

        // User:1 should be distinguishable from raw 1
        let encoded_user_1 = IdEncoding::encode(code1, 1);
        assert!(IdEncoding::is_encoded(encoded_user_1));
        assert_ne!(encoded_user_1, 1i64);
    }

    #[test]
    fn test_reverse_lookup() {
        let mut registry = LabelCodeRegistry::new();

        registry.get_or_assign("User");
        registry.get_or_assign("Post");

        assert_eq!(registry.get_label(1), Some("User".to_string()));
        assert_eq!(registry.get_label(2), Some("Post".to_string()));
        assert_eq!(registry.get_label(3), None);
    }

    #[test]
    fn test_max_id_value() {
        // Ensure 56-bit max value is handled correctly
        let max_56bit = 0x00FFFFFFFFFFFFFFi64;
        let encoded = IdEncoding::encode(1, max_56bit);
        let (code, value) = IdEncoding::decode(encoded);

        assert_eq!(code, 1);
        assert_eq!(value, max_56bit);
    }
}
