//! Deterministic ID Mapper for Neo4j Browser Compatibility
//!
//! Neo4j Browser uses `id(node)` function which expects integer IDs.
//! ClickGraph has string-based element_ids (e.g., "User:1", "Post:42").
//!
//! This module provides:
//! 1. Deterministic encoding: Label code + hash ensures globally unique IDs
//! 2. Session cache chaining: Cross-session lookups search other active sessions
//! 3. Automatic cleanup: Session caches are removed when connections close
//!
//! # ID Encoding
//!
//! ```text
//! ┌──────────────────────────────────────────────────┐
//! │  64-bit ID layout                                │
//! ├──────────┬───────────────────────────────────────┤
//! │  8 bits  │           56 bits                     │
//! │  label   │    hash(id_value) & 0x00FFFFFFFFFFFF  │
//! │  code    │                                       │
//! └──────────┴───────────────────────────────────────┘
//! ```
//!
//! This ensures "User:1" and "Post:1" have different IDs (different label codes).

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

// Use shared ID encoding utilities
use crate::utils::id_encoding::{IdEncoding, LABEL_CODE_REGISTRY};

lazy_static! {
    /// Registry of active session caches for cross-session lookups
    /// Maps connection_id → Arc<RwLock<SessionCache>>
    static ref ACTIVE_SESSION_CACHES: RwLock<HashMap<u64, Arc<RwLock<SessionCache>>>> =
        RwLock::new(HashMap::new());
}

/// Counter for generating unique connection IDs
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Session-local cache storing ID mappings for one connection
#[derive(Debug, Default)]
struct SessionCache {
    /// Map from element_id to assigned integer ID
    element_to_int: HashMap<String, i64>,
    /// Map from integer ID to element_id (for reverse lookup)
    int_to_element: HashMap<i64, String>,
}

/// Deterministic mapper between integer IDs and element_ids
///
/// Each connection gets its own IdMapper with a unique connection ID.
/// Cross-session lookups search other active sessions' caches.
#[derive(Debug)]
pub struct IdMapper {
    /// Unique connection ID for this mapper
    connection_id: u64,
    /// Shared reference to this session's cache (registered in ACTIVE_SESSION_CACHES)
    cache: Arc<RwLock<SessionCache>>,
}

impl Clone for IdMapper {
    fn clone(&self) -> Self {
        // Cloning shares the same cache and connection_id
        IdMapper {
            connection_id: self.connection_id,
            cache: Arc::clone(&self.cache),
        }
    }
}

impl Drop for IdMapper {
    fn drop(&mut self) {
        // Only unregister when this is the last reference to the cache
        // (Arc strong_count will be 2: one in ACTIVE_SESSION_CACHES, one here)
        if Arc::strong_count(&self.cache) <= 2 {
            if let Ok(mut registry) = ACTIVE_SESSION_CACHES.write() {
                registry.remove(&self.connection_id);
                log::debug!(
                    "IdMapper: Unregistered session {} (remaining sessions: {})",
                    self.connection_id,
                    registry.len()
                );
            }
        }
    }
}

impl IdMapper {
    /// Create a new IdMapper and register it in the global session registry
    pub fn new() -> Self {
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::SeqCst);
        let cache = Arc::new(RwLock::new(SessionCache::default()));

        // Register in the global session registry
        if let Ok(mut registry) = ACTIVE_SESSION_CACHES.write() {
            registry.insert(connection_id, Arc::clone(&cache));
            log::debug!(
                "IdMapper: Registered session {} (total sessions: {})",
                connection_id,
                registry.len()
            );
        }

        IdMapper {
            connection_id,
            cache,
        }
    }

    /// Get or compute a deterministic integer ID for the given element_id
    ///
    /// The ID encodes the label in the high byte and the hash of the id_value
    /// in the lower 56 bits, ensuring globally unique IDs across labels.
    pub fn get_or_assign(&mut self, element_id: &str) -> i64 {
        // Check local cache first (fastest)
        if let Ok(cache) = self.cache.read() {
            if let Some(&id) = cache.element_to_int.get(element_id) {
                return id;
            }
        }

        // Compute deterministic ID with label encoding
        let id = Self::compute_deterministic_id(element_id);

        // Cache locally
        if let Ok(mut cache) = self.cache.write() {
            cache.element_to_int.insert(element_id.to_string(), id);
            cache.int_to_element.insert(id, element_id.to_string());
        }

        log::debug!("IdMapper: {} -> {} (session {})", element_id, id, self.connection_id);
        id
    }

    /// Compute a deterministic integer ID from an element_id
    ///
    /// Format: "Label:id_value" or "Label:part1|part2" for composite keys
    ///
    /// Encodes the label in the high 8 bits and the id hash in the lower 56 bits.
    /// This ensures "User:1" and "Post:1" have different IDs.
    ///
    /// This is a public static method so result_transformer can use it as the single
    /// source of truth for ID generation across the codebase.
    pub fn compute_deterministic_id(element_id: &str) -> i64 {
        // Parse "Label:id_value" format
        let (label, id_part) = if let Some(colon_pos) = element_id.find(':') {
            (&element_id[..colon_pos], &element_id[colon_pos + 1..])
        } else {
            // No colon, use empty label and full string as id
            ("", element_id)
        };

        // Get or assign a label code (0-255)
        let label_code = if let Ok(mut registry) = LABEL_CODE_REGISTRY.write() {
            registry.get_or_assign(label)
        } else {
            0 // Fallback if lock fails
        };

        // Compute the id_value hash (56 bits)
        let id_hash = Self::compute_id_value_hash(id_part);

        // Combine: label_code in high 8 bits, id_hash in low 56 bits
        // Ensure positive by masking the sign bit
        let combined = ((label_code as i64) << 56) | (id_hash & 0x00FFFFFFFFFFFFFF);
        combined.abs().max(1)
    }

    /// Compute a 56-bit hash for the id_value portion
    ///
    /// For simple numeric IDs, use the number directly (if it fits in 56 bits).
    /// For strings/composite keys, use a hash.
    fn compute_id_value_hash(id_part: &str) -> i64 {
        // Check for composite key format "part1|part2"
        if id_part.contains('|') {
            return Self::hash_string(id_part);
        }

        // Try to parse as integer
        if let Ok(numeric_id) = id_part.parse::<i64>() {
            // Use the numeric ID directly if it fits in 56 bits
            if numeric_id >= 0 && numeric_id <= 0x00FFFFFFFFFFFFFF {
                return numeric_id.max(1);
            }
            // Large number, use hash
            return Self::hash_string(id_part);
        }

        // For non-numeric IDs (like "LAX"), use a hash
        Self::hash_string(id_part)
    }

    /// Hash a string to a positive 56-bit value
    fn hash_string(s: &str) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let hash = hasher.finish();
        // Mask to 56 bits and ensure positive
        ((hash & 0x00FFFFFFFFFFFFFF) as i64).abs().max(1)
    }

    /// Lookup element_id by integer ID (reverse lookup)
    ///
    /// Uses session cache chaining: first checks own cache, then searches
    /// other active sessions' caches.
    pub fn get_element_id(&self, id: i64) -> Option<String> {
        // Check own cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(element_id) = cache.int_to_element.get(&id) {
                return Some(element_id.clone());
            }
        }

        // Search other active sessions' caches (session cache chaining)
        if let Ok(registry) = ACTIVE_SESSION_CACHES.read() {
            for (&conn_id, session_cache) in registry.iter() {
                if conn_id == self.connection_id {
                    continue; // Skip own cache (already checked)
                }
                if let Ok(cache) = session_cache.read() {
                    if let Some(element_id) = cache.int_to_element.get(&id) {
                        log::debug!(
                            "IdMapper: Cross-session lookup found {} -> {} (from session {})",
                            id,
                            element_id,
                            conn_id
                        );
                        return Some(element_id.clone());
                    }
                }
            }
        }

        None
    }

    /// Get the integer ID for an element_id without assigning if missing
    #[allow(dead_code)]
    pub fn get_int_id(&self, element_id: &str) -> Option<i64> {
        if let Ok(cache) = self.cache.read() {
            return cache.element_to_int.get(element_id).copied();
        }
        None
    }

    /// Get the number of cached mappings
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        if let Ok(cache) = self.cache.read() {
            return cache.element_to_int.len();
        }
        0
    }

    /// Check if the mapper cache is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        if let Ok(cache) = self.cache.read() {
            return cache.element_to_int.is_empty();
        }
        true
    }

    /// Clear all cached mappings
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.element_to_int.clear();
            cache.int_to_element.clear();
        }
    }

    /// Get the connection ID for this mapper
    #[allow(dead_code)]
    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// Static function to look up element_id from encoded ID across all sessions
    ///
    /// This can be called from anywhere (e.g., FilterTagging) without needing
    /// an IdMapper instance. Searches all active session caches.
    pub fn static_lookup_element_id(encoded_id: i64) -> Option<String> {
        if let Ok(registry) = ACTIVE_SESSION_CACHES.read() {
            for (_conn_id, session_cache) in registry.iter() {
                if let Ok(cache) = session_cache.read() {
                    if let Some(element_id) = cache.int_to_element.get(&encoded_id) {
                        return Some(element_id.clone());
                    }
                }
            }
        }
        None
    }

    /// Decode an encoded ID to extract the label code and raw id value
    ///
    /// Returns (label_code, id_hash) where:
    /// - label_code is the 8-bit label identifier (1-255)
    /// - id_hash is the 56-bit value (could be raw ID or hash)
    ///
    /// For simple numeric IDs, id_hash IS the raw ID value.
    /// For complex IDs, id_hash is a hash and requires cache lookup.
    pub fn decode_id(encoded_id: i64) -> (u8, i64) {
        IdEncoding::decode(encoded_id)
    }

    /// Get the label name from a label code
    ///
    /// Returns the label string if found in the registry.
    pub fn get_label_from_code(label_code: u8) -> Option<String> {
        if let Ok(registry) = LABEL_CODE_REGISTRY.read() {
            registry.get_label(label_code)
        } else {
            None
        }
    }

    /// Check if an ID value appears to be an encoded ID (has label code in high bits)
    ///
    /// Returns true if the value has a non-zero label code (> 2^56)
    pub fn is_encoded_id(value: i64) -> bool {
        IdEncoding::is_encoded(value)
    }

    /// Try to decode an encoded ID to get the raw ID value for database lookup
    ///
    /// This is the key function for WHERE clause rewriting. Given an encoded ID,
    /// it returns the raw ID value that should be used in SQL comparisons.
    ///
    /// Priority:
    /// 1. Cache lookup - get exact element_id from session caches
    /// 2. Direct extraction - if id_hash looks like a simple numeric ID
    ///
    /// Returns (label, raw_id_value) if decodable, None otherwise.
    pub fn decode_for_query(encoded_id: i64) -> Option<(String, String)> {
        // First try cache lookup
        if let Some(element_id) = Self::static_lookup_element_id(encoded_id) {
            // Parse "Label:value" format
            if let Some(colon_pos) = element_id.find(':') {
                let label = element_id[..colon_pos].to_string();
                let value = element_id[colon_pos + 1..].to_string();
                return Some((label, value));
            }
        }

        // Fall back to direct extraction
        let (label_code, id_hash) = Self::decode_id(encoded_id);
        
        // If label_code is 0, this isn't an encoded ID
        if label_code == 0 {
            return None;
        }

        // Get the label name from the code
        let label = Self::get_label_from_code(label_code)?;

        // For simple numeric IDs (common case), id_hash IS the raw value
        // We can't distinguish hashed vs raw values, but for small numbers
        // (< 2^31), it's almost certainly the raw value
        if id_hash > 0 && id_hash < (1i64 << 31) {
            return Some((label, id_hash.to_string()));
        }

        // Large values might be hashed - we can't decode without cache
        // Return None and let the caller handle it
        None
    }

    /// Try to construct an element_id from an integer ID without prior mapping
    ///
    /// This is used for cross-session id() lookups. Given an integer ID and
    /// a list of known labels from the schema, we try to find a matching node.
    ///
    /// Returns a list of candidate element_ids to try (e.g., ["User:1", "Post:1"])
    pub fn generate_candidate_element_ids(&self, id: i64, labels: &[String]) -> Vec<String> {
        // First check if we have an exact mapping (including cross-session)
        if let Some(element_id) = self.get_element_id(id) {
            return vec![element_id];
        }

        // Extract the id_value from the lower 56 bits
        let id_value = id & 0x00FFFFFFFFFFFFFF;

        // Generate candidates: try each label with the ID value
        labels
            .iter()
            .map(|label| format!("{}:{}", label, id_value))
            .collect()
    }
}

impl Default for IdMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to extract label code from encoded ID
    fn get_label_code(id: i64) -> u8 {
        ((id >> 56) & 0xFF) as u8
    }

    /// Helper to extract id_value from encoded ID
    fn get_id_value(id: i64) -> i64 {
        id & 0x00FFFFFFFFFFFFFF
    }

    #[test]
    fn test_get_or_assign_new_numeric() {
        // For numeric IDs like "User:1", the label code is in high byte
        // and id_value (1) is in low 56 bits
        let mut mapper = IdMapper::new();
        let id = mapper.get_or_assign("User:1");

        // The id_value portion should be 1
        assert_eq!(get_id_value(id), 1);
        // There should be a label code assigned
        assert!(id > 0);
    }

    #[test]
    fn test_get_or_assign_non_numeric() {
        // For non-numeric IDs like "Airport:LAX", a hash is used for id_value
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("Airport:LAX");
        let id2 = mapper.get_or_assign("Airport:LAX");

        // Hash should be consistent
        assert_eq!(id1, id2);
        // The id_value should be a hash (large number)
        assert!(get_id_value(id1) > 1000);
    }

    #[test]
    fn test_get_or_assign_existing() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("User:42");
        let id2 = mapper.get_or_assign("User:42");

        // Same element_id should return same ID
        assert_eq!(id1, id2);
        // The id_value portion should be 42
        assert_eq!(get_id_value(id1), 42);
    }

    #[test]
    fn test_label_code_prevents_collision() {
        // Critical test: "User:1" and "Post:1" should have DIFFERENT IDs
        let mut mapper = IdMapper::new();
        let user_id = mapper.get_or_assign("User:1");
        let post_id = mapper.get_or_assign("Post:1");

        // They must be different (different label codes)
        assert_ne!(user_id, post_id);

        // But the id_value portion should be the same (both are 1)
        assert_eq!(get_id_value(user_id), 1);
        assert_eq!(get_id_value(post_id), 1);

        // Different label codes
        assert_ne!(get_label_code(user_id), get_label_code(post_id));
    }

    #[test]
    fn test_numeric_ids() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("User:1");
        let id2 = mapper.get_or_assign("Post:2");
        let id3 = mapper.get_or_assign("Comment:3");

        // id_values are extracted directly
        assert_eq!(get_id_value(id1), 1);
        assert_eq!(get_id_value(id2), 2);
        assert_eq!(get_id_value(id3), 3);

        // Verify reverse lookup works
        assert_eq!(mapper.get_element_id(id1), Some("User:1".to_string()));
        assert_eq!(mapper.get_element_id(id2), Some("Post:2".to_string()));
        assert_eq!(mapper.get_element_id(id3), Some("Comment:3".to_string()));
    }

    #[test]
    fn test_consistency() {
        let mut mapper = IdMapper::new();

        // Interleave assignments
        let user1_a = mapper.get_or_assign("User:1");
        let post2 = mapper.get_or_assign("Post:2");
        let user1_b = mapper.get_or_assign("User:1");
        let comment3 = mapper.get_or_assign("Comment:3");
        let user1_c = mapper.get_or_assign("User:1");

        // Same element_id should always get the same ID
        assert_eq!(user1_a, user1_b);
        assert_eq!(user1_b, user1_c);

        // Different labels get different IDs even with same numeric value
        assert_ne!(user1_a, post2);
        assert_ne!(post2, comment3);
    }

    #[test]
    fn test_session_cache_chaining() {
        // Create mapper 1, assign some IDs
        let mut mapper1 = IdMapper::new();
        let id1 = mapper1.get_or_assign("User:42");

        // id_value should be 42
        assert_eq!(get_id_value(id1), 42);

        // Create mapper 2 (simulating new connection)
        let mapper2 = IdMapper::new();

        // Should be able to look up from mapper1's cache via chaining
        let element_id = mapper2.get_element_id(id1);
        assert_eq!(element_id, Some("User:42".to_string()));
    }

    #[test]
    fn test_session_registration() {
        // Create a mapper and verify it has a valid connection_id
        let mapper = IdMapper::new();
        let conn_id = mapper.connection_id();
        assert!(conn_id > 0);

        // Verify the mapper is registered in the session cache
        let registered = {
            let registry = ACTIVE_SESSION_CACHES.read().unwrap();
            registry.contains_key(&conn_id)
        };
        assert!(registered, "IdMapper should be registered in ACTIVE_SESSION_CACHES");
    }

    #[test]
    fn test_connection_id_unique() {
        let mapper1 = IdMapper::new();
        let mapper2 = IdMapper::new();

        // Each mapper should have a unique connection ID
        assert_ne!(mapper1.connection_id(), mapper2.connection_id());
    }
}
