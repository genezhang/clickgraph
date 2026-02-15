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
//! # ID Encoding (JavaScript-Safe)
//!
//! ```text
//! ┌──────────────────────────────────────────────────┐
//! │  53-bit ID layout (within JS MAX_SAFE_INTEGER)   │
//! ├──────────┬───────────────────────────────────────┤
//! │  6 bits  │           47 bits                     │
//! │  label   │    id_value (raw or hash)             │
//! │  code    │    max: 140 trillion                  │
//! └──────────┴───────────────────────────────────────┘
//! ```
//!
//! This ensures "User:1" and "Post:1" have different IDs (different label codes),
//! while keeping all IDs within JavaScript's safe integer range (2^53 - 1).

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

// Use shared ID encoding utilities
use crate::utils::id_encoding::{IdEncoding, LABEL_CODE_REGISTRY};

lazy_static! {
    /// Registry of active session entries for cross-session lookups.
    /// Maps connection_id → SessionEntry (cache + scope metadata).
    /// Cross-session lookups only chain to sessions with matching scope
    /// (same schema_name and tenant_id) to prevent cross-tenant data leakage.
    static ref ACTIVE_SESSION_CACHES: RwLock<HashMap<u64, SessionEntry>> =
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

/// A registered session entry: cache + scope metadata for cross-session filtering.
#[derive(Debug)]
struct SessionEntry {
    cache: Arc<RwLock<SessionCache>>,
    schema_name: Option<String>,
    tenant_id: Option<String>,
}

/// Deterministic mapper between integer IDs and element_ids
///
/// Each connection gets its own IdMapper with a unique connection ID.
/// Cross-session lookups search other active sessions' caches, but only
/// those with matching scope (schema_name + tenant_id).
#[derive(Debug)]
pub struct IdMapper {
    /// Unique connection ID for this mapper
    connection_id: u64,
    /// Shared reference to this session's cache (registered in ACTIVE_SESSION_CACHES)
    cache: Arc<RwLock<SessionCache>>,
    /// Schema scope for cross-session filtering
    schema_name: Option<String>,
    /// Tenant scope for cross-session filtering
    tenant_id: Option<String>,
}

impl Clone for IdMapper {
    fn clone(&self) -> Self {
        // Cloning shares the same cache, connection_id, and scope
        IdMapper {
            connection_id: self.connection_id,
            cache: Arc::clone(&self.cache),
            schema_name: self.schema_name.clone(),
            tenant_id: self.tenant_id.clone(),
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
            registry.insert(
                connection_id,
                SessionEntry {
                    cache: Arc::clone(&cache),
                    schema_name: None,
                    tenant_id: None,
                },
            );
            log::debug!(
                "IdMapper: Registered session {} (total sessions: {})",
                connection_id,
                registry.len()
            );
        }

        IdMapper {
            connection_id,
            cache,
            schema_name: None,
            tenant_id: None,
        }
    }

    /// Update the scope (schema + tenant) for this session.
    /// Called when schema_name or tenant_id becomes known (HELLO, LOGON, or RUN).
    /// Updates both the local fields and the global registry entry.
    /// Clears the session cache when scope changes to prevent stale mappings
    /// from a previous scope leaking into the new one.
    pub fn set_scope(&mut self, schema_name: Option<String>, tenant_id: Option<String>) {
        let scope_changed = self.schema_name != schema_name || self.tenant_id != tenant_id;

        self.schema_name = schema_name.clone();
        self.tenant_id = tenant_id.clone();

        // Clear cache when scope changes so stale reverse mappings don't bleed across
        if scope_changed {
            if let Ok(mut cache) = self.cache.write() {
                cache.element_to_int.clear();
                cache.int_to_element.clear();
            }
        }

        // Update the registry entry
        if let Ok(mut registry) = ACTIVE_SESSION_CACHES.write() {
            if let Some(entry) = registry.get_mut(&self.connection_id) {
                entry.schema_name = schema_name;
                entry.tenant_id = tenant_id;
            }
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

        log::debug!(
            "IdMapper: {} -> {} (session {})",
            element_id,
            id,
            self.connection_id
        );
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
        // JavaScript's MAX_SAFE_INTEGER is 2^53 - 1 = 9007199254740991
        // We use 6 bits for label (64 labels) + 47 bits for id (140 trillion)
        // Total: 53 bits, safely within JavaScript's precision
        const LABEL_BITS: u32 = 6;
        const ID_BITS: u32 = 47;
        const ID_MASK: i64 = (1i64 << ID_BITS) - 1; // 0x7FFFFFFFFFFF (47 bits)
        const MAX_LABEL_CODE: u8 = (1 << LABEL_BITS) - 1; // 63

        // Parse "Label:id_value" format
        let (label, id_part) = if let Some(colon_pos) = element_id.find(':') {
            (&element_id[..colon_pos], &element_id[colon_pos + 1..])
        } else {
            // No colon, use empty label and full string as id
            ("", element_id)
        };

        // Get or assign a label code (0-63)
        let label_code = if let Ok(mut registry) = LABEL_CODE_REGISTRY.write() {
            registry.get_or_assign(label).min(MAX_LABEL_CODE)
        } else {
            0 // Fallback if lock fails
        };

        // Compute the id_value (47 bits)
        let id_value = Self::compute_id_value_hash(id_part, ID_MASK);

        // Combine: label_code in high 6 bits, id_value in low 47 bits
        let combined = ((label_code as i64) << ID_BITS) | (id_value & ID_MASK);
        combined.max(1)
    }

    /// Compute a 47-bit value for the id_value portion
    ///
    /// For simple numeric IDs, use the number directly (if it fits in 47 bits).
    /// For strings/composite keys, use a hash.
    fn compute_id_value_hash(id_part: &str, id_mask: i64) -> i64 {
        // Check for composite key format "part1|part2"
        if id_part.contains('|') {
            return Self::hash_string(id_part) & id_mask;
        }

        // Try to parse as integer
        if let Ok(numeric_id) = id_part.parse::<i64>() {
            // Use the numeric ID directly if it fits in 47 bits
            if numeric_id >= 0 && numeric_id <= id_mask {
                return numeric_id.max(1);
            }
            // Large number, use hash
            return Self::hash_string(id_part) & id_mask;
        }

        // For non-numeric IDs (like "LAX"), use a hash
        Self::hash_string(id_part) & id_mask
    }

    /// Hash a string to a positive 56-bit value
    fn hash_string(s: &str) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let hash = hasher.finish();
        // Mask to 47 bits for JS-safe encoding and ensure positive
        ((hash & 0x7FFFFFFFFFFF) as i64).abs().max(1)
    }

    /// Lookup element_id by integer ID (reverse lookup)
    ///
    /// Uses session cache chaining: first checks own cache, then searches
    /// other active sessions' caches with matching scope (schema + tenant).
    pub fn get_element_id(&self, id: i64) -> Option<String> {
        // Check own cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(element_id) = cache.int_to_element.get(&id) {
                return Some(element_id.clone());
            }
        }

        // Search other active sessions' caches with matching scope
        if let Ok(registry) = ACTIVE_SESSION_CACHES.read() {
            for (&conn_id, entry) in registry.iter() {
                if conn_id == self.connection_id {
                    continue; // Skip own cache (already checked)
                }
                // Only chain to sessions with matching scope
                if entry.schema_name != self.schema_name || entry.tenant_id != self.tenant_id {
                    continue;
                }
                if let Ok(cache) = entry.cache.read() {
                    if let Some(element_id) = cache.int_to_element.get(&id) {
                        log::debug!(
                            "IdMapper: Cross-session lookup found {} -> {} (from session {}, schema={:?}, tenant={:?})",
                            id,
                            element_id,
                            conn_id,
                            entry.schema_name,
                            entry.tenant_id,
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
    /// an IdMapper instance. Searches all active session caches regardless of scope.
    ///
    /// This is safe for decode_for_query because element_ids are deterministic —
    /// the same element_id string always maps to the same integer ID. The actual
    /// tenant/schema filtering happens at the SQL level via parameterized views.
    pub fn static_lookup_element_id(encoded_id: i64) -> Option<String> {
        if let Ok(registry) = ACTIVE_SESSION_CACHES.read() {
            for (_conn_id, entry) in registry.iter() {
                if let Ok(cache) = entry.cache.read() {
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
    /// it attempts to return the raw ID value that should be used in SQL comparisons.
    ///
    /// **IMPORTANT LIMITATIONS**:
    /// - Only works for **simple numeric primary keys** with values < 2^31 (2.1 billion)
    /// - Does NOT work for:
    ///   - String IDs (e.g., "LAX", "USER_123")  
    ///   - UUIDs
    ///   - Composite keys (e.g., "bank_id|account_num")
    ///   - Large numbers that get hashed (>= 2^31)
    ///
    /// **Callers should validate** that the decoded label's ID column is actually
    /// a numeric type before using the result, to avoid false positives.
    ///
    /// Priority:
    /// 1. Cache lookup - get exact element_id from session caches (always reliable)
    /// 2. Direct extraction - if id_hash looks like a simple numeric ID (heuristic)
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
        // We can't distinguish hashed vs raw values, but for small non-negative numbers
        // (0 to 2^31-1), it's almost certainly the raw value
        // NOTE: 0 is a valid ID value (e.g., user_id=0), so we accept the range [0, 2^31)
        if id_hash >= 0 && id_hash < (1i64 << 31) {
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

        // Extract the id_value from the lower 47 bits (JS-safe encoding)
        const ID_MASK: i64 = (1i64 << 47) - 1; // 0x7FFFFFFFFFFF
        let id_value = id & ID_MASK;

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
    use crate::utils::id_encoding::{ID_BITS, ID_MASK};

    /// Helper to extract label code from encoded ID (6 bits at position 47-52)
    fn get_label_code(id: i64) -> u8 {
        ((id >> ID_BITS) & 0x3F) as u8
    }

    /// Helper to extract id_value from encoded ID (47 low bits)
    fn get_id_value(id: i64) -> i64 {
        id & ID_MASK
    }

    #[test]
    fn test_get_or_assign_new_numeric() {
        // For numeric IDs like "User:1", the label code is in bits 47-52
        // and id_value (1) is in low 47 bits
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
        mapper1.set_scope(Some("test_schema".to_string()), None);
        let id1 = mapper1.get_or_assign("User:42");

        // id_value should be 42
        assert_eq!(get_id_value(id1), 42);

        // Create mapper 2 with SAME scope (simulating new connection to same schema)
        let mut mapper2 = IdMapper::new();
        mapper2.set_scope(Some("test_schema".to_string()), None);

        // Should be able to look up from mapper1's cache via chaining
        let element_id = mapper2.get_element_id(id1);
        assert_eq!(element_id, Some("User:42".to_string()));
    }

    #[test]
    fn test_session_scope_isolation_by_schema() {
        // Mapper 1 with schema "alpha"
        let mut mapper1 = IdMapper::new();
        mapper1.set_scope(Some("alpha".to_string()), None);
        let id1 = mapper1.get_or_assign("User:1");

        // Mapper 2 with DIFFERENT schema "beta"
        let mut mapper2 = IdMapper::new();
        mapper2.set_scope(Some("beta".to_string()), None);

        // Cross-session lookup should NOT find mapper1's data (different schema)
        assert_eq!(mapper2.get_element_id(id1), None);

        // But mapper with same schema should find it
        let mut mapper3 = IdMapper::new();
        mapper3.set_scope(Some("alpha".to_string()), None);
        assert_eq!(mapper3.get_element_id(id1), Some("User:1".to_string()));
    }

    #[test]
    fn test_session_scope_isolation_by_tenant() {
        // Mapper 1 with tenant "acme"
        let mut mapper1 = IdMapper::new();
        mapper1.set_scope(Some("shared_schema".to_string()), Some("acme".to_string()));
        let id1 = mapper1.get_or_assign("User:1");

        // Mapper 2 with DIFFERENT tenant "globex"
        let mut mapper2 = IdMapper::new();
        mapper2.set_scope(
            Some("shared_schema".to_string()),
            Some("globex".to_string()),
        );

        // Cross-session lookup should NOT find mapper1's data (different tenant)
        assert_eq!(mapper2.get_element_id(id1), None);

        // Mapper with same schema + same tenant should find it
        let mut mapper3 = IdMapper::new();
        mapper3.set_scope(Some("shared_schema".to_string()), Some("acme".to_string()));
        assert_eq!(mapper3.get_element_id(id1), Some("User:1".to_string()));
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
        assert!(
            registered,
            "IdMapper should be registered in ACTIVE_SESSION_CACHES"
        );
    }

    #[test]
    fn test_connection_id_unique() {
        let mapper1 = IdMapper::new();
        let mapper2 = IdMapper::new();

        // Each mapper should have a unique connection ID
        assert_ne!(mapper1.connection_id(), mapper2.connection_id());
    }
}
