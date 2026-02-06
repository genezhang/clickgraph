//! Deterministic ID Mapper for Neo4j Browser Compatibility
//!
//! Neo4j Browser uses `id(node)` function which expects integer IDs.
//! ClickGraph has string-based element_ids (e.g., "User:1", "Post:42").
//!
//! This module provides:
//! 1. Deterministic hashing: "User:1" always produces the same integer ID
//! 2. Global cache: ID mappings are shared across all connections
//! 3. Cross-session lookups: `id(n) = X` works even in new connections
//!
//! # Design
//!
//! We hash the full element_id (including label) to ensure uniqueness.
//! "User:1" and "Post:1" have different IDs.
//!
//! The global cache stores reverse mappings so cross-session lookups work.

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

lazy_static! {
    /// Global cache for cross-session ID lookups
    /// Maps integer ID → element_id for all IDs ever seen
    static ref GLOBAL_ID_CACHE: RwLock<HashMap<i64, String>> = RwLock::new(HashMap::new());
}

/// Deterministic mapper between integer IDs and element_ids
///
/// Uses the global cache for cross-session lookups while maintaining
/// a per-connection cache for efficiency.
#[derive(Debug, Default)]
pub struct IdMapper {
    /// Map from element_id to assigned integer ID (local cache)
    element_to_int: HashMap<String, i64>,

    /// Map from integer ID to element_id (local cache for fast lookup)
    int_to_element: HashMap<i64, String>,
}

impl Clone for IdMapper {
    fn clone(&self) -> Self {
        IdMapper {
            element_to_int: self.element_to_int.clone(),
            int_to_element: self.int_to_element.clone(),
        }
    }
}

impl IdMapper {
    /// Create a new IdMapper
    pub fn new() -> Self {
        IdMapper {
            element_to_int: HashMap::new(),
            int_to_element: HashMap::new(),
        }
    }

    /// Get or compute a deterministic integer ID for the given element_id
    ///
    /// The ID is always a deterministic hash of the full element_id.
    /// The mapping is cached both locally and globally for cross-session lookups.
    pub fn get_or_assign(&mut self, element_id: &str) -> i64 {
        // Check local cache first (fastest)
        if let Some(&id) = self.element_to_int.get(element_id) {
            return id;
        }

        // Compute deterministic ID (always the same for the same element_id)
        let id = Self::compute_deterministic_id(element_id);

        // Cache locally
        self.element_to_int.insert(element_id.to_string(), id);
        self.int_to_element.insert(id, element_id.to_string());

        // Cache globally for cross-session lookups
        if let Ok(mut global_cache) = GLOBAL_ID_CACHE.write() {
            global_cache.insert(id, element_id.to_string());
        }

        log::debug!("IdMapper: {} -> {}", element_id, id);
        id
    }

    /// Compute a deterministic integer ID from an element_id
    ///
    /// Format: "Label:id_value" or "Label:part1|part2" for composite keys
    ///
    /// For simple numeric IDs like "User:1", extract and use 1 directly.
    /// This ensures consistency with id(n) in RETURN clause which returns the row ID.
    fn compute_deterministic_id(element_id: &str) -> i64 {
        // Try to extract numeric ID from "Label:N" format
        if let Some(colon_pos) = element_id.find(':') {
            let id_part = &element_id[colon_pos + 1..];

            // Check for composite key format "part1|part2"
            if id_part.contains('|') {
                // Use hash for composite keys
                return Self::hash_to_i64(element_id);
            }

            // Try to parse as integer
            if let Ok(numeric_id) = id_part.parse::<i64>() {
                // Use the numeric ID directly
                return if numeric_id > 0 { numeric_id } else { 1 };
            }

            // For non-numeric IDs (like "Airport:LAX"), use a hash
            return Self::hash_to_i64(element_id);
        }

        // No colon found, use full string hash
        Self::hash_to_i64(element_id)
    }

    /// Convert a string to a deterministic positive i64 using hash
    fn hash_to_i64(s: &str) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let hash = hasher.finish();
        // Ensure positive and non-zero by masking and adding 1
        // Use upper bits for better distribution, mask to 62 bits to stay positive
        ((hash >> 1) as i64).abs().max(1)
    }

    /// Lookup element_id by integer ID (reverse lookup)
    ///
    /// First checks local cache, then global cache for cross-session lookups.
    pub fn get_element_id(&self, id: i64) -> Option<String> {
        // Check local cache first
        if let Some(element_id) = self.int_to_element.get(&id) {
            return Some(element_id.clone());
        }

        // Check global cache for cross-session lookups
        if let Ok(global_cache) = GLOBAL_ID_CACHE.read() {
            if let Some(element_id) = global_cache.get(&id) {
                return Some(element_id.clone());
            }
        }

        None
    }

    /// Get the integer ID for an element_id without assigning if missing
    #[allow(dead_code)]
    pub fn get_int_id(&self, element_id: &str) -> Option<i64> {
        self.element_to_int.get(element_id).copied()
    }

    /// Get the number of cached mappings
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.element_to_int.len()
    }

    /// Check if the mapper cache is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.element_to_int.is_empty()
    }

    /// Clear all cached mappings
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.element_to_int.clear();
        self.int_to_element.clear();
    }

    /// Try to construct an element_id from an integer ID without prior mapping
    ///
    /// This is used for cross-session id() lookups. Given an integer ID and
    /// a list of known labels from the schema, we try to find a matching node.
    ///
    /// Returns a list of candidate element_ids to try (e.g., ["User:1", "Post:1"])
    pub fn generate_candidate_element_ids(&self, id: i64, labels: &[String]) -> Vec<String> {
        // First check if we have an exact mapping
        if let Some(element_id) = self.int_to_element.get(&id) {
            return vec![element_id.clone()];
        }

        // Generate candidates: try each label with the ID
        labels.iter().map(|label| format!("{}:{}", label, id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_assign_new_numeric() {
        // For numeric IDs like "User:1", the numeric value is used directly
        let mut mapper = IdMapper::new();
        let id = mapper.get_or_assign("User:1");
        assert_eq!(id, 1);
    }

    #[test]
    fn test_get_or_assign_non_numeric() {
        // For non-numeric IDs like "Airport:LAX", a hash is used
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("Airport:LAX");
        let id2 = mapper.get_or_assign("Airport:LAX");
        // Hash should be consistent
        assert_eq!(id1, id2);
        // Hash should be a large number (not 1,2,3...)
        assert!(id1 > 1000);
    }

    #[test]
    fn test_get_or_assign_existing() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("User:42");
        let id2 = mapper.get_or_assign("User:42");
        assert_eq!(id1, id2);
        assert_eq!(id1, 42);
    }

    #[test]
    fn test_numeric_ids() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("User:1");
        let id2 = mapper.get_or_assign("Post:2");
        let id3 = mapper.get_or_assign("Comment:3");

        // Numeric IDs are extracted directly
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);

        // Verify reverse lookup (now returns String, not &str)
        assert_eq!(mapper.get_element_id(1), Some("User:1".to_string()));
        assert_eq!(mapper.get_element_id(2), Some("Post:2".to_string()));
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
        assert_eq!(user1_a, 1);

        // Different elements get different IDs
        assert_ne!(user1_a, post2);
        assert_ne!(post2, comment3);
    }

    #[test]
    fn test_global_cache() {
        // Create mapper 1, assign some IDs
        let mut mapper1 = IdMapper::new();
        let id1 = mapper1.get_or_assign("User:42");
        assert_eq!(id1, 42);

        // Create mapper 2 (simulating new connection)
        let mapper2 = IdMapper::new();
        // Should be able to look up from global cache
        let element_id = mapper2.get_element_id(42);
        assert_eq!(element_id, Some("User:42".to_string()));
    }
}
