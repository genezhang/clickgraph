//! Session-scoped ID Mapper for Neo4j Browser Compatibility
//!
//! Neo4j Browser uses `id(node)` function which expects integer IDs.
//! ClickGraph has string-based element_ids (e.g., "Airport:LAX").
//!
//! This module provides a session-scoped mapper that:
//! 1. Assigns monotonic integer IDs to each unique element_id
//! 2. Maintains bidirectional mapping for the session lifetime
//! 3. Ensures consistent IDs within a session for graph visualization

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

/// Session-scoped mapper between integer IDs and element_ids
///
/// # Thread Safety
/// The AtomicI64 counter is thread-safe. The HashMaps require external
/// synchronization (provided by Arc<Mutex<BoltContext>> in the connection).
#[derive(Debug, Default)]
pub struct IdMapper {
    /// Counter for generating unique integer IDs
    next_id: AtomicI64,

    /// Map from element_id to assigned integer ID
    element_to_int: HashMap<String, i64>,

    /// Map from integer ID to element_id (for reverse lookup)
    int_to_element: HashMap<i64, String>,
}

impl Clone for IdMapper {
    fn clone(&self) -> Self {
        IdMapper {
            next_id: AtomicI64::new(self.next_id.load(Ordering::SeqCst)),
            element_to_int: self.element_to_int.clone(),
            int_to_element: self.int_to_element.clone(),
        }
    }
}

impl IdMapper {
    /// Create a new IdMapper starting from ID 1
    pub fn new() -> Self {
        IdMapper {
            next_id: AtomicI64::new(1), // Start from 1, as 0 might be special
            element_to_int: HashMap::new(),
            int_to_element: HashMap::new(),
        }
    }

    /// Get or assign an integer ID for the given element_id
    ///
    /// If the element_id was seen before, returns the existing integer ID.
    /// Otherwise, assigns a new monotonic integer ID and stores the mapping.
    pub fn get_or_assign(&mut self, element_id: &str) -> i64 {
        if let Some(&id) = self.element_to_int.get(element_id) {
            return id;
        }

        // Assign new ID
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.element_to_int.insert(element_id.to_string(), id);
        self.int_to_element.insert(id, element_id.to_string());

        log::debug!("IdMapper: assigned {} -> {}", element_id, id);
        id
    }

    /// Lookup element_id by integer ID (reverse lookup)
    #[allow(dead_code)]
    pub fn get_element_id(&self, id: i64) -> Option<&str> {
        self.int_to_element.get(&id).map(|s| s.as_str())
    }

    /// Get the integer ID for an element_id without assigning if missing
    #[allow(dead_code)]
    pub fn get_int_id(&self, element_id: &str) -> Option<i64> {
        self.element_to_int.get(element_id).copied()
    }

    /// Get the number of mapped IDs
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.element_to_int.len()
    }

    /// Check if the mapper is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.element_to_int.is_empty()
    }

    /// Clear all mappings (useful for transaction boundaries)
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.element_to_int.clear();
        self.int_to_element.clear();
        // Don't reset next_id to maintain uniqueness across clears
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_assign_new() {
        let mut mapper = IdMapper::new();
        let id = mapper.get_or_assign("Airport:LAX");
        assert_eq!(id, 1);
    }

    #[test]
    fn test_get_or_assign_existing() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("Airport:LAX");
        let id2 = mapper.get_or_assign("Airport:LAX");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_multiple_elements() {
        let mut mapper = IdMapper::new();
        let id1 = mapper.get_or_assign("Airport:LAX");
        let id2 = mapper.get_or_assign("Airport:JFK");
        let id3 = mapper.get_or_assign("FLIGHT:LAX->JFK");

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);

        // Verify reverse lookup
        assert_eq!(mapper.get_element_id(1), Some("Airport:LAX"));
        assert_eq!(mapper.get_element_id(2), Some("Airport:JFK"));
    }

    #[test]
    fn test_consistency() {
        let mut mapper = IdMapper::new();

        // Interleave assignments
        let lax1 = mapper.get_or_assign("Airport:LAX");
        let jfk = mapper.get_or_assign("Airport:JFK");
        let lax2 = mapper.get_or_assign("Airport:LAX");
        let ord = mapper.get_or_assign("Airport:ORD");
        let lax3 = mapper.get_or_assign("Airport:LAX");

        // LAX should always get the same ID
        assert_eq!(lax1, lax2);
        assert_eq!(lax2, lax3);

        // All different elements get different IDs
        assert_ne!(lax1, jfk);
        assert_ne!(jfk, ord);
    }
}
