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
    /// Maps connection_id → SessionEntry (its cache). A cached id is read
    /// back only in the scope it was given in (`Scope`): its graph and
    /// tenant, so tenants and graphs stay apart (#1388).
    static ref ACTIVE_SESSION_CACHES: RwLock<HashMap<u64, SessionEntry>> =
        RwLock::new(HashMap::new());
}

/// The scope an id was given in: the graph, and the tenant (its view
/// parameter values, `IdMapper::set_scope_with_views`).
type Scope = (Option<String>, Option<String>);

/// Counter for generating unique connection IDs
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Session-local cache storing ID mappings for one connection
#[derive(Debug, Default)]
struct SessionCache {
    /// Map from element_id to assigned integer ID
    element_to_int: HashMap<String, i64>,
    /// Map from the scope it was given in and integer ID to element_id (for
    /// reverse lookup): a session that changes scope keeps the ids of each.
    int_to_element: HashMap<(Scope, i64), String>,
}

/// A registered session entry: its cache.
#[derive(Debug)]
struct SessionEntry {
    cache: Arc<RwLock<SessionCache>>,
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
            schema_name: graph_scope(None),
            tenant_id: None,
        }
    }

    /// Update the scope (graph + tenant) for this session, as known at
    /// HELLO, LOGON or BEGIN: [`Self::set_scope_with_views`] without view
    /// parameters.
    pub fn set_scope(&mut self, schema_name: Option<String>, tenant_id: Option<String>) {
        self.set_scope_with_views(schema_name, tenant_id.as_deref(), None);
    }

    /// Update the scope for this session's next ids (a RUN's, a request's):
    /// its graph (the default one where it names none), and its tenant and
    /// view parameter values, which decide the rows it reads. Ids are given
    /// and read back in their scope; the ids of a scope this session left
    /// are kept for when it returns (#1388).
    pub fn set_scope_with_views(
        &mut self,
        schema_name: Option<String>,
        tenant_id: Option<&str>,
        view_parameters: Option<&HashMap<String, String>>,
    ) {
        self.schema_name = graph_scope(schema_name);
        self.tenant_id = tenant_scope(tenant_id, view_parameters);
    }

    /// This session's current scope.
    fn scope(&self) -> Scope {
        (self.schema_name.clone(), self.tenant_id.clone())
    }

    /// Get or compute a deterministic integer ID for the given element_id
    ///
    /// The ID encodes the label in the high byte and the hash of the id_value
    /// in the lower 56 bits, ensuring globally unique IDs across labels.
    pub fn get_or_assign(&mut self, element_id: &str) -> i64 {
        let known = self
            .cache
            .read()
            .ok()
            .and_then(|cache| cache.element_to_int.get(element_id).copied());
        // Compute deterministic ID with label encoding
        let id = known.unwrap_or_else(|| Self::compute_deterministic_id(element_id));

        // Cache locally, read back in this scope (given in another, too).
        if let Ok(mut cache) = self.cache.write() {
            cache.element_to_int.insert(element_id.to_string(), id);
            cache
                .int_to_element
                .insert((self.scope(), id), element_id.to_string());
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

        // Strip the trailing `-` Neo4j-Browser-compat sentinel so the id_value
        // portion stays the unadorned id (e.g., `"User:1-"` → label `User`,
        // id_value `1`, not `"1-"`). Without this strip, a numeric id like `1`
        // would fail integer parse and fall back to a hash, breaking the
        // round-trip with id-rewriter callers. One: an id's text may end
        // with another (`a-` and `a` are two ids).
        let element_id = element_id.strip_suffix('-').unwrap_or(element_id);

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
        match Self::numeric_key(id_part, id_mask) {
            Some(numeric_id) => numeric_id.max(1),
            // A composite key, a string (like "LAX") or a large number.
            None => Self::hash_string(id_part) & id_mask,
        }
    }

    /// The key of an id's text where it is a plain non-negative integer that
    /// fits `id_mask`: its number holds the key's bits.
    fn numeric_key(id_part: &str, id_mask: i64) -> Option<i64> {
        if id_part.contains('|') {
            return None;
        }
        id_part
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0 && *n <= id_mask)
    }

    /// Whether the number of `element_id` is a hash of its key (a string,
    /// composite or large one) rather than the key's bits: a hash is read
    /// back only from the session that gave it (`get_or_assign`,
    /// [`resolve_node_id`]), the bits from the number alone (of a label
    /// whose key is an integer: [`IdMapper::holds_key_bits`]).
    pub fn is_hashed(element_id: &str) -> bool {
        let element_id = element_id.strip_suffix('-').unwrap_or(element_id);
        let id_part = element_id
            .find(':')
            .map_or(element_id, |at| &element_id[at + 1..]);
        Self::numeric_key(id_part, crate::utils::id_encoding::ID_MASK).is_none()
    }

    /// Whether the number of `element_id` can be read back from its bits
    /// alone ([`resolve_node_id`]): its key is an integer that fits them
    /// ([`IdMapper::is_hashed`]) and its label's key is an integer, as a
    /// string key of digits (`'12345678901'`) is not read from the bits. A
    /// number that cannot is kept in the session when it is given.
    pub fn holds_key_bits(schema: &crate::graph_catalog::GraphSchema, element_id: &str) -> bool {
        if Self::is_hashed(element_id) {
            return false;
        }
        let element_id = element_id.strip_suffix('-').unwrap_or(element_id);
        let Some((label, _)) = element_id.split_once(':') else {
            return false;
        };
        schema.node_schema(label).is_ok_and(|node| {
            matches!(
                node.node_id.dtype,
                crate::graph_catalog::schema_types::SchemaType::Integer
            )
        })
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
        let key = (self.scope(), id);
        // Check own cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(element_id) = cache.int_to_element.get(&key) {
                return Some(element_id.clone());
            }
        }

        // Search other active sessions' caches, for an id given in this scope
        if let Ok(registry) = ACTIVE_SESSION_CACHES.read() {
            for (&conn_id, entry) in registry.iter() {
                if conn_id == self.connection_id {
                    continue; // Skip own cache (already checked)
                }
                if let Ok(cache) = entry.cache.read() {
                    if let Some(element_id) = cache.int_to_element.get(&key) {
                        log::debug!(
                            "IdMapper: Cross-session lookup found {} -> {} (from session {}, scope {:?})",
                            id,
                            element_id,
                            conn_id,
                            key.0,
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
    /// - Only works for **simple numeric primary keys** with values < 2^47
    /// - Does NOT work for:
    ///   - String IDs (e.g., "LAX", "USER_123")  
    ///   - UUIDs
    ///   - Composite keys (e.g., "bank_id|account_num")
    ///   - Integer keys that get hashed (negative, >= 2^47)
    ///
    /// **Callers should validate** that the decoded label's ID column is actually
    /// a numeric type before using the result, to avoid false positives.
    ///
    /// By the bit pattern alone: an id a session gave is read through that
    /// session's mapper, scoped to its graph and tenant
    /// ([`IdMapper::get_element_id`], [`resolve_node_id`]), never from
    /// another scope's cache (#1388).
    ///
    /// Returns (label, raw_id_value) if decodable, None otherwise.
    pub fn decode_for_query(encoded_id: i64) -> Option<(String, String)> {
        let (label_code, id_hash) = Self::decode_id(encoded_id);

        // If label_code is 0, this isn't an encoded ID
        if label_code == 0 {
            return None;
        }

        // Get the label name from the code
        let label = Self::get_label_from_code(label_code)?;

        // The key's bits (47 of them), for a label whose key is an integer
        // (the caller checks that: `resolve_node_id`). A number that is the
        // hash of another key (a negative or huge integer, a string) is read
        // back from the session that gave it, before this is reached; one no
        // session gave may read as a key that exists, which a 47-bit hash
        // rarely is.
        Some((label, id_hash.to_string()))
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

/// The tenant part of a session's scope: its view parameter values, the
/// tenant among them (as the read merges it, `tenant_id`), in one text;
/// `None` without any.
fn tenant_scope(
    tenant_id: Option<&str>,
    view_parameters: Option<&HashMap<String, String>>,
) -> Option<String> {
    let mut values: Vec<(String, String)> = view_parameters
        .map(|v| v.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    if let Some(t) = tenant_id {
        if !values.iter().any(|(k, _)| k == "tenant_id") {
            values.push(("tenant_id".to_string(), t.to_string()));
        }
    }
    if values.is_empty() {
        return None;
    }
    values.sort();
    // Unambiguous: each key and value as a JSON string.
    Some(serde_json::to_string(&values).unwrap_or_default())
}

/// The graph a session's ids are scoped to: the one it names, else the
/// default graph, which a session naming none reads (a Bolt session without
/// a database, an HTTP request without a schema: one scope). Two names of
/// one graph (`default` and its schema's own) are two scopes.
fn graph_scope(schema_name: Option<String>) -> Option<String> {
    Some(schema_name.unwrap_or_else(|| "default".to_string()))
}

impl Default for IdMapper {
    fn default() -> Self {
        Self::new()
    }
}

/// The node an integer `id()` value names, as the server reads it: its
/// label and key's text (`element_id::node_id_text`: a composite key's
/// parts escaped and joined by `|`). The id `id_mapper`
/// gave it (this session's, or another of the same scope), else its bit
/// pattern decoded (`IdMapper::decode_for_query`: a label code and an
/// integer key of up to 47 bits), for a label whose key is an integer. `None`: it names no
/// node. The bound plan's `id()` lookup (P-4c S11e).
pub fn resolve_node_id(
    id_mapper: &IdMapper,
    schema: Option<&crate::graph_catalog::GraphSchema>,
    id_value: i64,
) -> Option<(String, String)> {
    if let Some(element_id) = id_mapper.get_element_id(id_value) {
        if let Ok((label, id_text)) =
            crate::graph_catalog::element_id::parse_node_element_id(&element_id)
        {
            return Some((label, id_text));
        }
    }
    // LIMITATION: only simple numeric keys (< 2^47) decode from the bit
    // pattern; not string ids, UUIDs, composite keys or hashed values.
    let (label, id_str) = IdMapper::decode_for_query(id_value)?;
    if let Some(schema) = schema {
        use crate::graph_catalog::schema_types::SchemaType;
        let Ok(node_schema) = schema.node_schema(&label) else {
            log::warn!("id() decode: label '{}' not found in schema", label);
            return None;
        };
        if !matches!(node_schema.node_id.dtype, SchemaType::Integer) {
            log::warn!(
                "id() decode: skipping {}:{} - ID type '{:?}' is not Integer",
                label,
                id_str,
                node_schema.node_id.dtype
            );
            return None;
        }
    }
    Some((label, id_str))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// I5b review: an integer key of up to 47 bits is read from its number
    /// (most LDBC ids are above 2^31).
    #[test]
    fn an_integer_key_of_47_bits_decodes_from_its_number() {
        let label = "I5bDecode";
        let key = 2_336_468_743_926_i64;
        let n = IdMapper::compute_deterministic_id(&format!("{label}:{key}-"));
        assert_eq!(
            IdMapper::decode_for_query(n),
            Some((label.to_string(), key.to_string()))
        );
        let top = (1_i64 << 47) - 1;
        let n = IdMapper::compute_deterministic_id(&format!("{label}:{top}-"));
        assert_eq!(
            IdMapper::decode_for_query(n),
            Some((label.to_string(), top.to_string()))
        );
    }

    #[test]
    fn a_number_is_a_hash_unless_it_holds_its_keys_bits() {
        assert!(!IdMapper::is_hashed("User:7-"));
        assert!(!IdMapper::is_hashed("User:0"));
        // Strings, composite keys and keys too large for 47 bits.
        assert!(IdMapper::is_hashed("Airport:LAX-"));
        assert!(IdMapper::is_hashed("Acct:b|7-"));
        assert!(IdMapper::is_hashed("User:-3-"));
        assert!(IdMapper::is_hashed("User:140737488355328-"));
        assert!(!IdMapper::is_hashed("User:140737488355327-"));
        assert!(!IdMapper::is_hashed("Message:2147483648-"));
    }

    #[test]
    fn ids_that_differ_in_a_trailing_dash_are_two() {
        // One trailing `-` is the sentinel: `a` and `a-` are two ids.
        assert_ne!(
            IdMapper::compute_deterministic_id("Item:a-"),
            IdMapper::compute_deterministic_id("Item:a--")
        );
        assert_eq!(
            IdMapper::compute_deterministic_id("User:7-"),
            IdMapper::compute_deterministic_id("User:7")
        );
    }
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

    /// An id another session cached (as Neo4j Browser's `User:2-`) decodes
    /// to its key without the sentinel, through a mapper of that session's
    /// scope: `'2-'` against an integer key column was a ClickHouse error
    /// (Code 53) on HTTP after a Bolt session. Of another scope (graph or
    /// tenant) it does not decode, but by its bit pattern (#1388).
    #[test]
    fn a_cached_id_decodes_in_its_own_scope_only() {
        let mut bolt = IdMapper::new();
        bolt.set_scope(Some("s1388".to_string()), Some("t1".to_string()));
        let id = bolt.get_or_assign("S1388Sentinel:x-");
        let composite = bolt.get_or_assign("S1388Sentinel:a|b-");
        let mut same = IdMapper::new();
        same.set_scope(Some("s1388".to_string()), Some("t1".to_string()));
        assert_eq!(
            resolve_node_id(&same, None, id),
            Some(("S1388Sentinel".to_string(), "x".to_string()))
        );
        assert_eq!(
            resolve_node_id(&same, None, composite),
            Some(("S1388Sentinel".to_string(), "a|b".to_string()))
        );
        for (schema, tenant) in [
            ("s1388", Some("t2")),
            ("other", Some("t1")),
            ("s1388", None),
        ] {
            let mut other = IdMapper::new();
            other.set_scope(Some(schema.to_string()), tenant.map(str::to_string));
            // Not the cached node: its number is a hash, which the other
            // scope never gave (it may read as some other integer key).
            assert_eq!(other.get_element_id(id), None, "{schema} {tenant:?}");
            assert_ne!(
                resolve_node_id(&other, None, id),
                Some(("S1388Sentinel".to_string(), "x".to_string())),
                "{schema} {tenant:?}"
            );
            assert_eq!(other.get_element_id(composite), None, "{schema} {tenant:?}");
        }
        // A session naming no graph is of the default graph's scope.
        let mut unnamed = IdMapper::new();
        unnamed.set_scope(None, None);
        let unnamed_id = unnamed.get_or_assign("S1388Sentinel:y-");
        let mut default = IdMapper::new();
        default.set_scope(Some("default".to_string()), None);
        assert_eq!(
            resolve_node_id(&default, None, unnamed_id),
            Some(("S1388Sentinel".to_string(), "y".to_string()))
        );
        // A session that changes scope keeps the ids of the one it left, for
        // when it returns; they are not read in the other.
        bolt.set_scope(Some("elsewhere".to_string()), None);
        assert_eq!(bolt.get_element_id(id), None);
        bolt.set_scope(Some("s1388".to_string()), Some("t1".to_string()));
        assert_eq!(
            bolt.get_element_id(id),
            Some("S1388Sentinel:x-".to_string())
        );
        // View parameter values are part of the tenant's scope; a tenant
        // named or given as its view parameter is one scope.
        let views = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let mut a = IdMapper::new();
        a.set_scope_with_views(
            Some("s1388".to_string()),
            None,
            Some(&views(&[("region", "eu")])),
        );
        let a_id = a.get_or_assign("S1388Sentinel:eu-");
        let mut b = IdMapper::new();
        b.set_scope_with_views(
            Some("s1388".to_string()),
            None,
            Some(&views(&[("region", "us")])),
        );
        assert_eq!(b.get_element_id(a_id), None);
        b.set_scope_with_views(
            Some("s1388".to_string()),
            None,
            Some(&views(&[("region", "eu")])),
        );
        assert_eq!(
            b.get_element_id(a_id),
            Some("S1388Sentinel:eu-".to_string())
        );
        let mut named = IdMapper::new();
        named.set_scope_with_views(
            Some("s1388".to_string()),
            None,
            Some(&views(&[("tenant_id", "t1")])),
        );
        assert_eq!(
            named.get_element_id(id),
            Some("S1388Sentinel:x-".to_string())
        );
    }
}
