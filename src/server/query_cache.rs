/// Query cache module for caching SQL templates
///
/// This module implements an LRU cache for SQL query templates to avoid
/// re-parsing, planning, and rendering the same Cypher queries repeatedly.
///
/// # Architecture
///
/// Cache Key: (normalized_query, schema_name)
/// Cache Value: SQL template with $paramName placeholders
///
/// # Neo4j Compatibility
///
/// Supports Neo4j's CYPHER query options for cache control:
/// - `CYPHER replan=default` - Normal cache behavior (LRU)
/// - `CYPHER replan=force` - Bypass cache, regenerate SQL, update cache
/// - `CYPHER replan=skip` - Always use cache (error if not cached)
///
/// # Configuration
///
/// Environment variables:
/// - `CLICKGRAPH_QUERY_CACHE_ENABLED` (default: true)
/// - `CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES` (default: 1000)
/// - `CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB` (default: 100)
///
/// # Error Handling
///
/// Mutex poisoning is handled gracefully - the cache will be disabled if the
/// mutex becomes poisoned, allowing queries to continue without caching.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Helper macro for safe mutex locking with graceful degradation
/// If the mutex is poisoned, log the error and return None/early to skip caching
macro_rules! lock_cache {
    ($mutex:expr, $operation:expr) => {
        match $mutex.lock() {
            Ok(guard) => guard,
            Err(e) => {
                log::error!(
                    "Query cache mutex poisoned during {}: {}. Cache disabled.",
                    $operation,
                    e
                );
                return None; // Gracefully skip caching
            }
        }
    };
    // For void operations (insert, clear, etc.) - return unit type
    ($mutex:expr, $operation:expr, void) => {
        match $mutex.lock() {
            Ok(guard) => guard,
            Err(e) => {
                log::error!(
                    "Query cache mutex poisoned during {}: {}. Cache disabled.",
                    $operation,
                    e
                );
                return; // Gracefully skip operation
            }
        }
    };
}

/// Cache control strategy from the `CYPHER replan=..` option (read by the
/// statement front end, [`crate::translate::prepared`]).
pub use crate::translate::prepared::ReplanOption;

/// Key for cache lookup
///
/// Uses normalized query, schema name, and view scope to uniquely identify a query template.
/// View scope includes tenant_id and view_parameters that affect parameterized view resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryCacheKey {
    /// Normalized Cypher query (with CYPHER prefix stripped)
    pub normalized_query: String,
    /// Schema name for multi-schema support
    pub schema_name: String,
    /// Tenant ID and view parameters affect SQL generation for parameterized views
    pub view_scope: String,
    /// The endpoint whose SQL this entry holds. Endpoints that translate the
    /// same text differently (or return it instead of executing it) must not
    /// share entries: `/query/sql` once cached analyzer-less SQL under the key
    /// `/query` then executed.
    pub route: &'static str,
}

/// `query` with each run of whitespace outside quotes one space, trimmed. A
/// string literal or an escaped name is kept as written: `'Alice  Johnson'`
/// and `'Alice Johnson'`, `` `x  y` `` and `` `x y` `` are different queries
/// (#1489 review). Cypher's quoting: `'…'` and `"…"` with `\` escapes,
/// `` `…` `` with a doubled backtick and no escapes.
fn normalize_whitespace(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    let mut pending_space = false;
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            pending_space = !out.is_empty();
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.push(c);
        if matches!(c, '\'' | '"' | '`') {
            while let Some(d) = chars.next() {
                out.push(d);
                if d == '\\' && c != '`' {
                    if let Some(e) = chars.next() {
                        out.push(e);
                    }
                } else if d == c {
                    if c == '`' && chars.peek() == Some(&'`') {
                        out.push('`');
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        }
    }
    out
}

impl QueryCacheKey {
    pub fn new(query: &str, schema_name: &str) -> Self {
        Self::with_view_scope(query, schema_name, None, None)
    }

    pub fn with_view_scope(
        query: &str,
        schema_name: &str,
        tenant_id: Option<&str>,
        view_parameters: Option<&std::collections::HashMap<String, String>>,
    ) -> Self {
        // Without its prelude (`CYPHER replan=..`): the same statement.
        let stripped = crate::translate::prepared::without_prelude(query);

        // Normalize whitespace: collapse multiple spaces/tabs/newlines into single space
        // (of the query without its comments: a quote inside one opens no
        // string, `// it's`)
        let normalized = normalize_whitespace(&crate::open_cypher_parser::strip_comments(stripped));

        // Build a scope string from tenant_id and view_parameters
        let view_scope = match (tenant_id, view_parameters) {
            (None, None) => String::new(),
            (Some(tid), None) => format!("tid={}", tid),
            (None, Some(vp)) => {
                let mut parts: Vec<String> =
                    vp.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                parts.sort();
                parts.join(",")
            }
            (Some(tid), Some(vp)) => {
                let mut parts: Vec<String> =
                    vp.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                parts.sort();
                // tenant_id may already be in view_parameters; add if not
                if !vp.contains_key("tenant_id") {
                    parts.push(format!("tid={}", tid));
                }
                parts.join(",")
            }
        };

        QueryCacheKey {
            normalized_query: normalized,
            schema_name: schema_name.to_string(),
            view_scope,
            route: "query",
        }
    }

    /// The same key, scoped to another endpoint's entries.
    pub fn for_route(mut self, route: &'static str) -> Self {
        self.route = route;
        self
    }
}

/// Cached entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    /// SQL template with $paramName placeholders
    sql_template: String,
    /// The statement classification kept with the template (`None` when the
    /// store path had no classification, e.g. the ad-hoc insert helper). A
    /// cache hit runs no parser of its own, so the hit's metrics, counter and
    /// span are labeled from here instead of collapsing into `other`.
    query_type: Option<String>,
    /// Approximate size in bytes for memory tracking
    size_bytes: usize,
    /// Last access timestamp (for LRU)
    last_accessed: u64,
    /// Number of times this entry was accessed
    access_count: u64,
}

impl CacheEntry {
    fn new(sql_template: String, query_type: Option<String>) -> Self {
        let size_bytes = sql_template.len() + std::mem::size_of::<Self>();
        CacheEntry {
            sql_template,
            query_type,
            size_bytes,
            last_accessed: current_timestamp(),
            access_count: 0,
        }
    }

    fn touch(&mut self) {
        self.last_accessed = current_timestamp();
        self.access_count += 1;
    }
}

/// Configuration for query cache
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// Enable or disable caching
    pub enabled: bool,
    /// Maximum number of entries (LRU eviction)
    pub max_entries: usize,
    /// Maximum memory size in bytes
    pub max_size_bytes: usize,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        QueryCacheConfig {
            enabled: true,
            max_entries: 1000,
            max_size_bytes: 100 * 1024 * 1024, // 100 MB
        }
    }
}

impl QueryCacheConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("CLICKGRAPH_QUERY_CACHE_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);

        let max_entries = std::env::var("CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let max_size_mb = std::env::var("CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100);

        QueryCacheConfig {
            enabled,
            max_entries,
            max_size_bytes: max_size_mb * 1024 * 1024,
        }
    }
}

/// Query cache with LRU eviction
pub struct QueryCache {
    /// Cache storage
    cache: Arc<Mutex<HashMap<QueryCacheKey, CacheEntry>>>,
    /// Configuration
    config: QueryCacheConfig,
    /// Metrics
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
}

impl QueryCache {
    /// Create a new query cache with configuration
    pub fn new(config: QueryCacheConfig) -> Self {
        QueryCache {
            cache: Arc::new(Mutex::new(HashMap::new())),
            config,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a new query cache with default configuration
    pub fn with_defaults() -> Self {
        Self::new(QueryCacheConfig::default())
    }

    /// Create a new query cache from environment variables
    pub fn from_env() -> Self {
        Self::new(QueryCacheConfig::from_env())
    }

    /// Get SQL template from cache
    ///
    /// Returns Some(sql) if found, None if not cached
    pub fn get(&self, key: &QueryCacheKey) -> Option<String> {
        self.get_with_type(key).map(|(sql, _)| sql)
    }

    /// Get the SQL template together with the statement classification kept
    /// with it (`get_with_type`). The type rides in the entry so a cache hit
    /// is recorded — counted and spanned — under the type it was classified
    /// as when the template was stored, not as `other`.
    pub fn get_with_type(&self, key: &QueryCacheKey) -> Option<(String, Option<String>)> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = lock_cache!(self.cache, "get");
        if let Some(entry) = cache.get_mut(key) {
            entry.touch();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some((entry.sql_template.clone(), entry.query_type.clone()))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert SQL template into cache
    ///
    /// May trigger LRU eviction if cache is full
    pub fn insert(&self, key: QueryCacheKey, sql_template: String) {
        self.insert_entry(key, sql_template, None);
    }

    /// Insert an SQL template together with the statement classification the
    /// caller just computed, so a later hit is recorded under that type
    /// ([`QueryCache::get_with_type`]).
    pub fn insert_typed(&self, key: QueryCacheKey, sql_template: String, query_type: String) {
        self.insert_entry(key, sql_template, Some(query_type));
    }

    fn insert_entry(&self, key: QueryCacheKey, sql_template: String, query_type: Option<String>) {
        if !self.config.enabled {
            return;
        }

        let entry = CacheEntry::new(sql_template, query_type);

        let mut cache = lock_cache!(self.cache, "insert", void);

        // Check if we need to evict entries
        if cache.len() >= self.config.max_entries {
            self.evict_lru(&mut cache);
        }

        // Check memory limit
        let current_size: usize = cache.values().map(|e| e.size_bytes).sum();
        if current_size + entry.size_bytes > self.config.max_size_bytes {
            self.evict_by_size(&mut cache, entry.size_bytes);
        }

        cache.insert(key, entry);
    }

    /// Evict least recently used entry
    fn evict_lru(&self, cache: &mut HashMap<QueryCacheKey, CacheEntry>) {
        if let Some((key, _)) = cache.iter().min_by_key(|(_, entry)| entry.last_accessed) {
            let key = key.clone();
            cache.remove(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Evict entries to make room for new_entry_size bytes
    fn evict_by_size(&self, cache: &mut HashMap<QueryCacheKey, CacheEntry>, needed_size: usize) {
        let current_size: usize = cache.values().map(|e| e.size_bytes).sum();
        let mut freed = 0;

        while current_size + needed_size - freed > self.config.max_size_bytes && !cache.is_empty() {
            if let Some((key, entry)) = cache.iter().min_by_key(|(_, e)| e.last_accessed) {
                let key = key.clone();
                let size = entry.size_bytes;
                cache.remove(&key);
                freed += size;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    /// Invalidate cache entries for a specific schema
    ///
    /// Called when a schema is reloaded to ensure cache consistency
    pub fn invalidate_schema(&self, schema_name: &str) {
        let mut cache = lock_cache!(self.cache, "invalidate_schema", void);
        cache.retain(|key, _| key.schema_name != schema_name);
    }

    /// Clear entire cache
    pub fn clear(&self) {
        let mut cache = lock_cache!(self.cache, "clear", void);
        cache.clear();
    }

    /// Get cache metrics
    pub fn metrics(&self) -> CacheMetrics {
        let (size, size_bytes) = match self.cache.lock() {
            Ok(cache) => {
                let s = cache.len();
                let sb = cache.values().map(|e| e.size_bytes).sum();
                (s, sb)
            }
            Err(e) => {
                log::error!(
                    "Query cache mutex poisoned during metrics: {}. Returning partial metrics.",
                    e
                );
                (0, 0)
            }
        };

        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size,
            size_bytes,
            max_entries: self.config.max_entries,
            max_size_bytes: self.config.max_size_bytes,
        }
    }
}

/// Cache metrics for monitoring
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub size_bytes: usize,
    pub max_entries: usize,
    pub max_size_bytes: usize,
}

impl CacheMetrics {
    /// Calculate cache hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Calculate memory utilization (0.0 to 1.0)
    pub fn memory_utilization(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            self.size_bytes as f64 / self.max_size_bytes as f64
        }
    }

    /// Calculate entry utilization (0.0 to 1.0)
    pub fn entry_utilization(&self) -> f64 {
        if self.max_entries == 0 {
            0.0
        } else {
            self.size as f64 / self.max_entries as f64
        }
    }
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time should not go backwards")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_route_separates_endpoints() {
        // `/query/sql` returns SQL instead of executing it and must never share
        // entries with `/query`, even for the same text and scope.
        let query = QueryCacheKey::new("MATCH (n) RETURN n", "s");
        let sql_endpoint = QueryCacheKey::new("MATCH (n) RETURN n", "s").for_route("query_sql");
        assert_eq!(query.route, "query");
        assert_ne!(query, sql_endpoint);
    }

    #[test]
    fn test_cache_key_creation() {
        let key1 = QueryCacheKey::new("MATCH (n) RETURN n", "default");
        let key2 = QueryCacheKey::new("CYPHER replan=force MATCH (n) RETURN n", "default");
        assert_eq!(key1.normalized_query, key2.normalized_query);
        // #1371: the prelude ends where the statement starts; its `USE` is
        // part of the key.
        let key3 = QueryCacheKey::new("CYPHER replan=force USE g MATCH (n) RETURN n", "default");
        assert_eq!(key3.normalized_query, "USE g MATCH (n) RETURN n");
    }

    #[test]
    fn test_cache_basic_operations() {
        let cache = QueryCache::with_defaults();
        let key = QueryCacheKey::new("MATCH (n) RETURN n", "default");

        // Cache miss
        assert_eq!(cache.get(&key), None);
        assert_eq!(cache.metrics().misses, 1);

        // Insert
        cache.insert(key.clone(), "SELECT * FROM nodes".to_string());

        // Cache hit
        assert_eq!(cache.get(&key), Some("SELECT * FROM nodes".to_string()));
        assert_eq!(cache.metrics().hits, 1);
    }

    #[test]
    fn test_cache_lru_eviction() {
        let config = QueryCacheConfig {
            enabled: true,
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
        };
        let cache = QueryCache::new(config);

        let key1 = QueryCacheKey::new("MATCH (n) RETURN n", "default");
        let key2 = QueryCacheKey::new("MATCH (n)-[r]->(m) RETURN n,m", "default");
        let key3 = QueryCacheKey::new("MATCH (n) WHERE n.age > 25 RETURN n", "default");

        // Insert first two entries
        cache.insert(key1.clone(), "SQL1".to_string());
        std::thread::sleep(std::time::Duration::from_millis(5)); // Small delay ensures distinct timestamps
        cache.insert(key2.clone(), "SQL2".to_string());
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Access key1 to update its timestamp (making key2 LRU)
        assert!(cache.get(&key1).is_some());
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Insert key3 should evict key2 (oldest last_accessed)
        cache.insert(key3.clone(), "SQL3".to_string());

        // Verify: key1 and key3 should be present, key2 should be evicted
        assert!(
            cache.get(&key1).is_some(),
            "key1 should still be in cache (recently accessed)"
        );
        assert!(
            cache.get(&key2).is_none(),
            "key2 should be evicted (least recently used)"
        );
        assert!(
            cache.get(&key3).is_some(),
            "key3 should be in cache (just inserted)"
        );
        assert_eq!(
            cache.metrics().evictions,
            1,
            "exactly one eviction should have occurred"
        );
    }

    #[test]
    fn test_schema_invalidation() {
        let cache = QueryCache::with_defaults();

        let key1 = QueryCacheKey::new("MATCH (n) RETURN n", "schema1");
        let key2 = QueryCacheKey::new("MATCH (n) RETURN n", "schema2");

        cache.insert(key1.clone(), "SQL1".to_string());
        cache.insert(key2.clone(), "SQL2".to_string());

        // Invalidate schema1
        cache.invalidate_schema("schema1");

        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_cache_metrics() {
        let cache = QueryCache::with_defaults();
        let key = QueryCacheKey::new("MATCH (n) RETURN n", "default");

        cache.insert(key.clone(), "SELECT * FROM nodes".to_string());
        cache.get(&key); // hit
        cache.get(&key); // hit

        let key2 = QueryCacheKey::new("MATCH (n)-[r]->(m) RETURN n", "default");
        cache.get(&key2); // miss

        let metrics = cache.metrics();
        assert_eq!(metrics.hits, 2);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hit_rate(), 2.0 / 3.0);
        assert_eq!(metrics.size, 1);
    }

    /// Whitespace inside a string or an escaped name is part of the query
    /// (#1489 review): such keys differ; outside quotes it is normalized.
    #[test]
    fn whitespace_inside_quotes_is_kept_in_the_key_1489() {
        let key = |q: &str| QueryCacheKey::new(q, "s").normalized_query;
        assert_ne!(
            key("MATCH (u) WHERE u.name = 'Alice  Johnson' RETURN u"),
            key("MATCH (u) WHERE u.name = 'Alice Johnson' RETURN u")
        );
        assert_ne!(key("RETURN 1 AS `x  y`"), key("RETURN 1 AS `x y`"));
        assert_ne!(key("RETURN u.`a  b`"), key("RETURN u.`a b`"));
        assert_ne!(key(r#"RETURN "a  b""#), key(r#"RETURN "a b""#));
        // A quote inside a comment opens no string (review).
        assert_ne!(
            key("MATCH (u) // it's\nWHERE u.name = 'Alice  Johnson' RETURN u"),
            key("MATCH (u) // it's\nWHERE u.name = 'Alice Johnson' RETURN u")
        );
        assert_eq!(
            key("  MATCH (u)\n\tRETURN   u.`a  b` , 'it\\'s  x' , `c``  d`  "),
            "MATCH (u) RETURN u.`a  b` , 'it\\'s  x' , `c``  d`"
        );
    }
}
