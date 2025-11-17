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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Cache control strategy from CYPHER replan option
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplanOption {
    /// Normal cache behavior - use cache if available, regenerate if needed
    Default,
    /// Force regeneration even if cached - useful for debugging or cache warming
    Force,
    /// Always use cache - error if not cached - useful to prevent planning latency spikes
    Skip,
}

impl Default for ReplanOption {
    fn default() -> Self {
        ReplanOption::Default
    }
}

impl ReplanOption {
    /// Parse replan option from query string prefix
    ///
    /// Examples:
    /// - "CYPHER replan=force MATCH ..." -> Some(ReplanOption::Force)
    /// - "CYPHER replan=skip MATCH ..." -> Some(ReplanOption::Skip)
    /// - "MATCH ..." -> None (use default)
    pub fn from_query_prefix(query: &str) -> Option<Self> {
        let trimmed = query.trim();
        if !trimmed.to_uppercase().starts_with("CYPHER") {
            return None;
        }

        // Simple parser for "CYPHER replan=<option>"
        if trimmed.to_uppercase().contains("REPLAN=FORCE") {
            Some(ReplanOption::Force)
        } else if trimmed.to_uppercase().contains("REPLAN=SKIP") {
            Some(ReplanOption::Skip)
        } else if trimmed.to_uppercase().contains("REPLAN=DEFAULT") {
            Some(ReplanOption::Default)
        } else {
            None
        }
    }

    /// Remove CYPHER prefix from query string
    ///
    /// Examples:
    /// - "CYPHER replan=force MATCH ..." -> "MATCH ..."
    /// - "MATCH ..." -> "MATCH ..."
    pub fn strip_prefix(query: &str) -> &str {
        let trimmed = query.trim();
        if !trimmed.to_uppercase().starts_with("CYPHER") {
            return query;
        }

        // Find first occurrence of MATCH, RETURN, WITH, UNWIND, CREATE, etc.
        // These are the actual Cypher clause keywords
        let cypher_keywords = [
            "MATCH", "RETURN", "WITH", "UNWIND", "CREATE", "MERGE", "DELETE", "SET", "REMOVE",
            "CALL", "EXPLAIN", "PROFILE", "USE",
        ];

        for keyword in cypher_keywords {
            if let Some(pos) = trimmed.to_uppercase().find(keyword) {
                return trimmed[pos..].trim();
            }
        }

        // If no keyword found, return original
        query
    }
}

/// Key for cache lookup
///
/// Uses normalized query and schema name to uniquely identify a query template.
/// View parameters are NOT part of the cache key - they are substituted at execution time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryCacheKey {
    /// Normalized Cypher query (with CYPHER prefix stripped)
    pub normalized_query: String,
    /// Schema name for multi-tenant support
    pub schema_name: String,
}

impl QueryCacheKey {
    pub fn new(query: &str, schema_name: &str) -> Self {
        // Strip CYPHER prefix if present
        let stripped = ReplanOption::strip_prefix(query);

        // Normalize whitespace: collapse multiple spaces/tabs/newlines into single space
        let normalized = stripped.split_whitespace().collect::<Vec<&str>>().join(" ");

        QueryCacheKey {
            normalized_query: normalized,
            schema_name: schema_name.to_string(),
        }
    }
}

/// Cached entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    /// SQL template with $paramName placeholders
    sql_template: String,
    /// Approximate size in bytes for memory tracking
    size_bytes: usize,
    /// Last access timestamp (for LRU)
    last_accessed: u64,
    /// Number of times this entry was accessed
    access_count: u64,
}

impl CacheEntry {
    fn new(sql_template: String) -> Self {
        let size_bytes = sql_template.len() + std::mem::size_of::<Self>();
        CacheEntry {
            sql_template,
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
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.cache.lock().unwrap();
        if let Some(entry) = cache.get_mut(key) {
            entry.touch();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.sql_template.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert SQL template into cache
    ///
    /// May trigger LRU eviction if cache is full
    pub fn insert(&self, key: QueryCacheKey, sql_template: String) {
        if !self.config.enabled {
            return;
        }

        let entry = CacheEntry::new(sql_template);

        let mut cache = self.cache.lock().unwrap();

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
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|key, _| key.schema_name != schema_name);
    }

    /// Clear entire cache
    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    /// Get cache metrics
    pub fn metrics(&self) -> CacheMetrics {
        let cache = self.cache.lock().unwrap();
        let size = cache.len();
        let size_bytes = cache.values().map(|e| e.size_bytes).sum();

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
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replan_option_parsing() {
        assert_eq!(ReplanOption::from_query_prefix("MATCH (n) RETURN n"), None);
        assert_eq!(
            ReplanOption::from_query_prefix("CYPHER replan=force MATCH (n) RETURN n"),
            Some(ReplanOption::Force)
        );
        assert_eq!(
            ReplanOption::from_query_prefix("CYPHER replan=skip MATCH (n) RETURN n"),
            Some(ReplanOption::Skip)
        );
        assert_eq!(
            ReplanOption::from_query_prefix("CYPHER replan=default MATCH (n) RETURN n"),
            Some(ReplanOption::Default)
        );
        assert_eq!(
            ReplanOption::from_query_prefix("cypher replan=FORCE match (n) return n"),
            Some(ReplanOption::Force)
        );
    }

    #[test]
    fn test_strip_prefix() {
        assert_eq!(
            ReplanOption::strip_prefix("MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            ReplanOption::strip_prefix("CYPHER replan=force MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            ReplanOption::strip_prefix("  CYPHER replan=skip  MATCH (n) RETURN n  "),
            "MATCH (n) RETURN n"
        );
    }

    #[test]
    fn test_cache_key_creation() {
        let key1 = QueryCacheKey::new("MATCH (n) RETURN n", "default");
        let key2 = QueryCacheKey::new("CYPHER replan=force MATCH (n) RETURN n", "default");
        assert_eq!(key1.normalized_query, key2.normalized_query);
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

        cache.insert(key1.clone(), "SQL1".to_string());
        cache.insert(key2.clone(), "SQL2".to_string());

        // Access key1 to make key2 LRU
        cache.get(&key1);

        // Insert key3 should evict key2
        cache.insert(key3.clone(), "SQL3".to_string());

        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_none());
        assert!(cache.get(&key3).is_some());
        assert_eq!(cache.metrics().evictions, 1);
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
}
