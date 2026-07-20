//! Table row-count statistics (S1 of stats-informed SQL generation).
//!
//! Fetches per-table `total_rows` from ClickHouse's `system.tables` and caches
//! them with a TTL so the query planner can rank semantically-equivalent
//! orderings (anchor/FROM choice) by table size. See
//! `docs/design/STATS_PLANNING.md` for the full design and the guardrails.
//!
//! ## Guardrails (PRIORITIES.md §1.7)
//!
//! Statistics may influence **ordering only** — join order, anchor/FROM
//! choice, traversal direction. They must NEVER change row membership: no
//! pruning of UNION arms, no skipping tables, no predicate changes. Consumers
//! of [`TableStatsSnapshot`] are expected to use it exclusively for ranking
//! among orderings that produce identical result sets.
//!
//! ## Gating
//!
//! Everything here is dormant unless the server explicitly wires it up
//! (`CLICKGRAPH_STATS_ENABLED=true`). The planner reads stats only through the
//! task-local `QueryContext` snapshot; when no snapshot was attached (the
//! default, and always the case in sql_only library paths and the golden/corpus
//! test suites), planning behavior is byte-identical to the stats-less engine.
//!
//! ## Source pluggability
//!
//! The fetch is abstracted behind [`TableStatsSource`] so other backends can
//! plug in later (e.g. a warehouse probe via `DESCRIBE TABLE EXTENDED`,
//! mirroring how `schema_discovery` has a per-backend sibling). Only the
//! ClickHouse source is implemented in this slice; tests use in-memory fixture
//! sources.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Client;

use crate::graph_catalog::graph_schema::GraphSchema;

/// Default TTL (seconds) for cached row counts (`CLICKGRAPH_STATS_TTL_SECS`).
pub const DEFAULT_STATS_TTL_SECS: u64 = 300;

// =============================================================================
// Snapshot — the immutable view the planner consumes
// =============================================================================

/// An immutable point-in-time view of per-table row counts, keyed by
/// fully-qualified `database.table` name (matching
/// `NodeSchema::full_table_name()` / `RelationshipSchema::full_table_name()`).
///
/// Tables with unknown row counts (e.g. non-MergeTree engines where
/// `system.tables.total_rows` is NULL, or tables not present at fetch time)
/// are simply absent — [`TableStatsSnapshot::row_count`] returns `None` and
/// the planner falls back to its stats-less behavior for those candidates.
#[derive(Debug, Default, Clone)]
pub struct TableStatsSnapshot {
    rows: HashMap<String, u64>,
    /// When this snapshot's data was fetched (informational; TTL enforcement
    /// lives in [`TableStatsCache`]).
    fetched_at: Option<Instant>,
}

impl TableStatsSnapshot {
    /// Build a snapshot from `full_table_name -> row_count` pairs. Public so
    /// tests can inject fixed fixtures without any live database.
    pub fn from_counts(rows: HashMap<String, u64>) -> Self {
        Self {
            rows,
            fetched_at: Some(Instant::now()),
        }
    }

    /// Row count for a fully-qualified `database.table` name, or `None` if
    /// unknown. Backtick quoting is normalized away so callers can pass table
    /// names as they appear in join metadata.
    pub fn row_count(&self, full_table_name: &str) -> Option<u64> {
        if let Some(n) = self.rows.get(full_table_name) {
            return Some(*n);
        }
        if full_table_name.contains('`') {
            let normalized: String = full_table_name.chars().filter(|c| *c != '`').collect();
            return self.rows.get(&normalized).copied();
        }
        None
    }

    /// Number of tables with known counts.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when no table has a known count.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// When the snapshot's data was fetched, if known.
    pub fn fetched_at(&self) -> Option<Instant> {
        self.fetched_at
    }
}

// =============================================================================
// Source abstraction — where row counts come from
// =============================================================================

/// A backend that can report `full_table_name -> row_count` for a set of
/// databases. `None`-count tables (unknown) must be omitted from the map.
#[async_trait::async_trait]
pub trait TableStatsSource: Send + Sync {
    async fn fetch(&self, databases: &[String]) -> Result<HashMap<String, u64>, String>;
}

/// ClickHouse implementation: one query over `system.tables`.
///
/// `total_rows` is `Nullable(UInt64)` — NULL for engines that don't track a
/// row count (views, some non-MergeTree engines). NULL rows are dropped from
/// the result (unknown), never coerced to 0: a 0 would make the planner prefer
/// that table as "smallest", which is exactly wrong for an unknown.
pub struct ClickHouseTableStatsSource {
    client: Client,
}

impl ClickHouseTableStatsSource {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Same identifier discipline as `schema_discovery::validate_sql_identifier`:
/// database names are interpolated into the query text, so restrict them to a
/// safe identifier charset instead of trusting schema-file contents.
fn validate_database_identifier(identifier: &str) -> Result<&str, String> {
    let ok = !identifier.is_empty()
        && identifier
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && identifier
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(identifier)
    } else {
        Err(format!("Invalid database identifier: {}", identifier))
    }
}

#[async_trait::async_trait]
impl TableStatsSource for ClickHouseTableStatsSource {
    async fn fetch(&self, databases: &[String]) -> Result<HashMap<String, u64>, String> {
        if databases.is_empty() {
            return Ok(HashMap::new());
        }
        let mut quoted = Vec::with_capacity(databases.len());
        for db in databases {
            quoted.push(format!("'{}'", validate_database_identifier(db)?));
        }

        #[derive(Debug, clickhouse::Row, serde::Deserialize)]
        struct StatsRow {
            database: String,
            name: String,
            total_rows: Option<u64>,
        }

        let query = format!(
            "SELECT database, name, total_rows FROM system.tables WHERE database IN ({})",
            quoted.join(", ")
        );
        let rows: Vec<StatsRow> = self
            .client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| format!("Failed to fetch table stats: {}", e))?;

        let mut out = HashMap::new();
        for row in rows {
            // NULL total_rows (non-MergeTree engines) => unknown => omit.
            if let Some(n) = row.total_rows {
                out.insert(format!("{}.{}", row.database, row.name), n);
            }
        }
        Ok(out)
    }
}

// =============================================================================
// Cache — TTL-refreshed store shared across queries
// =============================================================================

struct CacheState {
    snapshot: Option<Arc<TableStatsSnapshot>>,
    /// Databases *attempted* in the last fetch (success OR failure) — the union
    /// fetched at `last_attempt`. Freshness is keyed off this, not off what
    /// succeeded, so a database whose fetch failed still respects the TTL
    /// instead of forcing a refetch on every subsequent query. A request for a
    /// database outside this set is genuinely new and forces a refresh.
    attempted_dbs: BTreeSet<String>,
    /// Last fetch *attempt* (success or failure). Failures keep serving the
    /// stale snapshot but are not retried until the TTL elapses again, so a
    /// down backend can't be hammered on every query.
    last_attempt: Option<Instant>,
}

/// TTL-refreshed row-count cache. One instance is shared process-wide (server
/// mode); the embedded/remote library modes can construct their own later.
///
/// Refresh happens lazily on [`TableStatsCache::snapshot`] when the TTL has
/// elapsed or the requested databases aren't covered yet. Fetch errors are
/// logged and the previous (stale) snapshot keeps serving — stats are a
/// planning hint, never a correctness dependency, so degraded is fine.
pub struct TableStatsCache {
    source: Box<dyn TableStatsSource>,
    ttl: Duration,
    state: tokio::sync::RwLock<CacheState>,
}

impl TableStatsCache {
    pub fn new(source: Box<dyn TableStatsSource>, ttl: Duration) -> Self {
        Self {
            source,
            ttl,
            state: tokio::sync::RwLock::new(CacheState {
                snapshot: None,
                attempted_dbs: BTreeSet::new(),
                last_attempt: None,
            }),
        }
    }

    /// Current snapshot covering `databases`, refreshing first if the cache is
    /// stale (TTL elapsed / never fetched) or doesn't cover a requested
    /// database. Returns `None` only before the first successful fetch.
    pub async fn snapshot(&self, databases: &[String]) -> Option<Arc<TableStatsSnapshot>> {
        // Fast path: fresh and covering.
        {
            let st = self.state.read().await;
            if Self::is_fresh(&st, self.ttl, databases) {
                return st.snapshot.clone();
            }
        }

        let mut st = self.state.write().await;
        // Re-check under the write lock: another task may have refreshed.
        if Self::is_fresh(&st, self.ttl, databases) {
            return st.snapshot.clone();
        }

        // Fetch the union of previously-attempted and requested databases so a
        // multi-schema server keeps counts for all schemas it has served.
        let mut fetch_dbs: BTreeSet<String> = st.attempted_dbs.clone();
        fetch_dbs.extend(databases.iter().cloned());
        let fetch_list: Vec<String> = fetch_dbs.iter().cloned().collect();

        st.last_attempt = Some(Instant::now());
        // Record the attempt set BEFORE the fetch resolves: freshness keys off
        // what was attempted, not what succeeded, so a failed db still respects
        // the TTL rather than forcing a refetch on every query.
        st.attempted_dbs = fetch_dbs;
        match self.source.fetch(&fetch_list).await {
            Ok(rows) => {
                log::debug!(
                    "table stats refreshed: {} tables across {} database(s)",
                    rows.len(),
                    fetch_list.len()
                );
                st.snapshot = Some(Arc::new(TableStatsSnapshot::from_counts(rows)));
            }
            Err(e) => {
                log::warn!(
                    "table stats refresh failed ({}); keeping previous snapshot{}",
                    e,
                    if st.snapshot.is_some() {
                        ""
                    } else {
                        " (none yet — planner runs stats-less)"
                    }
                );
            }
        }
        st.snapshot.clone()
    }

    fn is_fresh(st: &CacheState, ttl: Duration, databases: &[String]) -> bool {
        let attempted_recently = st.last_attempt.is_some_and(|at| at.elapsed() < ttl);
        // `attempted_dbs` holds every db in the last fetch's union, whether it
        // succeeded or failed — so a recently-failed db is "covered" here and
        // won't be re-hit until the TTL elapses. Only a genuinely new db
        // (never attempted) misses coverage and forces a refresh.
        let attempted = databases.iter().all(|db| st.attempted_dbs.contains(db));
        attempted_recently && attempted
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Distinct databases referenced by a schema's node and relationship tables,
/// sorted for determinism. This is the schema-catalog API planning code uses
/// to know which databases a query's stats must cover (axis rule: table/db
/// names flow from the catalog, not from re-derived pattern flags).
pub fn schema_databases(schema: &GraphSchema) -> Vec<String> {
    let mut dbs: BTreeSet<String> = BTreeSet::new();
    for node in schema.all_node_schemas().values() {
        dbs.insert(node.database.clone());
    }
    for rel in schema.get_relationships_schemas().values() {
        dbs.insert(rel.database.clone());
    }
    dbs.remove("");
    dbs.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Fixture source: returns a fixed map, counts fetch invocations, can be
    /// told to fail (statically, or toggled at runtime via `fail_flag`).
    struct FixtureSource {
        rows: HashMap<String, u64>,
        calls: Arc<AtomicUsize>,
        fail: bool,
        /// Runtime-toggleable failure, checked in addition to `fail`.
        fail_flag: Arc<AtomicBool>,
    }

    impl FixtureSource {
        fn new(rows: HashMap<String, u64>) -> Self {
            Self {
                rows,
                calls: Arc::new(AtomicUsize::new(0)),
                fail: false,
                fail_flag: Arc::new(AtomicBool::new(false)),
            }
        }
        fn failing() -> Self {
            Self {
                rows: HashMap::new(),
                calls: Arc::new(AtomicUsize::new(0)),
                fail: true,
                fail_flag: Arc::new(AtomicBool::new(false)),
            }
        }
        /// Handles to observe call count and flip failure on/off after construction.
        fn probes(&self) -> (Arc<AtomicUsize>, Arc<AtomicBool>) {
            (self.calls.clone(), self.fail_flag.clone())
        }
    }

    #[async_trait::async_trait]
    impl TableStatsSource for FixtureSource {
        async fn fetch(&self, _databases: &[String]) -> Result<HashMap<String, u64>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail || self.fail_flag.load(Ordering::SeqCst) {
                Err("fixture failure".to_string())
            } else {
                Ok(self.rows.clone())
            }
        }
    }

    fn counts(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn snapshot_lookup_and_backtick_normalization() {
        let snap = TableStatsSnapshot::from_counts(counts(&[("social.users_bench", 8)]));
        assert_eq!(snap.row_count("social.users_bench"), Some(8));
        assert_eq!(snap.row_count("`social`.`users_bench`"), Some(8));
        // Unknown table => None (never 0)
        assert_eq!(snap.row_count("social.missing"), None);
    }

    #[tokio::test]
    async fn cache_serves_fresh_snapshot_without_refetch() {
        let src = FixtureSource::new(counts(&[("db.t", 5)]));
        let cache = TableStatsCache::new(Box::new(src), Duration::from_secs(3600));
        let dbs = vec!["db".to_string()];

        let s1 = cache.snapshot(&dbs).await.expect("first snapshot");
        assert_eq!(s1.row_count("db.t"), Some(5));
        let s2 = cache.snapshot(&dbs).await.expect("second snapshot");
        assert_eq!(s2.row_count("db.t"), Some(5));

        // Read the call count back out of the boxed source via a fresh probe:
        // instead, assert via pointer equality — same Arc means no refetch.
        assert!(Arc::ptr_eq(&s1, &s2), "fresh snapshot must be reused");
    }

    #[tokio::test]
    async fn cache_refetches_after_ttl_expiry() {
        let src = FixtureSource::new(counts(&[("db.t", 5)]));
        // TTL zero: every access is stale.
        let cache = TableStatsCache::new(Box::new(src), Duration::ZERO);
        let dbs = vec!["db".to_string()];

        let s1 = cache.snapshot(&dbs).await.expect("snapshot");
        let s2 = cache.snapshot(&dbs).await.expect("snapshot");
        assert!(
            !Arc::ptr_eq(&s1, &s2),
            "expired snapshot must be replaced by a fresh fetch"
        );
    }

    #[tokio::test]
    async fn cache_refreshes_when_new_database_requested() {
        let src = FixtureSource::new(counts(&[("a.t", 1), ("b.t", 2)]));
        let cache = TableStatsCache::new(Box::new(src), Duration::from_secs(3600));

        let s1 = cache
            .snapshot(&["a".to_string()])
            .await
            .expect("snapshot for a");
        // Requesting an uncovered database within TTL must force a refresh.
        let s2 = cache
            .snapshot(&["b".to_string()])
            .await
            .expect("snapshot for b");
        assert!(!Arc::ptr_eq(&s1, &s2), "uncovered database must refresh");
        assert_eq!(s2.row_count("b.t"), Some(2));
        // And the union stays covered: asking for a again is a cache hit.
        let s3 = cache.snapshot(&["a".to_string()]).await.expect("snapshot");
        assert!(Arc::ptr_eq(&s2, &s3));
    }

    #[tokio::test]
    async fn cache_failure_returns_none_then_does_not_hammer() {
        let src = FixtureSource::failing();
        let (calls, _) = src.probes();
        let cache = TableStatsCache::new(Box::new(src), Duration::from_secs(3600));
        let dbs = vec!["db".to_string()];

        assert!(
            cache.snapshot(&dbs).await.is_none(),
            "no snapshot on failure"
        );
        // Second call within TTL: failed attempt is remembered, no snapshot,
        // and (behaviorally) the planner just runs stats-less.
        assert!(cache.snapshot(&dbs).await.is_none());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "failed db must not be refetched within TTL"
        );
    }

    /// Regression (P-5 review MAJOR): a successful fetch covering db `a`,
    /// followed by a request for db `b` whose fetch FAILS, must not refetch `b`
    /// on every subsequent query within the TTL. Before the fix, `is_fresh`
    /// keyed off successfully-covered dbs, so the still-uncovered `b` forced a
    /// synchronous fetch under the write lock on every query.
    #[tokio::test]
    async fn cache_does_not_hammer_a_persistently_failing_uncovered_db() {
        let src = FixtureSource::new(counts(&[("a.t", 1)]));
        let (calls, fail_flag) = src.probes();
        let cache = TableStatsCache::new(Box::new(src), Duration::from_secs(3600));

        // Cover `a` successfully.
        let sa = cache.snapshot(&["a".to_string()]).await.expect("a covered");
        assert_eq!(sa.row_count("a.t"), Some(1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Now make fetches fail, then request the uncovered `b` repeatedly.
        fail_flag.store(true, Ordering::SeqCst);
        for _ in 0..5 {
            // `b` was attempted (and failed) once; further requests within TTL
            // must be served stats-less WITHOUT another fetch.
            let _ = cache.snapshot(&["b".to_string()]).await;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "b must be attempted exactly once, not on every query"
        );

        // `a` is still covered from the first successful fetch — cache hit, no refetch.
        let sa2 = cache
            .snapshot(&["a".to_string()])
            .await
            .expect("a still covered");
        assert_eq!(sa2.row_count("a.t"), Some(1));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "covered a must remain a cache hit"
        );
    }

    #[test]
    fn database_identifier_validation() {
        assert!(validate_database_identifier("social").is_ok());
        assert!(validate_database_identifier("db_1").is_ok());
        assert!(validate_database_identifier("").is_err());
        assert!(validate_database_identifier("bad-name").is_err());
        assert!(validate_database_identifier("x'; DROP TABLE t; --").is_err());
        assert!(validate_database_identifier("1starts_with_digit").is_err());
    }
}
