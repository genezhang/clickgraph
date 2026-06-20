//! Server observability registry.
//!
//! Aggregate counters, per-phase latency histograms, captured ClickHouse
//! execution stats, and a bounded slow-query ring buffer — rendered as both
//! Prometheus exposition text (`/metrics`) and a JSON snapshot (`/stats`).
//!
//! Deliberately **zero new dependencies**: hand-rolled `AtomicU64` counters and
//! fixed-bucket histograms, mirroring the `CacheMetrics` atomics style in
//! `query_cache.rs`. One source of truth ([`ServerMetrics`]) serves both
//! outputs. Prometheus exposition is just `name{labels} value` lines, so no
//! exporter crate is needed; percentiles are bucket-approximate, which is
//! standard for an ops dashboard.
//!
//! Recording is a single cheap call ([`ServerMetrics::record_query`]) made once
//! per query from both the HTTP handler and the Bolt handler. The hot path is
//! relaxed atomic adds plus a small bucket scan; the only lock is the
//! slow-query ring insert, which is off the counter path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::handlers::QueryPerformanceMetrics;

/// Runtime configuration for the registry, built from `ServerConfig` at startup.
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub slow_query_capacity: usize,
    pub slow_query_threshold_ms: u64,
    /// Whether to retain a truncated Cypher preview in the ring buffer.
    pub query_preview: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            slow_query_capacity: 128,
            slow_query_threshold_ms: 0,
            query_preview: false,
        }
    }
}

// ── bounded labels ───────────────────────────────────────────────────────────
// Only ever label by these bounded sets — never by raw query text, role,
// tenant, schema, or user (those are unbounded and would explode series count).

const QUERY_TYPES: [&str; 8] = [
    "read",
    "ddl",
    "update",
    "delete",
    "call",
    "procedure",
    "bolt",
    "other",
];

fn query_type_index(t: &str) -> usize {
    QUERY_TYPES
        .iter()
        .position(|&q| q == t)
        .unwrap_or(QUERY_TYPES.len() - 1) // "other"
}

/// Coarse classification of a failed query, used as a bounded Prometheus label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    BadRequest,
    NotFound,
    Capacity,
    Exec,
    Internal,
}

const ERROR_CLASSES: [&str; 5] = ["bad_request", "not_found", "capacity", "exec", "internal"];

impl ErrorClass {
    fn index(self) -> usize {
        match self {
            ErrorClass::BadRequest => 0,
            ErrorClass::NotFound => 1,
            ErrorClass::Capacity => 2,
            ErrorClass::Exec => 3,
            ErrorClass::Internal => 4,
        }
    }

    /// Map an HTTP status code to an error class (for the outer handler arm).
    pub fn from_status(code: u16) -> Self {
        match code {
            400 | 422 => ErrorClass::BadRequest,
            404 => ErrorClass::NotFound,
            429 | 503 => ErrorClass::Capacity,
            500 => ErrorClass::Internal,
            _ => ErrorClass::Exec,
        }
    }
}

/// Outcome of a recorded query.
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Ok,
    Err(ErrorClass),
}

// ── ClickHouse-side execution stats ──────────────────────────────────────────

/// Execution stats captured from the ClickHouse side for one query. `Phase A`
/// fills `network_bytes` from the response cursor (always available in remote
/// mode); `Phase B` (gated) fills the `read_*`/`elapsed` fields from the
/// `X-ClickHouse-Summary` header.
#[derive(Clone, Debug, Default)]
pub struct ChExecStats {
    pub network_bytes: u64,
    pub read_rows: Option<u64>,
    pub read_bytes: Option<u64>,
    pub elapsed_ns: Option<u64>,
}

tokio::task_local! {
    /// Per-query slot the executor writes ClickHouse stats into and the
    /// recording point reads. Scoped by the query handlers via
    /// [`with_ch_stats`]; absent in embedded/Databricks modes.
    static CH_STATS_SLOT: std::cell::RefCell<ChExecStats>;
}

/// Run `f` with a fresh ClickHouse-stats slot in scope. `record_ch_*` calls
/// from `remote.rs` during `f` land in the slot, and [`current_ch_stats`] reads
/// it at the recording point (which runs inside the same scope). No-op effect
/// in embedded/Databricks modes where nothing writes the slot.
pub async fn with_ch_stats_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CH_STATS_SLOT
        .scope(std::cell::RefCell::new(ChExecStats::default()), f)
        .await
}

/// Read the ClickHouse stats accumulated for the current query, or `None` when
/// not inside a [`with_ch_stats_scope`] (e.g. embedded/Databricks, or a code
/// path that doesn't wrap execution).
pub fn current_ch_stats() -> Option<ChExecStats> {
    CH_STATS_SLOT.try_with(|s| s.borrow().clone()).ok()
}

/// Record bytes transferred for the current query (Phase A). No-op outside a
/// [`with_ch_stats_scope`] scope.
pub fn record_ch_network_bytes(bytes: u64) {
    let _ = CH_STATS_SLOT.try_with(|s| s.borrow_mut().network_bytes += bytes);
}

/// Record the parsed `X-ClickHouse-Summary` figures for the current query
/// (Phase B). No-op outside a [`with_ch_stats`] scope.
pub fn record_ch_summary(read_rows: u64, read_bytes: u64, elapsed_ns: u64) {
    let _ = CH_STATS_SLOT.try_with(|s| {
        let mut st = s.borrow_mut();
        st.read_rows = Some(read_rows);
        st.read_bytes = Some(read_bytes);
        st.elapsed_ns = Some(elapsed_ns);
    });
}

// ── latency histogram ────────────────────────────────────────────────────────

/// Fixed upper bounds in seconds; an implicit `+Inf` bucket follows.
const BUCKET_BOUNDS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const N_BUCKETS: usize = BUCKET_BOUNDS.len() + 1; // + +Inf

/// A cumulative latency histogram over [`BUCKET_BOUNDS`].
struct LatencyHistogram {
    /// Per-bucket counts (NOT cumulative); index `BUCKET_BOUNDS.len()` is +Inf.
    buckets: [AtomicU64; N_BUCKETS],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn observe(&self, secs: f64) {
        let idx = BUCKET_BOUNDS
            .iter()
            .position(|&b| secs <= b)
            .unwrap_or(N_BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((secs * 1_000_000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        let counts: [u64; N_BUCKETS] =
            std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed));
        HistogramSnapshot {
            counts,
            sum_micros: self.sum_micros.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct HistogramSnapshot {
    counts: [u64; N_BUCKETS],
    sum_micros: u64,
    count: u64,
}

impl HistogramSnapshot {
    /// Bucket-approximate quantile in milliseconds: the upper bound of the
    /// bucket where the cumulative count first crosses `q`.
    fn percentile_ms(&self, q: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (q * self.count as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &c) in self.counts.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                // +Inf bucket → report the last finite bound as the floor.
                let bound = BUCKET_BOUNDS.get(i).copied().unwrap_or(f64::INFINITY);
                return if bound.is_finite() {
                    bound * 1000.0
                } else {
                    BUCKET_BOUNDS[BUCKET_BOUNDS.len() - 1] * 1000.0
                };
            }
        }
        BUCKET_BOUNDS[BUCKET_BOUNDS.len() - 1] * 1000.0
    }

    fn mean_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum_micros as f64 / self.count as f64) / 1000.0
        }
    }
}

/// The six query phases tracked as histograms. `total` and `exec` are recorded
/// for every query; the rest are HTTP-only (the Bolt path lacks per-phase
/// timers).
const PHASES: [&str; 6] = ["total", "parse", "plan", "render", "sqlgen", "exec"];

// ── slow-query ring buffer ───────────────────────────────────────────────────

/// One entry in the slow-query ring. `query_preview` is JSON-only (never a
/// Prometheus label) and present only when `query_preview` config is on.
#[derive(Clone, Debug, Serialize)]
pub struct SlowQueryRecord {
    pub timestamp_ms: u64,
    pub query_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_preview: Option<String>,
    pub total_ms: f64,
    pub exec_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ch_read_rows: Option<u64>,
    pub outcome: &'static str,
}

struct SlowQueryRing {
    buf: VecDeque<SlowQueryRecord>,
    cap: usize,
}

impl SlowQueryRing {
    fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap.min(1024)),
            cap: cap.max(1),
        }
    }

    fn push(&mut self, rec: SlowQueryRecord) {
        if self.buf.len() >= self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(rec);
    }

    /// Most recent `n`, newest first.
    fn recent(&self, n: usize) -> Vec<SlowQueryRecord> {
        self.buf.iter().rev().take(n).cloned().collect()
    }

    /// Top `n` by `total_ms`, slowest first.
    fn slowest(&self, n: usize) -> Vec<SlowQueryRecord> {
        let mut v: Vec<SlowQueryRecord> = self.buf.iter().cloned().collect();
        v.sort_by(|a, b| {
            b.total_ms
                .partial_cmp(&a.total_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        v.truncate(n);
        v
    }
}

// ── the registry ─────────────────────────────────────────────────────────────

/// A single query observation handed to [`ServerMetrics::record_query`].
pub struct QuerySample<'a> {
    pub metrics: &'a QueryPerformanceMetrics,
    pub outcome: Outcome,
    /// HTTP samples carry full per-phase timings; Bolt samples carry only
    /// `total`/`exec`, so the other phase histograms are skipped for them.
    pub has_phase_breakdown: bool,
    /// Raw Cypher, used only for the (config-gated, truncated) ring preview.
    pub query_text: Option<&'a str>,
    pub ch: Option<ChExecStats>,
}

/// RAII guard that increments the in-flight gauge on creation and decrements on
/// drop — robust against the query handler's many early returns and panics.
pub struct InFlightGuard<'a>(&'a AtomicI64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Aggregate server metrics registry. Held behind a global `OnceCell`.
pub struct ServerMetrics {
    start_time: Instant,
    cfg: MetricsConfig,

    queries_total: AtomicU64,
    queries_failed: AtomicU64,
    result_rows_total: AtomicU64,
    in_flight: AtomicI64,
    by_type: [AtomicU64; QUERY_TYPES.len()],
    errors_by_class: [AtomicU64; ERROR_CLASSES.len()],

    histograms: [LatencyHistogram; PHASES.len()],

    ch_network_bytes: AtomicU64,
    ch_read_rows: AtomicU64,
    ch_read_bytes: AtomicU64,

    slow_queries: Mutex<SlowQueryRing>,
}

impl ServerMetrics {
    pub fn new(cfg: MetricsConfig) -> Self {
        let cap = cfg.slow_query_capacity;
        Self {
            start_time: Instant::now(),
            cfg,
            queries_total: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            result_rows_total: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
            by_type: std::array::from_fn(|_| AtomicU64::new(0)),
            errors_by_class: std::array::from_fn(|_| AtomicU64::new(0)),
            histograms: std::array::from_fn(|_| LatencyHistogram::new()),
            ch_network_bytes: AtomicU64::new(0),
            ch_read_rows: AtomicU64::new(0),
            ch_read_bytes: AtomicU64::new(0),
            slow_queries: Mutex::new(SlowQueryRing::new(cap)),
        }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Increment in-flight; the returned guard decrements on drop.
    pub fn in_flight_guard(&self) -> InFlightGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard(&self.in_flight)
    }

    /// Record a single error by class without phase timings — for early-return
    /// paths (e.g. capacity reject, parse failure) that never built a full
    /// `QueryPerformanceMetrics`.
    pub fn record_error(&self, class: ErrorClass) {
        if !self.cfg.enabled {
            return;
        }
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.queries_failed.fetch_add(1, Ordering::Relaxed);
        self.errors_by_class[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed query (the single recording entry point).
    pub fn record_query(&self, sample: &QuerySample) {
        if !self.cfg.enabled {
            return;
        }
        let m = sample.metrics;
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.by_type[query_type_index(&m.query_type)].fetch_add(1, Ordering::Relaxed);
        if let Some(rows) = m.result_rows {
            self.result_rows_total
                .fetch_add(rows as u64, Ordering::Relaxed);
        }
        if let Outcome::Err(class) = sample.outcome {
            self.queries_failed.fetch_add(1, Ordering::Relaxed);
            self.errors_by_class[class.index()].fetch_add(1, Ordering::Relaxed);
        }

        // Histograms: total + exec always; the rest only for HTTP samples.
        self.histograms[0].observe(m.total_time);
        self.histograms[5].observe(m.execution_time);
        if sample.has_phase_breakdown {
            self.histograms[1].observe(m.parse_time);
            self.histograms[2].observe(m.planning_time);
            self.histograms[3].observe(m.render_time);
            self.histograms[4].observe(m.sql_generation_time);
        }

        if let Some(ch) = &sample.ch {
            self.ch_network_bytes
                .fetch_add(ch.network_bytes, Ordering::Relaxed);
            if let Some(r) = ch.read_rows {
                self.ch_read_rows.fetch_add(r, Ordering::Relaxed);
            }
            if let Some(b) = ch.read_bytes {
                self.ch_read_bytes.fetch_add(b, Ordering::Relaxed);
            }
        }

        // Slow-query ring (off the counter hot path).
        let total_ms = m.total_time * 1000.0;
        if total_ms >= self.cfg.slow_query_threshold_ms as f64 {
            let rec = SlowQueryRecord {
                timestamp_ms: now_ms(),
                query_type: m.query_type.clone(),
                query_preview: if self.cfg.query_preview {
                    sample
                        .query_text
                        .map(|q| q.chars().take(120).collect::<String>())
                } else {
                    None
                },
                total_ms,
                exec_ms: m.execution_time * 1000.0,
                result_rows: m.result_rows,
                ch_read_rows: sample.ch.as_ref().and_then(|c| c.read_rows),
                outcome: match sample.outcome {
                    Outcome::Ok => "ok",
                    Outcome::Err(c) => ERROR_CLASSES[c.index()],
                },
            };
            if let Ok(mut ring) = self.slow_queries.lock() {
                ring.push(rec);
            }
        }
    }

    pub fn recent_queries(&self, n: usize) -> Vec<SlowQueryRecord> {
        self.slow_queries
            .lock()
            .map(|r| r.recent(n))
            .unwrap_or_default()
    }

    pub fn slowest_queries(&self, n: usize) -> Vec<SlowQueryRecord> {
        self.slow_queries
            .lock()
            .map(|r| r.slowest(n))
            .unwrap_or_default()
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn in_flight(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// JSON snapshot for `/stats` (counters, latency percentiles, CH stats).
    /// Cache and pool stats are attached by the handler from their own sources.
    pub fn snapshot(&self) -> StatsSnapshot {
        let phases = std::array::from_fn::<PhaseLatency, { PHASES.len() }, _>(|i| {
            let s = self.histograms[i].snapshot();
            PhaseLatency {
                phase: PHASES[i],
                count: s.count,
                mean_ms: s.mean_ms(),
                p50_ms: s.percentile_ms(0.50),
                p95_ms: s.percentile_ms(0.95),
                p99_ms: s.percentile_ms(0.99),
            }
        });
        let by_type = QUERY_TYPES
            .iter()
            .enumerate()
            .map(|(i, &t)| (t.to_string(), self.by_type[i].load(Ordering::Relaxed)))
            .collect();
        let errors_by_class = ERROR_CLASSES
            .iter()
            .enumerate()
            .map(|(i, &c)| {
                (
                    c.to_string(),
                    self.errors_by_class[i].load(Ordering::Relaxed),
                )
            })
            .collect();
        StatsSnapshot {
            uptime_secs: self.uptime_secs(),
            queries_total: self.queries_total.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            in_flight: self.in_flight(),
            result_rows_total: self.result_rows_total.load(Ordering::Relaxed),
            by_type,
            errors_by_class,
            latency: phases.to_vec(),
            clickhouse: ChStatsSnapshot {
                network_bytes: self.ch_network_bytes.load(Ordering::Relaxed),
                read_rows: self.ch_read_rows.load(Ordering::Relaxed),
                read_bytes: self.ch_read_bytes.load(Ordering::Relaxed),
            },
        }
    }

    /// Render the registry's metrics in Prometheus exposition format. Cache and
    /// pool gauges are appended by the handler.
    pub fn render_prometheus(&self, out: &mut String) {
        use std::fmt::Write;

        let _ = writeln!(
            out,
            "# HELP clickgraph_queries_total Total queries recorded."
        );
        let _ = writeln!(out, "# TYPE clickgraph_queries_total counter");
        let _ = writeln!(
            out,
            "clickgraph_queries_total {}",
            self.queries_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP clickgraph_queries_failed_total Failed queries."
        );
        let _ = writeln!(out, "# TYPE clickgraph_queries_failed_total counter");
        let _ = writeln!(
            out,
            "clickgraph_queries_failed_total {}",
            self.queries_failed.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP clickgraph_queries_by_type_total Queries by type."
        );
        let _ = writeln!(out, "# TYPE clickgraph_queries_by_type_total counter");
        for (i, &t) in QUERY_TYPES.iter().enumerate() {
            let _ = writeln!(
                out,
                "clickgraph_queries_by_type_total{{type=\"{t}\"}} {}",
                self.by_type[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(out, "# HELP clickgraph_query_errors_total Errors by class.");
        let _ = writeln!(out, "# TYPE clickgraph_query_errors_total counter");
        for (i, &c) in ERROR_CLASSES.iter().enumerate() {
            let _ = writeln!(
                out,
                "clickgraph_query_errors_total{{class=\"{c}\"}} {}",
                self.errors_by_class[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP clickgraph_in_flight_queries Queries currently executing."
        );
        let _ = writeln!(out, "# TYPE clickgraph_in_flight_queries gauge");
        let _ = writeln!(out, "clickgraph_in_flight_queries {}", self.in_flight());

        let _ = writeln!(
            out,
            "# HELP clickgraph_result_rows_total Result rows returned."
        );
        let _ = writeln!(out, "# TYPE clickgraph_result_rows_total counter");
        let _ = writeln!(
            out,
            "clickgraph_result_rows_total {}",
            self.result_rows_total.load(Ordering::Relaxed)
        );

        // Per-phase latency histograms.
        let _ = writeln!(
            out,
            "# HELP clickgraph_query_duration_seconds Query phase latency."
        );
        let _ = writeln!(out, "# TYPE clickgraph_query_duration_seconds histogram");
        for (i, &phase) in PHASES.iter().enumerate() {
            let s = self.histograms[i].snapshot();
            let mut cumulative = 0u64;
            for (b, &bound) in BUCKET_BOUNDS.iter().enumerate() {
                cumulative += s.counts[b];
                let _ = writeln!(
                    out,
                    "clickgraph_query_duration_seconds_bucket{{phase=\"{phase}\",le=\"{bound}\"}} {cumulative}"
                );
            }
            cumulative += s.counts[N_BUCKETS - 1];
            let _ = writeln!(
                out,
                "clickgraph_query_duration_seconds_bucket{{phase=\"{phase}\",le=\"+Inf\"}} {cumulative}"
            );
            let _ = writeln!(
                out,
                "clickgraph_query_duration_seconds_sum{{phase=\"{phase}\"}} {}",
                s.sum_micros as f64 / 1_000_000.0
            );
            let _ = writeln!(
                out,
                "clickgraph_query_duration_seconds_count{{phase=\"{phase}\"}} {}",
                s.count
            );
        }

        // ClickHouse-side counters.
        let _ = writeln!(
            out,
            "# HELP clickgraph_clickhouse_network_bytes_total Bytes transferred from ClickHouse."
        );
        let _ = writeln!(
            out,
            "# TYPE clickgraph_clickhouse_network_bytes_total counter"
        );
        let _ = writeln!(
            out,
            "clickgraph_clickhouse_network_bytes_total {}",
            self.ch_network_bytes.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP clickgraph_clickhouse_read_rows_total Rows read by ClickHouse (summary)."
        );
        let _ = writeln!(out, "# TYPE clickgraph_clickhouse_read_rows_total counter");
        let _ = writeln!(
            out,
            "clickgraph_clickhouse_read_rows_total {}",
            self.ch_read_rows.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP clickgraph_clickhouse_read_bytes_total Bytes read by ClickHouse (summary)."
        );
        let _ = writeln!(out, "# TYPE clickgraph_clickhouse_read_bytes_total counter");
        let _ = writeln!(
            out,
            "clickgraph_clickhouse_read_bytes_total {}",
            self.ch_read_bytes.load(Ordering::Relaxed)
        );
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── JSON snapshot types (`/stats`) ───────────────────────────────────────────

#[derive(Serialize, Clone)]
pub struct PhaseLatency {
    pub phase: &'static str,
    pub count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Serialize)]
pub struct ChStatsSnapshot {
    pub network_bytes: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
}

#[derive(Serialize)]
pub struct StatsSnapshot {
    pub uptime_secs: u64,
    pub queries_total: u64,
    pub queries_failed: u64,
    pub in_flight: i64,
    pub result_rows_total: u64,
    pub by_type: std::collections::BTreeMap<String, u64>,
    pub errors_by_class: std::collections::BTreeMap<String, u64>,
    pub latency: Vec<PhaseLatency>,
    pub clickhouse: ChStatsSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_metrics(query_type: &str, total: f64, rows: usize) -> QueryPerformanceMetrics {
        QueryPerformanceMetrics {
            total_time: total,
            parse_time: total * 0.1,
            planning_time: total * 0.2,
            render_time: total * 0.05,
            sql_generation_time: total * 0.05,
            execution_time: total * 0.6,
            query_type: query_type.to_string(),
            sql_queries_count: 1,
            result_rows: Some(rows),
        }
    }

    fn sample<'a>(m: &'a QueryPerformanceMetrics, outcome: Outcome) -> QuerySample<'a> {
        QuerySample {
            metrics: m,
            outcome,
            has_phase_breakdown: true,
            query_text: None,
            ch: None,
        }
    }

    #[test]
    fn counters_and_by_type() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        let m = http_metrics("read", 0.01, 5);
        sm.record_query(&sample(&m, Outcome::Ok));
        sm.record_query(&sample(&m, Outcome::Ok));
        let mw = http_metrics("update", 0.02, 0);
        sm.record_query(&sample(&mw, Outcome::Err(ErrorClass::Exec)));

        let snap = sm.snapshot();
        assert_eq!(snap.queries_total, 3);
        assert_eq!(snap.queries_failed, 1);
        assert_eq!(snap.result_rows_total, 10);
        assert_eq!(snap.by_type["read"], 2);
        assert_eq!(snap.by_type["update"], 1);
        assert_eq!(snap.errors_by_class["exec"], 1);
    }

    #[test]
    fn record_error_no_phase() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        sm.record_error(ErrorClass::Capacity);
        let snap = sm.snapshot();
        assert_eq!(snap.queries_total, 1);
        assert_eq!(snap.queries_failed, 1);
        assert_eq!(snap.errors_by_class["capacity"], 1);
        // No latency observed.
        assert_eq!(snap.latency[0].count, 0);
    }

    #[test]
    fn disabled_records_nothing() {
        let cfg = MetricsConfig {
            enabled: false,
            ..MetricsConfig::default()
        };
        let sm = ServerMetrics::new(cfg);
        let m = http_metrics("read", 0.01, 5);
        sm.record_query(&sample(&m, Outcome::Ok));
        sm.record_error(ErrorClass::Exec);
        assert_eq!(sm.snapshot().queries_total, 0);
    }

    #[test]
    fn in_flight_guard_balances() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        assert_eq!(sm.in_flight(), 0);
        {
            let _g1 = sm.in_flight_guard();
            let _g2 = sm.in_flight_guard();
            assert_eq!(sm.in_flight(), 2);
        }
        assert_eq!(sm.in_flight(), 0);
    }

    #[test]
    fn in_flight_guard_drops_on_panic() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = sm.in_flight_guard();
            assert_eq!(sm.in_flight(), 1);
            panic!("boom");
        }));
        assert_eq!(sm.in_flight(), 0);
    }

    #[test]
    fn histogram_percentiles_in_range() {
        let h = LatencyHistogram::new();
        // 99 fast (~5ms) + 1 slow (~3s).
        for _ in 0..99 {
            h.observe(0.005);
        }
        h.observe(3.0);
        let s = h.snapshot();
        assert_eq!(s.count, 100);
        // p50 should land in an early (<=25ms) bucket.
        assert!(
            s.percentile_ms(0.50) <= 25.0,
            "p50={}",
            s.percentile_ms(0.50)
        );
        // p99/p100 should reflect the slow outlier (>1s bucket).
        assert!(s.percentile_ms(0.99) >= 5.0);
    }

    #[test]
    fn bolt_sample_skips_phase_histograms() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        let m = QueryPerformanceMetrics {
            total_time: 0.02,
            execution_time: 0.02,
            query_type: "bolt".to_string(),
            ..QueryPerformanceMetrics::default()
        };
        sm.record_query(&QuerySample {
            metrics: &m,
            outcome: Outcome::Ok,
            has_phase_breakdown: false,
            query_text: None,
            ch: None,
        });
        let snap = sm.snapshot();
        assert_eq!(snap.latency[0].count, 1); // total observed
        assert_eq!(snap.latency[5].count, 1); // exec observed
        assert_eq!(snap.latency[1].count, 0); // parse NOT observed
    }

    #[test]
    fn ring_buffer_evicts_and_sorts() {
        let cfg = MetricsConfig {
            slow_query_capacity: 3,
            ..MetricsConfig::default()
        };
        let sm = ServerMetrics::new(cfg);
        for (i, t) in [0.01, 0.05, 0.02, 0.10, 0.03].iter().enumerate() {
            let m = http_metrics("read", *t, i);
            sm.record_query(&sample(&m, Outcome::Ok));
        }
        // cap=3 → only the last 3 retained.
        let recent = sm.recent_queries(10);
        assert_eq!(recent.len(), 3);
        // recent is newest-first: last pushed was 0.03 (30ms).
        assert!((recent[0].total_ms - 30.0).abs() < 0.01);
        // slowest of the retained {0.02,0.10,0.03} is 0.10 (100ms).
        let slowest = sm.slowest_queries(1);
        assert!((slowest[0].total_ms - 100.0).abs() < 0.01);
    }

    #[test]
    fn prometheus_render_shape() {
        let sm = ServerMetrics::new(MetricsConfig::default());
        let m = http_metrics("read", 0.01, 5);
        sm.record_query(&sample(&m, Outcome::Ok));
        let mut out = String::new();
        sm.render_prometheus(&mut out);
        assert!(out.contains("clickgraph_queries_total 1"));
        assert!(out.contains("clickgraph_queries_by_type_total{type=\"read\"} 1"));
        assert!(
            out.contains("clickgraph_query_duration_seconds_bucket{phase=\"total\",le=\"+Inf\"} 1")
        );
        assert!(out.contains("clickgraph_query_duration_seconds_count{phase=\"total\"} 1"));
        // Every non-comment, non-empty line must be `name value` or
        // `name{labels} value`.
        for line in out.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let value = line.rsplit(' ').next().unwrap();
            assert!(
                value.parse::<f64>().is_ok(),
                "non-numeric metric value in line: {line}"
            );
        }
    }
}
