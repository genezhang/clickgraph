//! Vendor-neutral opt-in query tracing over OTLP/HTTP (#1497 follow-up).
//!
//! One span per translated query, emitted from [`ServerMetrics::record_query`]
//! (the single choke point both HTTP and Bolt paths call). Default off:
//! with no endpoint configured nothing is built, sent, or cloned per query.
//! No query text leaves the process unless explicitly enabled, and then only
//! in literals-stripped form. No new dependencies (reqwest/serde_json/tokio
//! are already non-optional).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

/// OTLP/HTTP send timeout: spans must never slow queries.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// Max in-flight span POSTs; beyond this spans drop (never block, never queue).
pub const MAX_IN_FLIGHT: usize = 64;

/// Strip string literals and numbers from Cypher so a sanitized preview
/// carries shape but no values. `'it''s'`-escaped quotes stay inside the
/// literal; identifiers keep their names.
pub fn sanitize_query(q: &str) -> String {
    use std::sync::OnceLock;
    static STRING_RE: OnceLock<regex::Regex> = OnceLock::new();
    static NUM_RE: OnceLock<regex::Regex> = OnceLock::new();
    let string_re = STRING_RE.get_or_init(|| regex::Regex::new(r"'(?:[^']|'')*'").unwrap());
    // No look-around in this crate: capture the boundary chars and re-emit.
    // `.` is allowed as a left boundary (decimals still match as one unit
    // via the optional fraction, so `1.5` never splits).
    let num_re =
        NUM_RE.get_or_init(|| regex::Regex::new(r"(^|[^\w$])(-?\d+(?:\.\d+)?)([^\w$]|$)").unwrap());
    let no_strings = string_re.replace_all(q, "'?'");
    num_re.replace_all(&no_strings, "${1}?${3}").into_owned()
}

/// OTLP attribute helpers (OTLP/HTTP JSON mapping: int64 as string).
pub fn str_attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

fn dbl_attr(key: &str, value: f64) -> Value {
    json!({"key": key, "value": {"doubleValue": value}})
}

fn int_attr(key: &str, value: u64) -> Value {
    json!({"key": key, "value": {"intValue": value.to_string()}})
}

/// Span identity (not cryptographic). Mixes the wall clock, the pid and a
/// per-process counter into 128 bits: the clock supplies the high bits so an
/// id is never all-zero, and pid+counter fill the low bits so two processes
/// do not emit visibly identical id tails.
pub fn new_ids() -> (String, String) {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let pid = std::process::id() as u128;
    let mixed = nanos
        .wrapping_add(pid.wrapping_shl(96))
        .wrapping_add(n.wrapping_shl(32));
    (format!("{mixed:032x}"), format!("{:016x}", mixed as u64))
}

/// Build the OTLP/HTTP `ExportTraceServiceRequest` for one query sample.
/// Pure function of its inputs (unit-tested); transport never touches it.
#[allow(clippy::too_many_arguments)]
pub fn build_trace_payload(
    metrics: &crate::server::handlers::QueryPerformanceMetrics,
    outcome_ok: bool,
    outcome_label: &str,
    schema: Option<&str>,
    query_text: Option<&str>,
    has_phase_breakdown: bool,
    include_query: bool,
    dialect: &str,
) -> Value {
    let (trace_id, span_id) = new_ids();
    let end_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let total_nanos = (metrics.total_time.max(0.0) * 1e9) as u128;
    let start_nanos = end_nanos.saturating_sub(total_nanos);

    let mut attrs = vec![
        str_attr("db.system", dialect),
        str_attr("cypher.query.type", &metrics.query_type),
        str_attr("cypher.outcome", outcome_label),
        dbl_attr("cypher.phase.total_s", metrics.total_time),
        dbl_attr("cypher.phase.exec_s", metrics.execution_time),
    ];
    if let Some(s) = schema {
        attrs.push(str_attr("cypher.schema", s));
    }
    if let Some(rows) = metrics.result_rows {
        attrs.push(int_attr("cypher.result_rows", rows as u64));
    }
    attrs.push(int_attr(
        "cypher.sql_queries",
        metrics.sql_queries_count as u64,
    ));
    if has_phase_breakdown {
        attrs.push(dbl_attr("cypher.phase.parse_s", metrics.parse_time));
        attrs.push(dbl_attr("cypher.phase.planning_s", metrics.planning_time));
        attrs.push(dbl_attr("cypher.phase.render_s", metrics.render_time));
        attrs.push(dbl_attr(
            "cypher.phase.sql_gen_s",
            metrics.sql_generation_time,
        ));
    }
    if include_query {
        if let Some(q) = query_text {
            attrs.push(str_attr("cypher.query.text", &sanitize_query(q)));
        }
    }

    let (status_code, status_msg) = if outcome_ok {
        (1, None)
    } else {
        (2, Some(outcome_label.to_string()))
    };
    let mut status = serde_json::Map::new();
    status.insert("code".to_string(), Value::from(status_code));
    if let Some(m) = status_msg {
        status.insert("message".to_string(), Value::String(m));
    }

    json!({
        "resourceSpans": [{
            "resource": {"attributes": [str_attr("service.name", "clickgraph")]},
            "scopeSpans": [{
                "scope": {"name": "clickgraph-server"},
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "cypher.query",
                    "kind": 1,
                    "startTimeUnixNano": start_nanos.to_string(),
                    "endTimeUnixNano": end_nanos.to_string(),
                    "attributes": attrs,
                    "status": status,
                }]
            }]
        }]
    })
}

/// Parse OTLP exporter headers from the standard
/// `OTEL_EXPORTER_OTLP_HEADERS` form: comma-separated `key=value` pairs.
/// Blank or malformed entries are skipped rather than failing startup, so a
/// typo degrades to "some headers missing" and shows up as a 401 instead of
/// an unbootable server.
pub fn parse_headers(spec: &str) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let mut map = HeaderMap::new();
    for pair in spec.split(',') {
        let pair = pair.trim();
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(k.trim().as_bytes()),
            HeaderValue::from_str(v.trim()),
        ) else {
            continue;
        };
        map.insert(name, value);
    }
    map
}

/// Fire-and-forget exporter: one shared client, timeout, bounded in-flight
/// (drop-when-full). `export` never blocks and is safe to call from any
/// async query path; outside a runtime (unit tests) it drops.
pub struct OtelExporter {
    client: reqwest::Client,
    url: String,
    include_query: bool,
    dialect: String,
    semaphore: Arc<tokio::sync::Semaphore>,
    timeout: Duration,
    /// Static auth/tenant headers sent with every export.
    headers: reqwest::header::HeaderMap,
    /// Sends the collector accepted (2xx) — observability on the exporter.
    /// `Arc` so the send task can tally without owning the exporter.
    sent: Arc<AtomicU64>,
    /// Sends the collector rejected or that failed outright. A tracing hook
    /// that drops spans silently is worse than no tracing: these make the
    /// loss visible in logs and in `/stats`.
    failed: Arc<AtomicU64>,
}

impl OtelExporter {
    pub fn new(
        endpoint: &str,
        include_query: bool,
        dialect: &str,
        headers: reqwest::header::HeaderMap,
    ) -> Self {
        let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
        let client = reqwest::Client::builder()
            .timeout(SEND_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            url,
            include_query,
            dialect: dialect.to_string(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT)),
            timeout: SEND_TIMEOUT,
            headers,
            sent: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Spans the collector accepted.
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    /// Spans rejected by the collector or failed in flight.
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// Queue one span for send. Returns false when dropped (full) so callers
    /// (and tests) can observe backpressure without I/O. Outside a runtime
    /// (unit tests) it also drops rather than panic.
    pub fn export(&self, payload: Value) -> bool {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                log::debug!("OTLP: {MAX_IN_FLIGHT} exports in flight; span dropped");
                return false;
            }
        };
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => return false,
        };
        let (client, url, headers) = (self.client.clone(), self.url.clone(), self.headers.clone());
        let sent = Arc::clone(&self.sent);
        let failed = Arc::clone(&self.failed);
        handle.spawn(async move {
            let _permit = permit;
            let result = client
                .post(&url)
                .headers(headers)
                .json(&payload)
                .send()
                .await;
            match result {
                Ok(resp) if resp.status().is_success() => {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
                Ok(resp) => {
                    let n = failed.fetch_add(1, Ordering::Relaxed) + 1;
                    log::warn!(
                        "OTLP: collector rejected span ({}); {} failed so far",
                        resp.status(),
                        n
                    );
                }
                Err(e) => {
                    let n = failed.fetch_add(1, Ordering::Relaxed) + 1;
                    log::warn!("OTLP: span send failed ({e}); {n} failed so far");
                }
            }
        });
        true
    }

    /// Build (with this exporter's config) and queue one query span.
    /// Returns false when dropped.
    pub fn export_sample(
        &self,
        metrics: &crate::server::handlers::QueryPerformanceMetrics,
        outcome_ok: bool,
        outcome_label: &str,
        schema: Option<&str>,
        query_text: Option<&str>,
        has_phase_breakdown: bool,
    ) -> bool {
        let payload = build_trace_payload(
            metrics,
            outcome_ok,
            outcome_label,
            schema,
            query_text,
            has_phase_breakdown,
            self.include_query,
            &self.dialect,
        );
        self.export(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn sanitize_keeps_identifiers_and_bounds() {
        assert_eq!(
            sanitize_query("MATCH (n) RETURN n AS total LIMIT 10"),
            "MATCH (n) RETURN n AS total LIMIT ?"
        );
    }

    #[test]
    fn sanitize_strips_literals_keeps_shape() {
        assert_eq!(
            sanitize_query("MATCH (u:User) WHERE u.name = 'Alice' AND u.age > 30 RETURN u"),
            "MATCH (u:User) WHERE u.name = '?' AND u.age > ? RETURN u"
        );
        // ''-escaped quote stays inside the literal.
        assert_eq!(sanitize_query("WHERE n = 'it''s'"), "WHERE n = '?'");
        // Relationship patterns and identifiers survive (bounds sanitize).
        assert_eq!(
            sanitize_query("MATCH (a)-[:FOLLOWS*1..3]->(b) RETURN b"),
            "MATCH (a)-[:FOLLOWS*?..?]->(b) RETURN b"
        );
    }

    #[test]
    fn payload_shape_no_text_by_default() {
        let m = crate::server::handlers::QueryPerformanceMetrics {
            total_time: 0.5,
            parse_time: 0.01,
            planning_time: 0.02,
            render_time: 0.03,
            sql_generation_time: 0.04,
            execution_time: 0.4,
            query_type: "read".to_string(),
            sql_queries_count: 1,
            result_rows: Some(7),
        };
        let p = build_trace_payload(
            &m,
            true,
            "ok",
            Some("social"),
            Some("MATCH (n) RETURN n"),
            true,
            false,
            "clickhouse",
        );
        let spans = &p["resourceSpans"][0]["scopeSpans"][0]["spans"];
        let span = &spans[0];
        assert_eq!(span["name"], "cypher.query");
        let attrs = span["attributes"].as_array().unwrap();
        let get = |k: &str| {
            attrs
                .iter()
                .find(|a| a["key"] == k)
                .unwrap_or_else(|| panic!("attr {k} present"))["value"]
                .clone()
        };
        assert_eq!(get("db.system")["stringValue"], "clickhouse");
        assert_eq!(get("cypher.schema")["stringValue"], "social");
        assert_eq!(get("cypher.query.type")["stringValue"], "read");
        assert_eq!(get("cypher.result_rows")["intValue"], "7");
        assert_eq!(get("cypher.phase.exec_s")["doubleValue"], 0.4);
        assert!(span["traceId"].as_str().unwrap().len() == 32);
        assert!(span["spanId"].as_str().unwrap().len() == 16);
        assert_eq!(span["status"]["code"], 1);
        // No query text unless explicitly enabled.
        assert!(
            attrs.iter().all(|a| a["key"] != "cypher.query.text"),
            "query text off by default"
        );
    }

    #[test]
    fn payload_error_shape_and_sanitized_text() {
        let m = crate::server::handlers::QueryPerformanceMetrics::default();
        // The dialect arrives as an opaque string from the axis and is echoed
        // verbatim: this module never branches on a dialect. `ServerMetrics::new`
        // supplies it from the dialect axis.
        let p = build_trace_payload(
            &m,
            false,
            "exec",
            None,
            Some("MATCH (u) WHERE u.name = 'Bob' RETURN u"),
            false,
            true,
            "some-backend",
        );
        let span = &p["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["status"]["code"], 2);
        let attrs = span["attributes"].as_array().unwrap();
        let text = attrs
            .iter()
            .find(|a| a["key"] == "cypher.query.text")
            .expect("sanitized text present when enabled");
        assert_eq!(
            text["value"]["stringValue"],
            "MATCH (u) WHERE u.name = '?' RETURN u"
        );
        // No phase attributes without a breakdown, but totals always present.
        assert!(attrs.iter().any(|a| a["key"] == "cypher.phase.total_s"));
        assert!(attrs.iter().all(|a| a["key"] != "cypher.phase.parse_s"));
        assert!(
            attrs
                .iter()
                .any(|a| a["key"] == "db.system" && a["value"]["stringValue"] == "some-backend"),
            "db.system echoes the supplied dialect verbatim"
        );
    }

    #[test]
    fn exporter_drops_when_full() {
        let exp = OtelExporter {
            client: reqwest::Client::new(),
            url: "http://127.0.0.1:9/v1/traces".to_string(),
            include_query: false,
            dialect: "clickgraph".to_string(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(0)),
            timeout: SEND_TIMEOUT,
            headers: reqwest::header::HeaderMap::new(),
            sent: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
        };
        assert!(!exp.export(json!({"ping": 1})), "zero permits drops");
        assert_eq!(exp.timeout(), SEND_TIMEOUT);
    }

    #[test]
    fn span_ids_are_wellformed_nonzero_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let (trace, span) = new_ids();
            assert_eq!(trace.len(), 32, "traceId is 16 bytes hex");
            assert_eq!(span.len(), 16, "spanId is 8 bytes hex");
            assert!(
                trace.chars().any(|c| c != '0'),
                "traceId must not be all-zero"
            );
            assert!(seen.insert(trace.clone()), "traceId unique: {trace}");
        }
    }

    #[test]
    fn parse_headers_matches_otel_syntax() {
        // OTEL's OTEL_EXPORTER_OTLP_HEADERS form: comma-separated key=value.
        let h = parse_headers("Authorization=Basic abc123,x-tenant=acme");
        assert_eq!(h.len(), 2);
        assert_eq!(h["authorization"], "Basic abc123");
        assert_eq!(h["x-tenant"], "acme");
        // Blank, malformed entries are skipped, not fatal.
        let h2 = parse_headers(" , a=b ,, noequals , c=d ");
        assert_eq!(h2.len(), 2);
        assert_eq!(h2["a"], "b");
        assert_eq!(h2["c"], "d");
        assert!(parse_headers("").is_empty());
    }

    #[tokio::test]
    async fn headers_reach_the_collector_and_failures_are_counted() {
        // Local one-shot listener: proves the auth header is actually sent and
        // that a rejected send is counted instead of vanishing.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            sock.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            req
        });

        let exp = OtelExporter::new(
            &format!("http://{addr}"),
            false,
            "clickhouse",
            parse_headers("Authorization=Basic secret-token"),
        );
        assert!(exp.export(json!({"ping": 1})), "export accepted");
        let req = server.await.unwrap();
        assert!(
            req.starts_with("POST /v1/traces HTTP/1.1"),
            "OTLP traces path: {req}"
        );
        assert!(
            req.to_lowercase()
                .contains("authorization: basic secret-token"),
            "auth header on the wire: {req}"
        );

        // The 401 must surface as a counted failure, never a silent drop.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(exp.failed(), 1, "rejected send counted");
        assert_eq!(exp.sent(), 0, "rejected send not counted as sent");
    }

    #[tokio::test]
    async fn accepted_send_counts_as_sent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 21\r\n\r\n{\"partialSuccess\":{}}")
                .await
                .unwrap();
        });
        let exp = OtelExporter::new(
            &format!("http://{addr}"),
            false,
            "clickhouse",
            reqwest::header::HeaderMap::new(),
        );
        assert!(exp.export(json!({"ping": 1})));
        server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(exp.sent(), 1);
        assert_eq!(exp.failed(), 0);
    }
}
