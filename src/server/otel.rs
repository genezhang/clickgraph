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

/// Longest sanitized query text kept in a span; longer queries are truncated
/// with a marker. Bounded so a span attribute can never carry a whole script.
pub const MAX_QUERY_TEXT: usize = 512;

/// Appended to a sanitized query that hit [`MAX_QUERY_TEXT`].
pub const TRUNCATION_MARKER: &str = "...";

/// Redact every value from a Cypher query, keeping its shape.
///
/// Two stages, because each already has to be right for a different reason:
/// 1. [`crate::open_cypher_parser::strip_comments`] removes `//`, `--` and
///    `/* */` comment bodies. Reused rather than reimplemented because it
///    already encodes the trap that `-->`/`--(`/`--[` are relationship
///    patterns, not comments, and it tracks quotes so a `--` inside a string
///    survives. Comment text is exactly where credentials get left behind
///    (`// password hunter2`), so it must never reach a collector.
/// 2. A single character pass then walks the remainder, masking quoted runs
///    (`'`, `"`, backtick) and numeric literals. Character-level because a
///    regex cannot see state: it cannot tell a quote from an escaped quote,
///    and each match consumes a boundary character, so `[10,20,30,40]`
///    redacted every *other* element.
///
/// Double-quoted and backticked runs are masked too. In Cypher those are
/// quoted identifiers rather than string literals, so this can hide a label or
/// property name; that is the deliberate trade for a redaction function, which
/// must fail closed.
pub fn sanitize_query(q: &str) -> String {
    let no_comments = crate::open_cypher_parser::strip_comments(q);
    let mut redacted = redact_literals(&no_comments);
    if redacted.chars().count() > MAX_QUERY_TEXT {
        let keep: String = redacted.chars().take(MAX_QUERY_TEXT).collect();
        redacted = keep + TRUNCATION_MARKER;
    }
    redacted
}

/// Mask quoted runs and numeric literals in already comment-free Cypher.
fn redact_literals(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' || c == '"' || c == '`' {
            // Copy the quote, mask the body, close the quote. The close is
            // emitted either way so an unterminated literal cannot run to the
            // end of the span unbalanced.
            out.push(c);
            out.push('?');
            out.push(c);
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' {
                    i += 2; // skip the escaped character
                    continue;
                }
                if chars[i] == c {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if let Some(len) = number_len(&chars, i) {
            out.push('?');
            i += len;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Length of the numeric literal starting at `i`, or `None` if none starts
/// there. A sign is only a sign when a digit follows and the character before
/// it is not an identifier character, so `x-1` keeps its operator and
/// `age > -5` loses its value.
fn number_len(chars: &[char], i: usize) -> Option<usize> {
    let mut j = i;
    // A digit glued to an identifier character is part of a name, not a value
    // (`t1`, `Table2`, `col3` must survive intact).
    if chars[j].is_ascii_digit() && i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_')
    {
        return None;
    }
    if matches!(chars[j], '-' | '+') {
        let prev_is_ident = j > 0 && (chars[j - 1].is_alphanumeric() || chars[j - 1] == '_');
        if !chars.get(j + 1).is_some_and(char::is_ascii_digit) || prev_is_ident {
            return None;
        }
        j += 1;
    } else if !chars[j].is_ascii_digit() {
        return None;
    }
    // Hex / binary / octal literals.
    if chars[j] == '0'
        && matches!(
            chars.get(j + 1),
            Some('x') | Some('X') | Some('b') | Some('B') | Some('o') | Some('O')
        )
    {
        let mut k = j + 2;
        while chars
            .get(k)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
        {
            k += 1;
        }
        return Some(k - i);
    }
    while chars.get(j).is_some_and(char::is_ascii_digit) {
        j += 1;
    }
    // Fraction, but only when a digit follows, so `1..3` (a VLP range) keeps
    // its dots and only the bounds are masked.
    if chars.get(j) == Some(&'.') && chars.get(j + 1).is_some_and(char::is_ascii_digit) {
        j += 1;
        while chars.get(j).is_some_and(char::is_ascii_digit) {
            j += 1;
        }
    }
    // Exponent, including its sign: `1e10`, `2.5E-3`.
    if matches!(chars.get(j), Some('e') | Some('E')) {
        let mut k = j + 1;
        if matches!(chars.get(k), Some('+') | Some('-')) {
            k += 1;
        }
        if chars.get(k).is_some_and(char::is_ascii_digit) {
            while chars.get(k).is_some_and(char::is_ascii_digit) {
                k += 1;
            }
            j = k;
        }
    }
    Some(j - i)
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

/// Span identity: a random v4 UUID per id, per W3C trace-context (both must be
/// lowercase hex, the right byte width, and never all-zero). Random rather than
/// derived from the clock so ids from two processes cannot collide, and so a
/// span id is independent of its trace id.
pub fn new_ids() -> (String, String) {
    let simple = |b: uuid::Uuid| b.simple().to_string();
    // A span id is 8 bytes, so take the low 64 bits of a fresh uuid v4 rather
    // than emitting a second 16-byte id.
    let span = format!("{:016x}", uuid::Uuid::new_v4().as_u128() as u64);
    (simple(uuid::Uuid::new_v4()), span)
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

    // OTLP status: 0 = Unset, 1 = Ok, 2 = Error. Success stays Unset rather than
    // explicitly Ok, so a collector can still tell "no error reported" from
    // "an OK was asserted".
    let mut status = serde_json::Map::new();
    if !outcome_ok {
        status.insert("code".to_string(), Value::from(2));
        status.insert(
            "message".to_string(),
            Value::String(outcome_label.to_string()),
        );
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
                    "kind": 2,
                    "startTimeUnixNano": start_nanos.to_string(),
                    "endTimeUnixNano": end_nanos.to_string(),
                    "attributes": attrs,
                    "status": status,
                }]
            }]
        }]
    })
}

/// Header names whose values are credentials. Their values are never logged or
/// echoed in diagnostics; only the name is.
const SENSITIVE_HEADERS: [&str; 3] = ["authorization", "x-api-key", "api-key"];

/// Percent-decode one header value, per OTEL's `OTEL_EXPORTER_OTLP_HEADERS`,
/// which percent-encodes values (Langfuse's docs use `Basic%20…`). Invalid
/// escapes are left verbatim rather than dropped, so a value that is not
/// encoded still arrives intact.
fn percent_decode(value: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        (b as char).to_digit(16).map(|d| d as u8)
    }
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

/// Parse OTLP exporter headers from the standard
/// `OTEL_EXPORTER_OTLP_HEADERS` form: comma-separated `key=value` pairs.
/// Values are percent-decoded. Blank or malformed entries are skipped with a
/// warning naming only the header (never its value, which is often a
/// credential) so a typo surfaces as a 401 at the collector rather than as an
/// unbootable server.
pub fn parse_headers(spec: &str) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let mut map = HeaderMap::new();
    for pair in spec.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let Some((k, v)) = pair.split_once('=') else {
            log::warn!("OTLP: ignoring header entry without '=': {pair:?}");
            continue;
        };
        let k = k.trim();
        let decoded = percent_decode(v.trim());
        match (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(&decoded),
        ) {
            (Ok(name), Ok(value)) => {
                map.insert(name, value);
            }
            _ => {
                let kind = if SENSITIVE_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
                    " (value withheld: sensitive)"
                } else {
                    ""
                };
                log::warn!("OTLP: ignoring malformed header entry {k:?}{kind}");
            }
        }
    }
    map
}

/// Fire-and-forget exporter: one shared client, timeout, bounded in-flight
/// Export counters, reported on `/stats` when tracing is enabled.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct OtelStats {
    pub sent: u64,
    pub failed: u64,
    pub dropped: u64,
}

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
    /// Spans dropped because the in-flight bound was full. Never logged above
    /// debug: a saturated collector is a condition to observe, not to be told
    /// about once per query.
    dropped: Arc<AtomicU64>,
    /// Failure count at which the next warning is due. Doubles after each
    /// warning (1, 2, 4, 8, …) so a long outage reports without flooding.
    next_warn_at: Arc<AtomicU64>,
}

impl OtelExporter {
    /// Build an exporter, or `None` when the HTTP client cannot be created. A
    /// failed build would otherwise fall back to a client with no timeout,
    /// which can hang a send task indefinitely.
    pub fn try_new(
        endpoint: &str,
        include_query: bool,
        dialect: &str,
        headers: reqwest::header::HeaderMap,
    ) -> Option<Self> {
        let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
        let client = match reqwest::Client::builder().timeout(SEND_TIMEOUT).build() {
            Ok(c) => c,
            Err(e) => {
                log::error!("OTLP: cannot build HTTP client ({e}); query tracing disabled");
                return None;
            }
        };
        log::info!(
            "OTLP: query tracing enabled -> {url} (query text {}, auth headers {})",
            if include_query {
                "included"
            } else {
                "excluded"
            },
            headers.len()
        );
        Some(Self {
            client,
            url,
            include_query,
            dialect: dialect.to_string(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT)),
            timeout: SEND_TIMEOUT,
            headers,
            sent: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
            next_warn_at: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Export counters, surfaced on `/stats` so tracing health is observable
    /// without waiting for a log line.
    pub fn stats(&self) -> OtelStats {
        OtelStats {
            sent: self.sent(),
            failed: self.failed(),
            dropped: self.dropped(),
        }
    }

    /// Send timeout applied to every export.
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

    /// Spans dropped because the in-flight bound was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Queue one span for send. Returns false when dropped (full) so callers
    /// (and tests) can observe backpressure without I/O. Outside a runtime
    /// (unit tests) it also drops rather than panic.
    pub fn export(&self, payload: Value) -> bool {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                log::debug!("OTLP: {MAX_IN_FLIGHT} exports in flight; span dropped ({n} total)");
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
        let next_warn_at = Arc::clone(&self.next_warn_at);
        handle.spawn(async move {
            let _permit = permit;
            let result = client
                .post(&url)
                .headers(headers)
                .json(&payload)
                .send()
                .await;
            let problem = match result {
                Ok(resp) if resp.status().is_success() => {
                    sent.fetch_add(1, Ordering::Relaxed);
                    None
                }
                Ok(resp) => Some(format!("collector rejected span ({})", resp.status())),
                Err(e) => Some(format!("span send failed ({e})")),
            };
            if let Some(problem) = problem {
                let n = failed.fetch_add(1, Ordering::Relaxed) + 1;
                // A dead collector fails every query, so warning per failure
                // would drown the log. Warn at exponentially growing totals
                // (1, 2, 4, 8, …); the running count in the message means the
                // rate never hides that failures happened.
                let due = next_warn_at.load(Ordering::Relaxed);
                if n >= due {
                    log::warn!("OTLP: {problem}; {n} failed so far");
                    next_warn_at.store(due.saturating_mul(2).max(due + 1), Ordering::Relaxed);
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
    fn sanitize_masks_single_quoted_literals() {
        assert_eq!(
            sanitize_query("MATCH (u:User) WHERE u.name = 'Alice' AND u.age > 30 RETURN u"),
            "MATCH (u:User) WHERE u.name = '?' AND u.age > ? RETURN u"
        );
        assert_eq!(
            sanitize_query("MATCH (n) RETURN n AS total LIMIT 10"),
            "MATCH (n) RETURN n AS total LIMIT ?"
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
        // Success leaves the status Unset rather than asserting Ok.
        assert!(
            span["status"].get("code").is_none(),
            "no status on success: {}",
            span["status"]
        );
        assert_eq!(span["kind"], 2, "SPAN_KIND_SERVER");
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
    fn exporter_drops_and_counts_when_full() {
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
            dropped: Arc::new(AtomicU64::new(0)),
            next_warn_at: Arc::new(AtomicU64::new(1)),
        };
        assert!(!exp.export(json!({"ping": 1})), "zero permits drops");
        // A saturated collector must be visible in /stats, not only at debug.
        assert_eq!(exp.dropped(), 1);
        assert_eq!(exp.stats().dropped, 1);
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
    fn sanitize_redacts_every_reported_leak() {
        // Each case is a leak reported in review; the assertion is that the
        // sensitive text is absent, not that the shape matches exactly.
        let cases: &[(&str, &str)] = &[
            // Double-quoted literals were not handled at all.
            (r#"MATCH (u) WHERE u.name = "Alice" RETURN u"#, "Alice"),
            // Backslash escapes desynchronised the regex, leaking the tail.
            (
                r#"MATCH (u) WHERE u.name = 'x\'y' OR u.pw = 'secret'"#,
                "secret",
            ),
            // Comment bodies must never ship.
            (
                "MATCH (u) WHERE u.pw = 'x' // password hunter2\nRETURN u",
                "hunter2",
            ),
            ("MATCH (u) /* token abc123 */ RETURN u", "abc123"),
            // Each number match consumed the following separator, so every
            // other element survived.
            ("MATCH (u) WHERE u.id IN [10,20,30,40] RETURN u", "10"),
            ("MATCH (u) WHERE u.id IN [10,20,30,40] RETURN u", "20"),
            ("MATCH (u) WHERE u.id IN [10,20,30,40] RETURN u", "30"),
            ("CALL range(1,10)", "10"),
            // Forms the number regex never matched.
            ("MATCH (u) WHERE u.score > 1e10 RETURN u", "1e10"),
            ("MATCH (u) WHERE u.mask = 0x1F RETURN u", "0x1F"),
            // Unterminated literal must still be fully masked.
            ("MATCH (u) WHERE u.name = 'Alice", "Alice"),
        ];
        for (q, secret) in cases {
            let got = sanitize_query(q);
            assert!(
                !got.contains(secret),
                "leaked {secret:?} from {q:?}\n  got: {got}"
            );
        }
        // Shape survives: the operator and both operands are still readable.
        let got = sanitize_query("MATCH (u) WHERE u.age > 30 RETURN u");
        assert_eq!(got, "MATCH (u) WHERE u.age > ? RETURN u");
        // A whole list redacts element-wise, separators intact.
        assert_eq!(
            sanitize_query("WHERE u.id IN [10,20,30,40]"),
            "WHERE u.id IN [?,?,?,?]"
        );
    }

    #[test]
    fn sanitize_preserves_structure_and_bounds() {
        // VLP range: dots kept, only the bounds masked.
        assert_eq!(
            sanitize_query("MATCH (a)-[:FOLLOWS*1..3]->(b) RETURN b"),
            "MATCH (a)-[:FOLLOWS*?..?]->(b) RETURN b"
        );
        // Untyped relationship arrows are structure, not comments.
        assert_eq!(
            sanitize_query("MATCH (a)--(b) RETURN b"),
            "MATCH (a)--(b) RETURN b"
        );
        assert_eq!(
            sanitize_query("MATCH (a)-->(b) RETURN b"),
            "MATCH (a)-->(b) RETURN b"
        );
        // Identifiers that merely contain digits keep their names.
        assert_eq!(
            sanitize_query("MATCH (t1:Table2) WHERE t1.col3 IS NOT NULL RETURN t1"),
            "MATCH (t1:Table2) WHERE t1.col3 IS NOT NULL RETURN t1"
        );
        // Parameters carry no value and must stay readable.
        assert_eq!(
            sanitize_query("MATCH (u) WHERE u.id = $userId RETURN u"),
            "MATCH (u) WHERE u.id = $userId RETURN u"
        );
        // A `--` inside a string is not a comment.
        assert_eq!(
            sanitize_query("MATCH (u) WHERE u.n = 'a--b' RETURN u"),
            "MATCH (u) WHERE u.n = '?' RETURN u"
        );
    }

    #[test]
    fn sanitize_output_is_length_capped() {
        let long = format!(
            "MATCH (u) WHERE u.id IN [{}] RETURN u",
            (0..400)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        let got = sanitize_query(&long);
        assert!(
            got.len() <= MAX_QUERY_TEXT + TRUNCATION_MARKER.len(),
            "capped: {} bytes",
            got.len()
        );
        assert!(got.ends_with(TRUNCATION_MARKER), "marked as truncated");
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

    #[test]
    fn parse_headers_percent_decodes_values() {
        // Langfuse's docs use `Basic%20…`; sent verbatim this 401s.
        let h = parse_headers("Authorization=Basic%20cGtuOjEyMw==");
        assert_eq!(h["authorization"], "Basic cGtuOjEyMw==");
        // Multi-byte sequences decode too.
        assert_eq!(parse_headers("x-note=a%20b%3Dc")["x-note"], "a b=c");
        // Not encoded, and not valid escapes, so they survive untouched.
        assert_eq!(parse_headers("x-note=plain value")["x-note"], "plain value");
        assert_eq!(parse_headers("x-note=100%")["x-note"], "100%");
        assert_eq!(parse_headers("x-note=%zz")["x-note"], "%zz");
    }

    #[test]
    fn uuid_backed_ids_are_random_and_wellformed() {
        let mut traces = std::collections::HashSet::new();
        let mut spans = std::collections::HashSet::new();
        for _ in 0..64 {
            let (trace, span) = new_ids();
            assert_eq!(trace.len(), 32, "traceId is 16 bytes hex");
            assert_eq!(span.len(), 16, "spanId is 8 bytes hex");
            assert!(trace.chars().all(|c| c.is_ascii_hexdigit()));
            assert!(span.chars().all(|c| c.is_ascii_hexdigit()));
            // Distinct span ids, and the span id is not just the trace id's
            // low half (a previous version derived it that way).
            assert!(spans.insert(span.clone()), "spanId unique: {span}");
            assert!(traces.insert(trace.clone()), "traceId unique: {trace}");
            assert_ne!(span, trace[16..], "span id is not the trace id low half");
        }
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

        let exp = OtelExporter::try_new(
            &format!("http://{addr}"),
            false,
            "clickhouse",
            parse_headers("Authorization=Basic secret-token"),
        )
        .expect("exporter builds");
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
    async fn failed_sends_are_rate_limited_but_all_counted() {
        // A collector that always refuses. The log must not carry one line
        // per failure, while the counter still reflects every one.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let _ = sock
                        .write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
                        .await;
                });
            }
        });
        let exp = OtelExporter::try_new(
            &format!("http://{addr}"),
            false,
            "clickhouse",
            reqwest::header::HeaderMap::new(),
        )
        .expect("exporter builds");

        const ATTEMPTS: u64 = 24;
        for _ in 0..ATTEMPTS {
            assert!(exp.export(json!({"ping": 1})));
            // Let each send reach its verdict before the next, so the totals
            // are deterministic.
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        server.abort();
        assert_eq!(exp.failed(), ATTEMPTS, "every failure is counted");
        // Thresholds 1,2,4,8,16 -> five warnings for 24 failures.
        assert_eq!(
            exp.next_warn_at.load(Ordering::Relaxed),
            32,
            "warning threshold doubles after each warning"
        );
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
        let exp = OtelExporter::try_new(
            &format!("http://{addr}"),
            false,
            "clickhouse",
            reqwest::header::HeaderMap::new(),
        )
        .expect("exporter builds");
        assert!(exp.export(json!({"ping": 1})));
        server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(exp.sent(), 1);
        assert_eq!(exp.failed(), 0);
    }
}
