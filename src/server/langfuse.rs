//! Langfuse trace + score hook for the outcome-learning loop.
//!
//! Fire-and-forget observability: one span per translated query (latency,
//! dialect, row counts) and score ingestion for feedback signals. Everything
//! is gated on host + public + secret key being set — unset means zero
//! overhead (a single `enabled` check per query, never per SELECT).
//!
//! Two wire protocols: OTLP/HTTP+JSON (default — works on v4 events-only
//! servers and any OTEL collector) and the legacy Langfuse ingestion API
//! (`CLICKGRAPH_LANGFUSE_PROTOCOL=ingestion`, for pre-v4/dual-write
//! servers). Scores always use the ingestion API, which v4 accepts.
//!
//! SOLID notes: pure builders ([`build_trace_event`], [`build_otlp_span`],
//! [`build_score_event`], [`redact`]) are separated from the I/O senders
//! ([`trace_translation`], [`send_score`]); senders are thin wrappers any
//! executor can replace.
//!
//! Privacy: no user ids are ever attached to traces; message bodies are
//! redacted best-effort by default ([`redact`]). Deployers needing stronger
//! guarantees should hash identifiers before they reach this hook.

use serde_json::{json, Value};

/// Current UTC time as RFC 3339 with millis (`2026-10-15T17:40:00.000Z`).
/// Langfuse ingestion requires an explicit `timestamp` on every event.
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Build the `enabled` decision from explicit values (pure, testable).
/// Host, public key, AND secret key are all required; anything else is off.
/// (The sender would otherwise spawn tasks that can only no-op.)
pub fn enabled_with(host: Option<&str>, public: Option<&str>, secret: Option<&str>) -> bool {
    matches!(
        (host, public, secret),
        (Some(h), Some(p), Some(s))
            if !h.is_empty() && !p.is_empty() && !s.is_empty()
    )
}

/// Whether Langfuse tracing is configured for this process. Read once per
/// query at the call site, never inside hot loops.
pub fn enabled() -> bool {
    enabled_with(
        std::env::var("CLICKGRAPH_LANGFUSE_HOST").ok().as_deref(),
        std::env::var("CLICKGRAPH_LANGFUSE_PUBLIC_KEY")
            .ok()
            .as_deref(),
        std::env::var("CLICKGRAPH_LANGFUSE_SECRET_KEY")
            .ok()
            .as_deref(),
    )
}

/// Best-effort PII scrubber for trace inputs. Redacts email-like tokens and
/// long digit runs (phones, account numbers); keeps everything else intact.
/// Deterministic and dependency-free so unit tests pin its behavior.
pub fn redact(text: &str) -> String {
    const TAIL: usize = 2000;
    let mut out = String::with_capacity(text.len().min(TAIL));
    for token in text.split_whitespace() {
        let scrubbed = if looks_like_email(token) || has_long_digit_run(token, 7) {
            "[redacted]"
        } else {
            token
        };
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(scrubbed);
        if out.len() >= TAIL {
            break;
        }
    }
    out
}

fn looks_like_email(token: &str) -> bool {
    let token = token.trim_matches(|c: char| ".,;:!?()[]\"'".contains(c));
    match token.split_once('@') {
        Some((local, domain)) => !local.is_empty() && domain.contains('.') && !domain.is_empty(),
        None => false,
    }
}

fn has_long_digit_run(token: &str, min: usize) -> bool {
    let mut run = 0;
    for c in token.chars() {
        if c.is_ascii_digit() {
            run += 1;
            if run >= min {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

/// A Langfuse trace-create event for one translated query. `timings_secs`
/// carries planning/render/sqlgen seconds. `rows` is `None`: row counts are
/// not plumbed through translation yet, so both sql-only and execute paths
/// omit it today.
pub fn build_trace_event(
    trace_id: &str,
    cypher: &str,
    dialect: &str,
    schema_name: &str,
    timings_secs: (f64, f64, f64),
    rows: Option<u64>,
) -> Value {
    let mut metadata = json!({
        "dialect": dialect,
        "schema": schema_name,
        "planning_s": timings_secs.0,
        "render_s": timings_secs.1,
        "sqlgen_s": timings_secs.2,
    });
    if let Some(n) = rows {
        metadata["rows"] = json!(n);
    }
    json!({
        "id": trace_id,
        "timestamp": now_iso(),
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "timestamp": now_iso(),
            "name": "clickgraph.query",
            "input": redact(cypher),
            "metadata": metadata,
            "tags": [dialect, schema_name],
        }
    })
}

/// A Langfuse score event (feedback mapper). `value` follows the score-type
/// convention: boolean as 1.0/0.0, numeric in range, categorical as string.
pub fn build_score_event(
    trace_id: &str,
    name: &str,
    value: Value,
    data_type: &str,
    comment: Option<&str>,
) -> Value {
    let mut body = json!({
        "traceId": trace_id,
        "name": name,
        "value": value,
        "dataType": data_type,
    });
    if let Some(c) = comment {
        body["comment"] = json!(c);
    }
    json!({
        "id": format!("{trace_id}-{name}"),
        "timestamp": now_iso(),
        "type": "score-create",
        "body": body,
    })
}

/// Thumbs up/down (or any bool signal) as a Langfuse boolean score event.
pub fn feedback_score_event(trace_id: &str, positive: bool) -> Value {
    build_score_event(
        trace_id,
        "user_feedback",
        json!(if positive { 1.0 } else { 0.0 }),
        "BOOLEAN",
        None,
    )
}

fn ingestion_url() -> Option<(String, String, String)> {
    let host = std::env::var("CLICKGRAPH_LANGFUSE_HOST").ok()?;
    let public = std::env::var("CLICKGRAPH_LANGFUSE_PUBLIC_KEY").ok()?;
    let secret = std::env::var("CLICKGRAPH_LANGFUSE_SECRET_KEY").ok()?;
    if host.is_empty() || public.is_empty() {
        return None;
    }
    Some((
        format!("{}/api/public/ingestion", host.trim_end_matches('/')),
        public,
        secret,
    ))
}

async fn post_events(events: Vec<Value>) {
    let Some((url, public, secret)) = ingestion_url() else {
        return;
    };
    let body = json!({ "batch": events });
    let result = reqwest::Client::new()
        .post(url)
        .basic_auth(public, Some(secret))
        .json(&body)
        .send()
        .await;
    if let Err(e) = result {
        log::debug!("langfuse ingestion failed: {e}");
    }
}

/// Unique trace id for one translated query (process-unique, time-ordered).
pub fn new_trace_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("cg-{}-{ms}-{n}", std::process::id())
}

/// Fixed-length lowercase hex ids for OTLP (`traceId` 32 chars, `spanId`
/// 16). Mixed from time + pid + counter: unique, never all-zero (which OTLP
/// forbids). Randomness quality is irrelevant; only uniqueness matters.
pub fn otel_hex_id(chars: usize, salt: u64) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0x9E3779B97F4A7C15);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x12345678);
    let n = C
        .fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed)
        .wrapping_add(salt);
    let mut x = n ^ nanos ^ ((std::process::id() as u64) << 32);
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    let mut s = format!("{x:016x}");
    while s.len() < chars {
        x = x
            .wrapping_mul(0x2545F4914F6CDD1D)
            .wrapping_add(0x9E3779B97F4A7C15);
        x ^= x >> 29;
        s += &format!("{x:016x}");
    }
    s.truncate(chars);
    if s.bytes().all(|b| b == b'0') {
        s.replace_range(chars - 1.., "1");
    }
    s
}

/// Which wire protocol the trace sender uses. `CLICKGRAPH_LANGFUSE_PROTOCOL`
/// is `otlp` by default (works on v4 events-only servers and any OTEL
/// collector); `ingestion` selects the legacy Langfuse ingestion API (needs
/// a dual-write or pre-v4 server for traces).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceProtocol {
    Otlp,
    Ingestion,
}

/// Parse the protocol switch (pure, testable). Unknown values fall back to
/// OTLP rather than failing closed on observability.
pub fn trace_protocol(var: Option<&str>) -> TraceProtocol {
    match var.map(str::to_ascii_lowercase).as_deref() {
        Some("ingestion") => TraceProtocol::Ingestion,
        _ => TraceProtocol::Otlp,
    }
}

fn otlp_attr_str(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

fn otlp_attr_num(key: &str, value: f64) -> Value {
    json!({"key": key, "value": {"doubleValue": value}})
}

fn otlp_attr_int(key: &str, value: u64) -> Value {
    // OTLP/JSON encodes 64-bit ints as strings.
    json!({"key": key, "value": {"intValue": value.to_string()}})
}

/// Inputs for one OTLP span. Bundled so [`build_otlp_span`] stays within
/// the argument-count lint (CI denies warnings).
pub struct SpanInput<'a> {
    pub trace_id_hex: &'a str,
    pub span_id_hex: &'a str,
    pub cypher: &'a str,
    pub dialect: &'a str,
    pub schema_name: &'a str,
    pub timings_secs: (f64, f64, f64),
    pub rows: Option<u64>,
    pub end_nanos: u64,
}

/// An OTLP/HTTP+JSON span for one translated query (no new dependencies:
/// plain JSON over the existing HTTP client). `end_nanos` backdates the
/// start so the span duration equals the measured translation time.
pub fn build_otlp_span(input: SpanInput<'_>) -> Value {
    let total = (input.timings_secs.0 + input.timings_secs.1 + input.timings_secs.2).max(0.0);
    let start = input.end_nanos.saturating_sub((total * 1e9) as u64);
    let mut attrs = vec![
        otlp_attr_str("cypher.input", &redact(input.cypher)),
        otlp_attr_str("dialect", input.dialect),
        otlp_attr_str("schema.name", input.schema_name),
        otlp_attr_num("timings.planning_s", input.timings_secs.0),
        otlp_attr_num("timings.render_s", input.timings_secs.1),
        otlp_attr_num("timings.sqlgen_s", input.timings_secs.2),
    ];
    if let Some(n) = input.rows {
        attrs.push(otlp_attr_int("rows", n));
    }
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "clickgraph"}}]},
            "scopeSpans": [{
                "scope": {"name": "clickgraph"},
                "spans": [{
                    "traceId": input.trace_id_hex,
                    "spanId": input.span_id_hex,
                    "name": "clickgraph.query",
                    "kind": 2,
                    "startTimeUnixNano": start.to_string(),
                    "endTimeUnixNano": input.end_nanos.to_string(),
                    "attributes": attrs,
                    "status": {"code": 1},
                }],
            }],
        }],
    })
}

async fn post_otlp(span: Value) {
    let host = match std::env::var("CLICKGRAPH_LANGFUSE_HOST") {
        Ok(h) if !h.is_empty() => h,
        _ => return,
    };
    let (public, secret) = match (
        std::env::var("CLICKGRAPH_LANGFUSE_PUBLIC_KEY"),
        std::env::var("CLICKGRAPH_LANGFUSE_SECRET_KEY"),
    ) {
        (Ok(p), Ok(s)) if !p.is_empty() => (p, s),
        _ => return,
    };
    let url = format!("{}/api/public/otel/v1/traces", host.trim_end_matches('/'));
    let result = reqwest::Client::new()
        .post(url)
        .basic_auth(public, Some(secret))
        .json(&span)
        .send()
        .await;
    if let Err(e) = result {
        log::debug!("langfuse otlp send failed: {e}");
    }
}

/// Trace one translated query. No-op unless configured; otherwise spawns a
/// detached task so the query path never waits on observability. Sends via
/// OTLP by default ([`TraceProtocol::Otlp`]); the legacy ingestion API is
/// available through `CLICKGRAPH_LANGFUSE_PROTOCOL=ingestion`.
///
/// Returns the trace id the span was filed under ([`otel_hex_id`] for OTLP,
/// the caller's id for ingestion) so later feedback can correlate through
/// [`send_score`]; `None` when disabled.
pub fn trace_translation(
    trace_id: String,
    cypher: String,
    dialect: String,
    schema_name: String,
    timings_secs: (f64, f64, f64),
    rows: Option<u64>,
) -> Option<String> {
    if !enabled() {
        return None;
    }
    let protocol = trace_protocol(
        std::env::var("CLICKGRAPH_LANGFUSE_PROTOCOL")
            .ok()
            .as_deref(),
    );
    match protocol {
        TraceProtocol::Otlp => {
            let tid = otel_hex_id(32, 0x51ab);
            let sid = otel_hex_id(16, 0x9c3d);
            let end = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let span = build_otlp_span(SpanInput {
                trace_id_hex: &tid,
                span_id_hex: &sid,
                cypher: &cypher,
                dialect: &dialect,
                schema_name: &schema_name,
                timings_secs,
                rows,
                end_nanos: end,
            });
            tokio::spawn(async move {
                post_otlp(span).await;
            });
            Some(tid)
        }
        TraceProtocol::Ingestion => {
            let event = build_trace_event(
                &trace_id,
                &cypher,
                &dialect,
                &schema_name,
                timings_secs,
                rows,
            );
            tokio::spawn(async move {
                post_events(vec![event]).await;
            });
            Some(trace_id)
        }
    }
}

/// Ingest one feedback score. Same fire-and-forget contract as
/// [`trace_translation`]. No in-server call site exists yet: feedback
/// arrives through downstream consumers, which call this (or POST scores
/// directly) with the originating `trace_id`.
pub fn send_score(trace_id: String, positive: bool) {
    if !enabled() {
        return;
    }
    tokio::spawn(async move {
        post_events(vec![feedback_score_event(&trace_id, positive)]).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_needs_host_and_keys() {
        assert!(enabled_with(Some("https://lf"), Some("pk"), Some("sk")));
        assert!(!enabled_with(None, Some("pk"), Some("sk")));
        assert!(!enabled_with(Some("https://lf"), None, Some("sk")));
        assert!(!enabled_with(Some("https://lf"), Some("pk"), None));
        assert!(!enabled_with(Some(""), Some("pk"), Some("sk")));
        assert!(!enabled_with(None, None, None));
    }

    #[test]
    fn redact_emails_and_digit_runs() {
        let out = redact("contact alice@example.com or call 5551234567 about MATCH (u:User)");
        assert!(out.contains("[redacted]"), "{out}");
        assert!(!out.contains("alice@example.com"), "{out}");
        assert!(!out.contains("5551234567"), "{out}");
        assert!(out.contains("MATCH (u:User)"), "{out}");
    }

    #[test]
    fn redact_leaves_plain_text() {
        let q = "MATCH (u:User) WHERE u.age = 30 RETURN u.name LIMIT 5";
        assert_eq!(redact(q), q);
    }

    #[test]
    fn redact_truncates_long_inputs() {
        let q = "x ".repeat(5000);
        assert!(redact(&q).len() <= 2100);
    }

    #[test]
    fn now_iso_is_rfc3339_utc() {
        let t = now_iso();
        assert!(t.ends_with('Z'), "{t}");
        assert!(t.contains('T'), "{t}");
    }

    #[test]
    fn otel_ids_are_shaped_hex() {
        for _ in 0..100 {
            let t = otel_hex_id(32, 1);
            let s = otel_hex_id(16, 2);
            assert_eq!(t.len(), 32);
            assert_eq!(s.len(), 16);
            assert!(t.bytes().all(|b| b.is_ascii_hexdigit()));
            assert!(s.bytes().all(|b| b.is_ascii_hexdigit()));
            assert!(t.bytes().any(|b| b != b'0'));
        }
        let (a, b) = (otel_hex_id(32, 3), otel_hex_id(32, 3));
        assert_ne!(a, b, "counter must advance");
    }

    #[test]
    fn trace_protocol_parsing() {
        assert_eq!(trace_protocol(None), TraceProtocol::Otlp);
        assert_eq!(trace_protocol(Some("otlp")), TraceProtocol::Otlp);
        assert_eq!(trace_protocol(Some("ingestion")), TraceProtocol::Ingestion);
        assert_eq!(trace_protocol(Some("weird")), TraceProtocol::Otlp);
    }

    #[test]
    fn otlp_span_shape() {
        let trace = otel_hex_id(32, 4);
        let span = otel_hex_id(16, 5);
        let e = build_otlp_span(SpanInput {
            trace_id_hex: &trace,
            span_id_hex: &span,
            cypher: "MATCH (u:User) WHERE u.email = 'a@b.co' RETURN u",
            dialect: "clickhouse",
            schema_name: "default",
            timings_secs: (0.1, 0.2, 0.05),
            rows: Some(7),
            end_nanos: 1_790_000_000_000_000_000,
        });
        let spans = &e["resourceSpans"];
        let s = &spans[0]["scopeSpans"][0]["spans"][0];
        assert_eq!(s["traceId"], trace.as_str());
        assert_eq!(s["spanId"], span.as_str());
        assert_eq!(s["name"], "clickgraph.query");
        let attrs = s["attributes"].as_array().unwrap();
        let input = attrs.iter().find(|a| a["key"] == "cypher.input").unwrap()["value"]
            ["stringValue"]
            .as_str()
            .unwrap();
        assert!(!input.contains("a@b.co"), "PII redacted: {input}");
        assert!(input.contains("MATCH"), "{input}");
    }

    #[test]
    fn events_carry_timestamps() {
        let e = build_trace_event("t", "RETURN 1", "clickhouse", "d", (0.0, 0.0, 0.0), None);
        assert!(e.get("timestamp").and_then(|v| v.as_str()).is_some());
        let s = feedback_score_event("t", true);
        assert!(s.get("timestamp").and_then(|v| v.as_str()).is_some());
    }
    #[test]
    fn trace_event_shape() {
        let e = build_trace_event(
            "t1",
            "MATCH (n) RETURN n",
            "clickhouse",
            "default",
            (0.1, 0.2, 0.05),
            Some(7),
        );
        assert_eq!(e["type"], "trace-create");
        assert_eq!(e["body"]["name"], "clickgraph.query");
        assert_eq!(e["body"]["metadata"]["rows"], 7);
        assert!(e["body"]["input"].as_str().unwrap().contains("MATCH"));
    }

    #[test]
    fn score_event_shape() {
        let e = feedback_score_event("t1", true);
        assert_eq!(e["type"], "score-create");
        assert_eq!(e["body"]["name"], "user_feedback");
        assert_eq!(e["body"]["value"], 1.0);
        assert_eq!(e["body"]["dataType"], "BOOLEAN");
        let e = feedback_score_event("t1", false);
        assert_eq!(e["body"]["value"], 0.0);
    }

    #[test]
    fn disabled_senders_are_noops_without_runtime() {
        // `enabled()` is false in test env (no vars set by any test here);
        // both senders must return without touching a runtime.
        if enabled() {
            return;
        }
        trace_translation(
            "t".into(),
            "q".into(),
            "d".into(),
            "s".into(),
            (0.0, 0.0, 0.0),
            None,
        );
        send_score("t".into(), true);
    }
}
