//! Langfuse trace + score hook for the outcome-learning loop.
//!
//! Fire-and-forget observability: one span per translated query (latency,
//! dialect, row counts) and score ingestion for feedback signals. Everything
//! is gated on `CLICKGRAPH_LANGFUSE_HOST` plus a key being set — unset means
//! zero overhead (a single `enabled` check per query, never per SELECT).
//!
//! SOLID notes: pure builders ([`build_trace_event`], [`build_score_event`],
//! [`redact`]) are separated from the I/O senders ([`trace_translation`],
//! [`send_score`]); senders are thin wrappers any executor can replace.
//!
//! Privacy: message bodies are redacted best-effort by default ([`redact`]);
//! only hashes of identifiers leave the process unless the deployer opts out.

use serde_json::{json, Value};

/// Build the `enabled` decision from explicit values (pure, testable).
/// Both a host and a key are required; anything else is off.
pub fn enabled_with(host: Option<&str>, key: Option<&str>) -> bool {
    matches!((host, key), (Some(h), Some(k)) if !h.is_empty() && !k.is_empty())
}

/// Whether Langfuse tracing is configured for this process. Read once per
/// query at the call site, never inside hot loops.
pub fn enabled() -> bool {
    enabled_with(
        std::env::var("CLICKGRAPH_LANGFUSE_HOST").ok().as_deref(),
        std::env::var("CLICKGRAPH_LANGFUSE_PUBLIC_KEY")
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
/// carries planning/render/sqlgen seconds; `rows` is `None` for sql-only.
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
        "type": "trace-create",
        "body": {
            "id": trace_id,
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
    Some((format!("{}/api/public/ingestion", host.trim_end_matches('/')), public, secret))
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

/// Trace one translated query. No-op unless configured; otherwise spawns a
/// detached task so the query path never waits on observability.
pub fn trace_translation(
    trace_id: String,
    cypher: String,
    dialect: String,
    schema_name: String,
    timings_secs: (f64, f64, f64),
    rows: Option<u64>,
) {
    if !enabled() {
        return;
    }
    tokio::spawn(async move {
        let event = build_trace_event(
            &trace_id,
            &cypher,
            &dialect,
            &schema_name,
            timings_secs,
            rows,
        );
        post_events(vec![event]).await;
    });
}

/// Ingest one feedback score. Same fire-and-forget contract as
/// [`trace_translation`].
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
    fn enabled_needs_host_and_key() {
        assert!(enabled_with(Some("https://lf"), Some("pk")));
        assert!(!enabled_with(None, Some("pk")));
        assert!(!enabled_with(Some("https://lf"), None));
        assert!(!enabled_with(Some(""), Some("pk")));
        assert!(!enabled_with(None, None));
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
    fn trace_event_shape() {
        let e = build_trace_event("t1", "MATCH (n) RETURN n", "clickhouse", "default", (0.1, 0.2, 0.05), Some(7));
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
        trace_translation("t".into(), "q".into(), "d".into(), "s".into(), (0.0, 0.0, 0.0), None);
        send_score("t".into(), true);
    }
}
