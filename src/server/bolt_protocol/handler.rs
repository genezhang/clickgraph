//! Bolt Protocol Message Handler
//!
//! This module processes incoming Bolt messages and generates appropriate responses.
//! It handles the complete Bolt protocol state machine and integrates with
//! Brahmand's query processing pipeline.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::auth::{AuthToken, AuthenticatedUser, Authenticator};
use super::errors::{BoltError, BoltResult};
use super::messages::{signatures, BoltMessage, BoltValue};
use super::result_transformer::extract_return_metadata;
use super::{BoltConfig, BoltContext, ConnectionState};

use crate::clickhouse_query_generator;
use crate::executor::QueryExecutor;
use crate::open_cypher_parser;
use crate::query_planner;
use crate::server::handlers::QueryPerformanceMetrics;
use crate::server::metrics::{self, ErrorClass, Outcome, QuerySample};
use crate::server::GLOBAL_SERVER_METRICS;

/// Execution plan for procedure-only queries (extracted before async execution)
#[derive(Debug)]
enum ExecutionPlan {
    SimpleProcedure {
        proc_name: String,
    },
    ProcedureWithReturn {
        proc_name: String,
        // Store index to look up in parsed statement after async
    },
    Union {
        branches: Vec<ProcedureBranch>,
    },
}

/// Branch information for UNION execution
#[derive(Debug)]
struct ProcedureBranch {
    proc_name: String,
    has_return: bool,
}

use crate::render_plan::plan_builder::RenderPlanBuilder;
use crate::server::{graph_catalog, parameter_substitution};

/// Helper macro for safe mutex locking with proper error handling
macro_rules! lock_context {
    ($mutex:expr) => {
        $mutex.lock().map_err(|e| {
            log::error!("Mutex poisoning detected in Bolt handler: {}", e);
            BoltError::mutex_poisoned(format!("Connection state synchronization failed: {}", e))
        })?
    };
}

/// Helper function to format BoltValue for logging
fn bolt_value_to_string(value: &BoltValue) -> String {
    match value {
        BoltValue::Json(v) => serde_json::to_string(v).unwrap_or_else(|_| format!("{:?}", v)),
        BoltValue::PackstreamBytes(bytes) => format!("<packstream: {} bytes>", bytes.len()),
    }
}

/// Detect Browser 5.x's bundled count query:
///   `MATCH (n) RETURN count(n) AS result UNION ALL MATCH ()-[r]->() RETURN count(r) AS result`
///
/// Older Browser versions issue these as two separate queries which flow through
/// the normal pipeline correctly. The bundled UNION ALL form crashes our SQL
/// generator, so it gets intercepted in `handle_run`. Tolerant of whitespace and
/// trailing semicolons; case-insensitive.
/// Build the two RECORD fields (`nodes`, `relationships`) returned by
/// `CALL db.schema.visualization()`: a virtual graph of the schema with one
/// node per label and one relationship per (type, from-label, to-label). The
/// node/relationship structures are packstream-encoded so Neo4j Browser renders
/// them as a real graph and derives default per-label styling from them.
fn build_schema_visualization_fields(
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Vec<BoltValue> {
    use crate::server::bolt_protocol::graph_objects::{encode_packstream_list, Node, Relationship};
    use std::collections::HashMap as StdHashMap;

    // One virtual node per (deduped) base label.
    let mut labels: Vec<String> = schema
        .all_node_schemas()
        .keys()
        .map(|k| k.rsplit("::").next().unwrap_or(k).to_string())
        .collect();
    labels.sort();
    labels.dedup();

    let mut label_id: StdHashMap<String, i64> = StdHashMap::new();
    let mut node_bytes: Vec<Vec<u8>> = Vec::with_capacity(labels.len());
    for (i, label) in labels.iter().enumerate() {
        let id = i as i64;
        label_id.insert(label.clone(), id);
        let mut props: StdHashMap<String, serde_json::Value> = StdHashMap::new();
        // `name` drives the Browser's default caption for the schema node.
        props.insert("name".to_string(), serde_json::Value::String(label.clone()));
        let element_id = format!("schema-node-{i}");
        node_bytes.push(Node::new(id, vec![label.clone()], props, element_id).to_packstream());
    }

    // One virtual relationship per distinct (type, from-label, to-label).
    let mut rel_bytes: Vec<Vec<u8>> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    for (key, rel) in schema.get_relationships_schemas().iter() {
        let rel_type = key.split("::").next().unwrap_or(key);
        if rel_type.is_empty() {
            continue;
        }
        let from_label = rel.from_node.clone();
        let to_label = rel.to_node.clone();
        if !seen.insert((rel_type.to_string(), from_label.clone(), to_label.clone())) {
            continue;
        }
        let (Some(&from_id), Some(&to_id)) = (label_id.get(&from_label), label_id.get(&to_label))
        else {
            continue;
        };
        let rid = rel_bytes.len() as i64;
        rel_bytes.push(
            Relationship::new(
                rid,
                from_id,
                to_id,
                rel_type.to_string(),
                StdHashMap::new(),
                format!("schema-rel-{rid}"),
                format!("schema-node-{from_id}"),
                format!("schema-node-{to_id}"),
            )
            .to_packstream(),
        );
    }

    vec![
        BoltValue::PackstreamBytes(encode_packstream_list(&node_bytes)),
        BoltValue::PackstreamBytes(encode_packstream_list(&rel_bytes)),
    ]
}

fn is_browser_count_union(query_upper: &str) -> bool {
    let normalized: String = query_upper.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.contains("MATCH (N) RETURN COUNT(N)")
        && normalized.contains("UNION ALL")
        && normalized.contains("MATCH ()-[R]->()")
        && normalized.contains("COUNT(R)")
}

/// Detect Browser 5.x's bundled metadata query:
///   `CALL db.labels() YIELD label RETURN COLLECT(label)[..$itemLimit] AS result`
///   `UNION ALL CALL db.relationshipTypes() YIELD ... RETURN COLLECT(...)[..$itemLimit] ...`
///   `UNION ALL CALL db.propertyKeys() YIELD ... RETURN COLLECT(...)[..$itemLimit] ...`
///
/// The slice expression in RETURN is unsupported by our procedure RETURN evaluator.
/// Older Browser versions issued each `CALL db.<x>()` separately — those still
/// flow through the normal procedure executor unchanged.
fn is_browser_labels_bundle(query_upper: &str) -> bool {
    let has_all_three = query_upper.contains("DB.LABELS")
        && query_upper.contains("DB.RELATIONSHIPTYPES")
        && query_upper.contains("DB.PROPERTYKEYS");
    let has_collect_slice = query_upper.contains("COLLECT(") && query_upper.contains("[..");
    let union_count = query_upper.matches("UNION ALL").count();
    has_all_three && has_collect_slice && union_count >= 2
}

/// Map Browser-issued read-only SHOW commands to the canonical Neo4j-5.x
/// field schema. Returns `Some(fields)` if the query matches a stubbed shape
/// (we always reply with zero rows). `None` means "fall through to the normal
/// pipeline."
///
/// The `query_upper` argument is expected to already be uppercased and trimmed.
/// Tolerates trailing semicolons and `YIELD *`/extra suffixes — Browser
/// commonly appends those. We deliberately accept a substring rather than an
/// exact match so older Browser variants with different suffixes also stub.
fn browser_show_stub_fields(query_upper: &str) -> Option<&'static [&'static str]> {
    let q = query_upper.trim_end_matches(';').trim();
    let starts = |head: &str| q == head || q.starts_with(&format!("{} ", head));

    if starts("SHOW INDEXES") || starts("SHOW INDEX") || starts("SHOW ALL INDEXES") {
        // Neo4j 5.x SHOW INDEXES schema
        return Some(&[
            "id",
            "name",
            "state",
            "populationPercent",
            "type",
            "entityType",
            "labelsOrTypes",
            "properties",
            "indexProvider",
            "owningConstraint",
            "lastRead",
            "readCount",
        ]);
    }
    if starts("SHOW CONSTRAINTS") || starts("SHOW CONSTRAINT") || starts("SHOW ALL CONSTRAINTS") {
        return Some(&[
            "id",
            "name",
            "type",
            "entityType",
            "labelsOrTypes",
            "properties",
            "ownedIndex",
            "propertyType",
        ]);
    }
    if starts("SHOW PROCEDURES") || starts("SHOW PROCEDURE") || starts("SHOW ALL PROCEDURES") {
        return Some(&[
            "name",
            "description",
            "mode",
            "worksOnSystem",
            "signature",
            "argumentDescription",
            "returnDescription",
            "admin",
            "rolesExecution",
            "rolesBoostedExecution",
            "option",
        ]);
    }
    if starts("SHOW FUNCTIONS")
        || starts("SHOW FUNCTION")
        || starts("SHOW ALL FUNCTIONS")
        || starts("SHOW BUILT IN FUNCTIONS")
        || starts("SHOW USER DEFINED FUNCTIONS")
    {
        return Some(&[
            "name",
            "category",
            "description",
            "signature",
            "isBuiltIn",
            "argumentDescription",
            "returnDescription",
            "aggregating",
            "rolesExecution",
            "rolesBoostedExecution",
        ]);
    }
    if starts("SHOW CURRENT USER") {
        return Some(&[
            "user",
            "roles",
            "passwordChangeRequired",
            "suspended",
            "home",
        ]);
    }
    if starts("SHOW USERS") || starts("SHOW USER") {
        return Some(&[
            "user",
            "roles",
            "passwordChangeRequired",
            "suspended",
            "home",
        ]);
    }
    if starts("SHOW ROLES")
        || starts("SHOW ROLE")
        || starts("SHOW POPULATED ROLES")
        || starts("SHOW ALL ROLES")
    {
        return Some(&["role"]);
    }
    if starts("SHOW PRIVILEGES")
        || starts("SHOW PRIVILEGE")
        || starts("SHOW USER PRIVILEGES")
        || starts("SHOW ROLE PRIVILEGES")
        || starts("SHOW ALL PRIVILEGES")
    {
        return Some(&["access", "action", "resource", "graph", "segment", "role"]);
    }
    if starts("SHOW SERVERS") || starts("SHOW SERVER") {
        return Some(&["name", "address", "state", "health", "hosting"]);
    }
    if starts("SHOW SETTINGS") || starts("SHOW SETTING") {
        return Some(&["name", "value", "isDynamic", "defaultValue", "description"]);
    }
    if starts("SHOW TRANSACTIONS") || starts("SHOW TRANSACTION") {
        return Some(&[
            "database",
            "transactionId",
            "currentQueryId",
            "username",
            "currentQuery",
            "startTime",
            "status",
            "elapsedTime",
        ]);
    }
    None
}

/// Pattern detection: `id(alias) IN $paramName` or `id(alias) = $paramName`
/// Substitute Cypher parameters into query string, keeping encoded IDs intact
/// This replaces $paramName with actual values so parser sees literals
/// Used for id() parameters to preserve encoded IDs for label extraction
/// This replaces only $paramName values used with id() so the parser sees literals
/// Used for id() parameters to preserve encoded IDs for label extraction
///
/// SECURITY:
/// - Uses regex with word boundaries to prevent partial matches ($id vs $ids)
/// - Escapes quotes and backslashes in string values
/// - Skips string literals to prevent injection
///
/// LIMITATION: This is a lexical approach that may have edge cases with complex
/// nested strings or comments. For production, consider AST-level parameter binding.
fn substitute_cypher_parameters(query: &str, parameters: &HashMap<String, Value>) -> String {
    use regex::Regex;

    // Helper to check if position is inside a string literal
    fn is_inside_string(query: &str, pos: usize) -> bool {
        let before = &query[..pos];
        let single_quotes = before.matches('\'').count();
        let double_quotes = before.matches('"').count();

        // Odd number of quotes means we're inside a string
        // This is simplified - doesn't handle escaped quotes perfectly
        (single_quotes % 2 == 1) || (double_quotes % 2 == 1)
    }

    fn escape_cypher_string(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '\'' => {
                    out.push('\\');
                    out.push('\'');
                }
                '\\' => {
                    out.push('\\');
                    out.push('\\');
                }
                _ => out.push(ch),
            }
        }
        out
    }

    fn value_to_cypher_literal(value: &Value) -> String {
        match value {
            Value::Array(arr) => {
                let values: Vec<String> =
                    arr.iter()
                        .map(|v| match v {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => format!("'{}'", escape_cypher_string(s)),
                            other => serde_json::to_string(other)
                                .unwrap_or_else(|_| format!("{:?}", other)),
                        })
                        .collect();
                format!("[{}]", values.join(", "))
            }
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("'{}'", escape_cypher_string(s)),
            other => serde_json::to_string(other).unwrap_or_else(|_| format!("{:?}", other)),
        }
    }

    // Regex to find id()-based predicates with a parameter
    let re = Regex::new(
        r"(?i)\bid\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*(?:IN|=)\s*\$([A-Za-z_][A-Za-z0-9_]*)",
    )
    .expect("regex for id() parameter substitution must compile");

    let mut result = query.to_string();
    let mut offset: isize = 0;

    // Collect all matches first to avoid modification-during-iteration issues
    let matches: Vec<_> = re.captures_iter(query).collect();

    for cap in matches {
        let full_match = cap.get(0).unwrap();
        let match_start = full_match.start();
        let param_name = &cap[1];

        // Skip if inside string literal
        if is_inside_string(query, match_start) {
            log::debug!(
                "⏭️  Skipping parameter ${} inside string literal",
                param_name
            );
            continue;
        }

        if let Some(param_value) = parameters.get(param_name) {
            let literal = value_to_cypher_literal(param_value);
            let pattern = format!("${}", param_name);
            let replacement = full_match.as_str().replacen(&pattern, &literal, 1);

            // Calculate position with offset adjustment
            let adjusted_start = (match_start as isize + offset) as usize;
            let adjusted_end = adjusted_start + full_match.as_str().len();

            result.replace_range(adjusted_start..adjusted_end, &replacement);

            // Update offset for subsequent replacements
            offset += replacement.len() as isize - full_match.as_str().len() as isize;
        }
    }

    // Pass 2: substitute integer parameters in `LIMIT $param` / `SKIP $param`.
    // Our Cypher parser only accepts integer literals after LIMIT/SKIP — it does
    // not currently support a Parameter node there. Browser 5.x's expand query
    // uses `LIMIT $maxNeighbours`, which must be inlined as an integer before
    // parsing.
    let re_limit_skip = Regex::new(r"(?i)\b(LIMIT|SKIP)\s+\$([A-Za-z_][A-Za-z0-9_]*)")
        .expect("regex for LIMIT/SKIP parameter substitution must compile");

    // Re-scan the (possibly already-modified) result string.
    let snapshot = result.clone();
    let mut offset2: isize = 0;
    for cap in re_limit_skip.captures_iter(&snapshot) {
        let full_match = cap.get(0).unwrap();
        let match_start = full_match.start();
        let keyword = &cap[1];
        let param_name = &cap[2];

        if is_inside_string(&snapshot, match_start) {
            continue;
        }

        // LIMIT/SKIP only accept non-negative integers. Any other type is a
        // user error that the parser will surface; leave it untouched so the
        // existing error path runs.
        let int_literal = match parameters.get(param_name) {
            Some(Value::Number(n)) if n.is_i64() || n.is_u64() => n.to_string(),
            _ => continue,
        };

        let replacement = format!("{} {}", keyword, int_literal);
        let adjusted_start = (match_start as isize + offset2) as usize;
        let adjusted_end = adjusted_start + full_match.as_str().len();
        result.replace_range(adjusted_start..adjusted_end, &replacement);
        offset2 += replacement.len() as isize - full_match.as_str().len() as isize;
    }

    // Pass 2.5: collapse Neo4j-Browser's de-duplication CASE pattern to a bare
    // alias. Browser emits:
    //   CASE WHEN elementId(o) IN $existingNodeIds THEN null ELSE o END AS o
    // intending null for nodes already on the canvas. But under CASE, our SQL
    // generator only projects the alias's id column (not the full property set
    // needed to materialize a Node), so Browser never receives a real neighbor
    // Node and click-to-expand can't render new circles. Simplifying to bare
    // `o AS o` always returns the full Node; Browser de-dupes client-side via
    // its own existingNodeIds tracking, so duplicates are harmless.
    let re_dedupe_case = Regex::new(
        r"(?i)CASE\s+WHEN\s+(?:elementId|id)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s+IN\s+\$[A-Za-z_][A-Za-z0-9_]*\s+THEN\s+null\s+ELSE\s+([A-Za-z_][A-Za-z0-9_]*)\s+END(\s+AS\s+([A-Za-z_][A-Za-z0-9_]*))?",
    )
    .expect("regex for Browser dedupe-CASE collapse must compile");
    let snapshot = result.clone();
    let mut offset_dc: isize = 0;
    for cap in re_dedupe_case.captures_iter(&snapshot) {
        let full_match = cap.get(0).unwrap();
        let match_start = full_match.start();
        let inner_alias = &cap[1];
        let else_alias = &cap[2];
        // Only collapse when the WHEN-target and ELSE-branch refer to the same
        // alias — that's Browser's specific de-duplication shape. Anything else
        // is a user CASE we must not touch.
        if !inner_alias.eq_ignore_ascii_case(else_alias) {
            continue;
        }
        if is_inside_string(&snapshot, match_start) {
            continue;
        }
        let replacement = if let Some(out_alias) = cap.get(4) {
            // Preserve the explicit `AS <alias>` — also require it match.
            if !out_alias.as_str().eq_ignore_ascii_case(else_alias) {
                continue;
            }
            format!("{} AS {}", else_alias, out_alias.as_str())
        } else {
            else_alias.to_string()
        };
        let adjusted_start = (match_start as isize + offset_dc) as usize;
        let adjusted_end = adjusted_start + full_match.as_str().len();
        result.replace_range(adjusted_start..adjusted_end, &replacement);
        offset_dc += replacement.len() as isize - full_match.as_str().len() as isize;
    }

    // Pass 3: rewrite `elementId(alias) IN $param` / `elementId(alias) = $param`
    // to `id(alias) IN [int_list]` / `id(alias) = int`. Modern Neo4j Browser
    // switches to elementId-mode (and emits these predicates) whenever the
    // first node in its canvas has an elementId containing `-` — which is
    // always true given our Browser-compat sentinel. Browser passes our
    // element_id strings (e.g., `"User:1-"`) through; we decode them with
    // parse_node_element_id and re-encode through compute_deterministic_id so
    // the existing id-rewriter pipeline (further downstream in the planner)
    // handles the rest.
    // `\b(?:AND|WHERE)\s+(?:NOT\s+)?` optionally captures a leading boolean
    // connector so that, for relationship-id predicates we want to drop, we
    // can rip the whole conjunct out of the WHERE clause instead of leaving a
    // dangling "AND". The connector is optional — used when `_lead` matches.
    let re_elem_id = Regex::new(
        r"(?i)(?:(?P<lead>\b(?:AND|WHERE)\s+(?:NOT\s+)?))?\belementId\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*(?P<op>IN|=)\s*\$(?P<param>[A-Za-z_][A-Za-z0-9_]*)",
    )
    .expect("regex for elementId() parameter substitution must compile");

    let snapshot = result.clone();
    let mut offset3: isize = 0;
    for cap in re_elem_id.captures_iter(&snapshot) {
        let full_match = cap.get(0).unwrap();
        let match_start = full_match.start();
        let alias = cap.name("alias").unwrap().as_str();
        let op = cap.name("op").unwrap().as_str();
        let param_name = cap.name("param").unwrap().as_str();
        let lead = cap.name("lead").map(|m| m.as_str().to_string());

        if is_inside_string(&snapshot, match_start) {
            continue;
        }

        let elem_strings: Vec<&str> = match parameters.get(param_name) {
            Some(Value::Array(arr)) => arr.iter().filter_map(Value::as_str).collect(),
            Some(Value::String(s)) => vec![s.as_str()],
            _ => continue,
        };

        // Relationship element_ids contain `->` (e.g., `"FOLLOWS:1->4-"`).
        // The downstream AST id-rewriter only knows how to translate `id(x)`
        // for node aliases — it has no path to map an encoded relationship id
        // back to a real DB column. Generating `id(r) IN [encoded_ints]` ends
        // up rendering as `r.id = 'value' OR ...` against a non-existent
        // column, the planner short-circuits to `WHERE false`, and the whole
        // expand returns zero rows.
        //
        // Browser uses this predicate purely for client-side dedup, so it's
        // safe to drop. If we have a leading `AND/AND NOT/WHERE/WHERE NOT`
        // captured, drop the entire conjunct; otherwise the user wrote a
        // standalone `elementId(r) IN ...` and we leave the predicate as a
        // tautology so the surrounding boolean shape stays valid.
        let is_rel_param = elem_strings.iter().any(|s| s.contains("->"));
        if is_rel_param {
            let adjusted_start = (match_start as isize + offset3) as usize;
            let adjusted_end = adjusted_start + full_match.as_str().len();
            let replacement = if lead.is_some() {
                String::new()
            } else {
                "true".to_string()
            };
            result.replace_range(adjusted_start..adjusted_end, &replacement);
            offset3 += replacement.len() as isize - full_match.as_str().len() as isize;
            continue;
        }

        // Decode each element_id string into the integer the id-rewriter
        // expects. compute_deterministic_id is the same function used to
        // assign Node.id values, so the round-trip is exact for nodes.
        use crate::server::bolt_protocol::id_mapper::IdMapper;
        let encoded_ints: Vec<String> = elem_strings
            .iter()
            .map(|s| IdMapper::compute_deterministic_id(s).to_string())
            .collect();

        let predicate = match op.to_uppercase().as_str() {
            "IN" => format!("id({}) IN [{}]", alias, encoded_ints.join(", ")),
            "=" => {
                if encoded_ints.len() == 1 {
                    format!("id({}) = {}", alias, encoded_ints[0])
                } else {
                    format!("id({}) IN [{}]", alias, encoded_ints.join(", "))
                }
            }
            _ => continue,
        };
        let replacement = match lead.as_deref() {
            Some(l) => format!("{}{}", l, predicate),
            None => predicate,
        };

        let adjusted_start = (match_start as isize + offset3) as usize;
        let adjusted_end = adjusted_start + full_match.as_str().len();
        result.replace_range(adjusted_start..adjusted_end, &replacement);
        offset3 += replacement.len() as isize - full_match.as_str().len() as isize;
    }

    log::debug!(
        "🔧 Parameter substitution: {} parameters processed",
        parameters.len()
    );
    result
}

/// Legacy function - replaced by substitute_cypher_parameters
/// Kept for potential future use but currently unused
#[allow(dead_code)]
fn decode_id_parameters(
    query: &str,
    mut parameters: HashMap<String, Value>,
) -> HashMap<String, Value> {
    use super::id_mapper::IdMapper;
    use regex::Regex;

    // Pattern: id(alias) IN $paramName
    let id_in_param = Regex::new(r"(?i)\bid\s*\(\s*\w+\s*\)\s+IN\s+\$(\w+)").unwrap();
    // Pattern: id(alias) = $paramName
    let id_eq_param = Regex::new(r"(?i)\bid\s*\(\s*\w+\s*\)\s*=\s*\$(\w+)").unwrap();

    // Collect parameter names used with id()
    let mut id_params: Vec<String> = Vec::new();

    for cap in id_in_param.captures_iter(query) {
        if let Some(param_name) = cap.get(1) {
            id_params.push(param_name.as_str().to_string());
        }
    }
    for cap in id_eq_param.captures_iter(query) {
        if let Some(param_name) = cap.get(1) {
            id_params.push(param_name.as_str().to_string());
        }
    }

    if id_params.is_empty() {
        return parameters;
    }

    log::info!(
        "🔧 decode_id_parameters: Found id() parameters: {:?}",
        id_params
    );

    // Decode each id() parameter
    for param_name in id_params {
        if let Some(value) = parameters.get_mut(&param_name) {
            match value {
                Value::Array(arr) => {
                    // Decode each element in the array
                    let decoded: Vec<Value> = arr
                        .iter()
                        .map(|v| {
                            if let Some(encoded_id) = v.as_i64() {
                                // Use IdMapper to decode (tries cache first)
                                if let Some((_label, raw_value)) =
                                    IdMapper::decode_for_query(encoded_id)
                                {
                                    log::debug!(
                                        "  Decoded {} -> {} (from cache)",
                                        encoded_id,
                                        raw_value
                                    );
                                    // Try to parse as integer, fallback to string
                                    if let Ok(int_val) = raw_value.parse::<i64>() {
                                        return Value::Number(int_val.into());
                                    }
                                    return Value::String(raw_value);
                                } else {
                                    // Fallback: extract raw_value directly from bit pattern
                                    // This handles cross-session IDs where cache doesn't have the mapping
                                    // Use 47-bit mask (matching the JS-safe encoding in id_encoding.rs)
                                    const ID_MASK: i64 = (1i64 << 47) - 1; // 0x7FFFFFFFFFFF
                                    let raw_value = encoded_id & ID_MASK;
                                    log::debug!(
                                        "  Decoded {} -> {} (from bit pattern)",
                                        encoded_id,
                                        raw_value
                                    );
                                    return Value::Number(raw_value.into());
                                }
                            }
                            // Keep original if not a number
                            v.clone()
                        })
                        .collect();

                    log::info!(
                        "🔧 Decoded parameter '{}': {} values (from {} original)",
                        param_name,
                        decoded.len(),
                        arr.len()
                    );
                    *value = Value::Array(decoded);
                }
                Value::Number(n) => {
                    // Single value
                    if let Some(encoded_id) = n.as_i64() {
                        if let Some((_label, raw_value)) = IdMapper::decode_for_query(encoded_id) {
                            log::info!(
                                "🔧 Decoded parameter '{}': {} -> {} (from cache)",
                                param_name,
                                encoded_id,
                                raw_value
                            );
                            if let Ok(int_val) = raw_value.parse::<i64>() {
                                *value = Value::Number(int_val.into());
                            } else {
                                *value = Value::String(raw_value);
                            }
                        } else {
                            // Fallback: extract raw_value directly from bit pattern
                            // Use 47-bit mask (matching the JS-safe encoding in id_encoding.rs)
                            const ID_MASK: i64 = (1i64 << 47) - 1; // 0x7FFFFFFFFFFF
                            let raw_value = encoded_id & ID_MASK;
                            log::info!(
                                "🔧 Decoded parameter '{}': {} -> {} (from bit pattern)",
                                param_name,
                                encoded_id,
                                raw_value
                            );
                            *value = Value::Number(raw_value.into());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    parameters
}

/// Bolt protocol message handler
pub struct BoltHandler {
    /// Connection context
    context: Arc<Mutex<BoltContext>>,
    /// Server configuration
    config: Arc<BoltConfig>,
    /// Authenticator
    authenticator: Authenticator,
    /// Current authenticated user
    authenticated_user: Option<AuthenticatedUser>,
    /// SQL executor for query execution
    executor: Arc<dyn QueryExecutor>,
    /// Cached query results for streaming
    cached_results: Option<Vec<Vec<BoltValue>>>,
}

impl BoltHandler {
    /// Create a new Bolt message handler
    pub fn new(
        context: Arc<Mutex<BoltContext>>,
        config: Arc<BoltConfig>,
        executor: Arc<dyn QueryExecutor>,
    ) -> Self {
        BoltHandler {
            context,
            config: config.clone(),
            authenticator: Authenticator::new(config.enable_auth, config.default_user.clone()),
            authenticated_user: None,
            executor,
            cached_results: None,
        }
    }

    /// Handle a Bolt message and return response messages
    pub async fn handle_message(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::debug!("Handling Bolt message: {}", message.type_name());

        match message.signature {
            signatures::HELLO => self.handle_hello(message).await,
            signatures::LOGON => self.handle_logon(message).await,
            signatures::LOGOFF => self.handle_logoff(message).await,
            signatures::GOODBYE => self.handle_goodbye(message).await,
            signatures::RESET => self.handle_reset(message).await,
            signatures::RUN => self.handle_run(message).await,
            signatures::PULL => self.handle_pull(message).await,
            signatures::DISCARD => self.handle_discard(message).await,
            signatures::BEGIN => self.handle_begin(message).await,
            signatures::COMMIT => self.handle_commit(message).await,
            signatures::ROLLBACK => self.handle_rollback(message).await,
            signatures::ROUTE => self.handle_route(message).await,
            _ => {
                log::warn!("Unhandled Bolt message type: {}", message.type_name());
                Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    format!("Unhandled message type: {}", message.type_name()),
                )])
            }
        }
    }

    /// Handle HELLO message (Bolt 5.1+: no auth, just connection initialization)
    async fn handle_hello(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        let (current_state, negotiated_version) = {
            let context = lock_context!(self.context);
            let version = match &context.state {
                ConnectionState::Negotiated(v) => *v,
                _ => 0,
            };
            (context.state.clone(), version)
        };

        if !matches!(current_state, ConnectionState::Negotiated(_)) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                "HELLO message received in invalid state".to_string(),
            )]);
        }

        // Determine if this is Bolt 5.1+ (authentication moved to LOGON)
        let is_bolt_51_plus = negotiated_version >= 0x00000501;

        // DEBUG: Log HELLO message structure
        log::info!("🔍 HELLO message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::info!("  HELLO Field[{}]: {}", i, bolt_value_to_string(field));
        }

        if is_bolt_51_plus {
            // Bolt 5.1+: HELLO just initializes connection, auth happens in LOGON
            log::info!("HELLO received (Bolt 5.1+), awaiting LOGON for authentication");

            // Extract database from HELLO extra field (routing context)
            let database = message.extract_database();
            log::info!("📊 Extracted database from HELLO: {:?}", database);

            // Store database selection in context for later use in LOGON
            if let Some(ref db_name) = database {
                let mut context = lock_context!(self.context);
                context.schema_name = Some(db_name.clone());
                context.id_mapper.set_scope(Some(db_name.clone()), None);
                log::info!("Database/schema specified in HELLO: {}", db_name);
            }

            // Update context to AUTHENTICATION state
            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Authentication(negotiated_version));
            }

            // Create success response with server information
            let mut metadata = HashMap::new();
            metadata.insert(
                "server".to_string(),
                Value::String(self.config.server_agent.clone()),
            );
            metadata.insert(
                "connection_id".to_string(),
                Value::String("bolt-1".to_string()),
            );

            // Add available databases (schemas) so browser can show them
            // Neo4j 2025.10+ browser uses this to populate database selection menu
            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                let schemas_map = schemas_lock.read().await;
                let available_databases: Vec<Value> = schemas_map
                    .iter()
                    .filter(|(name, _)| name != &"default") // Don't duplicate "default"
                    .map(|(name, _)| Value::String(name.clone()))
                    .collect();

                let db_count = available_databases.len();
                if db_count > 0 {
                    metadata.insert("databases".to_string(), Value::Array(available_databases));
                    log::info!("📚 Advertised {} database(s) in HELLO response", db_count);
                }
            }

            // Add server capabilities
            let mut hints = HashMap::new();
            hints.insert("utc_patch".to_string(), Value::Bool(false));
            hints.insert("patch_bolt".to_string(), Value::Bool(false));
            metadata.insert(
                "hints".to_string(),
                Value::Object(serde_json::Map::from_iter(hints)),
            );

            Ok(vec![BoltMessage::success(metadata)])
        } else {
            // Bolt 4.x and earlier: HELLO includes authentication
            // Debug: log HELLO message fields
            log::debug!("HELLO message has {} fields", message.fields.len());
            for (i, field) in message.fields.iter().enumerate() {
                log::debug!("  Field[{}]: {}", i, bolt_value_to_string(field));
            }

            let auth_token = message.extract_auth_token().unwrap_or_default();

            // Extract database selection (Neo4j 4.0+ multi-database support)
            let database = message.extract_database();
            log::debug!("Extracted database from HELLO: {:?}", database);

            // Parse authentication token
            let token = AuthToken::from_hello_fields(&auth_token)?;

            // Authenticate user
            match self.authenticator.authenticate(&token) {
                Ok(user) => {
                    self.authenticated_user = Some(user.clone());

                    // Update context
                    {
                        let mut context = lock_context!(self.context);
                        context.set_user(user.username.clone());
                        context.schema_name = database.clone();
                        let tenant_id = context.tenant_id.clone();
                        context.id_mapper.set_scope(database.clone(), tenant_id);
                        context.set_state(ConnectionState::Ready);
                    }

                    // Log database selection
                    if let Some(ref db) = database {
                        log::info!("Bolt connection using database/schema: {}", db);
                    } else {
                        log::info!("Bolt connection using default schema");
                    }

                    // Create success response with server information
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "server".to_string(),
                        Value::String(self.config.server_agent.clone()),
                    );
                    metadata.insert(
                        "connection_id".to_string(),
                        Value::String("bolt-1".to_string()),
                    );

                    // Add server capabilities
                    let mut hints = HashMap::new();
                    hints.insert("utc_patch".to_string(), Value::Bool(false));
                    hints.insert("patch_bolt".to_string(), Value::Bool(false));
                    metadata.insert(
                        "hints".to_string(),
                        Value::Object(serde_json::Map::from_iter(hints)),
                    );

                    log::info!("Bolt authentication successful for user: {}", user.username);
                    Ok(vec![BoltMessage::success(metadata)])
                }
                Err(auth_error) => {
                    log::warn!("Bolt authentication failed: {}", auth_error);

                    // Update context to failed state
                    {
                        let mut context = lock_context!(self.context);
                        context.set_state(ConnectionState::Failed);
                    }

                    Ok(vec![BoltMessage::failure(
                        auth_error.error_code().to_string(),
                        auth_error.to_string(),
                    )])
                }
            }
        }
    }

    /// Handle LOGON message (Bolt 5.1+ authentication)
    async fn handle_logon(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        let current_state = {
            let context = lock_context!(self.context);
            context.state.clone()
        };

        // LOGON can only be processed in AUTHENTICATION state (Bolt 5.1+)
        if !matches!(current_state, ConnectionState::Authentication(_)) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                format!(
                    "LOGON message received in invalid state: {:?}",
                    current_state
                ),
            )]);
        }

        // Debug: log LOGON message fields
        log::info!("🔍 LOGON message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::info!("  LOGON Field[{}]: {}", i, bolt_value_to_string(field));
        }

        // Extract authentication token from LOGON message
        // Handle empty LOGON (auth-less mode for Bolt 5.x)
        let auth_token = if message.fields.is_empty() {
            log::info!("Empty LOGON message received - using auth-less mode");
            HashMap::new() // Empty auth = no authentication required
        } else {
            message.extract_logon_auth().ok_or_else(|| {
                BoltError::invalid_message("Missing authentication data in LOGON message")
            })?
        };

        log::debug!(
            "Extracted auth token: {:?}",
            auth_token.keys().collect::<Vec<_>>()
        );

        // Parse authentication token
        let token = AuthToken::from_hello_fields(&auth_token)?;

        // Authenticate user
        match self.authenticator.authenticate(&token) {
            Ok(user) => {
                self.authenticated_user = Some(user.clone());

                // Extract database from auth_token if present (Bolt 5.1+ can include db in LOGON)
                let mut database = auth_token
                    .get("db")
                    .or_else(|| auth_token.get("database"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                // If no database in LOGON, check if it was set in HELLO (Bolt 5.1+)
                if database.is_none() {
                    let context = lock_context!(self.context);
                    database = context.schema_name.clone();
                    if database.is_some() {
                        log::debug!("Using database from HELLO: {:?}", database);
                    }
                }

                // If still no database specified, use the first loaded schema (if any)
                if database.is_none() {
                    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                        let schemas = schemas_lock.read().await;
                        // Find first non-default schema
                        let first_schema = schemas
                            .keys()
                            .find(|k| *k != "default")
                            .or_else(|| schemas.keys().next())
                            .cloned();
                        if let Some(schema_name) = first_schema {
                            log::info!(
                                "No database specified in LOGON, using first loaded schema: {}",
                                schema_name
                            );
                            database = Some(schema_name);
                        }
                    }
                }

                // Update context
                {
                    let mut context = lock_context!(self.context);
                    context.set_user(user.username.clone());
                    context.schema_name = database.clone();
                    context.id_mapper.set_scope(database.clone(), None);
                    context.set_state(ConnectionState::Ready);
                }

                // Log database selection
                if let Some(ref db) = database {
                    log::info!("Bolt LOGON successful, using database/schema: {}", db);
                } else {
                    log::info!("Bolt LOGON successful, using default schema");
                }

                // Create success response
                let metadata = HashMap::new();
                log::info!("Bolt authentication successful for user: {}", user.username);
                Ok(vec![BoltMessage::success(metadata)])
            }
            Err(auth_error) => {
                log::warn!("Bolt LOGON failed: {}", auth_error);

                // Update context to failed state
                {
                    let mut context = lock_context!(self.context);
                    context.set_state(ConnectionState::Failed);
                }

                Ok(vec![BoltMessage::failure(
                    auth_error.error_code().to_string(),
                    auth_error.to_string(),
                )])
            }
        }
    }

    /// Handle LOGOFF message (Bolt 5.1+ - log out and return to authentication state)
    async fn handle_logoff(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state - LOGOFF can only be called in READY state
        let current_state = {
            let context = lock_context!(self.context);
            context.state.clone()
        };

        if !matches!(current_state, ConnectionState::Ready) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                format!(
                    "LOGOFF message received in invalid state: {:?}",
                    current_state
                ),
            )]);
        }

        // Clear authentication
        let username = self.authenticated_user.as_ref().map(|u| u.username.clone());
        self.authenticated_user = None;

        // Get negotiated version to restore proper authentication state
        let negotiated_version = match current_state {
            ConnectionState::Ready => {
                // Get from context if we stored it
                0x00000501 // Default to 5.1 if we're handling LOGOFF
            }
            _ => 0x00000501,
        };

        // Update context to AUTHENTICATION state
        {
            let mut context = lock_context!(self.context);
            context.set_user(String::new());
            context.schema_name = None;
            context.tenant_id = None;
            context.id_mapper.set_scope(None, None);
            context.set_state(ConnectionState::Authentication(negotiated_version));
        }

        if let Some(user) = username {
            log::info!("Bolt LOGOFF successful for user: {}", user);
        }

        // Return success
        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle GOODBYE message (connection termination)
    async fn handle_goodbye(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("Received GOODBYE message, closing connection");

        // Update context
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Failed);
        }

        // No response needed for GOODBYE
        Ok(vec![])
    }

    /// Handle RESET message (connection reset)
    async fn handle_reset(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("Resetting Bolt connection");

        // Reset connection state but keep authentication
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
            context.tx_id = None; // Clear any active transaction
        }

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle session commands like `CALL sys.set('key', 'value')` or
    /// `CALL dbms.setConfigValue('key', 'value')` (browser-friendly alias).
    /// These are intercepted before Cypher parsing and modify BoltContext state.
    /// Returns Some(response) if the query was a session command, None otherwise.
    async fn handle_session_command(
        &mut self,
        query: &str,
    ) -> BoltResult<Option<Vec<BoltMessage>>> {
        let trimmed = query.trim();
        let lower = trimmed.to_lowercase();

        // Accept both CALL sys.set(...) and CALL dbms.setConfigValue(...)
        let is_session_cmd =
            lower.starts_with("call sys.set") || lower.starts_with("call dbms.setconfigvalue");
        if !is_session_cmd {
            return Ok(None);
        }

        // Bare command without parens — usage error
        if !trimmed.contains('(') {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL sys.set('key', 'value') or CALL dbms.setConfigValue('key', 'value')"
                    .to_string(),
            )]));
        }

        // Parse arguments from CALL sys.set(arg1, arg2)
        let inner = trimmed.trim_end_matches([';', ')']);
        let inner = if let Some(pos) = inner.find('(') {
            &inner[pos + 1..]
        } else {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL dbms.setConfigValue('key', 'value')".to_string(),
            )]));
        };

        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() != 2 {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL dbms.setConfigValue('key', 'value')".to_string(),
            )]));
        }

        let key = parts[0].trim().trim_matches('\'').trim_matches('"');
        let value = parts[1].trim().trim_matches('\'').trim_matches('"');

        log::info!("Session command: setting {} = {}", key, value);

        // Store in BoltContext metadata and update IdMapper scope
        {
            let mut context = lock_context!(self.context);
            context.metadata.insert(key.to_string(), value.to_string());

            // Special handling for tenant_id
            if key == "tenant_id" {
                context.tenant_id = Some(value.to_string());
                let schema = context.schema_name.clone();
                context.id_mapper.set_scope(schema, Some(value.to_string()));
            }

            // Set state to Streaming so PULL can deliver the cached result
            context.set_state(ConnectionState::Streaming);
        }

        // Return SUCCESS with fields metadata so browser can PULL the result
        let mut meta = HashMap::new();
        meta.insert(
            "fields".to_string(),
            Value::Array(vec![Value::String("result".to_string())]),
        );
        // Store confirmation message for PULL
        self.cached_results = Some(vec![vec![BoltValue::Json(Value::String(format!(
            "Session {} set to {}",
            key, value
        )))]]);

        Ok(Some(vec![BoltMessage::success(meta)]))
    }

    /// Handle RUN message (execute Cypher query)
    async fn handle_run(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "RUN message received in invalid state".to_string(),
                )]);
            }
        }

        // Extract query and parameters
        let query = message
            .extract_query()
            .ok_or_else(|| BoltError::invalid_message("RUN message missing query"))?;

        // Log incoming Cypher query for debugging
        log::info!("📨 BROWSER SENT CYPHER QUERY: {}", query.trim());

        // Handle EXPLAIN queries — browser sends "EXPLAIN <partial_query>" as autocomplete
        // probes while the user types. Return empty SUCCESS so probes don't show errors.
        // For fully-formed EXPLAIN queries, this also prevents unnecessary execution.
        if query.trim().to_lowercase().starts_with("explain ") {
            log::debug!("EXPLAIN query (returning empty plan): {}", query.trim());
            let mut meta = HashMap::new();
            meta.insert("fields".to_string(), Value::Array(vec![]));
            // Set state to Streaming so the subsequent PULL gets a clean completion
            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(vec![]);
            return Ok(vec![BoltMessage::success(meta)]);
        }

        // Intercept session commands before Cypher parsing
        if let Some(response) = self.handle_session_command(query).await? {
            return Ok(response);
        }

        let parameters = message.extract_parameters().unwrap_or_default();

        // Substitute Cypher parameters into query string (keeping encoded IDs)
        // This allows parser to see actual values as literals while preserving encoding
        // The AST transformer will then extract labels from encoded IDs for UNION pruning
        let query = substitute_cypher_parameters(query, &parameters);

        // NOTE: Do NOT rewrite the browser's directed relationship-fetch to undirected.
        // The browser's Path objects from the expand query already carry all relationship data.
        // Making it undirected causes Relationship objects with schema-direction start/end IDs
        // that reference nodes not in the browser's graph, crashing with "t.source is undefined".

        // Get selected schema from context, or from RUN message metadata
        let (schema_name, tenant_id, role, view_parameters) = {
            let context = lock_context!(self.context);

            // Debug: log RUN message fields
            log::info!("🔍 RUN message has {} fields", message.fields.len());
            for (i, field) in message.fields.iter().enumerate() {
                log::info!("  Field[{}]: {}", i, bolt_value_to_string(field));
            }

            // Check if RUN message specifies a database (Bolt 4.x)
            let schema_name = if let Some(run_db) = message.extract_run_database() {
                log::info!("✅ RUN message contains database: {}", run_db);
                if run_db != context.schema_name.as_deref().unwrap_or("default") {
                    log::debug!(
                        "RUN message overriding schema: {} -> {}",
                        context.schema_name.as_deref().unwrap_or("default"),
                        run_db
                    );
                }
                Some(run_db)
            } else {
                log::info!(
                    "⚠️  RUN message does NOT contain database field, using context schema: {:?}",
                    context.schema_name
                );
                context.schema_name.clone()
            };

            // Extract tenant_id from RUN message metadata, or fall back to session-level
            // value set via CALL sys.set('tenant_id', '...')
            let tenant_id = message
                .extract_run_tenant_id()
                .or_else(|| context.tenant_id.clone());
            if let Some(ref tid) = tenant_id {
                log::debug!("✅ Using tenant_id: {}", tid);
            }

            // Extract role from RUN message metadata (Phase 2 RBAC)
            let role = message.extract_run_role();
            if let Some(ref r) = role {
                log::debug!("✅ RUN message contains role: {}", r);
            }

            // Extract view_parameters from RUN message metadata (Phase 2 Multi-tenancy)
            let view_parameters = message.extract_run_view_parameters();
            if let Some(ref vp) = view_parameters {
                log::debug!("✅ RUN message contains view_parameters: {:?}", vp);
            }

            (schema_name, tenant_id, role, view_parameters)
        };

        // Store tenant_id on context (needed for execute_cypher_query fallback)
        if let Some(ref tid) = tenant_id {
            let mut context = lock_context!(self.context);
            context.tenant_id = Some(tid.clone());
        }

        log::info!("Executing Cypher query: {}", query);

        if let Some(ref schema) = schema_name {
            log::debug!("Query execution using schema: {}", schema);
        } else {
            log::debug!("Query execution using schema: default");
        }

        // Check for SHOW DATABASES command before normal query processing
        let query_upper = query.trim().to_uppercase();
        if query_upper == "SHOW DATABASES" || query_upper.starts_with("SHOW DATABASES ") {
            log::info!("🔍 Detected SHOW DATABASES command in Bolt handler");

            // Use shared SHOW DATABASES implementation
            let databases_result = crate::procedures::show_databases::execute_show_databases();

            // Field order must match the `fields` metadata sent in the SUCCESS reply
            // below. Neo4j Browser validates each row against a schema keyed off
            // those field names, so missing or out-of-order entries surface as
            // "Invalid database record received from the server".
            let databases: Vec<Vec<BoltValue>> = match databases_result {
                Ok(db_list) => db_list
                    .into_iter()
                    .map(|db| {
                        let get_str =
                            |key: &str, default: &str| {
                                BoltValue::Json(db.get(key).cloned().unwrap_or_else(|| {
                                    serde_json::Value::String(default.to_string())
                                }))
                            };
                        let get_bool = |key: &str, default: bool| {
                            BoltValue::Json(
                                db.get(key)
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Bool(default)),
                            )
                        };
                        let get_arr = |key: &str| {
                            BoltValue::Json(
                                db.get(key)
                                    .cloned()
                                    .unwrap_or_else(|| serde_json::Value::Array(vec![])),
                            )
                        };
                        vec![
                            get_str("name", "default"),
                            get_str("type", "standard"),
                            get_arr("aliases"),
                            get_str("access", "read-write"),
                            get_str("address", "localhost:7687"),
                            get_str("role", "primary"),
                            get_bool("writer", true),
                            get_str("requestedStatus", "online"),
                            get_str("currentStatus", "online"),
                            get_str("statusMessage", ""),
                            get_bool("default", false),
                            get_bool("home", false),
                            get_arr("constituents"),
                        ]
                    })
                    .collect(),
                Err(e) => {
                    log::error!("Failed to execute SHOW DATABASES: {}", e);
                    vec![vec![
                        BoltValue::Json(serde_json::Value::String("default".to_string())),
                        BoltValue::Json(serde_json::Value::String("standard".to_string())),
                        BoltValue::Json(serde_json::json!([])),
                        BoltValue::Json(serde_json::Value::String("read-write".to_string())),
                        BoltValue::Json(serde_json::Value::String("localhost:7687".to_string())),
                        BoltValue::Json(serde_json::Value::String("primary".to_string())),
                        BoltValue::Json(serde_json::json!(true)),
                        BoltValue::Json(serde_json::Value::String("online".to_string())),
                        BoltValue::Json(serde_json::Value::String("online".to_string())),
                        BoltValue::Json(serde_json::Value::String("".to_string())),
                        BoltValue::Json(serde_json::json!(true)),
                        BoltValue::Json(serde_json::json!(true)),
                        BoltValue::Json(serde_json::json!([])),
                    ]]
                }
            };

            log::info!("📊 Returning {} databases via Bolt", databases.len());

            // Update context to streaming state
            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }

            // Store the database records for PULL to stream
            self.cached_results = Some(databases);

            // Build result metadata for SUCCESS
            let mut result_metadata = HashMap::new();
            result_metadata.insert(
                "fields".to_string(),
                serde_json::json!([
                    "name",
                    "type",
                    "aliases",
                    "access",
                    "address",
                    "role",
                    "writer",
                    "requestedStatus",
                    "currentStatus",
                    "statusMessage",
                    "default",
                    "home",
                    "constituents"
                ]),
            );
            result_metadata.insert("result_consumed_after".to_string(), serde_json::json!(-1));

            return Ok(vec![BoltMessage::success(result_metadata)]);
        }

        // Intercept `CALL dbms.components()` — Browser calls this on connect to
        // discover the Neo4j version and refuses to load if the response is
        // missing or empty ("Invalid version: "). We respond with the version
        // we already advertise via server_agent ("Neo4j/5.8.0" in compat mode).
        let q_norm = query_upper.trim_end_matches(';').trim();
        let is_dbms_components = q_norm == "CALL DBMS.COMPONENTS()"
            || q_norm.starts_with("CALL DBMS.COMPONENTS() ")
            || q_norm.starts_with("CALL DBMS.COMPONENTS()YIELD")
            || q_norm == "CALL DBMS.COMPONENTS"
            || q_norm.starts_with("CALL DBMS.COMPONENTS ");
        if is_dbms_components {
            log::info!("🔍 Stubbing CALL dbms.components()");

            // Strip "Neo4j/" prefix from server_agent if present, leaving just
            // the version string (e.g., "5.8.0"). Falls back to "5.8.0" if the
            // agent string isn't in the expected form.
            let version = self
                .config
                .server_agent
                .strip_prefix("Neo4j/")
                .unwrap_or("5.8.0")
                .to_string();

            let row: Vec<BoltValue> = vec![
                BoltValue::Json(serde_json::Value::String("Neo4j Kernel".to_string())),
                BoltValue::Json(serde_json::json!([version])),
                BoltValue::Json(serde_json::Value::String("community".to_string())),
            ];

            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(vec![row]);

            let mut result_metadata = HashMap::new();
            result_metadata.insert(
                "fields".to_string(),
                serde_json::json!(["name", "versions", "edition"]),
            );
            result_metadata.insert("result_consumed_after".to_string(), serde_json::json!(-1));
            return Ok(vec![BoltMessage::success(result_metadata)]);
        }

        // Intercept `CALL db.schema.visualization()` — the Browser renders this
        // as the "Database Information" schema diagram AND uses the labels /
        // relationship types it returns to build the default graph styling
        // (per-label colours + captions) applied to query results. The
        // procedure-registry path can only emit flat JSON, so we build the
        // virtual graph here where Node/Relationship structures can be
        // packstream-encoded: one virtual node per label, one virtual
        // relationship per (type, from-label, to-label) triple.
        let is_schema_viz = q_norm == "CALL DB.SCHEMA.VISUALIZATION()"
            || q_norm == "CALL DB.SCHEMA.VISUALIZATION"
            || q_norm.starts_with("CALL DB.SCHEMA.VISUALIZATION() ")
            || q_norm.starts_with("CALL DB.SCHEMA.VISUALIZATION()YIELD")
            || q_norm.starts_with("CALL DB.SCHEMA.VISUALIZATION ");
        if is_schema_viz {
            log::info!("🔍 Building CALL db.schema.visualization() virtual graph");

            let effective_schema = schema_name.as_deref().unwrap_or("default").to_string();
            let schema_guard = crate::server::GLOBAL_SCHEMAS
                .get()
                .ok_or_else(|| BoltError::internal("Schema registry not initialized"))?;
            let schemas = schema_guard.read().await;
            let schema = schemas
                .get(&effective_schema)
                .ok_or_else(|| BoltError::internal("Schema not found"))?;

            let fields = build_schema_visualization_fields(schema);
            drop(schemas);

            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(vec![fields]);

            let mut result_metadata = HashMap::new();
            result_metadata.insert(
                "fields".to_string(),
                serde_json::json!(["nodes", "relationships"]),
            );
            result_metadata.insert("result_consumed_after".to_string(), serde_json::json!(-1));
            return Ok(vec![BoltMessage::success(result_metadata)]);
        }

        // Stub SHOW commands Browser issues on connect to populate sidebars.
        // We don't manage indexes, procedures, functions, users, or roles — but
        // returning the canonical Neo4j-5.x field schema with zero rows keeps
        // Browser happy. Each shape gets the field list Browser's schema
        // validator expects; missing fields surface as "Invalid record".
        if let Some(fields) = browser_show_stub_fields(&query_upper) {
            log::info!(
                "Stubbing {} with empty result set",
                query_upper
                    .split_whitespace()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(" ")
            );

            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(vec![]);

            let mut result_metadata = HashMap::new();
            result_metadata.insert("fields".to_string(), serde_json::json!(fields));
            result_metadata.insert("result_consumed_after".to_string(), serde_json::json!(-1));
            return Ok(vec![BoltMessage::success(result_metadata)]);
        }

        // Intercept Browser 5.x's bundled count query. Older Browsers issued two
        // separate queries that already work — those still flow through normally;
        // only the UNION ALL form is short-circuited because our SQL generator
        // can't handle UNION ALL of two count(*) projections over disjoint sets.
        if is_browser_count_union(&query_upper) {
            log::info!("Detected Browser count(n) UNION ALL count(r) query — short-circuiting");

            let effective_schema = schema_name.as_deref().unwrap_or("default").to_string();
            let schema_guard = crate::server::GLOBAL_SCHEMAS
                .get()
                .ok_or_else(|| BoltError::internal("Schema registry not initialized"))?;
            let schemas = schema_guard.read().await;
            let schema = schemas
                .get(&effective_schema)
                .ok_or_else(|| BoltError::internal("Schema not found"))?;

            // Distinct underlying tables — polymorphic schemas can map several labels
            // (or rel composite keys) onto a single physical table; counting that
            // table once is correct.
            let node_tables: std::collections::BTreeSet<String> = schema
                .all_node_schemas()
                .values()
                .map(|n| n.full_table_name())
                .collect();
            let rel_tables: std::collections::BTreeSet<String> = schema
                .get_relationships_schemas()
                .values()
                .map(|r| r.full_table_name())
                .collect();
            drop(schemas);

            let count_branch = |tables: &std::collections::BTreeSet<String>| -> String {
                if tables.is_empty() {
                    "SELECT toUInt64(0) AS result".to_string()
                } else {
                    let inner: Vec<String> = tables
                        .iter()
                        .map(|t| format!("SELECT count(*) AS c FROM {}", t))
                        .collect();
                    format!(
                        "SELECT sum(c) AS result FROM ({})",
                        inner.join(" UNION ALL ")
                    )
                }
            };
            let combined_sql = format!(
                "{} UNION ALL {}",
                count_branch(&node_tables),
                count_branch(&rel_tables),
            );
            log::debug!("Browser count UNION SQL: {}", combined_sql);

            match self
                .executor
                .execute_json(&combined_sql, role.as_deref())
                .await
            {
                Ok(rows) => {
                    let bolt_rows: Vec<Vec<BoltValue>> = rows
                        .into_iter()
                        .map(|row| {
                            let val = row
                                .get("result")
                                .cloned()
                                .unwrap_or(serde_json::Value::Number(0.into()));
                            vec![BoltValue::Json(val)]
                        })
                        .collect();

                    {
                        let mut context = lock_context!(self.context);
                        context.set_state(ConnectionState::Streaming);
                    }
                    self.cached_results = Some(bolt_rows);

                    let mut result_metadata = HashMap::new();
                    result_metadata.insert("fields".to_string(), serde_json::json!(["result"]));
                    result_metadata
                        .insert("result_consumed_after".to_string(), serde_json::json!(-1));
                    return Ok(vec![BoltMessage::success(result_metadata)]);
                }
                Err(e) => {
                    log::error!("Browser count UNION execution failed: {}", e);
                    return Ok(vec![BoltMessage::failure(
                        "Neo.ClientError.Statement.ExecutionFailed".to_string(),
                        format!("count(n)/count(r) execution failed: {}", e),
                    )]);
                }
            }
        }

        // Intercept Browser 5.x's bundled metadata query
        // (db.labels + db.relationshipTypes + db.propertyKeys, each wrapped in a
        // COLLECT(...)[..$itemLimit] slice). The slice in RETURN isn't supported
        // by our procedure RETURN evaluator. Old Browsers' separate CALLs flow
        // through the normal procedure executor unchanged.
        if is_browser_labels_bundle(&query_upper) {
            log::info!("Detected Browser labels/types/propertyKeys bundle — short-circuiting");

            let effective_schema = schema_name.as_deref().unwrap_or("default").to_string();
            let schema_guard = crate::server::GLOBAL_SCHEMAS
                .get()
                .ok_or_else(|| BoltError::internal("Schema registry not initialized"))?;
            let schemas = schema_guard.read().await;
            let schema = schemas
                .get(&effective_schema)
                .ok_or_else(|| BoltError::internal("Schema not found"))?;

            let collect_column = |records: Vec<HashMap<String, serde_json::Value>>,
                                  key: &str|
             -> Vec<serde_json::Value> {
                records
                    .into_iter()
                    .filter_map(|r| r.get(key).cloned())
                    .collect()
            };

            let labels = collect_column(
                crate::procedures::db_labels::execute(schema).map_err(BoltError::query_error)?,
                "label",
            );
            let rel_types = collect_column(
                crate::procedures::db_relationship_types::execute(schema)
                    .map_err(BoltError::query_error)?,
                "relationshipType",
            );
            let prop_keys = collect_column(
                crate::procedures::db_property_keys::execute(schema)
                    .map_err(BoltError::query_error)?,
                "propertyKey",
            );
            drop(schemas);

            // `$itemLimit` was substituted into the query text by
            // `substitute_cypher_parameters`, but the parameters map is still
            // intact — read it directly. Default to no slice if absent.
            let limit = parameters
                .get("itemLimit")
                .and_then(|v| v.as_i64())
                .filter(|n| *n >= 0)
                .map(|n| n as usize)
                .unwrap_or(usize::MAX);
            let take = |mut v: Vec<serde_json::Value>| -> serde_json::Value {
                v.truncate(limit);
                serde_json::Value::Array(v)
            };

            let bolt_rows: Vec<Vec<BoltValue>> = vec![
                vec![BoltValue::Json(take(labels))],
                vec![BoltValue::Json(take(rel_types))],
                vec![BoltValue::Json(take(prop_keys))],
            ];

            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(bolt_rows);

            let mut result_metadata = HashMap::new();
            result_metadata.insert("fields".to_string(), serde_json::json!(["result"]));
            result_metadata.insert("result_consumed_after".to_string(), serde_json::json!(-1));
            return Ok(vec![BoltMessage::success(result_metadata)]);
        }

        // Intercept the MCP server's apoc.meta.schema UNWIND query pattern.
        // Only the UNWIND variant needs interception — the procedure executor cannot
        // handle UNWIND + map indexing + map projection. Simple CALL apoc.meta.schema()
        // falls through to the normal procedure-only execution pipeline below.
        if query_upper.contains("APOC.META.SCHEMA") && query_upper.contains("UNWIND") {
            log::info!(
                "Detected apoc.meta.schema UNWIND query — short-circuiting for MCP compatibility"
            );

            let effective_schema = schema_name.as_deref().unwrap_or("default").to_string();

            let schema_guard = crate::server::GLOBAL_SCHEMAS
                .get()
                .ok_or_else(|| BoltError::internal("Schema registry not initialized"))?;
            let schemas = schema_guard.read().await;
            let schema = schemas
                .get(&effective_schema)
                .ok_or_else(|| BoltError::internal("Schema not found"))?;

            match crate::procedures::apoc_meta_schema::execute_unwound(schema) {
                Ok(records) => {
                    // The MCP UNWIND query always produces columns "key" and "value",
                    // even when the schema is empty (zero rows).
                    let fields = vec!["key".to_string(), "value".to_string()];

                    // Convert records to Bolt rows
                    let bolt_rows: Vec<Vec<BoltValue>> = records
                        .iter()
                        .map(|record| {
                            fields
                                .iter()
                                .map(|f| {
                                    BoltValue::Json(
                                        record.get(f).cloned().unwrap_or(serde_json::Value::Null),
                                    )
                                })
                                .collect()
                        })
                        .collect();

                    // Update context to streaming state
                    {
                        let mut context = lock_context!(self.context);
                        context.set_state(ConnectionState::Streaming);
                    }

                    self.cached_results = Some(bolt_rows);

                    let mut result_metadata = HashMap::new();
                    result_metadata.insert("fields".to_string(), serde_json::json!(fields));
                    result_metadata
                        .insert("result_consumed_after".to_string(), serde_json::json!(-1));

                    return Ok(vec![BoltMessage::success(result_metadata)]);
                }
                Err(e) => {
                    log::error!("apoc.meta.schema execution failed: {}", e);
                    return Ok(vec![BoltMessage::failure(
                        "Neo.ClientError.Procedure.ProcedureCallFailed".to_string(),
                        e,
                    )]);
                }
            }
        }

        // Parse and execute the query with task-local schema context
        // Note: id() predicates with encoded values are decoded in FilterTagging pass
        use crate::server::query_context::{with_query_context, QueryContext};
        let ctx = QueryContext::new(schema_name.clone());

        // Observability: the Bolt path doesn't build per-phase timings like the
        // HTTP handler, so record only total/exec latency under a coarse "bolt"
        // type. The in-flight guard + CH-stats scope mirror the HTTP path.
        let _inflight = GLOBAL_SERVER_METRICS.get().map(|r| r.in_flight_guard());
        let run_start = std::time::Instant::now();
        let exec_result = metrics::with_ch_stats_scope(with_query_context(
            ctx,
            self.execute_cypher_query(
                &query,
                parameters,
                schema_name,
                tenant_id,
                role,
                view_parameters,
            ),
        ))
        .await;

        let (messages, outcome) = match exec_result {
            Ok(result_metadata) => {
                // Update context to streaming state
                {
                    let mut context = lock_context!(self.context);
                    context.set_state(ConnectionState::Streaming);
                }

                // Return success with query metadata
                (vec![BoltMessage::success(result_metadata)], Outcome::Ok)
            }
            Err(query_error) => {
                let error_code = query_error.error_code().to_string();
                let error_message = query_error.to_string();
                log::error!("Query execution failed: {}", query_error);
                log::error!(
                    "Sending FAILURE: code='{}', message='{}'",
                    error_code,
                    error_message
                );

                // Don't update state - let client send RESET to recover
                // Setting to Failed would close the connection

                (
                    vec![BoltMessage::failure(error_code, error_message)],
                    Outcome::Err(ErrorClass::Exec),
                )
            }
        };

        if let Some(reg) = GLOBAL_SERVER_METRICS.get() {
            let elapsed = run_start.elapsed().as_secs_f64();
            let m = QueryPerformanceMetrics {
                total_time: elapsed,
                execution_time: elapsed,
                query_type: "bolt".to_string(),
                ..QueryPerformanceMetrics::default()
            };
            reg.record_query(&QuerySample {
                metrics: &m,
                outcome,
                has_phase_breakdown: false,
                query_text: Some(&query),
                ch: metrics::current_ch_stats(),
            });
        }

        Ok(messages)
    }

    /// Handle PULL message (fetch query results)
    async fn handle_pull(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !matches!(context.state, ConnectionState::Streaming) {
                // If we're not streaming, a FAILURE was likely already sent
                // Return IGNORED instead of sending another FAILURE
                log::debug!("PULL received in non-streaming state, returning IGNORED");
                return Ok(vec![BoltMessage::ignored()]);
            }
        }

        // Stream the cached results as RECORD messages
        let mut messages = Vec::new();

        if let Some(rows) = self.cached_results.take() {
            log::debug!("Streaming {} rows via Bolt RECORD messages", rows.len());

            // Send each row as a RECORD message
            for row in rows {
                // Row is already Vec<BoltValue> - pass directly
                messages.push(BoltMessage::record(row));
            }
        }

        // Send SUCCESS with completion metadata
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));
        metadata.insert("has_more".to_string(), Value::Bool(false));
        metadata.insert("t_last".to_string(), Value::Number(0.into()));

        messages.push(BoltMessage::success(metadata));

        // Update context back to ready state
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
        }

        Ok(messages)
    }

    /// Handle DISCARD message (discard query results)
    async fn handle_discard(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !matches!(context.state, ConnectionState::Streaming) {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "DISCARD message received in invalid state".to_string(),
                )]);
            }
        }

        log::debug!("Discarding query results");

        // Update context back to ready state
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
        }

        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));

        Ok(vec![BoltMessage::success(metadata)])
    }

    /// Handle BEGIN message (start transaction)
    async fn handle_begin(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "BEGIN message received in invalid state".to_string(),
                )]);
            }
        }

        // Extract database from BEGIN message extra field (Bolt 4.0+)
        log::debug!("BEGIN message has {} fields", message.fields.len());
        if !message.fields.is_empty() {
            log::debug!(
                "BEGIN Field[0]: {}",
                bolt_value_to_string(&message.fields[0])
            );
        }

        if let Some(db) = message.extract_begin_database() {
            log::info!("✅ BEGIN message contains database: {}", db);
            let mut context = lock_context!(self.context);
            if context.schema_name.as_deref() != Some(&db) {
                log::debug!(
                    "BEGIN message overriding schema: {:?} -> {}",
                    context.schema_name,
                    db
                );
                context.schema_name = Some(db.clone());
                let scope_tenant = context.tenant_id.clone();
                context.id_mapper.set_scope(Some(db), scope_tenant);
            }
        } else {
            log::debug!("BEGIN message does NOT contain database field");
        }

        // Generate transaction ID
        let tx_id = format!("tx-{}", chrono::Utc::now().timestamp_millis());

        // Update context with transaction
        {
            let mut context = lock_context!(self.context);
            context.tx_id = Some(tx_id.clone());
        }

        log::info!("Started transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle COMMIT message (commit transaction)
    async fn handle_commit(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify we're in a transaction
        let tx_id = {
            let mut context = lock_context!(self.context);
            if let Some(tx_id) = context.tx_id.take() {
                tx_id
            } else {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Transaction.TransactionNotFound".to_string(),
                    "No active transaction to commit".to_string(),
                )]);
            }
        };

        log::info!("Committed transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle ROLLBACK message (rollback transaction)
    async fn handle_rollback(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify we're in a transaction
        let tx_id = {
            let mut context = lock_context!(self.context);
            if let Some(tx_id) = context.tx_id.take() {
                tx_id
            } else {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Transaction.TransactionNotFound".to_string(),
                    "No active transaction to rollback".to_string(),
                )]);
            }
        };

        log::info!("Rolled back transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle ROUTE message (return routing table for database)
    /// ROUTE message format: ROUTE {routing_context} [bookmarks] {extra}
    /// where extra can contain {"db": "database_name"}
    async fn handle_route(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("ROUTE message received");

        // Extract database from ROUTE message (field 2 - extra metadata)
        let database = if message.fields.len() >= 3 {
            if let BoltValue::Json(Value::Object(extra_map)) = &message.fields[2] {
                extra_map
                    .get("db")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            }
        } else {
            None
        };

        let db_name = database.unwrap_or_else(|| "default".to_string());
        log::info!("ROUTE request for database: {}", db_name);

        // Verify schema exists
        let schema_exists = if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
            if let Ok(schemas) = schemas_lock.try_read() {
                schemas.contains_key(&db_name)
            } else {
                false
            }
        } else {
            false
        };

        if !schema_exists {
            log::warn!("ROUTE requested for non-existent database: {}", db_name);
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Database.DatabaseNotFound".to_string(),
                format!("Database '{}' not found", db_name),
            )]);
        }

        // Build routing table response
        // For ClickGraph (single server, no cluster), we return ourselves for all roles
        let server_address = format!("{}:{}", self.config.host, self.config.port);

        let mut routing_table = serde_json::Map::new();
        routing_table.insert("ttl".to_string(), Value::Number(300.into())); // 5 minutes TTL
        routing_table.insert("db".to_string(), Value::String(db_name));

        // Servers list: we are WRITE, READ, and ROUTE all in one
        let servers = serde_json::json!([
            {
                "role": "WRITE",
                "addresses": [server_address.clone()]
            },
            {
                "role": "READ",
                "addresses": [server_address.clone()]
            },
            {
                "role": "ROUTE",
                "addresses": [server_address]
            }
        ]);

        routing_table.insert("servers".to_string(), servers);

        // Return SUCCESS with routing table
        let mut metadata = HashMap::new();
        metadata.insert("rt".to_string(), Value::Object(routing_table));

        log::info!(
            "✅ Returning routing table for database: {}",
            metadata
                .get("rt")
                .and_then(|v| v.get("db"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        );

        Ok(vec![BoltMessage::success(metadata)])
    }

    /// Execute a Cypher query and return result metadata
    async fn execute_cypher_query(
        &mut self,
        query: &str,
        parameters: HashMap<String, Value>,
        schema_name: Option<String>,
        tenant_id: Option<String>,
        role: Option<String>,
        view_parameters: Option<HashMap<String, String>>,
    ) -> BoltResult<HashMap<String, Value>> {
        use crate::open_cypher_parser::ast::CypherStatement;

        // Strip comments once, up front: #516 made parse_cypher_statement
        // all-consuming, so every parse_cypher_statement(query) call below
        // (and the nested extract_copy_to_params helper, which is also
        // called with this same shadowed `query`) must see comment-free
        // input, or a legitimate trailing `//` / `/* */` comment in a Bolt
        // RUN message becomes a hard parse error for every Bolt client
        // (Neo4j Browser, drivers, MCP). Comments are semantically inert —
        // strip_comments() preserves string-literal contents and
        // relationship-pattern arrows byte-for-byte — so rebinding `query`
        // here keeps every downstream regex/position-based helper in this
        // function internally self-consistent.
        let stripped_query = open_cypher_parser::strip_comments(query);
        let query: &str = &stripped_query;

        // ============================================================
        // PHASE 1: Determine Schema (for id() transformation)
        // ============================================================

        // Parse once to extract schema name
        let effective_schema = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => match stmt {
                CypherStatement::Query { query, .. } => {
                    if let Some(use_clause) = query.use_clause {
                        use_clause.database_name.to_string()
                    } else {
                        schema_name.as_deref().unwrap_or("default").to_string()
                    }
                }
                CypherStatement::ProcedureCall(_) | CypherStatement::CopyTo(_) => {
                    schema_name.as_deref().unwrap_or("default").to_string()
                }
            },
            Err(_) => schema_name.as_deref().unwrap_or("default").to_string(),
        };

        // Load the actual GraphSchema object for id() transformation.
        // Set schema name in task-local context so downstream code can use it.
        crate::server::query_context::set_current_schema_name(Some(effective_schema.clone()));

        // Update IdMapper scope using effective schema (may differ from connection
        // schema if query contains a USE clause)
        {
            let mut context = lock_context!(self.context);
            let scope_tenant = context.tenant_id.clone();
            context
                .id_mapper
                .set_scope(Some(effective_schema.clone()), scope_tenant);
        }

        let graph_schema = if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
            if let Ok(schemas) = schemas_lock.try_read() {
                schemas.get(&effective_schema).cloned()
            } else {
                None
            }
        } else {
            None
        };

        if graph_schema.is_some() {
            log::info!(
                "✅ Loaded schema '{}' for id() transformation",
                effective_schema
            );
        } else {
            log::warn!(
                "⚠️  Schema '{}' not found for id() transformation",
                effective_schema
            );
        }

        // ============================================================
        // PHASE 2: Parse and Transform (synchronous, single pass)
        // ============================================================

        // ── COPY TO: intercept before general parse ──
        // Extract all owned data in a sync helper so non-Send CopyToStatement
        // is guaranteed dropped before any .await.
        fn extract_copy_to_params(
            query: &str,
        ) -> Result<
            Option<(
                String,
                String,
                &'static str,
                crate::procedures::apoc_export::ExportConfig,
            )>,
            BoltError,
        > {
            let query_upper = query.trim().to_uppercase();
            if !query_upper.starts_with("COPY") {
                return Ok(None);
            }
            let (_, stmt) = match open_cypher_parser::parse_cypher_statement(query) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(None),
            };
            let copy_stmt = match stmt {
                CypherStatement::CopyTo(c) => c,
                _ => return Ok(None),
            };
            let inner_query = copy_stmt.query.to_string();
            let destination = copy_stmt.destination.to_string();
            let ch_format = if let Some(fmt) = copy_stmt.format {
                crate::procedures::apoc_export::format_from_copy_format(fmt)
                    .map_err(BoltError::query_error)?
            } else {
                crate::procedures::apoc_export::format_from_extension(&destination).ok_or_else(
                    || {
                        BoltError::query_error(format!(
                            "Cannot determine format from '{}'. Use FORMAT clause.",
                            destination
                        ))
                    },
                )?
            };
            let config =
                crate::procedures::apoc_export::ExportConfig::from_copy_options(&copy_stmt.options);
            Ok(Some((inner_query, destination, ch_format, config)))
        }

        if let Some((inner_query, destination, ch_format, config)) = extract_copy_to_params(query)?
        {
            log::info!("Bolt COPY TO: destination={}", destination);

            let graph_schema_obj = graph_catalog::get_graph_schema_by_name(&effective_schema)
                .await
                .map_err(BoltError::query_error)?;
            crate::server::query_context::set_current_schema(Arc::new(graph_schema_obj.clone()));

            // Translate inner Cypher → SQL
            let inner_sql = {
                let (_, inner_stmt) = open_cypher_parser::parse_cypher_statement(&inner_query)
                    .map_err(|e| {
                        BoltError::query_error(format!("Inner Cypher parse error: {}", e))
                    })?;

                let inner_mapper = crate::server::bolt_protocol::id_mapper::IdMapper::new();
                let inner_arena = crate::query_planner::ast_transform::StringArena::new();
                let (inner_cypher, _) = crate::query_planner::ast_transform::transform_id_functions(
                    &inner_arena,
                    inner_stmt,
                    &inner_mapper,
                    Some(&graph_schema_obj),
                );

                crate::query_planner::logical_plan::reset_all_counters();
                let (logical_plan, plan_ctx) = query_planner::evaluate_read_statement(
                    inner_cypher,
                    &graph_schema_obj,
                    None,
                    None,
                    None,
                )
                .map_err(|e| {
                    BoltError::query_error(format!("Inner Cypher planning error: {}", e))
                })?;

                let render_plan = logical_plan
                    .to_render_plan_with_ctx(&graph_schema_obj, Some(&plan_ctx), None)
                    .map_err(|e| {
                        BoltError::query_error(format!("Inner Cypher render error: {}", e))
                    })?;

                clickhouse_query_generator::generate_sql(render_plan, 1000)
            };

            let export_sql = crate::procedures::apoc_export::build_export_sql(
                &inner_sql,
                &destination,
                ch_format,
                &config,
            )
            .map_err(BoltError::query_error)?;

            log::debug!("Bolt COPY TO SQL: {}", export_sql);

            self.executor
                .execute_text(&export_sql, "TabSeparated", role.as_deref())
                .await
                .map_err(|e| BoltError::query_error(format!("COPY TO execution failed: {}", e)))?;

            // Cache a single result record for PULL
            let result_record = vec![
                BoltValue::Json(serde_json::json!(destination)),
                BoltValue::Json(serde_json::json!(ch_format)),
                BoltValue::Json(serde_json::json!(inner_query)),
            ];
            self.cached_results = Some(vec![result_record]);

            // Return metadata
            let mut metadata = HashMap::new();
            metadata.insert(
                "fields".to_string(),
                Value::Array(vec![
                    Value::String("file".to_string()),
                    Value::String("format".to_string()),
                    Value::String("source".to_string()),
                ]),
            );
            metadata.insert("t_first".to_string(), Value::Number(0.into()));
            return Ok(metadata);
        }

        // Parse Cypher statement for transformation
        let parsed_stmt = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => stmt,
            Err(parse_error) => {
                return Err(BoltError::query_error(format!(
                    "Statement parsing failed: {}",
                    parse_error
                )));
            }
        };

        // Transform id() functions using IdMapper (AST-level transformation)
        // Clone IdMapper snapshot for transformation (read-only access)
        let id_mapper_snapshot = {
            let context = lock_context!(self.context);
            context.id_mapper.clone()
        };

        // Parse and transform in a limited scope to extract metadata
        let ast_arena = crate::query_planner::ast_transform::StringArena::new();
        let (is_procedure, _is_union, exec_plan, query_type, _label_constraints) = {
            // Transform id() functions with schema available
            let (transformed_stmt, label_constraints) =
                crate::query_planner::ast_transform::transform_id_functions(
                    &ast_arena,
                    parsed_stmt,
                    &id_mapper_snapshot,
                    graph_schema.as_ref(),
                );

            let is_procedure = crate::procedures::is_procedure_only_statement(&transformed_stmt);
            let is_union = crate::procedures::is_procedure_union_query(&transformed_stmt);

            log::debug!("Query execution using schema: {}", effective_schema);
            log::debug!(
                "Routing decision: is_procedure={}, is_union={}",
                is_procedure,
                is_union
            );

            // Extract execution plan for procedures
            let exec_plan = if is_procedure {
                Some(match &transformed_stmt {
                    CypherStatement::ProcedureCall(proc_call) => ExecutionPlan::SimpleProcedure {
                        proc_name: proc_call.procedure_name.to_string(),
                    },
                    CypherStatement::CopyTo(_) => {
                        // COPY TO is handled by the early intercept above;
                        // this branch is unreachable for valid COPY TO queries.
                        return Err(BoltError::query_error(
                            "Unexpected COPY TO in execution plan phase".to_string(),
                        ));
                    }
                    CypherStatement::Query {
                        query: query_ast,
                        union_clauses,
                    } => {
                        if !union_clauses.is_empty() {
                            // Extract branch metadata
                            let mut branches = Vec::new();

                            // Main branch
                            if let Some(call_clause) = &query_ast.call_clause {
                                branches.push(ProcedureBranch {
                                    proc_name: call_clause.procedure_name.to_string(),
                                    has_return: query_ast.return_clause.is_some(),
                                });
                            }

                            // Union branches
                            for union_clause in union_clauses {
                                if let Some(call_clause) = &union_clause.query.call_clause {
                                    branches.push(ProcedureBranch {
                                        proc_name: call_clause.procedure_name.to_string(),
                                        has_return: union_clause.query.return_clause.is_some(),
                                    });
                                }
                            }

                            ExecutionPlan::Union { branches }
                        } else {
                            // Single procedure with possible RETURN
                            if let Some(call_clause) = &query_ast.call_clause {
                                if query_ast.return_clause.is_some() {
                                    ExecutionPlan::ProcedureWithReturn {
                                        proc_name: call_clause.procedure_name.to_string(),
                                    }
                                } else {
                                    ExecutionPlan::SimpleProcedure {
                                        proc_name: call_clause.procedure_name.to_string(),
                                    }
                                }
                            } else {
                                return Err(BoltError::query_error(
                                    "No call clause found".to_string(),
                                ));
                            }
                        }
                    }
                })
            } else {
                None
            };

            // Extract query type for regular queries
            let query_type = match &transformed_stmt {
                CypherStatement::Query {
                    query,
                    union_clauses,
                } => {
                    // Check main query type
                    let main_type = query_planner::get_query_type(query);

                    // For UNION queries, all branches must be Read queries
                    if !union_clauses.is_empty() {
                        // Check each union branch
                        for union_clause in union_clauses {
                            let branch_type = query_planner::get_query_type(&union_clause.query);
                            if branch_type != query_planner::types::QueryType::Read {
                                log::debug!("UNION branch has non-Read type: {:?}", branch_type);
                                return Err(BoltError::query_error(
                                    "Only read queries are currently supported via Bolt protocol"
                                        .to_string(),
                                ));
                            }
                        }
                    }

                    main_type
                }
                CypherStatement::ProcedureCall(_) => {
                    // Procedures are handled above, this shouldn't happen
                    query_planner::types::QueryType::Read // dummy value
                }
                CypherStatement::CopyTo(_) => {
                    // COPY TO is handled by the early intercept above
                    return Err(BoltError::query_error(
                        "Unexpected COPY TO in query type phase".to_string(),
                    ));
                }
            };

            // transformed_stmt is dropped here at end of scope!
            (
                is_procedure,
                is_union,
                exec_plan,
                query_type,
                label_constraints,
            )
        };

        // ============================================================
        // PHASE 3: Route to Procedure or Regular Query Handler
        // ============================================================

        // Handle procedure-only queries (including UNION)
        if is_procedure {
            let exec_plan = exec_plan.expect("exec_plan must be Some when is_procedure=true");

            // Now execute based on exec_plan (no AST references remain)
            let registry = crate::procedures::ProcedureRegistry::new();

            let results = match exec_plan {
                ExecutionPlan::SimpleProcedure { proc_name } => {
                    // ── Export procedures: apoc.export.{csv|json|parquet}.query() ──
                    if crate::procedures::apoc_export::is_export_procedure(&proc_name) {
                        log::info!("Executing export procedure via Bolt: {}", proc_name);

                        let ch_format =
                            crate::procedures::apoc_export::format_from_procedure_name(&proc_name)
                                .map_err(BoltError::query_error)?;

                        // Re-parse to extract arguments
                        let export_args = {
                            let (_, stmt) = open_cypher_parser::parse_cypher_statement(query)
                                .map_err(|e| {
                                    BoltError::query_error(format!("Export parse error: {}", e))
                                })?;
                            let expressions: Vec<_> = match &stmt {
                                CypherStatement::ProcedureCall(pc) => pc.arguments.iter().collect(),
                                CypherStatement::Query { query: q, .. } => {
                                    let cc = q.call_clause.as_ref().ok_or_else(|| {
                                        BoltError::query_error(
                                            "No CALL clause in export query".to_string(),
                                        )
                                    })?;
                                    cc.arguments.iter().map(|a| &a.value).collect()
                                }
                                CypherStatement::CopyTo(_) => {
                                    // COPY TO is handled by the early intercept above
                                    return Err(BoltError::query_error(
                                        "Unexpected COPY TO in export args extraction".to_string(),
                                    ));
                                }
                            };
                            crate::procedures::apoc_export::parse_export_call(&expressions)
                                .map_err(BoltError::query_error)?
                        };

                        // Resolve schema
                        let graph_schema =
                            graph_catalog::get_graph_schema_by_name(&effective_schema)
                                .await
                                .map_err(BoltError::query_error)?;
                        crate::server::query_context::set_current_schema(Arc::new(
                            graph_schema.clone(),
                        ));

                        // Translate inner Cypher → SQL
                        let inner_sql = {
                            // `cypher_query` is a Cypher string-literal ARGUMENT to
                            // apoc.export.*.query(...) — a separate string from the
                            // outer `query` already stripped above, so it needs its
                            // own strip_comments() before this independent parse.
                            let stripped_inner =
                                open_cypher_parser::strip_comments(&export_args.cypher_query);
                            let (_, inner_stmt) =
                                open_cypher_parser::parse_cypher_statement(&stripped_inner)
                                    .map_err(|e| {
                                        BoltError::query_error(format!(
                                            "Inner Cypher parse error: {}",
                                            e
                                        ))
                                    })?;

                            use crate::server::bolt_protocol::id_mapper::IdMapper;
                            let inner_mapper = IdMapper::new();
                            let inner_arena =
                                crate::query_planner::ast_transform::StringArena::new();
                            let (inner_cypher, _) =
                                crate::query_planner::ast_transform::transform_id_functions(
                                    &inner_arena,
                                    inner_stmt,
                                    &inner_mapper,
                                    Some(&graph_schema),
                                );

                            crate::query_planner::logical_plan::reset_all_counters();
                            let (logical_plan, plan_ctx) = query_planner::evaluate_read_statement(
                                inner_cypher,
                                &graph_schema,
                                None,
                                None,
                                None,
                            )
                            .map_err(|e| {
                                BoltError::query_error(format!(
                                    "Inner Cypher planning error: {}",
                                    e
                                ))
                            })?;

                            let render_plan = logical_plan
                                .to_render_plan_with_ctx(&graph_schema, Some(&plan_ctx), None)
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "Inner Cypher render error: {}",
                                        e
                                    ))
                                })?;

                            let max_cte_depth = 1000;
                            clickhouse_query_generator::generate_sql(render_plan, max_cte_depth)
                        };

                        // Build export SQL
                        let export_sql = crate::procedures::apoc_export::build_export_sql(
                            &inner_sql,
                            &export_args.destination,
                            ch_format,
                            &export_args.config,
                        )
                        .map_err(BoltError::query_error)?;

                        log::info!("Bolt export SQL: {}", export_sql);

                        // Execute
                        self.executor
                            .execute_text(&export_sql, "TabSeparated", role.as_deref())
                            .await
                            .map_err(|e| {
                                BoltError::query_error(format!("Export execution failed: {}", e))
                            })?;

                        // Return status as a single record
                        vec![std::collections::HashMap::from([
                            (
                                "file".to_string(),
                                serde_json::json!(export_args.destination),
                            ),
                            ("format".to_string(), serde_json::json!(ch_format)),
                            (
                                "source".to_string(),
                                serde_json::json!(export_args.cypher_query),
                            ),
                        ])]
                    } else if crate::procedures::vector_search::is_vector_search_procedure(
                        &proc_name,
                    ) {
                        // ── Vector search: db.index.vector.queryNodes/queryRelationships ──
                        log::info!("Executing vector search via Bolt: {}", proc_name);

                        // Re-parse to extract arguments
                        let search_args = {
                            let (_, stmt) = open_cypher_parser::parse_cypher_statement(query)
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "Vector search parse error: {}",
                                        e
                                    ))
                                })?;
                            let expressions: Vec<_> = match &stmt {
                                CypherStatement::ProcedureCall(pc) => pc.arguments.iter().collect(),
                                CypherStatement::Query { query: q, .. } => {
                                    let cc = q.call_clause.as_ref().ok_or_else(|| {
                                        BoltError::query_error(
                                            "No CALL clause in vector search query".to_string(),
                                        )
                                    })?;
                                    cc.arguments.iter().map(|a| &a.value).collect()
                                }
                                CypherStatement::CopyTo(_) => {
                                    return Err(BoltError::query_error(
                                        "Unexpected COPY TO in vector search context".to_string(),
                                    ));
                                }
                            };
                            crate::procedures::vector_search::parse_vector_search_args(&expressions)
                                .map_err(BoltError::query_error)?
                        };

                        // Resolve schema and vector index
                        let graph_schema =
                            graph_catalog::get_graph_schema_by_name(&effective_schema)
                                .await
                                .map_err(BoltError::query_error)?;

                        let index_config = crate::procedures::vector_search::resolve_vector_index(
                            &graph_schema,
                            &search_args.index_name,
                        )
                        .map_err(BoltError::query_error)?;

                        // Generate and execute SQL
                        let search_sql = crate::procedures::vector_search::build_vector_search_sql(
                            &search_args,
                            index_config,
                        )
                        .map_err(BoltError::query_error)?;

                        log::debug!(
                            "Bolt vector search: index='{}', top_k={}",
                            search_args.index_name,
                            search_args.top_k
                        );

                        let result_text = self
                            .executor
                            .execute_text(&search_sql, "JSONEachRow", role.as_deref())
                            .await
                            .map_err(|e| {
                                BoltError::query_error(format!(
                                    "Vector search execution failed: {}",
                                    e
                                ))
                            })?;

                        // Parse JSONEachRow into Vec<HashMap>, failing on malformed rows
                        result_text
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .map(|line| {
                                serde_json::from_str::<std::collections::HashMap<String, Value>>(
                                    line,
                                )
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "Failed to parse JSONEachRow line: {}",
                                        e
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    } else if crate::procedures::fulltext_search::is_fulltext_search_procedure(
                        &proc_name,
                    ) {
                        // ── Fulltext search: db.index.fulltext.queryNodes ──
                        log::info!("Executing fulltext search via Bolt: {}", proc_name);

                        let search_args = {
                            let (_, stmt) = open_cypher_parser::parse_cypher_statement(query)
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "Fulltext search parse error: {}",
                                        e
                                    ))
                                })?;
                            let expressions: Vec<_> = match &stmt {
                                CypherStatement::ProcedureCall(pc) => pc.arguments.iter().collect(),
                                CypherStatement::Query { query: q, .. } => {
                                    let cc = q.call_clause.as_ref().ok_or_else(|| {
                                        BoltError::query_error(
                                            "No CALL clause in fulltext search query".to_string(),
                                        )
                                    })?;
                                    cc.arguments.iter().map(|a| &a.value).collect()
                                }
                                CypherStatement::CopyTo(_) => {
                                    return Err(BoltError::query_error(
                                        "Unexpected COPY TO in fulltext search context".to_string(),
                                    ));
                                }
                            };
                            crate::procedures::fulltext_search::parse_fulltext_search_args(
                                &expressions,
                            )
                            .map_err(BoltError::query_error)?
                        };

                        let graph_schema =
                            graph_catalog::get_graph_schema_by_name(&effective_schema)
                                .await
                                .map_err(BoltError::query_error)?;

                        let index_config =
                            crate::procedures::fulltext_search::resolve_fulltext_index(
                                &graph_schema,
                                &search_args.index_name,
                            )
                            .map_err(BoltError::query_error)?;

                        let search_sql =
                            crate::procedures::fulltext_search::build_fulltext_search_sql(
                                &search_args,
                                index_config,
                            );

                        log::debug!(
                            "Bolt fulltext search: index='{}', query='{}'",
                            search_args.index_name,
                            search_args.query_text.chars().take(50).collect::<String>()
                        );

                        let result_text = self
                            .executor
                            .execute_text(&search_sql, "JSONEachRow", role.as_deref())
                            .await
                            .map_err(|e| {
                                BoltError::query_error(format!(
                                    "Fulltext search execution failed: {}",
                                    e
                                ))
                            })?;

                        result_text
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .map(|line| {
                                serde_json::from_str::<std::collections::HashMap<String, Value>>(
                                    line,
                                )
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "Failed to parse JSONEachRow line: {}",
                                        e
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    } else {
                        log::info!("Executing simple procedure via Bolt: {}", proc_name);
                        crate::procedures::executor::execute_procedure_by_name(
                            &proc_name,
                            &effective_schema,
                            &registry,
                        )
                        .await
                        .map_err(|e| {
                            BoltError::query_error(format!("Procedure execution failed: {}", e))
                        })?
                    }
                }
                ExecutionPlan::ProcedureWithReturn { proc_name } => {
                    log::info!("Executing procedure with RETURN via Bolt: {}", proc_name);

                    // Execute procedure
                    let raw_results = crate::procedures::executor::execute_procedure_by_name(
                        &proc_name,
                        &effective_schema,
                        &registry,
                    )
                    .await
                    .map_err(BoltError::query_error)?;

                    // Parse original query to get RETURN clause (procedures don't have id() in RETURN)
                    let return_clause = match open_cypher_parser::parse_cypher_statement(query) {
                        Ok((_, CypherStatement::Query { query, .. })) => {
                            query.return_clause.ok_or_else(|| {
                                BoltError::query_error("Expected RETURN clause".to_string())
                            })?
                        }
                        _ => {
                            return Err(BoltError::query_error(
                                "Failed to parse RETURN clause".to_string(),
                            ))
                        }
                    };

                    // Apply RETURN clause
                    crate::procedures::return_evaluator::apply_return_clause(
                        raw_results,
                        &return_clause,
                    )
                    .map_err(|e| {
                        BoltError::query_error(format!("RETURN evaluation failed: {}", e))
                    })?
                }
                ExecutionPlan::Union { branches } => {
                    log::info!(
                        "Executing UNION of procedures via Bolt: {} branches",
                        branches.len()
                    );

                    let mut all_results = Vec::new();

                    // Execute each branch
                    for (idx, branch) in branches.iter().enumerate() {
                        let raw_results = crate::procedures::executor::execute_procedure_by_name(
                            &branch.proc_name,
                            &effective_schema,
                            &registry,
                        )
                        .await
                        .map_err(BoltError::query_error)?;

                        // Apply RETURN clause if branch has one
                        let transformed_results = if branch.has_return {
                            // Parse to get return clause for this branch (after await - safe)
                            let return_clause =
                                match open_cypher_parser::parse_cypher_statement(query) {
                                    Ok((
                                        _,
                                        CypherStatement::Query {
                                            query: main_q,
                                            union_clauses,
                                        },
                                    )) => {
                                        if idx == 0 {
                                            main_q.return_clause
                                        } else {
                                            union_clauses
                                                .get(idx - 1)
                                                .and_then(|uc| uc.query.return_clause.clone())
                                        }
                                    }
                                    _ => None,
                                };

                            if let Some(ref rc) = return_clause {
                                crate::procedures::return_evaluator::apply_return_clause(
                                    raw_results,
                                    rc,
                                )
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "RETURN evaluation failed: {}",
                                        e
                                    ))
                                })?
                            } else {
                                raw_results
                            }
                        } else {
                            raw_results
                        };

                        all_results.extend(transformed_results);
                    }

                    all_results
                }
            };

            // Convert to Bolt records
            let bolt_records: Vec<Vec<BoltValue>> = results
                .iter()
                .map(|record| {
                    let mut values: Vec<BoltValue> = Vec::new();
                    let mut keys: Vec<_> = record.keys().collect();
                    keys.sort();

                    for key in keys {
                        let json_value = &record[key];
                        values.push(BoltValue::Json(json_value.clone()));
                    }
                    values
                })
                .collect();

            // Cache results for PULL
            self.cached_results = Some(bolt_records);

            // Return metadata with field names
            let mut metadata = HashMap::new();
            if let Some(first_record) = results.first() {
                let mut keys: Vec<_> = first_record.keys().map(|k| k.to_string()).collect();
                keys.sort();

                metadata.insert(
                    "fields".to_string(),
                    Value::Array(keys.into_iter().map(Value::String).collect()),
                );
            } else {
                metadata.insert("fields".to_string(), Value::Array(vec![]));
            }
            metadata.insert("t_first".to_string(), Value::Number(0.into()));

            return Ok(metadata);
        }

        // Handle regular queries
        // Check query type
        if query_type != query_planner::types::QueryType::Read {
            return Err(BoltError::query_error(
                "Only read queries are currently supported via Bolt protocol".to_string(),
            ));
        }

        // Get graph schema (safe to await now - all Rc<RefCell<>> dropped)
        let graph_schema = match graph_catalog::get_graph_schema_by_name(&effective_schema).await {
            Ok(schema) => schema,
            Err(e) => {
                return Err(BoltError::query_error(format!("Schema error: {}", e)));
            }
        };

        // Set the resolved schema in task-local context so all downstream
        // code can access it via get_current_schema() without GLOBAL_SCHEMAS lookups
        crate::server::query_context::set_current_schema(std::sync::Arc::new(graph_schema.clone()));

        // S1 stats-informed planning: attach the current row-count snapshot
        // (no-op unless CLICKGRAPH_STATS_ENABLED installed the cache).
        crate::server::query_context::attach_current_table_stats(&graph_schema).await;

        // Re-parse and transform for planning (after async boundary)
        // Note: This is unavoidable due to Rc<RefCell<>> in AST not being Send
        let parsed_stmt_for_planning = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => stmt,
            Err(e) => {
                return Err(BoltError::query_error(format!("Re-parse failed: {}", e)));
            }
        };

        let id_mapper_snapshot = {
            let context = lock_context!(self.context);
            context.id_mapper.clone()
        };

        let (transformed_for_planning, label_constraints_from_second_pass) =
            crate::query_planner::ast_transform::transform_id_functions(
                &ast_arena, // Reuse same arena
                parsed_stmt_for_planning,
                &id_mapper_snapshot,
                Some(&graph_schema), // Pass schema for node_id property lookup
            );

        // Use label_constraints from the second pass (not first) since it has schema context
        log::info!(
            "🎯 Passing {} label constraints to query planner (from second pass)",
            label_constraints_from_second_pass.len()
        );

        // Reset global counters for deterministic SQL generation
        crate::query_planner::logical_plan::reset_all_counters();

        // Generate logical plan using transformed statement
        let (logical_plan, mut plan_ctx) = match query_planner::evaluate_read_statement(
            transformed_for_planning,
            &graph_schema,
            tenant_id,
            view_parameters,
            Some(20), // max_inferred_types - increased for UNION branches
        ) {
            Ok(result) => result,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Query planning failed: {}",
                    e
                )));
            }
        };

        // Inject label constraints into plan_ctx for UNION pruning
        if !label_constraints_from_second_pass.is_empty() {
            plan_ctx.set_where_label_constraints(label_constraints_from_second_pass);
        }

        // transformed_for_planning is now dropped

        // Extract return metadata for result transformation
        let return_metadata = match extract_return_metadata(&logical_plan, &plan_ctx) {
            Ok(metadata) => metadata,
            Err(e) => {
                log::warn!("Failed to extract return metadata: {}", e);
                Vec::new() // Fall back to no transformation
            }
        };
        let has_graph_objects = return_metadata.iter().any(|m| {
            matches!(
                m.item_type,
                super::result_transformer::ReturnItemType::Node { .. }
                    | super::result_transformer::ReturnItemType::Relationship { .. }
                    | super::result_transformer::ReturnItemType::Path { .. }
                    | super::result_transformer::ReturnItemType::IdFunction { .. }
            )
        });

        // Generate render plan - use _with_ctx to pass VLP endpoint information
        let render_plan =
            match logical_plan.to_render_plan_with_ctx(&graph_schema, Some(&plan_ctx), None) {
                Ok(plan) => plan,
                Err(e) => {
                    return Err(BoltError::query_error(format!(
                        "Render plan generation failed: {}",
                        e
                    )));
                }
            };

        // Generate ClickHouse SQL
        let max_cte_depth = 1000; // Use default from config
        let ch_sql = clickhouse_query_generator::generate_sql(render_plan, max_cte_depth);

        // Substitute parameters in SQL (for non-id() parameters like $name, $age, etc.)
        // Note: id() parameters were already handled in Cypher query substitution (line 741)
        let final_sql = match parameter_substitution::substitute_parameters(&ch_sql, &parameters) {
            Ok(sql) => sql,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Parameter substitution failed: {}",
                    e
                )));
            }
        };

        log::info!("📊 Executing SQL: {}", final_sql);

        // Execute the query using the backend-agnostic executor
        let rows_values = self
            .executor
            .execute_json(&final_sql, role.as_deref())
            .await
            .map_err(|e| BoltError::query_error(format!("Query execution failed: {}", e)))?;

        // Parse JSON results into field_names + row vectors
        let mut rows = Vec::new();
        let mut field_names = Vec::new();

        for value in rows_values {
            match value {
                Value::Object(obj) => {
                    if field_names.is_empty() {
                        field_names = obj.keys().cloned().collect();
                    }
                    let mut row_fields = Vec::new();
                    for field_name in &field_names {
                        row_fields.push(obj.get(field_name).cloned().unwrap_or(Value::Null));
                    }
                    rows.push(row_fields);
                }
                _ => {
                    log::warn!("Unexpected JSON format in result row");
                }
            }
        }

        // Transform results if we have graph objects (nodes, relationships, paths)
        if has_graph_objects {
            log::info!(
                "Transforming graph objects. Original field_names: {:?}, metadata items: {}",
                field_names,
                return_metadata.len()
            );

            let mut transformed_rows: Vec<Vec<BoltValue>> = Vec::new();

            // Get mutable access to id_mapper from context for session-scoped ID assignment
            let mut context = lock_context!(self.context);

            for row in &rows {
                // Convert row Vec back to HashMap for transformation
                let mut row_map = HashMap::new();
                for (i, field_name) in field_names.iter().enumerate() {
                    if let Some(value) = row.get(i) {
                        row_map.insert(field_name.clone(), value.clone());
                    }
                }

                match super::result_transformer::transform_row(
                    row_map,
                    &return_metadata,
                    &graph_schema,
                    &mut context.id_mapper,
                ) {
                    Ok(transformed) => {
                        log::debug!(
                            "Transformed row: {} fields → {} items",
                            field_names.len(),
                            transformed.len()
                        );
                        transformed_rows.push(transformed);
                    }
                    Err(e) => {
                        log::warn!("Failed to transform row to graph objects: {}", e);
                        // Fall back: produce one Null per metadata item to match field count
                        let fallback: Vec<BoltValue> = return_metadata
                            .iter()
                            .map(|_| BoltValue::Json(Value::Null))
                            .collect();
                        transformed_rows.push(fallback);
                    }
                }
            }

            // Release lock before caching
            drop(context);

            // Cache the transformed results
            self.cached_results = Some(transformed_rows);

            // Update field names to match transformed structure
            // Strip ".*" suffix for wildcard expansions (e.g., "a.*" → "a")
            field_names = return_metadata
                .iter()
                .map(|m| {
                    m.field_name
                        .strip_suffix(".*")
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| m.field_name.clone())
                })
                .collect();

            log::info!("After transformation: field_names: {:?}", field_names);
        } else {
            // No graph objects - wrap rows in BoltValue::Json and cache
            let wrapped_rows: Vec<Vec<BoltValue>> = rows
                .into_iter()
                .map(|row| row.into_iter().map(BoltValue::Json).collect())
                .collect();
            self.cached_results = Some(wrapped_rows);
        }

        // Return SUCCESS with metadata
        let mut metadata = HashMap::new();
        metadata.insert(
            "fields".to_string(),
            Value::Array(
                field_names
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
        metadata.insert("t_first".to_string(), Value::Number(0.into()));
        metadata.insert("qid".to_string(), Value::Number(1.into()));

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExecutorError, QueryExecutor};
    use async_trait::async_trait;
    use serde_json::Value;

    /// Minimal no-op executor for unit tests (never actually called).
    struct StubExecutor;

    #[async_trait]
    impl QueryExecutor for StubExecutor {
        async fn execute_json(
            &self,
            _sql: &str,
            _role: Option<&str>,
        ) -> Result<Vec<Value>, ExecutorError> {
            Ok(vec![])
        }
        async fn execute_text(
            &self,
            _sql: &str,
            _format: &str,
            _role: Option<&str>,
        ) -> Result<String, ExecutorError> {
            Ok(String::new())
        }
    }

    #[test]
    fn browser_count_union_pattern_matcher() {
        // Browser 5.x bundled form — should match
        let new = "MATCH (n) RETURN count(n) AS result UNION ALL MATCH ()-[r]->() RETURN count(r) AS result".to_uppercase();
        assert!(is_browser_count_union(&new));

        // Same with extra whitespace and trailing semicolon — should still match
        let messy = "  MATCH (n)\n  RETURN count(n) AS result\nUNION ALL\nMATCH ()-[r]->() RETURN count(r) AS result;".to_uppercase();
        assert!(is_browser_count_union(&messy));

        // Old-Browser separate query — must NOT match (flows through normal path)
        let old_node = "MATCH (n) RETURN count(n) AS result".to_uppercase();
        assert!(!is_browser_count_union(&old_node));
        let old_rel = "MATCH ()-[r]->() RETURN count(r) AS result".to_uppercase();
        assert!(!is_browser_count_union(&old_rel));

        // Unrelated user query — must NOT match
        let user = "MATCH (p:Person) RETURN p.name".to_uppercase();
        assert!(!is_browser_count_union(&user));
    }

    #[test]
    fn rewrites_browser_elementid_to_id() {
        let mut params: HashMap<String, Value> = HashMap::new();
        // Element IDs as Browser would send them (with the trailing `-` sentinel
        // our generators emit).
        params.insert(
            "nodeIds".to_string(),
            serde_json::json!(["Post:1-", "Post:2-"]),
        );
        params.insert("targetId".to_string(), serde_json::json!("User:5-"));
        params.insert(
            "existingRelationshipIds".to_string(),
            serde_json::json!(["FOLLOWS:1->4-", "AUTHORED:1->2-"]),
        );

        // IN with a list of node element_ids — rewrite to id() with two encoded ints.
        let q = "MATCH (a)-[r]-(o) WHERE elementId(a) IN $nodeIds RETURN r, o";
        let out = substitute_cypher_parameters(q, &params);
        assert!(out.contains("id(a) IN ["), "got: {}", out);
        assert!(!out.contains("elementId"), "got: {}", out);
        assert!(!out.contains("$nodeIds"), "got: {}", out);

        // = with a single string — should rewrite to id() = <int>
        let q = "MATCH (n) WHERE elementId(n) = $targetId RETURN n";
        let out = substitute_cypher_parameters(q, &params);
        assert!(out.contains("id(n) = "), "got: {}", out);
        assert!(!out.contains("elementId"), "got: {}", out);

        // Unknown parameter — leave untouched (avoid masking errors)
        let q = "MATCH (n) WHERE elementId(n) IN $unknown RETURN n";
        let out = substitute_cypher_parameters(q, &params);
        assert_eq!(out, q);

        // Relationship element_ids (contain `->`) inside a connected `AND NOT`
        // conjunct — drop the entire conjunct (including the leading `AND NOT`).
        let q = "MATCH (a)-[r]-(o) WHERE elementId(a) IN $nodeIds AND NOT elementId(r) IN $existingRelationshipIds RETURN r";
        let out = substitute_cypher_parameters(q, &params);
        assert!(!out.contains("elementId"), "got: {}", out);
        assert!(!out.contains("AND NOT"), "got: {}", out);
        // First predicate (node-shaped) still rewritten to id()
        assert!(out.contains("id(a) IN ["), "got: {}", out);

        // Relationship element_ids in a leading `WHERE NOT` — drop the conjunct.
        let q = "MATCH ()-[r]-() WHERE NOT elementId(r) IN $existingRelationshipIds RETURN r";
        let out = substitute_cypher_parameters(q, &params);
        assert!(!out.contains("elementId"), "got: {}", out);
        // The `WHERE NOT ...` should be entirely gone.
        assert!(!out.contains("WHERE NOT"), "got: {}", out);
    }

    #[test]
    fn collapses_browser_dedupe_case_pattern() {
        let params: HashMap<String, Value> = HashMap::new();

        // Browser's dedupe-CASE — should collapse to bare alias `o AS o`.
        let q = "RETURN r, CASE WHEN elementId(o) IN $existingNodeIds THEN null ELSE o END AS o";
        let out = substitute_cypher_parameters(q, &params);
        assert_eq!(out, "RETURN r, o AS o");

        // id() variant — also collapsed
        let q = "RETURN CASE WHEN id(x) IN $foo THEN null ELSE x END AS x";
        let out = substitute_cypher_parameters(q, &params);
        assert_eq!(out, "RETURN x AS x");

        // Different aliases (not Browser's pattern) — must NOT collapse
        let q = "RETURN CASE WHEN elementId(a) IN $foo THEN null ELSE b END AS c";
        let out = substitute_cypher_parameters(q, &params);
        assert!(out.contains("CASE"), "got: {}", out);

        // No AS clause — collapse to bare alias
        let q = "RETURN CASE WHEN elementId(o) IN $foo THEN null ELSE o END";
        let out = substitute_cypher_parameters(q, &params);
        assert_eq!(out, "RETURN o");
    }

    #[test]
    fn substitutes_limit_skip_integer_parameters() {
        let mut params: HashMap<String, Value> = HashMap::new();
        params.insert("maxNeighbours".to_string(), serde_json::json!(1000));
        params.insert("page".to_string(), serde_json::json!(20));
        params.insert("notAnInt".to_string(), serde_json::json!("oops"));

        // LIMIT with integer parameter — substitutes
        let q = "MATCH (n) RETURN n LIMIT $maxNeighbours";
        assert_eq!(
            substitute_cypher_parameters(q, &params),
            "MATCH (n) RETURN n LIMIT 1000"
        );

        // SKIP + LIMIT chained
        let q = "MATCH (n) RETURN n SKIP $page LIMIT $maxNeighbours";
        assert_eq!(
            substitute_cypher_parameters(q, &params),
            "MATCH (n) RETURN n SKIP 20 LIMIT 1000"
        );

        // Non-integer parameter — leave untouched, parser will surface the error
        let q = "MATCH (n) RETURN n LIMIT $notAnInt";
        assert_eq!(substitute_cypher_parameters(q, &params), q);

        // Missing parameter — leave untouched
        let q = "MATCH (n) RETURN n LIMIT $unknownParam";
        assert_eq!(substitute_cypher_parameters(q, &params), q);

        // Browser 5.x expand query — full integration: id() params + LIMIT
        let mut full_params: HashMap<String, Value> = HashMap::new();
        full_params.insert("nodeIds".to_string(), serde_json::json!([42]));
        full_params.insert("existingRelationshipIds".to_string(), serde_json::json!([]));
        full_params.insert("existingNodeIds".to_string(), serde_json::json!([1, 2, 3]));
        full_params.insert("maxNeighbours".to_string(), serde_json::json!(1000));
        let q =
            "MATCH (a)-[r]-(o) WHERE id(a) IN $nodeIds AND NOT id(r) IN $existingRelationshipIds \
                 RETURN r, CASE WHEN id(o) IN $existingNodeIds THEN null ELSE o END AS o \
                 LIMIT $maxNeighbours";
        let out = substitute_cypher_parameters(q, &full_params);
        assert!(out.contains("id(a) IN [42]"), "got: {}", out);
        assert!(out.contains("id(r) IN []"), "got: {}", out);
        assert!(out.contains("id(o) IN [1, 2, 3]"), "got: {}", out);
        assert!(out.ends_with("LIMIT 1000"), "got: {}", out);
        assert!(
            !out.contains("$"),
            "all params should be substituted, got: {}",
            out
        );
    }

    #[test]
    fn browser_show_stub_dispatches_each_command() {
        // Each shape Browser issues should resolve to a stubbed field schema.
        for q in [
            "SHOW INDEXES",
            "SHOW INDEXES YIELD *",
            "SHOW CONSTRAINTS;",
            "SHOW PROCEDURES",
            "SHOW FUNCTIONS",
            "SHOW BUILT IN FUNCTIONS",
            "SHOW CURRENT USER",
            "SHOW USERS",
            "SHOW ROLES",
            "SHOW PRIVILEGES",
            "SHOW SERVERS",
            "SHOW SETTINGS",
            "SHOW TRANSACTIONS",
        ] {
            assert!(
                browser_show_stub_fields(&q.to_uppercase()).is_some(),
                "expected stub fields for {:?}",
                q
            );
        }

        // SHOW DATABASES is handled by the dedicated block higher up — it must
        // NOT be stubbed here, otherwise it would shadow the real implementation.
        assert!(browser_show_stub_fields("SHOW DATABASES").is_none());

        // Unrelated user query — must NOT match.
        assert!(browser_show_stub_fields("MATCH (N) RETURN N").is_none());
    }

    #[test]
    fn browser_labels_bundle_pattern_matcher() {
        // Browser 5.x bundled form (post parameter substitution) — should match
        let bundled = "CALL db.labels() YIELD label RETURN COLLECT(label)[..1000] AS result \
            UNION ALL CALL db.relationshipTypes() YIELD relationshipType RETURN COLLECT(relationshipType)[..1000] AS result \
            UNION ALL CALL db.propertyKeys() YIELD propertyKey RETURN COLLECT(propertyKey)[..1000] AS result".to_uppercase();
        assert!(is_browser_labels_bundle(&bundled));

        // Old-Browser separate calls — must NOT match
        let old = "CALL db.labels()".to_uppercase();
        assert!(!is_browser_labels_bundle(&old));

        // Two of the three procedures — must NOT match (incomplete bundle)
        let two = "CALL db.labels() UNION ALL CALL db.relationshipTypes()".to_uppercase();
        assert!(!is_browser_labels_bundle(&two));

        // All three but no slice — must NOT match (different shape, falls through)
        let no_slice = "CALL db.labels() UNION ALL CALL db.relationshipTypes() UNION ALL CALL db.propertyKeys()".to_uppercase();
        assert!(!is_browser_labels_bundle(&no_slice));
    }

    fn create_test_handler() -> BoltHandler {
        let context = Arc::new(Mutex::new(BoltContext::new()));
        let config = Arc::new(BoltConfig::default());
        let executor: Arc<dyn QueryExecutor> = Arc::new(StubExecutor);
        BoltHandler::new(context, config, executor)
    }

    #[tokio::test]
    async fn test_hello_message_handling() {
        let mut handler = create_test_handler();

        // Set context to negotiated state
        {
            let mut context = handler.context.lock().unwrap();
            context.set_version(super::super::BOLT_VERSION_4_4);
        }

        let auth_token = HashMap::from([("scheme".to_string(), Value::String("none".to_string()))]);

        let hello = BoltMessage::hello("TestClient/1.0".to_string(), auth_token);
        let responses = handler.handle_message(hello).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].signature, signatures::SUCCESS);
    }

    #[tokio::test]
    async fn test_reset_message_handling() {
        let mut handler = create_test_handler();

        let reset = BoltMessage::reset();
        let responses = handler.handle_message(reset).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].signature, signatures::SUCCESS);
    }

    #[tokio::test]
    async fn test_goodbye_message_handling() {
        let mut handler = create_test_handler();

        let goodbye = BoltMessage::goodbye();
        let responses = handler.handle_message(goodbye).await.unwrap();

        // GOODBYE should return no responses
        assert_eq!(responses.len(), 0);

        // Context should be set to failed state
        {
            let context = handler.context.lock().unwrap();
            assert_eq!(context.state, ConnectionState::Failed);
        }
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let mut handler = create_test_handler();

        // Set context to ready state
        {
            let mut context = handler.context.lock().unwrap();
            context.set_state(ConnectionState::Ready);
        }

        // Begin transaction
        let begin = BoltMessage::begin(None);
        let responses = handler.handle_message(begin).await.unwrap();
        assert_eq!(responses[0].signature, signatures::SUCCESS);

        // Verify transaction started
        {
            let context = handler.context.lock().unwrap();
            assert!(context.tx_id.is_some());
        }

        // Commit transaction
        let commit = BoltMessage::commit();
        let responses = handler.handle_message(commit).await.unwrap();
        assert_eq!(responses[0].signature, signatures::SUCCESS);

        // Verify transaction cleared
        {
            let context = handler.context.lock().unwrap();
            assert!(context.tx_id.is_none());
        }
    }

    // ------------------------------------------------------------------
    // #516 adversarial-review finding: `execute_cypher_query` previously
    // called `parse_cypher_statement` directly on the raw, un-stripped Bolt
    // query (no comment stripping anywhere in this file), so #516 made a
    // perfectly standard, spec-legal trailing `//` / `/* */` comment in a
    // RUN message a hard `BoltError::query_error` for every Bolt client
    // (Neo4j Browser, drivers, MCP). Fixed by stripping comments once, up
    // front, into a shadowed `query` binding covering every
    // `parse_cypher_statement(query)` call in the function.
    //
    // No schema needs to be registered in `GLOBAL_SCHEMAS` for these checks:
    // `execute_cypher_query` tolerates a missing/unregistered schema (logs a
    // warning and continues) all the way past its own parse — we only need
    // to distinguish "still fails to PARSE" (the regression) from "parsing
    // succeeded, something else failed later" (unrelated, expected here
    // since no real schema is registered).
    // ------------------------------------------------------------------

    fn assert_not_a_parse_error(result: &BoltResult<HashMap<String, Value>>, cypher: &str) {
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("Statement parsing failed") && !msg.contains("Unexpected tokens"),
                "[{cypher}] must not fail at the PARSE stage (a standard trailing \
                 comment must not be mistaken for garbage input); got: {msg}"
            );
        }
        // Ok(_) trivially means parsing (and everything else) succeeded.
    }

    #[tokio::test]
    async fn execute_cypher_query_accepts_trailing_line_comment() {
        let mut handler = create_test_handler();
        let result = handler
            .execute_cypher_query(
                "RETURN 1 AS x // just a trailing note",
                HashMap::new(),
                None,
                None,
                None,
                None,
            )
            .await;
        assert_not_a_parse_error(&result, "RETURN 1 AS x // just a trailing note");
    }

    #[tokio::test]
    async fn execute_cypher_query_accepts_trailing_block_comment() {
        let mut handler = create_test_handler();
        let result = handler
            .execute_cypher_query(
                "RETURN 1 AS x /* trailing block comment */",
                HashMap::new(),
                None,
                None,
                None,
                None,
            )
            .await;
        assert_not_a_parse_error(&result, "RETURN 1 AS x /* trailing block comment */");
    }

    #[tokio::test]
    async fn execute_cypher_query_still_rejects_genuine_trailing_garbage() {
        // #516's actual fix must still hold through this real code path: this
        // is NOT a comment, it's a typo'd keyword, and must still be a hard
        // parse error surfaced as BoltError::query_error.
        let mut handler = create_test_handler();
        let result = handler
            .execute_cypher_query(
                "RETURN 1 AS x GARBAGE",
                HashMap::new(),
                None,
                None,
                None,
                None,
            )
            .await;
        let err = result.expect_err("genuine trailing garbage must still error");
        let msg = err.to_string();
        assert!(
            msg.contains("Statement parsing failed") || msg.contains("Unexpected tokens"),
            "expected a parse-stage error for genuine trailing garbage, got: {msg}"
        );
    }
}
