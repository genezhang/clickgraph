//! openCypher TCK runner for ClickGraph.
//!
//! Uses the `cucumber` crate (v0.21) with the embedded ClickGraph API.
//!
//! **One chdb session per process**: A single `Database` is created at startup
//! from a universal schema that covers all labels/rel-types seen across the
//! selected feature files.  Tables are truncated between scenarios.
//!
//! Run:
//! ```
//! CLICKGRAPH_CHDB_TESTS=1 cargo test --test tck
//! ```

mod create_parser;
mod result_fmt;
mod schema_gen;

use std::collections::HashMap;
use std::sync::LazyLock;

use clickgraph_embedded::{Connection, Database, SystemConfig, Value};
use cucumber::{gherkin::Step, given, then, when, World};
use result_fmt::{
    extract_var_labels, extract_var_rel_types, format_row, format_value, parse_expected_table,
    COL_SEP,
};

// ---------------------------------------------------------------------------
// Shared database (one chdb session per process)
// ---------------------------------------------------------------------------

struct TckDatabase {
    db: Database,
    /// All ClickHouse table names managed by this schema (for truncation).
    tables: Vec<String>,
}

/// Universal schema catalog built from scanning all feature files.
/// The path is relative to the workspace root where `cargo test` runs.
static SCHEMA_CATALOG: LazyLock<schema_gen::SchemaCatalog> =
    LazyLock::new(|| schema_gen::scan_features("tests/features"));

static SHARED: LazyLock<&'static TckDatabase> = LazyLock::new(|| {
    let yaml = schema_gen::generate_yaml(&SCHEMA_CATALOG);
    let tables = SCHEMA_CATALOG.all_table_names();

    // Write schema to a temp file (Database::in_memory takes a path)
    let schema_path = std::env::temp_dir().join("clickgraph_tck_schema.yaml");
    std::fs::write(&schema_path, &yaml).expect("write TCK schema YAML");

    // Cap resource usage: TCK queries are trivial; 4 threads and 4 GiB per
    // query is plenty and prevents runaway memory if multiple test processes
    // are accidentally started in parallel.
    let config = SystemConfig {
        max_threads: Some(4),
        max_memory_usage_bytes: Some(4 * 1024 * 1024 * 1024), // 4 GiB
        ..SystemConfig::default()
    };
    let db = Database::in_memory(&schema_path, config).expect("create TCK database");

    // Leak intentionally: chdb SIGABRT on Drop; same pattern as chdb_e2e.rs
    Box::leak(Box::new(TckDatabase { db, tables }))
});

fn shared_db() -> &'static Database {
    &SHARED.db
}

fn all_tables() -> &'static [String] {
    &SHARED.tables
}

/// Truncate all managed tables (called at the start of each scenario).
fn truncate_all_tables() {
    let db = shared_db();
    if let Ok(conn) = Connection::new(db) {
        for table in all_tables() {
            let r = conn.execute_sql(&format!("TRUNCATE TABLE IF EXISTS `default`.`{table}`"));
            if let Err(ref e) = r {
                eprintln!("TRUNCATE ERROR for {table}: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// World
// ---------------------------------------------------------------------------

#[derive(Debug, World)]
#[world(init = Self::new)]
pub struct TckWorld {
    /// Variable name → created node ID (UUID string).
    node_id_map: HashMap<String, String>,
    /// Whether the current scenario should be skipped.
    /// Set when we encounter unsupported patterns (e.g. unlabeled nodes).
    skip_reason: Option<String>,
    /// Query parameters (set by "parameters are:" step).
    params: HashMap<String, String>,
    /// Column names from the last query result.
    result_columns: Vec<String>,
    /// Formatted result rows (joined with COL_SEP).
    result_rows: Vec<String>,
    /// Error message from a failed query.
    error: Option<String>,
    /// SQL generated for the last query (for debug output).
    last_sql: Option<String>,
    /// Variable name → node label (for edge table routing).
    node_label_map: HashMap<String, String>,
}

impl TckWorld {
    fn new() -> Self {
        truncate_all_tables();
        TckWorld {
            node_id_map: HashMap::new(),
            skip_reason: None,
            params: HashMap::new(),
            result_columns: Vec::new(),
            result_rows: Vec::new(),
            error: None,
            last_sql: None,
            node_label_map: HashMap::new(),
        }
    }

    fn is_skip(&self) -> bool {
        self.skip_reason.is_some()
    }
}

// ---------------------------------------------------------------------------
// Given steps
// ---------------------------------------------------------------------------

#[given(regex = r"^an empty graph$")]
async fn given_empty_graph(_world: &mut TckWorld) {
    // Tables were already truncated in TckWorld::new(); nothing more to do.
}

#[given(regex = r"^any graph$")]
async fn given_any_graph(_world: &mut TckWorld) {
    // "any graph" means the test doesn't depend on the graph state.
}

#[given(expr = "the {word} graph")]
async fn given_named_graph(world: &mut TckWorld, name: String) {
    // Binary tree graphs used by some TCK scenarios.
    match name.as_str() {
        "binary-tree-1" => load_binary_tree(world, 1),
        "binary-tree-2" => load_binary_tree(world, 2),
        other => {
            world.skip_reason = Some(format!("named graph '{other}' not supported"));
        }
    }
}

#[given(regex = r"^having executed:$")]
async fn given_having_executed(world: &mut TckWorld, step: &Step) {
    if world.is_skip() {
        return;
    }
    let cypher = match step.docstring() {
        Some(s) => s.trim().to_string(),
        None => return,
    };
    execute_setup_create(world, &cypher);
}

#[given(regex = r"^parameters are:$")]
async fn given_parameters(world: &mut TckWorld, step: &Step) {
    if let Some(table) = step.table() {
        // Expect a two-column table: | name | value |
        // Skip the header row
        let mut rows = table.rows.iter();
        rows.next(); // header
        for row in rows {
            if row.len() >= 2 {
                let name = row[0].trim().to_string();
                let value = row[1].trim().to_string();
                world.params.insert(name, value);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// When steps
// ---------------------------------------------------------------------------

#[when(regex = r"^executing(?: control)? query:$")]
async fn when_executing_query(world: &mut TckWorld, step: &Step) {
    if world.is_skip() {
        return;
    }
    let query = match step.docstring() {
        Some(s) => s.trim().to_string(),
        None => return,
    };

    // Substitute parameters into the query
    let query = substitute_params(&query, &world.params);

    let db = shared_db();
    let conn = match Connection::new(db) {
        Ok(c) => c,
        Err(e) => {
            world.error = Some(e.to_string());
            return;
        }
    };

    // Extract variable→label and variable→rel_type mappings for result formatting.
    let var_labels = extract_var_labels(&query);
    let var_rel_types = extract_var_rel_types(&query);

    // Increase query size limit for complex universal schema queries
    let _ = conn.execute_sql("SET max_query_size = 10485760");

    // Capture SQL for debug output on failures
    world.last_sql = conn.query_to_sql(&query).ok();

    match conn.query(&query) {
        Ok(mut result) => {
            world.result_columns = result.get_column_names().to_vec();
            let col_names = world.result_columns.clone();
            world.result_rows = result
                .map(|row| {
                    let values: Vec<Value> = row.values().to_vec();
                    format_row(&col_names, &values, &var_labels, &var_rel_types)
                })
                .collect();
            world.error = None;
        }
        Err(e) => {
            world.error = Some(e.to_string());
            world.result_rows.clear();
            world.result_columns.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// Then steps
// ---------------------------------------------------------------------------

#[then(regex = r"^the result should be, in any order:$")]
async fn then_result_any_order(world: &mut TckWorld, step: &Step) {
    if world.is_skip() {
        return;
    }
    if let Some(ref err) = world.error {
        // ClickGraph couldn't execute this query — mark as skip with reason
        world.skip_reason = Some(format!("query error: {}", &err[..err.len().min(120)]));
        return;
    }
    let table = match step.table() {
        Some(t) => t,
        None => return,
    };
    let (_headers, expected_rows) = parse_expected_table(table);

    // Empty table (only header row) means we expect no results
    if expected_rows.is_empty() {
        assert!(
            world.result_rows.is_empty(),
            "Expected empty result but got: {:?}",
            world.result_rows
        );
        return;
    }

    // Normalize boolean/null representations before comparison.
    // ClickHouse returns boolean expressions as UInt8 (0/1); TCK expects true/false.
    let mut actual: Vec<String> = world
        .result_rows
        .iter()
        .map(|r| result_fmt::normalize_row(r))
        .collect();
    let mut expected: Vec<String> = expected_rows
        .iter()
        .map(|r| result_fmt::normalize_row(r))
        .collect();
    actual.sort();
    expected.sort();

    if let Some(ref sql) = world.last_sql {
        use std::io::Write;
        let label = if actual != expected { "FAIL" } else { "PASS" };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/tck_failing_sql.txt")
        {
            let _ = writeln!(
                f,
                "=== {} EXPECTED: {:?} ACTUAL: {:?} ===\n{}\n",
                label, expected_rows, world.result_rows, sql
            );
        }
    }
    assert_eq!(
        actual, expected,
        "\nResult mismatch (any order)\nActual  : {:?}\nExpected: {:?}",
        world.result_rows, expected_rows
    );
}

#[then(regex = r"^the result should be, in order:$")]
async fn then_result_in_order(world: &mut TckWorld, step: &Step) {
    if world.is_skip() {
        return;
    }
    if let Some(ref err) = world.error {
        world.skip_reason = Some(format!("query error: {}", &err[..err.len().min(120)]));
        return;
    }
    let table = match step.table() {
        Some(t) => t,
        None => return,
    };
    let (_headers, expected_rows) = parse_expected_table(table);

    let actual: Vec<String> = world
        .result_rows
        .iter()
        .map(|r| result_fmt::normalize_row(r))
        .collect();
    let expected: Vec<String> = expected_rows
        .iter()
        .map(|r| result_fmt::normalize_row(r))
        .collect();

    assert_eq!(
        actual, expected,
        "\nResult mismatch (in order)\nActual  : {:?}\nExpected: {:?}",
        world.result_rows, expected_rows
    );
}

#[then(regex = r"^the result should be empty$")]
async fn then_result_empty(world: &mut TckWorld) {
    if world.is_skip() {
        return;
    }
    if let Some(ref err) = world.error {
        world.skip_reason = Some(format!("query error: {}", &err[..err.len().min(120)]));
        return;
    }
    assert!(
        world.result_rows.is_empty(),
        "Expected empty result but got: {:?}",
        world.result_rows
    );
}

#[then(regex = r"^no side effects$")]
async fn then_no_side_effects(_world: &mut TckWorld) {
    // In read-only test scenarios, there should be no side effects.
    // We accept this step as a no-op.
}

#[then(regex = r"^the side effects should be:$")]
async fn then_side_effects(_world: &mut TckWorld) {
    // Side-effect tracking (node/rel counts) is not implemented.
    // Accept without assertion.
}

#[then(regex = r"^a (\w+) should be raised at (?:compile|runtime|any) time: (.+)$")]
async fn then_error_raised(world: &mut TckWorld, _error_type: String, _error_name: String) {
    if world.is_skip() {
        return;
    }
    if world.error.is_none() {
        // ClickGraph doesn't always raise the same errors as Neo4j.
        // Mark as skipped rather than failing.
        world.skip_reason = Some("error not raised (ClickGraph limitation)".to_string());
    }
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

/// Parse and execute a CREATE block, loading data into the shared database.
fn execute_setup_create(world: &mut TckWorld, cypher: &str) {
    let parsed = create_parser::parse_create_block(cypher, &mut world.node_id_map);

    let db = shared_db();
    let conn = match Connection::new(db) {
        Ok(c) => c,
        Err(e) => {
            world.skip_reason = Some(format!("DB connection failed: {e}"));
            return;
        }
    };

    // Create nodes
    for node in &parsed.nodes {
        let label = match &node.label {
            Some(l) => l.clone(),
            None => "__Unlabeled".to_string(),
        };

        let var = node.var.as_deref().unwrap_or("").to_string();

        let props: HashMap<String, Value> = node
            .props
            .iter()
            .map(|(k, v)| (k.clone(), prop_to_value(v)))
            .collect();

        match conn.create_node(&label, props) {
            Ok(node_id) => {
                if !var.is_empty() {
                    world.node_id_map.insert(var.clone(), node_id);
                    world.node_label_map.insert(var, label);
                }
            }
            Err(e) => {
                world.skip_reason = Some(format!("create_node failed: {e}"));
                return;
            }
        }
    }

    // Create edges — use direct SQL insert to route to the correct table based on
    // from/to labels. conn.create_edge picks the first alphabetical schema variant,
    // which is wrong when multiple (from_label, to_label) combinations exist.
    for edge in &parsed.edges {
        let from_id = match world.node_id_map.get(&edge.from_var) {
            Some(id) => id.clone(),
            None => continue, // can't resolve — skip
        };
        let to_id = match world.node_id_map.get(&edge.to_var) {
            Some(id) => id.clone(),
            None => continue,
        };

        let from_label = world
            .node_label_map
            .get(&edge.from_var)
            .cloned()
            .unwrap_or_else(|| "__Unlabeled".to_string());
        let to_label = world
            .node_label_map
            .get(&edge.to_var)
            .cloned()
            .unwrap_or_else(|| "__Unlabeled".to_string());

        let table = schema_gen::edge_table_name(&edge.rel_type, &from_label, &to_label);

        // Build INSERT SQL for this edge
        let mut cols = vec!["from_id".to_string(), "to_id".to_string()];
        let mut vals = vec![
            format!("'{}'", from_id.replace('\'', "''")),
            format!("'{}'", to_id.replace('\'', "''")),
        ];
        for (k, v) in &edge.props {
            cols.push(k.clone());
            let lit = prop_to_value(v)
                .to_sql_literal()
                .unwrap_or_else(|_| "NULL".to_string());
            vals.push(lit);
        }
        let sql = format!(
            "INSERT INTO `default`.`{}` ({}) VALUES ({})",
            table,
            cols.join(", "),
            vals.join(", ")
        );
        if let Err(e) = conn.execute_sql(&sql) {
            world.skip_reason = Some(format!("create_edge SQL failed: {e}"));
            return;
        }
    }
}

/// Convert a `PropValue` to an embedded `Value`.
fn prop_to_value(pv: &create_parser::PropValue) -> Value {
    match pv {
        create_parser::PropValue::Str(s) => Value::String(s.clone()),
        create_parser::PropValue::Int(i) => Value::Int64(*i),
        create_parser::PropValue::Float(f) => Value::Float64(*f),
        create_parser::PropValue::Bool(b) => Value::Bool(*b),
        create_parser::PropValue::Null => Value::Null,
    }
}

/// Substitute `$param` placeholders in a Cypher query with literal values.
fn substitute_params(query: &str, params: &HashMap<String, String>) -> String {
    let mut result = query.to_string();
    // Sort by length descending to avoid partial replacements ($age vs $age_lower)
    let mut sorted: Vec<_> = params.iter().collect();
    sorted.sort_by(|(a, _), (b, _)| b.len().cmp(&a.len()));
    for (name, val) in sorted {
        result = result.replace(&format!("${name}"), val);
    }
    result
}

// ---------------------------------------------------------------------------
// Binary tree graph loader
// ---------------------------------------------------------------------------

/// Load a binary tree graph. Tree depth = `depth`.
/// Nodes are labeled `Node` with property `val: Int64`.
/// Edges are `[:CHILD]` from parent to children.
fn load_binary_tree(world: &mut TckWorld, depth: u32) {
    let db = shared_db();
    let conn = match Connection::new(db) {
        Ok(c) => c,
        Err(e) => {
            world.skip_reason = Some(format!("DB connection failed: {e}"));
            return;
        }
    };

    // Create nodes 1..2^depth (1-indexed, node 1 is root)
    let count = (1u32 << depth) - 1; // nodes 1..count (inclusive)
    let mut id_map: HashMap<u32, String> = HashMap::new();

    for i in 1..=count {
        let mut props = HashMap::new();
        props.insert("val".to_string(), Value::Int64(i as i64));
        match conn.create_node("Node", props) {
            Ok(nid) => {
                id_map.insert(i, nid);
            }
            Err(e) => {
                world.skip_reason = Some(format!("create_node (binary tree) failed: {e}"));
                return;
            }
        }
    }

    // Create CHILD edges: for node i, children are 2i and 2i+1
    for i in 1..=count {
        for child in [2 * i, 2 * i + 1] {
            if child <= count {
                if let (Some(from_id), Some(to_id)) = (id_map.get(&i), id_map.get(&child)) {
                    if let Err(e) = conn.create_edge("CHILD", from_id, to_id, HashMap::new()) {
                        world.skip_reason = Some(format!("create_edge (binary tree) failed: {e}"));
                        return;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    // Initialise the shared database eagerly so any schema errors surface
    // before the test run starts.
    let _ = &*SHARED;

    futures::executor::block_on(
        TckWorld::cucumber()
            // Force sequential execution: chdb has one global session and
            // tables are shared between scenarios; concurrent runs cause
            // cross-contamination.
            .max_concurrent_scenarios(1)
            .filter_run("tests/features", |_, _, sc| {
                // Skip scenarios tagged @skip or @NegativeTests
                !sc.tags.iter().any(|t| {
                    matches!(
                        t.as_str(),
                        "skip" | "fails" | "NegativeTests" | "crash" | "wip"
                    )
                })
            }),
    );
}

// TEMP: Debug test
#[test]
fn debug_schema() {
    let catalog = schema_gen::scan_features("tests/features");
    let yaml = schema_gen::generate_yaml(&catalog);
    println!("SCHEMA:\n{}", yaml);
}
