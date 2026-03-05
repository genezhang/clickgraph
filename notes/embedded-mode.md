# Embedded Mode — Implementation Notes

## Summary

Embedded mode adds a `QueryExecutor` trait abstraction that decouples SQL execution from the ClickHouse client, then implements a `ChdbExecutor` backend that runs queries in-process via the chdb native library. A companion `clickgraph-embedded` crate exposes a Kuzu-compatible synchronous Rust API.

## Key Design Decisions

### 1. `QueryExecutor` Trait (not direct chdb calls)

Rather than calling chdb directly from handlers, we introduced a `QueryExecutor` async trait with two methods: `execute_json` and `execute_text`. This lets the server remain executor-agnostic — the existing remote path became `RemoteClickHouseExecutor`, and the new path is `ChdbExecutor`.

Benefit: clean separation of concerns, testable without a running database.

### 2. `AppState` holds `Arc<dyn QueryExecutor>`

`AppState` was refactored to hold `executor: Arc<dyn QueryExecutor>` alongside `clickhouse_client: Client`. The `clickhouse_client` is kept for admin-only operations (schema loading, introspection); all query execution goes through `executor`.

In embedded mode, `clickhouse_client` is `Client::default()` and admin endpoints fail gracefully.

### 3. `Arc<Mutex<Session>>` for chdb

chdb's `Session` is `!Clone` and synchronous (C FFI). Solution: wrap in `Arc<Mutex<Session>>` and use `tokio::task::spawn_blocking` to avoid blocking the async runtime. The mutex ensures single-threaded chdb access.

### 4. `source:` Field — Schema Extension

Added `source: Option<String>` to `NodeDefinition`, `StandardEdgeDefinition` (YAML config structs) and propagated to `NodeSchema`, `RelationshipSchema` (runtime structs). Tagged `#[serde(skip)]` on runtime structs so `source` is not emitted back to YAML.

VIEWs are created at `Database::new()` time via `data_loader::load_schema_sources()`. The VIEW name matches the YAML `table:` field; the database name matches the YAML `database:` field. This means generated SQL (which references `database.table`) resolves correctly.

### 5. `cypher_to_sql()` Public Bridge

`RenderPlanBuilder` is `pub(crate)` and not accessible from the `clickgraph-embedded` crate. Added `pub fn cypher_to_sql(cypher, schema, max_cte_depth) -> Result<String, String>` to `clickhouse_query_generator/mod.rs` as the public bridge. It sets up a `QueryContext`, runs the full pipeline, and returns the final SQL string.

### 6. Credential Application

`StorageCredentials` fields are applied as `SET key = 'value'` commands at session init (before any VIEWs are created). This is chdb's idiomatic way to set S3/GCS credentials — once set they apply to all subsequent table function calls in the session.

Values are escaped with `replace('\'', "\\'")`  before embedding in `SET` SQL statements.

### 7. SQL Injection Defense in `source_resolver.rs`

URI values are passed through `escape_sql_string()` before embedding in SQL strings (escapes `\` → `\\`, `'` → `\'`). The `table_function:` prefix bypasses this intentionally — it's an escape hatch for advanced use cases where the user constructs the full table function expression.

## Module Map

```
src/executor/
├── mod.rs              — QueryExecutor trait (2 methods: execute_json, execute_text)
├── errors.rs           — ExecutorError type
├── remote.rs           — RemoteClickHouseExecutor (wraps RoleConnectionPool)
├── chdb_embedded.rs    — ChdbExecutor + StorageCredentials (cfg feature="embedded")
├── source_resolver.rs  — URI → chdb table function mapper
└── data_loader.rs      — Creates chdb VIEWs at startup

clickgraph-embedded/src/
├── lib.rs              — Re-exports public API
├── database.rs         — Database + SystemConfig (top-level handle)
├── connection.rs       — Connection<'db> (query execution)
├── query_result.rs     — QueryResult (iterator) + Row
├── value.rs            — Value enum (JSON-to-Rust conversions)
└── error.rs            — EmbeddedError

clickgraph-embedded/tests/
└── integration.rs      — 10 e2e tests using StubExecutor injection
```

## Gotchas

- **`join_use_nulls = 1`** must be set at chdb session init. Without it, LEFT JOINs produce `0`/empty instead of `NULL` for missing rows, which breaks OPTIONAL MATCH semantics.
- **`spawn_blocking` for chdb calls**: The chdb FFI is blocking. Never call it directly from an async context without `spawn_blocking` (it would block the tokio thread pool).
- **VIEW names match schema `table:` field**: When `source:` is specified, `CREATE OR REPLACE VIEW database.table AS SELECT * FROM table_function(...)` is executed. The SQL generator already uses `database.table` in JOINs, so no SQL changes were needed.
- **Schema `source:` is `#[serde(skip)]` on runtime structs**: `NodeSchema` and `RelationshipSchema` use `#[serde(skip)]` to avoid round-tripping `source:` back to YAML output. The config structs (`NodeDefinition`, `StandardEdgeDefinition`) use `#[serde(default)]`.

## Test Coverage

| Test file | Tests | What it covers |
|-----------|-------|----------------|
| `src/executor/source_resolver.rs` | 9 | URI scheme resolution, SQL injection edge cases |
| `src/executor/chdb_embedded.rs` | 8 | StorageCredentials SET command generation |
| `clickgraph-embedded/src/value.rs` | 8 | JSON → Value conversions |
| `clickgraph-embedded/src/query_result.rs` | 6 | Row access, iteration, column names |
| `clickgraph-embedded/src/connection.rs` | 3 | query_to_sql correctness, error handling |
| `clickgraph-embedded/src/database.rs` | 3 | Database construction, schema access |
| `clickgraph-embedded/tests/integration.rs` | 10 | Full pipeline: YAML → schema → Cypher → SQL → QueryResult → Row → Value |

## Future Work

- Wire `SystemConfig.max_threads` to chdb session `max_threads` setting
- Wire `SystemConfig.data_dir` as base for relative `source:` path resolution
- Add Python bindings via PyO3 (similar to Kuzu's Python API)
- Support parameterized queries in the embedded API (`conn.query_with_params(cypher, params)`)
- Persist chdb views across sessions for warm startup
