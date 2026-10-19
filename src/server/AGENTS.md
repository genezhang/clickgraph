# server Module — Agent Guide

> **Purpose**: HTTP + Bolt protocol server layer. Receives Cypher queries, orchestrates the
> full pipeline (parse → plan → render → SQL gen → ClickHouse execution), manages schema
> lifecycle, query caching, connection pooling, and parameter substitution.
> This is the entry point for ALL query processing. Understand the data flow before changing anything.

## Module Architecture

```
                        ┌──────────────────────────────────────────┐
                        │              mod.rs                      │
                        │  Server startup, routing, global statics │
                        │  GLOBAL_SCHEMAS, GLOBAL_SCHEMA_CONFIGS   │
                        │  GLOBAL_QUERY_CACHE                      │
                        └────────┬────────────────────┬────────────┘
                                 │                    │
              ┌──────────────────┤                    │
              ▼                  ▼                    ▼
    ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────────┐
    │  handlers.rs    │  │ sql_generation_  │  │  bolt_protocol/       │
    │  (1243 lines)   │  │ handler.rs (425) │  │  (7986 lines total)   │
    │                 │  │                  │  │                       │
    │  POST /query    │  │  POST /query/sql │  │  bolt://localhost:7687│
    │  GET  /health   │  │  SQL-only, no    │  │  Neo4j Bolt v4.1–5.8 │
    │  GET  /schemas  │  │  execution       │  │  TCP + WebSocket      │
    │  POST /schemas  │  │                  │  │                       │
    └──────┬──────────┘  └──────┬───────────┘  └───────────────────────┘
           │                    │
           ▼                    ▼
    ┌──────────────────────────────────────────────────────────────┐
    │               Shared Infrastructure                         │
    │                                                              │
    │  query_context.rs (482)  — task-local per-query state       │
    │  query_cache.rs   (581)  — LRU cache for SQL templates      │
    │  parameter_substitution.rs (368) — $param → SQL escaping    │
    │  graph_catalog.rs (858)  — schema init/load/validate        │
    │  graph_output.rs  (100)  — format=Graph transformation      │
    │  connection_pool.rs (158) — role-based ClickHouse pools     │
    │  clickhouse_client.rs (75) — client factory                 │
    │  models.rs        (300)  — request/response types           │
    └──────────────────────────────────────────────────────────────┘
```

## Data Flow: Query Request Lifecycle

```
Client POST /query  { "query": "MATCH (n:User) RETURN n", "schema_name": "default" }
    │
    ▼
handlers.rs::query_handler()
    │
    ├── 1. Extract payload fields (format, sql_only, schema_name, parameters)
    ├── 2. translate::PreparedStatement::parse(): the prelude (CYPHER
    │      options → ReplanOption, EXPLAIN / PROFILE), comments stripped,
    │      every UNION arm's USE; .targets.graph(schema_name) → the schema
    ├── 3. Check for procedure calls (early return if standalone CALL/UNION)
    ├── 4. Syntax check (a syntax error before "Schema not found")
    │
    ├── 5. Create QueryContext, wrap in with_query_context()  ← CRITICAL
    │      └── query_handler_inner() runs inside task-local scope
    │
    ├── 6. Check query cache (unless replan=force)
    │      Cache HIT → substitute params → execute/return
    │
    ├── 7. Resolve schema: graph_catalog::get_graph_schema_by_name()
    │      └── set_current_schema(Arc<GraphSchema>) in task-local context
    │
    ├── 8. Parse:  open_cypher_parser::parse_cypher_statement()
    │      (classification: read / CALL / write)
    ├── 9-11. Translate: translate::translate_read() — the bound plan
    │      (clause list → bind → lower → print; P-4c, the legacy
    │      planner/renderer is unreachable since S14c1)
    │
    ├── 12. Cache SQL template (GLOBAL_QUERY_CACHE)
    │
    ├── 13. Parameter substitution (merge view_parameters + query parameters)
    ├── 14. Check for unsubstituted $param placeholders
    │
    ├── 15a. If format=Graph: execute SQL → JSON rows → graph_output::transform_to_graph()
    │         Return { nodes, edges, stats } with deduplication
    │
    └── 15b. Otherwise: Execute via connection_pool.get_client(role) → ClickHouse
             Return JSON/Pretty/CSV response with performance headers
```

## Key Files

| File | Lines | Responsibility |
|------|------:|---------------|
| `mod.rs` | 441 | Server startup, axum routing, global `OnceCell` statics, Bolt server spawn, signal handling |
| `handlers.rs` | 1243 | HTTP handlers: `query_handler` (main entry), `health_check`, schema CRUD, `execute_cte_queries`, performance metrics |
| `sql_generation_handler.rs` | 425 | `POST /query/sql` — translate-only endpoint, no ClickHouse execution, structured error responses |
| `graph_catalog.rs` | 858 | Schema lifecycle: `initialize_global_schema`, `load_schema_from_content`, `get_graph_schema_by_name`, DB fallback, schema validation, `monitor_schema_updates` |
| `query_cache.rs` | 581 | LRU cache: `QueryCache`, `QueryCacheKey`, `ReplanOption` (CYPHER replan=force/skip), `CacheMetrics`, schema-scoped invalidation |
| `query_context.rs` | 482 | **Task-local context** via `tokio::task_local!`: dialect, schema, per-query id counters, reduce binder types, the alias → label map, list-valued CTE columns, table stats |
| `parameter_substitution.rs` | 368 | `substitute_parameters()`, `find_unsubstituted_parameter()`, SQL injection prevention via string escaping |
| `models.rs` | 300 | `QueryRequest`, `OutputFormat` (incl. `Graph`), `SqlDialect`, `SqlGenerationRequest/Response`, `SqlOnlyResponse`, `GraphNode`, `GraphEdge`, `GraphQueryResponse`, `QueryStats` |
| `graph_output.rs` | 100 | `transform_to_graph()` — converts flat JSON rows to deduplicated `(Vec<GraphNode>, Vec<GraphEdge>)` by reusing the translation's `return_metadata()` and Bolt's `transform_to_node`/`transform_to_relationship` |
| `connection_pool.rs` | 260 | `RoleConnectionPool`: lazy-initialized per-role ClickHouse client pools with read/write lock, cluster load balancing via round-robin |
| `clickhouse_client.rs` | 75 | `try_get_client()`: creates ClickHouse client from env vars with safety limits (60s timeout, 1M rows, 1GB result) |
| `bolt_protocol/` | 7986 | Neo4j Bolt v4.1–5.8 wire protocol (see separate section below) |

**Total**: ~12,795 lines (server core: 4,809 + bolt: 7,986)

## Critical Invariants

### 1. Schema Access Pattern — MOST IMPORTANT

**Rule**: All query-processing code MUST access schema via task-local `QueryContext`, NEVER directly from `GLOBAL_SCHEMAS`.

```rust
// ✅ CORRECT — in query-processing code
use crate::server::query_context::get_current_schema;
let schema = get_current_schema().expect("schema must be set");

// ✅ CORRECT — in code also called from unit tests (fallback to GLOBAL_SCHEMAS)
use crate::server::query_context::get_current_schema_with_fallback;
let schema = get_current_schema_with_fallback();

// ❌ WRONG — direct GLOBAL_SCHEMAS access in query processing
let schemas = GLOBAL_SCHEMAS.get().unwrap().read().await;
let schema = schemas.get("default");  // Non-deterministic in multi-schema!
```

**Where GLOBAL_SCHEMAS is appropriate**: `mod.rs` (init), `graph_catalog.rs` (admin), `bolt_protocol/handler.rs` (connection setup), test setup.

### 2. with_query_context() Wrapping

`handlers.rs::query_handler()` creates a `QueryContext` and wraps **all** downstream processing in `with_query_context()`. Without this wrapper, `task_local!` variables return `None` and schema lookups silently fail.

```rust
// handlers.rs line ~395
let context = QueryContext::new(Some(schema_name.clone()));
with_query_context(context, async move {
    query_handler_inner(/* ... */).await
}).await
```

### Request Records (metrics, OTLP spans)

Every request is recorded exactly once (#1500). HTTP: queries that run the
pipeline are recorded at the finalization sites in `query_handler_inner`
(with their phase breakdown); everything else — procedures, SHOW DATABASES,
COPY TO, exports, a capacity rejection, every `Err` — by `query_handler`
from the status it returns. `record_query_with` sets the request's
`REQUEST_RECORD.recorded` flag, so the wrapper never records twice; an early
path only notes its type (`note_request_type`). Bolt: `handle_run` records
every RUN once, whatever answered it (a query, a procedure, an intercept,
EXPLAIN, a refusal), its class from the FAILURE's code. Do not add
`record_query` calls to early returns.

### 3. Schema Name Resolution Order

Schema name is determined in this priority, by one rule for HTTP `/query`,
`/query/sql` and Bolt (`translate::StatementTargets::graph`):
1. The first UNION arm's `USE <schema>` clause
2. `schema_name` field in the JSON request body (Bolt: the session's database)
3. `"default"` fallback (`translate::DEFAULT_GRAPH`)

A later arm's `USE` naming another graph is an error (#1370). Every early
return (procedures, COPY TO, apoc, vector / fulltext search) uses the same
resolved name; read the prelude only through `translate::prepared`, never by
searching the text for keywords (#1371).

### 4. Parse Before Schema Lookup

Syntax validation happens BEFORE schema resolution. This prevents misleading "Schema not found" errors when the real issue is a parse error. See `handlers.rs` ~line 340.

### 5. Parameter Substitution After Cache

Cache stores SQL **templates** with `$paramName` placeholders. Parameter substitution happens AFTER cache retrieval, keeping cache entries reusable across different parameter values.

### 6. Global Statics Initialization Order

```
1. GLOBAL_SCHEMA_CONFIG  (legacy, deprecated)
2. GLOBAL_SCHEMAS        (HashMap<String, GraphSchema>)
3. GLOBAL_SCHEMA_CONFIGS (HashMap<String, GraphSchemaConfig>)
4. GLOBAL_QUERY_CACHE    (QueryCache)
```

All use `OnceCell<RwLock<...>>` — set exactly once at startup. `GLOBAL_QUERY_CACHE` uses `OnceCell` without `RwLock` (internal `Mutex`).

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| Schema not in task-local | `get_current_schema()` returns `None` | Missing `with_query_context()` wrapper or `set_current_schema()` not called |
| Misleading "Schema not found" | User sees schema error for typo in query | Parse error check missing before schema lookup |
| Cached SQL with wrong params | Query returns wrong results | Cache key doesn't differentiate parameter values (by design — params substituted after) |
| Stale cache after schema reload | Old SQL references wrong columns | `invalidate_schema()` not called after schema reload |
| `$param` in executed SQL | ClickHouse syntax error | Missing `view_parameters` in request; `find_unsubstituted_parameter` check catches this |
| Mutex poisoning in cache | Cache silently disabled | Panic in another thread holding cache lock; `lock_cache!` macro degrades gracefully |
| Role pool accumulation | Memory growth over time | Role pools are never evicted (lazy-init only); acceptable for bounded role sets |
| Connection timeout | Bolt connection drops | Default 300s timeout in `BoltConfig`; configurable |

## HTTP API Routes

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/health` | `health_check` | Service health + version |
| POST | `/query` | `query_handler` | Full Cypher→SQL→execute pipeline |
| POST | `/query/sql` | `sql_generation_handler` | Cypher→SQL translation only (no execution) |
| GET | `/schemas` | `list_schemas_handler` | List loaded schemas |
| POST | `/schemas/load` | `load_schema_handler` | Load YAML schema at runtime |
| GET | `/schemas/{name}` | `get_schema_handler` | Get schema details |

## Key Types

### AppState (shared across all requests)
```rust
pub struct AppState {
    pub clickhouse_client: Client,          // Default ClickHouse client
    pub connection_pool: Arc<RoleConnectionPool>,  // Role-based pools
    pub config: ServerConfig,               // CLI/env configuration
}
```

### QueryContext (per-request, task-local)
```rust
pub struct QueryContext {
    pub dialect: SqlDialect,
    pub schema_name: Option<String>,
    pub schema: Option<Arc<GraphSchema>>,
    pub reduce_binder_types: HashMap<String, RenderType>, // #969
    pub alias_label_map: HashMap<String, String>,         // type inference
    pub array_cte_columns: HashSet<String>,               // Databricks size()
    pub table_stats: Option<Arc<TableStatsSnapshot>>,
    pub alias_counter: u32,
    pub cte_counter: u32,
}
```

### QueryRequest (HTTP input)
```rust
pub struct QueryRequest {
    pub query: String,
    pub format: Option<OutputFormat>,       // JSONEachRow, Pretty, CSV, Graph, etc.
    pub sql_only: Option<bool>,             // Return SQL without executing
    pub schema_name: Option<String>,        // Schema selection
    pub parameters: Option<HashMap<String, Value>>,  // Query params ($userId)
    pub view_parameters: Option<HashMap<String, Value>>,  // View params (tenant_id)
    pub role: Option<String>,               // ClickHouse RBAC role
    pub max_inferred_types: Option<usize>,  // Ignored (legacy planner's cap)
    pub tenant_id: Option<String>,          // Multi-tenant deployments
}
```

## Dependencies

### Upstream (this module calls)
- `open_cypher_parser` — parse Cypher queries
- `translate` — the read seam (`translate_read`: bind, lower, print SQL)
- `query_planner` — statement classification, `evaluate_call_query` (PageRank's arguments)
- `sql_generator` — PageRank SQL, dialects
- `graph_catalog` — schema types and config parsing
- `procedures` — procedure execution (schema_info, etc.)
- `config` — `ServerConfig` from CLI/env

### Downstream (external crates)
- `axum` — HTTP framework (routing, extraction, response)
- `clickhouse` — ClickHouse client (query execution)
- `tokio` — async runtime, `task_local!`, `OnceCell`, `RwLock`
- `serde_json` — JSON serialization
- `dotenvy` — environment variable loading

## Testing

### Unit Tests (in-file `#[cfg(test)]`)

| File | Tests | Coverage |
|------|-------|----------|
| `connection_pool.rs` | 1 (#[ignore]) | Pool creation, role isolation |
| `parameter_substitution.rs` | 8 | Escaping, substitution, SQL injection prevention |
| `query_cache.rs` | 5 | LRU eviction, schema invalidation, metrics, replan parsing |
| `query_context.rs` | 1 | Task-local isolation |

### Integration Tests
- `tests/integration/` — E2E tests via HTTP API
- `tests/integration/bolt/` — Bolt protocol tests
- Require running ClickHouse + server instance

### Manual Testing
```bash
# Start server
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph

# Health check
curl http://localhost:7475/health

# Query
curl -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u.name LIMIT 5"}'

# SQL-only (no execution)
curl -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u.name", "sql_only": true}'

# Graph format (structured nodes/edges/stats)
curl -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 5", "format": "Graph"}'

# SQL generation endpoint (structured response)
curl -X POST http://localhost:7475/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u.name"}'
```

## Bolt Protocol Sub-Module

The `bolt_protocol/` directory (7,986 lines, 11 files) implements the Neo4j Bolt wire protocol for compatibility with Neo4j Browser, drivers, and tools. **It warrants its own AGENTS.md.**

### Quick Summary

| File | Lines | Purpose |
|------|------:|---------|
| `mod.rs` | 383 | `BoltServer`, `BoltContext`, `BoltConfig`, `ConnectionState` enum, version constants (5.8→4.1) |
| `handler.rs` | 1825 | **Core** — processes HELLO/LOGON/RUN/PULL/BEGIN/COMMIT/ROUTE, integrates with query pipeline |
| `result_transformer.rs` | 2145 | **Largest** — transforms ClickHouse rows → Neo4j Node/Relationship/Path graph objects |
| `graph_objects.rs` | 716 | `Node`, `Relationship`, `Path` structs with packstream binary encoding |
| `connection.rs` | 604 | Handshake, version negotiation, message read/write loop, chunked encoding |
| `id_mapper.rs` | 757 | Deterministic int↔elementId mapping (53-bit JS-safe, label-encoded, session-scoped); `resolve_node_id` for the bound plan's `id()` reads |
| `messages.rs` | 562 | `BoltMessage`, `BoltValue`, message signatures (HELLO=0x01, RUN=0x10, SUCCESS=0x70, etc.) |
| `auth.rs` | 401 | `AuthScheme` (None/Basic/Kerberos), `AuthToken`, `Authenticator` with SHA-256 |
| `errors.rs` | 213 | `BoltError` enum with Neo4j-compatible error codes; `QueryErrorKind` (#1399) is a query failure's kind (syntax, argument, parameter missing, database / procedure not found, procedure failed, execution failed), whose `neo4j_code()` is its FAILURE code and whose metrics class is `ErrorClass::from_neo4j_code` |
| `websocket.rs` | 156 | `WebSocketBoltAdapter` — wraps WebSocket as AsyncRead/AsyncWrite for Bolt-over-WS |

### Bolt Data Flow
```
Neo4j Browser/Driver → TCP or WebSocket
    │
    ├── connection.rs: handshake + version negotiation (4.1–5.8)
    ├── handler.rs: HELLO → LOGON → RUN(cypher) → PULL
    │      RUN: translate::Prelude::parse (EXPLAIN → empty plan, PROFILE
    │      refused), parameter substitution, then StatementTargets::of(text)
    │      .graph(session db) → the graph (a session without one: "default")
    │   ├── Query pipeline same as HTTP (parse→bind→lower→SQL→execute)
    │   ├── result_transformer.rs: flat rows → Node/Relationship/Path objects
    │   └── id_mapper.rs: generate integer IDs for id() compat
    └── connection.rs: serialize BoltMessage → packstream → chunked TCP
```

### Bolt-Specific Concerns
- **State machine**: Connected → Negotiated → Authentication → Ready → Streaming → Failed
- **Version negotiation**: Server picks highest mutually supported version from client's 4 proposals
- **ID mapping**: `id_mapper.rs` encodes label (6 bits) + id hash (47 bits) within JS MAX_SAFE_INTEGER
- **`id()` reads**: Neo4j Browser expand/double-click sends `id(n) = 12345`; the bound plan resolves it through `id_mapper::resolve_node_id` to the node's label and key
- **Thread safety**: `BoltContext` wrapped in `Arc<Mutex<>>` per connection (NOT task-local like HTTP)
- **All bolt_protocol files have unit tests** (13 test modules total)

## Files You Should NOT Touch Casually

- **handlers.rs** — 1243 lines, orchestrates the entire HTTP query pipeline. Any change to the
  flow (cache lookup, schema resolution, parameter substitution order) can break query semantics.
- **graph_catalog.rs** — Schema initialization has multiple fallback paths (YAML → DB → empty).
  Changing initialization order can leave `GLOBAL_SCHEMAS` unset.
- **query_context.rs** — The `task_local!` pattern is subtle: a query's state lives in its
  task's context (`with_query_context`), never in a global.
- **bolt_protocol/result_transformer.rs** — 2145 lines, the most complex Bolt file. Transforms
  flat rows to graph objects with label inference, element_id generation, and VLP path reconstruction.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `CLICKHOUSE_URL` | (required) | ClickHouse HTTP endpoint |
| `CLICKHOUSE_USER` | (required) | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | (required) | ClickHouse password |
| `CLICKHOUSE_DATABASE` | `"default"` | Default database |
| `GRAPH_CONFIG_PATH` | (optional) | YAML schema file path |
| `CLICKHOUSE_CLUSTER` | (optional) | Cluster name for load balancing (discovers nodes from `system.clusters`) |
| `CLICKGRAPH_QUERY_CACHE_ENABLED` | `true` | Enable/disable query cache |
| `CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES` | `1000` | Max cached queries |
| `CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB` | `100` | Max cache memory |
