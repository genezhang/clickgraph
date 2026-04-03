# clickgraph-embedded — Agent Guide

> **Purpose**: Core embedded engine crate. Provides `Database`, `Connection`,
> `QueryResult`, and `Value` types that wrap ClickGraph's Cypher→SQL pipeline.
> Supports three execution backends: chdb in-process, remote ClickHouse, or
> SQL-only (translate without executing).

## Architecture

```
Database (schema loading + executor)
  ├── Database::sql_only(path)          → NullExecutor (translate only)
  ├── Database::new_remote(path, cfg)   → RemoteClickHouseExecutor (no chdb)
  └── Database::new(path, cfg)          → ChdbExecutor  [embedded feature]
        └── Connection (query execution)
              ├── query()              → QueryResult (rows of Value)  [embedded only]
              ├── query_to_sql()       → String (Cypher→SQL, always available)
              ├── export()             → file (Parquet/CSV/JSON/NDJSON)
              ├── query_remote()       → QueryResult (via remote ClickHouse)
              ├── query_graph()        → GraphResult (structured nodes + edges)
              ├── query_remote_graph() → GraphResult (remote → structured)
              └── store_subgraph()     → StoreStats (GraphResult → local tables)
```

This crate is the **foundation** consumed by all language bindings:
- `clickgraph-ffi` wraps it via UniFFI for Go and Python
- `clickgraph-py` wraps it via UniFFI for Python (pure Python wrapper over `clickgraph-ffi`)

## File Overview

```
src/
├── lib.rs           (50 lines)   ← Crate root, re-exports
├── database.rs      (260 lines)  ← Database: schema loading, chdb session, remote executor, sql_only mode
├── connection.rs    (900+ lines) ← Connection: query execution, remote queries, graph results, store_subgraph
├── query_result.rs  (162 lines)  ← QueryResult: row iteration, column access
├── value.rs         (175 lines)  ← Value enum: typed values from chdb JSON results
├── export.rs        (297 lines)  ← Export: file output (Parquet, CSV, TSV, JSON, NDJSON)
├── error.rs         (24 lines)   ← EmbeddedError type
tests/
├── integration.rs   (269 lines)  ← Stub-based tests (no chdb required)
├── chdb_e2e.rs      (313 lines)  ← Real chdb tests (CLICKGRAPH_CHDB_TESTS=1)
```

## Key Design Decisions

### chdb Single-Session Constraint
chdb supports **only one session per process**. The `Database` type manages this
constraint. In tests, a `LazyLock<&'static SharedFixture>` with `Box::leak()`
ensures the session is never dropped (avoids chdb SIGABRT on cleanup).

### Schema Source Resolution
The `source` field in schema YAML supports multiple URI schemes:
- Local files: `/path/to/data.parquet` → auto-detected format
- S3: `s3://bucket/key.parquet`
- Iceberg: `iceberg+s3://bucket/table/`
- Delta Lake: `delta+s3://bucket/table/`
- Escape hatch: `table_function:file(...)` → passed through verbatim

Resolution happens in `src/executor/source_resolver.rs` (main crate), called
during `Database::new()` → `load_schema_sources()`.

### `embedded` Feature Flag (opt-in)
The `embedded` feature gates all chdb-dependent code:
- `Database::new()`, `in_memory()`, `from_schema()` — require `embedded`
- `StorageCredentials` re-export — requires `embedded`
- `SystemConfig.credentials` field — requires `embedded`

Without the feature, only `sql_only` and `new_remote` constructors are available.
**`clickgraph-ffi`** and **`clickgraph-tck`** enable the feature; **`clickgraph-tool`** does not.

### sql_only Mode
`Database::sql_only()` creates a database that can translate Cypher→SQL without
a chdb session. Used for testing, SQL preview, and lightweight tooling. Calling
`query()` on an sql_only connection will fail; use `query_to_sql()` instead.

### Remote Mode (no chdb)
`Database::new_remote(schema_path, RemoteConfig)` connects to an external ClickHouse
cluster without starting a chdb session. Cypher is translated locally and executed
remotely via `RoleConnectionPool`. Use `Connection::query_remote()` to execute queries.
This is the execution backend used by the `cg` CLI tool.

### Hybrid Remote Query + Local Storage
When `SystemConfig.remote` is set to a `RemoteConfig` (used alongside the `embedded`
feature), `Database::from_schema()` creates both a `ChdbExecutor` (local) and a
`RemoteClickHouseExecutor`. This enables `query_remote()` and `query_remote_graph()`
to execute Cypher against a remote cluster, then store results locally via `store_subgraph()`.

### load_graph_schema() Helper
Shared by `Database::new()`, `sql_only()`, and `new_remote()` to avoid duplication.
Reads YAML → `GraphSchemaConfig` → `GraphSchema`.

## Conventions

- **Error handling**: All public methods return `Result<T, EmbeddedError>`
- **Thread safety**: `Database` is `Send + Sync`; `Connection` borrows `&Database`
- **JSON parsing**: chdb returns JSON; `Value::from_json()` converts to typed values
- **Column ordering**: Uses `serde_json` with `preserve_order` to maintain column order

## Test Gating

chdb e2e tests are gated behind `CLICKGRAPH_CHDB_TESTS=1` environment variable.
Without it, `cargo test` runs only the stub-based integration tests. This keeps
routine development fast and avoids requiring chdb for CI.
