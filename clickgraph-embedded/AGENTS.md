# clickgraph-embedded — Agent Guide

> **Purpose**: Core embedded engine crate. Provides `Database`, `Connection`,
> `QueryResult`, and `Value` types that wrap ClickGraph's Cypher→SQL pipeline
> with a chdb (embedded ClickHouse) backend for serverless execution.

## Architecture

```
Database (schema loading + chdb session)
  └── Connection (query execution)
        ├── query()        → QueryResult (rows of Value)
        ├── query_to_sql() → String (Cypher→SQL only, no chdb)
        └── export()       → file (Parquet/CSV/JSON/NDJSON)
```

This crate is the **foundation** consumed by all language bindings:
- `clickgraph-ffi` wraps it via UniFFI for Go and Python
- `clickgraph-py` wraps it via PyO3 for Python (alternative)

## File Overview

```
src/
├── lib.rs           (50 lines)   ← Crate root, re-exports
├── database.rs      (212 lines)  ← Database: schema loading, chdb session, sql_only mode
├── connection.rs    (776 lines)  ← Connection: query execution, SQL generation pipeline
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

### sql_only Mode
`Database::sql_only()` creates a database that can translate Cypher→SQL without
a chdb session. Used for testing and SQL preview. Calling `query()` on an
sql_only connection will fail; use `query_to_sql()` instead.

### load_graph_schema() Helper
Shared by both `Database::new()` and `Database::sql_only()` to avoid duplication.
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
