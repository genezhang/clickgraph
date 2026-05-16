# clickgraph-ffi — Agent Guide

> **Purpose**: UniFFI foreign function interface crate. Exposes `clickgraph-embedded`
> types to non-Rust languages (Go, Python) through a single shared library
> (`libclickgraph_ffi.so`). This is the **single source of truth** for all
> language bindings.

## Architecture

```
clickgraph-embedded (Rust API)
  └── clickgraph-ffi (UniFFI bridge — this crate)
        ├── libclickgraph_ffi.so  (shared library)
        ├── → clickgraph-go/      (Go via cgo + UniFFI)
        └── → clickgraph-py/ (Python via ctypes + UniFFI)
```

## File Overview

```
src/
└── lib.rs  (680+ lines) ← Entire FFI surface: Database, Connection,
                            QueryResult, GraphResult, Row, Value,
                            GraphNode, GraphEdge, StoreStats, RemoteConfig,
                            ExportOptions, SystemConfig, ClickGraphError
Cargo.toml               ← UniFFI dependency + cdylib crate type
```

## Key Design Principles

### One Crate, All Languages
Every language binding (Go, Python, future languages) uses the **same** FFI
definitions. Adding a method here automatically makes it available to all bindings
after regenerating the language-specific glue code.

### UniFFI Annotations
The crate uses `#[uniffi::export]` proc macros on impl blocks. UniFFI generates:
- C-compatible function symbols in the shared library
- Language-specific binding code via `uniffi-bindgen generate`

### Format Parsing at FFI Layer
Export format names (`parquet`, `csv`, `ndjson`, etc.) are parsed in `parse_format()`
within this crate, not in the language wrappers. This centralizes format validation.

### Graph Result Types
`GraphResult` is a UniFFI-exported object wrapping the embedded `GraphResult`.
It exposes `nodes() → Vec<GraphNode>`, `edges() → Vec<GraphEdge>`,
`node_count()`, `edge_count()`. `GraphNode` and `GraphEdge` are UniFFI records.
`StoreStats` is a record with `nodes_stored` and `edges_stored` counts.
`RemoteConfig` is a record for configuring remote ClickHouse connections.
`DatabricksConfig` (behind the `databricks` cargo feature) is the analogue for
Databricks SQL Warehouses; expose via `Database::open_databricks(...)`.

### Error Mapping
`ClickGraphError` is a UniFFI-exported enum with variants:
- `DatabaseError` — schema loading, chdb session issues
- `QueryError` — Cypher parsing, SQL generation failures
- `ExportError` — file output failures
- `ValidationError` — property/schema validation errors

All map from `EmbeddedError` via `From` impl.

## Conventions

- **Version match**: The `uniffi` crate version in `Cargo.toml` (0.29) must exactly
  match the `uniffi-bindgen` tool version used to generate bindings
- **Never edit generated code**: `_ffi.py` and Go UniFFI files are auto-generated;
  regenerate with `uniffi-bindgen generate --library target/debug/libclickgraph_ffi.so`
- **Row structure**: `Row { columns: Vec<String>, values: Vec<Value> }` — parallel
  arrays for column names and values, converted to dicts/maps by language wrappers
- **Value enum**: `Null | Bool(bool) | Int64(i64) | Float64(f64) | String(String) | List | Map`

## Adding New API Surface

1. Add the method to the appropriate type in `clickgraph-embedded`
2. Add a wrapper in `clickgraph-ffi/src/lib.rs` with `#[uniffi::export]`
3. Rebuild: `cargo build -p clickgraph-ffi`
4. Regenerate Go: `uniffi-bindgen-go ...`
5. Regenerate Python: `uniffi-bindgen generate --library ... --language python`
6. Update language-specific wrapper classes if needed

### Feature-Gated Surface (Databricks)

When the FFI is built with `--features databricks`, the cdylib additionally
exports `DatabricksConfig` and `Database.open_databricks(schema, config)`.
Distributions targeting Databricks users should:

```bash
cargo build -p clickgraph-ffi --release --features databricks
uniffi-bindgen generate --library target/release/libclickgraph_ffi.so \
    --language python -o clickgraph-py/clickgraph/
# (mv clickgraph_ffi.py _ffi.py per existing convention)
uniffi-bindgen-go --library target/release/libclickgraph_ffi.so \
    --out-dir clickgraph-go/clickgraph_ffi/
```

If you skip `--features databricks` at build time, the resulting `_ffi.py` /
Go bindings simply won't carry the Databricks symbols — callers will hit
`AttributeError` / undefined method, not a runtime crash. Pre-built wheels &
Go modules should always be built with the feature on.
