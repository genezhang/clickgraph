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
└── lib.rs  (366 lines)  ← Entire FFI surface: Database, Connection,
                            QueryResult, Row, Value, ExportOptions,
                            SystemConfig, ClickGraphError
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

### Error Mapping
`ClickGraphError` is a UniFFI-exported enum with variants:
- `DatabaseError` — schema loading, chdb session issues
- `QueryError` — Cypher parsing, SQL generation failures
- `ExportError` — file output failures

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
