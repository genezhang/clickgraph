# testing Module — Agent Guide

> **Purpose**: Mock implementations for unit testing other modules.
> **Status**: Partially implemented — `schema` and `query` submodules are declared but missing.
> **Not registered in `lib.rs`** — only used via `crate::testing::` in test code.

## Module Architecture

```
testing/
├── mod.rs        (11 lines) ← Module declarations (clickhouse, schema, query)
└── clickhouse.rs (63 lines) ← MockClickHouse using mockall
```

**Note**: `mod.rs` declares `pub mod schema;` and `pub mod query;` but those files
do **not** exist. This module is not included in `lib.rs`, so it only compiles
when referenced from test code (currently only `query_planner/tests/integration_tests.rs`).

**Total**: ~74 lines (of existing code)

## Key Files

### mod.rs — Module Declarations
```rust
pub mod clickhouse;
pub mod schema;   // ⚠️ FILE DOES NOT EXIST
pub mod query;    // ⚠️ FILE DOES NOT EXIST
```

### clickhouse.rs — Mock ClickHouse Client
Uses `mockall` crate to create a `MockClickHouse` with:
- `query(&str) → Result<Vec<HashMap<String,String>>>` — mock query execution
- `get_table_schema(&str) → Result<Vec<ColumnInfo>>` — mock schema introspection

`create_mock_client()` returns a pre-configured mock with:
- Table schema for "users" table (user_id, full_name, age)
- Query response for queries containing "users" (returns 2 rows)

## Critical Invariants

### 1. Not in lib.rs
This module is **not** declared in `src/lib.rs`. It's only reachable from test code
that uses `crate::testing::clickhouse::create_mock_client()`.

### 2. Missing Submodules
`schema` and `query` submodules are declared but don't exist as files. This will
cause compilation errors if the module is ever included unconditionally. Currently
safe because it's only used in `#[cfg(test)]` contexts.

## Dependencies

**What this module uses**:
- `mockall` crate — mock generation
- `async_trait` — async mock support
- `graph_catalog::schema_validator::ColumnInfo` — schema column type

**What uses this module**:
- `query_planner/tests/integration_tests.rs` — uses `create_mock_client()`

## Testing Guidance

- This module IS test infrastructure — it provides mocks for other tests
- Run dependent tests with: `cargo test --lib query_planner`
- If adding new mocks, consider whether `graph_catalog/testing/` (separate module) is more appropriate

## When to Modify

- **Adding mock capabilities**: Extend `MockClickHouse` expectations
- **New test fixtures**: Add pre-configured mock factories
- **Fix compilation**: Either create `schema.rs`/`query.rs` or remove the `pub mod` declarations
