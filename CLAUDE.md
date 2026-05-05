# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ClickGraph is a **read-only graph query engine** for ClickHouse, written in Rust. It translates Cypher queries into ClickHouse SQL. Write operations (CREATE, SET, DELETE, MERGE) are explicitly out of scope.

**Modes of operation:**
- **Server mode** — HTTP (axum) + Bolt v5.8 protocol servers, querying a remote ClickHouse instance
- **Embedded mode** — In-process serverless execution via chdb (ClickHouse embedded). Query Parquet, S3, Iceberg, Delta Lake directly without a running server
- **Remote mode** — Cypher translated locally, executed against an external ClickHouse (no chdb needed)
- **SQL-only mode** — Translate Cypher to SQL without executing (for debugging, testing, or external execution)

**Ground rules**: (1) Never change query semantics — honestly return what is asked, no more, no less. (2) No shortcuts — fully understand the processing flow before making changes. Quality over speed.

## Workspace Structure

```
clickgraph/                  # Main engine crate (Cypher parser, planner, SQL generator, server)
clickgraph-embedded/         # Embedded Rust API (Database/Connection/QueryResult, Kuzu-compatible)
clickgraph-ffi/              # UniFFI FFI layer (cdylib — single source of truth for all bindings)
clickgraph-go/               # Idiomatic Go bindings via cgo + UniFFI-generated C bridge
clickgraph-py/               # Pythonic wrapper over UniFFI-generated ctypes bridge
clickgraph-client/           # Interactive REPL client for querying ClickGraph servers (human use)
clickgraph-tool/             # cg CLI — agent/script-oriented tool (sql, validate, query, nl, schema)
```

**Workspace members** (in `Cargo.toml`): `clickgraph-client`, `clickgraph-embedded`, `clickgraph-ffi`, `clickgraph-tool`

Go and Python bindings are not Cargo workspace members — they consume `libclickgraph_ffi.so`.

## Build, Test, and Lint Commands

```bash
# Build
cargo build                        # Debug build (all workspace members)
cargo build --release              # Release build
cargo build -p clickgraph-ffi      # FFI shared library only

# Format (MANDATORY before push — CI will fail without this)
cargo fmt --all

# Lint
cargo clippy --all-targets

# Rust tests (~1,600 tests across workspace)
cargo test                         # All Rust tests
cargo test <test_name>             # Single test
cargo test -- --nocapture          # With output

# Python integration tests (~3,026 tests, requires running ClickHouse + ClickGraph server)
pytest tests/integration/
pytest tests/integration/test_optional_match.py -v   # Single file

# Go tests (sql_only, no chdb)
cd clickgraph-go && CGO_LDFLAGS="-L../target/debug" LD_LIBRARY_PATH="../target/debug" go test -v

# Python binding tests (sql_only, no chdb)
cd clickgraph-py && LD_LIBRARY_PATH="../target/debug" python3 -m pytest tests/test_bindings.py -v

# Pre-push checklist (all mandatory)
cargo fmt --all && cargo clippy --all-targets && cargo test

# Run server
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph

# Debug generated SQL without executing
curl -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n) RETURN n","sql_only":true}'

# cg CLI — agent/script-oriented tool (no server needed)
cg --schema schema.yaml sql "MATCH (n:Person) RETURN n.name"   # translate only
cg --schema schema.yaml validate "MATCH (n:Person) RETURN n"   # parse + plan check
cg --schema schema.yaml \
   --clickhouse http://localhost:8123 \
   query "MATCH (n:Person) RETURN n.name LIMIT 10"             # execute via remote CH
cg --schema schema.yaml nl "find people with more than 5 friends"  # NL → Cypher
cg --schema schema.yaml schema show                             # agent-friendly schema view
cg schema discover --clickhouse http://localhost:8123 \
   --database mydb --out schema.yaml                            # LLM-assisted discovery
```

## Architecture — Query Pipeline

```
Cypher Query → Parse → Plan → Optimize → Render → Generate SQL → Execute
```

| Stage | Module | Purpose |
|-------|--------|---------|
| Parse | `src/open_cypher_parser/` | Cypher → AST using `nom` combinators |
| Plan | `src/query_planner/` | AST → LogicalPlan (analysis, type inference, traversal planning) |
| Optimize | `src/query_planner/optimizer/` | Optimization passes (projection push-down, filter push-down) |
| Render | `src/render_plan/` | LogicalPlan → RenderPlan (CTEs, SELECT, FROM, JOINs) |
| Generate | `src/clickhouse_query_generator/` | RenderPlan → ClickHouse SQL string |
| Execute | `src/server/` | HTTP (axum) + Bolt v5.8 protocol servers, ClickHouse client |
| Schema | `src/graph_catalog/` | YAML graph schema management and validation |

### Key Submodules

- **`query_planner/analyzer/`** — Type inference (4-phase), view resolution, graph traversal planning
- **`query_planner/plan_ctx/`** — Query context, variable scoping
- **`render_plan/plan_builder_utils.rs`** — The largest file (~12K lines); WITH→CTE transformation, expression rewriting. Most regressions originate here. Always run full test suite after changes.
- **`render_plan/variable_scope.rs`** — Scope-aware variable resolution across WITH barriers
- **`render_plan/cte_manager/`** — CTE generation and management
- **`clickhouse_query_generator/to_sql_query.rs`** — Final SQL rendering, CTE flattening
- **`server/bolt_protocol/`** — Neo4j Bolt v5.8 wire protocol implementation
- **`server/query_context.rs`** — Task-local schema and variable registry

## Ecosystem Architecture

```
   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   │   Go App     │  │  Python App  │  │   Rust App   │  │  Agent/Script│
   │  (cgo)       │  │  (ctypes)    │  │  (direct)    │  │  (cg CLI)    │
   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
          │                 │                 │                 │
   clickgraph-go     clickgraph-py    clickgraph-embedded  clickgraph-tool
          │                 │           (sql_only/remote)       │
          └────────┬────────┘          (chdb: +embedded feat)   │
                   │                          │                 │
         ┌─────────▼──────────┐               └────────┬────────┘
         │  clickgraph-ffi    │                        │
         │ (libclickgraph_ffi │                        │
         │  .so / UniFFI)     │                        │
         └─────────┬──────────┘                        │
                   └──────────────────┬────────────────┘
                                      │
                           ┌──────────▼──────────┐
                           │  clickgraph (core)   │
                           │  Parser + Planner +  │
                           │  SQL Generator       │
                           └──────────┬──────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ ClickHouse / chdb    │
                           └─────────────────────┘
```

### FFI Layer (`clickgraph-ffi/`)

Single source of truth for all language bindings. Uses **UniFFI proc macros** (no `.udl` files). Exports: `Database`, `Connection`, `QueryResult`, `Row`, `Value`, `SystemConfig`, `ExportOptions`, `ClickGraphError`.

Adding a method here automatically becomes available to all bindings after regenerating.

**Regenerating bindings:**
```bash
# Python
uniffi-bindgen generate --library target/debug/libclickgraph_ffi.so --language python -o clickgraph-py/clickgraph/
mv clickgraph-py/clickgraph/clickgraph_ffi.py clickgraph-py/clickgraph/_ffi.py

# Go
uniffi-bindgen-go --library target/debug/libclickgraph_ffi.so --out-dir clickgraph-go/clickgraph_ffi/
```

### Embedded Mode (`clickgraph-embedded/`)

Core Rust crate with Kuzu-compatible sync API (`Database` → `Connection` → `QueryResult`). Three constructors:

| Constructor | Needs chdb? | Use case |
|---|---|---|
| `Database::sql_only(schema)` | No | Translate Cypher → SQL only |
| `Database::new_remote(schema, RemoteConfig)` | No | Execute against external ClickHouse |
| `Database::new(schema, SystemConfig)` | **Yes** (`embedded` feature) | In-process chdb execution |

The `embedded` feature flag is **opt-in** (default off). `clickgraph-ffi` and `clickgraph-tck` enable it; `clickgraph-tool` does not.

Schema `source:` field supports: local files, `s3://`, `iceberg+s3://`, `delta+s3://`, `table_function:...`.

### Go Bindings (`clickgraph-go/`)

Idiomatic Go API via cgo. Module: `github.com/genezhang/clickgraph-go`. Requires `libclickgraph_ffi.so` and `CGO_ENABLED=1`.

### Python Bindings (`clickgraph-py/`)

PyPI package: `clickgraph`. Thin wrapper over auto-generated UniFFI ctypes bridge (`_ffi.py` is auto-generated — never edit manually).

## Critical Architectural Rules

### 1. CTEs Are Flat — No Nesting
All CTEs must be top-level siblings under a single `WITH RECURSIVE`. Never nest CTE definitions inside another CTE body. Enforced by `flatten_all_ctes()` in `to_sql_query.rs`.

### 2. Variable Resolution: Forward Through Scope, Never Reverse
After a WITH→CTE barrier, downstream expressions reference CTE column names directly. No reverse mapping from DB columns back to CTE columns.

```
Before WITH:  p.name → Person.full_name        (DB column via schema mapping)
After WITH:   p.name → CTE1.p6_person_name     (CTE column via property_mapping)
```

### 3. Task-Local Schema Access
Query-processing code MUST access schema via task-local `QueryContext` (`get_current_schema()`), never directly from `GLOBAL_SCHEMAS`. GLOBAL_SCHEMAS is only for server init and admin endpoints.

### 4. Anchor-Aware Join Generation
JOIN ordering uses topological sort based on `anchor_connection`. When the anchor node is the right connection in OPTIONAL MATCH, FROM and JOIN order must be reversed.

### 5. WITH Clause Traversal Consistency
Five functions must agree on plan traversal for WITH processing (see `render_plan/AGENTS.md` §6). When adding a new `LogicalPlan` variant, ensure all five handle it:
- `has_with_clause_in_tree()`
- `plan_contains_with_clause()`
- `find_all_with_clauses_grouped()`
- `needs_processing()`
- `replace_with_clause_with_cte_reference_v2()`

### 6. CTE Column Naming
WITH CTE columns use unambiguous `p{N}_{alias}_{property}` format where N = character length of alias. Generated by `cte_column_name()` in `src/utils/cte_column_naming.rs`.

## Schema Discipline

**Always use the benchmark schema for development**: `benchmarks/social_network/schemas/social_benchmark.yaml`

Property names in Cypher may differ from ClickHouse column names (e.g., `u.name` → `full_name`, `u.email` → `email_address`). Schema defines these mappings.

Five schema variations exist: Standard, FK-edge, Denormalized, Polymorphic, Composite ID. Bug fixes should be tested against all relevant variations.

## Development Conventions

- **Branch naming**: `fix/`, `perf/`, `refactor/`, `test/`, `docs/`, `feature/` prefixes
- **Error handling**: `thiserror` for error types, `?` operator for propagation, no panics
- **Module pattern**: `mod.rs` as entry, separate files per component, `errors.rs` per module
- **Late-stage project**: Reuse existing code before writing new. Investigate thoroughly before claiming code is dead. Add regression tests for fixed bugs.
- **serde_json** uses `preserve_order` feature — column order in JSON responses must match SQL SELECT order

## Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `CLICKHOUSE_URL` | ClickHouse connection URL (server mode) |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | Credentials (server mode) |
| `GRAPH_CONFIG_PATH` | **Required for server mode** — YAML schema file path |
| `RUST_LOG` | Logging level (debug, info) |
| `CLICKGRAPH_THREAD_STACK_MB` | Tokio worker thread stack (default 128 MB) |
| `CLICKGRAPH_CHDB_TESTS` | Set to `1` to enable chdb e2e tests |
| `CLICKGRAPH_LLM_PROVIDER` | LLM provider for schema discovery (`anthropic` or `openai`) |
| `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | API keys for LLM schema discovery |
| `CG_SCHEMA` | Default schema file path for `cg` CLI |
| `CG_CLICKHOUSE_URL` | ClickHouse URL for `cg query` |
| `CG_CLICKHOUSE_USER` / `CG_CLICKHOUSE_PASSWORD` | Credentials for `cg query` |
| `CG_LLM_PROVIDER` | LLM provider for `cg nl` and `cg schema discover` |
| `CG_LLM_MODEL` / `CG_LLM_API_KEY` / `CG_LLM_BASE_URL` | LLM config for `cg` |

## Key Documentation Files

- **`STATUS.md`** — Single source of truth for project state
- **`CHANGELOG.md`** — Release history (Keep-a-Changelog format)
- **`DEV_QUICK_START.md`** — Essential developer workflow
- **`DEVELOPMENT_PROCESS.md`** — Detailed 6-phase development process
- **`.github/copilot-instructions.md`** — Comprehensive architecture guide
- **`*/AGENTS.md`** — Module-level architecture guides (in `src/`, `src/render_plan/`, `src/server/`, `clickgraph-ffi/`, `clickgraph-embedded/`, `clickgraph-tool/`, `clickgraph-go/`, `clickgraph-py/`, etc.)
- **`docs/wiki/cypher-language-reference.md`** — Primary feature documentation (must be updated for every feature)
