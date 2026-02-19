# Top-Level Source Files — Agent Guide

> **Purpose**: Application entry point, crate root, and configuration.
> These three files wire everything together.

## ⚠️ Cross-Cutting SQL Rules (All Agents MUST Know)

### CTEs Are Flat — One `WITH RECURSIVE`, All CTEs Top-Level

Every generated SQL query must follow this structure:

```sql
WITH RECURSIVE          -- once, at the very top (only if any CTE is recursive)
  cte_1 AS (...),       -- all CTEs are siblings, comma-separated
  cte_2 AS (...),       -- dependency order: if B references A, A comes first
  cte_3 AS (...)        -- CTE bodies reference sibling CTEs like tables
SELECT ... FROM cte_3   -- final query references the CTEs
```

**Never** nest a CTE definition inside another CTE body or subquery.
**Never** emit a second `WITH RECURSIVE` anywhere in the query.
This is enforced by `flatten_all_ctes()` in `clickhouse_query_generator/to_sql_query.rs`.

### Variable Resolution: Forward Through Scope, Never Reverse

**Rule**: Variable references always resolve FORWARD to the nearest enclosing scope's
column names. After a WITH→CTE barrier, downstream expressions reference CTE columns
directly. There is no "reverse mapping" from DB columns back to CTE columns.

**Why**: Each WITH creates a CTE. A CTE is just a table. Downstream SQL reads from
the CTE using CTE column names. The original DB table columns are invisible after
the scope barrier — just like you can't see inside a table's implementation.

```
Cypher: MATCH (p:Person) WITH p RETURN p.name

Scope 1 (MATCH):  p → Person table, p.name → Person.full_name  (DB column)
   ↓ WITH barrier → CTE1 (SELECT Person.full_name AS p6_person_name ...)
Scope 2 (RETURN): p → CTE1, p.name → CTE1.p6_person_name       (CTE column)
```

**Downstream expressions should be built with CTE column names from the start.**
The `property_mapping` in `cte_schemas` provides the forward mapping:
`(cypher_alias, cypher_property)` → `cte_column_name`.

**Do NOT**:
- Resolve to DB columns first, then reverse-map to CTE columns
- Bake variable names into opaque SQL strings before scope processing
- Use `reverse_mapping: HashMap<(alias, db_col), cte_col>` (architectural debt)

See `render_plan/AGENTS.md` §10 for the full architecture description.

## File Overview

```
src/
├── lib.rs    (40 lines)  ← Crate root: module declarations, debug macros
├── main.rs   (74 lines)  ← Binary entry point: CLI parsing, server startup
└── config.rs (191 lines) ← Server configuration: CLI, env vars, validation
```

**Total**: ~305 lines

## Key Files

### lib.rs — Crate Root

Declares all top-level modules and provides debug macros:

```rust
pub mod utils;
pub mod clickhouse_query_generator;
pub mod config;
pub mod graph_catalog;
pub mod open_cypher_parser;
pub mod packstream;
pub mod procedures;
pub mod query_planner;
pub mod render_plan;
pub mod server;
```

**Debug macros**:
- `debug_print!(...)` — `eprintln!` only in debug builds, zero-cost in release
- `debug_println!(...)` — `println!` only in debug builds, zero-cost in release

**Note**: The `testing` module is NOT declared here — it's only reachable from
test code within submodules.

### main.rs — Binary Entry Point

Uses `clap` for CLI argument parsing:

| Argument | Default | Description |
|----------|---------|-------------|
| `--http-host` | `0.0.0.0` | HTTP server bind address |
| `--http-port` | `8080` | HTTP server port |
| `--disable-bolt` | `false` | Disable Bolt protocol server |
| `--bolt-host` | `0.0.0.0` | Bolt server bind address |
| `--bolt-port` | `7687` | Bolt server port |
| `--max-cte-depth` | `100` | Max recursive CTE depth for variable-length paths |
| `--validate-schema` | `false` | Validate YAML schema against ClickHouse on startup |
| `--daemon` | `false` | Run as background daemon |

| `--neo4j-compat-mode` | `false` | Masquerade as Neo4j for tool compatibility |

**Runtime configuration**: `main.rs` builds a custom tokio runtime with enlarged worker thread stacks (128 MB default, configurable via `CLICKGRAPH_THREAD_STACK_MB` env var). This prevents stack overflow in deeply recursive plan traversal (e.g., bidirectional + WITH chains + UNWIND).

**Flow**: Parse CLI → Convert to `CliConfig` → Build `ServerConfig` → Build tokio runtime → `runtime.block_on(async_main())`

**Logger**: `env_logger` with default level `debug`, overridable via `RUST_LOG` env var.

### config.rs — Server Configuration

`ServerConfig` struct with validation via `validator` crate:
- Port range validation (1-65535)
- Non-empty host validation
- CTE depth range (1-1000)

**Configuration sources** (in priority order):
1. CLI arguments (`from_cli()`)
2. Environment variables (`from_env()`)
3. YAML file (`from_yaml_file()`)
4. Default values

**Environment variables**:
| Variable | Default | Maps to |
|----------|---------|---------|
| `CLICKGRAPH_HOST` | `0.0.0.0` | `http_host` |
| `CLICKGRAPH_PORT` | `8080` | `http_port` |
| `CLICKGRAPH_BOLT_HOST` | `0.0.0.0` | `bolt_host` |
| `CLICKGRAPH_BOLT_PORT` | `7687` | `bolt_port` |
| `CLICKGRAPH_BOLT_ENABLED` | `true` | `bolt_enabled` |
| `CLICKGRAPH_MAX_CTE_DEPTH` | `100` | `max_cte_depth` |
| `CLICKGRAPH_VALIDATE_SCHEMA` | `false` | `validate_schema` |
| `CLICKGRAPH_THREAD_STACK_MB` | `128` | Tokio worker thread stack size (MB). Increase for complex queries in debug builds. |

**Error handling**: Uses `thiserror` with `ConfigError` enum covering env var errors,
parse errors, and validation errors.

## Critical Invariants

### 1. Bolt Enabled by Default
The CLI uses `--disable-bolt` (inverted flag). `bolt_enabled = !cli.disable_bolt`.
The config struct stores the positive `bolt_enabled: bool`.

### 2. Module Declaration Order
The module declaration order in `lib.rs` doesn't affect compilation, but `utils`
is declared first because other modules depend on it.

### 3. Config Validation
`ServerConfig` is validated after construction from any source. Invalid configs
cause the server to exit with error code 1 (in `main.rs`).

## Dependencies

**What these files use**:
- `clap` — CLI argument parsing (main.rs)
- `env_logger` — logging initialization (main.rs)
- `validator` — struct validation (config.rs)
- `serde` / `serde_yaml` — YAML config deserialization (config.rs)
- `serde_json` (with **`preserve_order`** feature) — JSON serialization maintaining column order
- `thiserror` — error types (config.rs)

**Key Feature**: `serde_json` `preserve_order` feature uses `IndexMap` instead of `BTreeMap`, maintaining insertion order in JSON responses. Without this, ClickHouse's JSONEachRow column order (matching SQL SELECT order) would be alphabetized during parsing, breaking Neo4j Browser and API expectations.

**What uses these files**:
- `config::ServerConfig` is used by `server::run_with_config()`
- `lib.rs` macros (`debug_print!`, `debug_println!`) are used throughout the crate

## Testing Guidance

- `config.rs` has 4 unit tests: default validation, invalid port, invalid CTE depth, empty host
- `main.rs` has no tests (integration tested via server startup)
- Run with: `cargo test --lib config`
