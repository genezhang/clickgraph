# Top-Level Source Files — Agent Guide

> **Purpose**: Application entry point, crate root, and configuration.
> These three files wire everything together.

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

**Flow**: Parse CLI → Convert to `CliConfig` → Build `ServerConfig` → `server::run_with_config()`

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
- `thiserror` — error types (config.rs)

**What uses these files**:
- `config::ServerConfig` is used by `server::run_with_config()`
- `lib.rs` macros (`debug_print!`, `debug_println!`) are used throughout the crate

## Testing Guidance

- `config.rs` has 4 unit tests: default validation, invalid port, invalid CTE depth, empty host
- `main.rs` has no tests (integration tested via server startup)
- Run with: `cargo test --lib config`
