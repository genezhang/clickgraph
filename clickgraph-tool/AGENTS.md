# clickgraph-tool — Agent Guide

> **Purpose**: Agent/script-oriented CLI (`cg` binary). Translates and executes
> Cypher queries without requiring a running ClickGraph server. Designed for
> agentic callers, CI pipelines, and developer scripting.
>
> Compare with `clickgraph-client` (human REPL, connects to a running server).

## Commands

```
cg sql       --schema <file>  "<cypher>"              # Translate Cypher → SQL (no execution)
cg validate  --schema <file>  "<cypher>"              # Parse + plan check
cg query     --schema <file>  --clickhouse <url> "<cypher>"  # Execute via remote ClickHouse
cg query     --schema <file>  --sql-only "<cypher>"   # Same as cg sql
cg nl        --schema <file>  "<natural language>"    # NL → Cypher via LLM
cg nl        --schema <file>  --execute "<...>"       # NL → Cypher → execute
cg schema show   --schema <file> [--format text|json] # Compact agent-friendly schema view
cg schema validate [<file>]                           # Structural validation (no CH needed)
cg schema discover --clickhouse <url> --database <db> --out <file>  # LLM-assisted schema gen
cg schema diff <old.yaml> <new.yaml>                  # Node/relationship diff
```

### Dialect (`--dialect`, `CG_DIALECT`)

```
cg --dialect databricks sql      --schema <file> "<cypher>"  # Emit Spark SQL
cg --dialect databricks validate --schema <file> "<cypher>"  # Plan under Spark dialect
cg --dialect databricks query    --schema <file> --sql-only "<cypher>"  # Spark SQL via query path
```

Values: `clickhouse` (default) or `databricks`. Currently used by the SQL-emission
paths (`sql`, `validate`, `query --sql-only`); executing against Databricks via
`cg query` (without `--sql-only`) is not yet wired — use `--sql-only` and pipe
into your warehouse.

The dialect can also be set in `~/.config/cg/config.toml` as a top-level key:

```toml
dialect = "databricks"   # or "clickhouse"
```

Precedence: `--dialect` flag > `CG_DIALECT` env > `config.toml` > default
(`clickhouse`). An unrecognized config-file value warns to stderr and falls
back to the default rather than failing silently.

## Architecture

```
cg (main.rs — clap CLI)
  ├── commands/query.rs    → clickgraph-embedded (Database::sql_only / new_remote)
  ├── commands/schema.rs   → clickgraph core (GraphSchemaConfig, SchemaDiscovery, llm_prompt)
  ├── commands/nl.rs       → llm.rs + schema_fmt.rs + commands/query.rs
  ├── schema_fmt.rs        → compact text/JSON formatter from GraphSchema
  ├── llm.rs               → LlmClient (Anthropic / OpenAI-compatible)
  └── config.rs            → CgConfig (flags > env vars > ~/.config/cg/config.toml)
```

## File Overview

```
src/
├── main.rs          (~160 lines)  ← clap CLI definition and routing
├── config.rs        (~120 lines)  ← CgConfig: schema, CH URL, LLM settings
├── llm.rs           (~185 lines)  ← LlmClient: Anthropic + OpenAI-compatible API calls
├── schema_fmt.rs    (~130 lines)  ← GraphSchema → compact text / JSON for LLMs
└── commands/
    ├── mod.rs         (5 lines)   ← re-exports
    ├── query.rs      (~130 lines) ← sql, validate, query (uses clickgraph-embedded)
    ├── schema.rs     (~200 lines) ← show, validate, discover, diff
    └── nl.rs          (~60 lines) ← NL → Cypher via LLM
```

## Key Design Decisions

### No chdb Dependency
`clickgraph-tool` depends on `clickgraph-embedded` **without** the `embedded`
feature, so chdb is never compiled in. All execution goes through:
- `Database::sql_only()` for Cypher→SQL translation
- `Database::new_remote()` for executing against an external ClickHouse

This keeps compile times fast and removes the native library requirement.

### No ClickGraph Server Required
`cg schema discover` calls `SchemaDiscovery::introspect()` directly with a
`clickhouse::Client` — no server hop needed. The user provides ClickHouse
credentials via flags, env vars, or config file.

### Tokio Runtime Compatibility
`clickgraph-embedded` creates its own internal Tokio runtime for blocking calls.
To avoid the "cannot start a runtime from within a runtime" panic:
- Sync embedded calls use `tokio::task::block_in_place()`
- Remote query construction uses `tokio::task::spawn_blocking()`

### LLM Configuration Priority
1. CLI flags (`--llm-provider`, etc.)
2. `CG_LLM_*` env vars
3. `CLICKGRAPH_LLM_*` env vars (shared with `clickgraph-client`)
4. `~/.config/cg/config.toml` `[llm]` section
5. Defaults: Anthropic + `claude-sonnet-4-6`

Supported providers: `anthropic` (default), `openai` (any OpenAI-compatible
endpoint — set `CG_LLM_BASE_URL` for OpenRouter, Groq, Ollama, etc.)

### Schema Formatter (`schema_fmt.rs`)
Produces Cypher-native notation optimised for LLM consumption:
```
Graph: social_network

Node Labels:
  Person  {id: Int64, name: String, ...}

Relationships:
  (:Person)-[:KNOWS]-(:Person)     # undirected
  (:Person)-[:LIKES]->(:Post)      # directed

Notes:
  - Property names in Cypher may differ from ClickHouse column names
  - Undirected relationships (use -[]-): KNOWS
```
Use `--format json` for machine-readable output from `cg schema show`.

## Configuration File

`~/.config/cg/config.toml`:
```toml
[schema]
path = "/path/to/default/schema.yaml"

[clickhouse]
url = "http://localhost:8123"
user = "default"
password = ""
database = "mydb"  # optional

[llm]
provider = "anthropic"       # or "openai"
model = "claude-sonnet-4-6"  # model override
api_key = "sk-..."           # overrides ANTHROPIC_API_KEY / OPENAI_API_KEY
base_url = "https://..."     # override for OpenAI-compatible endpoints
max_tokens = 8192
```

## Environment Variables

| Variable | Purpose |
|---|---|
| `CG_SCHEMA` | Default schema YAML path |
| `CG_CLICKHOUSE_URL` | ClickHouse URL for `cg query` |
| `CG_CLICKHOUSE_USER` / `CG_CLICKHOUSE_PASSWORD` | ClickHouse credentials |
| `CG_CLICKHOUSE_DATABASE` | Target database |
| `CG_LLM_PROVIDER` | `anthropic` or `openai` |
| `CG_LLM_MODEL` | Model name override |
| `CG_LLM_API_KEY` | API key (falls back to `ANTHROPIC_API_KEY` / `OPENAI_API_KEY`) |
| `CG_LLM_BASE_URL` | Endpoint override for OpenAI-compatible APIs |
| `CG_LLM_MAX_TOKENS` | Token limit (default 8192) |

`CLICKGRAPH_LLM_PROVIDER` / `CLICKGRAPH_LLM_MODEL` / `CLICKGRAPH_LLM_API_URL` are
also honoured for compatibility with `clickgraph-client`.

## Phase 2 (Planned)

- `cg serve` — start embedded HTTP/Bolt server backed by chdb (replaces deploying the full server for embedded use)
- `cg --server URL` — route queries to a remote ClickGraph server (replaces `clickgraph-client` for scripting)
