---
description: Translate natural language to Cypher and query a ClickGraph database
---

The user wants to query a graph database. Their request: $ARGUMENTS

Use the `cg` CLI tool to translate this to Cypher and optionally execute it.

## Step 1 — Locate the schema

Find the schema file in this priority order:
1. `$CG_SCHEMA` environment variable
2. `schema.yaml` or `schemas/*.yaml` in the current working directory
3. `schema.path` in `~/.config/cg/config.toml`

If none found, ask the user to supply the path or set `CG_SCHEMA`.

## Step 2 — Generate Cypher

```bash
cg --schema <path> nl "$ARGUMENTS"
```

Display the generated Cypher in a code block. If the command fails, show the error and suggest checking the schema path and LLM API key (`ANTHROPIC_API_KEY` or `CG_LLM_API_KEY`).

## Step 3 — Show SQL translation

```bash
cg --schema <path> sql "<generated cypher>"
```

Show the SQL — this is useful for understanding what ClickHouse will execute and for debugging.

## Step 4 — Execute

If `CG_CLICKHOUSE_URL` is set (or `clickhouse.url` is in `~/.config/cg/config.toml`), execute:

```bash
cg --schema <path> --clickhouse "$CG_CLICKHOUSE_URL" query "<generated cypher>"
```

If ClickHouse is not configured, present the Cypher and SQL and suggest how to run it:
- `cg query --clickhouse http://localhost:8123 "<cypher>"`
- Via ClickGraph server: `curl -X POST http://localhost:8080/query -d '{"query": "<cypher>"}'`

## Notes

- `cg nl` requires an LLM API key: `ANTHROPIC_API_KEY` (default) or `OPENAI_API_KEY` with `CG_LLM_PROVIDER=openai`
- Property names in Cypher follow the schema mappings, not raw ClickHouse column names
- Use `cg schema show --schema <path>` to inspect available node labels and relationship types
