# Schema Discovery

ClickGraph can automatically generate a graph schema YAML from your ClickHouse database metadata using an LLM (Large Language Model). This replaces the manual process of writing schema YAML by hand.

## Overview

Schema discovery works in three steps:

1. **Introspect** -- the server reads table metadata (columns, types, PKs, sample rows) from ClickHouse
2. **Generate** -- an LLM analyzes the metadata and produces a graph schema YAML
3. **Load** -- the generated YAML is loaded into the server for querying

The server formats the prompt (it owns the schema format spec + introspection data). The client makes the LLM call (it owns the API key and is user-facing).

## Quick Start

### Using the REPL client

```bash
# Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...

# Start the client
clickgraph-client --url http://localhost:8080

# Discover schema for a database
clickgraph-client :) :discover mydb
```

The client will:
1. Fetch table metadata from the server
2. Send it to Claude for analysis
3. Display the generated YAML
4. Offer to save to a file and/or load into the server

### Without an API key

If `ANTHROPIC_API_KEY` is not set, `:discover` falls back to `:introspect` which shows the raw table metadata. You can then write the YAML manually or use the `:design` wizard.

## Configuration

Two LLM API providers are supported:

### Anthropic (default)

```bash
export ANTHROPIC_API_KEY="sk-ant-api03-..."
```

### OpenAI-compatible (OpenAI, Ollama, vLLM, Together, Groq, etc.)

```bash
export CLICKGRAPH_LLM_PROVIDER="openai"
export OPENAI_API_KEY="sk-..."
# For local models:
# export CLICKGRAPH_LLM_API_URL="http://localhost:11434/v1/chat/completions"
# export CLICKGRAPH_LLM_MODEL="llama3.1:70b"
```

### All Variables

| Variable | Default | Description |
|---------------------|---------|-------------|
| `CLICKGRAPH_LLM_PROVIDER` | `anthropic` | `anthropic` or `openai` — controls API format and auth |
| `ANTHROPIC_API_KEY` | *(required if anthropic)* | Anthropic API key |
| `OPENAI_API_KEY` | *(required if openai)* | OpenAI or compatible API key |
| `CLICKGRAPH_LLM_MODEL` | `claude-sonnet-4-20250514` / `gpt-4o` | Model ID (default depends on provider) |
| `CLICKGRAPH_LLM_API_URL` | Provider-specific | API endpoint (override for proxy/local models) |
| `CLICKGRAPH_LLM_MAX_TOKENS` | `8192` | Maximum response tokens |

See [Configuration Guide](../configuration.md#llm-schema-discovery-configuration) for full details including proxy setup and local models.

## How It Works

### What the LLM receives

For each table in the database, the prompt includes:
- Table name
- Column names, types, PK flags, ORDER BY flags
- Row count
- 3 sample rows (as JSON)

### What the LLM produces

A complete ClickGraph schema YAML with:
- **Nodes** -- tables classified as graph entities, with clean property names
- **Edges** -- junction tables mapped as relationships
- **FK-edges** -- node tables with foreign key columns mapped as additional edges
- **Property mappings** -- abbreviated ClickHouse column names mapped to clean Cypher property names

### Why LLM over heuristics?

Heuristic approaches (pattern matching on column names) achieve ~15% accuracy on real-world schemas with non-standard naming conventions. An LLM achieves ~95% accuracy because it can:

- Expand abbreviations (`usr` -> User, `dept_code` -> Department)
- Detect FKs without `_id` suffix (`reporter`, `assignee` -> User references)
- Resolve cross-type FKs (`dept_code` String -> `dept.code` String PK)
- Generate meaningful edge names (MANAGED_BY, REPORTED_BY)
- Skip event/audit tables that shouldn't be graph entities

See [docs/design/llm-schema-discovery.md](../design/llm-schema-discovery.md) for the full comparison.

### Large schemas

For databases with 40+ tables, the prompt is automatically batched into multiple LLM calls. Each batch includes a cross-reference header so the LLM knows about tables in other batches.

## Client Commands Reference

| Command | Alias | Description |
|---------|-------|-------------|
| `:discover <db>` | `:disc` | LLM-powered schema discovery |
| `:introspect <db>` | `:i` | Show raw table metadata (no LLM) |
| `:design <db>` | `:d` | Interactive manual schema wizard |
| `:schemas` | `:s` | List loaded schemas |
| `:load <file>` | | Load schema from YAML file |

## Server API Endpoints

### POST /schemas/discover-prompt

Returns formatted LLM prompt(s) for a database. Used internally by the client.

**Request:**
```json
{
  "database": "mydb"
}
```

**Response:**
```json
{
  "database": "mydb",
  "total_tables": 7,
  "prompts": [
    {
      "system_prompt": "You are a database schema analyst...",
      "user_prompt": "## ClickHouse Tables (database: mydb)\n\n### users\n...",
      "table_count": 7,
      "estimated_tokens": 2500
    }
  ]
}
```

### POST /schemas/introspect

Returns raw table metadata (columns, types, PKs, sample data, structural suggestions).

**Request:**
```json
{
  "database": "mydb"
}
```

### POST /schemas/draft

Generates YAML from manual hints (used by `:design` wizard).

### POST /schemas/load

Loads a schema YAML into the server. See [API Reference](API-Reference-HTTP.md).

## Tips

- **Review before loading** -- always review the generated YAML before loading. The LLM may occasionally misclassify a table or miss an edge.
- **Edit and reload** -- save to a file, edit as needed, then `:load` the corrected version.
- **Cost** -- schema discovery is a one-time operation. A typical 10-table database costs ~$0.01-0.05 per call.
- **Proxy support** -- set `CLICKGRAPH_LLM_API_URL` to route through an API gateway or local proxy.
