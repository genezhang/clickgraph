# clickgraph-mcp — Agent Guide

> **Purpose**: A stdio [Model Context Protocol](https://modelcontextprotocol.io)
> server so MCP clients (Claude Desktop, Cursor, Claude Code) can discover
> ClickGraph schemas, run Cypher and inspect the generated SQL. It is a thin
> client over a *running* ClickGraph server's HTTP API: no Rust, no query
> logic of its own.

This is the HTTP-based path. The other MCP path needs no code of ours: the
third-party `mcp-neo4j-cypher` server talks to ClickGraph's Bolt port (see
`docs/wiki/AI-Assistant-Integration-MCP.md`).

## Architecture

```
MCP client (stdio)
  └── clickgraph_mcp/server.py   (MCPServer + 4 @mcp.tool functions)
        └── clickgraph_mcp/client.py   (urllib HTTP client, error text extraction)
              └── ClickGraph HTTP API (default http://localhost:7475)
```

## File Overview

```
clickgraph_mcp/
├── server.py    ← Tool definitions; maps ClickGraphError → MCP ToolError
├── client.py    ← ClickGraphClient: GET /schemas, POST /query, POST /query/sql
├── __main__.py  ← `python -m clickgraph_mcp`
tests/
├── conftest.py      ← FakeClickGraph: in-process HTTP server recording requests
├── test_client.py   ← exact request bodies, error shapes, env defaults
├── test_server.py   ← tool registration, schema precedence, one real stdio round-trip
pyproject.toml       ← package `clickgraph-mcp`, script `clickgraph-mcp`, dep `mcp>=2,<3`
```

## Tools

| Tool | ClickGraph endpoint | Notes |
|---|---|---|
| `list_schemas` | `GET /schemas` | Names and node/relationship counts only — use `get_schema` for properties |
| `get_schema` | `POST /query` (`apoc.meta.schema()`) | Labels, relationship types, properties; payload is under `records` |
| `run_cypher` | `POST /query` (`format: JSONEachRow`) | Rows are under `results`; `CALL` procedures answer under `records` |
| `explain_query` | `POST /query/sql` | Generated SQL, never executes |

All four carry `readOnlyHint` (ClickGraph rejects writes). Keep it that way:
adding a tool that can change data would contradict the hint.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CLICKGRAPH_URL` | `http://localhost:7475` | ClickGraph HTTP endpoint |
| `CLICKGRAPH_SCHEMA` | unset | Schema when a call names none (else the server's `default`) |
| `CLICKGRAPH_TIMEOUT` | `30` | Request timeout in seconds |
| `CLICKGRAPH_MAX_ROWS` | `1000` | Max rows in a payload; `0` is unlimited |
| `CLICKGRAPH_VIEW_PARAMETERS` | unset | JSON object forwarded as `view_parameters` |
| `CLICKGRAPH_ROLE` | unset | Role forwarded on every query |

**The default port is 7475, not 8080.** The server's HTTP default moved from
8080 to 7475 in #310; `src/config.rs`, the Dockerfile and the README agree. A
test pins the client default so it cannot drift back.

## Testing

```bash
pip install -e './clickgraph-mcp[test]'
python -m pytest clickgraph-mcp/tests -q      # no ClickHouse or server needed
```

For a live check, run a server (`cargo run --bin clickgraph`, see the root
`CLAUDE.md`) and drive `clickgraph-mcp` with an MCP `ClientSession` over
stdio, or point any MCP client at it.

## Gotchas

- `/query` errors are plain text on some routes and JSON (`{"error": ...}`) on
  others; `client._error_text` handles both. Do not assume one shape.
- Tool failures must surface ClickGraph's own message (`ToolError`), not a
  generic one: agents repair their Cypher from it.
- `CLICKGRAPH_MAX_ROWS` caps `results` and `records`. Over the cap, the payload
  keeps the first N rows and adds `truncated`, `returned`, `total` and a
  `message` telling the agent to add a `LIMIT`. Never truncate silently and
  never inject a `LIMIT` into the Cypher.
- `CLICKGRAPH_VIEW_PARAMETERS` and `CLICKGRAPH_ROLE` are forwarded on `/query`
  and `/query/sql` so tenant-scoped parameterized views work. They are
  environment-only by design: tenant isolation must be set by the operator,
  never chosen by the agent as a tool argument.
