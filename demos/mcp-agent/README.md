# MCP agent demo — an AI agent queries your warehouse graph

Point an MCP-capable AI assistant (Claude Desktop, Cursor, or the Claude Code CLI)
at a ClickGraph or DeltaGraph Bolt endpoint, and it can **discover the graph schema
and run Cypher traversals against your warehouse as a live tool** — no graph
database, no ETL, no custom glue. ClickGraph is a **drop-in Neo4j MCP target**: it
speaks the Bolt protocol and answers `apoc.meta.schema()`, so the mainstream
`mcp-neo4j-cypher` server works unmodified.

The same setup works whether the warehouse is **ClickHouse** (ClickGraph) or
**Databricks / Spark SQL** (DeltaGraph) — only the Bolt port changes.

## What you need

- A running ClickGraph or DeltaGraph server with Bolt enabled (see the
  `demos/neo4j-browser/` runbook for the social-graph servers this uses).
- [`uv`](https://docs.astral.sh/uv/) on your PATH (provides `uvx`) — **no Node.js**.
- One MCP client: Claude Desktop, Cursor, or the `claude` CLI.

## 1 · Register the MCP server

`mcp-neo4j-cypher` (Neo4j Labs, on PyPI) is the verified server. Launch it with
`--read-only` — ClickGraph is read-only, so the write tool is inert by design.

### Claude Code CLI (quickest to verify)

```bash
claude mcp add clickgraph -- \
  uvx --from mcp-neo4j-cypher mcp-neo4j-cypher \
  --db-url bolt://localhost:7687 --username neo4j --password password \
  --read-only --transport stdio

claude mcp list        # → clickgraph: ... ✔ Connected
```

For the **Databricks** graph, point at the DeltaGraph Bolt port instead
(`bolt://localhost:7688` in the social demo).

### Claude Desktop / Cursor

Add to the MCP config (`~/Library/Application Support/Claude/claude_desktop_config.json`
on macOS, `~/.config/Claude/claude_desktop_config.json` on Linux):

```json
{
  "mcpServers": {
    "clickgraph": {
      "command": "uvx",
      "args": ["--from", "mcp-neo4j-cypher", "mcp-neo4j-cypher",
               "--db-url", "bolt://localhost:7687",
               "--username", "neo4j", "--password", "password",
               "--read-only", "--transport", "stdio"]
    }
  }
}
```

Restart the client so it loads the server.

## 2 · Ask questions in plain English

> **You:** Using the clickgraph MCP server, discover the schema, then show the
> top 3 users by follower count. Show the Cypher you ran.

The agent calls `get_neo4j_schema` (learns the `User`/`Post` labels and the
`FOLLOWS`/`AUTHORED`/`LIKED` relationships), writes the Cypher, calls
`read_neo4j_cypher`, and ClickGraph executes it on the warehouse.

## 3 · Verify the whole loop without an LLM

`verify_mcp.py` drives the *same* server binary over stdio and calls both tools
directly — deterministic proof the MCP → Bolt → warehouse path works.

```bash
uv venv .venv && uv pip install --python .venv/bin/python "mcp>=1.0"
.venv/bin/python verify_mcp.py bolt://localhost:7687      # ClickHouse
.venv/bin/python verify_mcp.py bolt://localhost:7688      # Databricks (DeltaGraph)
```

**Verified output (Databricks / DeltaGraph, social demo):**

```
=== MCP tools exposed to the agent (bolt://localhost:7688) ===
  • get_neo4j_schema: Returns nodes, their properties (with types and indexed flags)...
  • read_neo4j_cypher: Execute a read Cypher query on the neo4j database.

=== [tool] get_neo4j_schema  (graph discovery) ===
{"Post": {"type": "node", "properties": {"content": {"type": "STRING"}, ...},
 "relationships": {"AUTHORED": {"direction": "in", "labels": ["User"]}, "LIKED": {...}}},
 "User": {"type": "node", "properties": {"user_id": {"type": "STRING"}, ...}}}

=== [tool] read_neo4j_cypher ===
  MATCH (u:User)<-[:FOLLOWS]-(f:User) RETURN u.name AS user, count(f) AS followers
  ORDER BY followers DESC LIMIT 3
  → [{"user": "Tina", "followers": 5}, {"user": "Xander", "followers": 5},
     {"user": "Rachel", "followers": 5}]
```

## Why this works (compatibility notes)

Two things every Neo4j MCP client does that ClickGraph handles:

- **`db="neo4j"` default-database field.** The Neo4j driver's `execute_query()`
  tags every request with `db="neo4j"`. ClickGraph treats that reserved default as
  "use the loaded schema" rather than routing to a nonexistent database.
- **`apoc.meta.schema({sample: N})`.** The schema-discovery call passes a positional
  config map; ClickGraph parses it and returns APOC-format metadata.

Both were fixed in the `fix/bolt-mcp-drop-in-compat` change; older builds fail these
with an opaque `50N42` error.

## Databricks credentials (DeltaGraph)

Set credentials as **environment variables only** — never on the command line, and
never echo the token:

```bash
export DATABRICKS_HOST=...            # workspace host, no scheme
export DATABRICKS_WAREHOUSE_ID=...
export DATABRICKS_TOKEN=...           # PAT
```

The free-tier SQL Warehouse auto-suspends; the first query after idle pays a cold
start. Run one warm-up query before a live demo.
