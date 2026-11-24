# clickgraph-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server for
[ClickGraph](https://github.com/genezhang/clickgraph). It lets MCP clients such
as Claude Desktop and Cursor discover ClickGraph schemas, run Cypher, and
inspect the SQL a query would run.

The server is a thin stdio client for ClickGraph's HTTP API. It adds no query
behavior of its own.

## Requirements

- A running ClickGraph server (default `http://localhost:7475`)
- Python 3.10 or newer

## Install

From a checkout of the ClickGraph repository:

```bash
pip install ./clickgraph-mcp
```

For development, install the test extra:

```bash
pip install -e './clickgraph-mcp[test]'
```

This installs the `clickgraph-mcp` command and the `clickgraph_mcp` module.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `CLICKGRAPH_URL` | `http://localhost:7475` | ClickGraph HTTP endpoint |
| `CLICKGRAPH_SCHEMA` | unset | Schema used when a tool call does not name one. When unset, ClickGraph's own default schema is used. |
| `CLICKGRAPH_TIMEOUT` | `30` | Request timeout in seconds |

## MCP client configuration

Claude Desktop (`claude_desktop_config.json`) and Cursor
(`.cursor/mcp.json`) use the same shape:

```json
{
  "mcpServers": {
    "clickgraph": {
      "command": "clickgraph-mcp",
      "env": {
        "CLICKGRAPH_URL": "http://localhost:7475",
        "CLICKGRAPH_SCHEMA": "social"
      }
    }
  }
}
```

If `clickgraph-mcp` is not on the client's `PATH`, use its absolute path (for
example the `bin/` directory of the virtual environment where it is installed).

## Tools

| Tool | Arguments | ClickGraph endpoint |
| --- | --- | --- |
| `list_schemas` | none | `GET /schemas` |
| `run_cypher` | `query`, optional `schema`, optional `parameters` | `POST /query` |
| `explain_query` | `query`, optional `schema`, optional `parameters` | `POST /query/sql` |

`run_cypher` requests `JSONEachRow` output and returns ClickGraph's response
object, including its `results` array. `explain_query` returns the generated
SQL statements without executing them.

Typical flow:

1. `list_schemas` returns the registered graphs and their node/relationship
   counts.
2. `run_cypher` runs a statement against the chosen schema:

   ```cypher
   MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 10
   ```

3. `explain_query` shows the ClickHouse SQL for the same statement.

Tool failures are returned to the client as MCP tool errors carrying
ClickGraph's own error text (for example `Schema not found: nope`).

## Tests

The tests use a fake ClickGraph HTTP server plus one real stdio round-trip, so
they need no ClickHouse or ClickGraph instance:

```bash
python -m pytest clickgraph-mcp/tests -q
```

## Scope

Version 0.1 exposes these three tools over stdio. It does not add
authentication, an HTTP/SSE transport, write helpers, or a result-size cap;
queries still obey ClickGraph's own limits and configuration.
