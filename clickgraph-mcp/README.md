# clickgraph-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server for
[ClickGraph](https://github.com/genezhang/clickgraph). It lets MCP clients such
as Claude Desktop and Cursor discover ClickGraph schemas and their properties,
run Cypher, and inspect the SQL a query would run.

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
| `CLICKGRAPH_MAX_ROWS` | `1000` | Maximum rows returned in a query payload; `0` means unlimited |
| `CLICKGRAPH_VIEW_PARAMETERS` | unset | JSON object forwarded as `view_parameters` on every query |
| `CLICKGRAPH_ROLE` | unset | Role forwarded on every query |

`CLICKGRAPH_VIEW_PARAMETERS` and `CLICKGRAPH_ROLE` are read at startup and sent
to ClickGraph on every `/query` and `/query/sql` call. They are environment
only, not tool arguments: tenant scope is chosen by whoever deploys the server,
never by the model.

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
| `get_schema` | optional `schema` | `POST /query` (`apoc.meta.schema()`) |
| `run_cypher` | `query`, optional `schema`, optional `parameters` | `POST /query` |
| `explain_query` | `query`, optional `schema`, optional `parameters` | `POST /query/sql` |

`get_schema` returns every label's properties and every relationship type's
direction, endpoints and properties, so an agent does not have to guess Cypher
property names. Procedure responses arrive under `records`, and the tool
returns that payload unchanged.

`run_cypher` requests `JSONEachRow` output and returns ClickGraph's response
object, including its `results` array. `explain_query` returns the generated
SQL statements without executing them.

When a query payload has more rows than `CLICKGRAPH_MAX_ROWS`, `run_cypher` and
`get_schema` return the first N rows plus `truncated: true`, `returned`, `total`
and a message asking for a `LIMIT`. Truncation is never silent, and the server
does not inject a `LIMIT` into the Cypher.

Typical flow:

1. `list_schemas` returns the registered graphs and their node/relationship
   counts.
2. `get_schema` returns the selected graph's labels, relationship types and
   property names.
3. `run_cypher` runs a statement against the chosen schema:

   ```cypher
   MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 10
   ```

4. `explain_query` shows the ClickHouse SQL for the same statement.

Tool failures are returned to the client as MCP tool errors carrying
ClickGraph's own error text (for example `Schema not found: nope`).

## Tests

The tests use a fake ClickGraph HTTP server plus one real stdio round-trip, so
they need no ClickHouse or ClickGraph instance:

```bash
python -m pytest clickgraph-mcp/tests -q
```

## Scope

Version 0.1 exposes these four tools over stdio. It does not add
authentication, an HTTP/SSE transport, or write helpers; queries still obey
ClickGraph's own limits and configuration.
