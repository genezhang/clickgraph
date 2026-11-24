"""MCP tools for ClickGraph."""

from __future__ import annotations

import os
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from . import __version__
from .client import ClickGraphClient, ClickGraphError

# ClickGraph is a read-only engine (writes are rejected), so every tool here is
# read-only. Saying so lets MCP clients skip per-call approval prompts.
_READ_ONLY = ToolAnnotations(read_only_hint=True)

mcp = MCPServer(
    "clickgraph",
    version=__version__,
    instructions=(
        "ClickGraph runs Cypher queries over ClickHouse. Call list_schemas to see "
        "the registered graph schemas, then get_schema for the chosen schema to "
        "see its labels, relationship types and properties before writing Cypher. "
        "Use explain_query to see the SQL a query would run without executing it."
    ),
)


def _client() -> ClickGraphClient:
    return ClickGraphClient()


def _schema(schema: str | None) -> str | None:
    return schema or os.environ.get("CLICKGRAPH_SCHEMA") or None


@mcp.tool(annotations=_READ_ONLY)
def run_cypher(
    query: str,
    schema: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> Any:
    """Run a Cypher query against ClickGraph and return its rows.

    Args:
        query: Cypher statement, for example
            "MATCH (p:Person) RETURN p.name LIMIT 10".
        schema: Registered graph schema to query. Defaults to
            CLICKGRAPH_SCHEMA, else the server's default schema.
        parameters: Values for $parameters used in the query.
    """
    try:
        return _client().run_cypher(
            query, schema=_schema(schema), parameters=parameters
        )
    except ClickGraphError as exc:
        raise ToolError(str(exc)) from exc


@mcp.tool(annotations=_READ_ONLY)
def list_schemas() -> Any:
    """List the graph schemas registered on the ClickGraph server."""
    try:
        return _client().list_schemas()
    except ClickGraphError as exc:
        raise ToolError(str(exc)) from exc


@mcp.tool(annotations=_READ_ONLY)
def get_schema(schema: str | None = None) -> Any:
    """Return a schema's labels, relationship types and their properties.

    Use this before writing Cypher so property names come from the schema
    rather than a guess. The response is ClickGraph's procedure payload.

    Args:
        schema: Registered graph schema to describe. Defaults to
            CLICKGRAPH_SCHEMA, else the server's default schema.
    """
    try:
        return _client().get_schema(_schema(schema))
    except ClickGraphError as exc:
        raise ToolError(str(exc)) from exc


@mcp.tool(annotations=_READ_ONLY)
def explain_query(
    query: str,
    schema: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> Any:
    """Translate a Cypher query to ClickHouse SQL without executing it.

    Args:
        query: Cypher statement to translate.
        schema: Registered graph schema to use. Defaults to
            CLICKGRAPH_SCHEMA, else the server's default schema.
        parameters: Values for $parameters used in the query.
    """
    try:
        return _client().explain_query(
            query, schema=_schema(schema), parameters=parameters
        )
    except ClickGraphError as exc:
        raise ToolError(str(exc)) from exc


def main() -> None:
    """Run the server over stdio, as MCP clients expect."""
    mcp.run("stdio")
