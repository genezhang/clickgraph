import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.mcpserver.exceptions import ToolError

from clickgraph_mcp.server import explain_query, list_schemas, mcp, run_cypher


def test_tools_are_registered():
    tools = asyncio.run(mcp.list_tools())
    assert {t.name for t in tools} == {"run_cypher", "list_schemas", "explain_query"}


def test_every_tool_is_marked_read_only():
    # ClickGraph rejects writes, so no tool can change data. Clients use this
    # hint to skip per-call approval prompts.
    tools = asyncio.run(mcp.list_tools())
    assert tools
    for tool in tools:
        assert tool.annotations is not None, tool.name
        assert tool.annotations.read_only_hint is True, tool.name


def test_run_cypher_omits_schema_by_default(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.delenv("CLICKGRAPH_SCHEMA", raising=False)
    fake_clickgraph.responses[("POST", "/query")] = (200, {"results": [{"n": 1}]})
    assert run_cypher("RETURN 1") == {"results": [{"n": 1}]}
    assert "schema_name" not in fake_clickgraph.requests[0]["body"]


def test_run_cypher_uses_env_schema(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.setenv("CLICKGRAPH_SCHEMA", "movies")
    fake_clickgraph.responses[("POST", "/query")] = (200, {"results": []})
    run_cypher("MATCH (m:Movie) RETURN m")
    assert fake_clickgraph.requests[0]["body"]["schema_name"] == "movies"


def test_explicit_schema_wins_over_env(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.setenv("CLICKGRAPH_SCHEMA", "movies")
    fake_clickgraph.responses[("POST", "/query")] = (200, {"results": []})
    run_cypher("RETURN 1", schema="social")
    assert fake_clickgraph.requests[0]["body"]["schema_name"] == "social"


def test_empty_schema_falls_through(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.delenv("CLICKGRAPH_SCHEMA", raising=False)
    fake_clickgraph.responses[("POST", "/query")] = (200, {"results": []})
    run_cypher("RETURN 1", schema="")
    assert "schema_name" not in fake_clickgraph.requests[0]["body"]


def test_list_schemas_returns_payload(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    fake_clickgraph.responses[("GET", "/schemas")] = (
        200,
        {"schemas": [{"name": "social", "node_count": 1}]},
    )
    assert list_schemas() == {
        "schemas": [{"name": "social", "node_count": 1}]
    }


def test_explain_query_returns_sql(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.delenv("CLICKGRAPH_SCHEMA", raising=False)
    fake_clickgraph.responses[("POST", "/query/sql")] = (
        200,
        {"sql": ["SELECT 1"]},
    )
    assert explain_query("RETURN 1") == {"sql": ["SELECT 1"]}


def test_protocol_call_returns_rows(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    monkeypatch.delenv("CLICKGRAPH_SCHEMA", raising=False)
    fake_clickgraph.responses[("POST", "/query")] = (
        200,
        {"results": [{"p.name": "Alice"}]},
    )
    result = asyncio.run(
        mcp.call_tool("run_cypher", {"query": "MATCH (p:Person) RETURN p.name"})
    )
    assert result.is_error is False
    assert json.loads(result.content[0].text) == {
        "results": [{"p.name": "Alice"}]
    }


def test_tool_error_keeps_clickgraph_message(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", fake_clickgraph.url)
    fake_clickgraph.responses[("POST", "/query")] = (
        400,
        {"error": "Schema not found: nope"},
    )
    with pytest.raises(ToolError, match="Schema not found: nope"):
        run_cypher("RETURN 1", schema="nope")


def test_stdio_round_trip_reports_clickgraph_error(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query")] = (
        400,
        {"error": "Schema not found: nope"},
    )
    package_root = Path(__file__).resolve().parents[1]
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "clickgraph_mcp"],
        env={
            **os.environ,
            "CLICKGRAPH_URL": fake_clickgraph.url,
            "PYTHONPATH": str(package_root),
        },
    )

    async def call():
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool(
                    "run_cypher", {"query": "RETURN 1", "schema": "nope"}
                )

    result = asyncio.run(call())
    assert result.is_error is True
    assert "Schema not found: nope" in result.content[0].text
