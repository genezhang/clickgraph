#!/usr/bin/env python3
"""Verify ClickGraph / DeltaGraph as a drop-in Neo4j MCP target.

Drives the real `mcp-neo4j-cypher` server — the EXACT binary Claude Desktop,
Cursor, and Claude Code launch — over stdio, so the whole agentic loop is
checkable without a GUI or an LLM key:

    MCP client → mcp-neo4j-cypher (stdio) → Bolt → ClickGraph/DeltaGraph → warehouse

It calls both tools an agent uses: `get_neo4j_schema` (graph discovery via
`apoc.meta.schema`) and `read_neo4j_cypher` (a real traversal).

Prereqs:
    pip install "mcp>=1.0"      # MCP client library (this script)
    uv / uvx on PATH            # launches the server: uvx mcp-neo4j-cypher

Usage:
    python verify_mcp.py bolt://localhost:7687      # ClickHouse-backed
    python verify_mcp.py bolt://localhost:7688      # Databricks (DeltaGraph)
"""
import asyncio
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

BOLT = sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687"

SERVER = StdioServerParameters(
    command="uvx",
    args=[
        "--from", "mcp-neo4j-cypher", "mcp-neo4j-cypher",
        "--db-url", BOLT,
        "--username", "neo4j", "--password", "password",
        "--read-only", "--transport", "stdio",
    ],
)

# A real 2-hop-style graph question, exactly as an agent would emit it.
CYPHER = (
    "MATCH (u:User)<-[:FOLLOWS]-(f:User) "
    "RETURN u.name AS user, count(f) AS followers "
    "ORDER BY followers DESC LIMIT 3"
)


def _text(result) -> str:
    return "\n".join(
        c.text for c in result.content if getattr(c, "type", None) == "text"
    )


async def main() -> None:
    async with stdio_client(SERVER) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()

            tools = await session.list_tools()
            print(f"\n=== MCP tools exposed to the agent ({BOLT}) ===")
            for t in tools.tools:
                print(f"  • {t.name}: {t.description.splitlines()[0][:80]}")

            print("\n=== [tool] get_neo4j_schema  (graph discovery) ===")
            schema = _text(await session.call_tool("get_neo4j_schema", {}))
            print(schema[:600] + ("…" if len(schema) > 600 else ""))

            print(f"\n=== [tool] read_neo4j_cypher ===\n  {CYPHER}")
            rows = _text(await session.call_tool("read_neo4j_cypher", {"query": CYPHER}))
            print("  →", rows.replace("\n", "\n    "))


if __name__ == "__main__":
    asyncio.run(main())
