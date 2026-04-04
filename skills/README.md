# ClickGraph Agent Skills

Agent skills for querying ClickGraph databases using natural language. Built on the `cg` CLI — no MCP server or running ClickGraph server required.

## Skills

| Skill | File | What it does |
|-------|------|--------------|
| `/cypher` | `cypher.md` | Translate natural language → Cypher → SQL → execute |
| `/graph-schema` | `graph-schema.md` | Show graph schema (nodes, relationships, properties) |
| `/schema-discover` | `schema-discover.md` | Generate a schema YAML from ClickHouse metadata using LLM |

## Prerequisites

1. **`cg` binary** — download from [GitHub Releases](https://github.com/genezhang/clickgraph/releases/latest) or build:
   ```bash
   cargo build --release -p clickgraph-tool
   # binary at: target/release/cg
   ```

2. **Schema file** — your graph schema YAML. Set once and forget:
   ```bash
   export CG_SCHEMA="/path/to/schema.yaml"
   # or add to ~/.config/cg/config.toml:
   # [schema]
   # path = "/path/to/schema.yaml"
   ```

3. **LLM API key** (for `/cypher` NL translation):
   ```bash
   export ANTHROPIC_API_KEY="sk-ant-..."
   # or for OpenAI-compatible:
   # export CG_LLM_PROVIDER=openai
   # export OPENAI_API_KEY="sk-..."
   ```

4. **ClickHouse URL** (optional — for query execution):
   ```bash
   export CG_CLICKHOUSE_URL="http://localhost:8123"
   export CG_CLICKHOUSE_USER="default"
   export CG_CLICKHOUSE_PASSWORD=""
   ```

## Installation by Framework

### Claude Code

Copy skill files into your project's `.claude/commands/` directory:

```bash
mkdir -p .claude/commands
curl -L https://raw.githubusercontent.com/genezhang/clickgraph/main/skills/cypher.md \
  -o .claude/commands/cypher.md
curl -L https://raw.githubusercontent.com/genezhang/clickgraph/main/skills/graph-schema.md \
  -o .claude/commands/graph-schema.md
curl -L https://raw.githubusercontent.com/genezhang/clickgraph/main/skills/schema-discover.md \
  -o .claude/commands/schema-discover.md
```

Then use directly in Claude Code:
```
/cypher find users with more than 10 followers
/graph-schema
```

### Claude Desktop / any MCP client

Use the skills as reference prompts, or pair with the ClickGraph MCP server (see [AI-Assistant-Integration-MCP.md](../docs/wiki/AI-Assistant-Integration-MCP.md)).

### LangChain / AutoGen / CrewAI / custom agents

Use the skill file content as a system prompt or tool description. The `cg` CLI is the execution backend — call it as a subprocess:

```python
import subprocess

def nl_to_cypher(query: str, schema: str, ch_url: str = None) -> dict:
    if not ch_url:
        # Translation only: NL → Cypher (no execution)
        result = subprocess.run(
            ["cg", "--schema", schema, "nl", query],
            capture_output=True, text=True
        )
    else:
        # NL → Cypher → execute in one shot via cg nl --execute
        result = subprocess.run(
            ["cg", "--schema", schema, "--clickhouse", ch_url,
             "nl", "--execute", query],
            capture_output=True, text=True
        )
    return {"output": result.stdout, "error": result.stderr}
```

### OpenAI function calling

```json
{
  "name": "clickgraph_query",
  "description": "Translate natural language to Cypher and execute against a ClickGraph database",
  "parameters": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "Natural language description of the graph query"
      }
    },
    "required": ["query"]
  }
}
```

Implement the function by calling `cg nl "$query"` (and optionally `cg query`).

## Configuration Reference

All configuration can be set via environment variables, CLI flags, or `~/.config/cg/config.toml`.

```toml
# ~/.config/cg/config.toml
[schema]
path = "/path/to/schema.yaml"

[clickhouse]
url = "http://localhost:8123"
user = "default"
password = ""

[llm]
provider = "anthropic"       # or "openai"
model = "claude-sonnet-4-6"
# api_key = "sk-..."         # or set ANTHROPIC_API_KEY
# base_url = "..."           # for OpenRouter, Groq, Ollama, etc.
```

See [clickgraph-tool/AGENTS.md](../clickgraph-tool/AGENTS.md) for the full `cg` CLI reference.
