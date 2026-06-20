# clickgraph-client

An interactive REPL for querying a running **ClickGraph server** over its HTTP
API. Type Cypher and see results; use `:`-prefixed meta-commands to introspect a
ClickHouse database and build a graph schema (manually or LLM-assisted).

This is the **human-facing** client. For scripting and agent use, reach for the
[`cg` CLI](../clickgraph-tool/) (`clickgraph-tool`) instead — see
[When to use which](#when-to-use-which) below.

## Install / run

The client talks to a ClickGraph server, so start one first (see the repo
[Quick Start](../docs/wiki/Quick-Start-Guide.md)). Then:

```bash
# From the workspace root
cargo run -p clickgraph-client

# Point at a non-default server
cargo run -p clickgraph-client -- --url http://my-host:7475

# Or run the built binary
cargo build --release -p clickgraph-client
./target/release/clickgraph-client --url http://localhost:7475
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-u`, `--url <URL>` | `http://localhost:7475` | Base URL of the ClickGraph HTTP server |
| `--version` | — | Print version |
| `--help` | — | Print help |

On start you'll get a prompt:

```
Connected to ClickGraph server at http://localhost:7475.
Type :help for commands.

clickgraph-client :)
```

Anything that doesn't start with `:` is sent to the server's `/query` endpoint
as a Cypher query and rendered with ClickHouse's `PrettyCompact` formatter.
`Ctrl-C` / `Ctrl-D` exits. Line history (arrow keys) is provided by `rustyline`.

## REPL commands

| Command | Alias | Argument | What it does |
|---------|-------|----------|--------------|
| *(any text)* | — | — | Run as a Cypher query against `/query` |
| `:help` | `:h` | — | Show the command list |
| `:schemas` | `:s` | — | List schemas currently loaded on the server (`GET /schemas`) |
| `:introspect <db>` | `:i` | database | Show tables, columns, primary keys, and node/edge suggestions (`POST /schemas/introspect`) |
| `:discover <db>` | `:disc` | database | **LLM-powered** schema generation — emits a ready-to-load YAML (`POST /schemas/discover-prompt` + your LLM) |
| `:design <db>` | `:d` | database | Interactive step-by-step wizard to declare nodes, edges, and FK-edges, then generate YAML (`POST /schemas/draft`) |
| `:load <file>` | — | file path | Load a schema YAML file into the server (`POST /schemas/load`) |

### `:introspect` — see what's in a database

Lists each table with row counts and primary keys, then prints heuristic
suggestions (node candidates, edge candidates, FK-edge candidates). No LLM
required. Good first step before `:design` or `:discover`.

```
clickgraph-client :) :introspect mydb
```

### `:discover` — LLM-generated schema

Fetches table metadata from the server, sends it to an LLM, and prints a
complete graph-schema YAML. After generation you can **[s]ave**, **[l]oad**,
**[b]oth**, or **[n]** review only. Requires an API key (see
[LLM configuration](#llm-configuration)); if none is set, it falls back to
`:introspect`. Large databases are batched across multiple prompts automatically
and the partial YAMLs are merged.

```
clickgraph-client :) :discover mydb
```

### `:design` — interactive wizard

Walks you through five steps — introspect → nodes → edges → FK-edges → generate.
At each step it shows suggestions and accepts comma-separated input. Edges use
the form:

```
<table>:<TYPE>:<from_node>:<to_node>:<from_id>:<to_id>
# e.g. user_follows:FOLLOWS:User:User:follower_id:followed_id
```

The wizard prints YAML you can then load with `:load`.

### `:load` — load a schema file

```
clickgraph-client :) :load ./schemas/my_graph.yaml
```

The schema name defaults to the file stem (`my_graph.yaml` → `my_graph`).

## LLM configuration

`:discover` reads its provider/key from the environment (same variables as the
core schema-discovery feature):

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANTHROPIC_API_KEY` | — | Enables Anthropic (Claude) mode — the default provider |
| `OPENAI_API_KEY` | — | Used when `CLICKGRAPH_LLM_PROVIDER=openai` (falls back to `ANTHROPIC_API_KEY`) |
| `CLICKGRAPH_LLM_PROVIDER` | `anthropic` | `anthropic` or `openai` (OpenAI-compatible: OpenAI, Ollama, vLLM, LiteLLM, Together, Groq, …) |
| `CLICKGRAPH_LLM_MODEL` | `claude-sonnet-4-20250514` (Anthropic) / `gpt-4o` (OpenAI) | Override the model |
| `CLICKGRAPH_LLM_API_URL` | provider default | Override the API endpoint (point at a local/proxy server) |
| `CLICKGRAPH_LLM_MAX_TOKENS` | `8192` | Max output tokens per request |

If no key is found, `:discover` prints guidance and degrades to `:introspect`.

## When to use which

ClickGraph ships two command-line front-ends — pick by audience:

| | **clickgraph-client** (this crate) | **`cg` CLI** ([`clickgraph-tool`](../clickgraph-tool/)) |
|---|---|---|
| Audience | Humans, interactive | Agents, scripts, CI |
| Interface | Persistent REPL | One-shot subcommands |
| Needs a running server? | **Yes** (talks HTTP) | No (translates/executes locally) |
| Schema work | `:introspect` / `:discover` / `:design` / `:load` | `schema show` / `schema discover` |
| Cypher | Executes via server `/query` | `sql` (translate), `validate`, `query`, `nl` (NL→Cypher) |
| Output | Pretty tables | Machine-friendly |

Use **clickgraph-client** to explore a live server by hand. Use **`cg`** to wire
ClickGraph into automation or to translate/execute without standing up a server.

## DeltaGraph (Databricks) compatibility

The REPL works against a [DeltaGraph](../docs/wiki/Databricks-Deployment.md)
server (the `deltagraph` binary, Databricks backend) as well as a ClickHouse
one. Cypher queries render as tables client-side (the Databricks API has no
server-side pretty format), and `:introspect`/`:discover` drive Databricks'
`SHOW TABLES` / `DESCRIBE TABLE EXTENDED` instead of ClickHouse `system.*`.

One difference: Databricks namespaces are `catalog.schema`, so introspection
needs a catalog. Start the server with `DATABRICKS_CATALOG` set (or the schema
YAML `catalog:` field); then `:introspect <schema>` / `:discover <schema>` treat
their argument as the Spark schema within that catalog. Without a catalog those
commands return a 400 explaining the requirement.

## See also

- [HTTP API Reference](../docs/wiki/API-Reference-HTTP.md) — the endpoints this client calls
- [Schema Discovery](../docs/wiki/Schema-Discovery.md) — server-side and `cg` discovery
- [`cg` CLI guide](../clickgraph-tool/AGENTS.md) — the scripting/agent counterpart
