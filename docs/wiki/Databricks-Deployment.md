# Databricks Deployment (DeltaGraph)

**DeltaGraph** is ClickGraph's Databricks backend. It turns a Databricks SQL
Warehouse into a Cypher-queryable graph database: you send the same Cypher you'd
send to Neo4j, and the server translates it to **Spark SQL** and executes it
against your warehouse over the Statement Execution API. Neo4j Browser, NeoDash,
graph-notebook, and any Bolt v5 client connect unchanged.

It's the same engine as ClickGraph (shared parser, planner, Bolt/HTTP servers) —
only the SQL dialect and executor differ. Build it with the `databricks` feature
as the `deltagraph` binary.

> ⚠️ **Status:** DeltaGraph is an **early-adopter / beta** path in `v0.6.7-dev`.
> Read queries, variable-length paths, aggregations, OAuth M2M, and
> external-link result chunks work; writes (`CREATE`/`SET`/`DELETE`/`MERGE`) are
> GA scope (v0.7.x). See [GA readiness](../deltagraph/GA_READINESS.md).

## Quick reference

```bash
# Build (the databricks feature pulls in the Spark-SQL executor)
cargo build --release --features databricks --bin deltagraph

# Configure (token is env-only, never a CLI flag)
export DATABRICKS_HOST=dbc-abc123-def4.cloud.databricks.com
export DATABRICKS_WAREHOUSE_ID=...
export DATABRICKS_TOKEN=...
export GRAPH_CONFIG_PATH=./schemas/my_graph.yaml

# Run — same HTTP (7475) + Bolt (7687) surface as clickgraph
./target/release/deltagraph
```

Inspect the generated Spark SQL without executing:

```bash
curl -s -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u.name LIMIT 5","sql_only":true}' | jq -r .sql
```

## Full guides

The detailed, maintained DeltaGraph docs live under `docs/deltagraph/`:

- **[DeltaGraph Quickstart](../deltagraph/QUICKSTART.md)** — build, env vars,
  catalog precedence, Neo4j Browser setup, sample queries, troubleshooting, and
  pointing the `cg` CLI at the same warehouse.
- **[GA Readiness](../deltagraph/GA_READINESS.md)** — what's complete vs. the
  gating items for general availability.
- **[Local Testing Results](../deltagraph/LOCAL_TESTING_RESULTS.md)** —
  validation against the Spark/Delta docker environment.
- **[Zeta Fidelity](../deltagraph/ZETA_FIDELITY.md)** — fidelity of the
  zeta-databricks emulator vs. real Spark/Delta.

## Env vars at a glance

| Variable | Required | Purpose |
|---|---|---|
| `DATABRICKS_HOST` | yes | Workspace hostname, no scheme |
| `DATABRICKS_WAREHOUSE_ID` | yes | Target SQL Warehouse ID |
| `DATABRICKS_TOKEN` | yes (PAT auth) | Personal access token — **env-only** |
| `DATABRICKS_CATALOG` | no | Default Unity Catalog for unqualified names |
| `DATABRICKS_SCHEMA` | no | Default schema within the catalog |
| `CG_DATABRICKS_CLIENT_ID` / `CG_DATABRICKS_CLIENT_SECRET` | no | OAuth M2M service-principal auth (alternative to PAT) |
| `GRAPH_CONFIG_PATH` | yes | Path to the graph-schema YAML |

## Clients

Connect with **Neo4j Browser**, **NeoDash**, **graph-notebook**, or any Bolt v5
driver — these work unchanged. The `cg` CLI works too via `--dialect databricks`
(see Quickstart). The interactive **`clickgraph-client` REPL** also works against
DeltaGraph: Cypher queries render as tables client-side, and
`:introspect`/`:discover` drive Databricks' `SHOW TABLES` / `DESCRIBE TABLE
EXTENDED`. Introspection needs a catalog — set `DATABRICKS_CATALOG` (or the YAML
`catalog:` field) and the REPL's argument is treated as the Spark schema within
it.

## See also

- [Databricks Function Pass-Through](Databricks-Functions.md) — reach native Spark/Databricks functions from Cypher with the `dbx.` prefix
- [HTTP API Reference](API-Reference-HTTP.md) — the endpoints the server exposes
- [Schema Basics](Schema-Basics.md) — authoring the graph-schema YAML
- [`cg` CLI](../../clickgraph-tool/AGENTS.md) — `--dialect databricks` for ad-hoc queries
