# DeltaGraph Quickstart

DeltaGraph turns a Databricks SQL Warehouse into a Cypher-queryable graph
database. Same Cypher you'd send to Neo4j; the server translates it to
Spark SQL and executes against your warehouse over the Statement
Execution API. Neo4j Browser, NeoDash, graph-notebook, and any other
Bolt v5 client connect unchanged.

This document walks through the manual setup: build, configure, point
Neo4j Browser at the server, run sample Cypher.

> ⚠️ **Status:** DeltaGraph ships in `v0.6.7-dev`. Phases 1.x–4.3 and
> Phase 3 (catalog discovery, including the YAML `catalog:` field) are
> complete: dialect routing, executor, embedded API, FFI, `cg` CLI,
> server binary, Bolt e2e, `cg schema discover --dialect databricks`.
> Phase 5 (full release) is still ahead. Treat this as an early-adopter
> path until v0.7.0 lands.

## Prerequisites

- A Databricks workspace with at least one **SQL Warehouse** running.
- A **Personal Access Token** (PAT) with `SELECT` on the catalog/schema
  you want to query. OAuth M2M is on the roadmap; PAT is the only auth
  method today.
- A **schema YAML** describing how your tables map to a graph
  (`benchmarks/social_network/schemas/social_benchmark.yaml` is the
  smallest example in the repo).
- Rust 1.85+ to build from source. We do not yet ship pre-built
  `deltagraph` release artifacts.

## 1. Build

```bash
cargo build --release --features databricks --bin deltagraph
```

The `databricks` feature pulls in the executor and its `reqwest`
client. The default build (no feature) produces only `clickgraph`,
which talks to ClickHouse — that binary is unaffected by this work.

## 2. Configure the environment

The server reads these env vars at startup:

| Variable                     | Required | Purpose                                                              |
| ---------------------------- | -------- | -------------------------------------------------------------------- |
| `DATABRICKS_HOST`            | yes      | Workspace hostname, no scheme. `dbc-abc123-def4.cloud.databricks.com` |
| `DATABRICKS_WAREHOUSE_ID`    | yes      | Target SQL Warehouse ID (find under SQL Warehouses in the UI)         |
| `DATABRICKS_TOKEN`           | yes      | Personal access token. **Env-only; never accepted as a flag.**       |
| `DATABRICKS_CATALOG`         | no       | Default catalog for unqualified table names                          |
| `DATABRICKS_SCHEMA`          | no       | Default schema for unqualified table names                           |
| `GRAPH_CONFIG_PATH`          | yes      | Path to your graph-schema YAML                                       |

The token deliberately has no CLI flag — exposing it on the command
line would leak via `ps`, shell history, and CI log uploads.

The catalog also has a third source: a top-level `catalog:` field in
the schema YAML itself.

```yaml
name: my_graph
catalog: main           # Unity Catalog name; ignored under --dialect clickhouse
graph_schema:
  nodes:
    - label: User
      database: graphs  # Spark "schema" within the catalog
      table: users
      …
```

Precedence (highest first):

| Caller | Precedence (highest → lowest) |
| --- | --- |
| `deltagraph` server | `DATABRICKS_CATALOG` env → schema YAML `catalog:` |
| `cg query --dialect databricks` | `DATABRICKS_CATALOG` / `CG_DATABRICKS_CATALOG` env → `[databricks].catalog` in config.toml → schema YAML `catalog:` |
| `cg schema discover --dialect databricks` | `--catalog` flag → `DATABRICKS_CATALOG` / `CG_DATABRICKS_CATALOG` env → `[databricks].catalog` in config.toml → schema YAML `catalog:` |

(The `deltagraph` binary has no `--catalog` flag — the server reads
config from env vars only. The token has no CLI flag at all, for the
same security reason: env-only.)

Set the catalog in the YAML when the schema is permanently tied to one
Unity Catalog; use env to override per environment (staging vs prod).

## 3. Start the server

```bash
./target/release/deltagraph
```

You'll see:

```
DeltaGraph v0.6.7-dev

🧱 DeltaGraph mode: routing queries through a Databricks SQL Warehouse
✓ Schema initialization complete (YAML mode, 1 schema(s) registered)
✓ Successfully bound HTTP listener to 0.0.0.0:7475
Successfully bound Bolt listener to 0.0.0.0:7687
ClickGraph server is running
  HTTP API: http://0.0.0.0:7475
  Bolt Protocol: bolt://0.0.0.0:7687
```

By default the server starts in Neo4j compat mode (so Neo4j Browser
recognises it as a Neo4j 5.x server) and accepts unauthenticated
connections. The compat mode is the headline feature for the Browser
demo; pass `--disable-neo4j-compat` if you want the raw ClickGraph
identity.

## 4. Point Neo4j Browser at it

Open Neo4j Browser (web or desktop). Connect with:

```
Connection URI:  bolt://localhost:7687
Authentication:  no auth   (or: Basic, username `neo4j`, any password)
```

You should land on the standard Browser welcome page. Click the
schema icon in the sidebar to list node labels — they'll come from
your YAML.

### Sample queries (assuming the social-network schema)

```cypher
// Count all users
MATCH (u:User) RETURN count(u) AS users;

// Top followers
MATCH (u:User)<-[:FOLLOWS]-(f:User)
RETURN u.name AS user, count(f) AS followers
ORDER BY followers DESC
LIMIT 10;

// Friends-of-friends (variable-length path)
MATCH (me:User {user_id: 42})-[:FOLLOWS*2..2]->(fof:User)
WHERE fof.user_id <> 42
RETURN DISTINCT fof.name AS friend_of_friend
LIMIT 25;
```

Each query is translated to Spark SQL locally, posted to your
warehouse's Statement Execution API, and the result is mapped back
into the Bolt response Browser expects. Use Browser's "Query Plan" or
the equivalent HTTP probe (below) to see the actual SQL.

## 5. Inspect the generated SQL (without executing)

For debugging the translation:

```bash
curl -s -X POST http://localhost:7475/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u.name LIMIT 5","sql_only":true}' \
  | jq -r .sql
```

This returns the Spark SQL the executor would have sent, without
touching the warehouse. Equivalent to:

```bash
cg --schema schema.yaml --dialect databricks sql "MATCH (u:User) RETURN u.name LIMIT 5"
```

## What works today

- Read queries: `MATCH`, `WHERE`, `RETURN`, `WITH`, `ORDER BY`, `LIMIT`,
  `SKIP`, `DISTINCT`, `OPTIONAL MATCH`, `UNWIND`, `UNION ALL`.
- Variable-length paths (`*1..n`), shortest path, multi-hop traversals.
- Aggregations: `count`, `sum`, `avg`, `min`, `max`, `collect`.
- String / numeric / date / list / map functions — all routed through
  the dialect-aware `FunctionMapper` (e.g. `collect()` → `collect_list`,
  `toInt64()` → `bigint`).
- Pattern comprehension, list comprehension, `CASE` expressions.
- Neo4j Browser, NeoDash, graph-notebook, neo4rs, any Bolt v5 client.

## What's not in this iteration

- **Writes** (`CREATE`, `SET`, `DELETE`, `MERGE`). DeltaGraph is read-only
  against Databricks in this (beta) iteration; embedded chdb is the only
  write target today. Write support against Delta tables — `MERGE` plus
  relationship `CREATE`, edge-alias `DELETE`, `SET a += {…}` map-merge, and
  `REMOVE a:Label` — is part of the **GA scope (planned for v0.7.x)**, not
  the beta. See [`STATUS.md`](../../STATUS.md) and
  [GA readiness](GA_READINESS.md) for the current write gaps and timeline.
- **OAuth M2M** — *now supported* alongside PAT. Set
  `CG_DATABRICKS_CLIENT_ID` + `CG_DATABRICKS_CLIENT_SECRET` (or the
  `DATABRICKS_*` forms, or `[databricks] client_id/client_secret` in the
  config file) for service-principal auth; the executor exchanges them at
  the workspace OIDC endpoint and refreshes the token before expiry. PAT
  (`DATABRICKS_TOKEN`) still works and is the default. Validation against a
  real identity provider is pending a live workspace.
- **External-link result chunks** — *now supported.* The executor speaks
  both `INLINE` (default, ≤25 MB) and `EXTERNAL_LINKS` dispositions; set
  `DatabricksConfig::disposition = ResultDisposition::ExternalLinks` for
  large result sets that would exceed the inline cap. The executor
  downloads the presigned chunk URLs and follows `next_chunk_index`.
  Validation against a live warehouse's real cloud-storage staging is
  still pending (covered locally against the zeta-databricks emulator).
- **`CALL` subqueries** (e.g. LDBC bi-16) — same gap as ClickGraph;
  inherited from the shared planner.
- **Schema discovery from Unity Catalog** (`SHOW TABLES IN catalog.schema`,
  `DESCRIBE TABLE EXTENDED`). Phase 3 ships this; until then your YAML
  is hand-authored or generated via `cg schema discover` against a
  ClickHouse staging copy.

## Pointing `cg` at the same warehouse

The `cg` CLI also supports `--dialect databricks` for ad-hoc queries
without the full server:

```bash
export DATABRICKS_HOST=...
export DATABRICKS_WAREHOUSE_ID=...
export DATABRICKS_TOKEN=...

# Translate (no execution):
cg --schema schema.yaml --dialect databricks sql "MATCH (u:User) RETURN u.name"

# Execute against the warehouse (requires `cg` built with --features databricks):
cargo install --path clickgraph-tool --features databricks --force
cg --schema schema.yaml --dialect databricks query "MATCH (u:User) RETURN u.name LIMIT 5"
```

`cg` shares the same `DATABRICKS_*` env names, plus `CG_DATABRICKS_*`
overrides if you want to scope settings to `cg` alone without leaking
into a running `deltagraph` server.

## Troubleshooting

**`DATABRICKS_HOST not set`** — env var is missing or scrubbed. Check
that you didn't `env -i` the shell, and that `direnv` (if used) loaded
the right `.envrc`.

**`401 Unauthorized` from the executor** — PAT is invalid, expired, or
lacks `SELECT` on the catalog/schema. Verify with `curl` against
`https://$DATABRICKS_HOST/api/2.0/sql/warehouses/$DATABRICKS_WAREHOUSE_ID`.

**Browser shows "Could not connect"** — the server's Bolt port is bound
to `0.0.0.0` by default. Check `lsof -i :7687` on the server host. If
running in Docker, expose the port (`-p 7687:7687`).

**Query returns "Cannot resolve column …"** — the Cypher property names
in your query don't match the `property_mappings` in your schema YAML.
This is the most common source of friction; double-check the YAML.

**Slow first query** — Databricks SQL Warehouses scale to zero by
default. The first query after idle takes 30–90s for the warehouse to
warm up. Subsequent queries are sub-second.

## Where to file feedback

Issues go in the main ClickGraph repo with the `dialect:databricks`
label. The DeltaGraph design doc is at
[`docs/design/DELTAGRAPH_PLAN.md`](../design/DELTAGRAPH_PLAN.md).
