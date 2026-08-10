# DeltaGraph Docker Quickstart

Run DeltaGraph — Cypher over a Databricks SQL Warehouse — from the published
Docker image, and explore it visually in Neo4j Browser. No Rust toolchain, no
build step.

For the build-from-source walkthrough see [QUICKSTART.md](./QUICKSTART.md);
for how the image is put together see [PACKAGING.md](./PACKAGING.md).

> The `genezhang/clickgraph` image ships **both** server binaries: `clickgraph`
> (ClickHouse, the default entrypoint) and `deltagraph` (Databricks). You select
> DeltaGraph by overriding the entrypoint — shown below.

---

## Prerequisites

- **Docker** installed.
- A **Databricks SQL Warehouse** (any size; a serverless Starter warehouse on
  the free tier works — it cold-starts on the first query).
- A **Personal Access Token** with `SELECT` on the catalog/schema you'll query
  (User Settings → Developer → Access tokens). PAT is the only auth method today.
- A **graph-schema YAML** mapping your tables to nodes/edges. This quickstart
  uses the bundled social demo (`demos/neo4j-browser/social_demo.yaml`).

---

## 1. Pull the image

```bash
docker pull genezhang/clickgraph:latest
```

---

## 2. (Optional) seed the demo data into Delta

Skip this if you're pointing at your own tables. To reproduce the social-network
demo, create these five Delta tables in a schema named `social` inside your
catalog (e.g. `workspace.social`), using the values from
[`demos/neo4j-browser/init-db.sql`](../../demos/neo4j-browser/init-db.sql):

```sql
CREATE SCHEMA IF NOT EXISTS workspace.social;

CREATE OR REPLACE TABLE workspace.social.users
  (user_id INT, name STRING, email STRING, created_at STRING) USING DELTA;
CREATE OR REPLACE TABLE workspace.social.posts
  (post_id INT, content STRING, created_at STRING) USING DELTA;
CREATE OR REPLACE TABLE workspace.social.post_authored
  (user_id INT, post_id INT, created_at STRING) USING DELTA;
CREATE OR REPLACE TABLE workspace.social.post_likes
  (user_id INT, post_id INT, created_at STRING) USING DELTA;
CREATE OR REPLACE TABLE workspace.social.user_follows
  (follower_id INT, followed_id INT, created_at STRING) USING DELTA;

-- then INSERT the VALUES blocks from init-db.sql (30 users, 50 posts, …)
```

Run them from a Databricks SQL editor, or via the Statement Execution API. The
result should be 30 users / 50 posts / 50 authored / 80 likes / 60 follows.

---

## 3. Run DeltaGraph

The Databricks credentials are passed as **environment variables** — the PAT is
never a command-line flag (it would leak via `ps`/shell history). Keep them in a
file that Docker reads with `--env-file`, so nothing sensitive lands in your
shell history:

```bash
cat > dbx.env <<'EOF'
DATABRICKS_HOST=dbc-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=your_warehouse_id
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXX
DATABRICKS_CATALOG=workspace
EOF
chmod 600 dbx.env
```

> `DATABRICKS_HOST` is the **bare hostname**, no `https://`.

Then start the server, overriding the entrypoint to `deltagraph` and mounting
your schema YAML:

```bash
docker run -d --name deltagraph \
  --env-file dbx.env \
  -e GRAPH_CONFIG_PATH=/schema.yaml \
  -v "$PWD/demos/neo4j-browser/social_demo.yaml:/schema.yaml:ro" \
  -p 7475:7475 -p 7687:7687 \
  --entrypoint /usr/local/bin/deltagraph \
  genezhang/clickgraph:latest
```

Check it came up (the first query cold-starts the warehouse, so allow ~30 s):

```bash
docker logs -f deltagraph          # look for "ClickGraph server is running"

curl -s -X POST http://localhost:7475/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"MATCH (u:User) RETURN count(u) AS users"}'
# -> {"results":[{"users":30}]}
```

The server starts in **Neo4j compat mode** by default (so Neo4j Browser
recognises it as a Neo4j 5.x server) and accepts unauthenticated Bolt
connections.

---

## 4. Explore in Neo4j Browser

Neo4j Browser is a static web UI — it holds no data; it just talks Bolt to
DeltaGraph. You don't need a Neo4j database.

Start the Browser UI container:

```bash
docker run -d --name neo4j-browser-ui -p 7474:7474 -e NEO4J_AUTH=none neo4j:latest
```

Open **http://localhost:7474** and connect:

| Field | Value |
| --- | --- |
| Connect URL | `bolt://localhost:7687` |
| Authentication type | **No authentication** |

> If your Browser build hides the "No authentication" option, type any username
> and leave the password blank — DeltaGraph ignores credentials in compat mode.

### Try these

```cypher
// Nodes
MATCH (u:User {user_id: 1}) RETURN u;

// Relationships
MATCH (u:User {user_id: 1})-[r:FOLLOWS]->(f) RETURN u, r, f LIMIT 10;

// Friends of friends
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f)-[:FOLLOWS]->(fof)
RETURN u, f, fof LIMIT 20;

// Multiple edge types
MATCH (u:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker)
RETURN u, p, liker LIMIT 30;

// Variable-length path (recursive CTE / BFS on Spark)
MATCH (u:User {user_id: 1})-[:FOLLOWS*1..3]->(x) RETURN u, x LIMIT 50;
```

Each of these executes as Spark SQL against your warehouse. Nodes render as
circles coloured by label, relationships as typed edges.

---

## Running ClickGraph and DeltaGraph side by side

The same image runs both backends. To compare, run ClickGraph (ClickHouse) on
one pair of ports and DeltaGraph (Databricks) on another, then switch the Browser
connection URL between them:

```bash
# ClickHouse — default entrypoint, Bolt 7687
docker run -d --name clickgraph \
  -e CLICKHOUSE_URL=http://host.docker.internal:8123 \
  -e GRAPH_CONFIG_PATH=/schema.yaml \
  -v "$PWD/demos/neo4j-browser/social_demo.yaml:/schema.yaml:ro" \
  -p 7475:7475 -p 7687:7687 \
  genezhang/clickgraph:latest

# Databricks — deltagraph entrypoint, Bolt 7688
docker run -d --name deltagraph \
  --env-file dbx.env \
  -e GRAPH_CONFIG_PATH=/schema.yaml \
  -v "$PWD/demos/neo4j-browser/social_demo.yaml:/schema.yaml:ro" \
  -p 7476:7475 -p 7688:7687 \
  --entrypoint /usr/local/bin/deltagraph \
  genezhang/clickgraph:latest
```

In Neo4j Browser, connect to `bolt://localhost:7687` for ClickHouse or
`bolt://localhost:7688` for Databricks. With matching demo data, the two return
identical results — same Cypher, two backends.

---

## Managing the container

```bash
docker logs -f deltagraph          # follow logs
docker stop deltagraph             # stop (warehouse then auto-stops when idle)
docker rm -f deltagraph            # remove
```

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `403 Invalid access token` | PAT expired or revoked — free-tier PATs are short-lived. Generate a new one and update `dbx.env`. |
| First query hangs ~30 s | Serverless warehouse cold-start. Subsequent queries are fast. |
| `Node with label User not found` | The mounted schema YAML doesn't define that label, or `GRAPH_CONFIG_PATH` points at the wrong file. |
| `DATABRICKS_HOST` errors | Use the bare hostname, no `https://`. |
| Browser can't connect | Confirm `-p 7687:7687` is published and the container is healthy (`docker logs`). |

---

## What's read-only

DeltaGraph is a **read-only** query engine — `CREATE`/`SET`/`DELETE`/`MERGE` are
out of scope. It translates and executes `MATCH`/`RETURN`/`WITH`/`WHERE`, VLPs,
aggregations, and the supported function set. See the
[Cypher Language Reference](../wiki/Cypher-Language-Reference.md) for coverage.
