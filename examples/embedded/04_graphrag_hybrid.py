#!/usr/bin/env python3
"""GraphRAG Hybrid Workflow — Remote query + local storage.

The flagship use case: execute complex Cypher queries against a remote
ClickHouse cluster, store the resulting subgraph locally in chdb,
then run fast local queries for GraphRAG context retrieval.

Requirements:
    - A running ClickHouse with test data (see setup instructions below)
    - Set CLICKHOUSE_READY=1 to enable the remote parts

Run:
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      CLICKHOUSE_READY=1 python3 04_graphrag_hybrid.py

ClickHouse setup (run once):
    curl -s "http://localhost:8123/?user=test_user&password=test_pass" \\
      --data-binary "CREATE TABLE IF NOT EXISTS default.users (
        user_id UInt32, full_name String, age UInt32, country String
      ) ENGINE = ReplacingMergeTree() ORDER BY user_id"
    curl -s "http://localhost:8123/?user=test_user&password=test_pass" \\
      --data-binary "INSERT INTO default.users VALUES
        (1,'Alice',30,'US'),(2,'Bob',25,'UK'),(3,'Charlie',35,'CA'),
        (4,'Diana',28,'US'),(5,'Eve',32,'DE')"
    curl -s "http://localhost:8123/?user=test_user&password=test_pass" \\
      --data-binary "CREATE TABLE IF NOT EXISTS default.follows (
        follower_id UInt32, followed_id UInt32, follow_date String
      ) ENGINE = ReplacingMergeTree() ORDER BY (follower_id, followed_id)"
    curl -s "http://localhost:8123/?user=test_user&password=test_pass" \\
      --data-binary "INSERT INTO default.follows VALUES
        (1,2,'2024-01-15'),(1,3,'2024-02-20'),(2,3,'2024-03-10'),
        (3,1,'2024-04-05'),(4,1,'2024-05-12')"
"""
import os, clickgraph

schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_writable.yaml")
clickhouse_ready = os.environ.get("CLICKHOUSE_READY") == "1"

if not clickhouse_ready:
    print("Skipping remote examples (set CLICKHOUSE_READY=1 to enable)")
    print("Showing the workflow structure:\n")
    print("""
    # 1. Open database with remote config
    db = clickgraph.Database("schema.yaml",
        session_dir="/tmp/graphrag_session",
        remote_url="http://localhost:8123",
        remote_user="test_user",
        remote_password="test_pass")
    conn = db.connect()

    # 2. Query remote cluster for a subgraph
    graph = conn.query_remote_graph(
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.country = 'US' RETURN u, r, f"
    )
    print(f"Remote: {graph.node_count} nodes, {graph.edge_count} edges")

    # 3. Store subgraph locally
    stats = conn.store_subgraph(graph)
    print(f"Stored: {stats.nodes_stored} nodes, {stats.edges_stored} edges")

    # 4. Fast local queries (no remote calls)
    for row in conn.query("MATCH (u:User) RETURN u.name ORDER BY u.name"):
        print(f"  Local: {row['u.name']}")
    """)
    exit(0)

# --- Live execution ---

print("=== Step 1: Open database with remote config ===")
db = clickgraph.Database(
    schema_path,
    session_dir="/tmp/clickgraph_graphrag_demo",
    remote_url="http://localhost:8123",
    remote_user="test_user",
    remote_password="test_pass",
)
conn = db.connect()
print("  Database opened with remote executor")

print("\n=== Step 2: Query remote for subgraph ===")
graph = conn.query_remote_graph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.country = 'US' RETURN u, r, f"
)
print(f"  Remote result: {graph.node_count} nodes, {graph.edge_count} edges")
for node in graph.nodes:
    print(f"    Node: {node['id']} — {node['properties']}")
for edge in graph.edges:
    print(f"    Edge: {edge['from_id']} -[{edge['type_name']}]-> {edge['to_id']}")

print("\n=== Step 3: Store subgraph locally ===")
stats = conn.store_subgraph(graph)
print(f"  Stored: {stats.nodes_stored} nodes, {stats.edges_stored} edges")

print("\n=== Step 4: Fast local queries ===")
print("  Users in local store:")
for row in conn.query("MATCH (u:User) RETURN u.name, u.country ORDER BY u.name"):
    print(f"    {row['u.name']} ({row['u.country']})")

# Can also do tabular remote queries
print("\n=== Bonus: Tabular remote query ===")
result = conn.query_remote(
    "MATCH (u:User) RETURN u.name, u.age ORDER BY u.age DESC LIMIT 3"
)
print("  Top 3 oldest users (from remote):")
for row in result:
    print(f"    {row['u.name']}, age {row['u.age']}")

print("\nDone!")
