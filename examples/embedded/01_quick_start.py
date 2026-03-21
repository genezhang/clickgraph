#!/usr/bin/env python3
"""Quick Start — Query CSV files as a graph in 5 lines.

Run:
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      python3 01_quick_start.py
"""
import os, clickgraph

# Resolve data directory relative to this script
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.yaml")

# Patch the schema with the actual data path
schema_text = open(schema_path).read().replace("__DATA_DIR__", data_dir)
patched_schema = "/tmp/clickgraph_tutorial_schema.yaml"
open(patched_schema, "w").write(schema_text)

# --- The actual example starts here ---

db = clickgraph.Database(patched_schema)
conn = db.connect()

# Find who Alice follows
print("=== Who does Alice follow? ===")
for row in conn.query(
    "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice Chen' RETURN b.name, b.country"
):
    print(f"  {row['b.name']} ({row['b.country']})")

# Count followers per user
print("\n=== Follower counts ===")
for row in conn.query(
    "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.name, count(a) AS followers ORDER BY followers DESC"
):
    print(f"  {row['b.name']}: {row['followers']} followers")

# Multi-hop: friends of friends
print("\n=== Friends of friends of Alice (2 hops) ===")
for row in conn.query(
    "MATCH (a:User)-[:FOLLOWS*2]->(c:User) WHERE a.name = 'Alice Chen' RETURN DISTINCT c.name ORDER BY c.name"
):
    print(f"  {row['c.name']}")

print("\nDone!")
