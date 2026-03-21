#!/usr/bin/env python3
"""Write API — Build a knowledge graph from scratch.

Demonstrates create_node(), create_edge(), batch operations, and
import_file() for bulk loading.

Run:
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      python3 03_write_api.py
"""
import os, clickgraph

# Note: this uses schema_writable.yaml — tables without source: get
# auto-created as writable ReplacingMergeTree tables on startup.
schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_writable.yaml")

db = clickgraph.Database(schema_path)
conn = db.connect()

# --- Create individual nodes (native Python dicts) ---
print("=== Creating nodes ===")
id1 = conn.create_node("User", {"user_id": "u1", "name": "Alice", "age": 30, "country": "US"})
print(f"  Created user: {id1}")

id2 = conn.create_node("User", {"user_id": "u2", "name": "Bob", "age": 25, "country": "UK"})
print(f"  Created user: {id2}")

# --- Create an edge ---
print("\n=== Creating edges ===")
conn.create_edge("FOLLOWS", "u1", "u2", {"follow_date": "2024-01-15"})
print("  Created: u1 -[:FOLLOWS]-> u2")

# --- Batch create ---
print("\n=== Batch create ===")
ids = conn.create_nodes("User", [
    {"user_id": "u3", "name": "Charlie", "age": 35, "country": "CA"},
    {"user_id": "u4", "name": "Diana", "age": 28, "country": "US"},
])
print(f"  Created {len(ids)} users: {ids}")

# --- Query the data ---
print("\n=== Querying ===")
result = conn.query("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name")
for row in result:
    print(f"  {row['a.name']} follows {row['b.name']}")

print("\n=== All users ===")
for row in conn.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.name"):
    print(f"  {row['u.name']}, age {row['u.age']}")

# --- Bulk import from CSV ---
print("\n=== Bulk import from CSV ===")
csv_data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "users.csv")
try:
    conn.import_file("User", csv_data)
    print(f"  Imported users from {os.path.basename(csv_data)}")
except Exception as e:
    print(f"  Import note: {e}")

result = conn.query("MATCH (u:User) RETURN count(u) AS total")
for row in result:
    print(f"  Total users after import: {row['total']}")

print("\nDone!")
