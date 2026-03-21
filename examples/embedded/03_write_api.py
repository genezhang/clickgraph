#!/usr/bin/env python3
"""Write API — Build a knowledge graph from scratch.

Demonstrates create_node(), create_edge(), batch operations, and
import_file() for bulk loading.

Run:
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      python3 03_write_api.py
"""
import os, clickgraph

schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_writable.yaml")

db = clickgraph.Database(schema_path)
conn = db.connect()

# --- Create individual nodes ---
print("=== Creating nodes ===")
ffi = conn._ffi
id1 = ffi.create_node("User", {"user_id": clickgraph._ffi.Value.STRING(v="u1"),
                                 "name": clickgraph._ffi.Value.STRING(v="Alice"),
                                 "age": clickgraph._ffi.Value.INT64(v=30),
                                 "country": clickgraph._ffi.Value.STRING(v="US")})
print(f"  Created user: {id1}")

id2 = ffi.create_node("User", {"user_id": clickgraph._ffi.Value.STRING(v="u2"),
                                 "name": clickgraph._ffi.Value.STRING(v="Bob"),
                                 "age": clickgraph._ffi.Value.INT64(v=25),
                                 "country": clickgraph._ffi.Value.STRING(v="UK")})
print(f"  Created user: {id2}")

# --- Create an edge ---
print("\n=== Creating edges ===")
ffi.create_edge("FOLLOWS", "u1", "u2",
                {"follow_date": clickgraph._ffi.Value.STRING(v="2024-01-15")})
print("  Created: u1 -[:FOLLOWS]-> u2")

# --- Query the data ---
print("\n=== Querying ===")
result = conn.query("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name")
for row in result:
    print(f"  {row['a.name']} follows {row['b.name']}")

# --- Bulk import from CSV ---
print("\n=== Bulk import from CSV ===")
csv_data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "users.csv")
# Note: import_file auto-detects format from extension
try:
    ffi.import_file("User", csv_data)
    print(f"  Imported users from {os.path.basename(csv_data)}")
except Exception as e:
    print(f"  Import note: {e}")

result = conn.query("MATCH (u:User) RETURN count(u) AS total")
for row in result:
    print(f"  Total users after import: {row['total']}")

print("\nDone!")
