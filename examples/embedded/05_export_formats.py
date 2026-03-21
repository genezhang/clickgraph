#!/usr/bin/env python3
"""Export & Import — Round-trip data through Parquet, CSV, and JSON.

Run:
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      python3 05_export_formats.py
"""
import os, tempfile, clickgraph

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.yaml")
schema_text = open(schema_path).read().replace("__DATA_DIR__", data_dir)
patched = "/tmp/clickgraph_tutorial_schema.yaml"
open(patched, "w").write(schema_text)

db = clickgraph.Database(patched)
conn = db.connect()

with tempfile.TemporaryDirectory() as tmpdir:
    # --- Export to Parquet ---
    parquet_path = os.path.join(tmpdir, "users.parquet")
    conn.export(
        "MATCH (u:User) RETURN u.name, u.age, u.country ORDER BY u.name",
        parquet_path,
    )
    size = os.path.getsize(parquet_path)
    print(f"=== Exported to Parquet: {size:,} bytes ===")

    # --- Export to CSV ---
    csv_path = os.path.join(tmpdir, "users.csv")
    conn.export(
        "MATCH (u:User) RETURN u.name, u.age, u.country ORDER BY u.name",
        csv_path,
        format="csv",
    )
    print(f"\n=== Exported to CSV ===")
    print(open(csv_path).read())

    # --- Export to NDJSON ---
    json_path = os.path.join(tmpdir, "users.ndjson")
    conn.export(
        "MATCH (u:User) RETURN u.name, u.age ORDER BY u.name LIMIT 3",
        json_path,
        format="ndjson",
    )
    print(f"=== Exported to NDJSON ===")
    print(open(json_path).read())

    # --- Preview export SQL without executing ---
    sql = conn.export_to_sql(
        "MATCH (u:User) RETURN u.name",
        "output.parquet",
        format="parquet",
        compression="zstd",
    )
    print(f"=== Export SQL (preview) ===")
    print(sql)

print("\nDone!")
