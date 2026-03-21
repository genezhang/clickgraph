#!/usr/bin/env python3
"""DataFrame Output — Convert query results to Pandas, Polars, or PyArrow.

Run:
    pip install pandas pyarrow polars  # install one or more
    LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
      python3 02_dataframe_output.py

Any of the three libraries can be used independently — only the one
you call needs to be installed.
"""
import os, clickgraph

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.yaml")
schema_text = open(schema_path).read().replace("__DATA_DIR__", data_dir)
patched = "/tmp/clickgraph_tutorial_schema.yaml"
open(patched, "w").write(schema_text)

db = clickgraph.Database(patched)
conn = db.connect()

result = conn.query(
    "MATCH (u:User) RETURN u.name, u.age, u.country ORDER BY u.age DESC"
)

# --- Query timing ---
print(f"Compile time: {result._ffi.get_compiling_time():.2f}ms")
print(f"Execution time: {result._ffi.get_execution_time():.2f}ms")
print(f"Column types: {result._ffi.get_column_data_types()}")
print()

# --- Pandas ---
try:
    df = result.get_as_df()
    print("=== Pandas DataFrame ===")
    print(df.to_string(index=False))
    print(f"\nMean age: {df['u.age'].mean():.1f}")
    print(f"Users per country:\n{df['u.country'].value_counts().to_string()}")
except ImportError as e:
    print(f"Pandas: {e}")

print()

# --- PyArrow ---
try:
    table = result.get_as_arrow()
    print("=== PyArrow Table ===")
    print(table)
    print(f"Schema: {table.schema}")
except ImportError as e:
    print(f"PyArrow: {e}")

print()

# --- Polars ---
try:
    df = result.get_as_pl()
    print("=== Polars DataFrame ===")
    print(df)
except ImportError as e:
    print(f"Polars: {e}")

print("\nDone!")
