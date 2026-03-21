# ClickGraph Embedded Mode Examples

Runnable Python examples demonstrating ClickGraph's embedded mode features.

## Prerequisites

```bash
# Build the shared library
cargo build -p clickgraph-ffi

# Symlink for development (or set LD_LIBRARY_PATH)
ln -sf ../../target/debug/libclickgraph_ffi.so ../../clickgraph-py/clickgraph/

# Optional: install data science packages
pip install pandas pyarrow polars
```

## Examples

| Script | What it demonstrates |
|--------|---------------------|
| [01_quick_start.py](01_quick_start.py) | Query CSV files as a graph — basic MATCH, filters, multi-hop |
| [02_dataframe_output.py](02_dataframe_output.py) | Convert results to Pandas, PyArrow, Polars DataFrames |
| [03_write_api.py](03_write_api.py) | Build a graph from scratch — create_node, create_edge, import_file |
| [04_graphrag_hybrid.py](04_graphrag_hybrid.py) | Remote query + local storage for GraphRAG workflows |
| [05_export_formats.py](05_export_formats.py) | Export to Parquet, CSV, NDJSON; preview export SQL |

## Running

```bash
# From this directory:
LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
  python3 01_quick_start.py

# For hybrid example (requires running ClickHouse):
LD_LIBRARY_PATH=../../target/debug PYTHONPATH=../../clickgraph-py \
  CLICKHOUSE_READY=1 python3 04_graphrag_hybrid.py
```

## Sample Data

The `data/` directory contains a small social network:

- **8 users** across 6 countries (US, UK, CA, DE, KR, JP)
- **11 follow relationships** forming two clusters
- **5 topics** in Technology, AI, and Lifestyle categories
- **12 interest connections** between users and topics
