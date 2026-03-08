# ClickGraph Python Bindings (UniFFI)

Python bindings for ClickGraph embedded graph query engine, built on
[UniFFI](https://mozilla.github.io/uniffi-rs/) — the same FFI layer used by
the Go bindings.

## Architecture

```
clickgraph (Python wrapper)        ← Thin Pythonic API (~380 lines)
  └─ clickgraph._ffi               ← Auto-generated ctypes bindings (~2300 lines)
       └─ libclickgraph_ffi.so     ← Shared Rust library (same as Go)
            └─ clickgraph-embedded  ← Core Rust crate
```

## Quick Start

```python
import clickgraph

db = clickgraph.Database("schema.yaml")
conn = db.connect()
for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
    print(row["u.name"])
```

## API

The API is identical to the PyO3 version:

- `Database(schema_path, **kwargs)` — open a database
- `Database.sql_only(schema_path)` — SQL-only mode (no chdb)
- `db.connect()` → `Connection`
- `Connection(db)` — Kuzu-compatible constructor
- `conn.query(cypher)` → `QueryResult`
- `conn.execute(cypher)` — alias for `query()`
- `conn.run(cypher)` — alias for `query()` (Neo4j-compatible)
- `conn.query_to_sql(cypher)` → SQL string
- `conn.export(cypher, path, format=, compression=)`
- `conn.export_to_sql(cypher, path, format=, compression=)` → SQL string
- `QueryResult` — iterable, indexable, `len()`, `has_next()`/`get_next()`, `as_dicts()`

## Development

```bash
# Build the Rust shared library
cargo build -p clickgraph-ffi

# Symlink the library for development
ln -sf ../../target/debug/libclickgraph_ffi.so clickgraph/libclickgraph_ffi.so

# Run tests
CHDB_DIR=$(ls -d ../target/debug/build/chdb-rust-*/out/ | head -1)
LD_LIBRARY_PATH="../target/debug:${CHDB_DIR}" \
  python3 -m pytest tests/ -v
```

## Regenerating FFI Bindings

When `clickgraph-ffi/src/lib.rs` changes:

```bash
pip install uniffi-bindgen==0.29.5
uniffi-bindgen generate --library target/debug/libclickgraph_ffi.so \
  --language python -o clickgraph-py-uniffi/clickgraph/
mv clickgraph-py-uniffi/clickgraph/clickgraph_ffi.py \
   clickgraph-py-uniffi/clickgraph/_ffi.py
```

## Comparison with PyO3 Version

| Aspect | PyO3 (`clickgraph-py`) | UniFFI (`clickgraph-py-uniffi`) |
|--------|------------------------|----------------------------------|
| Rust to maintain | 468 lines (Python-specific) | 0 (reuses `clickgraph-ffi`) |
| Python wrapper | 39 lines | 379 lines |
| Shares FFI with Go | ❌ | ✅ |
| Build tool | maturin | cargo build + symlink/bundle |
| Adding new method | Edit Rust + Python | Edit `clickgraph-ffi` → regenerate |
