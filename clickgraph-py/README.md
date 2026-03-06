# clickgraph — Python bindings

Embedded graph query engine — run Cypher queries over Parquet, Iceberg, Delta Lake and S3 data without a ClickHouse server.

## Quick Start

```python
import clickgraph

db = clickgraph.Database("schema.yaml")
conn = db.connect()

for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
    print(row["u.name"])
```

## API Compatibility

ClickGraph's Python API is designed to be familiar to users of other graph databases:

| Operation | ClickGraph | Kuzu | Neo4j |
|-----------|-----------|------|-------|
| Open database | `Database("schema.yaml")` | `Database("path")` | `GraphDatabase.driver(uri)` |
| Get connection | `db.connect()` or `Connection(db)` | `Connection(db)` | `driver.session()` |
| Run query | `conn.query(cypher)` | `conn.execute(cypher)` | `session.run(cypher)` |
| Iterate rows | `for row in result:` | `while result.has_next():` | `for record in result:` |
| Access by name | `row["col"]` (dict) | `row[0]` (tuple) | `record["col"]` (dict-like) |

All three calling styles work — use whichever feels natural:

```python
# ClickGraph style
conn = db.connect()
result = conn.query("MATCH (u:User) RETURN u.name")

# Kuzu style
conn = clickgraph.Connection(db)
result = conn.execute("MATCH (u:User) RETURN u.name")
while result.has_next():
    row = result.get_next()
    print(row[0])

# Neo4j style
conn = db.connect()
result = conn.run("MATCH (u:User) RETURN u.name")
for row in result:
    print(row["u.name"])
```

## API

### `Database(schema_path, **kwargs)`

Open an embedded database from a YAML schema file.

**Keyword arguments** (all optional):
- `session_dir` — directory for chdb session data (default: temp dir)
- `data_dir` — base directory for relative `source:` paths
- `max_threads` — maximum threads for chdb
- `s3_access_key_id`, `s3_secret_access_key`, `s3_region`, `s3_endpoint_url`, `s3_session_token` — S3 credentials
- `gcs_access_key_id`, `gcs_secret_access_key` — GCS HMAC credentials
- `azure_storage_account_name`, `azure_storage_account_key`, `azure_storage_connection_string` — Azure credentials

### `Database.connect() → Connection`

Create a connection for executing queries.

### `Connection(db)` *(Kuzu-compatible constructor)*

Alternative to `db.connect()` — creates a connection from a Database instance.

### `Database.execute(cypher) → QueryResult`

Shorthand — execute a query without creating a separate connection.

### `Connection.query(cypher) → QueryResult`

Execute a Cypher query. Returns an iterable of row dicts.

### `Connection.execute(cypher) → QueryResult` *(Kuzu-compatible alias)*

Alias for `query()`.

### `Connection.run(cypher) → QueryResult` *(Neo4j-compatible alias)*

Alias for `query()`.

### `Connection.query_to_sql(cypher) → str`

Translate Cypher to ClickHouse SQL without executing.

### `Connection.export(cypher, output_path, *, format=None, compression=None)`

Export query results directly to a file. The format is auto-detected from the file extension, or can be specified explicitly. Results are streamed to disk by the embedded engine — no in-memory buffering of the full result set.

**Supported formats**: Parquet (`.parquet`, `.pq`), CSV (`.csv`), TSV (`.tsv`), JSON (`.json`), NDJSON (`.ndjson`, `.jsonl`)

**Keyword arguments** (all optional):
- `format` — explicit format string: `"parquet"`, `"csv"`, `"tsv"`, `"json"`, `"ndjson"`
- `compression` — Parquet compression: `"snappy"`, `"gzip"`, `"lz4"`, `"zstd"`

```python
conn.export("MATCH (u:User) RETURN u.name, u.email", "users.parquet")
conn.export("MATCH (u:User) RETURN u.name", "users.csv")
conn.export("MATCH (u:User) RETURN u.name", "data.parquet", compression="zstd")
conn.export("MATCH (u:User) RETURN u.name", "data.out", format="parquet")
```

### `Connection.export_to_sql(cypher, output_path, *, format=None, compression=None) → str`

Generate the export SQL without executing (for debugging). Returns the `INSERT INTO FUNCTION file(...)` statement.

### `QueryResult`

**Dict-style access** (ClickGraph/Neo4j pattern):
- Iterable: `for row in result:` — each row is a `dict`
- `result[i]` — access row by index (supports negative indexing)
- `result.column_names` — list of column names
- `result.num_rows` — number of rows
- `result.as_dicts()` — all rows as a list of dicts
- `result.get_row(i)` — single row by index as dict
- `len(result)` — number of rows

**Tuple-style access** (Kuzu pattern):
- `result.has_next()` — True if more rows remain
- `result.get_next()` — next row as a list of values (column order)
- `result.reset_iterator()` — restart the cursor

## Installation

```bash
# From source (requires Rust toolchain + chdb)
cd clickgraph-py
pip install maturin
maturin develop
```

## Example with S3 data

```python
import clickgraph

db = clickgraph.Database(
    "schema.yaml",
    s3_access_key_id="AKIA...",
    s3_secret_access_key="...",
    s3_region="us-east-1",
)

conn = db.connect()
result = conn.query("""
    MATCH (u:User)-[:FOLLOWS]->(f:User)
    WHERE u.name = 'Alice'
    RETURN f.name, f.email
""")

for row in result:
    print(f"{row['f.name']}: {row['f.email']}")
```
