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

### `Database.execute(cypher) → QueryResult`

Shorthand — execute a query without creating a separate connection.

### `Connection.query(cypher) → QueryResult`

Execute a Cypher query. Returns an iterable of row dicts.

### `Connection.query_to_sql(cypher) → str`

Translate Cypher to ClickHouse SQL without executing.

### `QueryResult`

- Iterable: `for row in result:` — each row is a `dict`
- `result.column_names` — list of column names
- `result.num_rows` — number of rows
- `result.as_dicts()` — all rows as a list of dicts
- `result.get_row(i)` — single row by index as dict
- `len(result)` — number of rows

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
