# Embedded Mode — In-Process Graph Queries

ClickGraph can run entirely **in-process** without an external ClickHouse server, using [chdb](https://github.com/chdb-io/chdb) — ClickHouse's embeddable engine. Query Parquet, Iceberg, Delta Lake, and CSV files directly from your application or from a standalone server binary.

This is similar to how [DuckDB](https://duckdb.org/) and [Kuzu](https://kuzudb.com/) work — a fully self-contained analytical engine that requires no separate database process.

---

## When to Use Embedded Mode

| Scenario | Recommendation |
|----------|----------------|
| Existing ClickHouse cluster | Standard mode (default) |
| Query local Parquet / CSV files | **Embedded mode** |
| Query S3 / Iceberg / Delta Lake without a server | **Embedded mode** |
| Embed graph queries in a Rust application | **Embedded mode (Rust library)** |
| Embed graph queries in a Python application | **Embedded mode (Python library)** |
| Edge / serverless deployment | **Embedded mode** |
| Development & prototyping without a database | **Embedded mode** |

---

## Three Ways to Use Embedded Mode

### Option A — Standalone Server (HTTP + Bolt)

Run the ClickGraph HTTP/Bolt server without ClickHouse — connect Neo4j Browser, curl, or any Bolt driver as normal:

```bash
GRAPH_CONFIG_PATH=./schema.yaml \
CLICKGRAPH_EMBEDDED=true \
./clickgraph
```

Or with the CLI flag:

```bash
./clickgraph --embedded
```

Everything works as in standard mode: HTTP REST API on port 8080, Bolt on port 7687. The only difference is that queries run against chdb in-process rather than a remote ClickHouse server.

### Option B — Library (Rust API)

Embed ClickGraph directly in your Rust application. The API mirrors [Kuzu's Rust API](https://docs.kuzudb.com/client-apis/rust/):

```toml
# Cargo.toml
[dependencies]
clickgraph-embedded = "0.6"
```

```rust
use clickgraph_embedded::{Connection, Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("schema.yaml", SystemConfig::default())?;
    let conn = Connection::new(&db)?;

    let mut result = conn.query(
        "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.country = 'US' RETURN u.name, f.name LIMIT 10"
    )?;

    while let Some(row) = result.next() {
        println!("{} → {}", row[0], row[1]);
    }
    Ok(())
}
```

### Option C — Python Library

Use ClickGraph from Python — ideal for data science and analytics:

```bash
cd clickgraph-py
pip install maturin
maturin develop    # builds and installs the 'clickgraph' package
```

```python
import clickgraph

db = clickgraph.Database("schema.yaml")
conn = db.connect()

for row in conn.query("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 10"):
    print(f"{row['u.name']} → {row['f.name']}")
```

**With S3 credentials:**

```python
db = clickgraph.Database(
    "schema.yaml",
    s3_access_key_id="AKIA...",
    s3_secret_access_key="...",
    s3_region="us-east-1",
)
```

**SQL debugging:**

```python
conn = db.connect()
print(conn.query_to_sql("MATCH (u:User) RETURN u.name"))
# → SELECT users.full_name AS `u.name` FROM test_db.users
```

**QueryResult API:**

```python
result = conn.query("MATCH (u:User) RETURN u.name, u.email LIMIT 5")
print(result.column_names)   # ['u.name', 'u.email']
print(result.num_rows)       # 5
print(len(result))           # 5
print(result[0])             # {'u.name': 'Alice', 'u.email': '...'}
print(result[-1])            # last row
print(result.as_dicts())     # [{'u.name': 'Alice', 'u.email': '...'}, ...]

# Dict-style iteration (default):
for row in result:
    print(row)               # each row is a dict
```

**Kuzu-compatible API:**

If you're coming from Kuzu, the same patterns work:

```python
from clickgraph import Database, Connection

db = Database("schema.yaml")
conn = Connection(db)              # same as db.connect()
result = conn.execute("MATCH (u:User) RETURN u.name")  # same as conn.query()

# Tuple-style cursor iteration (like Kuzu):
while result.has_next():
    row = result.get_next()        # list of values in column order
    print(row[0])

result.reset_iterator()            # restart from beginning
```

**Neo4j-compatible API:**

```python
conn = db.connect()
result = conn.run("MATCH (u:User) RETURN u.name")  # same as conn.query()
for row in result:
    print(row["u.name"])           # dict access, same as Neo4j Record
```

---

## Schema Configuration

Embedded mode uses the same YAML schema format as standard mode, with one addition: an optional `source:` field on each node and relationship that tells ClickGraph where to read the data from.

```yaml
name: social_network
graph_schema:
  nodes:
    - label: User
      database: social
      table: users              # used as the table name in generated SQL
      source: s3://my-bucket/users.parquet    # ← new field
      node_id: user_id
      property_mappings:
        name: full_name
        email: email_address
        country: country

    - label: Post
      database: social
      table: posts
      source: ./data/posts.parquet           # local file
      node_id: post_id
      property_mappings:
        title: title
        content: body

  edges:
    - type: FOLLOWS
      database: social
      table: follows
      source: s3://my-bucket/follows.parquet
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
```

> **Note**: The `source:` field is optional. If omitted, ClickGraph assumes the table already exists in the chdb session (or, in standard mode, in ClickHouse).

### Supported Source URI Schemes

| URI | Format | Notes |
|-----|--------|-------|
| `/abs/path/file.parquet` | Parquet | Absolute local path |
| `./rel/path/file.csv` | CSV | Relative local path |
| `file:///abs/path/file.parquet` | Parquet | Explicit `file://` URI |
| `s3://bucket/key.parquet` | Parquet | AWS S3 |
| `gs://bucket/key.parquet` | Parquet | Google Cloud Storage |
| `iceberg+s3://bucket/table/` | Iceberg | Apache Iceberg on S3 |
| `iceberg+local:///path/to/table/` | Iceberg | Iceberg on local disk |
| `delta+s3://bucket/table/` | Delta Lake | Delta Lake on S3 |
| `table_function:<expr>` | Custom | Raw chdb table function (advanced) |

**Glob patterns** are supported for partitioned datasets:

```yaml
source: s3://my-bucket/events/year=2024/month=*/events.parquet
```

**Format auto-detection** from file extension:

| Extension | Format |
|-----------|--------|
| `.parquet`, `.parq` | Parquet |
| `.csv` | CSV |
| `.tsv` | TSV |
| `.json`, `.ndjson` | JSONEachRow |
| `.orc` | ORC |
| `.avro` | Avro |
| directory / other | Parquet (default) |

---

## Storage Credentials

Credentials are resolved in priority order:

### 1. Explicit via `SystemConfig` (Rust library)

```rust
use clickgraph_embedded::{Database, StorageCredentials, SystemConfig};

let db = Database::new("schema.yaml", SystemConfig {
    credentials: StorageCredentials {
        s3_access_key_id:     Some("AKIAIOSFODNN7EXAMPLE".into()),
        s3_secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
        s3_region:            Some("us-east-1".into()),
        s3_endpoint_url:      None,   // uses AWS default; set for MinIO etc.
        gcs_credentials_file: None,
        azure_connection_str: None,
    },
    ..SystemConfig::default()
})?;
```

### 2. Environment variables (server mode or library)

Set before starting the server or calling `Database::new`:

```bash
# AWS S3
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1

# Custom S3-compatible endpoint (MinIO, Cloudflare R2, etc.)
export AWS_ENDPOINT_URL=http://localhost:9000

# Google Cloud Storage
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### 3. Instance profile / workload identity (automatic)

If no credentials are configured, chdb uses:
- **AWS**: IMDSv2 instance profile (EC2, ECS, Lambda)
- **GKE**: Workload Identity via metadata server
- **Azure**: Managed Identity

---

## Rust Library API Reference

### `Database`

```rust
// Open from YAML schema file
let db = Database::new("schema.yaml", SystemConfig::default())?;

// Open with pre-loaded schema
let db = Database::from_schema(Arc::new(schema), SystemConfig::default())?;

// Inspect the schema
let schema = db.schema();
```

### `Connection`

Multiple connections can be created from a single `Database`:

```rust
let conn = Connection::new(&db)?;

// Execute Cypher — synchronous, blocks until complete
let result = conn.query("MATCH (u:User) RETURN u.name LIMIT 5")?;

// Inspect generated SQL (useful for debugging)
let sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")?;
println!("{}", sql);
```

### `QueryResult` and `Row`

```rust
let mut result = conn.query("MATCH (u:User) RETURN u.name, u.user_id")?;

// Column names
println!("{:?}", result.column_names());  // ["u.name", "u.user_id"]

// Iterate rows
while let Some(row) = result.next() {
    println!("{} — {}", row[0], row[1]);
}

// Or collect to Vec
let rows: Vec<_> = result.collect();
```

### `Value` enum

Each cell in a row is a `Value`:

```rust
use clickgraph_embedded::Value;

match &row[0] {
    Value::String(s)  => println!("string: {}", s),
    Value::Int64(n)   => println!("int: {}", n),
    Value::Float64(f) => println!("float: {}", f),
    Value::Bool(b)    => println!("bool: {}", b),
    Value::Null       => println!("null"),
    other             => println!("other: {:?}", other),
}

// Convenience methods
let name: Option<&str>  = row[0].as_str();
let id:   Option<i64>   = row[1].as_i64();
let f:    Option<f64>   = row[2].as_f64();
let b:    Option<bool>  = row[3].as_bool();
```

### `SystemConfig`

```rust
use clickgraph_embedded::SystemConfig;
use std::path::PathBuf;

let config = SystemConfig {
    // Where chdb stores session data.
    // None (default) = auto temp dir, cleaned on drop.
    session_dir: Some(PathBuf::from("/tmp/my-graph-session")),

    // Storage credentials (see above)
    credentials: StorageCredentials::default(),

    // Reserved for future use:
    data_dir:    None,   // base dir for relative source: paths (coming soon)
    max_threads: None,   // thread count for chdb (coming soon)
};
```

---

## Examples

### Social Network from Local Parquet

```yaml
# schema.yaml
name: social
graph_schema:
  nodes:
    - label: Person
      database: social
      table: persons
      source: ./persons.parquet
      node_id: id
      property_mappings: {name: name, age: age, city: city}
  edges:
    - type: KNOWS
      database: social
      table: knows
      source: ./knows.parquet
      from_node: Person
      to_node: Person
      from_id: src_id
      to_id: dst_id
      property_mappings: {}
```

```rust
let db = Database::new("schema.yaml", SystemConfig::default())?;
let conn = Connection::new(&db)?;

// Friends of friends
let result = conn.query(
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person)
     WHERE NOT (a)-[:KNOWS]->(fof)
     RETURN DISTINCT fof.name, fof.city"
)?;
for row in result {
    println!("{} ({})", row[0], row[1]);
}
```

### Apache Iceberg Data Lake

```yaml
nodes:
  - label: Transaction
    database: finance
    table: transactions
    source: iceberg+s3://my-data-lake/finance/transactions/
    node_id: txn_id
    property_mappings: {amount: amount_usd, merchant: merchant_name, ts: created_at}
  - label: Account
    database: finance
    table: accounts
    source: iceberg+s3://my-data-lake/finance/accounts/
    node_id: account_id
    property_mappings: {owner: owner_name, type: account_type}
edges:
  - type: PAID_FROM
    database: finance
    table: txn_accounts
    source: iceberg+s3://my-data-lake/finance/txn_accounts/
    from_node: Transaction
    to_node: Account
    from_id: txn_id
    to_id: account_id
    property_mappings: {}
```

```rust
let result = conn.query(
    "MATCH (t:Transaction)-[:PAID_FROM]->(a:Account)
     WHERE t.amount > 10000 AND a.type = 'personal'
     RETURN a.owner, sum(t.amount) AS total
     ORDER BY total DESC LIMIT 20"
)?;
```

### MinIO (S3-Compatible)

```rust
let db = Database::new("schema.yaml", SystemConfig {
    credentials: StorageCredentials {
        s3_access_key_id:     Some("minioadmin".into()),
        s3_secret_access_key: Some("minioadmin".into()),
        s3_region:            Some("us-east-1".into()),
        s3_endpoint_url:      Some("http://localhost:9000".into()),
        ..Default::default()
    },
    ..SystemConfig::default()
})?;
```

---

## Comparison: Embedded vs. Standard Mode

| Feature | Standard Mode | Embedded Mode |
|---------|--------------|---------------|
| Data source | ClickHouse tables | Parquet, Iceberg, Delta, CSV, S3 |
| External server required | ✅ ClickHouse | ❌ None |
| Write operations | ❌ Read-only | ❌ Read-only |
| HTTP + Bolt endpoints | ✅ | ✅ (`--embedded` flag) |
| Rust library API | ❌ | ✅ (`clickgraph-embedded`) |
| Performance at scale | ✅ Full ClickHouse cluster | ✅ Single-node chdb |
| Schema admin endpoints | ✅ | ⚠️ Unavailable (no ClickHouse) |
| Cargo feature flag | (default) | `--features embedded` |

---

## Limitations

- **Read-only**: Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are not supported in any mode.
- **Build size**: The `embedded` feature links the chdb native library (~100 MB).
- **Schema admin endpoints** (`/schemas/load`, `/schemas/discover-prompt`) are not available in embedded server mode.
- **`data_dir` and `max_threads`** in `SystemConfig` are reserved for future releases.

---

## Related Pages

- **[Schema Basics](Schema-Basics.md)** — YAML schema configuration
- **[API Reference HTTP](API-Reference-HTTP.md)** — HTTP endpoint documentation
- **[Quick Start Guide](Quick-Start-Guide.md)** — Standard ClickHouse setup
