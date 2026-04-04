# Embedded Mode — In-Process Graph Queries

`clickgraph-embedded` provides three execution backends depending on your use case:

| Constructor | Requires chdb? | Description |
|-------------|---------------|-------------|
| `Database::new(schema, config)` | Yes (`embedded` feature) | Full in-process execution via chdb — query Parquet, S3, Iceberg, Delta Lake |
| `Database::new_remote(schema, remote)` | No | Translate Cypher locally, execute against a remote ClickHouse cluster |
| `Database::sql_only(schema)` | No | Cypher→SQL translation only, no execution |

The **`embedded` feature** (chdb) is **opt-in** — it is not compiled by default. This keeps compile times fast and removes the native library dependency for tools that only need translation or remote execution (e.g., the `cg` CLI).

When `embedded` is enabled, ClickGraph becomes similar to [DuckDB](https://duckdb.org/) and [Kuzu](https://kuzudb.com/) — a fully self-contained analytical engine that requires no separate database process.

---

## When to Use Which Mode

| Scenario | Recommendation |
|----------|----------------|
| Existing ClickHouse cluster | Remote mode (`new_remote`) or standard server mode |
| Query local Parquet / CSV files | **Embedded mode** (`new` with `embedded` feature) |
| Query S3 / Iceberg / Delta Lake without a server | **Embedded mode** |
| Translate Cypher to SQL without executing | **SQL-only mode** (`sql_only`) |
| Embed graph queries in a Rust application | **Embedded mode (Rust library)** |
| Embed graph queries in a Python application | **Embedded mode (Python library)** |
| Embed graph queries in a Go application | **Embedded mode (Go library)** |
| Edge / serverless deployment | **Embedded mode** |
| AI agent building local knowledge graph (GraphRAG) | **Embedded mode (write API)** |
| Query remote CH cluster + store results locally | **Hybrid remote mode** |
| Development & debugging (inspect SQL) | **SQL-only or remote mode** |

---

## Four Ways to Use Embedded Mode

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
# Cargo.toml — full embedded mode (chdb in-process)
[dependencies]
clickgraph-embedded = { version = "0.6", features = ["embedded"] }

# Cargo.toml — remote / sql-only (no chdb dependency)
[dependencies]
clickgraph-embedded = "0.6"
```

**Full embedded mode (chdb):**

```rust
use clickgraph_embedded::{Connection, Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("schema.yaml", SystemConfig::default())?;  // requires `embedded` feature
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

**Remote mode (no chdb — executes against external ClickHouse):**

```rust
use clickgraph_embedded::{Connection, Database, RemoteConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote = RemoteConfig {
        url: "http://localhost:8123".to_string(),
        user: "default".to_string(),
        password: String::new(),
        database: None,
        cluster_name: None,
    };
    let db = Database::new_remote("schema.yaml", remote)?;
    let conn = Connection::new(&db)?;
    let result = conn.query_remote("MATCH (u:User) RETURN u.name LIMIT 5")?;
    for row in result { println!("{:?}", row); }
    Ok(())
}
```

**SQL-only mode (translate without executing):**

```rust
let db = Database::sql_only("schema.yaml")?;
let conn = Connection::new(&db)?;
let sql = conn.query_to_sql("MATCH (u:User)-[:FOLLOWS]->(f) RETURN f.name LIMIT 5")?;
println!("{}", sql);
```

### Option C — Python Library

Use ClickGraph from Python — ideal for data science and analytics:

```bash
# Build the shared library
cargo build -p clickgraph-ffi

# Set up the Python package
cd clickgraph-py
ln -sf $(realpath ../target/debug/libclickgraph_ffi.so) clickgraph/libclickgraph_ffi.so
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
# → SELECT <table>.<column> AS `u.name` FROM <database>.<table>
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

### Option D — Go Library

Embed ClickGraph in a Go application. Bindings are auto-generated via [Mozilla UniFFI](https://github.com/mozilla/uniffi-rs):

```go
package main

import (
    "fmt"
    "log"

    clickgraph "github.com/genezhang/clickgraph-go"
)

func main() {
    db, err := clickgraph.Open("schema.yaml")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    conn, err := db.Connect()
    if err != nil { log.Fatal(err) }
    defer conn.Close()

    result, err := conn.Query("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 10")
    if err != nil { log.Fatal(err) }
    defer result.Close()

    for result.HasNext() {
        row := result.Next()
        fmt.Printf("%v → %v\n", row.Get("u.name"), row.Get("f.name"))
    }
}
```

**With S3 credentials:**

```go
db, _ := clickgraph.OpenWithConfig("schema.yaml", clickgraph.Config{
    S3AccessKeyID:     "AKIA...",
    S3SecretAccessKey: "...",
    S3Region:          "us-east-1",
})
```

**SQL debugging:**

```go
sql, _ := conn.QueryToSQL("MATCH (u:User) RETURN u.name")
fmt.Println(sql)
// → SELECT <table>.<column> AS `u.name` FROM <database>.<table>
```

**Export results to files:**

```go
conn.Export("MATCH (u:User) RETURN u.name, u.email", "users.parquet", nil)
conn.Export("MATCH (u:User) RETURN u.name", "users.csv", nil)
conn.Export("MATCH (u:User) RETURN u.name", "data.parquet", &clickgraph.ExportOptions{
    Compression: "zstd",
})
```

**Building** requires the `clickgraph-ffi` Rust shared library:

```bash
cargo build -p clickgraph-ffi --release
export CGO_LDFLAGS="-L/path/to/clickgraph/target/release -lclickgraph_ffi"
export LD_LIBRARY_PATH="/path/to/clickgraph/target/release"
cd clickgraph-go && go build ./...
```

👉 **Full Go API documentation** → [`clickgraph-go/README.md`](../../clickgraph-go/README.md)
👉 **All language bindings** → [Language Bindings](Language-Bindings.md)

**Export results to files:**

Write query results directly to Parquet, CSV, TSV, JSON, or NDJSON files. The format is auto-detected from the file extension:

```python
conn = db.connect()

# Export to Parquet (default, great for downstream pipelines)
conn.export("MATCH (u:User) RETURN u.name, u.email", "users.parquet")

# Export to CSV
conn.export("MATCH (u:User) RETURN u.name, u.email", "users.csv")

# Export with Parquet compression
conn.export("MATCH (u:User) RETURN u.name", "users.parquet", compression="zstd")

# Explicit format when extension doesn't match
conn.export("MATCH (u:User) RETURN u.name", "output.dat", format="parquet")

# Debug: see the generated SQL without executing
print(conn.export_to_sql("MATCH (u:User) RETURN u.name", "users.parquet"))
# → INSERT INTO FUNCTION file('users.parquet', 'Parquet') SELECT ...
```

Supported file extensions: `.parquet` / `.pq`, `.csv`, `.tsv`, `.json`, `.ndjson` / `.jsonl`

**APOC Export Procedures (Neo4j-compatible):**

Use Neo4j APOC-style `CALL` syntax for exports with flexible destinations:

```python
conn = db.connect()

# Export to local file
conn.query('CALL apoc.export.parquet.query("MATCH (u:User) RETURN u.name", "/tmp/users.parquet", {})')

# Export to CSV
conn.query('CALL apoc.export.csv.query("MATCH (u:User) RETURN u.name, u.email", "users.csv", {})')

# Export to S3
conn.query('CALL apoc.export.json.query("MATCH (u:User) RETURN u", "s3://bucket/users.json", {})')

# Export with Parquet compression
conn.query('CALL apoc.export.parquet.query("MATCH (u:User) RETURN u.name", "output.parquet", {compression: "zstd"})')
```

Supported destinations: local files, `s3://`, `gs://`, `azure://`, `http://`, `https://`

The APOC export syntax also works in server mode (HTTP and Bolt), providing a unified export interface across all deployment modes.

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

// Export results to a file (Parquet, CSV, TSV, JSON, NDJSON)
conn.export("MATCH (u:User) RETURN u.name, u.email", "users.parquet", ExportOptions::default())?;

// Export with compression
conn.export("MATCH (u:User) RETURN u.name", "users.parquet", ExportOptions {
    compression: Some("zstd".to_string()),
    ..Default::default()
})?;

// Debug: inspect the export SQL without executing
let sql = conn.export_to_sql("MATCH (u:User) RETURN u.name", "users.parquet", ExportOptions::default())?;
// → INSERT INTO FUNCTION file('users.parquet', 'Parquet') SELECT ...

// Structured graph result (nodes + edges)
let graph = conn.query_graph("MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 100")?;
println!("{} nodes, {} edges", graph.node_count(), graph.edge_count());

// Remote query (requires RemoteConfig in SystemConfig)
let graph = conn.query_remote_graph("MATCH (u:User) RETURN u LIMIT 1000")?;
let stats = conn.store_subgraph(&graph)?;
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

    // Optional remote ClickHouse connection (hybrid mode):
    remote: Some(RemoteConfig {
        url: "http://ch-cluster:8123".to_string(),
        user: "analyst".to_string(),
        password: "secret".to_string(),
        database: Some("analytics".to_string()),
        cluster_name: None,
    }),

    // Reserved for future use:
    data_dir:    None,   // base dir for relative source: paths (coming soon)
    max_threads: None,   // thread count for chdb (coming soon)
};
```

### Write API (Embedded Mode Only)

The write API enables AI agents to build knowledge graphs incrementally — extract entities from documents, store them, then query with Cypher for GraphRAG context retrieval.

**Requirements**: Schema entries for writable tables must NOT have a `source:` field. On startup, ClickGraph auto-creates `ReplacingMergeTree` tables for these entries.

```yaml
# Schema with both read-only and writable tables
nodes:
  - label: Document
    source: "s3://bucket/documents.parquet"  # Read-only (VIEW)
    # ...
  - label: Entity
    database: knowledge
    table: entities          # Writable (no source: field)
    node_id: entity_id
    property_mappings:
      name: name
      type: entity_type

edges:
  - type: MENTIONS
    database: knowledge
    table: mentions           # Writable (no source: field)
    from_id: doc_id
    to_id: entity_id
    from_node: Document
    to_node: Entity
```

#### Creating Nodes and Edges

```rust
use std::collections::HashMap;
use clickgraph_embedded::Value;

let conn = Connection::new(&db)?;

// Create a node — auto-generated UUID
let props = HashMap::from([
    ("name".to_string(), Value::String("Alice".into())),
    ("type".to_string(), Value::String("Person".into())),
]);
let id = conn.create_node("Entity", props)?;
println!("Created entity: {}", id);

// Create a node with caller-provided ID (for deduplication)
let props = HashMap::from([
    ("entity_id".to_string(), Value::String("person:alice".into())),
    ("name".to_string(), Value::String("Alice".into())),
    ("type".to_string(), Value::String("Person".into())),
]);
let id = conn.create_node("Entity", props)?;

// Create an edge
let edge_props = HashMap::from([]);
conn.create_edge("MENTIONS", "doc:123", "person:alice", edge_props)?;
```

#### Upsert (Deduplicate)

Re-extracting the same entity from a different document? `upsert_node` inserts with `ReplacingMergeTree` dedup — latest version wins.

> **Deduplication timing**: ReplacingMergeTree deduplication is *eventual* — it happens during background merges, not at INSERT time. Cypher queries automatically use `FINAL` to read only the latest version, so query results are always consistent. No action needed from the caller.

```rust
// Upsert — requires the ID property
let props = HashMap::from([
    ("entity_id".to_string(), Value::String("person:alice".into())),
    ("name".to_string(), Value::String("Alice Johnson".into())),  // Updated name
    ("type".to_string(), Value::String("Person".into())),
]);
conn.upsert_node("Entity", props)?;  // Replaces previous version
```

#### Batch Operations

For bulk extraction, batch methods generate a single INSERT with multiple rows:

```rust
let batch = vec![
    HashMap::from([
        ("entity_id".into(), Value::String("person:alice".into())),
        ("name".into(), Value::String("Alice".into())),
        ("type".into(), Value::String("Person".into())),
    ]),
    HashMap::from([
        ("entity_id".into(), Value::String("org:acme".into())),
        ("name".into(), Value::String("Acme Corp".into())),
        ("type".into(), Value::String("Organization".into())),
    ]),
];
let ids = conn.create_nodes("Entity", batch)?;
```

#### Query After Write

After writing, query the accumulated graph with Cypher as usual:

```rust
let result = conn.query("MATCH (e:Entity) WHERE e.type = 'Person' RETURN e.name")?;
```

#### Raw SQL (Tier 1)

For advanced use cases, `execute_sql` passes raw SQL directly to chdb:

```rust
conn.execute_sql("INSERT INTO knowledge.entities (entity_id, name) VALUES ('x', 'Test')")?;
```

#### Python Example

```python
from clickgraph import Database, Connection, SystemConfig

db = Database("schema.yaml", SystemConfig())
conn = Connection(db)

# Create nodes
conn.create_node("Entity", {"entity_id": "person:alice", "name": "Alice", "type": "Person"})
conn.create_node("Entity", {"entity_id": "org:acme", "name": "Acme Corp", "type": "Organization"})

# Create edge
conn.create_edge("MENTIONS", "doc:123", "person:alice", {})

# Query
result = conn.query("MATCH (d:Document)-[:MENTIONS]->(e:Entity) RETURN e.name")
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
| **Export to file** | ❌ | ✅ Parquet, CSV, TSV, JSON, NDJSON |
| HTTP + Bolt endpoints | ✅ | ✅ (`--embedded` flag) |
| Rust library API | ❌ | ✅ (`clickgraph-embedded`) |
| Python library API | ❌ | ✅ (`clickgraph-py`) |
| Go library API | ❌ | ✅ (`clickgraph-go`) |
| Performance at scale | ✅ Full ClickHouse cluster | ✅ Single-node chdb |
| Schema admin endpoints | ✅ | ⚠️ Unavailable (no ClickHouse) |
| Cargo feature flag | (default) | `--features embedded` |

---

## Hybrid Remote Query + Local Storage

Execute Cypher queries against a remote ClickHouse cluster from embedded mode, then store results locally in chdb for fast re-querying. No separate ClickGraph server needed — embedded mode handles Cypher→SQL translation and the remote executor is cleanly separated from server code.

### Setup

Provide a `RemoteConfig` when opening the database:

```rust
use clickgraph_embedded::{Database, SystemConfig, RemoteConfig};

let db = Database::new("schema.yaml", SystemConfig {
    session_dir: Some("/tmp/local-graphrag".into()),
    remote: Some(RemoteConfig {
        url: "http://ch-cluster:8123".to_string(),
        user: "analyst".to_string(),
        password: "secret".to_string(),
        database: Some("analytics".to_string()),
        cluster_name: None,
    }),
    ..Default::default()
})?;
```

### Workflow

```rust
let conn = Connection::new(&db)?;

// 1. Query remote cluster → structured graph result
let graph = conn.query_remote_graph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.country = 'US' RETURN u, r, f LIMIT 10000"
)?;
// graph.nodes(): [GraphNode { id: "User:42", labels: ["User"], properties: {...} }, ...]
// graph.edges(): [GraphEdge { id: "FOLLOWS:42:99", type_name: "FOLLOWS", ... }, ...]

// 2. Store subgraph locally in chdb
let stats = conn.store_subgraph(&graph)?;
// stats.nodes_stored, stats.edges_stored

// 3. Query local subgraph for GraphRAG context
let result = conn.query("MATCH (u:User)-[:FOLLOWS*1..3]->(f:User) RETURN u.name, f.name")?;
```

### Available Methods

| Method | Description |
|--------|-------------|
| `query_remote(cypher)` | Execute on remote cluster, return tabular `QueryResult` |
| `query_remote_graph(cypher)` | Execute on remote cluster, return structured `GraphResult` |
| `query_graph(cypher)` | Execute locally, return structured `GraphResult` |
| `store_subgraph(graph)` | Decompose `GraphResult` into nodes/edges, batch-INSERT locally |

### Python Example

```python
db = clickgraph.Database("schema.yaml",
    session_dir="/tmp/local-graphrag",
    remote_url="http://ch-cluster:8123",
    remote_user="analyst",
    remote_password="secret",
    remote_database="analytics")
conn = db.connect()

graph = conn.query_remote_graph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 10000")
stats = conn.store_subgraph(graph)
print(f"Stored {stats.nodes_stored} nodes, {stats.edges_stored} edges")

result = conn.query("MATCH (u:User)-[:FOLLOWS*1..3]->(f:User) RETURN u.name, f.name")
```

### Go Example

```go
db, _ := clickgraph.OpenWithConfig("schema.yaml", clickgraph.Config{
    SessionDir: "/tmp/local-graphrag",
    Remote: &clickgraph.RemoteConfig{
        URL: "http://ch-cluster:8123", User: "analyst", Password: "secret",
        Database: "analytics",
    },
})
defer db.Close()

conn, _ := db.Connect()
defer conn.Close()

graph, _ := conn.QueryRemoteGraph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 10000")
defer graph.Close()

stats, _ := conn.StoreSubgraph(graph)
fmt.Printf("Stored %d nodes, %d edges\n", stats.NodesStored, stats.EdgesStored)
```

---

## Limitations

- **Write support is embedded-only**: The typed write API (`create_node`, `create_edge`, etc.) is only available in embedded mode. Server mode remains read-only to protect production ClickHouse.
- **Cypher write syntax not supported**: `CREATE`, `SET`, `DELETE`, `MERGE` Cypher statements are not parsed. Use the typed API or `execute_sql()` for writes.
- **Standard schema only**: Writable tables use standard node/edge schemas. Denormalized, coupled, and polymorphic schemas remain read-only.
- **No DELETE**: Row deletion is not supported. Use upsert to update existing records.
- **Build size**: The `embedded` feature links the chdb native library (~100 MB).
- **Schema admin endpoints** (`/schemas/load`, `/schemas/discover-prompt`) are not available in embedded server mode.
- **`data_dir` and `max_threads`** in `SystemConfig` are reserved for future releases.

---

## Related Pages

- **[Language Bindings](Language-Bindings.md)** — Comparison of Rust, Python, and Go APIs
- **[Schema Basics](Schema-Basics.md)** — YAML schema configuration
- **[API Reference HTTP](API-Reference-HTTP.md)** — HTTP endpoint documentation
- **[Quick Start Guide](Quick-Start-Guide.md)** — Standard ClickHouse setup
