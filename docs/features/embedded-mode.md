# Embedded Mode — In-Process Graph Queries with chdb

ClickGraph can run entirely **in-process** using [chdb](https://github.com/chdb-io/chdb) — ClickHouse's embeddable engine — with no external ClickHouse server required. This is similar to how [DuckDB](https://duckdb.org/) and [Kuzu](https://kuzudb.com/) work.

Two modes are available:

| Mode | Description |
|------|-------------|
| **Server embedded** | Run the ClickGraph HTTP/Bolt server with `--embedded` flag — no ClickHouse server needed |
| **Library embedded** | Embed ClickGraph directly in your Rust application via the `clickgraph-embedded` crate |

---

## Why Embedded Mode?

- **Zero dependencies**: Query Parquet, Iceberg, Delta files or local data without running a database server
- **Single binary deployment**: Bundle the graph query engine into your application
- **Fast iteration**: Load files directly during development without a database setup
- **Edge deployments**: Run graph analytics on-device or in serverless environments

---

## Cargo Feature Flag

Embedded mode is gated behind the `embedded` Cargo feature (it requires linking the chdb native library):

```toml
[dependencies]
clickgraph = { version = "0.6", features = ["embedded"] }
```

Or for the standalone library crate:

```toml
[dependencies]
clickgraph-embedded = { version = "0.6", features = [] }  # embedded is the default
```

---

## Library API (`clickgraph-embedded` crate)

The API is modelled after [Kuzu's Rust API](https://docs.kuzudb.com/client-apis/rust/) for familiarity.

### API Comparison

| Kuzu | ClickGraph Embedded |
|------|---------------------|
| `Database::new(path, config)` | `Database::new(schema_yaml, config)` |
| `Connection::new(&db)` | `Connection::new(&db)` |
| `conn.query(cypher)` | `conn.query(cypher)` |
| `result.next()` → `FlatTuple` | `result.next()` → `Row` |
| `row[0]` | `row[0]` |

### Key differences from Kuzu

- **No data loading step** — ClickGraph reads Parquet/Iceberg/Delta files directly via the `source:` field in the schema YAML.
- **No DDL** — graph structure is declared in YAML, not Cypher `CREATE NODE TABLE` statements.
- **Read-only** — ClickGraph is an analytical engine; write operations are out of scope.

### Quick Start

```rust
use clickgraph_embedded::{Connection, Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database from a YAML schema
    let db = Database::new("schema.yaml", SystemConfig::default())?;

    // Create a connection (multiple connections can share one Database)
    let conn = Connection::new(&db)?;

    // Run a Cypher query
    let mut result = conn.query("MATCH (u:User) RETURN u.name, u.country LIMIT 5")?;

    // Iterate rows
    while let Some(row) = result.next() {
        println!("{} from {}", row[0], row[1]);
    }

    Ok(())
}
```

### Iterating Results

```rust
let mut result = conn.query("MATCH (u:User) RETURN u.name, u.user_id")?;

// Option 1: while let
while let Some(row) = result.next() {
    println!("name={}, id={}", row[0], row[1]);
}

// Option 2: for loop (QueryResult implements IntoIterator)
let result = conn.query("MATCH (u:User) RETURN u.name LIMIT 10")?;
for row in result {
    println!("{}", row[0]);
}

// Option 3: collect
let result = conn.query("MATCH (u:User) RETURN count(u) AS n")?;
let rows: Vec<_> = result.collect();
println!("count = {}", rows[0][0]);
```

### Accessing Column Values

`Row` is indexed by position (`row[0]`) and each value is a `Value` enum:

```rust
use clickgraph_embedded::Value;

let mut result = conn.query("MATCH (u:User) RETURN u.name, u.user_id, u.is_active")?;
while let Some(row) = result.next() {
    match &row[0] {
        Value::String(s) => println!("name: {}", s),
        Value::Null      => println!("name: (null)"),
        other            => println!("name: {:?}", other),
    }

    // Convenience conversions
    let id: Option<i64> = row[1].as_i64();
    let active: Option<bool> = row[2].as_bool();
}
```

`Value` variants: `Null`, `Bool(bool)`, `Int64(i64)`, `Float64(f64)`, `String(String)`, `Array(Vec<Value>)`, `Object(serde_json::Map<..>)`.

### Debug: View Generated SQL

```rust
let conn = Connection::new(&db)?;
let sql = conn.query_to_sql("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name")?;
println!("{}", sql);
```

### SystemConfig

```rust
use clickgraph_embedded::{SystemConfig, StorageCredentials};
use std::path::PathBuf;

let config = SystemConfig {
    // Where chdb stores its session files.
    // None (default) = auto temp dir, cleaned up on drop.
    session_dir: Some(PathBuf::from("/tmp/my-session")),

    // Storage credentials for remote sources
    credentials: StorageCredentials {
        s3_access_key_id:     Some("AKIAIOSFODNN7EXAMPLE".into()),
        s3_secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
        s3_region:            Some("us-east-1".into()),
        s3_endpoint_url:      None,  // use AWS default
        gcs_credentials_file: None,
        azure_connection_str: None,
    },

    ..SystemConfig::default()
};

let db = Database::new("schema.yaml", config)?;
```

---

## Schema: `source:` Field

Each node or relationship definition in the YAML schema can have an optional `source:` field pointing to file(s) to read data from. ClickGraph creates a chdb VIEW for each source at startup.

```yaml
name: analytics
graph_schema:
  nodes:
    - label: User
      database: mydb
      table: users          # VIEW name — used in generated SQL
      source: s3://my-bucket/users.parquet
      node_id: user_id
      property_mappings:
        name: full_name
        email: email_address

    - label: Post
      database: mydb
      table: posts
      source: file:///data/posts.parquet
      node_id: post_id
      property_mappings:
        title: title
        content: body

  edges:
    - type: FOLLOWS
      database: mydb
      table: follows
      source: s3://my-bucket/follows.parquet
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
```

### Supported `source:` URI Schemes

| URI Scheme | Mapped to | Notes |
|------------|-----------|-------|
| `/abs/path/file.parquet` | `file('/abs/path/file.parquet', Parquet)` | Absolute local path |
| `./rel/path/file.csv` | `file('./rel/path/file.csv', CSV)` | Relative local path |
| `file:///abs/path/file.parquet` | `file('/abs/path/file.parquet', Parquet)` | Explicit file URI |
| `s3://bucket/key.parquet` | `s3('s3://bucket/key.parquet', Parquet)` | AWS S3 |
| `gs://bucket/key.parquet` | `s3('gs://bucket/key.parquet', Parquet)` | Google Cloud Storage (via S3-compatible API) |
| `iceberg+s3://bucket/table/` | `iceberg('s3://bucket/table/')` | Apache Iceberg on S3 |
| `iceberg+local:///path/to/table/` | `icebergLocal('/path/to/table/')` | Iceberg metadata on local disk |
| `delta+s3://bucket/table/` | `deltaLake('s3://bucket/table/')` | Delta Lake on S3 |
| `table_function:<raw>` | `<raw>` (verbatim) | Escape hatch for custom chdb table functions |

### Auto-Detected File Formats

Format is inferred from the file extension:

| Extension | Format |
|-----------|--------|
| `.parquet`, `.parq` | `Parquet` |
| `.csv` | `CSV` |
| `.tsv` | `TSV` |
| `.json`, `.ndjson` | `JSONEachRow` |
| `.orc` | `ORC` |
| `.avro` | `Avro` |
| (other / directory) | `Parquet` (default) |

### Glob Patterns

chdb supports glob patterns inside table functions, so you can reference entire partitions:

```yaml
source: s3://my-bucket/events/year=2024/month=*/events.parquet
```

---

## Storage Credentials

ClickGraph resolves credentials in priority order:

### 1. Explicit `StorageCredentials` in `SystemConfig` (highest priority)

```rust
let config = SystemConfig {
    credentials: StorageCredentials {
        s3_access_key_id:     Some("AKIA...".into()),
        s3_secret_access_key: Some("secret...".into()),
        s3_region:            Some("us-west-2".into()),
        s3_endpoint_url:      Some("https://s3.us-west-2.amazonaws.com".into()),
        ..Default::default()
    },
    ..SystemConfig::default()
};
```

Applied as chdb `SET` commands before any VIEWs are created — credentials therefore apply automatically to every table function call in the session.

### 2. Environment Variables (inherited automatically)

chdb inherits these standard environment variables if no explicit credentials are set:

| Variable | Purpose |
|----------|---------|
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `AWS_DEFAULT_REGION` | S3 region |
| `AWS_ENDPOINT_URL` | Custom S3 endpoint (MinIO, etc.) |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCS service account JSON path |

### 3. Instance Profile / Workload Identity (lowest priority)

If neither explicit credentials nor environment variables are set, chdb uses:
- **AWS**: IMDSv2 instance profile (EC2, ECS, Lambda)
- **GKE**: Workload Identity via metadata server
- **Azure**: Managed Identity

---

## Server Embedded Mode

Run the ClickGraph HTTP/Bolt server without an external ClickHouse instance:

```bash
# Build with embedded feature
cargo build --features embedded --bin clickgraph

# Start server (reads schema from GRAPH_CONFIG_PATH; data from source: fields)
GRAPH_CONFIG_PATH=./schema.yaml \
CLICKGRAPH_EMBEDDED=true \
cargo run --features embedded --bin clickgraph

# Or with the CLI flag
cargo run --features embedded --bin clickgraph -- --embedded

# With AWS credentials
AWS_ACCESS_KEY_ID=AKIA... \
AWS_SECRET_ACCESS_KEY=secret... \
GRAPH_CONFIG_PATH=./schema.yaml \
cargo run --features embedded --bin clickgraph -- --embedded
```

In embedded mode the standard HTTP and Bolt endpoints remain available — you can connect Neo4j Browser, use the REST API, or drive queries with any Bolt driver, all without a ClickHouse server.

> **Note**: In embedded mode, admin endpoints (`/schemas/load`, `POST /schemas/discover-prompt`) return an error because they require a live ClickHouse connection. Query endpoints (`/query`, Bolt) work fully.

---

## Examples

### Local Parquet Files

```yaml
# schema.yaml
name: social
graph_schema:
  nodes:
    - label: User
      database: social
      table: users
      source: ./data/users.parquet
      node_id: id
      property_mappings:
        name: name
        age: age
  edges:
    - type: KNOWS
      database: social
      table: knows
      source: ./data/knows.parquet
      from_node: User
      to_node: User
      from_id: src
      to_id: dst
      property_mappings: {}
```

```rust
let db = Database::new("schema.yaml", SystemConfig::default())?;
let conn = Connection::new(&db)?;

let result: Vec<_> = conn.query(
    "MATCH (a:User)-[:KNOWS]->(b:User) WHERE a.age > 30 RETURN a.name, b.name LIMIT 10"
)?.collect();
```

### S3 Parquet with Credentials

```rust
let db = Database::new("s3_schema.yaml", SystemConfig {
    credentials: StorageCredentials {
        s3_access_key_id:     Some(std::env::var("AWS_ACCESS_KEY_ID")?),
        s3_secret_access_key: Some(std::env::var("AWS_SECRET_ACCESS_KEY")?),
        s3_region:            Some("us-east-1".into()),
        ..Default::default()
    },
    ..SystemConfig::default()
})?;
```

### Apache Iceberg

```yaml
nodes:
  - label: Transaction
    database: finance
    table: transactions
    source: iceberg+s3://my-data-lake/finance/transactions/
    node_id: txn_id
    property_mappings:
      amount: amount_usd
      ts: created_at
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

### Custom chdb Table Function (Escape Hatch)

For advanced use cases requiring a chdb function not covered by the standard URI schemes:

```yaml
source: "table_function:s3Cluster('mycluster', 's3://bucket/file.parquet', 'key', 'secret', 'Parquet')"
```

The value after `table_function:` is passed verbatim to the `CREATE VIEW` SQL — no escaping is applied.

---

## Testing Without chdb

The `Database::from_executor()` constructor allows injecting a stub executor for unit testing — no chdb installation required:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use async_trait::async_trait;
    use clickgraph_embedded::{Connection, Database};
    use clickgraph::executor::{ExecutorError, QueryExecutor};

    struct StubExecutor {
        rows: Vec<serde_json::Value>,
    }

    #[async_trait]
    impl QueryExecutor for StubExecutor {
        async fn execute_json(&self, _sql: &str, _role: Option<&str>)
            -> Result<Vec<serde_json::Value>, ExecutorError>
        {
            Ok(self.rows.clone())
        }
        async fn execute_text(&self, _sql: &str, _fmt: &str, _role: Option<&str>)
            -> Result<String, ExecutorError>
        {
            Ok(String::new())
        }
    }

    #[test]
    fn test_with_stub() {
        let schema = build_schema_from_yaml(SCHEMA_YAML);
        let db = Database::from_executor(
            schema,
            Arc::new(StubExecutor {
                rows: vec![serde_json::json!({"name": "Alice"})],
            }),
        );
        let conn = Connection::new(&db).unwrap();
        let mut result = conn.query("MATCH (u:User) RETURN u.name").unwrap();
        let row = result.next().unwrap();
        assert_eq!(row[0].to_string(), "Alice");
    }
}
```

---

## Limitations

- **Read-only**: Write Cypher operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are not supported in any mode.
- **chdb feature flag**: Must compile with `--features embedded`; the chdb native library (~100 MB) is linked at build time.
- **Session state**: Each `Database` instance creates its own isolated chdb session. VIEWs created at startup are per-session.
- **`data_dir` and `max_threads`** in `SystemConfig` are reserved for future use — not yet wired.
- **GCS credentials**: Passed via `gcs_credentials_file` in `StorageCredentials` or `GOOGLE_APPLICATION_CREDENTIALS` env var.
- **Azure**: Pass via `azure_connection_str` in `StorageCredentials`.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│             clickgraph-embedded               │
│  Database::new(schema.yaml, SystemConfig)     │
│       ↓ loads schema                          │
│  ChdbExecutor::new_with_credentials(...)      │
│       ↓ creates chdb session                  │
│  data_loader::load_schema_sources(...)        │
│       ↓ CREATE VIEW per source: entry         │
│  Connection::query(cypher)                    │
│       ↓ cypher_to_sql()                       │
│       ↓ executor.execute_json(sql)            │
│       ↓ parse JSON rows → QueryResult         │
└──────────────────────────────────────────────┘
           ↕ in-process FFI
┌─────────────────────────┐
│    chdb (libchdb.so)    │
│  (embedded ClickHouse)  │
└─────────────────────────┘
```

See also: [`notes/embedded-mode.md`](../../notes/embedded-mode.md) for implementation details.
