# Embedded Mode Tutorials

Step-by-step tutorials for ClickGraph's embedded mode features. Each section includes runnable code — see [`examples/embedded/`](../../examples/embedded/) for the complete scripts.

---

## Tutorial 1: Query CSV Files as a Graph

Query Parquet, CSV, or JSON files directly — no ClickHouse server needed.

### Schema

```yaml
name: social
graph_schema:
  nodes:
    - label: User
      database: default
      table: users
      node_id: user_id
      source: "table_function:file('/path/to/users.csv', 'CSVWithNames')"
      property_mappings:
        user_id: user_id
        name: full_name
        age: age
        country: country
  edges:
    - type: FOLLOWS
      database: default
      table: follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      source: "table_function:file('/path/to/follows.csv', 'CSVWithNames')"
      property_mappings:
        follow_date: follow_date
```

### Python

```python
import clickgraph

db = clickgraph.Database("schema.yaml")
conn = db.connect()

# Basic query
for row in conn.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.age DESC"):
    print(f"{row['u.name']}, age {row['u.age']}")

# Relationship traversal
for row in conn.query(
    "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice Chen' RETURN b.name"
):
    print(f"Alice follows {row['b.name']}")

# Multi-hop traversal
for row in conn.query(
    "MATCH (a:User)-[:FOLLOWS*2]->(c:User) WHERE a.name = 'Alice Chen' "
    "RETURN DISTINCT c.name ORDER BY c.name"
):
    print(f"  2 hops away: {row['c.name']}")
```

### Rust

```rust
use clickgraph_embedded::{Database, Connection, SystemConfig};

let db = Database::new("schema.yaml", SystemConfig::default())?;
let conn = Connection::new(&db)?;

let mut result = conn.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.age DESC")?;
while let Some(row) = result.next() {
    println!("{}, age {}", row[0], row[1]);
}
```

---

## Tutorial 2: DataFrame Output

Convert query results to Pandas, PyArrow, or Polars for data science workflows.

```python
result = conn.query("MATCH (u:User) RETURN u.name, u.age, u.country")

# Pandas DataFrame
df = result.get_as_df()
print(df.describe())
print(df.groupby("u.country")["u.age"].mean())

# PyArrow Table (zero-copy interop with other Arrow tools)
table = result.get_as_arrow()
print(table.schema)

# Polars DataFrame (fast columnar operations)
df = result.get_as_pl()
print(df.filter(df["u.age"] > 30))
```

Each library is a lazy import — only the one you use needs to be installed:

```bash
pip install pandas    # for get_as_df()
pip install pyarrow   # for get_as_arrow()
pip install polars    # for get_as_pl()
```

### Query Timing

```python
result = conn.query("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name")

print(f"Compile: {result._ffi.get_compiling_time():.2f}ms")
print(f"Execute: {result._ffi.get_execution_time():.2f}ms")
print(f"Columns: {result._ffi.get_column_data_types()}")
# → ["String", "String"]
```

---

## Tutorial 3: Write API — Build a Knowledge Graph

Create nodes and edges programmatically — ideal for AI agents extracting entities from documents.

### Schema (writable)

Writable tables omit the `source:` field. ClickGraph auto-creates `ReplacingMergeTree` tables:

```yaml
name: knowledge_graph
graph_schema:
  nodes:
    - label: Entity
      database: default
      table: entities
      node_id: entity_id
      property_mappings:
        entity_id: entity_id
        name: name
        type: entity_type
  edges:
    - type: RELATES_TO
      database: default
      table: relations
      from_node: Entity
      to_node: Entity
      from_id: from_entity_id
      to_id: to_entity_id
      property_mappings:
        relation: relation_type
```

### Python

```python
db = clickgraph.Database("schema.yaml")
conn = db.connect()

# Single node
conn._ffi.create_node("Entity", {
    "entity_id": Value.STRING(v="e1"),
    "name": Value.STRING(v="ClickGraph"),
    "type": Value.STRING(v="Software"),
})

# Batch nodes (single INSERT)
conn._ffi.create_nodes("Entity", [
    {"entity_id": Value.STRING(v="e2"), "name": Value.STRING(v="Rust"), ...},
    {"entity_id": Value.STRING(v="e3"), "name": Value.STRING(v="ClickHouse"), ...},
])

# Create edge
conn._ffi.create_edge("RELATES_TO", "e1", "e2", {
    "relation": Value.STRING(v="written_in"),
})

# Bulk import from file (CSV, Parquet, JSON auto-detected)
conn._ffi.import_file("Entity", "/path/to/entities.csv")
```

### Rust

```rust
use std::collections::HashMap;
use clickgraph_embedded::Value;

let mut props = HashMap::new();
props.insert("entity_id".to_string(), Value::String("e1".to_string()));
props.insert("name".to_string(), Value::String("ClickGraph".to_string()));
conn.create_node("Entity", props)?;

// Bulk import
conn.import_file("Entity", "/path/to/entities.csv")?;  // auto-detects CSV
conn.import_parquet_file("Entity", "/path/to/entities.parquet")?;
```

---

## Tutorial 4: Hybrid Remote Query + Local Storage (GraphRAG)

Execute Cypher queries against a remote ClickHouse cluster, store the resulting subgraph locally, then query locally for fast GraphRAG context retrieval.

```
Remote ClickHouse         Local chdb
┌─────────────┐          ┌────────────┐
│ Large-scale │ ──────── │ Subgraph   │
│ S3/Iceberg  │  query   │ for fast   │
│ data        │  remote  │ re-query   │
└─────────────┘  graph   └────────────┘
                  │              │
          query_remote_graph()   query()
                  │              │
                  ▼              ▼
            GraphResult ──► store_subgraph()
```

### Python

```python
import clickgraph

# Open with remote config
db = clickgraph.Database("schema.yaml",
    session_dir="/tmp/graphrag_session",
    remote_url="http://ch-cluster:8123",
    remote_user="analyst",
    remote_password="secret",
    remote_database="analytics",
)
conn = db.connect()

# 1. Query remote cluster → structured graph
graph = conn.query_remote_graph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) "
    "WHERE u.country = 'US' "
    "RETURN u, r, f LIMIT 10000"
)
print(f"Remote: {graph.node_count} nodes, {graph.edge_count} edges")

# 2. Store subgraph locally
stats = conn.store_subgraph(graph)
print(f"Stored: {stats.nodes_stored} nodes, {stats.edges_stored} edges")

# 3. Fast local queries (no more remote calls)
result = conn.query(
    "MATCH (u:User)-[:FOLLOWS*1..3]->(f:User) "
    "RETURN u.name, f.name"
)
df = result.get_as_df()
print(df.head())
```

### Rust

```rust
use clickgraph_embedded::{Database, Connection, SystemConfig, RemoteConfig};

let db = Database::new("schema.yaml", SystemConfig {
    session_dir: Some("/tmp/graphrag_session".into()),
    remote: Some(RemoteConfig {
        url: "http://ch-cluster:8123".to_string(),
        user: "analyst".to_string(),
        password: "secret".to_string(),
        database: Some("analytics".to_string()),
        cluster_name: None,
    }),
    ..Default::default()
})?;
let conn = Connection::new(&db)?;

// Remote → structured graph
let graph = conn.query_remote_graph(
    "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.country = 'US' RETURN u, r, f"
)?;

// Store locally
let stats = conn.store_subgraph(&graph)?;
println!("Stored {} nodes, {} edges", stats.nodes_stored, stats.edges_stored);

// Fast local query
let result = conn.query("MATCH (u:User) RETURN u.name ORDER BY u.name")?;
```

---

## Tutorial 5: Export to Files

Export query results directly to Parquet, CSV, or JSON files.

```python
# Auto-detect format from extension
conn.export("MATCH (u:User) RETURN u.name, u.age", "users.parquet")
conn.export("MATCH (u:User) RETURN u.name, u.age", "users.csv")
conn.export("MATCH (u:User) RETURN u.name, u.age", "users.ndjson")

# Explicit format + compression
conn.export("MATCH (u:User) RETURN u.name", "users.parquet",
            format="parquet", compression="zstd")

# Preview the generated SQL
sql = conn.export_to_sql("MATCH (u:User) RETURN u.name", "users.parquet")
# → INSERT INTO FUNCTION file('users.parquet', 'Parquet') SELECT ...
```

---

## Quick Reference

| Method | Description |
|--------|-------------|
| `conn.query(cypher)` | Execute Cypher, return `QueryResult` |
| `conn.query_to_sql(cypher)` | Translate Cypher to SQL (no execution) |
| `conn.query_remote(cypher)` | Execute on remote ClickHouse cluster |
| `conn.query_graph(cypher)` | Execute locally, return `GraphResult` |
| `conn.query_remote_graph(cypher)` | Execute remotely, return `GraphResult` |
| `conn.store_subgraph(graph)` | Store `GraphResult` into local tables |
| `conn.export(cypher, path)` | Export results to file |
| `result.get_as_df()` | Convert to Pandas DataFrame |
| `result.get_as_arrow()` | Convert to PyArrow Table |
| `result.get_as_pl()` | Convert to Polars DataFrame |
| `conn._ffi.create_node(label, props)` | Create a single node |
| `conn._ffi.create_edge(type, from, to, props)` | Create a single edge |
| `conn._ffi.import_file(label, path)` | Import from file (CSV/Parquet/JSON) |

### Value Types

| ClickHouse Type | ClickGraph Value | Python | `type_name()` |
|----------------|-----------------|--------|---------------|
| Int64 | `Value::Int64(n)` | `int` | `"Int64"` |
| Float64 | `Value::Float64(f)` | `float` | `"Float64"` |
| String | `Value::String(s)` | `str` | `"String"` |
| Bool | `Value::Bool(b)` | `bool` | `"Bool"` |
| Date | `Value::Date(s)` | `str` | `"Date"` |
| DateTime | `Value::Timestamp(s)` | `str` | `"Timestamp"` |
| UUID | `Value::UUID(s)` | `str` | `"UUID"` |
| NULL | `Value::Null` | `None` | `"Null"` |
| Array | `Value::List(v)` | `list` | `"List"` |
| Map | `Value::Map(v)` | `dict` | `"Map"` |

---

## Related Pages

- **[Embedded Mode](Embedded-Mode.md)** — Full API reference and configuration
- **[Language Bindings](Language-Bindings.md)** — Rust, Python, Go binding comparison
- **[Schema Basics](Schema-Basics.md)** — YAML schema configuration
