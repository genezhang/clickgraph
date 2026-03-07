# Language Bindings

ClickGraph can be embedded directly in your application as a library. In addition to the core **Rust** API, official bindings are available for **Python** and **Go**.

All bindings share the same architecture:

```
Your Application (Rust / Python / Go)
    ↓
clickgraph-embedded (Rust core)
    ↓
chdb (embedded ClickHouse)
    ↓
Parquet / Iceberg / Delta Lake / CSV / S3 / GCS / Azure
```

---

## At a Glance

| | Rust | Python | Go |
|---|---|---|---|
| **Package** | `clickgraph-embedded` | `clickgraph` (PyPI) | `clickgraph-go` |
| **Binding tech** | Native | PyO3 + maturin | UniFFI + cgo |
| **Open database** | `Database::new(path, config)` | `Database(path)` | `Open(path)` |
| **Get connection** | `Connection::new(&db)` | `db.connect()` | `db.Connect()` |
| **Run query** | `conn.query(cypher)` | `conn.query(cypher)` | `conn.Query(cypher)` |
| **Translate to SQL** | `conn.query_to_sql(cypher)` | `conn.query_to_sql(cypher)` | `conn.QueryToSQL(cypher)` |
| **Export to file** | `conn.export(cypher, path, opts)` | `conn.export(cypher, path)` | `conn.Export(cypher, path, opts)` |
| **Row access** | `row[0]`, `row[1]` | `row["col"]` (dict) | `row.Get("col")` |
| **Cursor iteration** | `while let Some(row) = result.next()` | `for row in result:` | `for result.HasNext() { row := result.Next() }` |
| **Bulk access** | `result.collect()` | `result.as_dicts()` | `result.Rows()` |
| **Resource cleanup** | Automatic (Drop) | Automatic (GC) | `defer db.Close()` |
| **Install** | `cargo add clickgraph-embedded` | `maturin develop` | `cargo build -p clickgraph-ffi` + cgo |

---

## Rust

The core embedded API. Zero overhead — all other bindings call through this layer.

```toml
[dependencies]
clickgraph-embedded = "0.6"
```

```rust
use clickgraph_embedded::{Connection, Database, SystemConfig};

let db = Database::new("schema.yaml", SystemConfig::default())?;
let conn = Connection::new(&db)?;

let mut result = conn.query("MATCH (u:User) RETURN u.name LIMIT 5")?;
while let Some(row) = result.next() {
    println!("{}", row[0]);
}
```

**Full API reference** → [Embedded Mode § Rust Library API](Embedded-Mode.md#rust-library-api-reference)

### Value Types (Rust)

```rust
match &row[0] {
    Value::String(s)  => println!("{}", s),
    Value::Int64(n)   => println!("{}", n),
    Value::Float64(f) => println!("{}", f),
    Value::Bool(b)    => println!("{}", b),
    Value::Null       => println!("null"),
    _ => {}
}
```

### Export

```rust
use clickgraph_embedded::ExportOptions;

conn.export("MATCH (u:User) RETURN u.name", "users.parquet", ExportOptions::default())?;
conn.export("MATCH (u:User) RETURN u.name", "users.csv", ExportOptions {
    format: Some("csv".into()),
    ..Default::default()
})?;
```

### Storage Credentials

```rust
use clickgraph_embedded::{StorageCredentials, SystemConfig};

let db = Database::new("schema.yaml", SystemConfig {
    credentials: StorageCredentials {
        s3_access_key_id:     Some("AKIA...".into()),
        s3_secret_access_key: Some("...".into()),
        s3_region:            Some("us-east-1".into()),
        ..Default::default()
    },
    ..SystemConfig::default()
})?;
```

---

## Python

PyO3-based bindings with dict-style row access. Compatible with Kuzu and Neo4j calling conventions.

### Install

```bash
cd clickgraph-py
pip install maturin
maturin develop
```

### Quick Start

```python
import clickgraph

db = clickgraph.Database("schema.yaml")
conn = db.connect()

for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
    print(row["u.name"])
```

### Three Calling Styles

```python
# ClickGraph style
result = conn.query("MATCH (u:User) RETURN u.name")
for row in result:
    print(row["u.name"])

# Kuzu style
conn2 = clickgraph.Connection(db)
result = conn2.execute("MATCH (u:User) RETURN u.name")
while result.has_next():
    row = result.get_next()
    print(row[0])

# Neo4j style
result = conn.run("MATCH (u:User) RETURN u.name")
for row in result:
    print(row["u.name"])
```

### QueryResult API

| Method | Returns | Description |
|--------|---------|-------------|
| `for row in result:` | `dict` | Iterate rows as dicts |
| `result[i]` | `dict` | Index access (supports negative) |
| `result.column_names` | `list[str]` | Column names |
| `result.num_rows` | `int` | Row count |
| `result.as_dicts()` | `list[dict]` | All rows as dicts |
| `result.has_next()` | `bool` | Cursor: more rows? |
| `result.get_next()` | `list` | Cursor: next row as values |
| `result.reset_iterator()` | — | Restart cursor |

### Export

```python
conn.export("MATCH (u:User) RETURN u.name", "users.parquet")
conn.export("MATCH (u:User) RETURN u.name", "users.csv")
conn.export("MATCH (u:User) RETURN u.name", "data.parquet", compression="zstd")
```

### S3 Credentials

```python
db = clickgraph.Database(
    "schema.yaml",
    s3_access_key_id="AKIA...",
    s3_secret_access_key="...",
    s3_region="us-east-1",
)
```

**Full documentation** → [`clickgraph-py/README.md`](../../clickgraph-py/README.md)

---

## Go

Auto-generated bindings via [Mozilla UniFFI](https://github.com/mozilla/uniffi-rs) and [uniffi-bindgen-go](https://github.com/NordSecurity/uniffi-bindgen-go). Idiomatic Go API with `Close()` / `defer` resource management.

### Build

```bash
# 1. Build the Rust shared library
cargo build -p clickgraph-ffi --release

# 2. Set library paths
export CGO_LDFLAGS="-L/path/to/clickgraph/target/release -lclickgraph_ffi"
export LD_LIBRARY_PATH="/path/to/clickgraph/target/release"

# 3. Build and test
cd clickgraph-go
go build ./...
go test -v ./...
```

### Quick Start

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

    result, err := conn.Query("MATCH (u:User) RETURN u.name LIMIT 5")
    if err != nil { log.Fatal(err) }
    defer result.Close()

    for result.HasNext() {
        row := result.Next()
        fmt.Println(row.Get("u.name"))
    }
}
```

### API Summary

| Type | Method | Description |
|------|--------|-------------|
| — | `Open(path) → (*Database, error)` | Open database from schema YAML |
| — | `OpenWithConfig(path, Config{}) → (*Database, error)` | Open with credentials |
| `Database` | `Connect() → (*Connection, error)` | Create connection |
| `Database` | `Close()` | Release resources |
| `Connection` | `Query(cypher) → (*Result, error)` | Execute Cypher |
| `Connection` | `QueryToSQL(cypher) → (string, error)` | Translate to SQL |
| `Connection` | `Export(cypher, path, *ExportOptions) → error` | Export to file |
| `Connection` | `Close()` | Release connection |
| `Result` | `HasNext() → bool` | Cursor: more rows? |
| `Result` | `Next() → *Row` | Cursor: next row |
| `Result` | `Rows() → []*Row` | All rows at once |
| `Result` | `ColumnNames() → []string` | Column names |
| `Result` | `NumRows() → int` | Row count |
| `Result` | `Reset()` | Restart cursor |
| `Result` | `Close()` | Release result |
| `Row` | `Get(column) → interface{}` | Access by column name |
| `Row` | `Values() → []interface{}` | All values in order |
| `Row` | `Columns() → []string` | Column names |
| `Row` | `AsMap() → map[string]interface{}` | Row as map |

### Value Types (Go)

| Cypher / ClickHouse | Go Type |
|---|---|
| NULL | `nil` |
| Boolean | `bool` |
| Integer | `int64` |
| Float | `float64` |
| String | `string` |
| List | `[]interface{}` |
| Map | `map[string]interface{}` |

### Export

```go
// Auto-detect format from extension
conn.Export("MATCH (u:User) RETURN u.name", "users.parquet", nil)
conn.Export("MATCH (u:User) RETURN u.name", "users.csv", nil)

// Explicit options
conn.Export("MATCH ...", "output.dat", &clickgraph.ExportOptions{
    Format:      "parquet",
    Compression: "zstd",
})
```

### S3 Credentials

```go
db, _ := clickgraph.OpenWithConfig("schema.yaml", clickgraph.Config{
    S3AccessKeyID:     "AKIA...",
    S3SecretAccessKey: "...",
    S3Region:          "us-east-1",
})
```

**Full documentation** → [`clickgraph-go/README.md`](../../clickgraph-go/README.md)

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  Your Application                 │
├──────────┬──────────────────┬─────────────────────┤
│   Rust   │     Python       │        Go           │
│  (native)│  (PyO3/maturin)  │  (UniFFI/cgo)       │
├──────────┴──────────────────┴─────────────────────┤
│              clickgraph-embedded (Rust)            │
│  Cypher parser → Query planner → SQL generator    │
├───────────────────────────────────────────────────┤
│                  chdb (embedded ClickHouse)        │
├───────────────────────────────────────────────────┤
│  Parquet │ Iceberg │ Delta │ CSV │ S3 │ GCS │ ... │
└───────────────────────────────────────────────────┘
```

### Binding Layer Details

**Python** (`clickgraph-py/`):
- Uses [PyO3](https://pyo3.rs/) for direct Rust↔Python FFI
- Built with [maturin](https://www.maturin.rs/) for packaging
- ~400 lines of `#[pyclass]` / `#[pymethods]` annotations

**Go** (`clickgraph-go/` + `clickgraph-ffi/`):
- `clickgraph-ffi`: Thin Rust wrapper with `#[uniffi::export]` annotations → compiled as `cdylib`
- `uniffi-bindgen-go`: Auto-generates Go + C header from the compiled library
- `clickgraph-go/clickgraph.go`: Idiomatic Go wrapper over the generated FFI types
- Performance overhead: ~5–10µs per FFI call (negligible vs. query execution time)

---

## Choosing a Binding

| If you need… | Use |
|---|---|
| Maximum performance, no overhead | **Rust** |
| Data science, notebooks, rapid prototyping | **Python** |
| Microservices, CLIs, high concurrency | **Go** |
| Server mode (HTTP / Bolt) | **Standalone server** ([Embedded Mode § Option A](Embedded-Mode.md)) |

All bindings produce identical SQL and query results — the Cypher→SQL translation is shared.

---

## Related Pages

- **[Embedded Mode](Embedded-Mode.md)** — Server, Rust, Python, and Go deployment options
- **[Schema Basics](Schema-Basics.md)** — YAML schema configuration
- **[API Reference HTTP](API-Reference-HTTP.md)** — HTTP endpoint documentation (server mode)
