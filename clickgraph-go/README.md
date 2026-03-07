# clickgraph-go

Go bindings for [ClickGraph](https://github.com/genezhang/clickgraph), an embedded graph query engine that translates Cypher queries into ClickHouse SQL. Query Parquet, Iceberg, Delta Lake, and S3 data as a graph — no server needed.

## Quick Start

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

    // Execute Cypher query
    result, err := conn.Query("MATCH (u:User) RETURN u.name LIMIT 10")
    if err != nil { log.Fatal(err) }
    defer result.Close()

    // Iterate rows
    for result.HasNext() {
        row := result.Next()
        fmt.Println(row.Get("u.name"))
    }
}
```

## API

### Database

```go
// Open with defaults
db, err := clickgraph.Open("schema.yaml")

// Open with config (S3, GCS, Azure credentials)
db, err := clickgraph.OpenWithConfig("schema.yaml", clickgraph.Config{
    S3AccessKeyID:     "...",
    S3SecretAccessKey: "...",
    S3Region:          "us-east-1",
})
defer db.Close()
```

### Connection

```go
conn, err := db.Connect()
defer conn.Close()

// Query
result, err := conn.Query("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 5")

// Translate Cypher to SQL (debugging)
sql, err := conn.QueryToSQL("MATCH (n) RETURN n LIMIT 1")

// Export to file
err = conn.Export("MATCH (u:User) RETURN u.name", "users.parquet", nil)
err = conn.Export("MATCH (u:User) RETURN u.name", "users.csv", &clickgraph.ExportOptions{
    Format:      "csv",
    Compression: "gzip",
})
```

### Result

```go
result, _ := conn.Query("MATCH (u:User) RETURN u.name, u.age")
defer result.Close()

// Cursor-style iteration
for result.HasNext() {
    row := result.Next()
    name := row.Get("u.name")    // column-name access
    vals := row.Values()         // index access
    m    := row.AsMap()          // as map[string]interface{}
}

// Reset cursor
result.Reset()

// Bulk retrieval
rows := result.Rows()

// Metadata
cols := result.ColumnNames()
n    := result.NumRows()
```

### Value Types

Query results are returned as native Go types:

| Cypher/ClickHouse | Go Type |
|---|---|
| NULL | `nil` |
| Boolean | `bool` |
| Integer | `int64` |
| Float | `float64` |
| String | `string` |
| List | `[]interface{}` |
| Map | `map[string]interface{}` |

## Building

Requires the `libclickgraph_ffi` shared library (built from the `clickgraph-ffi` Rust crate):

```bash
# Build the Rust shared library
cd /path/to/clickgraph
cargo build -p clickgraph-ffi --release

# Set library paths for Go
export CGO_LDFLAGS="-L/path/to/clickgraph/target/release -lclickgraph_ffi"
export LD_LIBRARY_PATH="/path/to/clickgraph/target/release"

# Build and test
cd clickgraph-go
go build ./...
go test -v ./...
```

## Architecture

```
Go application
    ↓ (idiomatic Go API)
clickgraph-go/clickgraph.go
    ↓ (cgo/UniFFI generated)
clickgraph-go/clickgraph_ffi/clickgraph_ffi.go
    ↓ (C ABI)
clickgraph-ffi (Rust cdylib)
    ↓
clickgraph-embedded (Rust)
    ↓
chdb (embedded ClickHouse)
```

The Go bindings are auto-generated from the Rust FFI layer using [Mozilla UniFFI](https://github.com/mozilla/uniffi-rs) and [uniffi-bindgen-go](https://github.com/NordSecurity/uniffi-bindgen-go).

## License

Apache-2.0
