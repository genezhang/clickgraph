# clickgraph-go — Agent Guide

> **Purpose**: Go language bindings for ClickGraph's embedded mode. Uses cgo + UniFFI
> to call into `libclickgraph_ffi.so`. Provides a pure-Go API for running Cypher
> queries over Parquet, CSV, and other file formats via embedded chdb.

## Architecture

```
Go application code
  └── clickgraph.go (Go API: Database, Connection, QueryResult)
        └── cgo → libclickgraph_ffi.so (UniFFI-generated C bridge)
              └── clickgraph-ffi (Rust) → clickgraph-embedded → chdb
```

## File Overview

```
clickgraph.go           (520+ lines) ← Public Go API + UniFFI cgo bridge
clickgraph_test.go      (224 lines)  ← Unit tests (no chdb)
integration_test.go     (413 lines)  ← sql_only integration tests (32 tests)
chdb_e2e_test.go        (423 lines)  ← Real chdb e2e tests (12 tests)
go.mod / go.sum                      ← Go module definition
```

## Key API

```go
// Open database with schema
db, err := clickgraph.Open("schema.yaml")
defer db.Close()

// Create connection
conn, err := db.Connect()
defer conn.Close()

// Execute Cypher query
result, err := conn.Query("MATCH (u:User) RETURN u.name")
defer result.Close()

// Access results
for _, row := range result.Rows() {
    name := row.Get("u.name").(string)
}

// SQL-only mode (no chdb needed)
db, err := clickgraph.OpenSqlOnly("schema.yaml")
conn, _ := db.Connect()
sql, err := conn.QueryToSQL("MATCH (u:User) RETURN u.name")

// Export to file
err = conn.Export("MATCH ...", "output.parquet", nil)
err = conn.Export("MATCH ...", "output.csv", &clickgraph.ExportOptions{Format: "csv"})

// Hybrid remote query + local storage
db, err := clickgraph.OpenWithConfig("schema.yaml", clickgraph.Config{
    Remote: &clickgraph.RemoteConfig{
        URL: "http://ch-cluster:8123", User: "analyst", Password: "secret",
    },
})
conn, _ := db.Connect()
graph, err := conn.QueryRemoteGraph("MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 1000")
stats, err := conn.StoreSubgraph(graph)  // → StoreStats{NodesStored, EdgesStored}

// Structured graph result (local)
graph, err := conn.QueryGraph("MATCH (u:User) RETURN u LIMIT 10")
nodes := graph.Nodes()  // []GraphNode{ID, Labels, Properties}
edges := graph.Edges()  // []GraphEdge{ID, TypeName, FromID, ToID, Properties}
```

## Conventions

- **Resource cleanup**: All types that hold FFI resources (`Database`, `Connection`,
  `QueryResult`) have `Close()` methods. Always use `defer obj.Close()`.
- **Error handling**: All methods return `(result, error)` following Go conventions.
  Errors originate from Rust via UniFFI error propagation.
- **Value types**: `Row.Get(column)` returns `interface{}`. Type-assert to
  `string`, `int64`, `float64`, `bool`, or `nil` as needed.
- **Test gating**: chdb e2e tests require `CLICKGRAPH_CHDB_TESTS=1` env var.
  Default `go test` runs only sql_only tests.

## Build Requirements

- Go 1.21+
- `libclickgraph_ffi.so` must be built first: `cargo build -p clickgraph-ffi`
- CGO_ENABLED=1 (default on Linux)
- `CGO_LDFLAGS` and `LD_LIBRARY_PATH` must include the directory containing
  `libclickgraph_ffi.so` and `libchdb.so`

## Test Commands

```bash
# sql_only tests (no chdb required)
CGO_LDFLAGS="-L../target/debug" LD_LIBRARY_PATH="../target/debug" go test -v

# Full e2e tests (requires chdb)
CLICKGRAPH_CHDB_TESTS=1 CGO_LDFLAGS="-L../target/debug" \
  LD_LIBRARY_PATH="../target/debug:../target/debug/build/chdb-rust-*/out" \
  go test -v
```
