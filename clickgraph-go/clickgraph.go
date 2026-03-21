// Package clickgraph provides Go bindings for the ClickGraph embedded graph
// query engine. It translates Cypher queries into ClickHouse SQL and executes
// them against Parquet, Iceberg, Delta Lake, or S3 data sources — no server
// needed.
//
// Quick start:
//
//	db, err := clickgraph.Open("schema.yaml")
//	if err != nil { log.Fatal(err) }
//	defer db.Close()
//
//	conn, err := db.Connect()
//	if err != nil { log.Fatal(err) }
//	defer conn.Close()
//
//	result, err := conn.Query("MATCH (u:User) RETURN u.name LIMIT 10")
//	if err != nil { log.Fatal(err) }
//	defer result.Close()
//
//	for result.HasNext() {
//	    row := result.Next()
//	    fmt.Println(row.Get("u.name"))
//	}
package clickgraph

import (
	"fmt"

	ffi "github.com/genezhang/clickgraph-go/clickgraph_ffi"
)

// errClosed is returned when calling methods on a closed Database or Connection.
var errClosed = fmt.Errorf("clickgraph: resource is closed")

// Database is an embedded ClickGraph database loaded from a YAML schema.
// Create with [Open] or [OpenWithConfig]. Must be closed with [Database.Close].
type Database struct {
	inner *ffi.Database
}

// Open creates a new Database from a YAML schema file with default settings.
func Open(schemaPath string) (*Database, error) {
	db, err := ffi.DatabaseOpen(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: open %q: %w", schemaPath, err)
	}
	return &Database{inner: db}, nil
}

// OpenSQLOnly creates a Database in SQL-only mode (no chdb backend).
// Supports QueryToSQL() and ExportToSQL() for Cypher→SQL translation.
// Calling Query() or Export() will return an error.
func OpenSQLOnly(schemaPath string) (*Database, error) {
	db, err := ffi.DatabaseOpenSqlOnly(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: open_sql_only %q: %w", schemaPath, err)
	}
	return &Database{inner: db}, nil
}

// RemoteConfig holds connection details for a remote ClickHouse cluster.
// When provided in [Config], enables [Connection.QueryRemote] and
// [Connection.QueryRemoteGraph] for hybrid remote query + local storage.
type RemoteConfig struct {
	// ClickHouse HTTP endpoint URL (e.g., "http://ch-cluster:8123").
	URL string
	// ClickHouse username.
	User string
	// ClickHouse password.
	Password string
	// Database name (defaults to "default" if empty).
	Database string
	// Cluster name for multi-node round-robin (empty = single-node).
	ClusterName string
}

// Config holds optional settings for the embedded database session.
type Config struct {
	// Directory for chdb session data (temp dir if empty).
	SessionDir string
	// Base directory for relative source: paths in the schema.
	DataDir string
	// Max query threads (0 = default).
	MaxThreads uint32

	// S3 credentials
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
	S3EndpointURL     string
	S3SessionToken    string

	// GCS credentials (HMAC)
	GCSAccessKeyID     string
	GCSSecretAccessKey string

	// Azure credentials
	AzureStorageAccountName      string
	AzureStorageAccountKey       string
	AzureStorageConnectionString string

	// Remote ClickHouse connection for hybrid query + local storage.
	Remote *RemoteConfig
}

// OpenWithConfig creates a new Database with custom configuration.
func OpenWithConfig(schemaPath string, cfg Config) (*Database, error) {
	ffiCfg := ffi.SystemConfig{
		SessionDir:                   strPtr(cfg.SessionDir),
		DataDir:                      strPtr(cfg.DataDir),
		MaxThreads:                   uint32Ptr(cfg.MaxThreads),
		S3AccessKeyId:                strPtr(cfg.S3AccessKeyID),
		S3SecretAccessKey:            strPtr(cfg.S3SecretAccessKey),
		S3Region:                     strPtr(cfg.S3Region),
		S3EndpointUrl:                strPtr(cfg.S3EndpointURL),
		S3SessionToken:               strPtr(cfg.S3SessionToken),
		GcsAccessKeyId:               strPtr(cfg.GCSAccessKeyID),
		GcsSecretAccessKey:           strPtr(cfg.GCSSecretAccessKey),
		AzureStorageAccountName:      strPtr(cfg.AzureStorageAccountName),
		AzureStorageAccountKey:       strPtr(cfg.AzureStorageAccountKey),
		AzureStorageConnectionString: strPtr(cfg.AzureStorageConnectionString),
		Remote:                       toFfiRemoteConfig(cfg.Remote),
	}
	db, err := ffi.DatabaseOpenWithConfig(schemaPath, ffiCfg)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: open %q: %w", schemaPath, err)
	}
	return &Database{inner: db}, nil
}

// Close releases the database resources.
func (db *Database) Close() {
	if db.inner != nil {
		db.inner.Destroy()
		db.inner = nil
	}
}

// Connect creates a new connection to the database.
func (db *Database) Connect() (*Connection, error) {
	if db.inner == nil {
		return nil, fmt.Errorf("clickgraph: connect: %w", errClosed)
	}
	conn, err := db.inner.Connect()
	if err != nil {
		return nil, fmt.Errorf("clickgraph: connect: %w", err)
	}
	return &Connection{inner: conn}, nil
}

// Connection executes Cypher queries against a [Database].
// Must be closed with [Connection.Close].
type Connection struct {
	inner *ffi.Connection
}

// Close releases the connection resources.
func (c *Connection) Close() {
	if c.inner != nil {
		c.inner.Destroy()
		c.inner = nil
	}
}

// Query executes a Cypher query and returns a [Result].
func (c *Connection) Query(cypher string) (*Result, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("clickgraph: query: %w", errClosed)
	}
	qr, err := c.inner.Query(cypher)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: query: %w", err)
	}
	return &Result{inner: qr}, nil
}

// QueryToSQL translates a Cypher query to ClickHouse SQL without executing it.
func (c *Connection) QueryToSQL(cypher string) (string, error) {
	if c.inner == nil {
		return "", fmt.Errorf("clickgraph: query_to_sql: %w", errClosed)
	}
	sql, err := c.inner.QueryToSql(cypher)
	if err != nil {
		return "", fmt.Errorf("clickgraph: query_to_sql: %w", err)
	}
	return sql, nil
}

// ExportOptions controls the output format for [Connection.Export].
type ExportOptions struct {
	// Format name: "parquet", "csv", "tsv", "json", "ndjson".
	// Auto-detected from file extension if empty.
	Format string
	// Parquet compression: "snappy", "gzip", "lz4", "zstd".
	Compression string
}

// Export writes Cypher query results directly to a file.
// Format is auto-detected from the extension if not specified.
func (c *Connection) Export(cypher, outputPath string, opts *ExportOptions) error {
	if c.inner == nil {
		return fmt.Errorf("clickgraph: export: %w", errClosed)
	}
	ffiOpts := ffi.ExportOptions{}
	if opts != nil {
		ffiOpts.Format = strPtr(opts.Format)
		ffiOpts.Compression = strPtr(opts.Compression)
	}
	if err := c.inner.Export(cypher, outputPath, ffiOpts); err != nil {
		return fmt.Errorf("clickgraph: export: %w", err)
	}
	return nil
}

// ExportToSQL generates the export SQL without executing it (for debugging).
func (c *Connection) ExportToSQL(cypher, outputPath string, opts *ExportOptions) (string, error) {
	if c.inner == nil {
		return "", fmt.Errorf("clickgraph: export_to_sql: %w", errClosed)
	}
	ffiOpts := ffi.ExportOptions{}
	if opts != nil {
		ffiOpts.Format = strPtr(opts.Format)
		ffiOpts.Compression = strPtr(opts.Compression)
	}
	sql, err := c.inner.ExportToSql(cypher, outputPath, ffiOpts)
	if err != nil {
		return "", fmt.Errorf("clickgraph: export_to_sql: %w", err)
	}
	return sql, nil
}

// GraphNode is a node in a [GraphResult].
type GraphNode struct {
	ID         string
	Labels     []string
	Properties map[string]interface{}
}

// GraphEdge is an edge in a [GraphResult].
type GraphEdge struct {
	ID         string
	TypeName   string
	FromID     string
	ToID       string
	Properties map[string]interface{}
}

// GraphResult is a structured graph result with deduplicated nodes and edges.
// Returned by [Connection.QueryGraph] and [Connection.QueryRemoteGraph].
// Can be passed to [Connection.StoreSubgraph] to persist locally.
type GraphResult struct {
	inner *ffi.GraphResult
}

// Close releases the graph result resources.
func (g *GraphResult) Close() {
	if g.inner != nil {
		g.inner.Destroy()
		g.inner = nil
	}
}

// Nodes returns all nodes as [GraphNode] values.
func (g *GraphResult) Nodes() []GraphNode {
	ffiNodes := g.inner.Nodes()
	nodes := make([]GraphNode, len(ffiNodes))
	for i, n := range ffiNodes {
		props := make(map[string]interface{}, len(n.Properties))
		for k, v := range n.Properties {
			props[k] = toGoValue(v)
		}
		nodes[i] = GraphNode{
			ID:         n.Id,
			Labels:     n.Labels,
			Properties: props,
		}
	}
	return nodes
}

// Edges returns all edges as [GraphEdge] values.
func (g *GraphResult) Edges() []GraphEdge {
	ffiEdges := g.inner.Edges()
	edges := make([]GraphEdge, len(ffiEdges))
	for i, e := range ffiEdges {
		props := make(map[string]interface{}, len(e.Properties))
		for k, v := range e.Properties {
			props[k] = toGoValue(v)
		}
		edges[i] = GraphEdge{
			ID:         e.Id,
			TypeName:   e.TypeName,
			FromID:     e.FromId,
			ToID:       e.ToId,
			Properties: props,
		}
	}
	return edges
}

// NodeCount returns the number of nodes.
func (g *GraphResult) NodeCount() uint64 {
	return g.inner.NodeCount()
}

// EdgeCount returns the number of edges.
func (g *GraphResult) EdgeCount() uint64 {
	return g.inner.EdgeCount()
}

// StoreStats is the result of [Connection.StoreSubgraph].
type StoreStats struct {
	NodesStored uint64
	EdgesStored uint64
}

// QueryRemote executes a Cypher query against the remote ClickHouse cluster.
// Requires [RemoteConfig] to have been provided in [Config].
func (c *Connection) QueryRemote(cypher string) (*Result, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("clickgraph: query_remote: %w", errClosed)
	}
	qr, err := c.inner.QueryRemote(cypher)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: query_remote: %w", err)
	}
	return &Result{inner: qr}, nil
}

// QueryGraph executes a Cypher query locally and returns a structured [GraphResult].
func (c *Connection) QueryGraph(cypher string) (*GraphResult, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("clickgraph: query_graph: %w", errClosed)
	}
	gr, err := c.inner.QueryGraph(cypher)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: query_graph: %w", err)
	}
	return &GraphResult{inner: gr}, nil
}

// QueryRemoteGraph executes a Cypher query on the remote cluster and returns
// a structured [GraphResult]. The result can be passed to [Connection.StoreSubgraph].
func (c *Connection) QueryRemoteGraph(cypher string) (*GraphResult, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("clickgraph: query_remote_graph: %w", errClosed)
	}
	gr, err := c.inner.QueryRemoteGraph(cypher)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: query_remote_graph: %w", err)
	}
	return &GraphResult{inner: gr}, nil
}

// StoreSubgraph stores a [GraphResult] into local writable tables.
func (c *Connection) StoreSubgraph(graph *GraphResult) (*StoreStats, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("clickgraph: store_subgraph: %w", errClosed)
	}
	stats, err := c.inner.StoreSubgraph(graph.inner)
	if err != nil {
		return nil, fmt.Errorf("clickgraph: store_subgraph: %w", err)
	}
	return &StoreStats{
		NodesStored: stats.NodesStored,
		EdgesStored: stats.EdgesStored,
	}, nil
}

// Result holds the rows returned by a Cypher query. Supports both cursor-style
// iteration (HasNext/Next) and bulk retrieval (Rows).
// Must be closed with [Result.Close].
type Result struct {
	inner *ffi.QueryResult
}

// Close releases the result resources.
func (r *Result) Close() {
	if r.inner != nil {
		r.inner.Destroy()
		r.inner = nil
	}
}

// ColumnNames returns the ordered list of column names.
func (r *Result) ColumnNames() []string {
	return r.inner.ColumnNames()
}

// NumRows returns the total number of rows.
func (r *Result) NumRows() uint64 {
	return r.inner.NumRows()
}

// HasNext returns true if the cursor has more rows.
func (r *Result) HasNext() bool {
	return r.inner.HasNext()
}

// Next returns the next row, advancing the cursor.
// Returns nil when all rows have been consumed.
func (r *Result) Next() *Row {
	ffiRow := r.inner.GetNext()
	if ffiRow == nil {
		return nil
	}
	return &Row{columns: ffiRow.Columns, values: ffiRow.Values}
}

// Reset rewinds the cursor to the first row.
func (r *Result) Reset() {
	r.inner.Reset()
}

// Rows returns all rows at once (bulk retrieval).
func (r *Result) Rows() []Row {
	ffiRows := r.inner.GetAllRows()
	rows := make([]Row, len(ffiRows))
	for i, fr := range ffiRows {
		rows[i] = Row{columns: fr.Columns, values: fr.Values}
	}
	return rows
}

// Row is a single result row with column-name and index access.
type Row struct {
	columns []string
	values  []ffi.Value
}

// Get returns the value for a column by name. Returns nil if not found.
func (r *Row) Get(column string) interface{} {
	for i, col := range r.columns {
		if col == column {
			return toGoValue(r.values[i])
		}
	}
	return nil
}

// Values returns all values in column order as native Go types.
func (r *Row) Values() []interface{} {
	out := make([]interface{}, len(r.values))
	for i, v := range r.values {
		out[i] = toGoValue(v)
	}
	return out
}

// Columns returns the column names for this row.
func (r *Row) Columns() []string {
	return r.columns
}

// AsMap returns the row as a map[string]interface{}.
func (r *Row) AsMap() map[string]interface{} {
	m := make(map[string]interface{}, len(r.columns))
	for i, col := range r.columns {
		m[col] = toGoValue(r.values[i])
	}
	return m
}

// toGoValue converts a UniFFI Value to a native Go type.
func toGoValue(v ffi.Value) interface{} {
	switch val := v.(type) {
	case ffi.ValueNull:
		return nil
	case ffi.ValueBool:
		return val.V
	case ffi.ValueInt64:
		return val.V
	case ffi.ValueFloat64:
		return val.V
	case ffi.ValueString:
		return val.V
	case ffi.ValueDate:
		return val.V // "YYYY-MM-DD" string
	case ffi.ValueTimestamp:
		return val.V // "YYYY-MM-DD HH:MM:SS" string
	case ffi.ValueUuid:
		return val.V // UUID hex string
	case ffi.ValueList:
		items := make([]interface{}, len(val.Items))
		for i, item := range val.Items {
			items[i] = toGoValue(item)
		}
		return items
	case ffi.ValueMap:
		m := make(map[string]interface{}, len(val.Entries))
		for _, entry := range val.Entries {
			m[entry.Key] = toGoValue(entry.Value)
		}
		return m
	default:
		return nil
	}
}

// strPtr returns a *string for non-empty strings, nil otherwise.
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// uint32Ptr returns a *uint32 for non-zero values, nil otherwise.
func uint32Ptr(v uint32) *uint32 {
	if v == 0 {
		return nil
	}
	return &v
}

// toFfiRemoteConfig converts a Go RemoteConfig to an FFI RemoteConfig pointer.
// Returns nil if rc is nil (no remote configured).
func toFfiRemoteConfig(rc *RemoteConfig) *ffi.RemoteConfig {
	if rc == nil {
		return nil
	}
	return &ffi.RemoteConfig{
		Url:         rc.URL,
		User:        rc.User,
		Password:    rc.Password,
		Database:    strPtr(rc.Database),
		ClusterName: strPtr(rc.ClusterName),
	}
}
