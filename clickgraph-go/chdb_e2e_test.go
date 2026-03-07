package clickgraph

// End-to-end tests with real chdb execution.
//
// These tests create CSV data in a temp directory, build a schema that
// references them via table_function:file(…, CSVWithNames), open a real
// chdb-backed Database via Open(), execute Cypher queries through the
// full Go → cgo/UniFFI → Rust → chdb pipeline, and verify actual results.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture: creates CSV data files and schema YAML in a temp directory.
// ---------------------------------------------------------------------------

type chdbFixture struct {
	dir        string
	schemaPath string
}

func newChdbFixture(t *testing.T) *chdbFixture {
	t.Helper()
	dir := t.TempDir()

	// Users CSV
	usersCSV := filepath.Join(dir, "users.csv")
	must(t, os.WriteFile(usersCSV, []byte(
		"user_id,full_name,age,country\n"+
			"1,Alice,30,US\n"+
			"2,Bob,25,UK\n"+
			"3,Charlie,35,CA\n"+
			"4,Diana,28,US\n"+
			"5,Eve,32,DE\n",
	), 0644))

	// Follows CSV
	followsCSV := filepath.Join(dir, "follows.csv")
	must(t, os.WriteFile(followsCSV, []byte(
		"follower_id,followed_id,follow_date\n"+
			"1,2,2024-01-15\n"+
			"1,3,2024-02-20\n"+
			"2,3,2024-03-10\n"+
			"3,1,2024-04-05\n"+
			"4,1,2024-05-12\n"+
			"4,2,2024-06-01\n"+
			"5,1,2024-07-20\n",
	), 0644))

	// Schema YAML with absolute paths to CSV files
	schemaYAML := fmt.Sprintf(`name: chdb_e2e
graph_schema:
  nodes:
    - label: User
      database: default
      table: users
      node_id: user_id
      source: "table_function:file('%s', 'CSVWithNames')"
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
      source: "table_function:file('%s', 'CSVWithNames')"
      property_mappings:
        follow_date: follow_date
`, usersCSV, followsCSV)

	schemaPath := filepath.Join(dir, "schema.yaml")
	must(t, os.WriteFile(schemaPath, []byte(schemaYAML), 0644))

	return &chdbFixture{dir: dir, schemaPath: schemaPath}
}

// chdbConn opens a real chdb-backed Database and returns a Connection.
func (f *chdbFixture) chdbConn(t *testing.T) *Connection {
	t.Helper()
	db, err := Open(f.schemaPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Node scan tests
// ---------------------------------------------------------------------------

func TestChdb_BasicNodeScan(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) RETURN u.name ORDER BY u.name")
	must(t, err)
	defer result.Close()

	if result.NumRows() != 5 {
		t.Fatalf("expected 5 rows, got %d", result.NumRows())
	}

	expected := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	rows := result.Rows()
	for i, want := range expected {
		got, ok := rows[i].Get("u.name").(string)
		if !ok || got != want {
			t.Errorf("row %d: expected %q, got %v", i, want, rows[i].Get("u.name"))
		}
	}
}

func TestChdb_WhereFilterGreaterThan(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) WHERE u.age > 30 RETURN u.name ORDER BY u.name")
	must(t, err)
	defer result.Close()

	expected := []string{"Charlie", "Eve"}
	rows := result.Rows()
	if len(rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
	}
	for i, want := range expected {
		got := rows[i].Get("u.name").(string)
		if got != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got)
		}
	}
}

func TestChdb_WhereFilterEquals(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) WHERE u.country = 'US' RETURN u.name ORDER BY u.name")
	must(t, err)
	defer result.Close()

	expected := []string{"Alice", "Diana"}
	rows := result.Rows()
	if len(rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
	}
	for i, want := range expected {
		got := rows[i].Get("u.name").(string)
		if got != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Aggregation tests
// ---------------------------------------------------------------------------

func TestChdb_CountAggregation(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) RETURN count(u) AS cnt")
	must(t, err)
	defer result.Close()

	if result.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", result.NumRows())
	}
	row := result.Rows()[0]
	cnt, ok := row.Get("cnt").(int64)
	if !ok || cnt != 5 {
		t.Errorf("expected cnt=5, got %v (type %T)", row.Get("cnt"), row.Get("cnt"))
	}
}

func TestChdb_CountByCountry(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query(
		"MATCH (u:User) RETURN u.country AS country, count(u) AS cnt " +
			"ORDER BY cnt DESC, country")
	must(t, err)
	defer result.Close()

	if result.NumRows() != 4 {
		t.Fatalf("expected 4 rows, got %d", result.NumRows())
	}
	// First row should be US with count 2
	first := result.Rows()[0]
	if got := first.Get("country").(string); got != "US" {
		t.Errorf("expected country=US, got %q", got)
	}
	if got := first.Get("cnt").(int64); got != 2 {
		t.Errorf("expected cnt=2, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// ORDER BY and LIMIT
// ---------------------------------------------------------------------------

func TestChdb_OrderByLimit(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 3")
	must(t, err)
	defer result.Close()

	// Charlie(35), Eve(32), Alice(30)
	expected := []string{"Charlie", "Eve", "Alice"}
	rows := result.Rows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, want := range expected {
		got := rows[i].Get("u.name").(string)
		if got != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// DISTINCT
// ---------------------------------------------------------------------------

func TestChdb_DistinctValues(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query("MATCH (u:User) RETURN DISTINCT u.country ORDER BY u.country")
	must(t, err)
	defer result.Close()

	expected := []string{"CA", "DE", "UK", "US"}
	rows := result.Rows()
	if len(rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
	}
	for i, want := range expected {
		got := rows[i].Get("u.country").(string)
		if got != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Relationship traversal
// ---------------------------------------------------------------------------

func TestChdb_RelationshipTraversal(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	// Alice (user_id=1) follows Bob (2) and Charlie (3)
	result, err := conn.Query(
		"MATCH (a:User)-[:FOLLOWS]->(b:User) " +
			"WHERE a.user_id = 1 " +
			"RETURN b.name ORDER BY b.name")
	must(t, err)
	defer result.Close()

	expected := []string{"Bob", "Charlie"}
	rows := result.Rows()
	if len(rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
	}
	for i, want := range expected {
		got := rows[i].Get("b.name").(string)
		if got != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got)
		}
	}
}

func TestChdb_FollowerCount(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	// Followers of Alice (user_id=1): Charlie(3), Diana(4), Eve(5) → 3
	result, err := conn.Query(
		"MATCH (a:User)-[:FOLLOWS]->(b:User) " +
			"WHERE b.user_id = 1 " +
			"RETURN b.name, count(a) AS follower_count")
	must(t, err)
	defer result.Close()

	if result.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", result.NumRows())
	}
	row := result.Rows()[0]
	if got := row.Get("b.name").(string); got != "Alice" {
		t.Errorf("expected Alice, got %q", got)
	}
	if got := row.Get("follower_count").(int64); got != 3 {
		t.Errorf("expected follower_count=3, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Multiple properties
// ---------------------------------------------------------------------------

func TestChdb_MultipleProperties(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	result, err := conn.Query(
		"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.age, u.country")
	must(t, err)
	defer result.Close()

	if result.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", result.NumRows())
	}
	row := result.Rows()[0]
	if got := row.Get("u.name").(string); got != "Alice" {
		t.Errorf("expected Alice, got %q", got)
	}
	if got := row.Get("u.age").(int64); got != 30 {
		t.Errorf("expected age=30, got %d", got)
	}
	if got := row.Get("u.country").(string); got != "US" {
		t.Errorf("expected country=US, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Export to file (real execution)
// ---------------------------------------------------------------------------

func TestChdb_ExportToParquet(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	outPath := filepath.Join(f.dir, "output.parquet")
	err := conn.Export("MATCH (u:User) RETURN u.name ORDER BY u.name", outPath, nil)
	must(t, err)

	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("output file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}
}

func TestChdb_ExportToCSV(t *testing.T) {
	f := newChdbFixture(t)
	conn := f.chdbConn(t)

	outPath := filepath.Join(f.dir, "output.csv")
	err := conn.Export("MATCH (u:User) RETURN u.name ORDER BY u.name", outPath,
		&ExportOptions{Format: "csv"})
	must(t, err)

	data, err := os.ReadFile(outPath)
	must(t, err)
	content := string(data)
	if len(content) == 0 {
		t.Fatal("CSV output is empty")
	}
	// CSV should contain the names
	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		if !contains(content, name) {
			t.Errorf("CSV missing %q: %s", name, content)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
