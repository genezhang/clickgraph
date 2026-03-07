package clickgraph

// Integration tests for ClickGraph Go bindings.
//
// These tests exercise the full pipeline: Go → cgo/UniFFI → Rust parser →
// query planner → SQL generator → back through UniFFI → Go. They use
// OpenSQLOnly() so no chdb backend is required.

import (
	"os"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const testSchemaPath = "testdata/schema.yaml"

func sqlOnlyConn(t *testing.T) *Connection {
	t.Helper()
	db, err := OpenSQLOnly(testSchemaPath)
	if err != nil {
		t.Fatalf("OpenSQLOnly(%q): %v", testSchemaPath, err)
	}
	t.Cleanup(func() { db.Close() })

	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("db.Connect(): %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// assertSQLContains checks that the generated SQL contains all expected substrings.
func assertSQLContains(t *testing.T, sql string, substrings ...string) {
	t.Helper()
	lower := strings.ToLower(sql)
	for _, s := range substrings {
		if !strings.Contains(lower, strings.ToLower(s)) {
			t.Errorf("expected SQL to contain %q, got:\n%s", s, sql)
		}
	}
}

// ---------------------------------------------------------------------------
// Database construction
// ---------------------------------------------------------------------------

func TestSQLOnlyOpen(t *testing.T) {
	db, err := OpenSQLOnly(testSchemaPath)
	if err != nil {
		t.Fatalf("OpenSQLOnly: %v", err)
	}
	defer db.Close()
}

func TestSQLOnlyInvalidPath(t *testing.T) {
	_, err := OpenSQLOnly("/nonexistent/schema.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent schema")
	}
}

func TestSQLOnlyInvalidYAML(t *testing.T) {
	badPath := t.TempDir() + "/bad.yaml"
	if err := os.WriteFile(badPath, []byte("not: valid: schema: {{{"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenSQLOnly(badPath)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestSQLOnlyMultipleConnections(t *testing.T) {
	db, err := OpenSQLOnly(testSchemaPath)
	if err != nil {
		t.Fatalf("OpenSQLOnly: %v", err)
	}
	defer db.Close()

	c1, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Both connections should independently produce SQL
	sql1, err := c1.QueryToSQL("MATCH (u:User) RETURN u.name")
	if err != nil {
		t.Fatal(err)
	}
	sql2, err := c2.QueryToSQL("MATCH (u:User) RETURN u.name")
	if err != nil {
		t.Fatal(err)
	}
	if sql1 != sql2 {
		t.Errorf("connections produced different SQL:\n  c1: %s\n  c2: %s", sql1, sql2)
	}
}

// ---------------------------------------------------------------------------
// Basic MATCH patterns → SQL
// ---------------------------------------------------------------------------

func TestQueryToSQL_BasicMatch(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN u.name")
	if err != nil {
		t.Fatalf("QueryToSQL: %v", err)
	}
	assertSQLContains(t, sql, "users", "full_name")
}

func TestQueryToSQL_MatchWithLimit(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN u.name LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "users", "full_name", "limit", "10")
}

func TestQueryToSQL_MatchWithWhere(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) WHERE u.age > 30 RETURN u.name")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "users", "full_name", "age", "30")
}

func TestQueryToSQL_MatchWithWhereEquals(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "users", "full_name", "email_address", "user_id")
}

func TestQueryToSQL_MultipleProperties(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN u.name, u.email, u.country")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "full_name", "email_address", "country")
}

func TestQueryToSQL_CountAggregation(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN count(u) AS total")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "count")
}

func TestQueryToSQL_OrderBy(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN u.name ORDER BY u.name")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "order by")
}

func TestQueryToSQL_Distinct(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL("MATCH (u:User) RETURN DISTINCT u.country")
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "distinct", "country")
}

// ---------------------------------------------------------------------------
// Relationship patterns → SQL with JOINs
// ---------------------------------------------------------------------------

func TestQueryToSQL_SingleHopRelationship(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "follows", "full_name")
}

func TestQueryToSQL_RelationshipWithFilter(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.country = 'US' RETURN b.name",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "follows", "country", "US")
}

func TestQueryToSQL_RelationshipProperty(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.name, b.name, r.follow_date",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "follows", "follow_date", "full_name")
}

func TestQueryToSQL_DifferentRelType(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "authored", "full_name", "title")
}

func TestQueryToSQL_LikeRelationship(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (u:User)-[l:LIKED]->(p:Post) RETURN u.name, p.title, l.liked_at",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "liked", "liked_at", "full_name", "title")
}

func TestQueryToSQL_FollowerCount(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (u:User)<-[:FOLLOWS]-(f:User) RETURN u.name, count(f) AS followers ORDER BY followers DESC LIMIT 5",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "follows", "count", "order by", "desc", "limit", "5")
}

// ---------------------------------------------------------------------------
// WITH clause (multi-stage queries)
// ---------------------------------------------------------------------------

func TestQueryToSQL_WithClause(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (u:User) WITH u.name AS name, u.age AS age WHERE age > 25 RETURN name",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "full_name", "age")
}

// ---------------------------------------------------------------------------
// OPTIONAL MATCH
// ---------------------------------------------------------------------------

func TestQueryToSQL_OptionalMatch(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.QueryToSQL(
		"MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
	)
	if err != nil {
		t.Fatal(err)
	}
	// OPTIONAL MATCH generates LEFT JOIN
	assertSQLContains(t, sql, "left join", "authored", "full_name", "title")
}

// ---------------------------------------------------------------------------
// Export SQL generation
// ---------------------------------------------------------------------------

func TestExportToSQL_Parquet(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "output.parquet", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "insert into function file(", "output.parquet", "parquet")
}

func TestExportToSQL_CSV(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "results.csv", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "results.csv", "csvwithnames")
}

func TestExportToSQL_JSON(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "data.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "data.json", "json")
}

func TestExportToSQL_NDJSON(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "data.ndjson", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "data.ndjson", "jsoneachrow")
}

func TestExportToSQL_ExplicitFormat(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "data.out", &ExportOptions{
		Format: "parquet",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "data.out", "parquet")
}

func TestExportToSQL_Compression(t *testing.T) {
	conn := sqlOnlyConn(t)
	sql, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "output.parquet", &ExportOptions{
		Compression: "zstd",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSQLContains(t, sql, "zstd")
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestQueryToSQL_InvalidCypher(t *testing.T) {
	conn := sqlOnlyConn(t)
	_, err := conn.QueryToSQL("NOT VALID CYPHER @@@@")
	if err == nil {
		t.Fatal("expected error for invalid Cypher")
	}
}

func TestQueryToSQL_UnknownLabel(t *testing.T) {
	conn := sqlOnlyConn(t)
	_, err := conn.QueryToSQL("MATCH (x:NonExistentLabel) RETURN x")
	if err == nil {
		t.Fatal("expected error for unknown label")
	}
}

func TestExportToSQL_UnknownFormat(t *testing.T) {
	conn := sqlOnlyConn(t)
	_, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "data.xyz", nil)
	if err == nil {
		t.Fatal("expected error for unknown file extension")
	}
}

func TestExportToSQL_InvalidFormatString(t *testing.T) {
	conn := sqlOnlyConn(t)
	_, err := conn.ExportToSQL("MATCH (u:User) RETURN u.name", "data.out", &ExportOptions{
		Format: "xlsx",
	})
	if err == nil {
		t.Fatal("expected error for invalid format string")
	}
}

func TestQuery_FailsInSQLOnlyMode(t *testing.T) {
	conn := sqlOnlyConn(t)
	_, err := conn.Query("MATCH (u:User) RETURN u.name LIMIT 1")
	if err == nil {
		t.Fatal("expected error when executing query in sql_only mode")
	}
	if !strings.Contains(err.Error(), "sql_only") {
		t.Errorf("error should mention sql_only mode, got: %v", err)
	}
}

func TestExport_FailsInSQLOnlyMode(t *testing.T) {
	conn := sqlOnlyConn(t)
	err := conn.Export("MATCH (u:User) RETURN u.name", "/tmp/out.parquet", nil)
	if err == nil {
		t.Fatal("expected error when exporting in sql_only mode")
	}
}
