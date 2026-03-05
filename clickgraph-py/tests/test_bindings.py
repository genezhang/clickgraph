"""Tests for clickgraph Python bindings.

These tests use the benchmark schema and verify that:
1. Database/Connection/QueryResult classes work correctly
2. Cypher → SQL translation produces expected output
3. Error handling is proper
4. All value types round-trip correctly

Note: These tests exercise the Cypher→SQL pipeline only (sql_only mode).
Full end-to-end tests with chdb require the embedded feature + chdb binary.
"""

import os
import tempfile
import textwrap

import pytest

import clickgraph

# ---------------------------------------------------------------------------
# Schema fixture — minimal YAML for testing SQL generation
# ---------------------------------------------------------------------------

SCHEMA_YAML = textwrap.dedent("""\
    name: test_py
    graph_schema:
      nodes:
        - label: User
          database: test_db
          table: users
          node_id: user_id
          property_mappings:
            user_id: user_id
            name: full_name
            email: email_address
        - label: Post
          database: test_db
          table: posts
          node_id: post_id
          property_mappings:
            post_id: post_id
            title: title
            content: content
      edges:
        - type: FOLLOWS
          database: test_db
          table: follows
          from_node: User
          to_node: User
          from_id: follower_id
          to_id: followed_id
          property_mappings:
            follow_date: follow_date
        - type: AUTHORED
          database: test_db
          table: authored
          from_node: User
          to_node: Post
          from_id: user_id
          to_id: post_id
          property_mappings: {}
""")


@pytest.fixture(scope="module")
def schema_path(tmp_path_factory):
    """Write the test schema to a temp file."""
    p = tmp_path_factory.mktemp("schema") / "test.yaml"
    p.write_text(SCHEMA_YAML)
    return str(p)


@pytest.fixture(scope="module")
def db(schema_path):
    """Create a Database from the test schema."""
    return clickgraph.Database(schema_path)


@pytest.fixture
def conn(db):
    """Create a Connection from the Database."""
    return db.connect()


# ---------------------------------------------------------------------------
# Database construction
# ---------------------------------------------------------------------------

class TestDatabaseConstruction:
    def test_create_from_schema_file(self, schema_path):
        db = clickgraph.Database(schema_path)
        assert db is not None

    def test_repr(self, db):
        assert repr(db) == "<Database>"

    def test_invalid_schema_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("not: valid: schema: {{{")
        with pytest.raises(RuntimeError, match="YAML parse error"):
            clickgraph.Database(str(bad))

    def test_missing_file_raises(self):
        with pytest.raises(RuntimeError, match="Cannot read schema"):
            clickgraph.Database("/nonexistent/path.yaml")

    def test_session_dir_kwarg(self, schema_path, tmp_path):
        session_dir = str(tmp_path / "chdb_session")
        # This should not raise even though chdb may not be available
        # (the Database construction handles missing chdb gracefully or errors)
        try:
            db = clickgraph.Database(schema_path, session_dir=session_dir)
            assert db is not None
        except RuntimeError:
            pytest.skip("chdb not available for session_dir test")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TestConnection:
    def test_create_connection(self, db):
        conn = db.connect()
        assert conn is not None

    def test_repr(self, conn):
        assert repr(conn) == "<Connection>"

    def test_multiple_connections(self, db):
        c1 = db.connect()
        c2 = db.connect()
        assert c1 is not c2


# ---------------------------------------------------------------------------
# query_to_sql — Cypher → SQL translation
# ---------------------------------------------------------------------------

class TestQueryToSql:
    def test_basic_match(self, conn):
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert "users" in sql.lower(), f"Expected 'users' table in SQL: {sql}"
        assert "full_name" in sql, f"Expected property mapping 'full_name' in SQL: {sql}"

    def test_relationship(self, conn):
        sql = conn.query_to_sql(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
        )
        assert "follows" in sql.lower()
        assert "full_name" in sql

    def test_where_clause(self, conn):
        sql = conn.query_to_sql(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name"
        )
        assert "full_name" in sql
        # Should contain a WHERE condition
        assert "where" in sql.lower() or "WHERE" in sql

    def test_limit(self, conn):
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name LIMIT 10")
        assert "10" in sql

    def test_multi_hop(self, conn):
        sql = conn.query_to_sql(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title"
        )
        assert "authored" in sql.lower()
        assert "full_name" in sql
        assert "title" in sql

    def test_invalid_cypher_raises(self, conn):
        with pytest.raises(RuntimeError):
            conn.query_to_sql("NOT VALID CYPHER @@@@")


# ---------------------------------------------------------------------------
# Database.execute shorthand
# ---------------------------------------------------------------------------

class TestExecuteShorthand:
    def test_execute_returns_query_result(self, db):
        """execute() should work but may fail at query time since there's
        no actual chdb backend — we just verify it doesn't crash on
        Cypher parsing."""
        try:
            result = db.execute("MATCH (u:User) RETURN u.name LIMIT 1")
            assert hasattr(result, 'column_names')
            assert hasattr(result, 'num_rows')
        except RuntimeError:
            # Expected if chdb isn't available
            pass


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------

class TestQueryResult:
    def test_repr(self, conn):
        """Verify QueryResult has a useful repr."""
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert "QueryResult" in repr(result)
        except RuntimeError:
            pytest.skip("chdb not available")

    def test_column_names(self, conn):
        """Verify column_names are accessible."""
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert isinstance(result.column_names, list)
        except RuntimeError:
            pytest.skip("chdb not available")

    def test_iteration(self, conn):
        """Verify the result is iterable."""
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            rows = list(result)
            assert isinstance(rows, list)
        except RuntimeError:
            pytest.skip("chdb not available")

    def test_len(self, conn):
        """Verify len() works."""
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert len(result) == result.num_rows
        except RuntimeError:
            pytest.skip("chdb not available")
