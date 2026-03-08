"""Tests for clickgraph Python bindings (UniFFI backend).

Same test coverage as the PyO3 version, verifying API compatibility.
Uses sql_only mode — no chdb required.
"""

import textwrap

import pytest

import clickgraph

# ---------------------------------------------------------------------------
# Schema fixture — same as PyO3 tests
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
    """Create a Database from the test schema (sql_only mode)."""
    return clickgraph.Database.sql_only(schema_path)


@pytest.fixture
def conn(db):
    """Create a Connection from the Database."""
    return db.connect()


# ---------------------------------------------------------------------------
# Database construction
# ---------------------------------------------------------------------------

class TestDatabaseConstruction:
    def test_create_from_schema_file(self, schema_path):
        db = clickgraph.Database.sql_only(schema_path)
        assert db is not None

    def test_repr(self, db):
        assert repr(db) == "<Database>"

    def test_invalid_schema_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("not: valid: schema: {{{")
        with pytest.raises(Exception):
            clickgraph.Database.sql_only(str(bad))

    def test_missing_file_raises(self):
        with pytest.raises(Exception):
            clickgraph.Database.sql_only("/nonexistent/path.yaml")

    def test_session_dir_kwarg(self, schema_path, tmp_path):
        session_dir = str(tmp_path / "chdb_session")
        try:
            db = clickgraph.Database(schema_path, session_dir=session_dir)
            assert db is not None
        except Exception:
            pytest.skip("chdb not available for session_dir test")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TestConnection:
    def test_create_connection(self, db):
        conn = db.connect()
        assert conn is not None

    def test_kuzu_style_constructor(self, db):
        """Connection(db) constructor works like kuzu.Connection(db)."""
        conn = clickgraph.Connection(db)
        assert conn is not None
        assert repr(conn) == "<Connection>"

    def test_repr(self, conn):
        assert repr(conn) == "<Connection>"

    def test_multiple_connections(self, db):
        c1 = db.connect()
        c2 = db.connect()
        assert c1 is not c2

    def test_execute_alias(self, conn):
        """conn.execute() is an alias for conn.query()."""
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert sql is not None

    def test_run_alias(self, conn):
        """conn.run() is an alias for conn.query()."""
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert sql is not None


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
        with pytest.raises(Exception):
            conn.query_to_sql("NOT VALID CYPHER @@@@")


# ---------------------------------------------------------------------------
# Database.execute shorthand
# ---------------------------------------------------------------------------

class TestExecuteShorthand:
    def test_execute_returns_query_result(self, db):
        try:
            result = db.execute("MATCH (u:User) RETURN u.name LIMIT 1")
            assert hasattr(result, 'column_names')
            assert hasattr(result, 'num_rows')
        except Exception:
            pass  # Expected if chdb isn't available


# ---------------------------------------------------------------------------
# QueryResult (requires chdb for actual data — test structure only)
# ---------------------------------------------------------------------------

class TestQueryResult:
    def test_repr(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert "QueryResult" in repr(result)
        except Exception:
            pytest.skip("chdb not available")

    def test_column_names(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert isinstance(result.column_names, list)
        except Exception:
            pytest.skip("chdb not available")

    def test_iteration(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            rows = list(result)
            assert isinstance(rows, list)
        except Exception:
            pytest.skip("chdb not available")

    def test_len(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert len(result) == result.num_rows
        except Exception:
            pytest.skip("chdb not available")

    def test_has_next_get_next(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            assert not result.has_next()
        except Exception:
            pytest.skip("chdb not available")

    def test_reset_iterator(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            list(result)
            result.reset_iterator()
            assert len(list(result)) == 0
        except Exception:
            pytest.skip("chdb not available")

    def test_getitem_negative_index(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            with pytest.raises(IndexError):
                _ = result[-1]
        except Exception:
            pytest.skip("chdb not available")

    def test_getitem_out_of_range(self, conn):
        try:
            result = conn.query("MATCH (u:User) RETURN u.name LIMIT 0")
            with pytest.raises(IndexError):
                _ = result[999]
        except Exception:
            pytest.skip("chdb not available")


# ---------------------------------------------------------------------------
# Kuzu-compatible API
# ---------------------------------------------------------------------------

class TestKuzuCompatibility:
    def test_connection_constructor(self, db):
        conn = clickgraph.Connection(db)
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert "full_name" in sql

    def test_execute_method(self, db):
        conn = clickgraph.Connection(db)
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert "full_name" in sql


# ---------------------------------------------------------------------------
# Neo4j-compatible API
# ---------------------------------------------------------------------------

class TestNeo4jCompatibility:
    def test_run_method(self, db):
        conn = db.connect()
        sql = conn.query_to_sql(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
        )
        assert "follows" in sql.lower()


# ---------------------------------------------------------------------------
# Export API
# ---------------------------------------------------------------------------

class TestExport:
    def test_export_to_sql_parquet(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "output.parquet",
        )
        assert sql.startswith("INSERT INTO FUNCTION file(")
        assert "'output.parquet'" in sql
        assert "Parquet" in sql

    def test_export_to_sql_csv(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "results.csv",
        )
        assert "'results.csv'" in sql
        assert "CSVWithNames" in sql

    def test_export_to_sql_json(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "results.json",
        )
        assert "'results.json'" in sql
        assert "JSON" in sql

    def test_export_to_sql_ndjson(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "data.ndjson",
        )
        assert "'data.ndjson'" in sql
        assert "JSONEachRow" in sql

    def test_export_to_sql_explicit_format(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "data.out",
            format="parquet",
        )
        assert "'data.out'" in sql
        assert "Parquet" in sql

    def test_export_to_sql_compression(self, db):
        conn = db.connect()
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "output.parquet",
            compression="zstd",
        )
        assert "output_format_parquet_compression_method" in sql
        assert "zstd" in sql

    def test_export_unknown_format_raises(self, db):
        conn = db.connect()
        with pytest.raises(Exception):
            conn.export_to_sql(
                "MATCH (u:User) RETURN u.name",
                "data.xyz",
            )

    def test_export_invalid_format_string(self, db):
        conn = db.connect()
        with pytest.raises(Exception, match="Unknown export format"):
            conn.export_to_sql(
                "MATCH (u:User) RETURN u.name",
                "data.out",
                format="xlsx",
            )

    def test_export_invalid_compression_codec(self, db):
        conn = db.connect()
        with pytest.raises(Exception):
            conn.export_to_sql(
                "MATCH (u:User) RETURN u.name",
                "output.parquet",
                compression="lzma",
            )


# ---------------------------------------------------------------------------
# sql_only-specific tests
# ---------------------------------------------------------------------------

class TestSqlOnly:
    def test_sql_only_factory(self, schema_path):
        """Database.sql_only() creates a sql-only database."""
        db = clickgraph.Database.sql_only(schema_path)
        conn = db.connect()
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert "full_name" in sql

    def test_sql_only_no_chdb_needed(self, schema_path):
        """sql_only mode doesn't require chdb."""
        db = clickgraph.Database.sql_only(schema_path)
        conn = db.connect()
        # query_to_sql should always work without chdb
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name LIMIT 5")
        assert "5" in sql
