"""End-to-end tests with real chdb execution over Parquet data.

These tests create Parquet files (via PyArrow) in a temp directory, build a
schema that references them, open a real chdb-backed Database, execute Cypher
queries through the full Python → UniFFI → Rust → chdb pipeline, and verify
actual results.

This exercises the realistic use case: querying graph relationships stored
in columnar Parquet files without a running ClickHouse server.

Gating: These tests are skipped by default. Set CLICKGRAPH_CHDB_TESTS=1
to run them.
"""

import os
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import clickgraph

# ---------------------------------------------------------------------------
# Gate: skip all tests in this module unless CLICKGRAPH_CHDB_TESTS is set
# ---------------------------------------------------------------------------

_CHDB_ENABLED = os.environ.get("CLICKGRAPH_CHDB_TESTS", "").lower() in ("1", "true")

pytestmark = pytest.mark.skipif(
    not _CHDB_ENABLED,
    reason="set CLICKGRAPH_CHDB_TESTS=1 to run chdb e2e tests",
)


# ---------------------------------------------------------------------------
# Fixture: Parquet data + schema YAML in a temp directory
# ---------------------------------------------------------------------------

def _write_users_parquet(path: str):
    """Write users node data as Parquet."""
    table = pa.table({
        "user_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "full_name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"]),
        "age": pa.array([30, 25, 35, 28, 32], type=pa.int64()),
        "country": pa.array(["US", "UK", "CA", "US", "DE"]),
    })
    pq.write_table(table, path)


def _write_follows_parquet(path: str):
    """Write follows edge data as Parquet."""
    table = pa.table({
        "follower_id": pa.array([1, 1, 2, 3, 4, 4, 5], type=pa.int64()),
        "followed_id": pa.array([2, 3, 3, 1, 1, 2, 1], type=pa.int64()),
        "follow_date": pa.array([
            "2024-01-15", "2024-02-20", "2024-03-10", "2024-04-05",
            "2024-05-12", "2024-06-01", "2024-07-20",
        ]),
    })
    pq.write_table(table, path)


def _write_posts_parquet(path: str):
    """Write posts node data as Parquet."""
    table = pa.table({
        "post_id": pa.array([101, 102, 103, 104, 105], type=pa.int64()),
        "title": pa.array([
            "Hello World", "Python Tips", "Graph Databases",
            "Travel Notes", "Cooking 101",
        ]),
        "content": pa.array([
            "First post by Alice", "Bob shares Python tips",
            "Charlie on graph DBs", "Alice's travel blog",
            "Diana's recipes",
        ]),
        "created_date": pa.array([
            "2024-01-01", "2024-02-15", "2024-03-20",
            "2024-04-10", "2024-05-05",
        ]),
    })
    pq.write_table(table, path)


def _write_authored_parquet(path: str):
    """Write authored edge data as Parquet."""
    table = pa.table({
        "user_id": pa.array([1, 2, 3, 1, 4], type=pa.int64()),
        "post_id": pa.array([101, 102, 103, 104, 105], type=pa.int64()),
    })
    pq.write_table(table, path)


# Schema uses direct Parquet paths (auto-detected by source_resolver).
SCHEMA_TEMPLATE = textwrap.dedent("""\
    name: chdb_e2e
    graph_schema:
      nodes:
        - label: User
          database: default
          table: users
          node_id: user_id
          source: "{users_pq}"
          property_mappings:
            user_id: user_id
            name: full_name
            age: age
            country: country
        - label: Post
          database: default
          table: posts
          node_id: post_id
          source: "{posts_pq}"
          property_mappings:
            post_id: post_id
            title: title
            content: content
            created_date: created_date
      edges:
        - type: FOLLOWS
          database: default
          table: follows
          from_node: User
          to_node: User
          from_id: follower_id
          to_id: followed_id
          source: "{follows_pq}"
          property_mappings:
            follow_date: follow_date
        - type: AUTHORED
          database: default
          table: authored
          from_node: User
          to_node: Post
          from_id: user_id
          to_id: post_id
          source: "{authored_pq}"
          property_mappings: {{}}
""")


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory):
    """Write Parquet files and schema YAML to a temp directory."""
    d = tmp_path_factory.mktemp("chdb_e2e")

    users_path = str(d / "users.parquet")
    follows_path = str(d / "follows.parquet")
    posts_path = str(d / "posts.parquet")
    authored_path = str(d / "authored.parquet")

    _write_users_parquet(users_path)
    _write_follows_parquet(follows_path)
    _write_posts_parquet(posts_path)
    _write_authored_parquet(authored_path)

    schema_yaml = SCHEMA_TEMPLATE.format(
        users_pq=users_path,
        follows_pq=follows_path,
        posts_pq=posts_path,
        authored_pq=authored_path,
    )
    schema_path = d / "schema.yaml"
    schema_path.write_text(schema_yaml)

    return d


@pytest.fixture(scope="module")
def db(data_dir):
    """Open a real chdb-backed Database."""
    schema_path = str(data_dir / "schema.yaml")
    return clickgraph.Database(schema_path)


@pytest.fixture(scope="module")
def conn(db):
    """Create a Connection from the Database."""
    return db.connect()


# ===========================================================================
# Node scan tests
# ===========================================================================

class TestNodeScan:
    def test_basic_scan_all_users(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name ORDER BY u.name")
        assert result.num_rows == 5
        names = [row["u.name"] for row in result]
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def test_scan_all_posts(self, conn):
        result = conn.query("MATCH (p:Post) RETURN p.title ORDER BY p.title")
        assert result.num_rows == 5
        titles = [row["p.title"] for row in result]
        assert titles == [
            "Cooking 101",
            "Graph Databases",
            "Hello World",
            "Python Tips",
            "Travel Notes",
        ]


# ===========================================================================
# WHERE clause filtering
# ===========================================================================

class TestWhereFilter:
    def test_greater_than(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.age > 30 RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Charlie", "Eve"]

    def test_less_than(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.age < 30 RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Bob", "Diana"]

    def test_equals_string(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.country = 'US' RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Alice", "Diana"]

    def test_not_equals(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.country <> 'US' RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Bob", "Charlie", "Eve"]

    def test_equals_integer(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 3 RETURN u.name"
        )
        assert result.num_rows == 1
        assert result[0]["u.name"] == "Charlie"

    def test_and_condition(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.country = 'US' AND u.age > 29 "
            "RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Alice"]

    def test_or_condition(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.country = 'UK' OR u.country = 'DE' "
            "RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Bob", "Eve"]

    def test_in_list(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id IN [1, 3, 5] "
            "RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Alice", "Charlie", "Eve"]

    def test_starts_with(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.name STARTS WITH 'A' RETURN u.name"
        )
        assert result.num_rows == 1
        assert result[0]["u.name"] == "Alice"

    def test_contains_string(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.name CONTAINS 'li' RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Alice", "Charlie"]

    def test_ends_with(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.name ENDS WITH 'e' RETURN u.name ORDER BY u.name"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Alice", "Charlie", "Eve"]


# ===========================================================================
# Aggregations
# ===========================================================================

class TestAggregation:
    def test_count(self, conn):
        result = conn.query("MATCH (u:User) RETURN count(u) AS cnt")
        assert result.num_rows == 1
        assert result[0]["cnt"] == 5

    def test_count_by_country(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.country AS country, count(u) AS cnt "
            "ORDER BY cnt DESC, country"
        )
        assert result.num_rows == 4
        first = result[0]
        assert first["country"] == "US"
        assert first["cnt"] == 2

    def test_sum(self, conn):
        result = conn.query("MATCH (u:User) RETURN sum(u.age) AS total_age")
        assert result.num_rows == 1
        assert result[0]["total_age"] == 150  # 30+25+35+28+32

    def test_avg(self, conn):
        result = conn.query("MATCH (u:User) RETURN avg(u.age) AS avg_age")
        assert result.num_rows == 1
        assert result[0]["avg_age"] == 30.0  # 150/5

    def test_min_max(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN min(u.age) AS youngest, max(u.age) AS oldest"
        )
        assert result.num_rows == 1
        assert result[0]["youngest"] == 25
        assert result[0]["oldest"] == 35

    def test_count_posts_per_author(self, conn):
        result = conn.query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) "
            "RETURN u.name, count(p) AS post_count "
            "ORDER BY post_count DESC, u.name"
        )
        # Alice: 2 posts (101, 104), Bob: 1, Charlie: 1, Diana: 1
        assert result.num_rows == 4
        assert result[0]["u.name"] == "Alice"
        assert result[0]["post_count"] == 2


# ===========================================================================
# ORDER BY and LIMIT / SKIP
# ===========================================================================

class TestOrderByLimit:
    def test_order_by_desc_limit(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 3"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Charlie", "Eve", "Alice"]

    def test_limit_1(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 1"
        )
        assert result.num_rows == 1
        assert result[0]["u.name"] == "Alice"

    def test_limit_with_order_asc(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.age ASC LIMIT 2"
        )
        names = [row["u.name"] for row in result]
        assert names == ["Bob", "Diana"]  # age 25, 28


# ===========================================================================
# DISTINCT
# ===========================================================================

class TestDistinct:
    def test_distinct_countries(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN DISTINCT u.country ORDER BY u.country"
        )
        countries = [row["u.country"] for row in result]
        assert countries == ["CA", "DE", "UK", "US"]

    def test_distinct_count(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN count(DISTINCT u.country) AS num_countries"
        )
        assert result[0]["num_countries"] == 4


# ===========================================================================
# Relationship traversal
# ===========================================================================

class TestRelationshipTraversal:
    def test_who_does_alice_follow(self, conn):
        """Alice (user_id=1) follows Bob (2) and Charlie (3)."""
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            "WHERE a.user_id = 1 "
            "RETURN b.name ORDER BY b.name"
        )
        names = [row["b.name"] for row in result]
        assert names == ["Bob", "Charlie"]

    def test_who_follows_alice(self, conn):
        """Alice is followed by Charlie (3), Diana (4), Eve (5)."""
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            "WHERE b.user_id = 1 "
            "RETURN a.name ORDER BY a.name"
        )
        names = [row["a.name"] for row in result]
        assert names == ["Charlie", "Diana", "Eve"]

    def test_follower_count(self, conn):
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            "WHERE b.user_id = 1 "
            "RETURN b.name, count(a) AS follower_count"
        )
        assert result.num_rows == 1
        assert result[0]["b.name"] == "Alice"
        assert result[0]["follower_count"] == 3

    def test_all_follow_pairs(self, conn):
        """Total follow edges: 7."""
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            "RETURN count(*) AS total"
        )
        assert result[0]["total"] == 7

    def test_authored_posts(self, conn):
        """Alice authored 'Hello World' and 'Travel Notes'."""
        result = conn.query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) "
            "WHERE u.user_id = 1 "
            "RETURN p.title ORDER BY p.title"
        )
        titles = [row["p.title"] for row in result]
        assert titles == ["Hello World", "Travel Notes"]

    def test_post_author(self, conn):
        """'Graph Databases' was authored by Charlie."""
        result = conn.query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) "
            "WHERE p.post_id = 103 "
            "RETURN u.name"
        )
        assert result.num_rows == 1
        assert result[0]["u.name"] == "Charlie"


# ===========================================================================
# Multiple properties / aliasing
# ===========================================================================

class TestMultipleProperties:
    def test_return_all_properties(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 1 "
            "RETURN u.name, u.age, u.country"
        )
        assert result.num_rows == 1
        row = result[0]
        assert row["u.name"] == "Alice"
        assert row["u.age"] == 30
        assert row["u.country"] == "US"

    def test_alias(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 2 "
            "RETURN u.name AS person_name, u.age AS person_age"
        )
        assert result.num_rows == 1
        row = result[0]
        assert row["person_name"] == "Bob"
        assert row["person_age"] == 25


# ===========================================================================
# WITH clause (chaining)
# ===========================================================================

class TestWithClause:
    def test_with_filter(self, conn):
        result = conn.query(
            "MATCH (u:User) "
            "WITH u.name AS name, u.age AS age "
            "WHERE age >= 30 "
            "RETURN name ORDER BY name"
        )
        names = [row["name"] for row in result]
        assert names == ["Alice", "Charlie", "Eve"]

    def test_with_aggregation(self, conn):
        result = conn.query(
            "MATCH (u:User) "
            "WITH u.country AS country, count(u) AS cnt "
            "WHERE cnt >= 2 "
            "RETURN country, cnt"
        )
        assert result.num_rows == 1
        assert result[0]["country"] == "US"
        assert result[0]["cnt"] == 2


# ===========================================================================
# QueryResult API tests (with real data)
# ===========================================================================

class TestQueryResultAPI:
    def test_column_names(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name, u.age ORDER BY u.user_id LIMIT 1"
        )
        assert result.column_names == ["u.name", "u.age"]

    def test_num_rows(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name")
        assert result.num_rows == 5
        assert len(result) == 5

    def test_iteration(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id"
        )
        names = [row["u.name"] for row in result]
        assert len(names) == 5
        assert names[0] == "Alice"

    def test_getitem_positive(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id"
        )
        assert result[0]["u.name"] == "Alice"
        assert result[4]["u.name"] == "Eve"

    def test_getitem_negative(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id"
        )
        assert result[-1]["u.name"] == "Eve"
        assert result[-5]["u.name"] == "Alice"

    def test_getitem_out_of_range(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name LIMIT 1")
        with pytest.raises(IndexError):
            _ = result[999]

    def test_as_dicts(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 2"
        )
        dicts = result.as_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 2
        assert dicts[0]["u.name"] == "Alice"
        assert dicts[1]["u.name"] == "Bob"

    def test_get_row(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id"
        )
        row = result.get_row(2)
        assert row["u.name"] == "Charlie"

    def test_get_row_out_of_range(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name LIMIT 1")
        with pytest.raises(RuntimeError):
            result.get_row(100)

    def test_repr(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name")
        r = repr(result)
        assert "QueryResult" in r
        assert "rows=5" in r

    def test_kuzu_cursor_has_next_get_next(self, conn):
        """Kuzu-compatible cursor interface: has_next() / get_next()."""
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 3"
        )
        values = []
        while result.has_next():
            row = result.get_next()  # returns list
            values.append(row[0])
        assert values == ["Alice", "Bob", "Charlie"]

    def test_get_next_as_dict(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 1"
        )
        row = result.get_next(as_dict=True)
        assert row["u.name"] == "Alice"

    def test_get_next_past_end(self, conn):
        result = conn.query("MATCH (u:User) RETURN u.name LIMIT 1")
        result.get_next()
        with pytest.raises(RuntimeError):
            result.get_next()

    def test_reset_iterator(self, conn):
        result = conn.query(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 2"
        )
        first_pass = [row["u.name"] for row in result]
        result.reset_iterator()
        second_pass = [row["u.name"] for row in result]
        assert first_pass == second_pass == ["Alice", "Bob"]


# ===========================================================================
# Connection API
# ===========================================================================

class TestConnectionAPI:
    def test_kuzu_constructor(self, db):
        """Connection(db) works like kuzu.Connection(db)."""
        conn = clickgraph.Connection(db)
        result = conn.query("MATCH (u:User) RETURN u.name LIMIT 1")
        assert result.num_rows == 1

    def test_execute_alias(self, conn):
        """execute() is an alias for query()."""
        result = conn.execute(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 1"
        )
        assert result[0]["u.name"] == "Alice"

    def test_run_alias(self, conn):
        """run() is an alias for query() (Neo4j driver compat)."""
        result = conn.run(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 1"
        )
        assert result[0]["u.name"] == "Alice"

    def test_query_to_sql(self, conn):
        """query_to_sql returns SQL without executing."""
        sql = conn.query_to_sql("MATCH (u:User) RETURN u.name")
        assert "full_name" in sql
        assert isinstance(sql, str)

    def test_multiple_connections(self, db):
        c1 = db.connect()
        c2 = db.connect()
        r1 = c1.query("MATCH (u:User) RETURN count(u) AS cnt")
        r2 = c2.query("MATCH (u:User) RETURN count(u) AS cnt")
        assert r1[0]["cnt"] == r2[0]["cnt"] == 5


# ===========================================================================
# Database.execute shorthand
# ===========================================================================

class TestDatabaseExecute:
    def test_direct_execute(self, db):
        result = db.execute(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 1"
        )
        assert result[0]["u.name"] == "Alice"


# ===========================================================================
# Data types
# ===========================================================================

class TestDataTypes:
    def test_string_value(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name"
        )
        name = result[0]["u.name"]
        assert isinstance(name, str)
        assert name == "Alice"

    def test_integer_value(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u.age"
        )
        age = result[0]["u.age"]
        assert isinstance(age, int)
        assert age == 30

    def test_integer_id(self, conn):
        result = conn.query(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u.user_id"
        )
        uid = result[0]["u.user_id"]
        assert isinstance(uid, int)
        assert uid == 1

    def test_float_from_avg(self, conn):
        result = conn.query("MATCH (u:User) RETURN avg(u.age) AS avg_age")
        avg_age = result[0]["avg_age"]
        assert isinstance(avg_age, (int, float))
        assert abs(avg_age - 30.0) < 0.01

    def test_count_returns_integer(self, conn):
        result = conn.query("MATCH (u:User) RETURN count(u) AS cnt")
        cnt = result[0]["cnt"]
        assert isinstance(cnt, int)


# ===========================================================================
# Export (real file execution)
# ===========================================================================

class TestExport:
    def test_export_parquet(self, conn, data_dir):
        out_path = str(data_dir / "export_users.parquet")
        conn.export(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            out_path,
        )
        assert os.path.exists(out_path)
        assert os.path.getsize(out_path) > 0

    def test_export_csv(self, conn, data_dir):
        out_path = str(data_dir / "export_users.csv")
        conn.export(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            out_path,
            format="csv",
        )
        assert os.path.exists(out_path)
        content = open(out_path).read()
        assert "Alice" in content
        assert "Bob" in content
        assert "Charlie" in content

    def test_export_json(self, conn, data_dir):
        out_path = str(data_dir / "export_users.json")
        conn.export(
            "MATCH (u:User) RETURN u.name, u.age ORDER BY u.user_id LIMIT 2",
            out_path,
            format="json",
        )
        assert os.path.exists(out_path)
        content = open(out_path).read()
        assert "Alice" in content

    def test_export_ndjson(self, conn, data_dir):
        out_path = str(data_dir / "export_users.ndjson")
        conn.export(
            "MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 3",
            out_path,
            format="ndjson",
        )
        assert os.path.exists(out_path)
        lines = [line for line in open(out_path).readlines() if line.strip()]
        assert len(lines) >= 3

    def test_export_parquet_compressed(self, conn, data_dir):
        out_path = str(data_dir / "export_users_zstd.parquet")
        conn.export(
            "MATCH (u:User) RETURN u.name, u.age",
            out_path,
            compression="zstd",
        )
        assert os.path.exists(out_path)
        assert os.path.getsize(out_path) > 0

    def test_export_to_sql_returns_string(self, conn):
        """export_to_sql returns SQL without executing."""
        sql = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "/tmp/test.parquet",
        )
        assert isinstance(sql, str)
        assert "Parquet" in sql
        assert "/tmp/test.parquet" in sql

    def test_export_relationship_results(self, conn, data_dir):
        """Export relationship traversal results."""
        out_path = str(data_dir / "export_follows.parquet")
        conn.export(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            "RETURN a.name AS follower, b.name AS followed "
            "ORDER BY follower, followed",
            out_path,
        )
        assert os.path.exists(out_path)
        assert os.path.getsize(out_path) > 0


# ===========================================================================
# Error handling
# ===========================================================================

class TestErrors:
    def test_invalid_cypher(self, conn):
        with pytest.raises(Exception):
            conn.query("NOT VALID CYPHER @@@@")

    def test_invalid_label(self, conn):
        """Querying an unknown label should raise an error."""
        with pytest.raises(Exception):
            conn.query("MATCH (x:NonexistentLabel) RETURN x")

    def test_invalid_relationship(self, conn):
        """Querying an unknown relationship type should raise."""
        with pytest.raises(Exception):
            conn.query(
                "MATCH (a:User)-[:NONEXISTENT]->(b:User) RETURN a.name"
            )


# ===========================================================================
# Complex queries
# ===========================================================================

class TestComplexQueries:
    def test_mutual_follows(self, conn):
        """Find mutual follow pairs (a->b AND b->a)."""
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User), "
            "(b)-[:FOLLOWS]->(a) "
            "WHERE a.user_id < b.user_id "
            "RETURN a.name, b.name ORDER BY a.name"
        )
        # From the data: Alice(1)->Charlie(3) and Charlie(3)->Alice(1)
        assert result.num_rows >= 1
        found = False
        for row in result:
            if row["a.name"] == "Alice" and row["b.name"] == "Charlie":
                found = True
        assert found, "Expected mutual follow: Alice <-> Charlie"

    def test_followers_who_alice_also_follows(self, conn):
        """People who follow Alice AND Alice also follows them back."""
        # Alice follows: Bob(2), Charlie(3)
        # Followers of Alice: Charlie(3), Diana(4), Eve(5)
        # Mutual: Charlie
        result = conn.query(
            "MATCH (alice:User)-[:FOLLOWS]->(friend:User), "
            "(friend)-[:FOLLOWS]->(alice) "
            "WHERE alice.user_id = 1 "
            "RETURN friend.name ORDER BY friend.name"
        )
        names = [row["friend.name"] for row in result]
        assert names == ["Charlie"]

    def test_multi_hop_user_to_post(self, conn):
        """Users that Alice follows who have posts."""
        result = conn.query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:AUTHORED]->(p:Post) "
            "WHERE a.user_id = 1 "
            "RETURN b.name, p.title ORDER BY b.name, p.title"
        )
        # Alice follows Bob(2) and Charlie(3)
        # Bob authored 'Python Tips', Charlie authored 'Graph Databases'
        assert result.num_rows == 2
        rows = result.as_dicts()
        assert rows[0]["b.name"] == "Bob"
        assert rows[0]["p.title"] == "Python Tips"
        assert rows[1]["b.name"] == "Charlie"
        assert rows[1]["p.title"] == "Graph Databases"
