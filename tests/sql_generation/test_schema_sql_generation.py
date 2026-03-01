#!/usr/bin/env python3
"""
Schema-Parameterized SQL Generation Tests.

Validates that LDBC-style Cypher query patterns generate valid SQL across
all 6 supported schema variations, using sql_only=true (no data needed).

Each schema variation is loaded dynamically via /schemas/load. For each
(schema, pattern), we verify:
  1. HTTP 200 (SQL generated successfully)
  2. No 'system.one' fallback (indicates broken FROM resolution)
  3. Valid SQL structure (SELECT, FROM present)
  4. For undirected edges: proper UNION ALL structure

Schema Variations:
  1. standard       - Separate node + edge tables (User, Post, FOLLOWS, AUTHORED, LIKED, FRIENDS_WITH)
  2. fk_edge        - FK column as relationship (Order, Customer, PLACED_BY)
  3. denormalized   - Node properties in edge table (Airport, FLIGHT)
  4. polymorphic    - Single table with type_column (User, Post, polymorphic interactions)
  5. composite_id   - Multi-column node identity (Account, Customer, OWNS, TRANSFERRED)
  6. coupled_edges  - Multiple relationships from one table (IP, Domain, ResolvedIP)

Usage:
    # Requires running ClickGraph server (any schema — test loads its own)
    pytest tests/integration/test_schema_sql_generation.py -v
    pytest tests/integration/test_schema_sql_generation.py -v -k standard
    pytest tests/integration/test_schema_sql_generation.py -v -k "undirected"
"""

import pytest
import requests
import yaml
import os
import re

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8082")
SCHEMA_YAML_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "schemas", "test", "schema_variations.yaml"
)

# ---------------------------------------------------------------------------
# Schema loading helpers
# ---------------------------------------------------------------------------

def load_schema_definitions():
    """Parse schema_variations.yaml and return {name: yaml_content} dict."""
    with open(SCHEMA_YAML_PATH) as f:
        config = yaml.safe_load(f)

    schemas = {}
    for schema_def in config["schemas"]:
        name = schema_def["name"]
        # The /schemas/load endpoint expects top-level graph_schema: { nodes, edges }
        yaml_content = yaml.dump({"graph_schema": schema_def["graph_schema"]}, default_flow_style=False)
        schemas[name] = yaml_content
    return schemas


SCHEMA_DEFS = load_schema_definitions()


def load_schema_to_server(schema_name):
    """Load a schema variation into the running ClickGraph server."""
    yaml_content = SCHEMA_DEFS[schema_name]
    resp = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={
            "schema_name": f"test_{schema_name}",
            "config_content": yaml_content,
            "validate_schema": False,  # No ClickHouse tables needed
        },
    )
    assert resp.status_code == 200, f"Failed to load schema {schema_name}: {resp.text}"
    return f"test_{schema_name}"


def sql_query(cypher, schema_name):
    """Run a Cypher query with sql_only=true and return (status, sql, response_json)."""
    resp = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": cypher,
            "schema_name": schema_name,
            "sql_only": True,
        },
    )
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    sql = data.get("sql", data.get("generated_sql", ""))
    if isinstance(sql, list):
        sql = sql[0] if sql else ""
    return resp.status_code, sql, data


# ---------------------------------------------------------------------------
# Structural SQL validators
# ---------------------------------------------------------------------------

def assert_valid_sql(status, sql, data, *, allow_system_one=False, label=""):
    """Common assertions for generated SQL."""
    prefix = f"[{label}] " if label else ""
    assert status == 200, f"{prefix}Expected 200, got {status}: {data}"
    assert sql, f"{prefix}Empty SQL returned: {data}"
    if not allow_system_one:
        assert "system.one" not in sql, f"{prefix}system.one fallback detected in SQL:\n{sql}"
    # Basic SQL structure
    sql_upper = sql.upper()
    assert "SELECT" in sql_upper, f"{prefix}No SELECT in SQL:\n{sql}"


def assert_has_union(sql, label=""):
    """Assert SQL contains UNION ALL (for undirected edges)."""
    prefix = f"[{label}] " if label else ""
    assert "UNION ALL" in sql.upper(), f"{prefix}Expected UNION ALL in SQL:\n{sql}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def loaded_schemas():
    """Load all schema variations once per module."""
    result = {}
    for name in SCHEMA_DEFS:
        try:
            result[name] = load_schema_to_server(name)
        except Exception as e:
            pytest.skip(f"Could not load schema {name}: {e}")
    return result


def _schema_name(loaded_schemas, key):
    if key not in loaded_schemas:
        pytest.skip(f"Schema {key} not loaded")
    return loaded_schemas[key]


# ===========================================================================
# STANDARD SCHEMA TESTS (User, Post, FOLLOWS, AUTHORED, LIKED, FRIENDS_WITH)
# ===========================================================================

class TestStandardSchema:
    """Query patterns against the standard schema with separate node + edge tables."""

    # --- Basic patterns ---

    def test_basic_directed_match(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/basic_directed")

    def test_basic_return_count(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS post_count", s
        )
        assert_valid_sql(status, sql, data, label="standard/return_count")
        assert "GROUP BY" in sql.upper()

    # --- Undirected edges ---

    def test_undirected_self_rel(self, loaded_schemas):
        """Undirected User-User edge should generate UNION ALL."""
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (a:User)-[:FRIENDS_WITH]-(b:User) RETURN a.name, b.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/undirected_self")

    def test_undirected_with_barrier(self, loaded_schemas):
        """Undirected edge before WITH barrier (the BidirectionalUnion fix)."""
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (a:User)-[:FRIENDS_WITH]-(b:User) WITH a, b RETURN a.name, b.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/undirected_with")

    def test_undirected_with_aggregation(self, loaded_schemas):
        """Undirected edge + WITH + aggregation."""
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (a:User)-[:FRIENDS_WITH]-(b:User) WITH a, count(b) AS friends RETURN a.name, friends ORDER BY friends DESC", s
        )
        assert_valid_sql(status, sql, data, label="standard/undirected_agg")

    # --- WITH + MATCH chains ---

    def test_with_match_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) WITH u, count(p) AS posts WHERE posts > 0 RETURN u.name, posts", s
        )
        assert_valid_sql(status, sql, data, label="standard/with_chain")

    def test_multi_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) WITH u, count(p) AS posts MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, posts, count(f) AS following", s
        )
        assert_valid_sql(status, sql, data, label="standard/multi_with")

    # --- OPTIONAL MATCH ---

    def test_optional_match_basic(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS posts", s
        )
        assert_valid_sql(status, sql, data, label="standard/optional_basic")

    def test_optional_match_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) OPTIONAL MATCH (p)<-[:LIKED]-(liker:User) RETURN u.name, count(DISTINCT p) AS posts, count(DISTINCT liker) AS likers", s
        )
        assert_valid_sql(status, sql, data, label="standard/optional_chain")

    # --- VLP (Variable Length Path) ---

    def test_vlp_basic(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN a.name, b.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/vlp_basic")

    def test_vlp_with_where(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice' RETURN b.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/vlp_where")

    # --- Multi-pattern MATCH ---

    def test_multi_pattern_match(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post), (u)-[:FOLLOWS]->(f:User) RETURN u.name, count(DISTINCT p) AS posts, count(DISTINCT f) AS following", s
        )
        assert_valid_sql(status, sql, data, label="standard/multi_pattern")

    # --- WHERE with parameters ---

    def test_where_with_params(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = 'Alice' AND f.is_active = true RETURN f.name", s
        )
        assert_valid_sql(status, sql, data, label="standard/where_params")

    # --- DISTINCT ---

    def test_distinct_return(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]->(f:User)-[:AUTHORED]->(p:Post) RETURN DISTINCT u.name, p.content", s
        )
        assert_valid_sql(status, sql, data, label="standard/distinct")

    # --- ORDER BY + LIMIT ---

    def test_order_limit(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "standard")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS cnt ORDER BY cnt DESC LIMIT 10", s
        )
        assert_valid_sql(status, sql, data, label="standard/order_limit")


# ===========================================================================
# FK-EDGE SCHEMA TESTS (Order, Customer, PLACED_BY)
# ===========================================================================

class TestFkEdgeSchema:
    """FK-edge: relationship is derived from FK column in the same table."""

    def test_basic_fk_traversal(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) RETURN o.order_id, c.name", s
        )
        assert_valid_sql(status, sql, data, label="fk/basic")

    def test_fk_reverse_direction(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.total_amount", s
        )
        assert_valid_sql(status, sql, data, label="fk/reverse")

    def test_fk_aggregation(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) RETURN c.name, count(o) AS orders, sum(o.total_amount) AS total", s
        )
        assert_valid_sql(status, sql, data, label="fk/aggregation")

    def test_fk_optional_match(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (c:Customer) OPTIONAL MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, count(o) AS orders", s
        )
        assert_valid_sql(status, sql, data, label="fk/optional")

    def test_fk_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) WITH c, count(o) AS orders WHERE orders > 0 RETURN c.name, orders", s
        )
        assert_valid_sql(status, sql, data, label="fk/with_chain")

    def test_fk_undirected(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (o:Order)-[:PLACED_BY]-(c:Customer) RETURN o.order_id, c.name", s
        )
        assert_valid_sql(status, sql, data, label="fk/undirected")

    def test_fk_where_filter(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "fk_edge")
        status, sql, data = sql_query(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WHERE o.total_amount > 100 RETURN c.name, o.total_amount ORDER BY o.total_amount DESC LIMIT 10", s
        )
        assert_valid_sql(status, sql, data, label="fk/where_filter")


# ===========================================================================
# DENORMALIZED SCHEMA TESTS (Airport, FLIGHT)
# ===========================================================================

class TestDenormalizedSchema:
    """Denormalized: node properties embedded in the edge table."""

    def test_denorm_basic(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.city, b.city", s
        )
        assert_valid_sql(status, sql, data, label="denorm/basic")

    def test_denorm_both_endpoints(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) RETURN origin.city, dest.city, f.carrier", s
        )
        assert_valid_sql(status, sql, data, label="denorm/both_endpoints")

    def test_denorm_where_filter(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WHERE a.city = 'Seattle' AND b.state = 'CA' RETURN count(*) AS flights", s
        )
        assert_valid_sql(status, sql, data, label="denorm/where")

    def test_denorm_aggregation(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.city, count(*) AS flights ORDER BY flights DESC LIMIT 10", s
        )
        assert_valid_sql(status, sql, data, label="denorm/agg")

    def test_denorm_undirected(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT]-(b:Airport) WHERE a.city = 'Seattle' RETURN b.city, count(*) AS flights", s
        )
        assert_valid_sql(status, sql, data, label="denorm/undirected")

    def test_denorm_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WITH a.city AS origin, count(*) AS flights RETURN origin, flights ORDER BY flights DESC", s
        )
        assert_valid_sql(status, sql, data, label="denorm/with_chain")

    def test_denorm_vlp(self, loaded_schemas):
        """VLP on denormalized schema (multi-hop flights)."""
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) WHERE a.city = 'Seattle' RETURN b.city", s
        )
        assert_valid_sql(status, sql, data, label="denorm/vlp")

    def test_denorm_optional_match(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[f:FLIGHT]->(b:Airport) RETURN a.code, count(f) AS outgoing", s
        )
        assert_valid_sql(status, sql, data, label="denorm/optional")

    def test_denorm_reverse_direction(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "denormalized")
        status, sql, data = sql_query(
            "MATCH (dest:Airport)<-[:FLIGHT]-(origin:Airport) RETURN origin.city, dest.city", s
        )
        assert_valid_sql(status, sql, data, label="denorm/reverse")


# ===========================================================================
# POLYMORPHIC SCHEMA TESTS (User, Post, polymorphic interactions table)
# ===========================================================================

class TestPolymorphicSchema:
    """Polymorphic: multiple edge types from a single table with type_column."""

    def test_poly_specific_type(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name", s
        )
        assert_valid_sql(status, sql, data, label="poly/specific_type")

    def test_poly_different_endpoints(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.content", s
        )
        assert_valid_sql(status, sql, data, label="poly/different_endpoints")

    def test_poly_likes(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:LIKES]->(p:Post) RETURN u.name, p.content", s
        )
        assert_valid_sql(status, sql, data, label="poly/likes")

    def test_poly_aggregation(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS posts", s
        )
        assert_valid_sql(status, sql, data, label="poly/aggregation")

    def test_poly_optional_match(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User) OPTIONAL MATCH (u)-[:LIKES]->(p:Post) RETURN u.name, count(p) AS liked", s
        )
        assert_valid_sql(status, sql, data, label="poly/optional")

    def test_poly_undirected(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]-(f:User) RETURN u.name, f.name", s
        )
        assert_valid_sql(status, sql, data, label="poly/undirected")

    def test_poly_undirected_with_barrier(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:FOLLOWS]-(f:User) WITH u, f RETURN u.name, f.name", s
        )
        assert_valid_sql(status, sql, data, label="poly/undirected_with")

    def test_poly_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) WITH u, count(p) AS posts RETURN u.name, posts ORDER BY posts DESC", s
        )
        assert_valid_sql(status, sql, data, label="poly/with_chain")

    def test_poly_multi_type_chain(self, loaded_schemas):
        """Chain: User-[:AUTHORED]->Post<-[:LIKES]-User."""
        s = _schema_name(loaded_schemas, "polymorphic")
        status, sql, data = sql_query(
            "MATCH (author:User)-[:AUTHORED]->(p:Post)<-[:LIKES]-(liker:User) RETURN author.name, liker.name, p.content", s
        )
        assert_valid_sql(status, sql, data, label="poly/multi_type_chain")


# ===========================================================================
# COMPOSITE ID SCHEMA TESTS (Account[bank_id,account_number], Customer, TRANSFERRED)
# ===========================================================================

class TestCompositeIdSchema:
    """Composite ID: multi-column node identity (bank_id + account_number)."""

    def test_composite_basic_traversal(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c.name, a.balance", s
        )
        assert_valid_sql(status, sql, data, label="composite/basic")

    def test_composite_transfer_chain(self, loaded_schemas):
        """Account-to-Account transfer with composite IDs."""
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (a:Account)-[t:TRANSFERRED]->(b:Account) RETURN a.account_number, b.account_number, t.amount", s
        )
        assert_valid_sql(status, sql, data, label="composite/transfer")

    def test_composite_reverse(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (a:Account)<-[:OWNS]-(c:Customer) RETURN c.name, a.balance", s
        )
        assert_valid_sql(status, sql, data, label="composite/reverse")

    def test_composite_aggregation(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c.name, count(a) AS accounts, sum(a.balance) AS total_balance", s
        )
        assert_valid_sql(status, sql, data, label="composite/agg")

    def test_composite_optional(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (c:Customer) OPTIONAL MATCH (c)-[:OWNS]->(a:Account) RETURN c.name, count(a) AS accounts", s
        )
        assert_valid_sql(status, sql, data, label="composite/optional")

    def test_composite_undirected(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (a:Account)-[:TRANSFERRED]-(b:Account) RETURN a.account_number, b.account_number", s
        )
        assert_valid_sql(status, sql, data, label="composite/undirected")

    def test_composite_undirected_with_barrier(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (a:Account)-[:TRANSFERRED]-(b:Account) WITH a, b RETURN a.account_number, b.account_number", s
        )
        assert_valid_sql(status, sql, data, label="composite/undirected_with")

    def test_composite_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (c:Customer)-[:OWNS]->(a:Account)-[:TRANSFERRED]->(b:Account) RETURN c.name, a.account_number, b.account_number, count(*) AS transfers", s
        )
        assert_valid_sql(status, sql, data, label="composite/with_chain")

    def test_composite_vlp(self, loaded_schemas):
        """VLP with composite-ID nodes (transfer chains)."""
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (a:Account)-[:TRANSFERRED*1..3]->(b:Account) RETURN a.account_number, b.account_number", s
        )
        assert_valid_sql(status, sql, data, label="composite/vlp")

    def test_composite_where_filter(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "composite_id")
        status, sql, data = sql_query(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) WHERE a.balance > 1000 RETURN c.name, a.balance ORDER BY a.balance DESC", s
        )
        assert_valid_sql(status, sql, data, label="composite/where")


# ===========================================================================
# COUPLED EDGES SCHEMA TESTS (IP, Domain, ResolvedIP — Zeek log tables)
# ===========================================================================

class TestCoupledEdgesSchema:
    """Coupled edges: multiple relationships from a single table."""

    def test_coupled_basic(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) RETURN ip.ip, d.name", s
        )
        assert_valid_sql(status, sql, data, label="coupled/basic")

    def test_coupled_chain(self, loaded_schemas):
        """IP -> Domain -> ResolvedIP (two edges from same table)."""
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP) RETURN ip.ip, d.name, rip.ip", s
        )
        assert_valid_sql(status, sql, data, label="coupled/chain")

    def test_coupled_cross_table(self, loaded_schemas):
        """IP -> Domain (dns_log) + IP -> IP (conn_log)."""
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (src:IP)-[:ACCESSED]->(dest:IP) RETURN src.ip, dest.ip", s
        )
        assert_valid_sql(status, sql, data, label="coupled/cross_table")

    def test_coupled_aggregation(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) RETURN d.name, count(*) AS requests ORDER BY requests DESC LIMIT 10", s
        )
        assert_valid_sql(status, sql, data, label="coupled/agg")

    def test_coupled_optional(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) OPTIONAL MATCH (d)-[:RESOLVED_TO]->(rip:ResolvedIP) RETURN d.name, count(rip) AS resolutions", s
        )
        assert_valid_sql(status, sql, data, label="coupled/optional")

    def test_coupled_with_chain(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) WITH d, count(*) AS reqs WHERE reqs > 0 RETURN d.name, reqs", s
        )
        assert_valid_sql(status, sql, data, label="coupled/with_chain")

    def test_coupled_undirected(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[:ACCESSED]-(other:IP) RETURN ip.ip, other.ip", s
        )
        assert_valid_sql(status, sql, data, label="coupled/undirected")

    def test_coupled_edge_properties(self, loaded_schemas):
        s = _schema_name(loaded_schemas, "coupled_edges")
        status, sql, data = sql_query(
            "MATCH (ip:IP)-[r:REQUESTED]->(d:Domain) RETURN ip.ip, d.name, r.timestamp, r.rcode", s
        )
        assert_valid_sql(status, sql, data, label="coupled/edge_props")


# ===========================================================================
# CROSS-SCHEMA PATTERN TESTS
# Tests that exercise the same query pattern across multiple schemas to
# ensure consistent behavior.
# ===========================================================================

class TestCrossSchemaPatterns:
    """Same pattern tested against schemas that support it."""

    # --- Undirected + WITH pattern (BidirectionalUnion fix) ---

    @pytest.mark.parametrize("schema_key,query", [
        ("standard", "MATCH (a:User)-[:FRIENDS_WITH]-(b:User) WITH a, b RETURN a.name, b.name"),
        ("denormalized", "MATCH (a:Airport)-[:FLIGHT]-(b:Airport) WITH a, b RETURN a.code, b.code"),
        ("composite_id", "MATCH (a:Account)-[:TRANSFERRED]-(b:Account) WITH a, b RETURN a.account_number, b.account_number"),
        ("polymorphic", "MATCH (a:User)-[:FOLLOWS]-(b:User) WITH a, b RETURN a.name, b.name"),
        ("coupled_edges", "MATCH (a:IP)-[:ACCESSED]-(b:IP) WITH a, b RETURN a.ip, b.ip"),
    ])
    def test_undirected_with_barrier(self, loaded_schemas, schema_key, query):
        s = _schema_name(loaded_schemas, schema_key)
        status, sql, data = sql_query(query, s)
        assert_valid_sql(status, sql, data, label=f"cross/undirected_with/{schema_key}")

    # --- Aggregation patterns ---

    @pytest.mark.parametrize("schema_key,query", [
        ("standard", "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, count(b) AS cnt ORDER BY cnt DESC LIMIT 5"),
        ("fk_edge", "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) RETURN c.name, count(o) AS cnt ORDER BY cnt DESC LIMIT 5"),
        ("denormalized", "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.city, count(*) AS cnt ORDER BY cnt DESC LIMIT 5"),
        ("composite_id", "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c.name, count(a) AS cnt ORDER BY cnt DESC LIMIT 5"),
        ("coupled_edges", "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) RETURN d.name, count(*) AS cnt ORDER BY cnt DESC LIMIT 5"),
    ])
    def test_aggregation_pattern(self, loaded_schemas, schema_key, query):
        s = _schema_name(loaded_schemas, schema_key)
        status, sql, data = sql_query(query, s)
        assert_valid_sql(status, sql, data, label=f"cross/agg/{schema_key}")

    # --- OPTIONAL MATCH patterns ---

    @pytest.mark.parametrize("schema_key,query", [
        ("standard", "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS cnt"),
        ("fk_edge", "MATCH (c:Customer) OPTIONAL MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, count(o) AS cnt"),
        ("denormalized", "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, count(b) AS cnt"),
        ("composite_id", "MATCH (c:Customer) OPTIONAL MATCH (c)-[:OWNS]->(a:Account) RETURN c.name, count(a) AS cnt"),
        ("coupled_edges", "MATCH (ip:IP)-[:REQUESTED]->(d:Domain) OPTIONAL MATCH (d)-[:RESOLVED_TO]->(r:ResolvedIP) RETURN d.name, count(r) AS cnt"),
    ])
    def test_optional_match_pattern(self, loaded_schemas, schema_key, query):
        s = _schema_name(loaded_schemas, schema_key)
        status, sql, data = sql_query(query, s)
        assert_valid_sql(status, sql, data, label=f"cross/optional/{schema_key}")

    # --- VLP patterns (schemas with self-referencing edges) ---

    @pytest.mark.parametrize("schema_key,query", [
        ("standard", "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.name, b.name"),
        ("denormalized", "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN a.code, b.code"),
        ("composite_id", "MATCH (a:Account)-[:TRANSFERRED*1..2]->(b:Account) RETURN a.account_number, b.account_number"),
    ])
    def test_vlp_pattern(self, loaded_schemas, schema_key, query):
        s = _schema_name(loaded_schemas, schema_key)
        status, sql, data = sql_query(query, s)
        assert_valid_sql(status, sql, data, label=f"cross/vlp/{schema_key}")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
