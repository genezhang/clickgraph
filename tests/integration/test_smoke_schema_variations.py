"""
Runtime smoke tests for the FK-edge, polymorphic, composite-ID, and
denormalized schema variations (P0.7, docs/design/REFACTORING_SAFETY_PLAN.md
§3.3).

Unlike tests/integration/test_schema_variations.py (sql_only mode — asserts
on generated-SQL *strings*), these tests EXECUTE against live ClickHouse data
and assert on actual returned values. Per the #439 lesson referenced in the
safety plan: string-level SQL assertions cannot catch CTE-scoping/JOIN
regressions that only manifest at execution time — only an executed query
does.

Data is loaded by (see .github/workflows/ci.yml "Setup test data" step):
  scripts/setup/setup_fk_edge_data.sh      -> db_fk_edge
  scripts/setup/setup_polymorphic_data.sh  -> db_polymorphic
  scripts/setup/setup_composite_id_data.sh -> db_composite_id
  scripts/setup/setup_denormalized_data.sh -> db_denormalized

We load our OWN copy of each schema under a `smoke_*`-prefixed name (module
fixture below), the same self-contained pattern
test_graphrag_schema_variations.py uses, rather than reusing the bare
"fk_edge"/"polymorphic"/"composite_id" names: conftest.py's autouse
`load_all_test_schemas` fixture registers its OWN (different!) definitions
under those exact names (e.g. "fk_edge" -> schemas/examples/orders_customers_fk.yaml,
pointing at test_integration.orders_fk, not db_fk_edge; "polymorphic" ->
schemas/examples/social_polymorphic.yaml, pointing at brahmand.interactions,
not db_polymorphic). Depending on fixture/import order to win that race would
be fragile; loading under our own name sidesteps it entirely.
"""

import os

import pytest
import requests

from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_contains_value,
    get_single_value,
    get_column_values,
)

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:7475")
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

pytestmark = pytest.mark.smoke

# (smoke schema name, source YAML) -- source files already target the exact
# db_fk_edge/db_polymorphic/db_composite_id/db_denormalized tables the CI
# setup scripts populate (see module docstring).
_SMOKE_SCHEMAS = [
    ("smoke_fk_edge", "schemas/test/fk_edge.yaml"),
    ("smoke_polymorphic", "schemas/dev/social_polymorphic.yaml"),
    ("smoke_composite_id", "schemas/examples/composite_node_id_test.yaml"),
    ("smoke_denormalized", "schemas/dev/flights_denormalized.yaml"),
]


@pytest.fixture(scope="module", autouse=True)
def load_smoke_variation_schemas():
    """Load each schema-variation YAML under a collision-free `smoke_*` name."""
    for schema_name, yaml_path in _SMOKE_SCHEMAS:
        full_path = os.path.join(_PROJECT_ROOT, yaml_path)
        with open(full_path, "r") as f:
            yaml_content = f.read()
        response = requests.post(
            f"{CLICKGRAPH_URL}/schemas/load",
            json={"schema_name": schema_name, "config_content": yaml_content},
            timeout=10,
        )
        assert response.status_code == 200, (
            f"Failed to load schema '{schema_name}' from {yaml_path}: {response.text}"
        )
    return True


class TestFKEdgeSmoke:
    """FK-edge pattern: Order-[:PLACED_BY]->Customer (FK column as relationship,
    no separate edge table). Data: scripts/setup/setup_fk_edge_data.sh."""

    def test_single_hop_join(self):
        """Customer 100 (Alice) placed orders 1, 2, 7."""
        response = execute_cypher(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) "
            "WHERE c.customer_id = 100 "
            "RETURN o.order_id ORDER BY o.order_id",
            schema_name="smoke_fk_edge",
        )
        assert_query_success(response)
        assert_row_count(response, 3)
        ids = get_column_values(response, "o.order_id", convert_to_int=True)
        assert sorted(ids) == [1, 2, 7]

    def test_reverse_direction_aggregation(self):
        """Customer 101 (Bob) placed exactly 2 orders (3, 4)."""
        response = execute_cypher(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) "
            "WHERE c.customer_id = 101 "
            "RETURN count(o) as order_count",
            schema_name="smoke_fk_edge",
        )
        assert_query_success(response)
        assert get_single_value(response, "order_count", convert_to_int=True) == 2

    def test_where_on_joined_customer_property(self):
        """Carol (customer 102) placed orders 5, 8 — WHERE filters across the
        FK-edge join, not just the anchor table."""
        response = execute_cypher(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) "
            "WHERE c.name = 'Carol' "
            "RETURN o.order_id ORDER BY o.order_id",
            schema_name="smoke_fk_edge",
        )
        assert_query_success(response)
        assert_row_count(response, 2)
        ids = get_column_values(response, "o.order_id", convert_to_int=True)
        assert sorted(ids) == [5, 8]


class TestPolymorphicSmoke:
    """Polymorphic pattern: single `interactions` table with a type_column
    discriminator serving FOLLOWS/LIKES/AUTHORED/COMMENTED/SHARED.
    Data: scripts/setup/setup_polymorphic_data.sh."""

    def test_single_type_filter(self):
        """User 1 (Alice) FOLLOWS exactly 2 users (2, 3)."""
        response = execute_cypher(
            "MATCH (u:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id ORDER BY neighbor.user_id",
            schema_name="smoke_polymorphic",
        )
        assert_query_success(response)
        assert_row_count(response, 2)
        ids = get_column_values(response, "neighbor.user_id", convert_to_int=True)
        assert sorted(ids) == [2, 3]

    def test_different_type_same_table_disjoint(self):
        """User 1 LIKES exactly 2 posts (1, 2) — must not include FOLLOWS rows
        from the same physical table."""
        response = execute_cypher(
            "MATCH (u:User)-[:LIKES]->(p:Post) "
            "WHERE u.user_id = 1 "
            "RETURN p.post_id ORDER BY p.post_id",
            schema_name="smoke_polymorphic",
        )
        assert_query_success(response)
        assert_row_count(response, 2)
        ids = get_column_values(response, "p.post_id", convert_to_int=True)
        assert sorted(ids) == [1, 2]

    def test_multi_type_union(self):
        """[:FOLLOWS|LIKES] from user 1 must union both branches: 2 FOLLOWS
        + 2 LIKES = 4 rows total."""
        response = execute_cypher(
            "MATCH (u:User)-[:FOLLOWS|LIKES]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN count(*) as total",
            schema_name="smoke_polymorphic",
        )
        assert_query_success(response)
        assert get_single_value(response, "total", convert_to_int=True) == 4


class TestCompositeIdSmoke:
    """Composite-ID pattern: Account keyed by (bank_id, account_number).
    Data: scripts/setup/setup_composite_id_data.sh."""

    def test_owns_join_on_composite_key(self):
        """Customer 1 owns exactly 4 accounts (3 primary + 1 joint), all
        components of the composite key must be carried through the JOIN."""
        response = execute_cypher(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) "
            "WHERE c.customer_id = 1 "
            "RETURN a.bank_id, a.account_number",
            schema_name="smoke_composite_id",
        )
        assert_query_success(response)
        assert_row_count(response, 4)
        pairs = {
            (row.get("a.bank_id"), row.get("a.account_number"))
            for row in response["results"]
        }
        assert pairs == {
            ("CHASE", "CHK-001"),
            ("CHASE", "SAV-002"),
            ("WELLS", "WF-1001"),
            ("CHASE", "CHK-003"),
        }

    def test_transferred_composite_to_composite_join(self):
        """TRANSFERRED joins Account->Account on BOTH sides of the composite
        key — CHASE/CHK-001 is the source of exactly 3 transfers."""
        response = execute_cypher(
            "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) "
            "WHERE a1.bank_id = 'CHASE' AND a1.account_number = 'CHK-001' "
            "RETURN count(*) as total",
            schema_name="smoke_composite_id",
        )
        assert_query_success(response)
        assert get_single_value(response, "total", convert_to_int=True) == 3

    def test_group_by_composite_node(self):
        """Regression guard for #457 (GROUP BY on a bare composite-id node
        variable must key on ALL id columns, not just the first)."""
        response = execute_cypher(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) "
            "WHERE c.customer_id = 1 "
            "RETURN a, count(*) as cnt",
            schema_name="smoke_composite_id",
        )
        assert_query_success(response)
        # 4 distinct (bank_id, account_number) accounts -> 4 groups, each cnt=1
        assert_row_count(response, 4)


class TestDenormalizedFlightsSmoke:
    """Denormalized pattern: Airport node properties embedded directly in the
    `flights_denorm` edge table (no separate Airport table).
    Data: scripts/setup/setup_denormalized_data.sh."""

    def test_from_position_properties(self):
        """origin.city for a JFK-origin flight resolves to the FROM-position
        denormalized column (origin_city), not the TO-position one."""
        response = execute_cypher(
            "MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN dest.code, origin.city",
            schema_name="smoke_denormalized",
        )
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_contains_value(response, "dest.code", "LAX")
        assert_contains_value(response, "origin.city", "New York")

    def test_to_position_properties_multiple_inbound(self):
        """Two flights land at LAX (from ATL and from JFK) — TO-position
        properties (dest.state) must resolve per row, not collapse the union."""
        response = execute_cypher(
            "MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport) "
            "WHERE dest.code = 'LAX' "
            "RETURN origin.code ORDER BY origin.code",
            schema_name="smoke_denormalized",
        )
        assert_query_success(response)
        assert_row_count(response, 2)
        codes = get_column_values(response, "origin.code")
        assert sorted(codes) == ["ATL", "JFK"]

    def test_reverse_direction_swaps_positions(self):
        """Reversing the pattern direction must swap which side reads
        origin_* vs dest_* columns, not just swap JOIN order."""
        response = execute_cypher(
            "MATCH (dest:Airport)<-[:FLIGHT]-(origin:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN dest.code",
            schema_name="smoke_denormalized",
        )
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_contains_value(response, "dest.code", "LAX")
