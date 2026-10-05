"""
#1199: a selective predicate on an INNER-joined node promotes the edge table to
FROM, and the edge's own restrictions (a polymorphic edge's type/label
discriminator) must survive the promotion.

Needs the `polymorphic` schema (schemas/dev/social_polymorphic.yaml) over
`db_polymorphic.interactions` (scripts/setup/setup_polymorphic_data.sh). The
FOLLOWS (User -> User) rows are:

    1->2 1->3 2->1 2->3 3->1 3->2 4->1 4->5 5->1 5->2

and the table also holds other interaction types ending at user 3 (a SHARED,
a COMMENTED and an AUTHORED row to posts with id 3), which the unrestricted
query counted.
"""

import pytest
from conftest import execute_cypher, assert_query_success

SCHEMA = "polymorphic"


@pytest.fixture(autouse=True)
def _polymorphic_fixture_present():
    response = execute_cypher(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(*) AS n", schema_name=SCHEMA
    )
    if "results" not in response or response["results"][0]["n"] != 10:
        pytest.skip("db_polymorphic fixture not loaded")


def _rows(cypher):
    response = execute_cypher(cypher, schema_name=SCHEMA)
    assert_query_success(response)
    return response["results"]


def test_single_hop_end_filter_counts_only_follows_1199():
    # Into user 3 by FOLLOWS: 1->3 and 2->3.
    assert _rows(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE b.user_id = 3 RETURN count(*) AS n"
    ) == [{"n": 2}]


def test_single_hop_end_filter_returns_only_follows_sources_1199():
    rows = _rows(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE b.user_id = 3 "
        "RETURN a.user_id AS id ORDER BY id"
    )
    assert [r["id"] for r in rows] == [1, 2]


def test_fixed_length_end_filter_keeps_both_hops_restricted_1199():
    # x -> m -> 3 over FOLLOWS: m=1 has 4 in-edges (2,3,4,5), m=2 has 3 (1,3,5).
    assert _rows(
        "MATCH (a:User)-[:FOLLOWS*2..2]->(b:User) WHERE b.user_id = 3 "
        "RETURN count(*) AS n"
    ) == [{"n": 7}]


def test_mid_chain_filter_keeps_every_hop_restricted_1199():
    # Same paths, filtered on the middle node.
    assert _rows(
        "MATCH (a:User)-[:FOLLOWS]->(m:User)-[:FOLLOWS]->(b:User) "
        "WHERE m.user_id = 3 RETURN count(*) AS n"
    ) == [{"n": 4}]
