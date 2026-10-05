"""
Several WITH scopes in front of a path: only the LAST WITH's CTE joins the final query (#1188).

An earlier WITH's CTE was consumed by the next scope's CTE body and its aliases are out of
scope; the final query used to join it again as `JOIN <cte> ON 1 = 1`, a cross join that
multiplied the rows (1512 instead of 168).
"""

from collections import Counter

import pytest
from test_with_hop_before_vlp import EDGES, _OUT, _got, _paths, schemas  # noqa: F401

# Standard schema only: the denormalized WITH CTE is joined on a guessed column (#1189).


@pytest.mark.parametrize("lo,hi", [(1, 2), (2, 3)])
def test_two_with_scopes_then_hop_and_path(schemas, lo, hi):  # noqa: F811
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c MATCH (c)-[:FOLLOWS]->(a:User) WITH a "
             f"MATCH (a)-[:FOLLOWS]->(b:User)-[:FOLLOWS*{lo}..{hi}]->(d:User) "
             "RETURN a.user_id AS col0, d.user_id AS col1")
    expected = Counter()
    for _z, c in EDGES:
        for _i, a in _OUT[c]:
            for j, b in _OUT[a]:  # the last MATCH: hop and path are relationship-unique
                for d in _paths(b, lo, hi, {j}):
                    expected[(a, d)] += 1
    assert _got(schemas["std"], query, 2) == expected


def test_three_with_scopes(schemas):  # noqa: F811
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c MATCH (c)-[:FOLLOWS]->(a:User) WITH a "
             "MATCH (a)-[:FOLLOWS]->(b:User) WITH b "
             "MATCH (b)-[:FOLLOWS]->(d:User)-[:FOLLOWS*1..2]->(e:User) "
             "RETURN b.user_id AS col0, e.user_id AS col1")
    expected = Counter()
    for _z, c in EDGES:
        for _i, a in _OUT[c]:
            for _j, b in _OUT[a]:
                for k, d in _OUT[b]:
                    for e in _paths(d, 1, 2, {k}):
                        expected[(b, e)] += 1
    assert _got(schemas["std"], query, 2) == expected


def test_two_carried_nodes_from_different_scopes(schemas):  # noqa: F811
    """`WITH c, a` re-exports `c`: the last CTE carries both, nothing else is joined."""
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c MATCH (c)-[:FOLLOWS]->(a:User) "
             "WITH c, a MATCH (a)-[:FOLLOWS*1..2]->(b:User) "
             "RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2")
    expected = Counter()
    for _z, c in EDGES:
        for _i, a in _OUT[c]:
            for b in _paths(a, 1, 2):
                expected[(c, a, b)] += 1
    assert _got(schemas["std"], query, 3) == expected
