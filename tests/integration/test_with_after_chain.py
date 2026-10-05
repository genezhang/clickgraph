"""
#1177: a `WITH` AFTER a fixed hop chained in front of a path.

    MATCH (c)-[:R]->(a)-[:R*1..2]->(b) [WHERE c.x = ..] WITH count(*) AS k RETURN k

The WITH body's join sort failed (`t1 needs ["t"]`: the VLP FROM alias was not available) and the
scope silently lost EVERY join, so the count was the path alone (26 where 45 is correct) and a
predicate on `c` referenced an unjoined alias. Oracle: brute force over the fixture; the hop's
edge may not be one of the path's edges (#1175), the path is edge-unique inside itself.
"""

from collections import Counter

import pytest
from test_chained_vlp_trailing_hop import EDGES, NODES, _OUT, schemas  # noqa: F401
from conftest import execute_cypher


def _rows():
    """(c, a, b) for `(c)-[]->(a)-[*1..2]->(b)`."""
    rows = []
    for i1, (c, a) in enumerate(EDGES):
        def walk(n, used, depth):
            if depth >= 1:
                rows.append((c, a, n))
            if depth < 2:
                for i, t in _OUT[n]:
                    if i != i1 and i not in used:
                        walk(t, used | {i}, depth + 1)

        walk(a, frozenset(), 0)
    return rows


def _scalar(schema, query):
    result = execute_cypher(query, schema_name=schema)
    assert "results" in result, f"query failed: {result}"
    assert len(result["results"]) == 1, result
    return int(next(iter(result["results"][0].values())))


CHAIN = "MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)"
FILTERS = [("", None), ("WHERE c.user_id = 2", 0), ("WHERE a.user_id = 2", 1),
           ("WHERE b.user_id = 2", 2)]


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("where, idx", FILTERS)
def test_with_count_after_a_filtered_chain(schemas, which, where, idx):  # noqa: F811
    expected = len([r for r in _rows() if idx is None or r[idx] == 2])
    assert _scalar(schemas[which], f"{CHAIN} {where} WITH count(*) AS k RETURN k") == expected


@pytest.mark.parametrize("which", ["std"])
@pytest.mark.parametrize("where, idx", FILTERS)
def test_with_projection_after_a_filtered_chain(schemas, which, where, idx):  # noqa: F811
    expected = len([r for r in _rows() if idx is None or r[idx] == 2])
    for tail in ("WITH b.user_id AS n RETURN count(n)",
                 "WITH a, b RETURN count(*)",
                 "WITH collect(b.user_id) AS l RETURN size(l)"):
        assert _scalar(schemas[which], f"{CHAIN} {where} {tail}") == expected, tail


@pytest.mark.parametrize("which", ["std"])
def test_with_grouped_by_the_chain_start(schemas, which):  # noqa: F811
    result = execute_cypher(
        f"{CHAIN} WITH c.user_id AS n, count(*) AS k RETURN n, k ORDER BY n",
        schema_name=schemas[which])
    assert "results" in result, result
    got = Counter({int(r["n"]): int(r["k"]) for r in result["results"]})
    assert got == Counter(r[0] for r in _rows())
