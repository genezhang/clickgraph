"""
#1236: `OPTIONAL MATCH (n0)-[:R]->(n1) WHERE <pred on n1> RETURN count(*)`. The optional edge+node
pair is folded into ONE `LEFT JOIN (SELECT … WHERE <pred>)` subquery (#479), keyed on the edge's
anchor column — 0..N matches per anchor. With nothing projecting `n1` the join was removed as
"unreferenced" / "bridge", so each anchor was counted once and the predicate vanished.

Python oracle: rows per anchor = max(1, number of qualifying out/in/any neighbours).
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _model():
    edges = [(int(r["a"]), int(r["b"])) for r in _rows(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")]
    users = {int(r["a"]): r["age"] for r in _rows(
        "MATCH (a:User) RETURN a.user_id AS a, a.age AS age")}
    return edges, users


ARROWS = {"o": "-[:FOLLOWS]->", "i": "<-[:FOLLOWS]-", "u": "-[:FOLLOWS]-"}


def _neighbours(edges, d):
    out = collections.defaultdict(list)
    for a, b in edges:
        if d in "ou":
            out[a].append(b)
        if d in "iu":
            out[b].append(a)
    return out


@pytest.mark.parametrize("d", ["o", "i", "u"])
@pytest.mark.parametrize("threshold", [30, 40])
def test_count_over_optional_with_a_where_on_the_optional_node(d, threshold):
    edges, users = _model()
    nb = _neighbours(edges, d)
    want = sum(max(1, sum(1 for m in nb[u] if users[m] is not None and users[m] > threshold))
               for u in users)
    got = int(_rows(f"MATCH (n0:User) OPTIONAL MATCH (n0){ARROWS[d]}(n1:User) "
                    f"WHERE n1.age > {threshold} RETURN count(*) AS n")[0]["n"])
    assert got == want
    # the projected form must agree (it always did)
    rows = _rows(f"MATCH (n0:User) OPTIONAL MATCH (n0){ARROWS[d]}(n1:User) "
                 f"WHERE n1.age > {threshold} RETURN n0.user_id AS a, n1.user_id AS b")
    assert len(rows) == want
