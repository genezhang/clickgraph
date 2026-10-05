"""
#1175: relationship uniqueness holds across a fixed hop and an adjacent CTE-backed
variable-length path of ONE MATCH.

The path's recursive CTE only knew its own edges, so `(c)-[:R]->(a)-[:R*1..2]->(b)` let the
path walk back over the edge the hop had just used (e.g. 1->2 then 2->1 then 1->2). The
oracle below is brute force over the fixture's own edge list: every edge of the pattern must
be distinct, and the fixture has 2-cycles (1<->2, 1<->3 ...), so a missing guard over-counts.
"""

import collections

import pytest
from conftest import execute_cypher, assert_query_success

SCHEMA = "social_integration"


def _edges():
    response = execute_cypher(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b",
        schema_name=SCHEMA,
    )
    assert_query_success(response)
    return [(i, r["a"], r["b"]) for i, r in enumerate(response["results"])]


def _count(edges, pattern):
    """Pattern segments: ('h', '>'|'<') one hop, ('p', lo, hi) forward path."""
    out, inn = collections.defaultdict(list), collections.defaultdict(list)
    for e in edges:
        out[e[1]].append(e)
        inn[e[2]].append(e)
    nodes = {n for e in edges for n in e[1:]}
    total = 0

    def rec(i, node, used):
        nonlocal total
        if i == len(pattern):
            total += 1
            return
        seg = pattern[i]
        if seg[0] == "h":
            for e in out[node] if seg[1] == ">" else inn[node]:
                if e[0] not in used:
                    rec(i + 1, e[2] if seg[1] == ">" else e[1], used | {e[0]})
        else:
            def walk(n, u, k):
                if k >= seg[1]:
                    rec(i + 1, n, u)
                if k < seg[2]:
                    for e in out[n]:
                        if e[0] not in u:
                            walk(e[2], u | {e[0]}, k + 1)

            walk(node, used, 0)

    for n in nodes:
        rec(0, n, frozenset())
    return total


CASES = [
    ("hop then path",
     "MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(*) AS n",
     [("h", ">"), ("p", 1, 2)]),
    ("hop then longer path",
     "MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..3]->(b:User) RETURN count(*) AS n",
     [("h", ">"), ("p", 1, 3)]),
    ("path then hop",
     "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS]->(d:User) RETURN count(*) AS n",
     [("p", 1, 2), ("h", ">")]),
    ("hop, path, hop",
     "MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS]->(d:User) "
     "RETURN count(*) AS n",
     [("h", ">"), ("p", 1, 2), ("h", ">")]),
    ("two hops then path",
     "MATCH (c:User)-[:FOLLOWS]->(m:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
     "RETURN count(*) AS n",
     [("h", ">"), ("h", ">"), ("p", 1, 2)]),
    ("incoming hop before the path",
     "MATCH (c:User)<-[:FOLLOWS]-(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(*) AS n",
     [("h", "<"), ("p", 1, 2)]),
    ("comma pattern in one MATCH",
     "MATCH (c:User)-[:FOLLOWS]->(a:User), (a)-[:FOLLOWS*1..2]->(b:User) RETURN count(*) AS n",
     [("h", ">"), ("p", 1, 2)]),
]


@pytest.mark.parametrize("name, cypher, pattern", CASES, ids=[c[0] for c in CASES])
def test_hop_and_path_never_share_a_relationship_1175(name, cypher, pattern):
    expected = _count(_edges(), pattern)
    response = execute_cypher(cypher, schema_name=SCHEMA)
    assert_query_success(response)
    assert response["results"] == [{"n": expected}], name


def test_separate_match_clauses_may_reuse_the_edge_1175():
    """Uniqueness is per MATCH clause (#586): a second MATCH re-matches freely, so the
    guard must NOT be applied across clauses."""
    edges = _edges()
    # hop and path are independent: |hops| x |paths|-starting-at-the-hop's-end, edges reusable.
    out = collections.defaultdict(list)
    for e in edges:
        out[e[1]].append(e)
    expected = 0
    for e1 in edges:
        for e2 in out[e1[2]]:
            expected += 1
            expected += sum(1 for e3 in out[e2[2]] if e3[0] != e2[0])
    response = execute_cypher(
        "MATCH (c:User)-[:FOLLOWS]->(a:User) MATCH (a)-[:FOLLOWS*1..2]->(b:User) "
        "RETURN count(*) AS n",
        schema_name=SCHEMA,
    )
    assert_query_success(response)
    assert response["results"] == [{"n": expected}]
