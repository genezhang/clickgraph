"""
A fixed hop AFTER a variable-length path, with the path nested on the hop's right (#1192).

    MATCH (a)-[:R*1..2]->(b)<-[:R]-(c)      -- incoming hop after the path
    MATCH (a)<-[:R*1..2]-(b)<-[:R]-(c)      -- path written backwards, hop after it

The join builder expanded the nested path as if it were a hop and emitted its edge table a
second time (`JOIN edge AS t1 ON t1.followed_id = t.end_id`, tied to nothing else) — and a
node-table join that mentions its own alias nowhere — so the rows were multiplied.

Reuses the cyclic fixtures and the brute-force enumeration of test_with_hop_before_vlp.py.
The engine keeps relationship-uniqueness inside a path, not between a hop and a path (#1175).
"""

from collections import Counter, defaultdict

import pytest
from test_with_hop_before_vlp import EDGES, _OUT, _got, _paths, schemas  # noqa: F401

_IN = defaultdict(list)
for _i, (_f, _t) in enumerate(EDGES):
    _IN[_t].append((_i, _f))
NODES = sorted({x for e in EDGES for x in e})


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("lo,hi", [(1, 2), (2, 3)])
def test_forward_path_then_incoming_hop(schemas, which, lo, hi):  # noqa: F811
    query = (f"MATCH (a:User)-[:FOLLOWS*{lo}..{hi}]->(b:User)<-[:FOLLOWS]-(c:User) "
             f"RETURN a.user_id AS col0, b.user_id AS col1, c.user_id AS col2")
    expected = Counter()
    for a in NODES:
        for b in _paths(a, lo, hi):
            for _i, c in _IN[b]:
                expected[(a, b, c)] += 1
    assert _got(schemas[which], query, 3) == expected


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("lo,hi", [(1, 2), (2, 3)])
def test_backwards_path_then_incoming_hop(schemas, which, lo, hi):  # noqa: F811
    query = (f"MATCH (a:User)<-[:FOLLOWS*{lo}..{hi}]-(b:User)<-[:FOLLOWS]-(c:User) "
             f"RETURN a.user_id AS col0, b.user_id AS col1, c.user_id AS col2")
    expected = Counter()
    for b in NODES:  # the path runs b -> ... -> a
        for a in _paths(b, lo, hi):
            for _i, c in _IN[b]:
                expected[(a, b, c)] += 1
    assert _got(schemas[which], query, 3) == expected


@pytest.mark.parametrize("which", ["std", "den"])
def test_backwards_path_then_two_incoming_hops(schemas, which):  # noqa: F811
    query = ("MATCH (a:User)<-[:FOLLOWS*1..2]-(b:User)<-[:FOLLOWS]-(c:User)<-[:FOLLOWS]-(d:User) "
             "RETURN a.user_id AS col0, b.user_id AS col1, c.user_id AS col2, d.user_id AS col3")
    expected = Counter()
    for b in NODES:
        for a in _paths(b, 1, 2):
            for i, c in _IN[b]:
                for j, d in _IN[c]:
                    if i != j:  # the two fixed hops are pairwise relationship-unique
                        expected[(a, b, c, d)] += 1
    assert _got(schemas[which], query, 4) == expected
