"""
Fixed hops chained after a `WITH` are pairwise relationship-unique (#1187).

Within one MATCH two hops must not traverse the same relationship.  Without a WITH the
engine emits the predicate; after a WITH the chain is rendered from the WITH CTE and the
predicate was silently dropped, so a self-loop edge could be walked twice
(`(c)-[:R]->(m)-[:R]->(a)` with c = m = a over ONE loop edge).

The fixture of test_with_hop_before_vlp.py has a cycle, a branch and a self-loop (6, 6).
"""

import itertools
from collections import Counter

import pytest
from test_with_hop_before_vlp import EDGES, _got, schemas  # noqa: F401

# Direction strings the engine renders correctly after a WITH on BOTH schemas (the other
# mixes hit separate, older defects).
CHAINS = ["<>", "<>>", ">>", ">>>", ">><"]


def _expected(dirs):
    rows = Counter()
    for _z, c in EDGES:  # one seed row per `(z)-[:R]->(c)` edge
        def rec(i, cur, used, path):
            if i == len(dirs):
                rows[tuple(path)] += 1
                return
            for k, (f, t) in enumerate(EDGES):
                if k in used:
                    continue
                if dirs[i] == ">" and f == cur:
                    rec(i + 1, t, used | {k}, path + [t])
                if dirs[i] == "<" and t == cur:
                    rec(i + 1, f, used | {k}, path + [f])
        rec(0, c, frozenset(), [c])
    return rows


def _query(dirs):
    pattern = "(c)"
    names = ["c"]
    for i, d in enumerate(dirs, 1):
        pattern += (f"-[:FOLLOWS]->(n{i}:User)" if d == ">" else f"<-[:FOLLOWS]-(n{i}:User)")
        names.append(f"n{i}")
    ret = ", ".join(f"{n}.user_id AS col{j}" for j, n in enumerate(names))
    return (f"MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c MATCH {pattern} RETURN {ret}",
            len(names))


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("dirs", CHAINS)
def test_hops_after_with_do_not_reuse_a_relationship(schemas, which, dirs):  # noqa: F811
    query, width = _query(dirs)
    assert _got(schemas[which], query, width) == _expected(dirs)


def test_the_fixture_makes_the_predicate_matter():
    """The loop edge (6, 6) is what a missing predicate would walk twice."""
    for dirs in CHAINS:
        with_pred = sum(_expected(dirs).values())
        assert with_pred > 0
    # without the predicate `>>` over c = 6 would also count the loop twice
    assert (6, 6, 6) not in _expected(">>")


# The same rule for the hops BEFORE the WITH (they are the WITH CTE's body).
INNER = ["><", ">>", "<<", "<>"]


def _expected_inner(dirs):
    rows = Counter()
    starts = sorted({x for e in EDGES for x in e})
    for s in starts:
        def rec(i, cur, used):
            if i == len(dirs):
                rows[(cur,)] += 1
                return
            for k, (f, t) in enumerate(EDGES):
                if k in used:
                    continue
                if dirs[i] == ">" and f == cur:
                    rec(i + 1, t, used | {k})
                if dirs[i] == "<" and t == cur:
                    rec(i + 1, f, used | {k})
        rec(0, s, frozenset())
    return rows


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("dirs", INNER)
def test_hops_before_with_do_not_reuse_a_relationship(schemas, which, dirs):  # noqa: F811
    pattern = "(a0:User)"
    for i, d in enumerate(dirs, 1):
        last = "c" if i == len(dirs) else f"a{i}"
        pattern += (f"-[:FOLLOWS]->({last}:User)" if d == ">" else f"<-[:FOLLOWS]-({last}:User)")
    query = f"MATCH {pattern} WITH c RETURN c.user_id AS col0"
    assert _got(schemas[which], query, 1) == _expected_inner(dirs)


# #1195: a hop nested on the right of the first one (`(c)->(n1)<-(n2)`) lost the first hop's
# tie to the WITH CTE (`ON t2.followed_id = t2.followed_id`, a cross join).  Standard schema
# only: on the denormalized one these mixes are still wrong (a separate, older defect).
RIGHT_NESTED = ["><", "><>", "><<", "><><", "><>>"]


@pytest.mark.parametrize("dirs", RIGHT_NESTED)
def test_incoming_hop_after_the_first_hop_is_tied_to_the_with_cte(schemas, dirs):  # noqa: F811
    query, width = _query(dirs)
    assert _got(schemas["std"], query, width) == _expected(dirs)
