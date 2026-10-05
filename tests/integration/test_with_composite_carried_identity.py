"""
A node carried through WITH on the composite-id layout (`Account`, node_id [bank_id, account_number]),
then matched again (P-4b slice S4, docs/design/WITH_EXPORT_CONTRACT.md).

- A hop off the carried node joined on ONE column under a guessed name (`t2.from_bank_id = c.id`,
  Code 47): the node's label is gone after the barrier, so its identity fell back to `id`.
- A variable-length path from/to it compared the path's `bank|account` id with the CTE's first id
  column alone, so it never matched: 0 rows where 16 are right (#1286).

The identity now comes from the WITH export contract: the CTE columns the CTE actually emits.
The oracle enumerates trails (relationship-unique within the second MATCH) over the edge list the
engine returns for a plain single hop.
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "composite_id"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges():
    rows = _rows(
        "MATCH (z:Account)-[:TRANSFERRED]->(c:Account) "
        "RETURN z.bank_id AS zb, z.account_number AS za, c.bank_id AS cb, c.account_number AS ca"
    )
    return [((r["zb"], r["za"]), (r["cb"], r["ca"])) for r in rows]


def _trails(edges, src, lo, hi):
    """Yield (end node) for every trail of length lo..hi from src."""
    out = []

    def walk(node, used):
        if lo <= len(used) <= hi:
            out.append(node)
        if len(used) == hi:
            return
        for i, (s, d) in enumerate(edges):
            if s == node and i not in used:
                walk(d, used | {i})

    walk(src, frozenset())
    return out


def _bag(query):
    return collections.Counter((r["x"], r["y"]) for r in _rows(query))


CARRY = "MATCH (z:Account)-[:TRANSFERRED]->(c:Account) WITH {w} "


@pytest.mark.parametrize("lo, hi", [(1, 1), (1, 2), (2, 3)])
def test_path_out_of_carried_composite_node(lo, hi):
    edges = _edges()
    rng = "" if (lo, hi) == (1, 1) else f"*{lo}..{hi}"
    q = CARRY.format(w="c") + (
        f"MATCH (c)-[:TRANSFERRED{rng}]->(a:Account) RETURN c.account_number AS x, a.account_number AS y"
    )
    expected = collections.Counter()
    for _, c in edges:
        for end in _trails(edges, c, lo, hi):
            expected[(c[1], end[1])] += 1
    assert _bag(q) == expected, q


@pytest.mark.parametrize("lo, hi", [(1, 1), (1, 2)])
def test_path_into_carried_composite_node(lo, hi):
    edges = _edges()
    rng = "" if (lo, hi) == (1, 1) else f"*{lo}..{hi}"
    q = CARRY.format(w="c") + (
        f"MATCH (a:Account)-[:TRANSFERRED{rng}]->(c) RETURN a.account_number AS x, c.account_number AS y"
    )
    nodes = {n for e in edges for n in e}
    expected = collections.Counter()
    for _, c in edges:
        for a in nodes:
            n = sum(1 for end in _trails(edges, a, lo, hi) if end == c)
            if n:
                expected[(a[1], c[1])] += n
    assert _bag(q) == expected, q


@pytest.mark.parametrize("carried", ["c, z", "z, c"])
def test_path_between_two_carried_composite_nodes(carried):
    edges = _edges()
    q = CARRY.format(w=carried) + (
        "MATCH (z)-[:TRANSFERRED*1..2]->(c) RETURN z.account_number AS x, c.account_number AS y"
    )
    expected = collections.Counter()
    for z, c in edges:
        n = sum(1 for end in _trails(edges, z, 1, 2) if end == c)
        if n:
            expected[(z[1], c[1])] += n
    assert _bag(q) == expected, q


# --- a composite node that ENDS a variable-length path, then carried through WITH ------------------
# The path's `end_id` is the `bank|account` concat; the WITH projected it as `bank_id` (values
# 'CHASE|CHK-003'), so the identity was wrong and a hop off the carried node matched nothing.


def test_path_endpoint_carried_through_with_keeps_its_id_columns():
    edges = _edges()
    nodes = {n for e in edges for n in e}
    expected = collections.Counter()
    for src in nodes:
        for end in _trails(edges, src, 1, 2):
            expected[end[0]] += 1
    rows = _rows(
        "MATCH (c:Account)-[:TRANSFERRED*1..2]->(a:Account) WITH a "
        "RETURN a.bank_id AS x, count(*) AS k"
    )
    assert {r["x"]: r["k"] for r in rows} == dict(expected)


def test_hop_off_a_carried_path_endpoint():
    edges = _edges()
    nodes = {n for e in edges for n in e}
    expected = sum(
        1 for src in nodes for end in _trails(edges, src, 1, 2) for (s, _) in edges if s == end
    )
    rows = _rows(
        "MATCH (c:Account)-[:TRANSFERRED*1..2]->(a:Account) WITH a "
        "MATCH (a)-[:TRANSFERRED]->(b:Account) RETURN count(*) AS k"
    )
    assert rows[0]["k"] == expected
