"""
A fixed hop after `WITH`, chained in front of a variable-length path (#1182).

    MATCH (z)-[:R]->(c) WITH c MATCH (c)-[:R]->(a)-[:R*1..2]->(b) RETURN c, a, b

The hop `(c)-[:R]->(a)` must be tied to the WITH-carried `c` AND to the path's start.
It used to lose both (`FROM <path> AS t JOIN <with cte> AS c ON 1 = 1`), cross-joining
the WITH CTE onto the path: 234 rows instead of 91.

Compared against a brute-force enumeration over the fixture.  The engine keeps
relationship-uniqueness inside the path and between the fixed hops of one MATCH, but
NOT between a fixed hop and a path (#1175), so the oracle applies uniqueness inside the
path only.  The fixture has a cycle, a branch, parallel routes and a self-loop (an
acyclic graph cannot reuse an edge and would agree with a missing predicate by
coincidence).
"""

from collections import Counter, defaultdict

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

# (follower, followed)
EDGES = [(1, 2), (2, 3), (3, 1), (2, 4), (4, 5), (1, 5), (5, 6), (1, 3), (1, 4), (6, 6),
         (4, 8), (3, 5)]

_STANDARD = """
name: with_hop_vlp_std_1182
version: "1.0"
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: whv_users_1182
      node_id: user_id
      property_mappings: {user_id: user_id}
  edges:
    - type: FOLLOWS
      database: test_integration
      table: whv_follows_1182
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
"""

# Fully denormalized: `Airport` has no table of its own; its id is a column of the edge.
_DENORM = """
name: with_hop_vlp_den_1182
version: "1.0"
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: whv_flights_1182
      node_id: user_id
      property_mappings: {}
      from_node_properties: {user_id: src}
      to_node_properties: {user_id: dst}
  edges:
    - type: FOLLOWS
      database: test_integration
      table: whv_flights_1182
      from_node: User
      to_node: User
      from_id: src
      to_id: dst
      edge_id: eid
      property_mappings: {}
"""


@pytest.fixture(scope="module")
def schemas(clickhouse_client):
    c = clickhouse_client
    tables = ("whv_users_1182", "whv_follows_1182", "whv_flights_1182")
    for t in tables:
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.whv_users_1182 (user_id UInt32) "
              "ENGINE = MergeTree ORDER BY user_id")
    c.command("CREATE TABLE test_integration.whv_follows_1182 "
              "(follower_id UInt32, followed_id UInt32) ENGINE = MergeTree "
              "ORDER BY (follower_id, followed_id)")
    c.command("CREATE TABLE test_integration.whv_flights_1182 "
              "(eid UInt32, src UInt32, dst UInt32) ENGINE = MergeTree ORDER BY eid")
    c.insert("test_integration.whv_users_1182", [[n] for n in range(1, 9)],
             column_names=["user_id"])
    c.insert("test_integration.whv_follows_1182", [list(e) for e in EDGES],
             column_names=["follower_id", "followed_id"])
    c.insert("test_integration.whv_flights_1182",
             [[i, f, t] for i, (f, t) in enumerate(EDGES)],
             column_names=["eid", "src", "dst"])
    names = {}
    for key, body, name in (("std", _STANDARD, "with_hop_vlp_std_1182"),
                            ("den", _DENORM, "with_hop_vlp_den_1182")):
        response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                                 json={"schema_name": name, "config_content": body})
        assert response.status_code == 200, f"schema load failed: {response.text}"
        names[key] = name
    yield names
    for t in tables:
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")


_OUT = defaultdict(list)
for _i, (_f, _t) in enumerate(EDGES):
    _OUT[_f].append((_i, _t))


def _paths(start, lo, hi):
    """End nodes of every edge-unique path of lo..hi hops from `start` (one per path)."""
    ends = []

    def rec(node, used, depth):
        if depth >= lo:
            ends.append(node)
        if depth == hi:
            return
        for i, t in _OUT[node]:
            if i not in used:
                rec(t, used | {i}, depth + 1)

    rec(start, frozenset(), 0)
    return ends


def _rows(lo, hi, carried_z=False, distinct_c=False):
    rows = Counter()
    firsts = [(z, c) for z, c in EDGES]  # one row per `(z)-[:R]->(c)` edge
    if distinct_c:
        firsts = [(None, c) for c in sorted({c for _, c in EDGES})]
    for z, c in firsts:
        for _, a in _OUT[c]:
            for b in _paths(a, lo, hi):
                rows[(z, c, a, b) if carried_z else (c, a, b)] += 1
    return rows


def _got(schema, query, width):
    result = execute_cypher(query, schema_name=schema)
    assert "results" in result, f"query failed: {result}"
    return Counter(tuple(int(r[f"col{i}"]) for i in range(width)) for r in result["results"])


@pytest.mark.parametrize("which", ["std", "den"])
@pytest.mark.parametrize("lo,hi", [(1, 2), (2, 2), (1, 3)])
def test_with_carried_hop_before_vlp(schemas, which, lo, hi):
    query = (f"MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             f"MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*{lo}..{hi}]->(b:User) "
             f"RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2")
    assert _got(schemas[which], query, 3) == _rows(lo, hi)


@pytest.mark.parametrize("which", ["std", "den"])
def test_two_carried_nodes(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c, z "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "RETURN z.user_id AS col0, c.user_id AS col1, a.user_id AS col2, "
             "b.user_id AS col3")
    assert _got(schemas[which], query, 4) == _rows(1, 2, carried_z=True)


@pytest.mark.parametrize("which", ["std", "den"])
def test_distinct_carried_node(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH DISTINCT c "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2")
    assert _got(schemas[which], query, 3) == _rows(1, 2, distinct_c=True)


@pytest.mark.parametrize("which", ["std", "den"])
def test_filter_on_path_endpoints(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "WHERE a.user_id <> b.user_id "
             "RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2")
    expected = Counter({k: v for k, v in _rows(1, 2).items() if k[1] != k[2]})
    assert _got(schemas[which], query, 3) == expected


@pytest.mark.parametrize("which", ["std", "den"])
def test_aggregate_over_the_chain(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "RETURN c.user_id AS col0, count(*) AS col1")
    expected = Counter()
    for (c, _a, _b), n in _rows(1, 2).items():
        expected[c] += n
    assert _got(schemas[which], query, 2) == Counter(expected.items())


# Standard schema only: on the denormalized schema the WITH CTE built from the first
# VLP projects `start_code`, which that CTE does not have (Code 47) — a separate
# defect in the CTE body, not in the hop this file covers.
@pytest.mark.parametrize("which", ["std"])
def test_vlp_before_the_with_then_hop_and_vlp(schemas, which):
    query = ("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WITH a, b "
             "MATCH (b)-[:FOLLOWS]->(d:User)-[:FOLLOWS*1..2]->(e:User) "
             "RETURN a.user_id AS col0, e.user_id AS col1")
    expected = Counter()
    for a in sorted({f for f, _ in EDGES}):
        for b in _paths(a, 1, 2):
            for _, d in _OUT[b]:
                for e in _paths(d, 1, 2):
                    expected[(a, e)] += 1
    assert _got(schemas[which], query, 2) == expected


@pytest.mark.parametrize("which", ["std", "den"])
def test_two_hops_before_the_vlp(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(m:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "RETURN c.user_id AS col0, m.user_id AS col1, a.user_id AS col2, "
             "b.user_id AS col3")
    expected = Counter()
    for _z, c in EDGES:
        for i, m in _OUT[c]:
            for j, a in _OUT[m]:
                if i == j:  # fixed hops are pairwise relationship-unique
                    continue
                for b in _paths(a, 1, 2):
                    expected[(c, m, a, b)] += 1
    assert _got(schemas[which], query, 4) == expected


@pytest.mark.parametrize("which", ["std", "den"])
def test_hop_after_the_vlp(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS]->(d:User) "
             "RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2, "
             "d.user_id AS col3")
    expected = Counter()
    for _z, c in EDGES:
        for i, a in _OUT[c]:
            for b in _paths(a, 1, 2):
                for j, d in _OUT[b]:
                    if i != j:  # the two fixed hops are pairwise relationship-unique
                        expected[(c, a, b, d)] += 1
    assert _got(schemas[which], query, 4) == expected


# Standard schema only: with the VLP FIRST and a WITH-carried start on a denormalized
# schema the join key of the WITH CTE is guessed as `p1_c_start_id` (Code 47) — a
# separate defect in `generate_vlp_with_cte_join_conditions`. It used to return wrong
# rows (272 vs 46) because a hop-uniqueness predicate stood in for that join key.
@pytest.mark.parametrize("which", ["std"])
def test_two_hops_after_the_vlp(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS*1..2]->(a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(d:User) "
             "RETURN c.user_id AS col0, a.user_id AS col1, b.user_id AS col2, "
             "d.user_id AS col3")
    expected = Counter()
    for _z, c in EDGES:
        for a in _paths(c, 1, 2):
            for i, b in _OUT[a]:
                for j, d in _OUT[b]:
                    if i != j:
                        expected[(c, a, b, d)] += 1
    assert _got(schemas[which], query, 4) == expected


@pytest.mark.parametrize("which", ["std", "den"])
def test_optional_hop_after_the_vlp(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
             "OPTIONAL MATCH (b)-[:FOLLOWS]->(d:User) "
             "RETURN c.user_id AS col0, d.user_id AS col1")
    result = execute_cypher(query, schema_name=schemas[which])
    got = Counter((int(r["col0"]), None if r["col1"] is None else int(r["col1"]))
                  for r in result["results"])
    expected = Counter()
    for _z, c in EDGES:
        for _i, a in _OUT[c]:
            for b in _paths(a, 1, 2):
                ds = [d for _j, d in _OUT[b]] or [None]
                for d in ds:
                    expected[(c, d)] += 1
    assert got == expected


# Standard schema only: on the denormalized schema this shape is still loud (the WITH
# CTE join is ordered before the hop it depends on).
@pytest.mark.parametrize("which", ["std"])
def test_incoming_hop_after_the_vlp(schemas, which):
    query = ("MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
             "MATCH (c)-[:FOLLOWS]->(n1:User)-[:FOLLOWS*1..2]->(n2:User)<-[:FOLLOWS]-(n3:User) "
             "RETURN c.user_id AS col0, n1.user_id AS col1, n2.user_id AS col2, "
             "n3.user_id AS col3")
    inn = defaultdict(list)
    for i, (f, t) in enumerate(EDGES):
        inn[t].append((i, f))
    expected = Counter()
    for _z, c in EDGES:
        for i, n1 in _OUT[c]:
            for n2 in _paths(n1, 1, 2):
                for j, n3 in inn[n2]:
                    if i != j:
                        expected[(c, n1, n2, n3)] += 1
    assert _got(schemas[which], query, 4) == expected
