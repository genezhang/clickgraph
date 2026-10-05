"""
#1181: a fixed hop after CHAINED variable-length paths hangs off the LAST path's end.

    MATCH (a)-[:R*1..2]->(b)-[:R*1..2]->(c)-[:R]->(y)

The chain renders `FROM vlp_a_b AS t JOIN vlp_b_c AS t_ch_0 ON t_ch_0.start_id = t.end_id`, and the
hop was tied to `t.end_id` (= b) instead of `t_ch_0.end_id` (= c): rows from the wrong node.

The oracle is brute force over the fixture's edges and follows the engine's contract for these
shapes: each path is edge-unique inside itself and the chained paths share no edge (#544); fixed
hops are pairwise unique; a hop and a path are NOT constrained against each other (#1203).
The fixture has a cycle, a branch, parallel routes and a self-loop.
"""

from collections import Counter, defaultdict

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

EDGES = [(1, 2), (2, 3), (3, 1), (2, 4), (4, 5), (1, 5), (5, 6), (1, 3), (1, 4), (6, 6),
         (4, 8), (3, 5)]

_STANDARD = """
name: chain_hop_std_1181
version: "1.0"
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: ch1181_users
      node_id: user_id
      property_mappings: {user_id: user_id}
  edges:
    - type: FOLLOWS
      database: test_integration
      table: ch1181_follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
"""

_DENORM = """
name: chain_hop_den_1181
version: "1.0"
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: ch1181_flights
      node_id: user_id
      property_mappings: {}
      from_node_properties: {user_id: src}
      to_node_properties: {user_id: dst}
  edges:
    - type: FOLLOWS
      database: test_integration
      table: ch1181_flights
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
    tables = ("ch1181_users", "ch1181_follows", "ch1181_flights")
    for t in tables:
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.ch1181_users (user_id UInt32) "
              "ENGINE = MergeTree ORDER BY user_id")
    c.command("CREATE TABLE test_integration.ch1181_follows "
              "(follower_id UInt32, followed_id UInt32) ENGINE = MergeTree "
              "ORDER BY (follower_id, followed_id)")
    c.command("CREATE TABLE test_integration.ch1181_flights "
              "(eid UInt32, src UInt32, dst UInt32) ENGINE = MergeTree ORDER BY eid")
    c.insert("test_integration.ch1181_users", [[n] for n in range(1, 9)],
             column_names=["user_id"])
    c.insert("test_integration.ch1181_follows", [list(e) for e in EDGES],
             column_names=["follower_id", "followed_id"])
    c.insert("test_integration.ch1181_flights", [[i, f, t] for i, (f, t) in enumerate(EDGES)],
             column_names=["eid", "src", "dst"])
    names = {}
    for key, body, name in (("std", _STANDARD, "chain_hop_std_1181"),
                            ("den", _DENORM, "chain_hop_den_1181")):
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
NODES = sorted({x for e in EDGES for x in e})


def _chain_rows(n_paths, hops, lo=1, hi=2):
    """(a, c, y1, y2..) rows of `n_paths` chained paths followed by `hops` directed hops."""
    rows = Counter()

    def paths(node, banned):
        found = []

        def rec(n, used, depth):
            if depth >= lo:
                found.append((n, used))
            if depth == hi:
                return
            for i, t in _OUT[n]:
                if i not in used and i not in banned:
                    rec(t, used | {i}, depth + 1)

        rec(node, frozenset(), 0)
        return found

    def after_paths(first, node, banned, k):
        if k == n_paths:
            yield first, node, banned
            return
        for end, used in paths(node, banned):
            yield from after_paths(first, end, banned | used, k + 1)

    for a in NODES:
        for first, tail, _ in after_paths(a, a, frozenset(), 0):
            def hop(node, used_hops, depth, acc):
                if depth == hops:
                    rows[(first, tail) + tuple(acc)] += 1
                    return
                for i, t in _OUT[node]:
                    if i not in used_hops:
                        hop(t, used_hops | {i}, depth + 1, acc + [t])

            hop(tail, frozenset(), 0, [])
    return rows


def _got(schema, query, width):
    result = execute_cypher(query, schema_name=schema)
    assert "results" in result, f"query failed: {result}"
    return Counter(tuple(int(r[f"col{i}"]) for i in range(width)) for r in result["results"])


@pytest.mark.parametrize("which", ["std", "den"])
def test_hop_after_two_chained_paths(schemas, which):
    query = ("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User)"
             "-[:FOLLOWS]->(y:User) "
             "RETURN a.user_id AS col0, c.user_id AS col1, y.user_id AS col2")
    assert _got(schemas[which], query, 3) == _chain_rows(2, 1)


@pytest.mark.parametrize("which", ["std", "den"])
def test_two_hops_after_two_chained_paths(schemas, which):
    query = ("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User)"
             "-[:FOLLOWS]->(y:User)-[:FOLLOWS]->(z:User) "
             "RETURN a.user_id AS col0, c.user_id AS col1, y.user_id AS col2, z.user_id AS col3")
    assert _got(schemas[which], query, 4) == _chain_rows(2, 2)


@pytest.mark.parametrize("which", ["std", "den"])
def test_hop_after_three_chained_paths(schemas, which):
    query = ("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User)"
             "-[:FOLLOWS*1..2]->(d:User)-[:FOLLOWS]->(y:User) "
             "RETURN a.user_id AS col0, d.user_id AS col1, y.user_id AS col2")
    assert _got(schemas[which], query, 3) == _chain_rows(3, 1)


@pytest.mark.parametrize("which", ["std", "den"])
def test_filter_on_the_hops_end(schemas, which):
    query = ("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User)"
             "-[:FOLLOWS]->(y:User) WHERE y.user_id = 5 "
             "RETURN a.user_id AS col0, c.user_id AS col1, y.user_id AS col2")
    expected = Counter({k: v for k, v in _chain_rows(2, 1).items() if k[2] == 5})
    assert _got(schemas[which], query, 3) == expected
