"""
#1183: shortestPath / allShortestPaths answer per (start, end) PAIR.

The CTE ranked with `PARTITION BY end_id` (one start per end) or `PARTITION BY start_id` (one
end per start), and `allShortestPaths` took the GLOBAL minimum length — so every pair beyond the
globally closest was missing unless a start filter happened to pin the partition. With both
ends pinned by id, a BFS shortcut returned one row even where several shortest paths exist.

The oracle is brute force over the fixture's own edge list: all node-unique paths of 1..N hops,
the minimum length per ordered pair (a != b), and one row (shortestPath) or one row per path of
that length (allShortestPaths).
"""

import collections

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher, assert_query_success

SCHEMA = "shortest_pairs_1183"


# A diamond (1->2->4, 1->3->4: two shortest 1 -> 4 routes), a chord, a back edge and a tail.
EDGES = [(1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (5, 1), (2, 5), (5, 6)]

_SCHEMA_YAML = """
name: shortest_pairs_1183
version: "1.0"
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: sp1183_users
      node_id: user_id
      property_mappings: {user_id: user_id}
  edges:
    - type: FOLLOWS
      database: test_integration
      table: sp1183_follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
"""


@pytest.fixture(scope="module", autouse=True)
def _graph(clickhouse_client):
    c = clickhouse_client
    for t in ("sp1183_users", "sp1183_follows"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.sp1183_users (user_id UInt32) "
              "ENGINE = MergeTree ORDER BY user_id")
    c.command("CREATE TABLE test_integration.sp1183_follows "
              "(follower_id UInt32, followed_id UInt32) ENGINE = MergeTree "
              "ORDER BY (follower_id, followed_id)")
    c.insert("test_integration.sp1183_users", [[n] for n in range(1, 7)],
             column_names=["user_id"])
    c.insert("test_integration.sp1183_follows", [list(e) for e in EDGES],
             column_names=["follower_id", "followed_id"])
    response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                             json={"schema_name": SCHEMA, "config_content": _SCHEMA_YAML})
    assert response.status_code == 200, f"schema load failed: {response.text}"
    yield
    for t in ("sp1183_users", "sp1183_follows"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")


def _edges():
    return list(EDGES)


def _oracle(edges, mode, hi, starts=None, ends=None):
    out = collections.defaultdict(list)
    for a, b in edges:
        out[a].append(b)
    rows = collections.Counter()
    for a in sorted({x for e in edges for x in e}):
        if starts is not None and a not in starts:
            continue
        lengths = collections.defaultdict(list)

        def rec(node, seen, k):
            if k >= 1 and node != a:
                lengths[node].append(k)
            if k < hi:
                for m in out[node]:
                    if m not in seen:
                        rec(m, seen | {m}, k + 1)

        rec(a, {a}, 0)
        for b, ls in lengths.items():
            if ends is not None and b not in ends:
                continue
            shortest = min(ls)
            rows[(a, b, shortest)] += 1 if mode == "shortestPath" else ls.count(shortest)
    return rows


def _got(mode, hi, where=""):
    response = execute_cypher(
        f"MATCH p = {mode}((a:User)-[:FOLLOWS*1..{hi}]->(b:User)) {where} "
        "RETURN a.user_id AS a, b.user_id AS b, length(p) AS l",
        schema_name=SCHEMA,
    )
    assert_query_success(response)
    return collections.Counter((r["a"], r["b"], r["l"]) for r in response["results"])


@pytest.mark.parametrize("mode", ["shortestPath", "allShortestPaths"])
@pytest.mark.parametrize("hi", [3, 4])
def test_unfiltered_returns_every_reachable_pair_1183(mode, hi):
    assert _got(mode, hi) == _oracle(_edges(), mode, hi)


@pytest.mark.parametrize("mode", ["shortestPath", "allShortestPaths"])
def test_start_pinned_1183(mode):
    assert _got(mode, 4, "WHERE a.user_id = 1") == _oracle(_edges(), mode, 4, starts={1})


@pytest.mark.parametrize("mode", ["shortestPath", "allShortestPaths"])
def test_end_pinned_1183(mode):
    assert _got(mode, 4, "WHERE b.user_id = 4") == _oracle(_edges(), mode, 4, ends={4})


@pytest.mark.parametrize("mode", ["shortestPath", "allShortestPaths"])
def test_multi_valued_end_filter_1183(mode):
    assert _got(mode, 4, "WHERE b.user_id IN [4, 5]") == _oracle(_edges(), mode, 4, ends={4, 5})


@pytest.mark.parametrize("mode", ["shortestPath", "allShortestPaths"])
def test_both_ends_pinned_1183(mode):
    """Both ends pinned by id takes the BFS shortcut for shortestPath (one row); it must
    not for allShortestPaths, whose pair can have several shortest paths (1-2-4, 1-3-4)."""
    expected = _oracle(_edges(), mode, 4, starts={1}, ends={4})
    got = _got(mode, 4, "WHERE a.user_id = 1 AND b.user_id = 4")
    assert got == expected
    assert sum(got.values()) == (2 if mode == "allShortestPaths" else 1)
