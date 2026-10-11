"""
#1218: `WHERE length(p) <op> k` on a variable-length path was silently DROPPED — the recursive-CTE
categorizer filed it as a "path function filter" the generator never reads, and the outer query
skips the VLP's own predicate — so every path was returned.

Oracle: the length histogram of the same pattern (`RETURN length(p), count(*)`, which counts
`hop_count` exactly) predicts the count under every comparison and bound.

On shortestPath / allShortestPaths a WHERE condition on the path applies DURING the search (Neo4j
semantics): per (start, end) pair the shortest path that satisfies it is picked. Oracle: the
per-pair length histogram of the plain variable-length pattern.
"""

import itertools

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

MIXED = "mixed_path_fn_1218"
MIXED_YAML = """
name: mixed_path_fn_1218
version: "1.0"
graph_schema:
  nodes:
    - label: Person
      database: test_integration
      table: pf1218_people
      node_id: pid
      is_denormalized: true
      property_mappings: {pid: pid, name: name}
      from_node_properties: {pid: mgr_id}
  edges:
    - type: REPORTS_TO
      database: test_integration
      table: pf1218_reports
      from_node: Person
      to_node: Person
      from_id: mgr_id
      to_id: emp_id
      property_mappings: {}
"""
EDGES = [(1, 2), (2, 3), (5, 3), (3, 4), (2, 4), (4, 5)]


@pytest.fixture(scope="module", autouse=True)
def _mixed_graph(clickhouse_client):
    c = clickhouse_client
    for t in ("pf1218_people", "pf1218_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.pf1218_people (pid UInt32, name String) "
              "ENGINE = MergeTree ORDER BY pid")
    c.command("CREATE TABLE test_integration.pf1218_reports (emp_id UInt32, mgr_id UInt32) "
              "ENGINE = MergeTree ORDER BY emp_id")
    c.insert("test_integration.pf1218_people", [[i, f"p{i}"] for i in range(1, 6)],
             column_names=["pid", "name"])
    c.insert("test_integration.pf1218_reports", [[e, m] for m, e in EDGES],
             column_names=["emp_id", "mgr_id"])
    response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                             json={"schema_name": MIXED, "config_content": MIXED_YAML})
    assert response.status_code == 200, f"schema load failed: {response.text}"
    yield
    for t in ("pf1218_people", "pf1218_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


OPS = {">": lambda l, k: l > k, ">=": lambda l, k: l >= k, "<": lambda l, k: l < k,
       "<=": lambda l, k: l <= k, "=": lambda l, k: l == k, "<>": lambda l, k: l != k}

PATTERNS = [
    ("social_integration", "(a:User)-[:FOLLOWS*1..3]->(b:User)"),
    ("social_integration", "(a:User)-[:FOLLOWS*2..3]->(b:User)"),
    ("social_integration", "(a:User)-[:FOLLOWS*]->(b:User)"),
    ("social_integration", "(a:User)-[:FOLLOWS*1..3]-(b:User)"),
    ("denormalized_flights", "(a:Airport)-[:FLIGHT*1..3]->(b:Airport)"),
    ("denormalized_flights", "(a:Airport)-[:FLIGHT*1..3]-(b:Airport)"),
    ("social_polymorphic", "(a:User)-[:FOLLOWS*1..3]->(b:User)"),
    ("composite_id", "(a:Account)-[:TRANSFERRED*1..3]->(b:Account)"),
    (MIXED, "(a:Person)-[:REPORTS_TO*1..3]->(b:Person)"),
]


@pytest.mark.parametrize("schema, pattern", PATTERNS)
def test_where_on_length_matches_the_length_histogram(schema, pattern):
    hist = {int(r["l"]): int(r["n"]) for r in
            _rows(schema, f"MATCH p={pattern} RETURN length(p) AS l, count(*) AS n")}
    assert hist, "pattern has no paths — the oracle would be vacuous"
    assert len(hist) > 1, f"need several lengths to tell the filter apart: {hist}"
    for op, k in itertools.product(OPS, [1, 2, 3]):
        want = sum(n for l, n in hist.items() if OPS[op](l, k))
        got = int(_rows(schema, f"MATCH p={pattern} WHERE length(p) {op} {k} "
                                "RETURN count(*) AS n")[0]["n"])
        assert got == want, f"{schema} {pattern}: length(p) {op} {k}: got {got}, want {want}"


# (schema, shortest function, pattern, node id property)
SHORTEST = [
    ("social_integration", "shortestPath", "(a:User)-[:FOLLOWS*1..4]->(b:User)", "user_id"),
    ("social_integration", "allShortestPaths", "(a:User)-[:FOLLOWS*1..4]->(b:User)", "user_id"),
    ("denormalized_flights", "shortestPath", "(a:Airport)-[:FLIGHT*1..4]->(b:Airport)", "code"),
]


@pytest.mark.parametrize("schema, fn, pattern, key", SHORTEST)
def test_where_on_length_applies_during_the_shortest_search(schema, fn, pattern, key):
    per_pair = {}
    for r in _rows(schema, f"MATCH p={pattern} WHERE a <> b "
                           f"RETURN a.{key} AS a, b.{key} AS b, length(p) AS l, count(*) AS n"):
        per_pair.setdefault((r["a"], r["b"]), {})[int(r["l"])] = int(r["n"])
    assert per_pair, "pattern has no paths — the oracle would be vacuous"

    def want(keep):
        total = 0
        for hist in per_pair.values():
            ok = [l for l in hist if keep(l)]
            if ok:
                total += 1 if fn == "shortestPath" else hist[min(ok)]
        return total

    unfiltered = int(_rows(schema, f"MATCH p={fn}({pattern}) RETURN count(*) AS n")[0]["n"])
    assert unfiltered == want(lambda l: True)
    lengths = {int(r["l"]) for r in
               _rows(schema, f"MATCH p={fn}({pattern}) RETURN length(p) AS l, count(*) AS n")}
    assert len(lengths) > 1, f"need several lengths to tell the filter apart: {lengths}"
    for op, k in itertools.product(OPS, [1, 2, 3]):
        expected = want(lambda l: OPS[op](l, k))
        got = int(_rows(schema, f"MATCH p={fn}({pattern}) WHERE length(p) {op} {k} "
                                "RETURN count(*) AS n")[0]["n"])
        assert got == expected, f"{schema} {fn}: length(p) {op} {k}: got {got}, want {expected}"


def test_path_predicate_combined_with_an_endpoint_filter():
    schema, pattern = "social_integration", "(a:User)-[:FOLLOWS*1..3]->(b:User)"
    hist = {int(r["l"]): int(r["n"]) for r in _rows(
        schema, f"MATCH p={pattern} WHERE a.user_id < 5 RETURN length(p) AS l, count(*) AS n")}
    want = sum(n for l, n in hist.items() if l >= 2)
    got = int(_rows(schema, f"MATCH p={pattern} WHERE length(p) >= 2 AND a.user_id < 5 "
                            "RETURN count(*) AS n")[0]["n"])
    assert got == want and want > 0


def test_path_predicate_under_an_aggregate_and_order():
    schema, pattern = "social_integration", "(a:User)-[:FOLLOWS*1..3]->(b:User)"
    rows = _rows(schema, f"MATCH p={pattern} WHERE length(p) >= 2 "
                         "RETURN length(p) AS l, count(*) AS n ORDER BY l")
    full = {int(r["l"]): int(r["n"]) for r in
            _rows(schema, f"MATCH p={pattern} RETURN length(p) AS l, count(*) AS n")}
    assert {int(r["l"]): int(r["n"]) for r in rows} == {l: n for l, n in full.items() if l >= 2}


def test_node_count_of_a_hop_plus_path_matches_the_trail_enumeration():
    """`size(nodes(p))` over a fixed hop plus a `*1..2` part: the path is a trail (no repeated
    edge) of 2 or 3 edges, so `size(nodes(p)) > 3` keeps exactly the 3-edge trails."""
    edges = [(int(r["a"]), int(r["b"])) for r in _rows(
        "social_integration",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")]
    out = {}
    for i, (a, b) in enumerate(edges):
        out.setdefault(a, []).append((i, b))

    def trails(node, used, n):
        if n == 3:
            return 1
        return sum(trails(nxt, used | {i}, n + 1)
                   for i, nxt in out.get(node, []) if i not in used)

    want = sum(trails(start, frozenset(), 0) for start in out)
    assert want > 0, "the oracle would be vacuous: no 3-edge trails"
    got = int(_rows("social_integration",
                    "MATCH p=(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) "
                    "WHERE size(nodes(p)) > 3 RETURN count(*) AS n")[0]["n"])
    assert got == want
