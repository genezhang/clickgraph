"""
#1202: `length(p)` over a path of fixed hop(s) plus ONE variable-length part counted only the
variable-length hops (the recursive CTE's `hop_count`). It is now `hop_count + k`.

Independent oracle: a path of k fixed hops followed/preceded by a `*1..2` part is exactly a TRAIL
(no repeated edge) of k+1 or k+2 edges, so the expected length histogram is computed by
enumerating trails over the edge list in Python — not from the engine's own `hop_count`.
`nodes(p)`, `relationships(p)` and the bare path are checked against the same trail enumeration
(relationship uniqueness spans the whole path, fixed hops included; verified against Neo4j 5.26).
"""

import collections

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

MIXED = "mixed_path_len_1202"
MIXED_YAML = """
name: mixed_path_len_1202
version: "1.0"
graph_schema:
  nodes:
    - label: Person
      database: test_integration
      table: pl1202_people
      node_id: pid
      is_denormalized: true
      property_mappings: {pid: pid, name: name}
      from_node_properties: {pid: mgr_id}
  edges:
    - type: REPORTS_TO
      database: test_integration
      table: pl1202_reports
      from_node: Person
      to_node: Person
      from_id: mgr_id
      to_id: emp_id
      property_mappings: {}
"""
MIXED_EDGES = [(1, 2), (2, 3), (5, 3), (3, 4), (2, 4), (4, 5), (4, 1)]


@pytest.fixture(scope="module", autouse=True)
def _mixed_graph(clickhouse_client):
    c = clickhouse_client
    for t in ("pl1202_people", "pl1202_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.pl1202_people (pid UInt32, name String) "
              "ENGINE = MergeTree ORDER BY pid")
    c.command("CREATE TABLE test_integration.pl1202_reports (emp_id UInt32, mgr_id UInt32) "
              "ENGINE = MergeTree ORDER BY emp_id")
    c.insert("test_integration.pl1202_people", [[i, f"p{i}"] for i in range(1, 6)],
             column_names=["pid", "name"])
    c.insert("test_integration.pl1202_reports", [[e, m] for m, e in MIXED_EDGES],
             column_names=["emp_id", "mgr_id"])
    response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                             json={"schema_name": MIXED, "config_content": MIXED_YAML})
    assert response.status_code == 200, f"schema load failed: {response.text}"
    yield
    for t in ("pl1202_people", "pl1202_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")

def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _trail_histogram(edges, max_len):
    out = collections.defaultdict(list)
    for i, (a, b) in enumerate(edges):
        out[a].append((i, b))
    hist = collections.Counter()

    def go(node, used, n):
        if n:
            hist[n] += 1
        if n == max_len:
            return
        for i, nxt in out[node]:
            if i not in used:
                go(nxt, used | {i}, n + 1)

    for start in list(out):
        go(start, frozenset(), 0)
    return hist


def _graphs():
    """(schema, label, relationship type, edge list). Own-table (standard) and denormalized
    layouts plus the mixed-access one (whose hop a declared path variable used to drop, #1220)."""
    social = [(int(r["a"]), int(r["b"])) for r in _rows(
        "social_integration",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")]
    flights = [(r["a"], r["b"]) for r in _rows(
        "denormalized_flights",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code AS a, b.code AS b")]
    return [
        ("social_integration", "User", "FOLLOWS", social),
        ("denormalized_flights", "Airport", "FLIGHT", flights),
        (MIXED, "Person", "REPORTS_TO", MIXED_EDGES),
    ]


def _shapes(label, rel):
    n = f"(%s:{label})"
    r = f"-[:{rel}]->"
    v = f"-[:{rel}*1..2]->"
    return {
        "hop+vlp": (f"{n % 'c'}{r}{n % 'a'}{v}{n % 'b'}", 1),
        "vlp+hop": (f"{n % 'a'}{v}{n % 'b'}{r}{n % 'c'}", 1),
        "hop+hop+vlp": (f"{n % 'x'}{r}{n % 'c'}{r}{n % 'a'}{v}{n % 'b'}", 2),
        "hop+vlp+hop": (f"{n % 'c'}{r}{n % 'a'}{v}{n % 'b'}{r}{n % 'd'}", 2),
    }


CASES = [(g, s) for g in range(3) for s in ("hop+vlp", "vlp+hop", "hop+hop+vlp", "hop+vlp+hop")]


@pytest.mark.parametrize("graph_idx, shape", CASES)
def test_length_matches_the_trail_enumeration(graph_idx, shape):
    schema, label, rel, edges = _graphs()[graph_idx]
    pattern, k = _shapes(label, rel)[shape]
    hist = _trail_histogram(edges, 2 + k)
    want = {length + k: hist[length + k] for length in (1, 2) if hist[length + k]}
    assert want, "the oracle would be vacuous: no such trails"
    if graph_idx == 0:
        assert len(want) == 2, f"need both lengths populated to tell k apart: {want}"

    got = {int(r["l"]): int(r["n"]) for r in
           _rows(schema, f"MATCH p={pattern} RETURN length(p) AS l, count(*) AS n")}
    assert got == want, f"{schema} {shape}: RETURN length(p)"

    for op, keep in {">": lambda l, t: l > t, "<=": lambda l, t: l <= t,
                     "=": lambda l, t: l == t}.items():
        for t in (k + 1, k + 2):
            expected = sum(n for l, n in want.items() if keep(l, t))
            count = int(_rows(schema, f"MATCH p={pattern} WHERE length(p) {op} {t} "
                                      "RETURN count(*) AS n")[0]["n"])
            assert count == expected, f"{schema} {shape}: length(p) {op} {t}"


def test_length_in_with_for_leading_hops():
    schema, label, rel, edges = _graphs()[0]
    pattern, k = _shapes(label, rel)["hop+vlp"]
    hist = _trail_histogram(edges, 2 + k)
    want = {length + k: hist[length + k] for length in (1, 2)}
    rows = _rows(schema, f"MATCH p={pattern} WITH length(p) AS len RETURN len")
    assert dict(collections.Counter(int(r["len"]) for r in rows)) == want


def _trail_node_sequences(edges, lengths):
    """Node-id sequences of every trail (no repeated edge) whose edge count is in `lengths`."""
    out = collections.defaultdict(list)
    for i, (a, b) in enumerate(edges):
        out[a].append((i, b))
    seqs = collections.Counter()

    def go(nodes, used):
        if len(nodes) - 1 in lengths:
            seqs[tuple(nodes)] += 1
        if len(nodes) - 1 == max(lengths):
            return
        for i, nxt in out[nodes[-1]]:
            if i not in used:
                go(nodes + [nxt], used | {i})

    for start in list(out):
        go([start], frozenset())
    return seqs


_COMPOSITE = "MATCH p=(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN {} AS v"


def test_values_of_a_composite_path_match_the_trail_enumeration():
    """A fixed hop followed by a `*1..2` part is a trail of 2 or 3 edges: `nodes(p)`,
    `relationships(p)`, the bare path and `size(nodes(p))` all describe that trail."""
    edges = _graphs()[0][3]
    want = _trail_node_sequences(edges, (2, 3))
    assert sum(want.values()) == 99, f"fixture drifted: {sum(want.values())} trails"

    def ids(nodes):
        return tuple(int(n["properties"]["user_id"]) for n in nodes)

    paths = [r["v"] for r in _rows("social_integration", _COMPOSITE.format("p"))]
    assert collections.Counter(ids(path[0::2]) for path in paths) == want
    for path in paths:
        nodes, rels = path[0::2], path[1::2]
        assert len({r["elementId"] for r in rels}) == len(rels), f"edge repeated: {path}"
        for k, r in enumerate(rels):
            assert r["startNodeElementId"] == nodes[k]["elementId"], path
            assert r["endNodeElementId"] == nodes[k + 1]["elementId"], path

    node_lists = [r["v"] for r in _rows("social_integration", _COMPOSITE.format("nodes(p)"))]
    assert collections.Counter(ids(n) for n in node_lists) == want

    rel_lists = [r["v"] for r in
                 _rows("social_integration", _COMPOSITE.format("relationships(p)"))]
    assert (collections.Counter(tuple(r["elementId"] for r in rels) for rels in rel_lists)
            == collections.Counter(tuple(r["elementId"] for r in path[1::2]) for path in paths))

    sizes = collections.Counter(
        int(r["v"]) for r in _rows("social_integration", _COMPOSITE.format("size(nodes(p))")))
    assert sizes == collections.Counter(len(seq) for seq in want.elements())


def test_plain_variable_length_path_is_unchanged():
    got = {int(r["l"]): int(r["n"]) for r in _rows(
        "social_integration",
        "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN length(p) AS l, count(*) AS n")}
    edges = _graphs()[0][3]
    hist = _trail_histogram(edges, 2)
    assert got == {1: hist[1], 2: hist[2]}
