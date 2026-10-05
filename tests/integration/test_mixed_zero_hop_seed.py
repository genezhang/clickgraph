"""
#1180: a zero-lower-bound path whose START is embedded in the edge table (mixed access: the node
has its own table, but its id/properties are declared on the edge) seeds the recursion from the
EDGE table, one row per edge. A node that starts N edges was seeded N times and every path from
it was returned N times. Brute-force oracle below: edge-unique trails of 0..N hops from every node.
"""

import collections

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher, assert_query_success

SCHEMA = "mixed_zero_hop_1180"
# (manager, employee): `from` = manager. Person 2 manages two people.
EDGES = [(1, 2), (2, 3), (5, 3), (3, 4), (2, 4), (4, 5)]

_YAML = """
name: mixed_zero_hop_1180
version: "1.0"
graph_schema:
  nodes:
    - label: Person
      database: test_integration
      table: mz1180_people
      node_id: pid
      is_denormalized: true
      property_mappings: {pid: pid, name: name}
      from_node_properties: {pid: mgr_id}
  edges:
    - type: REPORTS_TO
      database: test_integration
      table: mz1180_reports
      from_node: Person
      to_node: Person
      from_id: mgr_id
      to_id: emp_id
      property_mappings: {}
"""


@pytest.fixture(scope="module", autouse=True)
def _graph(clickhouse_client):
    c = clickhouse_client
    for t in ("mz1180_people", "mz1180_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.mz1180_people (pid UInt32, name String) "
              "ENGINE = MergeTree ORDER BY pid")
    c.command("CREATE TABLE test_integration.mz1180_reports (emp_id UInt32, mgr_id UInt32) "
              "ENGINE = MergeTree ORDER BY emp_id")
    c.insert("test_integration.mz1180_people", [[i, f"p{i}"] for i in range(1, 6)],
             column_names=["pid", "name"])
    c.insert("test_integration.mz1180_reports", [[e, m] for m, e in EDGES],
             column_names=["emp_id", "mgr_id"])
    response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                             json={"schema_name": SCHEMA, "config_content": _YAML})
    assert response.status_code == 200, f"schema load failed: {response.text}"
    yield
    for t in ("mz1180_people", "mz1180_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")


def _oracle(lo, hi, start=None):
    """Cypher TRAIL semantics (#1230): an edge may not repeat, a node may — a walk can return to
    its start through a cycle (the original node-unique oracle dropped those)."""
    out = collections.defaultdict(list)
    for i, (a, b) in enumerate(EDGES):
        out[a].append((i, b))
    rows = collections.Counter()
    for a in range(1, 6):
        if start is not None and a != start:
            continue

        def rec(n, used, k):
            if k >= lo:
                rows[(a, n)] += 1
            if k < hi:
                for i, m in out[n]:
                    if i not in used:
                        rec(m, used | {i}, k + 1)

        rec(a, frozenset(), 0)
    return rows


def _got(cypher):
    response = execute_cypher(cypher, schema_name=SCHEMA)
    assert_query_success(response)
    return collections.Counter((r["a"], r["b"]) for r in response["results"])


@pytest.mark.parametrize("hi", [1, 2, 3])
def test_zero_hop_from_embedded_start_counts_each_path_once_1180(hi):
    got = _got(f"MATCH (a:Person)-[:REPORTS_TO*0..{hi}]->(b:Person) "
               "RETURN a.pid AS a, b.pid AS b")
    assert got == _oracle(0, hi)


def test_zero_hop_start_filter_1180():
    got = _got("MATCH (a:Person)-[:REPORTS_TO*0..2]->(b:Person) WHERE a.pid = 2 "
               "RETURN a.pid AS a, b.pid AS b")
    assert got == _oracle(0, 2, start=2)
    assert sum(got.values()) == 5  # was 10: every path from person 2 appeared twice
