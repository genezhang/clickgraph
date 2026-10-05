"""
#1220: declaring a path variable (`MATCH p=...`) must not change the ROW COUNT of the pattern.

The emitter's "spurious JOIN" cleanup for a path variable over a VLP CTE kept only joins whose alias
was read somewhere (SELECT/WHERE/another kept join's ON). A fixed hop chained to the path whose
edge-table alias nothing reads (`RETURN count(*)`) was dropped, so the query returned the path's
own row count — 55 where 90 is correct on the standard layout, 17 where 24 on the mixed-access
layout (where the #1175 uniqueness guard, which happens to read the alias, does not apply).

Differential oracle: the same pattern WITHOUT the path variable (which never took that cleanup).
"""

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

MIXED = "mixed_path_var_1220"
MIXED_YAML = """
name: mixed_path_var_1220
version: "1.0"
graph_schema:
  nodes:
    - label: Person
      database: test_integration
      table: pv1220_people
      node_id: pid
      is_denormalized: true
      property_mappings: {pid: pid, name: name}
      from_node_properties: {pid: mgr_id}
  edges:
    - type: REPORTS_TO
      database: test_integration
      table: pv1220_reports
      from_node: Person
      to_node: Person
      from_id: mgr_id
      to_id: emp_id
      property_mappings: {}
"""
EDGES = [(1, 2), (2, 3), (5, 3), (3, 4), (2, 4), (4, 5), (4, 1)]


@pytest.fixture(scope="module", autouse=True)
def _mixed_graph(clickhouse_client):
    c = clickhouse_client
    for t in ("pv1220_people", "pv1220_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")
    c.command("CREATE TABLE test_integration.pv1220_people (pid UInt32, name String) "
              "ENGINE = MergeTree ORDER BY pid")
    c.command("CREATE TABLE test_integration.pv1220_reports (emp_id UInt32, mgr_id UInt32) "
              "ENGINE = MergeTree ORDER BY emp_id")
    c.insert("test_integration.pv1220_people", [[i, f"p{i}"] for i in range(1, 6)],
             column_names=["pid", "name"])
    c.insert("test_integration.pv1220_reports", [[e, m] for m, e in EDGES],
             column_names=["emp_id", "mgr_id"])
    response = requests.post(f"{CLICKGRAPH_URL}/schemas/load",
                             json={"schema_name": MIXED, "config_content": MIXED_YAML})
    assert response.status_code == 200, f"schema load failed: {response.text}"
    yield
    for t in ("pv1220_people", "pv1220_reports"):
        c.command(f"DROP TABLE IF EXISTS test_integration.{t}")


def _count(schema, pattern, path_var):
    head = "p=" if path_var else ""
    result = execute_cypher(f"MATCH {head}{pattern} RETURN count(*) AS n",
                            schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{pattern}: {result}"
    return int(result["results"][0]["n"])


PATTERNS = [
    # trailing hop of ANOTHER relationship type (not covered by #1175's guard)
    ("social_integration", "(a:User)-[:FOLLOWS*1..2]->(b:User)-[:AUTHORED]->(c:Post)"),
    ("social_integration", "(a:User)-[:FOLLOWS*1..2]->(b:User)-[:LIKED]->(c:Post)"),
    # same relationship type
    ("social_integration", "(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)"),
    ("social_integration", "(a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS]->(c:User)"),
    ("social_integration", "(x:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)"),
    ("social_polymorphic", "(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User)"),
    ("denormalized_flights", "(c:Airport)-[:FLIGHT]->(a:Airport)-[:FLIGHT*1..2]->(b:Airport)"),
    ("denormalized_flights", "(a:Airport)-[:FLIGHT*1..2]->(b:Airport)-[:FLIGHT]->(c:Airport)"),
    (MIXED, "(c:Person)-[:REPORTS_TO]->(a:Person)-[:REPORTS_TO*1..2]->(b:Person)"),
    (MIXED, "(a:Person)-[:REPORTS_TO*1..2]->(b:Person)-[:REPORTS_TO]->(c:Person)"),
    (MIXED, "(x:Person)-[:REPORTS_TO]->(c:Person)-[:REPORTS_TO]->(a:Person)-[:REPORTS_TO*1..2]->(b:Person)"),
]


@pytest.mark.parametrize("schema, pattern", PATTERNS)
def test_path_variable_does_not_change_the_row_count(schema, pattern):
    plain = _count(schema, pattern, path_var=False)
    assert plain > 0, "vacuous oracle"
    assert _count(schema, pattern, path_var=True) == plain
