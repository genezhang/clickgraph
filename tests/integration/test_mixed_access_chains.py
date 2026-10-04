"""
Chained fixed hops on a MIXED-access (foreign-embedded) schema (#1158).

One endpoint of the edge is embedded in the edge table, the other lives on its own
node table.  The hops of a chain must be tied to each other through the node they
share; before #1158 the second hop rendered `JOIN <edge> ON 1 = 1` — a cartesian
product — and the shared node read two different values in one row.

Every variable's pid AND name is projected, and the rows are compared with a
brute-force enumeration over the fixture (Cypher relationship-uniqueness within the
MATCH).  The fixture has a cycle, a branch, a triangle and a self-loop, because an
acyclic graph cannot reuse an edge and would agree with a missing uniqueness
predicate by coincidence.
"""

from collections import Counter

import pytest
import requests
from conftest import CLICKGRAPH_URL, execute_cypher

PEOPLE = {'p1': 'Alice', 'p2': 'Bob', 'p3': 'Carol', 'p4': 'Dan', 'p5': 'Eve', 'p6': 'Fay',
          'p7': 'Gus', 'p8': 'Hal'}
# (manager, employee): a 3-cycle p1->p2->p3->p1, a branch at p2, the triangle
# p1->p2->p3 with p1->p3, a fan at p1 and a self-loop on p6.  p8 only has an incoming
# edge and p7 none at all, so an OPTIONAL hop really does miss for some nodes.
REPORTS = [('p1', 'p2'), ('p2', 'p3'), ('p3', 'p1'), ('p2', 'p4'), ('p4', 'p5'),
           ('p1', 'p5'), ('p5', 'p6'), ('p1', 'p3'), ('p1', 'p4'), ('p6', 'p6'), ('p4', 'p8')]

_SCHEMA = """
name: {name}
version: "1.0"
graph_schema:
  nodes:
    - label: Person
      database: test_integration
      table: mixed_people_1158
      node_id: pid
      is_denormalized: true
      property_mappings:
        pid: pid
        name: name
      {embedded}_node_properties:
        pid: {embedded_col}
  edges:
    - type: REPORTS_TO
      database: test_integration
      table: mixed_reports_1158
      from_node: Person
      to_node: Person
      from_id: mgr_id
      to_id: emp_id
      property_mappings: {{}}
"""


@pytest.fixture(scope="module")
def mixed_schemas(clickhouse_client):
    c = clickhouse_client
    c.command("DROP TABLE IF EXISTS test_integration.mixed_people_1158")
    c.command("DROP TABLE IF EXISTS test_integration.mixed_reports_1158")
    c.command("CREATE TABLE test_integration.mixed_people_1158 (pid String, name String) "
              "ENGINE = MergeTree ORDER BY pid")
    c.command("CREATE TABLE test_integration.mixed_reports_1158 (mgr_id String, emp_id String) "
              "ENGINE = MergeTree ORDER BY (mgr_id, emp_id)")
    c.insert('test_integration.mixed_people_1158', [[k, v] for k, v in PEOPLE.items()],
             column_names=['pid', 'name'])
    c.insert('test_integration.mixed_reports_1158', [list(e) for e in REPORTS],
             column_names=['mgr_id', 'emp_id'])
    names = {}
    for role, col in (('from', 'mgr_id'), ('to', 'emp_id')):
        name = f'mixed_access_{role}_1158'
        response = requests.post(
            f'{CLICKGRAPH_URL}/schemas/load',
            json={'schema_name': name,
                  'config_content': _SCHEMA.format(name=name, embedded=role, embedded_col=col)},
        )
        assert response.status_code == 200, f"schema load failed: {response.text}"
        names[role] = name
    yield names
    c.command("DROP TABLE IF EXISTS test_integration.mixed_people_1158")
    c.command("DROP TABLE IF EXISTS test_integration.mixed_reports_1158")


def _expected(variables, edges):
    """Rows (pid, name per variable) over `edges` (graph orientation), every edge used
    at most once."""
    rows = Counter()

    def rec(i, bind, used):
        if i == len(edges):
            rows[tuple(x for v in variables for x in (bind[v], PEOPLE[bind[v]]))] += 1
            return
        f, t = edges[i]
        for k, (ef, et) in enumerate(REPORTS):
            if k in used or (f in bind and bind[f] != ef) or (t in bind and bind[t] != et):
                continue
            if f == t and ef != et:
                continue
            rec(i + 1, {**bind, f: ef, t: et}, used | {k})

    rec(0, {}, frozenset())
    return rows


# (id, MATCH pattern, variables in RETURN order, edges as (from_var, to_var))
SHAPES = [
    ('chain2', '(a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person)',
     'abc', [('a', 'b'), ('b', 'c')]),
    ('chain2-incoming', '(c:Person)<-[:REPORTS_TO]-(b:Person)<-[:REPORTS_TO]-(a:Person)',
     'abc', [('a', 'b'), ('b', 'c')]),
    ('chain3', '(a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person)'
               '-[:REPORTS_TO]->(d:Person)', 'abcd', [('a', 'b'), ('b', 'c'), ('c', 'd')]),
    ('fan-out', '(a:Person)<-[:REPORTS_TO]-(b:Person)-[:REPORTS_TO]->(c:Person)',
     'abc', [('b', 'a'), ('b', 'c')]),
    ('fan-in', '(a:Person)-[:REPORTS_TO]->(b:Person)<-[:REPORTS_TO]-(c:Person)',
     'abc', [('a', 'b'), ('c', 'b')]),
    ('zigzag', '(a:Person)-[:REPORTS_TO]->(b:Person)<-[:REPORTS_TO]-(c:Person)'
               '-[:REPORTS_TO]->(d:Person)', 'abcd', [('a', 'b'), ('c', 'b'), ('c', 'd')]),
    ('comma-chain', '(a:Person)-[:REPORTS_TO]->(b:Person), (b)-[:REPORTS_TO]->(c:Person)',
     'abc', [('a', 'b'), ('b', 'c')]),
    ('triangle', '(a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person), '
                 '(a)-[:REPORTS_TO]->(c)', 'abc', [('a', 'b'), ('b', 'c'), ('a', 'c')]),
    ('cycle', '(a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person)'
              '-[:REPORTS_TO]->(a)', 'abc', [('a', 'b'), ('b', 'c'), ('c', 'a')]),
    # Two patterns with nothing in common ARE a cross product (minus the pairs that
    # would use one edge twice).
    ('disconnected', '(a:Person)-[:REPORTS_TO]->(b:Person), (c:Person)-[:REPORTS_TO]->(d:Person)',
     'abcd', [('a', 'b'), ('c', 'd')]),
]


@pytest.mark.parametrize('role', ['from', 'to'])
@pytest.mark.parametrize('shape', SHAPES, ids=[s[0] for s in SHAPES])
def test_mixed_chain_rows_match_the_oracle_1158(mixed_schemas, role, shape):
    _, pattern, variables, edges = shape
    ret = ', '.join(f'{v}.pid AS {v}_pid, {v}.name AS {v}_name' for v in variables)
    response = execute_cypher(f'MATCH {pattern} RETURN {ret}', schema_name=mixed_schemas[role])
    got = Counter(
        tuple(r[f'{v}_{p}'] for v in variables for p in ('pid', 'name'))
        for r in response['results']
    )
    expected = _expected(variables, edges)
    assert not (expected - got), f"missing rows: {sorted((expected - got).elements())[:5]}"
    assert not (got - expected), f"fabricated rows: {sorted((got - expected).elements())[:5]}"


@pytest.mark.parametrize('role', ['from', 'to'])
def test_mixed_chain_count_matches_the_oracle_1158(mixed_schemas, role):
    """`count(*)` references no node column, so the node joins are pruned; the hops
    must still be linked."""
    response = execute_cypher(
        'MATCH (a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person) RETURN count(*) AS n',
        schema_name=mixed_schemas[role],
    )
    assert int(response['results'][0]['n']) == sum(
        _expected('abc', [('a', 'b'), ('b', 'c')]).values())


# --- OPTIONAL hops ------------------------------------------------------------------

def _expected_optional(variables, required, optional):
    """Left-join semantics: every row of the required pattern, extended by the optional
    edge when one matches the already-bound nodes, else NULL (''). Relationship
    uniqueness holds within a clause, not across clauses."""
    def extend(edges, bind):
        out = []

        def rec(i, b, used):
            if i == len(edges):
                out.append(b)
                return
            f, t = edges[i]
            for k, (ef, et) in enumerate(REPORTS):
                if k in used or (f in b and b[f] != ef) or (t in b and b[t] != et):
                    continue
                if f == t and ef != et:
                    continue
                rec(i + 1, {**b, f: ef, t: et}, used | {k})

        rec(0, bind, frozenset())
        return out

    rows = Counter()
    for b in extend(required, {}):
        for c in extend(optional, b) or [b]:
            rows[tuple(x for v in variables for x in
                       ((c[v], PEOPLE[c[v]]) if v in c else ('', '')))] += 1
    return rows


def _run_rows(schema, pattern, variables):
    ret = ', '.join(f'{v}.pid AS {v}_pid, {v}.name AS {v}_name' for v in variables)
    response = execute_cypher(f'{pattern} RETURN {ret}', schema_name=schema)
    return Counter(
        tuple('' if r[f'{v}_{p}'] is None else r[f'{v}_{p}'] for v in variables for p in ('pid', 'name'))
        for r in response['results']
    )


# Shapes that are supported on BOTH roles: the node the OPTIONAL hop closes on was
# embedded in an earlier edge, so its first binding is the one that is read.
@pytest.mark.parametrize('role', ['from', 'to'])
def test_mixed_optional_closing_edge_matches_the_oracle_1158(mixed_schemas, role):
    pattern = ('MATCH (a:Person)-[:REPORTS_TO]->(b:Person)-[:REPORTS_TO]->(c:Person) '
               'OPTIONAL MATCH (a)-[:REPORTS_TO]->(c)')
    got = _run_rows(mixed_schemas[role], pattern, 'abc')
    assert got == _expected_optional('abc', [('a', 'b'), ('b', 'c')], [('a', 'c')])


def test_mixed_to_embedded_optional_hop_after_a_hop_matches_the_oracle_1158(mixed_schemas):
    pattern = ('MATCH (a:Person)-[:REPORTS_TO]->(b:Person) '
               'OPTIONAL MATCH (b)-[:REPORTS_TO]->(c:Person)')
    got = _run_rows(mixed_schemas['to'], pattern, 'abc')
    assert got == _expected_optional('abc', [('a', 'b')], [('b', 'c')])


def test_mixed_to_embedded_optional_fan_closing_matches_the_oracle_1158(mixed_schemas):
    pattern = ('MATCH (a:Person)<-[:REPORTS_TO]-(b:Person)-[:REPORTS_TO]->(c:Person) '
               'OPTIONAL MATCH (a)-[:REPORTS_TO]->(c)')
    got = _run_rows(mixed_schemas['to'], pattern, 'abc')
    assert got == _expected_optional('abc', [('b', 'a'), ('b', 'c')], [('a', 'c')])


# Shapes that would return NULL ids (or ids that do not belong to their names) must
# fail loud instead.
LOUD = [
    ('from', 'MATCH (a:Person)-[:REPORTS_TO]->(b:Person) OPTIONAL MATCH (b)-[:REPORTS_TO]->(c:Person)'
             ' RETURN a.pid, b.pid, c.pid'),
    ('to', 'MATCH (b:Person) OPTIONAL MATCH (a:Person)-[:REPORTS_TO]->(b) RETURN b.pid, a.pid'),
    ('from', 'MATCH (a:Person)-[:REPORTS_TO]-(c:Person)-[:REPORTS_TO]-(e:Person) RETURN c.pid, count(*)'),
    ('to', 'MATCH (a:Person)-[:REPORTS_TO]->(c:Person)-[:REPORTS_TO]-(e:Person) RETURN c.pid, count(*)'),
]


@pytest.mark.parametrize('role,query', LOUD, ids=[f'{r}-{i}' for i, (r, _) in enumerate(LOUD)])
def test_mixed_unresolvable_shapes_fail_loud_1158(mixed_schemas, role, query):
    response = execute_cypher(query, schema_name=mixed_schemas[role], raise_on_error=False)
    assert response.get('status') == 'error', f"expected a loud error, got rows: {response}"
    assert '#1186' in str(response['error'])


# An undirected FIRST hop, or an explicitly directed hop after it, is fine.
@pytest.mark.parametrize('role', ['from', 'to'])
def test_mixed_undirected_first_hop_matches_the_oracle_1158(mixed_schemas, role):
    got = _run_rows(
        mixed_schemas[role],
        'MATCH (a:Person)-[:REPORTS_TO]-(b:Person)-[:REPORTS_TO]->(c:Person)', 'abc')
    expected = Counter()
    for i, (f1, t1) in enumerate(REPORTS):
        for (a, b) in {(f1, t1), (t1, f1)}:
            for j, (f2, t2) in enumerate(REPORTS):
                if j != i and f2 == b:
                    expected[(a, PEOPLE[a], b, PEOPLE[b], t2, PEOPLE[t2])] += 1
    assert got == expected
