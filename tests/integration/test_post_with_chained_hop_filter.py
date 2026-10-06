"""
#1177: a WHERE on a node of a fixed hop chained to a variable-length path, in the scope AFTER a WITH.

The chained hops' predicates (#1170) were gated off whenever the query had a WITH, and nothing else
emitted them: `WITH c MATCH (c)-[:R]->(a)-[:R*1..2]->(b) WHERE a.id = 1` returned every row (202
where 66 are right), silently. They are now emitted after a WITH, except a conjunct on a carried node
(`c`), which is applied where the node is (the path CTE, or against the WITH CTE).

The oracle enumerates trails (relationship-unique within each MATCH) over the edge list the engine
returns for a plain single hop.
"""

import re

import pytest
from conftest import execute_cypher

LAYOUTS = {
    "standard": ("social_integration", "User", "FOLLOWS", "user_id"),
    "denormalized": ("denormalized_flights", "Airport", "FLIGHT", "code"),
    "polymorphic": ("social_polymorphic", "User", "FOLLOWS", "user_id"),
}


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges(schema, label, rel, key):
    rows = _rows(schema, f"MATCH (a:{label})-[:{rel}]->(b:{label}) RETURN a.{key} AS a, b.{key} AS b")
    return [(r["a"], r["b"]) for r in rows]


TOKEN = re.compile(r"\((\w+)\)|(<?)-\[:R(?:\*(\d+)\.\.(\d+))?\]-(>?)")


def _parse(pattern):
    out = []
    for node, left, lo, hi, right in TOKEN.findall(pattern):
        if node:
            out.append(node)
        else:
            out.append((int(lo or 1), int(hi or 1), ">" if right else "<"))
    return out


def _matches(pattern, edges, binding):
    """Every binding of `pattern` extending `binding`, relationship-unique within the pattern."""
    pat = _parse(pattern)
    nodes = sorted({n for e in edges for n in e}, key=str)
    found = []

    def walk(i, bound, used):
        if i == len(pat) - 1:
            found.append(dict(bound))
            return
        lo, hi, direction = pat[i + 1]
        nxt = pat[i + 2]

        def go(node, path):
            if lo <= len(path) <= hi:
                if nxt in bound:
                    if bound[nxt] == node:
                        walk(i + 2, bound, used | set(path))
                else:
                    walk(i + 2, {**bound, nxt: node}, used | set(path))
            if len(path) == hi:
                return
            for eid, (s, d) in enumerate(edges):
                if eid in path or eid in used:
                    continue
                if direction == ">" and s == node:
                    go(d, path + [eid])
                if direction == "<" and d == node:
                    go(s, path + [eid])

        go(bound[pat[i]], [])

    first = pat[0]
    for start in [binding[first]] if first in binding else nodes:
        walk(0, {**binding, first: start}, set())
    return found


SHAPES = [
    ("(c)-[:R]->(a)-[:R*1..2]->(b)", "a"),
    ("(a)-[:R]->(c)-[:R*1..2]->(b)", "a"),
    ("(c)-[:R]->(a)-[:R]->(m)-[:R*1..2]->(b)", "a"),
    ("(c)-[:R]->(a)-[:R]->(m)-[:R*1..2]->(b)", "m"),
    ("(c)<-[:R]-(a)-[:R*1..2]->(b)", "a"),
    # a filter on the carried node itself stays where it was applied before
    ("(c)-[:R]->(a)-[:R*1..2]->(b)", "c"),
    ("(a)-[:R]->(c)-[:R*1..2]->(b)", "c"),
]


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("pattern, var", SHAPES)
@pytest.mark.parametrize("op", ["=", "<>"])
def test_filter_after_with_on_a_chained_hop_node(layout, pattern, var, op):
    if layout == "polymorphic" and "<-" in pattern:
        pytest.skip("#1300: an incoming hop into a carried node before a path is wrong on this layout")
    if layout == "denormalized" and pattern.startswith("(a)-[:R]->(c)"):
        pytest.skip("#1189: the carried node's id column is guessed (`p1_c_start_id`); refused loudly")
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    value = max({n for e in edges for n in e}, key=lambda n: sum(n in e for e in edges))
    carried = [{"c": c} for _, c in edges]
    expected = sum(
        1
        for row in carried
        for m in _matches(pattern, edges, row)
        if (m[var] == value) == (op == "=")
    )
    cypher = re.sub(r"\((\w)\)", lambda m: "(c)" if m[1] == "c" else f"({m[1]}:{label})", pattern)
    literal = f"'{value}'" if isinstance(value, str) else str(value)
    q = (
        f"MATCH (z:{label})-[:{rel}]->(c:{label}) WITH c "
        f"MATCH {cypher.replace(':R', ':' + rel)} WHERE {var}.{key} {op} {literal} RETURN count(*) AS k"
    )
    assert _rows(schema, q)[0]["k"] == expected, q


# A conjunct mixing a scope node with a WITH value (a scalar, or a carried node's property) is not
# a carried-node filter: nothing else applies it, so it is emitted too (#1303 review: 202 vs 85).
@pytest.mark.parametrize(
    "with_items, where, keep",
    [
        ("c, 30 AS lim", "a.age > lim", lambda z, c, a: a["age"] > 30),
        ("c, z.age AS za", "a.age > za", lambda z, c, a: a["age"] > z["age"]),
        ("c, c.age AS ca", "a.age > ca", lambda z, c, a: a["age"] > c["age"]),
        ("c, 30 AS lim", "c.age > lim", lambda z, c, a: c["age"] > 30),
        ("c, 30 AS lim", "a.age > lim AND a.user_id <> 1", lambda z, c, a: a["age"] > 30 and a["user_id"] != 1),
    ],
)
def test_filter_after_with_mixing_a_with_value(with_items, where, keep):
    schema = "social_integration"
    users = {
        r["id"]: {"user_id": r["id"], "age": r["age"]}
        for r in _rows(schema, "MATCH (u:User) RETURN u.user_id AS id, u.age AS age")
    }
    edges = _edges(schema, "User", "FOLLOWS", "user_id")
    expected = sum(
        1
        for z, c in edges
        for m in _matches("(c)-[:R]->(a)-[:R*1..2]->(b)", edges, {"c": c})
        if keep(users[z], users[c], users[m["a"]])
    )
    q = (
        f"MATCH (z:User)-[:FOLLOWS]->(c:User) WITH {with_items} "
        f"MATCH (c)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) WHERE {where} RETURN count(*) AS k"
    )
    assert _rows(schema, q)[0]["k"] == expected, q
