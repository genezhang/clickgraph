"""
#1202: `length(p)` over a path of fixed hop(s) plus ONE variable-length part counted only the
variable-length hops (the recursive CTE's `hop_count`). It is now `hop_count + k`.

Independent oracle: a path of k fixed hops followed/preceded by a `*1..2` part is exactly a TRAIL
(no repeated edge) of k+1 or k+2 edges, so the expected length histogram is computed by
enumerating trails over the edge list in Python — not from the engine's own `hop_count`.
Everything else that would read the CTE's path columns (nodes/relationships/bare p) must fail.
"""

import collections

import pytest
from conftest import execute_cypher

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
    layouts; the mixed-access layout drops the hop when a path variable is declared (#1220)."""
    social = [(int(r["a"]), int(r["b"])) for r in _rows(
        "social_integration",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")]
    flights = [(r["a"], r["b"]) for r in _rows(
        "denormalized_flights",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code AS a, b.code AS b")]
    return [
        ("social_integration", "User", "FOLLOWS", social),
        ("denormalized_flights", "Airport", "FLIGHT", flights),
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


CASES = [(g, s) for g in range(2) for s in ("hop+vlp", "vlp+hop", "hop+hop+vlp", "hop+vlp+hop")]


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


@pytest.mark.parametrize("projection", ["nodes(p)", "relationships(p)", "p", "size(nodes(p))"])
def test_other_uses_of_a_composite_path_fail_loudly(projection):
    result = execute_cypher(
        f"MATCH p=(c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN {projection}",
        schema_name="social_integration", raise_on_error=False)
    assert "results" not in result and "path variable `p`" in str(result), result


def test_plain_variable_length_path_is_unchanged():
    got = {int(r["l"]): int(r["n"]) for r in _rows(
        "social_integration",
        "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN length(p) AS l, count(*) AS n")}
    edges = _graphs()[0][3]
    hist = _trail_histogram(edges, 2)
    assert got == {1: hist[1], 2: hist[2]}
