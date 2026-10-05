"""
#1203: relationship uniqueness between a fixed hop and an adjacent variable-length path for paths
written BACKWARDS (`(a)<-[:R*1..2]-(b)`) and UNDIRECTED (`(a)-[:R*1..2]-(b)`, one doubled-edge walk).
The #1175 guard (`NOT has(path.path_edges, <hop edge>)`) used to be fenced to forward-written
paths, so those shapes over-counted on a cyclic graph (e.g. `(c)-[:R]->(a)-[:R*1..2]-(b)`: 297
where 192 is correct).

Oracle: Python trail enumeration with GLOBAL edge uniqueness (see test_zero_lower_bound_trails).
"""

import itertools

import pytest
from test_zero_lower_bound_trails import _arrow, _count, _graph, _rows

SCHEMA, LABEL, REL = "social_integration", "User", "FOLLOWS"


def _query(segments):
    pattern = f"(n0:{LABEL})"
    for i, (d, lo, hi) in enumerate(segments):
        pattern += _arrow(d, REL, lo, hi) + f"(n{i + 1}:{LABEL})"
    return f"MATCH {pattern} RETURN count(*) AS n"


@pytest.mark.parametrize("path_dir, bounds", list(itertools.product(
    ["out", "in", "both"], [(1, 2), (0, 2), (2, 3), (1, 3)])))
@pytest.mark.parametrize("hop_dir", ["out", "in"])
@pytest.mark.parametrize("order", ["hop+path", "path+hop"])
def test_hop_next_to_a_path_in_every_direction_matches_the_trail_enumeration(
        order, hop_dir, path_dir, bounds):
    graph = _graph(SCHEMA, LABEL, REL, "user_id")
    path = (path_dir, *bounds)
    segments = [(hop_dir, 1, 1), path] if order == "hop+path" else [path, (hop_dir, 1, 1)]
    got = int(_rows(SCHEMA, _query(segments))[0]["n"])
    assert got == _count(graph, segments), _query(segments)


def test_two_hops_around_a_backwards_path():
    graph = _graph(SCHEMA, LABEL, REL, "user_id")
    segments = [("out", 1, 1), ("in", 1, 2), ("out", 1, 1)]
    got = int(_rows(SCHEMA, _query(segments))[0]["n"])
    assert got == _count(graph, segments)


# --- other layouts: polymorphic (single walk, all directions) and denormalized (directed only;
# the denormalized undirected path is the two-arm split with its own under-count) ---------------

OTHER = [
    ("social_polymorphic", "User", "FOLLOWS", "user_id", ["out", "in", "both"]),
    ("denormalized_flights", "Airport", "FLIGHT", "code", ["out", "in"]),
]


@pytest.mark.parametrize("schema, label, rel, id_prop, path_dirs", OTHER)
def test_hop_next_to_a_path_on_other_layouts(schema, label, rel, id_prop, path_dirs):
    graph = _graph(schema, label, rel, id_prop)
    for path_dir, hop_dir, bounds in itertools.product(path_dirs, ["out", "in"], [(1, 2), (0, 2)]):
        for order in ("hop+path", "path+hop"):
            path = (path_dir, *bounds)
            segments = [(hop_dir, 1, 1), path] if order == "hop+path" else [path, (hop_dir, 1, 1)]
            pattern = f"(n0:{label})"
            for i, (d, lo, hi) in enumerate(segments):
                pattern += _arrow(d, rel, lo, hi) + f"(n{i + 1}:{label})"
            got = int(_rows(schema, f"MATCH {pattern} RETURN count(*) AS n")[0]["n"])
            assert got == _count(graph, segments), f"{schema} {pattern}"
