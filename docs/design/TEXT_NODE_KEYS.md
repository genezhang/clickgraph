# Spec: Text node keys cast edge-end parts to node types (#1360)

Status: DRAFT — design only, implementation deferred.
Owner: hari2353

## Problem

Composite/multi-definition walks identify nodes by a text key:
- S7b3b (union search): `concat(label, ':', toString(id))`
- S8d2a (composite shortest path): `toString(tuple(c1, c2))`
- S8d2b (multi-definition walk): `concat(label, ':', toString(tuple(c1, c2)))`

When a node's id column and the relationship's end column hold the same
value under different types (Date vs DateTime, Bool vs UInt8, FixedString vs
String), ClickHouse's `=` matches but their texts differ, so the
relationship silently relates nothing.

## Fix direction

Cast each edge end part to its node column's type before keying, reusing
the `cast_key_part` machinery (ELEMENT_IDENTITY.md I1, `path_key_targets`).

For single-column ids, the BFS joins typed columns (no text key). For
composite ids, the cast targets are already computed by `path_key_targets`.
The text key paths need to apply those casts.

## Affected code

- `value::node_key_text` (value.rs:205) — takes a single `id: &str`. Need to
  accept per-part casts.
- `value::rel_key` (value.rs:190) — takes `ids: &[String]`. Need casts.
- `mod.rs:5774` — `value::node_key_text(&spelling, &label, &id)` (union walk)
- `mod.rs:3950` — `value::rel_key(g, ...)` (single rel text)
- `mod.rs:6677` — `value::rel_key(g, ...)` (union walk)

## Design decision needed

Option A: change `node_key_text`/`rel_key` to accept cast targets and apply
`cast_key_part` per part. Requires changing all callers to compute and pass
targets.

Option B: compare by columns where the relation allows (as S8d1 walks do).
This avoids text keys entirely but requires restructuring the union walk.

## Effort

M/L — touches walk_key, union walk keys, path search keys, and value.rs.
Needs careful testing on real ClickHouse + Neo4j oracle.

## Success criteria

1. `MATCH (a:Day {nm:'A1'})-[:E1|E2*1..3]->(b) RETURN b.nm, count(*)`
   returns rows (currently 0).
2. Date vs DateTime, Bool vs UInt8, FixedString vs String all match.
3. Corpus goldens byte-identical or justified regen.
