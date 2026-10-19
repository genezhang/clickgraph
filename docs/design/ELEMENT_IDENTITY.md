# Element identity: spell a node's and a relationship's identity once

Status: **accepted** (2026-10-19; decisions §2). Owner entry: `PRIORITIES.md` P-4d,
cluster 2. Origin: the 2026-10-18 open-issue analysis; a site-by-site fix
of #1358 (VALUE_TYPES.md T3) was parked after review because identity is
spelled at too many sites for one of them to be fixed alone.

## 0. The problem

A node is its label and its id; a relationship is its type and its
`edge_id`, else its stored ends (user, #1356). ClickGraph spells that
identity in **eight forms**, chosen per site:

| Form | Spelling | Where |
|---|---|---|
| F1 typed columns | the id columns, compared part by part (`=`, OR of `<>`) | ties of a relationship to its nodes, OPTIONAL / subquery correlation, edge-constraint joins, embedded nodes, `a = b`, DISTINCT / ORDER BY of elements |
| F2 typed tuple | `tuple(c1, c2)` | `count(DISTINCT n)`, members subqueries, list elements, `path_edges` |
| F3 unlabeled text | `toString(tuple(..))` (Spark `to_json`) | composite shortest-path search keys, generator `start_id` of a composite |
| F4 labeled text | `concat(label, ':', toString(id or tuple))` | union walks, union searches, several-labels walk keys |
| F5 relationship text | `concat('TYPE:FROM:TO:', toString(ids))` | union-walk `path_edges`, `key_differs` |
| F6 element id | `Label:a\|b-`, `TYPE:from->to-` | `elementId`, graph values, Bolt, HTTP Graph output, embedded `query_graph` |
| F7 Bolt number | 6-bit label code + 47-bit raw id or `DefaultHasher` hash | Bolt `id()` |
| F8 engine sort key | `CypherUnion::order_key` / `distinct_key` | several-labels tie breaks |

Each site picks its own form and its own handling of types, composite ids,
several labels and NULL parts. The open issues are where two sites disagree:

| Divergence | Issues |
|---|---|
| Typed joins of ids whose columns have no common type (a `UInt64` id, `Int64` ends) fail; text keys cast per #1360, typed joins never | #1358 |
| A several-labels node's id is the arms' common type: a `Variant` (`'01'` reads as `1`; joins of two such Variants fail) | #1390, #1393 |
| F6 / F7 ignore `edge_id`: parallel relationships share one element id and number; HTTP Graph and embedded dedupe one away | #1334 (#1409) |
| `id(r) = N` has no decode for relationships | #1410 |
| `id()` of a composite node as a value is refused | #1387 |
| `id()` across a mixed-label UNION is refused | #1383 |
| Bolt's static id lookup ignores the session's scope | #1388 |
| A NULL id part: present for `null_test`, `'null'` in F6, an error in Rust, never equal in F1, dropped by the embedded relation, the union walk and the search | #1354 |

And spellings that agree today only by accident: F6's composite `a|b` is
ambiguous (`'a|b'` + `'c'` vs `'a'` + `'b|c'`) where F3 / F4 are not; the
trailing `-` sentinel is stripped with `trim_end_matches`, so ids `a-` and
`a` collide; Bolt label codes clamp at 63 and are process-global and
first-seen ordered; `DefaultHasher` is not stable across Rust versions.

## 1. Principle

One identity value per element, with **three spellings** derived from it,
and every site reads one of them:

1. **For comparison** (F1, typed): the id parts, each in its label's
   *comparable type*, which is the one type every column holding that part
   meets in: the node's id column and every relationship end at that label
   (VALUE_TYPES.md T1 types; cast only where the backend has no common type,
   `common_type_where_none`). Joins, correlations, constraint joins,
   unions of ends and `a = b` all compare this spelling, so they agree by
   construction. Unknown types: the columns as today.
2. **As a key** (F4 / F5, text): one canonical text, `label`, a separator and
   the parts' unambiguous text (`toString(tuple(..))` of the comparable
   parts), used wherever a key must be one column: walks, searches, path
   edges, several-labels nodes, DISTINCT of elements of several labels.
   It replaces F3, F4, F5 and the several-labels `Variant` key.
3. **For output** (F6 / F7): the element id derived from the key text, with
   `edge_id` in a relationship's (#1334), unambiguous for composite ids, and
   parsed by the same module that builds it (Rust and SQL from one
   definition). The Bolt number is a scoped mapping of it (#1388) with a
   relationship decode (#1410).

A site never spells identity by hand: it asks the identity of a variable for
one of the three.

## 2. Decisions (user, 2026-10-19)

- **A NULL id part (#1354): not a node.** A row whose id has a NULL part is
  dropped from node scans too, as the embedded relation, union walks and
  searches already drop it. (Neo4j has no NULL ids; its loader skips
  relationships with a NULL end.) Ties stay plain `=`.
- **Element-id format: may change.** Composite ids and relationship ids that
  carry an `edge_id` get an unambiguous encoding; single-column node ids keep
  today's text.

## 3. Slices

- [x] **I1 — the comparable spelling** (#1358). One comparable type per
  (label, id part): the fold of the node's id part type and every
  relationship end at that label in the schema, cast only where the backend
  has no common type (`SqlDialect::common_type_where_none`, VALUE_TYPES.md
  T4a; `Lowerer::comparable_targets` / `comparable`). An integer part is
  folded across every label's integer parts, since unions mix labels (a
  node of several labels, a walk over them). Every producer of an identity part for comparison casts to it, so
  every comparison agrees by construction (the parked attempt folded per
  site and broke where two sites met):
  - a node's id columns where tied (`tie_end`), its label-union arm ids
    (`label_union`'s `LABEL_ID_i`, per arm label), its embedded relation's
    source ids, a union walk's node CTE ids;
  - a relationship's stored ends (`stored_ends`, then `turned_ends`' CASE),
    an undirected one's two directions (`both_directions`), a union's
    `REL_FROM` / `REL_TO` / `BOTH_START` / `BOTH_END` / stored-end
    `REL_ID` per arm label, a directed walk's edge (read through a CTE of
    its ends cast);
  - elided endpoints (`endpoint_of`) are not elided where a cast is needed:
    the node is read and tied, cast;
  - edge-constraint node joins (`constrained_rows`) cast both sides;
  - an embedded node's relation casts each source's id.
  Unknown or agreeing types, and Databricks: the SQL as before.
- [ ] **I2 — the key spelling** (#1390, #1393). One `identity_key` for walks,
  searches, path edges and several-labels nodes (whose `__cg_id_*` becomes
  the label and key text instead of a `Variant`).
- [ ] **I3 — relationship ids** (#1334, #1410). `edge_id` in the element
  id; `id(r) = N` decoded through the session's mapping.
- [ ] **I4 — Bolt ids** (#1388): scoped static lookup; stable hash; label
  codes per schema.
- [ ] **I5 — ids as values** (#1387, #1383): `id()` of a composite node and
  across a mixed-label UNION, from the output spelling.
- [ ] **I6 — NULL id parts** (#1354): not a node (§2).
