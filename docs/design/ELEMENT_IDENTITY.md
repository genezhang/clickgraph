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
   `common_type_where_none`). Joins, constraint joins and unions of ends
   compare this spelling, so they agree by construction (`a = b` and
   correlations compare one label's raw columns, or label unions' cast ones,
   on both sides). Unknown types: the columns as today.
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
  T4a; `Lowerer::comparable_targets` / `comparable`). In a column that
  mixes labels (a node of several labels, a walk over them, a union of
  relationships between them) an integer part is folded across every
  label's integer parts; elsewhere the label's own fold is used, so a label
  whose ids and ends agree keeps its SQL (a cast join key keeps ClickHouse
  from pushing a filter into the other table's key). Also closes #1390.
  Every producer of an identity part for comparison casts to it, so every
  comparison agrees (a label's own fold where it meets its ends, the
  cross-label one in a column that mixes labels; the two meet in
  ClickHouse's integer supertypes, `Int128` with any integer of at most 64
  bits, and a wider id is never cast). The parked attempt folded per site
  and broke where two sites met:
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
- [x] **I2a — ids of several kinds as texts** (#1393). Where the schema's
  labels hold an id part in values of several kinds (an integer id and a
  string one, a `Date` and a number: `schema_types::of_several_kinds`,
  `SqlDialect::ids_held_as_text`, ClickHouse only), a column that mixes
  labels holds that part as a text instead of a `Variant` (in which a string
  `'01'` reads as `1`): `Lowerer::held_as_text`, decided over every label as
  I1's cross-label fold is, so any two such columns agree. Each label spells
  its text as its ids and ends spell theirs (`part_text`,
  `schema_types::text_spelling`): the #1360 lattice (a `Date` id with
  `DateTime` ends spells the `DateTime`); times of several zones, or a zoned
  time with a `Date`, as instants in one zone (`DateTime64(s, zone)`, which
  walk keys now spell too); a `Date` with times of several zones
  `DayOrTime` (`FunctionMapper::day_or_time_text`: its text in its own zone with a
  midnight time of day dropped, every year a `Date32` / `DateTime64` holds),
  as `=` reads the `Date` at midnight in each time's zone; walk keys spell it
  too (`value::DAY_OR_TIME`). The same on its node's and its ends' side,
  nullable (a typed side may be an OPTIONAL MATCH's NULL). A union of
  relationships, and a walk, between nodes of one such label hold their ids
  as texts too (`union_holds_texts`, `walk_holds_texts`: typed, a column of
  them took one zone and matched the wrong nodes, also on main).
  - Producers: every `compared_as(.., across)` (several-labels nodes,
    union-walk nodes and their relationship unions' ends), a relationship
    between two labels read in both orientations (`turned_ends`), and a
    union's identity columns where some arm's stored end is a text
    (`rel_id_texts`; an `edge_id` there too).
  - Meetings with a typed side spell it as a text (`as_held`): ties of such a
    column to one label's ends and of one label's node to such ends
    (`tie_end`, `ends_mix_labels`), `a = b` of a node of one label and one of
    several, a walk's restriction rows, and a relationship compared with or
    distinct from a union of them (`definition_identity`).
  - Read back in each row's own type where its text was spelled in another
    (`own_texts`, `own_end_text`, `walk_key`): element ids (a node's is a node
    of one label's, `P:2020-01-01-`), `id()` lookups, a walk's search keys,
    embedded write targets.
  - Ids of one kind, unknown types, Databricks: the SQL as before, but for
    walk keys over times of several zones (instants of one zone, where each
    zone's wall clock matched nothing).
- [ ] **I2b — one key spelling** (#1390). One `identity_key` for walks,
  searches and path edges (F3 / F4 / F5), from the comparable spelling.
- [x] **I3a — relationship element ids** (#1334). A relationship whose
  schema declares an `edge_id` carries it: `TYPE:edge_id:from->to-`
  (`element_id::relationship_element_id`), its parts' texts escaped (`\`,
  `:`, `|` as `\\`, `\:`, `\|`, a NULL part `\N`) and joined by `|`
  (`edge_id_text`), so no two edge ids spell one text: it is read up to its
  first unescaped `:` (`parse_edge_element_id`), placed before the ends,
  whose texts are not escaped. Without one, `TYPE:from->to-` as before. The
  SQL spells it, once (`value::edge_id_sql`, `FunctionMapper::replace_text`;
  a NULL part tested `IS NULL`, as a union's `Variant` of NULL is the text
  `''`; in a union per definition, `Lowerer::union_edge_text`, each part
  read back in its own type where the union holds days and times, or floats,
  in a wider one, `own_identity_part`). A returned relationship carries it
  as a column (`name.elementId`, key `REL_ELEMENT_ID_KEY`, after its
  properties, so no property's column moves), which Bolt, the HTTP graph
  output and embedded `query_graph` (now through `transform_to_relationship`,
  so a composite end whole) take as is: no second spelling to disagree (a
  float's text, a property named `edge_id`: review). A relationship's ends,
  in its element id, `startNodeElementId` / `endNodeElementId` and its
  returned `from_id` / `to_id`, are spelled as their nodes spell their ids
  (`schema_types::end_spelling`, `Lowerer::end_spellings`): a time end of a
  `Date` node as `DayOrTime`, a day or time end of a time node of another
  zone or a coarser scale as an instant of the node's type, a finer one as
  the node's text where its value in the node's type is it, else its own;
  one to one, so a returned end still keys its rows (`ElementColumns`'
  spelled identity). Left (on main alike): end labels are not in the element
  id (two definitions of one type can share one), properties named
  `from_id` / `to_id` collide with the ends' columns, walks and DISTINCT over
  unions of edge ids of several kinds fail (`Variant`, I3c).
- [ ] **I3c — unions of edge ids of several kinds**: walks over them fail
  (Code 386) and DISTINCT of them (Code 44, a `Variant` in GROUP BY); hold
  such identity columns as texts, as I2a holds ends.
- [x] **I3b — `id(r) = N`** (#1410). On the server an integer compared
  with a relationship's `id()` (`=`, `<>`, `[NOT] IN` a list) is decoded
  through the session's mapping (`IdLookup::element`, `IdMapper::
  get_element_id`; Bolt now assigns `RETURN id(r)` numbers in it, as it
  does a returned relationship's), and the relationship's element id as its
  value spells it (`Lowerer::rel_element_id_sql`, of one definition or a
  union of them) is compared with it; an id of another type, or one never
  given, names none. `RETURN id(r)` of a union of definitions is lowered
  too. Left: Bolt's `elementId(r) IN $ids` rewrite still drops a
  relationship's predicate (Browser's dedup), and the library has no number
  to decode (refused).
- [ ] **I4 — Bolt ids** (#1388): scoped static lookup; stable hash; label
  codes per schema.
- [ ] **I5 — ids as values** (#1387, #1383): `id()` of a composite node and
  across a mixed-label UNION, from the output spelling.
- [ ] **I6 — NULL id parts** (#1354): not a node (§2).
