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
| `id()` of a composite node as a value is refused (I5a) | #1387 |
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
- **Revised 2026-10-24 (user): a composite id with a NULL part is a node.**
  Asked whether "not a node" loses facts, the user chose to keep them: NULL
  stays NULL in the data, its element id writes the part `\N` (escaped as
  an `edge_id`'s parts are, I3a) and reads back as NULL. Identity is
  compared NULL-safely (an element-id lookup's `\N` is `IS NULL`; `n = m`,
  DISTINCT and a walk's visited nodes treat a node as itself), while a
  relationship's end still joins its node with `=`, so no relationship
  attaches to such a node (as in Neo4j, where `MATCH (m {b: null})` matches
  nothing). A single-column NULL id, or a composite one that is all NULL,
  stays no node: every such row would share one id. A relationship whose
  `edge_id` has a NULL part is kept (in Neo4j only a missing property).
  Rejected: a random number for NULL (not stable; Browser reads a node
  again by its id) and the text `null` (a real string `'null'`).

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
  Survey 2026-10-24: #1390 was closed by I1, and the key texts are already
  built by shared helpers (`identity_text`, `rel_key`, `walk_key_sql`,
  `composite_key`, I1's `cast_key_part`). What is left is folding
  `composite_key` and the walk generator's `emit_id_expr` into
  `identity_text`: a tidy with no failing query behind it, so parked behind
  I5b, #1528 and I4b.
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
- [x] **I3c — unions of edge ids of several kinds.** A union's identity
  column (`REL_ID_{i}`) whose arms' values are of several kinds (a string
  `edge_id` with an integer one, or with a definition's integer ends; a
  `Date` with a number) was a `Variant`, which GROUP BY refuses (Code 44:
  `DISTINCT r`, `WITH DISTINCT r`, grouping by `r`) and a walk's recursive
  union has no type for (Code 386), and in which `count(DISTINCT r)` read a
  string `'1'` as the integer `1` (silently 3 for 4). It is now held as
  each arm's text (`rel_id_texts`, by `SqlDialect::ids_held_as_text` of
  `identity_part_types`); integers of no common type (`UInt64` with a
  signed one) meet in `Int128` (`common_type_where_none`). Two definitions'
  relationships are never one, so no arm's text needs to meet another's.
  A union held in a type compared with one held as texts reads each
  definition's part back in its own type before its text
  (`union_part_text`; a `Float32` as a `Float64` spells other digits:
  review, also wrong on main against a union of mixed labels). A walk over
  one definition reads such a union's part back in its own type, on that
  definition's rows (`walked_identity_part`), before `has(path_edges, …)`.
  Left (other clusters): `collect()` of relationships of several types
  (lists, cluster 5), a node of several labels whose same-named properties
  differ in type (VALUE_TYPES).
- [x] **I3b — `id(r) = N`** (#1410). On the server an integer compared
  with a relationship's `id()` (`=`, `<>`, `[NOT] IN` a list) is decoded
  through the session's mapping (`IdLookup::element`, `IdMapper::
  get_element_id`; Bolt now assigns `RETURN id(r)` numbers in it, as it
  does a returned relationship's), and the relationship's element id as its
  value spells it (`Lowerer::rel_element_id_sql`, of one definition or a
  union of them) is compared with it; an id of another type, or one never
  given, names none. `RETURN id(r)` of a union of definitions is lowered
  too. Such SQL holds only while the ids are known
  (`ReadTranslation::session_ids`): HTTP does not cache it (review; a
  node's `id(n) = N` alike, which a cache answered stale on main), and the
  comparisons are `=` / OR of `=` (a RETURN item of one a boolean). Left: Bolt's `elementId(r) IN $ids` rewrite still drops a
  relationship's predicate (Browser's dedup), and the library has no number
  to decode (refused).
- [x] **I4a — scoped lookup** (#1388). An `id()` number is read through
  the session's mapper only, which reads ids given in its scope: its graph
  (the default one where it names none: Bolt without a database, HTTP
  without a schema) and its tenant and view parameter values
  (`set_scope_with_views`; Bolt per RUN, HTTP per request). Each cached id
  carries the scope it was given in, so a session that changes scope keeps
  the ids of the one it left. `IdMapper::decode_for_query` no longer reads
  every session's cache (bit pattern alone), the unused Bolt parameter
  decoder that did is gone, and exports (COPY TO, apoc export) read through
  the caller's mapper. Two names of one graph are two scopes.
- [x] **I6b — composite node element ids escaped.** A composite id's text
  in its element id (`element_id::node_id_text`) escapes each part as an
  `edge_id`'s (`\`, `:`, `|` as `\\`, `\:`, `\|`; a NULL part `\N`), in
  Rust and in SQL alike (`value::element_id_sql` is `edge_id_sql` for several
  parts), so `('a|b', 'c')` and `('a', 'b|c')` are two ids and NULL is not
  the text `'null'`. Reading one back is by the label's number of parts
  (`node_id_parts`: one column's id is its whole text): `id(n) = N`'s lookup
  compares each part, a `\N` one `IS NULL`. `parse_node_element_id` returns
  the id's text and strips one trailing `-`. Bolt's node, relationship ends
  (now texts, not split on `|` and joined again) and embedded `query_graph`
  spell it through it, a composite id's NULL part as `\N`. Single-column
  ids keep their text.
- [x] **I6c — a composite id with a NULL part is a node** (§2, revised
  2026-10-24). `id_present` filters a row only where its id is all NULL
  (every part's column may be NULL: `IS NOT NULL` of the one, or of any),
  so a single-column NULL id stays no node. Where a node is compared as
  itself, a part its column type says may be NULL is compared NULL-safely
  (`Lowerer::identity_nullable` / `elem_nullable`, `same_part` /
  `same_identity`: `FunctionMapper::null_safe_equal`, a join key, in a
  boolean `… AND true`): `a = b` / `a <> b` (one label, several labels, a
  list's elements, NULL where an element is), `x IN list` (NULL where `x`
  is: every part of a composite id NULL), the correlation of OPTIONAL
  MATCH, subqueries and drives to the node's own rows, and a walk's
  `start_*` / `end_*` columns to its end nodes (read from their rows:
  `Lowerer::tie_identity`). A relationship's ends still tie with `=`
  (`tie_end`, the walk's recursion, an elided endpoint's correlation), so
  no relationship attaches to such a node, as in Neo4j. Text keys (walk
  keys, search keys) spell NULL in their tuple's text and are unchanged.
  Live: two batteries of 75 queries against Neo4j holding the same graph
  (one node per id, relationships whose ends are whole ids) agree on every
  identity shape; Bolt returns `L:x|\N-` and reads it back.
  A walk from earlier rows (`restriction`) keys such an id by its tuple's
  text (a tuple holding NULL is `IN` no rows); `id(n) = N` on the server
  reads a nullable part `coalesce(k = 'v', false)` (false, not NULL, where
  the node's is NULL). Consequences of "no relationship attaches": an
  embedded (denormalized) node exists only as its edges' end, so one whose
  id has a NULL part is none; on an FK-edge table (node table = edge table)
  the row is a node but its edge, from its own id, is no relationship.
  I6d (below) does the same for relationships.
- [x] **I6d — a relationship whose `edge_id` has a NULL part** (#1354
  comment; §2: kept, as in Neo4j a missing property). Where an `edge_id`
  part's column type says it may be NULL, a relationship is identified by
  its stored ends and then its `edge_id` (`RelationshipSchema::
  identity_columns` / `identity_part` / `edge_id_nullable`), as its element
  id `TYPE:\N:from->to-` tells it apart; the ends first, so its first
  identity column is NULL only where an OPTIONAL MATCH left it so
  (`count(r)`, `r IS NULL`). Every identity site reads it: `scan_identity`,
  a walk's `path_edges` (`path::edge_identity`, the generator's), union
  shapes and their `REL_ID_i` typing and texts (`rel_union_shape`,
  `identity_part_types`, `rel_id_texts`, `walked_identity_part`,
  `definition_identity`, `union_part_text`, `union_edge_text` reading the
  parts after the ends), list tuples (`Layout::identity_slots`; the element
  id's text from `edge_slots`), and the `edge_id` parts are compared
  NULL-safely (`identity_nullable` / `elem_nullable` of a relationship,
  uniqueness of one MATCH's relationships, and a union's identity past its
  definition: `union_differs`, `defined_identity_comparison`, two-valued);
  joined on its properties too where read again (`drive_correlation`'s
  `by_ends`), and its properties in a re-read's sort keys (`tie_breaks`), as
  a relationship of ends is. Its element id spells the
  `edge_id` alone. Live: 41 queries against Neo4j holding the same
  relationships agree but for two that are not identity (a union walk's
  `relationships(p)` comprehension, refused on main too; a pattern
  comprehension's list order); was 16 wrong (`count(r)` 3 for 7, two-hop
  patterns 2 for 12). Two relationships with one NULL `edge_id` and the
  same ends are one (as their element ids are): a known discrepancy with
  Neo4j (user, 2026-10-24; `docs/wiki/Known-Limitations.md` §4).
- [ ] **I4b — Bolt ids**: stable hash (not `DefaultHasher`); label codes
  per schema (process-global, clamped at 63, first-seen ordered).
- [x] **I5a — `id()` of a composite node** (#1387, #1531). `id(n)` of a node whose
  id has several columns, as a RETURN item or a value (WITH, ORDER BY,
  DISTINCT, `collect`), is the id's text as its element id spells it
  (`Lowerer::composite_id_value`: `value::element_id_sql` of the parts,
  escaped and joined, a NULL part `\N`), NULL where an OPTIONAL MATCH found no
  node; it was refused. HTTP and the library return the text (a node's `id()`
  is its key, S11l). Bolt numbers it from the element id, as it does a
  single-column node's (`ReturnItemType::IdFunction`), so `id(n)` is the id
  of the node returned whole and `WHERE id(n) = N` finds it (I6b). Review:
  the text compares only with another `id()` (`id(a) = id(b)`) or an integer
  literal the server decodes; `IN [$n]`, `<> $n`, `> 3` or a string are
  refused (`Lowerer::composite_id_item`; also an item of a WITH or of the
  RETURN that is the id or passes it on, `Lowerer::composite_id_vars`: its
  ORDER BY reads the alias), as a number matches no text and a text of one
  label equals another's. The number of a hashed key (a composite or string
  key, `IdMapper::is_hashed`) is assigned in the Bolt session when
  `RETURN id(n)` gives it (`get_or_assign`), as a returned node's is, so
  `WHERE id(n) = N` reads it back though the node was never returned whole
  (also for a string key, on main alike). Left: the id as a value is the
  key's text, which compares as text with text and which two labels can
  share (as a single-column key, S11l): wrapped in a map, a list, a CASE or
  a function (`{k: id(a)}`, `collect(id(a))`, `toString(id(a))`, `max(id(a))`)
  and as a pattern's property value (`(b {bank: id(a)})`) it is not
  guarded. An integer key of 2^31 to 2^47 is found by `id()` only after its
  node was returned whole (`decode_for_query` reads smaller bits only; on
  main alike); a Decimal part of 16 or more digits, a Float or a
  FixedString has another text in Rust and SQL (#1528).
- [x] **I5b — `id()` across labels** (#1383): `id()` of a node of several
  labels (`MATCH (n) RETURN id(n)`) and of a UNION column whose arms are
  nodes of different labels was refused: the key alone is ambiguous (a Post
  and a User share `1`), and Bolt needs each row's label. Now (user
  2026-10-24, "follow your recommendations") it is a relationship's `id()`:
  the node's element id (`Lowerer::node_element_id_sql`: the label column,
  `:`, the id as `element_id_sql` spells it, `-`), returned as the server's
  number for it (`ResultKind::ElementIdNumber`, formerly `RelId`), on HTTP
  and in the library as over Bolt, where it is the `id` of the node
  returned whole; a number that holds its key's bits (an integer key) is not
  kept in the session, a hash is (`IdMapper::is_hashed`), so
  `WHERE id(n) = N` reads either back. A UNION column of `id()`s of
  different labels (or of a relationship) returns the element id in every
  arm, under UNION and UNION ALL (`mixed_ids`, `Query::as_values`). Other
  uses of the value are refused, as a relationship's: `WITH id(n)`,
  `collect(id(n))`, `count(DISTINCT id(n))`, comparisons with a number but
  `id(n) = N`; `ORDER BY` the item sorts by the node's identity (label, key)
  as `ORDER BY id(n)` does, and is refused where the rows are grouped or
  distinct by it. The id of a node of one label stays its key on HTTP.
- [x] **I6 — NULL id parts** (#1354): not a node (§2; revised by I6c). Every scan of a
  label (a node of one label, each arm of a node of several, a walk's
  nodes and its node relation, OPTIONAL MATCH) reads a row as a node only
  where its id's parts its columns' types say may be NULL are not
  (`Lowerer::id_present`, beside the schema `filter:`): the node scans now
  agree with walks, searches and the embedded relation, which dropped it
  already. Unknown types, or no nullable part: the SQL as before. Left: a
  relationship whose `edge_id` (or an elided end) has a NULL part (#1354
  comment).
