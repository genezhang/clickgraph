# Value types: give every SQL column a Cypher type

Status: **in progress** (2026-10-18). Owner entry: `PRIORITIES.md` P-4d.
Origin: the 2026-10-18 analysis of the 56 open issues, which found six
root-cause clusters. This doc covers cluster 1, the largest one.

## 0. The problem

The lowering does not know the physical type of most SQL columns it reads.
Wherever values from different sources meet in one SQL expression,
ClickHouse's common-type rule picks the type instead of Cypher's:

| Where values meet | What ClickHouse does | Issues |
|---|---|---|
| UNION ALL arms (both directions of an undirected walk, a several-label node, a node embedded in several edge tables) | no supertype (UInt64 vs Int64: Code 386), or a `Variant`, or an untyped `NULL` arm | #1358, #1390, #1393, #1430, #1490 |
| array literals, `collect` of mixed columns | `Array(Variant(...))`: equal integers become two values | #1396, #1491 |
| text keys (walk keys, the label-union key, the distinct key of containers) | `toString` differs per type for one value (Date vs DateTime, Bool vs UInt8) | #1360, #1393, #1482 |
| GROUP BY / ORDER BY / DISTINCT of a Variant | refused (Code 44) | #1362, #1491 |
| a property whose column is an `Array` / `Map` | the schema has no list type, so the property's kind is unknown | #1485, #504 |
| a whole-number float inside a `Map(String, Dynamic)` | printed with no decimal point | #1474 |
| a typed NULL predicate on Spark | `WHERE NULL` is VOID | #1414 |

Each was patched, or left open, at the site that failed. #1498 (a
contributor's) did the first structural slice: walk keys cast each key part to
the least common type of its (node, edge) pair. It only works when the schema
sets `auto_discover_columns`, because only those elements carry column types.

## 1. Principle

1. **The catalog knows every column's physical type**, wherever it can be
   known (§2). An unknown type keeps today's SQL exactly.
2. **Values meet in a type chosen by Cypher, not by the backend.** At every
   place values meet, each side is cast to one target computed from the
   sides' known types (`KeyPartType::widen_to` / `fold_common`, extended).
   No site picks its own rule.
3. **Mixed Cypher kinds stay mixed, but are compared by one key.** When the
   sides really are different Cypher kinds (a string and an integer in one
   list), the value is a Variant, and equality, DISTINCT, grouping and
   ordering go through one recursive (kind, value) key. The ClickHouse scalar
   `distinct_key` and the Databricks `variant_key` already work this way;
   containers still key by `toString` (#1482).

## 2. Where column types come from (user decision, 2026-10-18)

- **Discovery at schema load, for every table**, wherever a connection
  exists: the server (`system.columns` through its client), remote and
  `cg --clickhouse` (`system.columns`, one read for every table, within 5 s),
  embedded chdb (`DESCRIBE` of a `source:`; a table chdb creates from the
  schema is not read). A table that cannot be read gets no types, and that is
  not an error: the library reports it (`Database::load_warnings`, printed by
  `cg`; not yet exposed through the FFI bindings). Discovery of *types* does not discover *properties*:
  `auto_discover_columns` alone still decides which columns are properties
  (the undeclared-property rule is unchanged).
- **Declared types win**: a `column_types:` map on a node or edge definition
  (column → ClickHouse type spelling, e.g. `DateTime64(3)`, `Array(String)`),
  checked at load: a type spelling, and, where the table's columns were read,
  one of them.
- **Freeze**: a `cg schema` option writes the discovered types into the
  YAML's `column_types`, for deployments that want SQL that depends on the
  YAML alone.
- Accepted consequence: `cg sql` with no connection, on a YAML with no
  `column_types`, emits the SQL of an unknown type (today's), which can differ
  from the server's.
- Databricks: no discovery yet (its types are Spark's). Declared
  `column_types` apply there too, spelled as ClickHouse types.
- The id of a node embedded in edge tables (`from_node_properties` /
  `to_node_properties`) has one column per role, so it keeps its types as
  before T1 (only `auto_discover_columns`); its edges' end columns get theirs.

## 3. Slices

Each slice is one PR, keeps the golden net byte-identical unless it says why,
and is verified live against ClickHouse and the Neo4j oracle.

- [x] **T1 — catalog column types** (PR #1503). `column_types` in the YAML; discovery of
  every table's types in the server, remote, `cg` and chdb loaders (non-fatal);
  `NodeIdSchema.part_types` and the edges' `*_node_id_part_types` filled for
  declared elements too, which completes #1498 for declared schemas (#1360
  item 1). `node_id.dtype` (Bolt `id()` decoding) is unchanged.
- [ ] **T1b — freeze.** `cg schema` writes discovered `column_types` into the
  YAML.
- [x] **T2a — array properties are lists** (#1485). Nodes and
  relationships keep their table's column types (`column_types`); a property
  not declared typed whose column is an `Array(T)` in every label / type of
  its element is a list of `T`'s kind (`Lowerer::column_list_kind`), and such
  a property is NULL where an OPTIONAL MATCH left its element NULL
  (`Lowerer::list_null`), so `IS NULL`, `size()`, `coalesce()`, `count()`,
  `collect()` and `+` read it as Neo4j does. Scalar columns keep their
  unknown kind: what knowing it would change (#1419's UNWIND of a scalar
  property, booleans) is a later slice. Not covered (they read `[]` as on
  main): such a property inside a map or list literal, a CASE or a
  `coalesce()` with a non-list, and a node from `UNWIND [q]` (its binding is
  not nullable).
- [ ] **T2c — map columns.** A property whose column is a `Map(K, V)` as a
  map of `V`'s kind (its keys are the rows', not the schema's).
- [ ] **T2b — the absent side of an embedded node's union** (#1430, #1490):
  the sources that do not hold a property spell a NULL of its column's type.
  Prerequisite for #504 (ARRAY JOIN of an array-valued edge column).
- [ ] **T3 — node identities meet in one comparable type** (#1358, #1390,
  #1393). A first, site-by-site attempt (branch
  `fix/1358-undirected-mixed-signedness`: cast ids of a `UInt64` / signed
  pair to `Int128` where a tie, a union of ends or a walk meets them) was
  parked after review: it broke undirected relationships whose end can be
  several labels, and missed the sites that compare identities on their own
  (OPTIONAL / pattern-predicate correlation, edge-constraint joins,
  denormalized walks, mixed-id walks). Every one of them compares a node's
  identity columns, so the fix is one spelling of an identity for comparison
  (its key parts in the label's comparable type) next to its spelling for
  output: cluster 2, element identity. The several-labels key (`'01'` read as
  `1`, a `Variant`) is the same representation.
- [ ] **T4 — one value key.** Equality, DISTINCT, grouping and ordering of a
  value of mixed or container kind go through one recursive key on ClickHouse
  (#1396, #1482, #1491, #1362), with a sortable form for ORDER BY.
- [ ] **T5 — output fidelity.** A float keeps its decimal point inside a graph
  value (#1474); typed NULL predicates on Databricks (#1414); embedded writes
  create typed columns (#1436, decision pending there).

## 4. Out of scope here

- Element identity (cluster 2: #1334, #1410, #1387, #1383, #1388, #1354) uses
  T1/T3's canonical key parts, but is its own design.
- Lists of nodes in two physical forms (cluster 5: #1362, #1378, #1380,
  #1425) depends on this and on cluster 2.
- Three-valued list equality (#1478) and the temporal constructors and output
  (#1487, #1068) are spelling gaps in `FunctionMapper`, not typing.
