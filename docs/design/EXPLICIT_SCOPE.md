# Explicit scope: bind every name once, then lower clause by clause

Status: **proposed** (2026-10-06). Owner entry: `PRIORITIES.md` P-4c.
It has had one adversarial review: 3 blocking and 10 important findings,
all folded in. Two of them were live silent bugs in today's engine, filed as
#1311 and #1312.
Supersedes the open parts of P-4b (`WITH_EXPORT_CONTRACT.md`) roots B and C.
The open P-4 slices (`FORWARD_RESOLUTION_PLAN.md`) and
`VLP_ENDPOINT_RESOLUTION.md` are subsumed by §4.

## 0. TL;DR

ClickGraph has no object that says which variables are in scope at a point in
a query, what each one is, or which SQL columns hold it. Scope is implied in
several ways:
- by alias names in about a dozen maps that cover the whole query;
- by the nesting of the AST and the `LogicalPlan`;
- by the order the analyzer passes run in.

After planning, all scopes are merged into one flat map by name. Rendering
then puts every clause into a single FROM/JOIN list, and **48 passes** patch
that list after it is built. Most WITH, OPTIONAL MATCH and path bugs come from
two things: a name meaning different variables in different parts of the
query, and a clause boundary (a WITH, or an OPTIONAL MATCH's WHERE) that no
data structure records.

This design adds a stage between parsing and SQL rendering:

```
parse → clause list → BIND → bound plan → LOWER → RenderPlan → SQL
                       │                    │
          names resolved once,      one rule per operator; no pass
          against an explicit       edits SQL after it is assembled
          scope per clause
```

- **Bind.** Resolve every variable name exactly once, against an explicit
  `Scope` belonging to the clause where the name appears. After binding, no
  code looks anything up by name. A WITH creates a new scope. A name re-bound
  in a later query part is a different variable (`VarId`).
- **Bound plan.** A small relational algebra. Every operator has an output
  `Scope` that lists its bindings and the columns that hold each one.
  - An OPTIONAL MATCH is **one** operator, whose WHERE sits inside it.
  - A variable-length path is an ordinary relation with start, end and
    edge-identity columns.
  - The direction alternatives of an undirected edge, and the label
    alternatives of a polymorphic edge, are a union **inside that element**,
    not a union around the whole query.
- **Lower.** Each operator becomes SQL by a fixed rule. The rules reuse the
  existing layout-aware generators: `PatternSchemaContext`, the recursive
  path CTE generator, property mapping, schema filters and the dialect
  emitter. Pushing a filter down is a separate optimization, allowed only
  when a legality rule over the explicit scope says it is safe.

**Evidence that this fixes the open issues and is not a rewrite for its own
sake (§1.3).** For six open, silently wrong issues I hand-wrote the SQL this
design produces and ran it on the `test_integration` fixture. All six match
**Neo4j 5.26 loaded with the same graph**. The current engine is wrong on all
six.

**Migration (§7).** The new path is built alongside the old one. A query uses
it only when every construct in it is supported; otherwise it falls back to
today's pipeline. Each slice is accepted on **results compared with Neo4j**
over the corpus on every layout. SQL text is not the acceptance test. The 48
repair passes and the name-keyed maps are deleted as coverage reaches 100%.

## 1. Evidence

### 1.1 Issue flow

From 2026-09-28 to 2026-10-06, 74 issues were filed and 53 closed. The last
round filed 8 and fixed 4. Every one of those 8 was already wrong on `main`.
They were found by reviews of fixes that made more query shapes executable.

Of the 46 open issues, about 30 fall in three groups:

| Group | Open issues |
|---|---|
| OPTIONAL MATCH | #504 #615 #1178.1 #1184.3 #1186.1 #1190 #1235 #1249 #1305 #1306 |
| Variable-length path attachment | #1203 #1210 #1300 #1302 #1307 #1310 #1177 (rest) #1178.2/.3 #1292 #1155 #1106 #840 #1007 #627 |
| WITH scope | #1189 #1149 #1160 #1263 #933 #1089 #1304 |

### 1.2 Where scope is implied today (verified against `e1241a9c`)

**Scope during planning.**
- `process_with_clause_chain` plans the part after a WITH in a child
  `PlanCtx` (`plan_builder.rs:432`).
- It then **copies the child back into the parent by name**
  (`plan_builder.rs:586-596`).
  - `insert_table_ctx` overwrites the parent's entry; `VariableRegistry::merge`
    does not.
  - So for a reused name, the table context and the typed variable can
    describe **different variables**.
- Every analyzer and optimizer pass after that sees one flat map.
- The only clause counter is `match_clause_index`, and nothing marks which
  query part a clause belongs to.

**Places where a lookup by name crosses a scope.** Items marked * were traced
by reading the code but not reproduced.

1. The flat copy-back described above.
2. `register_with_cte_references` (`inference.rs:558-633`) sets
   `cte_reference` on every exported name, and the outermost WITH wins. A fresh
   variable with the same name in a later scope inherits the CTE as its table
   (#1283).
3. `already_available` in join generation is a set of names. With
   `vlp_available`, every alias that has a CTE is in it*.
4. FilterTagging puts conjuncts into `TableCtx[name].filters`, and
   FilterIntoGraphRel injects them into **every** scan with that name, inside
   WITH bodies too (`filter_into_graph_rel.rs:520-1114`). This is #1304.
5. `optional_aliases` is keyed by name. Aliases of an OPTIONAL MATCH after a
   WITH are not copied back to the root set*.
6. CteReferencePopulator walks the whole subtree and adds a WITH's own
   exports to **its own input**.
7. CartesianJoinExtraction collects aliases from inside WITH bodies,
   including hidden ones*.
8. `projection_aliases` is never scoped: any name a WITH or UNWIND ever
   defined is treated as a projection alias everywhere.
9. VariableResolver's `ScopeContext::lookup` walks parents with no barrier.
10. `with_cte_identity`, `with_cte_labels` and `cte_scope_for_correlation`
    are scoped by "generation", but one generation spans the whole WITH
    chain. `carried_labels` is never reset.
11. `pattern_contexts`, `denormalized_node_edges`, `vlp_endpoints` and
    `denormalized_aliases` are keyed by name for the whole query
    (#1291 came from `vlp_endpoints`).

**The parser also fixes a clause order.** After a WITH the AST holds at most
one UNWIND, one MATCH, a list of OPTIONAL MATCHes and one next WITH
(`ast.rs:167-190`). As a result,
`MATCH (a) WITH a MATCH (a)-->(b) MATCH (b)-->(c) RETURN count(*)` **fails to
parse** (Neo4j: 35), and so does `WITH a UNWIND .. UNWIND ..`.

**Repairs after assembly.** The render layer receives a join list built by the
analyzer (`GraphJoinInference`, which runs *before* `FilterIntoGraphRel`).
48 passes then rewrite the assembled plan:
- 20 in WITH finalization;
- 13 in the main path;
- 9 in `plan_optimizer`;
- 6 in the emitter, plus `flatten_all_ctes`, which is printing, not repair.

Examples:
- `extract_cte_join_condition_from_filter` takes any `cte.x = y` equality in
  WHERE as a join key;
- `fix_orphan_table_aliases` adds `CROSS JOIN`s;
- `fold_optional_edge_node_join_with_predicate` recognizes one OPTIONAL shape
  in the finished SQL.

The full list with `file:line` is in Appendix A.

### 1.3 Prototype: the lowering this design produces, compared with Neo4j

Fixture: `test_integration` (30 users, 20 FOLLOWS). The hand-written SQL and
the harness are in Appendix B. Neo4j is `neo4j:5-community` 5.26.31, loaded
with the same nodes and edges.

| Issue | Query (abridged) | Neo4j | Lowered SQL | Engine today |
|---|---|---|---|---|
| #1235 | `MATCH (n0) OPTIONAL MATCH (n0)->(n1)->(n2)` | 58 | 58 | 66 |
| #1305 | `WITH c MATCH (c)-[t1]->(a)-[*1..2]->(b) OPTIONAL MATCH (b)->(d) WHERE a.user_id=2` | 272 | 272 | 125 |
| #1310 | `MATCH (z)->(a)-[*1..3]->(a)` | 14 | 14 | 77 |
| #1307 | `WITH c UNWIND [2,3] AS x MATCH (c)->(a)-[*1..2]->(b)` | 404 | 404 | 2200 |
| #1304 | `MATCH (a)->(c) WITH c MATCH (c)->(a)-[*1..2]->(b) WHERE a.user_id=2` | 55 | 55 | 33 |
| #1306 | `MATCH (c)->(a) OPTIONAL MATCH (a)-[*1..2]->(b) WHERE a.user_id=2` | 53 | 53 | 36 |

None of the lowered queries needed special-case code. Each is the same
operator rules applied to a different query:
- one join per pattern element;
- ties come from shared variables;
- uniqueness predicates over the relationships of the clause;
- WHERE goes on its own clause;
- an OPTIONAL MATCH is a LEFT JOIN to one unit.

## 2. Why the earlier refactors did not stop this

P-1 through P-4b each unified one *decision*:
- traversal (`children()`/`walk()`);
- property resolution (`VariableRegistry`, forward resolution);
- CTE management;
- path endpoint resolution;
- the WITH export contract.

Each unification was correct within its own area. But each one read state
that is keyed by name and shared across scopes (§1.2). So each needed a gate
saying where the new decision applies (`verified_chain`, `supported_chain`,
`trailing_hops_only`, `is_render_safe`). The gate is the edge of the fix, and
every shape outside it kept its old behaviour. Widening a gate brought in
shapes that hit other heuristics, and the next review found the next bug
(see the #1301 → #1303 → #1309 chain).

This design changes the **representation**: once binding is done, the wrong
answer to "which variable is `a` here?" cannot be expressed. It does not add
another layer that tries to reconcile the existing ones.

## 3. Semantics the design must implement (openCypher)

A query part runs its clauses in order over a **driving table** of records.
Each record binds the variables in scope. Each clause is a function from table
to table:

| Clause | Meaning |
|---|---|
| `MATCH P WHERE W` | For each input record, every match of `P` that agrees with the variables already bound, filtered by `W`. Relationship uniqueness holds across **all** patterns of this clause (comma parts included) and not across clauses. |
| `OPTIONAL MATCH P WHERE W` | As MATCH, but `W` decides whether a match counts. A record with no match that passes `W` is kept once, with the clause's new variables set to NULL. `W` never drops input records. |
| `WITH items` followed by `ORDER BY`, `SKIP`, `LIMIT` and `WHERE` | Projection, with aggregation if any item aggregates. The non-aggregated items are the grouping keys; an expression over an aggregate is computed after aggregating. The **output scope is exactly the projected names.** **The WITH's own modifiers come in a fixed order, ORDER BY, SKIP/OFFSET, LIMIT, WHERE, and are evaluated in that order:** the `WHERE` filters the limited rows. Neo4j returns 0 for `WITH u ORDER BY u.age LIMIT 5 WHERE u.age > 30`, where today's engine returns 5 (#1311). An ORDER BY, SKIP/OFFSET or LIMIT written out of that order, or after any other clause (`UNWIND ... AS x ORDER BY x LIMIT 2`), is a **free-standing clause** (GQL style). It can repeat, and it sees only the previous clause's output: `WITH x AS y LIMIT 2 ORDER BY x` is "Variable `x` not defined". There is no free-standing WHERE. A RETURN's modifiers are ORDER BY, SKIP, LIMIT in that order, and nothing may follow RETURN. The WITH's own `WHERE` and `ORDER BY` can use the projected aliases and also the pre-projection variables. When the WITH aggregates or is DISTINCT, a pre-projection reference is allowed only if it is a projected expression or variable (`WITH DISTINCT u.name AS n ORDER BY u.name` is accepted, `… ORDER BY u.age` is an error). Later clauses see only the projected names. |
| `UNWIND e AS x` | One record per element; `x` is a new variable. An empty list or NULL gives no records; a non-list value gives one record. |
| `RETURN` | Like WITH, and it ends the query. |
| `q1 UNION [ALL] q2` | Each arm is a complete query with its own scopes. The arms' column names must match. |
| `EXISTS {…}`, a pattern used as a predicate, `size(pattern)`, `COUNT {…}`, pattern comprehension | A subquery correlated with the current record. Its own variables are local to it. |
| List comprehension, `reduce`, quantifiers | A nested expression scope for the lambda variable, which shadows any outer variable with the same name. |

Further rules, each checked on Neo4j 5.26:

- **A reused bound node variable** means "this same node": an identity
  equality.
- **A reused bound relationship variable** means the same relationship.
  - The endpoint ties depend on the direction written:
    - after `WITH r`, `(a)-[r]-(b)` matches both orientations (40);
    - `(a)<-[r]-(b)` matches only the reversed one (20).
  - A bound relationship still takes part in the clause's uniqueness:
    `WITH r MATCH (a)-[r]-(b)-[s]-(c)` = 122, not 162.
  - `(a)-[r]->(b)-[r]->(c)` matches nothing.
- **A label on an already-bound variable** in a later MATCH is a filter.
- **A variable-length relationship `-[r*]->` binds a list** of
  relationships, which can be matched again:
  - `WITH r MATCH ()-[r*]->()`;
  - `UNWIND r AS x MATCH ()-[x]->()`.
- **An undirected pattern over a self-loop matches once**, not once per
  direction.
- **shortestPath with a WHERE:** conjuncts that depend on the path (or on
  its relationships or nodes) are part of the search, and the shortest path
  that satisfies them is returned. Picking first and filtering afterwards
  drops pairs. Today's engine does this: 27 vs 47 3-hop paths (#1312).
- **OPTIONAL MATCH NULLs:** an unmatched `r*` list, `nodes(p)` and
  `size(r)` are NULL, not `[]` or 0.

## 4. Design

### 4.1 Where it sits

```
open_cypher_parser ──► ClauseList (new, §4.2)
                         │
                         ├─(not supported by new path)─► legacy: LogicalPlan → analyzer → render
                         ▼
                      binder (new) ──► BoundPlan (new) ──► lowering (new) ──► RenderPlan ──► emitter
                         ▲                                     ▲
                 GraphSchema +                   PatternSchemaContext, VLP CTE generator,
                 PatternSchemaContext            property mapping, schema filters,
                 (label feasibility)             ViewTableRef, RenderExpr + Dialect
```

New module: `src/bound_plan/` with `scope.rs`, `binder/`, `plan.rs`,
`labels.rs`, `lower/`, `pushdown.rs` and `result_shape.rs`. It does not depend
on `query_planner::analyzer` or on the render-layer composition code
(`with_to_cte`, `join_builder`, `from_builder`, `plan_optimizer`). A ratchet
test enforces this.

### 4.2 The clause list (parser)

The parser's grammar becomes `QueryPart* FinalPart`. Each part is a
`Vec<Clause>`, where `Clause` is one of `Match`, `OptionalMatch`, `Unwind`,
`With` (projection plus its own ORDER BY, SKIP, LIMIT and WHERE),
free-standing `OrderBy` / `Skip` / `Limit`, `Return`, `Call` or `Union`, in
source order and with no fixed per-part layout. The grammar rules are listed
in §3, and each one was checked against Neo4j 5.26. It accepts the output of the AST
pre-passes that run today (the `id()` rewrite `transform_id_functions` with
`IdMapper`, and `$param` handling) unchanged. The legacy AST
(`OpenCypherQueryAst` with `subsequent_*`) is then *derived* from the clause
list whenever it can represent the query, so the legacy pipeline does not
change. A query the legacy AST cannot represent (for example two MATCHes after
a WITH) is supported only by the new path, or fails loudly if the new path
does not support it yet.

### 4.3 Scope and bindings

```rust
pub struct VarId(u32);                    // allocated by the binder, unique per query
pub struct RelId(u32);                    // one relation instance in the bound plan

pub struct Scope {                        // ordered; a name occurs at most once
    pub bindings: Vec<Binding>,
}
pub struct Binding {
    pub var: VarId,
    pub name: Option<String>,             // None for an anonymous pattern element
    pub nullable: bool,                   // introduced by OPTIONAL MATCH, or carried from one
    pub kind: BindingKind,
}
pub enum BindingKind {
    Node  { labels: LabelSet, identity: Vec<Col>, props: PropSource },
    Rel   { types: TypeSet, identity: Vec<Col>, from: Vec<Col>, to: Vec<Col>,
            props: PropSource, direction_known: bool },
    Path  { elements: Vec<PathElement> }, // nodes, fixed relationships, variable-length segments
    List  { elem: Box<BindingKind>, source: ListSource }, // `-[r*]->` relationships, collect(n),
                                          // nodes(p): element identities + the demanded property arrays
    Value { col: Col, ty: Option<SchemaType> },
}
pub struct Col { pub rel: RelId, pub column: ColumnName }   // a concrete column of a relation
pub enum PropSource {
    Columns(BTreeMap<String, Col>),       // properties materialized in a relation (a WITH CTE, a node table)
    Element(ElementAccess),               // resolved on demand through PatternSchemaContext
}
```

**Invariants:**
- I1: no code after the binder takes a variable *name* as input.
- I2: every `Col` names a relation that is present in the operator's input.
  This is checked when lowering; a violation is an internal error, never a
  guess.
- I3: a `Scope` is produced only by an operator. No code edits it as a side
  effect.

Composite identities are `Vec<Col>` throughout, and so is a denormalized
node's identity: its columns are the role columns of the edge relation that
supplies it (§4.6).

A `List` binding lets a list of entities flow through `WITH`, `UNWIND` and a
re-match (`UNWIND r AS x MATCH ()-[x]->()`) while still carrying identities.
The demand pass (§4.10) follows a reference through `collect`, `UNWIND` and
`nodes(p)`/`relationships(p)` to the property arrays it needs. One example is
`[n IN nodes(p) | n.name]`, where the path relation must export a `name`
array.

### 4.4 Binder rules

The binder walks the clause list once, keeping the *current scope*:

- **MATCH / OPTIONAL MATCH.** For each pattern variable:
  - If the name is in the current scope, it refers to that binding: an
    identity tie, plus a label filter if a label is written.
  - Otherwise allocate a new `VarId`.
  - An anonymous element gets a `VarId` with `name: None`. No `tN` aliases
    are generated for it, so there are no collisions (#1081 family).
  - WHERE expressions resolve against *the current scope plus the clause's
    new variables*.
  - Variables introduced by OPTIONAL MATCH are `nullable`.
- **WITH / RETURN.**
  - Items resolve against the current scope.
  - The **output scope is new**: one binding per item.
    - A bare variable keeps its `VarId` and kind, but its columns now point
      at the projection's output relation.
    - Any other expression is a `Value`. `WITH a.x AS a` makes `a` a Value.
      It no longer refers to the node (#1263).
  - The WITH's own `ORDER BY`, `SKIP`, `LIMIT` and `WHERE` are evaluated in
    that fixed order, so the WHERE filters the limited rows (#1311).
    - They resolve against the projected aliases plus the input scope.
    - In the plan the WITH is `Extend` (computes the aliases and keeps the
      input bindings), then `Sort`, `Skip`, `Limit`, `Filter`, then a
      dropping `Project`. This lets `WITH u.name AS n ORDER BY n, u.age`
      use both.
  - A free-standing ORDER BY / SKIP / LIMIT clause is bound against the
    previous clause's output scope only.
  - When the WITH aggregates or is DISTINCT, the modifiers sit above the
    `Aggregate`/`Distinct`. A pre-projection reference is rewritten to the
    output column of the projected expression or variable it equals
    (`WITH u.age AS a, count(*) AS c WHERE u.age > 1`). Any other
    pre-projection reference is a bind error, as in Neo4j.
  - An expression over aggregates (`RETURN u.age, u.age + count(*)`)
    becomes `Aggregate` then `Project`. Its non-aggregate parts must be
    grouping keys, otherwise it is a bind error as in Neo4j.
  - `WITH *` / `RETURN *` expand to every named binding in the current
    scope. A map projection `n{.*}` demands all of `n`'s properties.
- **UNWIND.** Adds a `Value` binding (a whole node or relationship when the
  element type is known).

  **Implemented in S7c** (`bound_plan/lower/unwind.rs`), for a list of values:
  - **Rows.** UNWIND ends a segment like a SKIP / LIMIT: the rows so far
    become a CTE whose SELECT repeats each row once per element (ClickHouse
    `ARRAY JOIN`, after the joins) and exports the scope plus the element. A
    first UNWIND reads the one-row table. The spelling comes from
    `FunctionMapper::unwind`; Databricks has none, so UNWIND stays on the
    legacy path there.
  - **What is a list** is decided from the expression (`Kind`): a list
    literal, `collect`, `range`, `split`, `keys`, a slice, a projected or
    unwound value of those. An empty list literal, NULL, or a property that
    is NULL by the schema (of an element that matches nothing) gives no rows
    (an empty relation: ClickHouse refuses `ARRAY JOIN []` and a NULL array).
    A literal, an arithmetic or comparison, or a property whose type the
    schema declares is not a list: it is the list of itself, or of nothing
    when NULL (`if(isNull(e), [], [e])`; a function, since `NOT a IS NULL`
    reads as `NOT (a IS NULL)`). A value of unknown type (a
    parameter, an undeclared property, a function the classifier does not
    know) is read as a list through `arrayConcat`, which fails the query when
    it is not one. Without that check a map would silently become one row
    per entry.
  - **Values.** A boolean element (a comparison, a declared boolean
    property) is cast to `Nullable(Bool)`: ClickHouse holds it as `UInt8`,
    which would show 1 / 0. An element that is always NULL (of `[]`,
    `[null]`) is the NULL constant: ClickHouse cannot project the element of
    an `Array(Nothing)`. `split(s, d)` of a NULL `s` or `d` is NULL in
    Cypher, but `['']` in ClickHouse: UNWIND of it gives no rows.
  - **Order.** Neo4j emits each row's elements together, in list order.
    - One input row (no MATCH yet, or an aggregation with no grouping item
      since): the rows are ordered by the element's position.
    - Rows in an order (after an ORDER BY): the rows are numbered in that
      order first (`row_number()`, one more CTE), then ordered by number,
      then by position.
    - Several rows in no order: the order is `Lost`, so a later SKIP / LIMIT
      or `collect()` that relies on it is not lowered. (S7e1 lowers
      `collect()` over ordered rows.)
  - **Not lowered:** a list of nodes, relationships or paths (`[a]`,
    `nodes(p)`, `-[r*]->`, `collect(n)`), and a property of an element
    (`m.k` of a map), both S7e; UNWIND in the Databricks dialect.
  - **Known gaps:**
    - A map literal's values are strings (`RETURN {a: 1}` on the new path
      too, from S4; fixed in S7e3).
    - Older value gaps an UNWIND makes easy to reach, the same on both
      paths: `split()` of NULL elsewhere is `['']`; a list of booleans is
      `[0]` (fixed on the new path in S7e1); `x / 2` of an integer element is a float division (`intDiv` only
      for integer constants, #847); `size()` of a string counts bytes; an
      OPTIONAL MATCH's unmatched node returned whole shows its list
      properties as `[]`.
    - Loud where Neo4j answers: a NULL inside a list of lists (Code 386), a
      parameter that is NULL, `[]` or not a list.
    - `+` of strings from an UNWIND is ClickHouse `plus` (Code 43, loud): the
      element's type is not known.
    - A shape only the clause-list parser takes, such as
      `MATCH .. UNWIND .. MATCH ..` or two UNWINDs after a WITH, still fails
      to parse on the server. The seam parses with the legacy parser first,
      until slice 10.
- **UNION.** Each arm is bound from an empty scope. Output names must match.

  **Implemented in S7d** (`bound_plan/lower/union.rs`):
  - **Arms.** Each arm is lowered on its own, continuing the CTE names, to a
    CTE of its RETURN's columns in the UNION's column order (matched by
    name). Its ORDER BY, SKIP and LIMIT stay in it, as in Neo4j, where an
    ORDER BY / LIMIT after the last arm belongs to that arm. A CTE is the
    `UNION ALL` of a SELECT of each arm's CTE; the final SELECT reads it.
  - **Values of any type.** A column of values can hold an integer in one
    arm and a boolean in another. ClickHouse's common type would convert
    one of them (the common type of `Bool` and `UInt8` is `Bool`, so `5`
    becomes `true`; that of `Bool` and `Int64` is `Int64`, so `true`
    becomes `1`). Each arm's value is therefore cast to `Dynamic`, which
    keeps every value's own type. A node, relationship, graph value or
    `id()` keeps its columns' types; it must be of one label or type in
    every arm.
  - **UNION ALL order.** Neo4j returns each arm's rows in turn, in their
    order. When an arm's rows are ordered (its RETURN's ORDER BY, or input
    still ordered as in §3: an ORDER BY not followed by a MATCH, DISTINCT
    or aggregation), the result is ordered by arm, then by the row's
    number in its arm. `row_number()` over the sort keys the arm's CTE
    exports is taken after the arm's own DISTINCT and LIMIT. Arms with no
    order are numbered 0. When no arm is ordered (a constant key orders
    nothing), the result has no order: Neo4j does not promise one.
  - **UNION (DISTINCT).** One row per group of rows equal in every column:
    - Every column is compared by `(kind, text)`, which is Cypher's
      DISTINCT equality: `1` = `1.0` = an `Int64` 1 = a `UInt8` 1, and
      `1` ≠ `'1'` ≠ `true`.
      - The kinds are NULL, boolean, number, list / map / tuple, and string.
        String covers everything else, including what the output shows as a
        string: FixedString, Enum, UUID and dates.
      - A number's text is normalized (`-0` is `0`), and a FixedString's is
        padded back to its width (`toString` drops the zero bytes the
        output shows).
      - The key is not JSON: `toJSONString` quotes a 64-bit integer under
        `output_format_json_quote_64bit_integers`.
      - Comparing through the key also lets ClickHouse group a `Dynamic`
        column.
    - Each arm also exports the identity of every node, relationship and
      graph value it returns, for two reasons. Rows equal in every returned
      column can be different relationships (an `edge_id` that is not a
      property). And a relationship's graph value carries only its ends'
      `elementId`. An element's columns are compared too: a relationship
      with no `edge_id` is identified by its ends, and parallel ones differ
      by their properties.
    - A map literal is lowered with its entries in key order, so maps
      written in different orders are equal (in DISTINCT and grouping too).
    - The result has no order (Neo4j keeps the first-seen order). Of rows
      that are equal but differ in type (`1`, `1.0`), any one is returned;
      Neo4j returns the first. Every output prints them alike.
  - **Not lowered:**
    - a column that is a node in one arm and a value, or a node of another
      label, in another (Neo4j answers; legacy errors or is wrong);
    - identities of different columns in different arms;
    - UNION in the Databricks dialect (no column type of any value).
  - **Known gaps:**
    - A map literal's values are strings (S4; fixed in S7e3).
    - A map column's equality depends on its key order. This is older, and
      the same on both paths and in DISTINCT.
    - Relationships with no `edge_id` that are parallel and equal in every
      property are one in DISTINCT, UNION and paths. This is older, and the
      same on both paths.
  **Implemented in S7e2a** (`bound_plan/lower/elements.rs`), lists of
  nodes and relationships built from the current rows:
  - **Representation.** A list's element is a tuple of its columns, of the
    types its table has: a node's id and its label's declared properties
    (in name order); a relationship's stored ends' ids, its `edge_id` (if
    any) and its type's declared properties (`Layout`). Every element of a
    list is of one label, or of one definition (type and label pair), so
    the list is an array of one tuple type (`Kind::Element`); a list (or
    `+` of lists) mixing labels, types or values is not lowered (its tuples
    would be returned raw). A property the schema declares boolean on an
    integer column is cast in the tuple, as a returned column is. A property of
    an element is its slot and keeps its type: it is compared, sorted,
    grouped and aggregated as the column is. (ClickHouse's `Dynamic`, which
    the S6c values hold, is refused by `IN`, ORDER BY, GROUP BY and `sum` /
    `max`.) A NULL element (an OPTIONAL MATCH's) has a NULL id.
  - **Lists.** `collect(n)` (`groupArrayIf` of the tuples whose id is not
    NULL; ordered as S7e1's), `[a, b]`, and `nodes(p)` / `relationships(p)`
    of a path of fixed relationships whose nodes (relationships) are of one
    label (definition) and that an OPTIONAL MATCH does not leave NULL. A
    node or relationship put in a list carries every declared property (the
    demand pass). A WITH carries the tuples (also `WITH nodes(p) AS ns`).
  - **Elements.** A comprehension's parameter and an UNWIND's variable over
    such a list are elements: `x.prop` (an undeclared property is NULL where
    the label's properties are complete or in Neo4j-compat mode, otherwise
    not lowered: the tuple has none), `x IS [NOT] NULL`, `x = y` / `x <> y`
    with an element or a node of the relation (by identity; elements of
    different labels are never equal), `x:Label`, `type(x)`, `count(x)`,
    `collect(x)`.
  - **Output.** Returned, an element or a list of them is its value (S6c's
    `{elementId, labels, properties}` form, built from the slots; a NULL
    element is NULL), grouped and made distinct by its tuples.
  - **Not lowered:** an element of a list by index, a slice, `head()` /
    `last()` (an index out of range reads a tuple of defaults, not NULL);
    ORDER BY an element (a tuple sorts by its columns);
    any other function, operator, CASE or map of elements (a tuple is never
    NULL: `coalesce` would be wrong); a list of nodes of several labels
    (`nodes(p)` of a user–post path), of several types, of paths, a NULL in
    a list of elements; a variable-length relationship's list and
    `nodes(p)` / `relationships(p)` of a path with one (S7e2b lowers those
    of walks of one definition); `labels(x)`
    (NULL of a NULL element is a NULL array); re-matching an element
    (`UNWIND l AS x MATCH (x)-->()`); composite ids (S8).
  - **Known gaps** (older, now reachable through lists):
    - Relationships with no `edge_id` that are parallel and equal in every
      property are one in DISTINCT (`collect(DISTINCT k)`, `count(DISTINCT
      k)`), and `r = s` compares their ends only.
    - A relationship's `elementId` is built from its ends, also when its
      type has an `edge_id`: over Bolt the driver merges parallel ones
      (HTTP output is right).
    - Two UNWINDs in a row, `l[0].prop` and `head(l).prop` do not parse.

  **Implemented in S7e2b** (`bound_plan/lower/path.rs`, `elements.rs`), a
  walk's nodes and relationships as lists of elements:
  - A walk of one definition (`Walked::One`, the generator's or a shortest
    path's search) carries its nodes / relationships as element tuples
    (`path_node_tuples` / `path_rel_tuples`, beside S6c's value columns)
    when the demand pass asks for them: a read inside an expression (a
    comprehension's list, `size` of a filter, `+`), an UNWIND's list, a
    WITH item. A RETURN item `nodes(p)` stays a value (no tuples). A
    recursive CTE's first rows fix its columns' types, and `[]` is
    `Array(Nothing)` (ClickHouse 25.8 cannot convert the later rows to it):
    the empty list is an aggregate of the tuple over the table reading no
    row (`(SELECT groupArray(tuple(…)) FROM t WHERE 0)`, one row also over
    none).
  - `nodes(p)` / `relationships(p)` of a path with walks of one label (one
    definition), and a variable-length relationship's list `r`, are lists of
    elements: the fixed elements' tuples and the walks' carried ones, in
    path order (reversed when the walk starts at the pattern's right end; a
    walk's first node is the node before it). A MATCH condition reading them
    is lowered against the walk's scan before the walk is built, so that
    scan carries the demand's flags too.
  - **Not lowered:** a walk of several types or labels, or whose elements
    have no tuple layout (a composite `edge_id`): it carries no tuples, and
    its S6c values still work; an
    OPTIONAL MATCH's path (its list would be NULL); a shortestPath whose
    WHERE holds a comprehension (S6b's refusal of conditions it cannot place
    in the search).
  - **Known gap (older):** a walk over relationships with no `edge_id`
    takes parallel ones for one relationship (the walk's uniqueness keys on
    the ends), so paths using both are missing (`(a)-[*1..2]-(b)` over two
    parallel edges: 10 rows, Neo4j 12; also on `main` and legacy).

  **Implemented in S7e3** (`bound_plan/lower/expr.rs`, `unwind.rs`), map
  literals as values:
  - **A map literal** is a `Map(String, Dynamic)` (`FunctionMapper::lists()
    .map_of`), as a graph value is (S6c): `map('k', CAST(v, 'Dynamic'), …)`,
    keys in order, boolean values cast; `{}` a typed empty map. Each value
    keeps its own type, so the output prints `{a: 1}` (S4 printed every value
    as a string), and maps of any keys and values are of one type: a list or
    a CASE of maps of different keys keeps each map's own keys. A map
    literal given to a temporal or spatial constructor (`duration({days:
    5})`) stays as the constructor's emitter reads it. Maps flow through
    WITH, UNWIND, `collect` and comprehensions; DISTINCT, grouping and UNION
    use S7d's DISTINCT key (`1` = `1.0` ≠ `'1'`) and `any()` of the group's.
  - `keys(m)` is `mapKeys(m)`: its keys in key order (as `main` gave them).
  - **Not lowered: reading a map** — `m.k`, `m['k']`, a map given to any
    other function. A map's value is held as a value of any type
    (`Dynamic`), and two review rounds found every way of reading it wrong
    somewhere: as named tuples (typed per key), ClickHouse compares tuples by
    position and converts their values' types (`{a: 1} = {b: 1}` was true),
    merges tuples of different keys positionally and cannot spell an
    aggregate's type; as `Dynamic`, a value compared with a column of
    another width matches nothing (on 25.8 a `UInt8` key joins no `Int64`
    row), a boolean equals a number, `count` / `groupArray` count a NULL
    `Dynamic`, and lists and maps sort as text. Reading a map needs each
    key's value carried with its own type (a later slice). Also not lowered:
    a comparison or operator over maps, ORDER BY a map,
    `collect(DISTINCT)` / `count(DISTINCT)` / `min` / `max` / `sum` / `avg`
    of maps, `size()` of a map (Neo4j refuses it), a map of nodes or
    relationships (S7e2a), a map with a repeated key. These fall back to
    legacy, which stringifies maps (as on `main`).

- **Subquery expressions.** A child scope whose parent is the current scope.
  The parent's variables used inside become the subquery's **correlation
  set**, an explicit `Vec<VarId>`.
- **Lambdas** (comprehensions, `reduce`, quantifiers). An expression-local
  scope; the binder resolves the shadowing.

  **Implemented in S7e1** (`bound_plan/lower/expr.rs`, `unwind.rs`), for
  lists of values:
  - **Comprehensions.** `[x IN l WHERE p]` keeps the elements whose
    predicate is true (not false, not NULL), and `[x IN l | e]` maps them;
    `[x IN l WHERE p | e]` is the map of the filter (the AST converter
    spells them as `arrayFilter` / `arrayMap` of a lambda, read back as
    comprehensions). The spellings come from `FunctionMapper::lists`; the
    parameter is printed by its generated name, which ClickHouse resolves
    before a SELECT alias of the same name. A comprehension of a NULL list
    (or of a property the schema makes NULL) is NULL. The parameter's kind
    is an element of the list's (`Kind`), so a comprehension is a list
    UNWIND can read, and comprehensions nest.
  - **Lists of booleans.** ClickHouse holds a comparison as `UInt8`; a
    column of it is cast by the emitter, but an element of a list was not,
    so the list showed `[1, 0]`. A boolean element (`Kind::Boolean`) of a
    list literal, a comprehension's mapped value and a collected value is
    cast to `Nullable(Bool)`.
  - **`collect()` over ordered rows.** Neo4j lists the input rows in their
    order, whatever the projection's own ORDER BY does to its output. Over
    rows in an order (§3), the rows are numbered in it first
    (`row_number()`, a CTE, as before an UNWIND), and `collect` sorts its
    values by that number (`groupArrayIf((n, x), x IS NOT NULL)`, sorted,
    then the values). `collect(DISTINCT x)` keeps each value where it first
    occurs. Over rows in an order the SQL lost (after DISTINCT or an
    aggregation) it is still not lowered.
  - **`DISTINCT` in an aggregate.** The parser reads `DISTINCT` as a
    prefix operator of the argument's first operand (`collect(DISTINCT a >
    1)` as `(DISTINCT a) > 1`). The binder moves it to the whole argument,
    and the emitter's type classifier reads `DISTINCT x` as `x`'s type (so
    `max(DISTINCT a > 1)` still shows a boolean).
  - **`+` with a list** (review finding). The emitter decides whether `+`
    concatenates from the shape of its operands, and a comprehension or an
    ordered `collect` is not one it knows: ClickHouse `plus` added the
    arrays element by element (`[1, 2] + [3, 4]` gave `[4, 6]`). `+` with an
    operand whose `Kind` is a list is lowered as concatenation; a value is
    appended or prepended as a list of itself, and only a literal: `[1] +
    NULL` is NULL in Cypher, and a ClickHouse array cannot be NULL (25.8
    finds no common type; 26.x makes it a `Variant`), so `+` of a list and
    a value that may be NULL is not lowered. A list `+` NULL literal is
    NULL.
  - **Parameters.** A comprehension's parameter is printed as written
    (raw), not as a table alias, which the emitter compares as a node
    (`x = y` became `x.id = y.id`).
  - **An aggregate in a comprehension's body** is an error, as in Neo4j
    ("Can't use aggregating expressions inside of expressions executing
    over lists").
  - **Not lowered:** a comprehension over nodes, relationships or paths
    (`[n IN nodes(p) | n.name]`, S7e2; S7e2a lowers lists built from the
    current rows); a lambda of a ClickHouse function
    written in the query; comprehensions in the Databricks dialect.
  - **Known gaps:**
    - The quantifiers `all`, `any`, `none` and `single` do not parse (on
      either path).
    - `reduce` whose list is a comprehension does not parse (on either
      path: the parser takes the comprehension's `|` as the reduce's).
    - `+` of strings is ClickHouse `plus` when neither side's type is known
      (an undeclared property, an element of such a list): loud, as in S7c.
    - A list of values of different types (`[1, true]`, `[1, 'a']`) still
      takes ClickHouse's common type, or fails (S4). `[1 > 0, 2]` was
      `[1, 2]` and is now `[true, true]` (Neo4j: `[true, 2]`).
    - A list that may be NULL (`[x IN CASE … END | x]`, an UNWIND of
      `[[1], null]` read as a list) fails: a ClickHouse array is not NULL. A
      list property of a node an OPTIONAL MATCH left NULL reads as `[]`
      (older, both paths).
- **Errors.** An unknown variable is a bind error ("Variable `x` not
  defined", as in Neo4j). An unsupported construct is `Unsupported`, which
  falls back to legacy during migration and is a loud error after it.

Expressions are bound to `BoundExpr`: `LogicalExpr`'s operators, with every
variable or property reference replaced by `Ref(VarId, Option<prop>)`.
Conversion from the AST reuses `logical_expr/ast_conversion.rs` for operators,
literals and functions.

### 4.5 Bound plan operators

```rust
pub enum BoundPlan {
    Unit,                                                     // one empty record
    Element(ElementRel),                                      // node scan, edge scan, path, alternatives
    Join     { left, right, kind: Inner | Cross, on: Vec<(Vec<Col>, Vec<Col>)>, residual: Option<BoundExpr> },
    Optional { input, inner, correlation: Vec<VarId> },       // OPTIONAL MATCH as one unit (§4.9)
    Filter   { input, predicate: BoundExpr },
    Unwind   { input, expr: BoundExpr, var: VarId },
    Extend   { input, items: Vec<(VarId, BoundExpr)> },                  // adds bindings, keeps input
    Project  { input, items: Vec<(VarId, BoundExpr)>, distinct: bool },        // WITH / RETURN; drops
    Aggregate{ input, keys: Vec<(VarId, BoundExpr)>, aggs: Vec<(VarId, AggCall)> },
    Sort     { input, keys }, Skip { input, n }, Limit { input, n },
    Union    { arms: Vec<BoundPlan>, all: bool },             // arms are symmetric; there is no "arm 0"
    Apply    { input, sub: BoundPlan, kind: Semi | Anti | Mark(VarId) | Count | Collect,
               correlation: Vec<VarId> },                               // Mark: boolean column (EXISTS under OR/CASE/RETURN)
}
pub enum ElementRel {
    NodeScan { var: VarId, label: Label, access: NodeAccessStrategy },
    EdgeScan { var: VarId, rel_type: RelType, from_var: VarId, to_var: VarId,
               access: EdgeAccessStrategy, direction: Directed },
    PathScan { var: Option<VarId>, from_var: VarId, to_var: VarId,
               spec: PathSpec },                             // range, types, direction, uniqueness, shortest mode
    Alternatives { arms: Vec<ElementRel> },                  // same output columns in every arm
}
```

Every operator has a `scope()`. `Project` and `Aggregate` are the only
operators that **drop** bindings. That rule is the precise meaning of
"a WITH is a scope barrier".

### 4.6 Lowering a MATCH clause: element relations, ties, uniqueness

For `MATCH P1, …, Pk WHERE W` with input `I`:

1. **Elements.**
   - Each relationship becomes an `EdgeScan`.
   - Each variable-length segment becomes a `PathScan`.
   - Each node that no edge supplies, and that is not already bound, becomes
     a `NodeScan`.
   - Which nodes the edges supply, and with which columns, is decided by
     `PatternSchemaContext` (`NodeAccessStrategy::OwnTable / EmbeddedInEdge /
     Virtual`). There is no branching on raw layout flags, so this is
     compatible with the ratchet.
   - **Node-scan elision.** A labelled own-table node whose properties are not
     demanded may be omitted, with the edge's foreign key standing in for
     its identity, as today's engine does. This is allowed only when **all**
     of these hold:
     - the schema declares that the edge's endpoint references are
       integral, a new per-edge `endpoint_integrity` flag, set to today's
       behaviour for the existing fixtures;
     - the node has no `filter:`;
     - the node has no view parameters;
     - the node does not use `FINAL`.

     Neo4j cannot hold a dangling edge, so the oracle fixtures get seeded
     dangling edges (§6) to keep this rule honest.
   - A **bound relationship variable** (from the input scope, or used twice
     in the clause) becomes an `EdgeScan` tied on its identity. Its endpoint
     ties follow the written direction. For an undirected reuse they come
     from `Alternatives`. It takes part in the clause's uniqueness pairs.
     Using it twice in one clause can never match (each use is a separate
     element, and uniqueness forbids the two being equal).
2. **Ties.** For every node variable `v` that appears more than once (in two
   elements, or in an element and the input scope), equate the identity
   columns of each appearance with the first: `Vec<Col>` = `Vec<Col>`,
   element by element.
   - A closed pattern `(a)-[*]->(a)` gets two ties on `a`. It needs no special
     handling (#1310).
   - A WITH-carried node is "in the input scope". It needs no
     `cte_references` repair (#1182 family, #1300, #1307).
   - A denormalized node shared by two edges ties the two edges' role
     columns directly.
3. **Relationship uniqueness.** One rule for every pair of
   relationship-bearing elements *of this clause*:
   - edge and edge: identities differ;
   - edge and path: `NOT has(path.edges, edge.identity)`;
   - path and path: `NOT hasAny(p1.edges, p2.edges)`.

   Inside a path the recursive CTE enforces it. Edge identity comes from the
   schema's `edge_id`, or from the endpoint tuple as today (the #887 policy).
   This removes the #1175, #1187 and #1203 per-shape guards.
4. **Join tree.** Left-deep, in a deterministic order: input first, then
   elements in pattern order, with a selective anchor first when there is no
   input. Join order is an optimization, and every order is correct because
   ties are equalities. Stats-informed ordering (P-5) applies here.
5. **WHERE** becomes `Filter(W)` over the clause's join. Placement is §4.8.
6. **Path variable.** `p` becomes a `Path` binding listing its elements.
   `length(p)` is the number of fixed edges plus each segment's `hop_count`,
   and `nodes(p)` / `relationships(p)` are built from element columns (#1202
   in general form).

`Alternatives`:
- An undirected edge `(a)-[r]-(b)` is two directed `EdgeScan` arms with
  identical output columns (from-side identity, to-side identity, edge
  identity, properties).
  - The reverse arm excludes self-loops (`from = to`), so a self-loop
    matches once, as in Neo4j.
  - Edge identity always uses the **stored** orientation (the schema's
    `edge_id`, or the stored `(from, to)` tuple), never the direction
    traversed. Otherwise uniqueness between undirected hops breaks for
    schemas without `edge_id`.
- A polymorphic edge with several possible endpoint labels has one arm per
  feasible (from-label, type, to-label) combination.
- An unlabeled node has one arm per feasible label.

Arms whose columns have different types are cast to one declared type per
column; the schema's declared property type wins. They are never left to
ClickHouse's common-type inference: this server sets
`use_variant_as_common_type=1`, which produces `Variant` columns whose
comparisons fail at runtime. A property missing from an arm is a typed NULL.

The union stays inside the element, so the rest of the query sees one
relation. That removes the arm-0 asymmetry (root C) and the per-arm NULL
extension of #1249.

**Implemented in S7a** (undirected relationships, standard layout):
- **Which directions.** For `(a)-[r:T]-(b)` with one label at each end, the
  directions are those the schema defines (`Lowerer::decide_rel`, an exact
  lookup: `get_rel_schema_with_nodes` falls back to the type's first schema
  whatever its ends, so it cannot say whether a direction exists):
  - one direction: the relationship is read as a directed one, in it
    (`(p:Post)-[:LIKED]-(u:User)` is `(p)<-[:LIKED]-(u)`);
  - both, and they are one table (the two ends have one label): the table
    in both directions, below;
  - both, as two relationship schemas: not lowered (S7b, with several
    labels and types);
  - none: it matches nothing.
- **Both directions** are the CTE `v{N}_both` (`Lowerer::both_directions`):
  `SELECT <columns>, from AS __cg_start_0, to AS __cg_end_0 FROM t UNION ALL
  SELECT <columns>, to, from FROM t WHERE from <> to`. The left end is tied
  to the node a row leaves, the right end to the node it enters. The stored
  columns keep their own names, so a relationship's identity, endpoints
  (`startNode` / `endNode` in a value) and properties are its own whichever
  way it is read, and uniqueness, values and exports need no case of their
  own. The columns are named, not `*`, which leaves out ALIAS and
  MATERIALIZED columns in ClickHouse (review finding): the identity and
  endpoint columns, every property mapping's columns, and the same-named
  column of each undeclared property read (the demand pass). The table's
  `filter:`, view parameters and FINAL apply inside both arms. A self-loop
  is read once (reversed, it is the same row), as Neo4j matches it once, for
  a hop and in a variable-length path alike (checked on Neo4j).
  - An `OR` in the join (`ON r.from = a.id OR r.to = a.id`) would read the
    table once, but ClickHouse runs it only with the `hash` join algorithm
    (`grace_hash`, `parallel_hash`, the merge joins refuse it, Code 48).
- **Variable-length and shortest paths** walk the same relation: the
  generator takes it as `CteGenerationContext::walk_relation` (its standard
  arms read `rel_source()`), walked from `__cg_start_0` to `__cg_end_0`, and
  the shortest-path search and walk read it in place of the table. The
  trail's identities (`path_edges`) are the stored ones, so a path does not
  use a relationship twice in opposite directions. The walk starts at the
  restricted end, as a directed one does; the path's order is the walk's
  from the left end, reversed from the right end.
- **Not lowered here:** an undirected variable-length relationship between
  two labels (as directed), and a variable-length relationship, of either
  direction, whose type has several schemas, are walks of a union (S7b3a,
  below). (A relationship bound before matched undirected, and two
  directions as two schemas, are S7b2.) Its nodes need not have one label: with `T` from N to N
  and from Z to N, `(:N)-[:T*1..2]-(:Z)` crosses from one to the other.
  Before, a pair of labels the type does not join fell back to the type's
  first schema (`get_rel_schema_with_nodes`), and the path matched nothing
  (0 against Neo4j's 13; review finding, also on the directed form).
- **Measured cost**, social benchmark at scale 100 (100K users, 10M
  follows), with ClickHouse's join statistics on (its default). Without
  them the first run builds the hash table on the two-direction relation,
  whose size the planner cannot estimate: every pair `count(*)` takes
  400 ms on that first run.

  | Shape | Legacy | New (directed) |
  |---|---|---|
  | one hop from a pinned user, `count(*)` | Code 47 | 8 ms |
  | every pair, `count(*)` (20M) | 5 ms (no node joins; a self-loop twice) | 58 ms (47 ms) |
  | two hops from a pinned user | 32 ms | 39 ms |
  | `*1..2` / `*1..3` (`count(DISTINCT b)`) from a pinned user | 69 / 200 ms | 80 / 215 ms (73 ms for `*1..2`) |
  | `shortestPath` of a pinned pair, `length(p)` / `p` | 720 / 736 ms (2 rows, wrong) | 512 / 815 ms (111 ms) |
  | `allShortestPaths` of a pinned pair, `count(*)` | out of memory | 708 ms |
  | `shortestPath` from one user to every user | out of memory | 1.7 s |

  An undirected search reaches about twice the nodes per level, so at
  depth 3 it reads about eight times the relationships of a directed one.

**Implemented in S7b1** (a node of several possible labels, standard
layout; `Scan::Labels`, `Lowerer::label_union`):
- **One relation.** The node is the CTE `v{N}_labels`: one arm per label
  (`SELECT 'L' AS __cg_label, <id> AS __cg_id_0, <properties> FROM <table>`,
  with the table's `filter:`, view parameters and FINAL), `UNION ALL`. A node
  is its label and its id: its identity is `(__cg_label, __cg_id_i…)`, so a
  post and a user with one id are two nodes (`a = b` compares both,
  `count(DISTINCT n)` counts the pair, NULL when an OPTIONAL MATCH left it
  NULL). The labels' ids must have one arity (else S8).
- **Properties.** The arms carry the properties read of the node (the
  demand pass, now also carrying a path value's demand back through a WITH):
  each label's mapping; NULL in a label that does not declare it when
  another label does (the node has no such property, as in Neo4j, which has
  no schema and reads NULL for any property a node lacks); and when no label
  declares it, the undeclared-property rule in every arm (the same-named
  column; NULL for discovered columns and in Neo4j-compat mode). The cost
  (review finding, kept by user decision 2026-10-08 as the closer to
  Neo4j): when a label's table has a same-named column it does not declare,
  `(n:Robot|Company).name` is NULL for robots while `(r:Robot).name` reads
  the column.
- **Types.** A union's column takes its arms' common type, converting
  values (`Bool` and `UInt8`, `Float32` to `Float64`, `Date` to
  `DateTime`; S7b2 review finding), or with none a `Variant`
  (`use_variant_as_common_type`, on by default in 26.x), which answers
  differently: `count` and `groupArray` count its NULLs, `toString` of its
  NULL is `''`, and `8` and `8.0` are two values to DISTINCT (S7b1 review
  finding). So a property several labels have a value of is one column per
  label (`…__cg{k}`, NULL in the other labels' rows), read as their
  `coalesce` through `FunctionMapper::one_type_guard`: on ClickHouse, cast
  to its own type when the columns' types are the same up to integer width,
  string representation (`String`, `FixedString`, `LowCardinality`) and
  nullability, which change no value; else to a type name that does not
  exist, an error while the query is analysed, rows or none (an integer on
  one label and a float on another is an error too: Neo4j returns each as
  its own type). A setting in the CTE does not reach the union's type; a
  query-level setting would need every executor to send it.
- **Relationships.** A relationship of one type at a node of several labels
  is read between the one pair of labels the schema joins it at
  (`Lowerer::end_labels`, in either orientation when undirected), and that
  end is filtered on its label (`Lowerer::holds_label`). Variable inference
  narrows a node the clause introduces already; this is for a node carried
  from an earlier clause. Several pairs are a union of arms (S7b2, below);
  a variable-length relationship at such a node is a walk of a union
  (S7b3a).
- **Labels.** `n:L` is `__cg_label = 'L'` (NULL where an OPTIONAL MATCH left
  the node NULL), `labels(n)` is `[__cg_label]`, and a written label on a
  carried node filters its label column.
- **Returned whole**, the node is a node value (§4.11's `GraphType::Node`,
  elementId and labels from the label column, the properties of every label,
  NULL ones dropped): Bolt, the graph output and embedded `query_graph` take
  its label from each row. The HTTP rows hold the value (a single-label node
  keeps its `n.<prop>` columns).
- **Not lowered:** `n.*` and `id(n)` of such a node.

**Implemented in S7b2** (a relationship of several possible types or label
pairs, standard layout; `Scan::Rels`, `Lowerer::rel_union`):
- **Arms.** For each type the relationship can have, each left and right
  label its ends can have, and each orientation its direction allows, the
  definition the schema has (`Lowerer::rel_arms`, an exact lookup): an arm
  is a definition read as stored or reversed. One arm is its table, as
  before; one definition between nodes of one label in both orientations is
  S7a's two-direction CTE; any other set is one relation, the CTE
  `v{N}_rels`, an arm per definition and orientation (its table with its
  `filter:`, view parameters and FINAL), `UNION ALL`. This covers `[:A|B]`,
  an untyped relationship, a type with several definitions (two tables), an
  undirected type whose two directions are two definitions, and a node of
  several labels a type joins in several ways.
- **Rows.** Each row carries its type and the labels of its stored ends
  (`__cg_type`, `__cg_from_label`, `__cg_to_label`: its definition), its
  identity (`__cg_rid_i`: the `edge_id`, else the stored ends, #887; NULL
  past the definition's own arity), the ids of its stored ends
  (`__cg_from_i`, `__cg_to_i`), and the label and id of the node it leaves
  and enters here (`__cg_start_label` / `__cg_start_i`, `__cg_end_label` /
  `__cg_end_i`). A relationship is its definition and its identity, so two
  definitions' equal ids are two relationships. The definitions' ends must
  have one id arity each (else S8). A definition between nodes of one label
  read in both orientations reads a self-loop once (the reversed arm leaves
  it out), as Neo4j matches it once.
- **Ties.** Each end is tied to the ids of the node the row leaves or
  enters, and to its label (`Lowerer::tie_end`): a node of several labels
  equals the row's label column; a node of one label holds it (a filter,
  when the rows can have another); an end the relationship cannot be at
  makes the clause match nothing (this also catches a relationship carried
  from an earlier clause re-matched at a node of another label, which tied
  ids alone before).
- **Properties** follow S7b1's rule (user decision 2026-10-08): the
  definition's mapping; NULL in a definition that does not declare it when
  another does; the undeclared-property rule when none does. A property
  several definitions have a value for is one column per definition (the
  two orientations of one share it), read through `one_type_guard` as on a
  node of several labels: one value when their types differ only in integer
  width, string representation or nullability, else an error while the
  query is analysed.
- **Identity.** `type(r)` and `r:T` read the type column; `r = s` compares
  definitions, then identities NULL-safely (the padding); `count(DISTINCT r)`
  counts the tuple. Relationship uniqueness compares the definition and
  identity of a union with another relationship (or union) of a common
  definition, and keeps a union's rows of a variable-length relationship's
  definition off its path (`NOT has(path_edges, …)`, spelled as the path
  spells it). An OPTIONAL MATCH correlates on a union's identity NULL-safely.
- **Carried.** A WITH exports the definition, identity and stored ends; a
  later clause ties them as stored (directed) or both ways (below), its
  labels as above. A written type (`()-[r:A|B]->()` on a carried `r`) filters
  the type column.
- **A relationship bound before, matched undirected** (S7a's refusal): the
  rows so far join a two-row orientation relation (`v{N}_turns`, `ON 1 = 1`),
  and the left end is the stored `from` when the row is read as stored and
  the stored `to` reversed (a `CASE` in the tie); a self-loop is read once,
  as Neo4j (`NOT reversed OR from <> to`). An end already joined is tied in
  WHERE (the orientation join comes after it). One used twice in one MATCH
  matches nothing (uniqueness; Neo4j: 0 rows); the binder refuses that
  shape today.
- **Returned whole**, it is a relationship value (§4.11's
  `GraphType::Relationship`: element id, type and end element ids from its
  columns, the properties of every definition, NULL ones dropped), grouped by
  its identity and its carried properties (as a node of several labels). A
  path value reads its relationships the same way; a path key spells a union
  relationship as its row's `__cg_key` (S7b3a: as every scan spells it).
- **Not lowered:** `r.*` of such a relationship. (A variable-length
  relationship of several types or at a node of several labels is S7b3a.)
- **Known gaps** (loud): definitions whose identity or end ids are a string
  in one and a number in another make a `Variant` column, which ClickHouse
  refuses to group by (`RETURN DISTINCT r`, Code 44) or to compare with a
  number (`r = s`, uniqueness, Code 386). Element ids carry no end labels
  (`T:1->1-`), so two definitions' relationships between equal ids share one
  (as parallel relationships of one definition already did).
- **Measured cost**, social benchmark at scale 100 (100K users, 10M
  follows, 2M authored, 5M likes), join statistics on:

  | Shape | Legacy | New |
  |---|---|---|
  | untyped hop from a pinned user, by type | 13 ms | 11 ms |
  | `[:AUTHORED\|LIKED]` from a pinned user, `count(DISTINCT p)` | 9 ms | 8 ms |
  | untyped undirected hop from a pinned user | 13 ms | 12 ms |
  | untyped hop from a pinned user, `RETURN r LIMIT 25` | 10 ms | 12 ms |
  | two untyped hops from a pinned user | error | 71 ms |
  | 100 users, OPTIONAL untyped hop, `count(r)` | 13 ms | 20 ms |
  | every untyped hop, by type (17M) | 143 ms | 698 ms |
  | a carried relationship re-matched undirected | 5 ms (another count) | 35 ms |
  | a carried untyped relationship re-matched | 66 ms (another count) | 17 ms |

  The whole-graph count joins every relationship to the union of its end
  labels' tables on (label, id), where legacy joins each type to its own
  table; joining each arm to its end's own table is an optimization left
  for later.

**Implemented in S7b3a** (a variable-length relationship of several types,
definitions or labels, standard layout; `Walked::Union`,
`Lowerer::build_union_path`, `path::union_path_ctes`). A relationship of one
type of one definition between nodes of its one label is still the
generator's walk of its table (`Walked::One`); any other is a walk of the
union of its definitions, whose nodes are each a label and an id:
- **Arms.** Every definition of each of its types, in the orientations that
  lead away from the end the walk starts at (`Lowerer::walk_arms`): as stored
  or reversed for a directed relationship (reversed when the walk starts at
  its `to` end), both for an undirected one. Only those a path between its
  ends can use are read: a relationship leaves a node of the label the one
  before it entered, so an arm whose rows leave a label no walk from the
  first end's labels reaches, or enter one from which no walk reaches the
  last end's labels, matches nothing here, whatever its layout. (An
  untyped path from a user to a user on the social schema reads FOLLOWS
  only.) The arms are a relationship union (`Lowerer::rel_union_of`, S7b2's
  relation, each row with the node it leaves and enters), whose rows also
  carry their identity as a text (`__cg_key`, below) and, when the path is a
  value, their relationship's value.
- **Nodes.** The nodes a path can visit are one relation (`vlp_{r}_nodes`:
  an arm per label an arm leaves or enters, and the first end's labels, each
  its table with its `filter:`, view parameters and FINAL), each row its
  label, id, identity as a text and, when the path is a value, its value. A
  walk joins the node of each relationship it follows here, so it visits
  only nodes there are, as a walk of one table does.
- **Walk** (`vlp_{r}_trails`, recursive): its first rows are the paths of
  none at each first node (a row of the nodes of the first end's labels,
  restricted by `(label, id) IN` the rows so far or the end's own conjuncts:
  §4.8 d, as a walk of one table is); each step extends a path by a
  relationship whose start label and id are its last node's, not on it
  already (`NOT has(path_edges, key)`: a trail), that satisfies the
  relationship's property map (each definition's spelling of the property,
  read through `one_type_guard`). `vlp_{r}_path` is those of the range.
  Columns: `start_label`, `start_id`, `end_label`, `end_id`, `hop_count`,
  `path_edges` and `path_nodes` (texts), and the values. Each end is tied
  by label and id (`Lowerer::tie_end`): a node of several labels by its
  label column, one of one label by a filter on the path's end label where
  the path can end at another. The walk starts at the restricted end, as a
  walk of one table does; its order is the path's from the left end,
  reversed from the right. An unbounded range is unbounded.
- **Identity as a text** (`value::rel_key`, `value::node_key_text`): a
  relationship is `type:from_label:to_label:` and the text of its identity
  (its `edge_id`, else its stored ends: one column, or their tuple, as a
  path's `path_edges` spell it); a node is `label:` and its id. Every scan
  spells one alike: a table's relationship from its columns, a
  relationship union's row from its own `__cg_key` (computed in its arm,
  from the arm's own column types), a walk of one table by prefixing its
  `path_edges`, a union walk's as they are. So a path that splits between
  its parts in several ways (`*0..1`, a hop, `*0..1`) has one identity. Before
  (S7b2), a union hop spelled its identity column by column while a walk of
  one table spelled `type:` and a tuple: `DISTINCT p` over
  `[:KNOWS*0..1]->()-[r:KNOWS|FOLLOWS]->()-[:KNOWS*0..1]` counted 19 paths
  against Neo4j's 16 (found by this slice's sweep, fixed by the spelling).
- **Uniqueness.** A union walk and another relationship of one MATCH with a
  definition in common differ by these texts (`Lowerer::key_differs`): a
  relationship's is not on the walk's `path_edges`; two paths' do not
  overlap.
- **Carried** through a WITH, a union walk exports its end labels with its
  ends; it is grouped by its first node and relationships, as a walk of one
  table is.
- **Not lowered:** a walk any of whose arms (after the pruning above) or
  nodes is not the standard layout or has composite ids (S8). (A
  `shortestPath` / `allShortestPaths` over them is S7b3b, below.)
- **Known gaps:** a property several labels have, read in a condition on
  the first end, is the dialect's guarded `coalesce` (raw SQL), which the
  conjunct analysis does not read: the walk starts at every node of the
  end's labels and the condition holds after it (correct, slower). Ids of
  different types across the walk's labels or definitions make a `Variant`
  column, as in S7b2 (an error, not a wrong answer).
- **Measured cost**, social benchmark at scale 100 (100K users, 10M
  follows, 2M posts, 5M likes), on a shared machine (load average about
  22: the absolute times are inflated, legacy's alike), from a pinned user
  unless said:

  | Shape | Legacy | New |
  |---|---|---|
  | untyped `*1..2`, by label | 108 ms | 679 ms |
  | `[:FOLLOWS\|LIKED*1..2]` | 80 ms | 654 ms |
  | untyped `*1..3` to a user (walks FOLLOWS only) | 201 ms | 444 ms (`[:FOLLOWS*1..3]`, one table: 232 ms) |
  | untyped undirected `*1..2` | 327 ms | 817 ms |
  | untyped `*1..2` into a pinned user | 17.7 s | 300 ms |
  | `[:FOLLOWS\|AUTHORED*1..2]`, `RETURN p LIMIT 25` | 80 ms | 1.5 s |

  Legacy expands a fixed range into fixed-length chains over the base
  tables, which ClickHouse orders by their sizes; a walk's steps read the
  unions as CTEs, which it cannot size, so each step builds its hash tables
  on the relationship union and the node union (the node join is about 60%
  of a pinned walk). Streaming the node union instead (the step's paths
  hashed) halved a two-hop walk but was five times slower at depth 3 (its
  frontier of 100K paths, with their arrays, hashed): not taken. A value of
  a path builds every node's and relationship's value in the unions, for
  every row, at every step; building them after the join is left for later,
  with joining each arm to its end's own table (S7b2).

**Implemented in S7b3b** (a `shortestPath` / `allShortestPaths` over several
types, definitions or labels; `Lowerer::build_union_path`, `path::Search`,
`path::UnionWalk::search`, `path::union_ends_cte`). The search of §4.11
(S6b, S6d) runs over a union walk's relations:
- **Search.** The breadth-first search, the walk back over its levels and
  the pick read a `path::Search`: the relations of the nodes and the
  relationships, the column that identifies a node, the range, the
  conditions on the first node, the last node and every relationship, and
  how a relationship's identity and each element's value are spelled. A walk
  of one table builds it from its tables (`PathCall::search`, the SQL
  unchanged). A union walk builds it from its nodes (`vlp_{r}_nodes`) and
  relationships (`{r}_rels`). A node is then identified by its identity as
  a text (`__cg_key`, `label:id`), and each relationship row carries the
  identities of the nodes it leaves and enters (`__cg_start_key`,
  `__cg_end_key`). The identity is one column, so the search, the walk back
  and the pick are the same SQL as a table's. The texts are those a path's
  identity spells, so `path_nodes` and `path_edges` are a union walk's as
  they are.
- **Ends.** The last node is restricted as the first is (by label and id
  `IN` the rows so far or its own conjuncts, §4.8 d), and the search stops
  once it has reached every value the last node can have. A label condition
  that every node the walk visits satisfies is left out, at either end:
  otherwise every step would check targets that hold of all nodes. The pairs'
  ends are the nodes of their identities (`vlp_{r}_ends`: `start_label`,
  `start_id`, `end_label`, `end_id`), tied as a union walk's ends are.
- **Conditions** (§4.11): the trails the pick reads are the union walk's
  (`vlp_{r}_trails`), carrying their ends' identities (`UnionWalk::keyed`).
  An end a condition reads is joined by its identity: a node of one label
  from its table, one of several from its labels' union.
- **Not lowered:** a condition on the path that reads a property declared
  by several of an end's labels. That read is the guarded `coalesce`, raw
  SQL the conjunct analysis cannot read (S7b3a's known gap), so the legacy
  pipeline answers it, as it answered every shortest path over several
  labels or types before.
- **Known cost:** under a condition on the path, the pick reads every trail
  from the first nodes of the pairs whose distance fails it (S6b), as a walk
  of one table does. Unbounded and undirected over a dozen relationships
  (12 relationships of the S7b2 graph), from two first nodes, they are 12.8
  million trails of up to 18 relationships. Without values the query takes
  6 s; with `RETURN p` each trail carries its values and ClickHouse runs out
  of memory (an error). Over five types (all of the graph's relationships),
  even one first node without values exceeds a 4 GB limit within seconds
  (the review). An upper bound (`*..6`) answers in 0.4 s. Building values
  after the pick is the S7b3a follow-up (values after the join); the trails
  themselves are S6b's design.
- **Measured cost**, social benchmark at scale 100, pinned ends, on a
  shared machine (load average about 8):

  | Shape | Legacy | New |
  |---|---|---|
  | `shortestPath`, `[:FOLLOWS*]`, one table | 307 ms | 121 ms |
  | `shortestPath`, `[:FOLLOWS\|LIKED*]` undirected, `length(p)` | 117 ms (93 rows, wrong) | 1.9 s |
  | the same, `RETURN p` | 162 ms (93 rows, wrong) | 2.6 s |
  | `allShortestPaths`, the same, `RETURN p` (93 paths) | 170 ms | 3.1 s |
  | `shortestPath`, `[:FOLLOWS\|LIKED*..4]->` to a pinned post | 16 ms | 529 ms |
  | `shortestPath` from a user to each post with `post_id < 1000` within 3 | 19 ms (298, wrong) | 544 ms (250) |

  Each step of the search joins the relationship union on text keys and
  the node union (CTEs ClickHouse cannot size), against one table's integer
  joins.

**Implemented in S8a** (FK edges; `RelationshipSchema::is_edge_row_table`,
`path::standard_layout`, `Identifier::crosses_positionally`):
- **What an FK edge is.** Its table is one of its ends' node tables: each
  row is a node holding a reference (`from_id` or `to_id`) to the other end
  (`AUTHORED` on the posts table, a self-referencing `PARENT` on a file
  table). Like an edge table's, its rows are its relationships, one per
  row, from the node its `from_id` names to the node its `to_id` names.
- **Ends.** For an edge table, a row's `from_id` holds its from-node's id
  and its `to_id` its to-node's. A self-referencing FK edge (one label at
  both ends) relates each row's node to the node its reference names,
  whichever of `from_id` / `to_id` the schema writes the reference in: the
  documented `from_id: parent_id, to_id: object_id` is child to parent, as
  is `from_id: comment_id, to_id: reply_of_id` (#632, legacy's reading;
  the review found the first cut reading the first form backwards). The
  catalog writes such an edge's ends as an edge table's
  (`GraphSchema::with_fk_edges_as_edge_rows`: `from_id` the node's id,
  `to_id` the reference), and the lowering reads that schema
  (`lower_statement`), so every scan, walk, identity and value agrees. The
  oracle loader reads the same rule.
- **Table options.** An FK edge's rows are its owning node's rows. Where
  the edge declares no view parameters, its table is read with its node's
  (the review: a node on a parameterized view with an edge declaring none
  read the bare view, a ClickHouse error, where legacy answers). It is read
  with FINAL when its node's is (a loaded `use_final` is always set, so an
  edge's unset one cannot be told apart from `false`); a joined read cannot
  print FINAL yet, so such an edge is not lowered rather than read with
  stale row versions (the review). A `filter:` stays the edge's own: it
  says which rows are relationships.
- **Read as an edge table.** The relationship is a read of the table and
  each end a read of its own label's table, tied on the end's id, as for an
  edge table (§4.6 1–2): the same scans, ties, uniqueness, unions, walks and
  values. A row whose reference is NULL or names no node, or whose own node
  is not one (its label's `filter:`), joins no node, so it is no
  relationship. That is the oracle loader's graph: it loads one
  relationship per row and drops those whose ends are NULL or no node.
- **Walks.** The generator is given an edge table's walk
  (`JoinStrategy::Traditional`, each step joining the relationship's row
  and the node it enters) in place of the `FkEdgeJoin` that
  `PatternSchemaContext` reports. Its FK-edge walk reads the referenced
  node's row in place of the relationship's and keeps node uniqueness at a
  zero lower bound (#902); with it the mutation check's sweep errors or
  differs on 10 shapes.
- **Crossed ends.** An end whose columns are its node's id columns in
  another order (`to_id: [forum_id, region]` against the id `[region,
  forum_id]`) is refused: paired position by position it would tie
  `forum_id = region` (#672 / #1010, which legacy refuses with the same
  test, now `Identifier::crosses_positionally`). This also holds for edge
  tables, which S4 lowered with the columns crossed. A refusal falls back,
  and legacy's guard misses some shapes (an unlabeled end, a walk of
  several types), which it answers crossed, as `main` does (#1348).
- **Known cost.** The table is read twice, for the relationship and for
  the node whose row it is. Reading that node from the relationship's row
  is a join elimination for later: it holds only where the node's
  `filter:`, view parameters and FINAL hold of the relationship's read.
- **Undeclared properties** of an FK edge read the same-named column of its
  table (the rule, §4.4), so a node's column: `r.title` of `AUTHORED` is the
  post's title, where Neo4j has none.

**Implemented in S8b** (nodes embedded in edge tables;
`GraphSchema::with_node_relations`, `lower/embedded.rs`):
- **What an embedded label is.** A denormalized node (`from_node_properties`
  / `to_node_properties`) whose definition's table is the table of an edge
  it is an end of has no table of its own (`Airport` in a flights table,
  `IP` in DNS and connection logs). Each role of each definition is a
  source: the rows of its table (with the definition's `filter:`, view
  parameters and FINAL), its columns of the label's properties. The
  label's nodes are the distinct non-NULL ids its sources hold; a
  property is the value its sources hold for the id. That is the schema's
  statement (a property is the node's): where rows of one id disagree (the
  zeek fixture's `IP.port`, a port per connection; or a NULL in one row
  and a value in another, the review), the node has no defined value, the
  oracle loader leaves it off and reports it, and the lowering returns one
  of them: the row's when the node is read from a relationship's row, else
  the relation's (`any`, which skips NULLs).
- **Node relations.** The lowering reads the schema's view in which such
  a label is an own-table node over a relation named `__cg_nodes_<label>`
  whose columns are its property names (`NodeRelation`): every scan, tie,
  union, walk, search and value reads it as a node table. The relation is
  two CTEs, built when a node of the label is first read
  (`Lowerer::node_table`): `…__cg_rows`, a `UNION ALL` of the sources
  (each property of each source its own column, so each keeps its type),
  and the relation, grouped by id (`any`, through
  `FunctionMapper::one_type_guard` when several sources hold a property,
  applied per row: `toTypeName` of an aggregate is no constant).
- **All definitions.** The catalog keeps one node schema per label; a
  label embedded in several tables keeps its definitions apart
  (`GraphSchema::set_node_definitions`, at build) for its relation. A
  label embedded in some tables and its own in others, or with several
  own tables, is left out of the view (not lowered): one relation of
  embedded and own sources is not built yet, and reading one of its
  definitions (the first cut, by YAML order) answers for part of its
  nodes (review).
- **Composite ids.** A source names the id's columns in the id's order;
  an edge end embedded in it may list them in another
  (`to_id: [ap_st, ap_code]` over `[code, st]`). The view rewrites such an
  end in the source's order (`NodeRelation::ordered_end`): the end is
  then tied column by column (the first cut tied `st` to `code`, 0 rows;
  review). Crossed ends elsewhere are refused (S8a).
- **Read from the row** (§4.6 node-scan elision; `Lowerer::embed_end`). A
  node of the clause at an end of a relationship whose row embeds it (a
  source of its relation is the relationship's table at that end: the
  same id columns, no `filter:` or view parameters, its FINAL) and read
  only for properties that source holds is read from the row: its
  identity the end columns, its properties the row's, its relation not
  joined; a row whose end is NULL is filtered out. Only onto the first
  relationship of the clause the node is an end of: a later one's tie
  reads the node after the first's (the first cut elided onto a later
  relationship when the first's row lacked a property, and the first's tie
  read a relation never joined: a ClickHouse error). A node read whole, at
  a variable-length relationship's end, from an earlier clause, or for a
  property another source holds reads the relation. The generated SQL of a
  path of embedded hops is then legacy's (`FROM fl JOIN fl ON g.org =
  f.dst`).
- **Foreign denormalized labels.** A denormalized definition whose table
  is no edge table of its label (edges carry its id: `foreign_selfloop`)
  is its own table's nodes; the edges' copies of its id are a way to read
  it, which legacy uses without the node's table. The lowering joins it
  (elision would need `endpoint_integrity`).
- **Edges.** An edge whose ends are embedded is a table of relationships
  like any other (`RelationshipSchema::is_edge_row_table` now asks only
  for fixed endpoint labels). Relationship uniqueness compares
  relationships of one definition only: two types on one table's rows are
  never one relationship (it compared tables, so a `REQUESTED` and a
  `RESOLVED_TO` of one DNS row excluded each other; the same held of two FK
  edge types on one table).
- **Coupled edges** (two types on one row sharing a node) are two
  relationships tied through their shared node, as the logical graph has
  them, not legacy's same-row reading. Every relationship is a row of the
  table; what the tie adds is paths through the shared node across rows:
  `(i:IP)-[r:REQ]->(d:Domain)-[v:VIA]->(s:Server)` pairs each request of a
  domain with each server any row resolved it to (Neo4j's answer on the
  same graph), where the same-row reading drops the cross-row pairs. The
  same-row question is a query's to state (`WHERE r.uid = v.uid`, which a
  later optimization can read from one row), or the schema's (the request
  a node of its own).
- **Not lowered:** an undeclared property of an embedded node (no table
  whose same-named column the rule could read; legacy reads the row's).

**Implemented in S8c** (polymorphic edges and label-column node tables;
`GraphSchema::with_discriminators_as_filters`):
- **Discriminators are row filters.** A polymorphic edge (rows of several
  types or end labels in one table: `type_column`, `from_label_column` /
  `to_label_column`, `$any` ends) is, in the lowering's view, a definition
  per type and pair of end labels (`T::From::To`), its rows those its
  discriminators name, as a `filter:` (`kind = 'T' AND from_type = 'From'
  AND to_type = 'To'`, with its own `filter:`). Labels sharing a table
  through `label_column` / `label_value` are that table's rows of their
  value. Every scan, tie, union, walk, search and value then reads them as
  fixed-label definitions and plain node tables: nothing downstream knows
  the layout. Label columns hold label names (legacy compares them so).
- **Which labels.** An `$any` end is each label of the schema, or of its
  `from_label_values` / `to_label_values` (closed world): a row whose label
  column names another label, or none, holds no relationship. Label
  inference (`labels.rs`, `Feasibility`) closes the end alike, so an
  unlabeled end is a union of only the labels the schema allows there.
- **Read as written.** A polymorphic edge on a node table (`CONTAINS` from
  `parent_id` to `fs_id` on the objects table) is not an FK edge
  (`is_fk_edge` is false): its `from_id` is its from-node's, as legacy and
  the YAML read it, not #632's self-reference rule.
- **An FK edge's rows are its owner's.** An FK edge (S8a) is read with its
  owner's `filter:`, which on a table labels share holds its label
  condition: another label's row of the same id is no reference (the
  review: `(x:A)-[:CHILD_OF]->(y:A)` read a `B` row's parent). Of two labels
  on the edge's table, the owner is the end whose columns hold its node's
  id (`from_id: parent, to_id: id` is the to-node's row). The oracle loader
  reads FK edges alike.
- **Walks over a filtered table.** The generator's walk of one definition
  reads no table options; a walk of one definition over a table with a
  `filter:`, view parameters or FINAL is the walk of definitions
  (`Walked::Union`, S7b3a), which reads them, rather than refused (also for
  standard edges with a `filter:`).
- **Not lowered:** a discriminator the filter grammar cannot hold (a quote
  or backslash in a value, a backtick in a column); an `$any` end without a
  label column (validation requires one); and a pair of labels a standard
  definition of the same type also joins (the catalog holds one definition
  per key, and its type index hides the polymorphic one; the config warns
  that mixing them is unsupported). Label inference reads every definition
  by its key, so the polymorphic table's other pairs are lowered.

**Implemented in S8d1** (walks between composite ids):
- **By the ids' columns.** The generator spells a composite id as the text
  of its columns joined by `|` (`start_id`, `end_id`, `path_nodes`), which
  two ids can share (`('B|1', '2')` and `('B', '1|2')` are both `B|1|2`).
  The lowering's walks never join or tie by it: each step joins the
  relationship to the path's last node by the id's columns (the walk
  carries `end_<col>`; `CteGenerationContext::composite_by_columns`, set
  by the lowering only, so legacy's SQL is unchanged), the walk's ends are
  tied by `start_<col>` / `end_<col>`, and its first node is restricted by
  the tuple of its columns (`(bank, num) IN (SELECT …)`).
- **Relationships by their identity.** A relationship whose ends are
  composite is identified, as the generator spells a `path_edges` element,
  by its `edge_id` or the tuple of its stored ends' columns: a hop beside a
  walk is unique against it (`path::edge_identity_sql`, total now), and a
  union arm against a walk compares that many `REL_ID` columns. A
  shortest-path search over single ids with a composite `edge_id` spells
  its relationships alike (now lowered).
- An undirected walk reads its table in both directions with a column per
  identity column (`BOTH_START_k` / `BOTH_END_k`).
- **Refused (review):** a relationship whose end has another number of
  columns than its node's id (it tied part of the id; checked for every
  relationship, `standard_edge`), and a walk between composite ids one of
  whose columns is named `id` (the generator's `start_<col>` is then its
  `start_id`).
- **Known gap (#1354):** a node whose id, or a part of it, is NULL is
  dropped from a zero-length walk (its ties and first-node restriction use
  `=` / `IN`; a single nullable id since S6a).
- A composite id in a value or a list: S8d2c below.

**Implemented in S8d2a** (shortest paths between composite ids,
`path::composite_key`):
- **Nodes keyed by the text of their id's tuple.** The search identifies a
  node by one value; between composite ids it is the dialect's text of the
  tuple of the id's columns (`toString(tuple(bank, num))`), which no two ids
  share: ClickHouse quotes a string part, so `('B|1','2')` and `('B','1|2')`
  differ, unlike the generator's `|`-joined text. The key is computed where
  the search reads a node or a relationship's end (`Search::node_key` /
  `rel_start_key` / `rel_end_key`), over the tables as they are (a subquery's `*`
  would drop MATERIALIZED and ALIAS columns a condition reads: review),
  and the relation's `start_id` / `end_id` are tied to the ends by the same
  key over their id columns.
- **NULL parts.** A relationship row whose end has a NULL part relates no
  node, as the columns' `=` decides (the text of `(NULL, 1)` would equal
  itself and invent relationships: review); a node with a NULL part is
  still its own zero-length path's end.
- **Known gap:** a part whose node and edge columns spell one value
  differently (`FixedString` against `String`, `Bool` against `UInt8`,
  `DateTime`s in other time zones, `-0.`) does not match, where `=` would
  (and the reverse for `Float32` against `Float64`).
- **Conditions the search cannot hold** (§4.8: a length it does not bound,
  a lower bound above 1, a condition on an end): the pick reads the
  generator's trails through `vlp_{var}_keyed`, their ends keyed alike from
  the `start_<col>` / `end_<col>` the walk carries, restricted to the pairs
  failing the conditions by that key; an end a condition reads is joined
  by it.
- A composite shortest path read as a path: S8d2c below.

**Implemented in S8d2b** (walks of several definitions between composite
ids, `Lowerer::walks_by_keys` / `walk_key`, `walk_key_sql`):
- **A walk by keys.** A union walk (S7b3a/b) joins its nodes by label and
  one id column. When a label it can visit has a composite id, every node's
  id in the walk (`__cg_id_0` of `vlp_{v}_nodes`) is its identity as a text
  (`KEY`: its label and the text of its id's tuple, or of its single id),
  and so is each relationship row's end (`__cg_start_0` / `__cg_end_0` of
  `{v}_rels`): one column of one type whatever the arity, which every label
  and definition spells alike and no two ids share. The trails, the search
  and the ends' recovery read them as before; the first node is restricted
  by `(label, key) IN (…)`, and the walk's ends tied by the key of their
  label and id. A walk over single-id labels only is unchanged.
- **NULL parts.** A relationship row whose composite end (either end) has
  a NULL part relates no node (`=` decides; the text would equal a node's
  of the same parts), as in S8d2a.
- **Join order.** A key tie is SQL text whose columns the lowering does not
  see, so the joins of a relation holding one stay in the order they were
  placed (`Lowerer::path_first` does not move a path first): moved, a tie
  read a node not joined yet (review).
- **Known gap (#1360, also S7b3b's and S8d2a's searches):** a text key
  misses equal values of differently typed node and edge columns (`Date`
  against `DateTime`, `Bool` against `UInt8`, `FixedString` against
  `String`): the relationship relates nothing, where `=` would match.
- A walk's definitions still share one end arity (`rel_union_shape`); a
  union's identity columns of different types fail as before (#1352).

**Implemented in S8d2c** (composite ids in values and lists):
- **One spelling of a composite id's identity.** Under
  `composite_by_columns` (the lowering's walks only) the generator spells a
  composite id as the text of its tuple (`emit_id_expr`), as the searches
  (S8d2a) and walks by keys (S8d2b) do: a walk's `start_id`, `end_id` and
  `path_nodes` are collision-free, so the identity of a path's nodes
  (`DISTINCT nodes(p)` as a value) tells pipe-sharing ids apart, and a
  shortest path's trails and recovered paths spell nodes alike (a shortest
  path read as a path, with a condition the search cannot hold, is
  lowered: the keyed trails carry its columns). A value's identity
  (`node_key`) is the label and the text of the tuple.
- **Element ids** follow ClickGraph's contract (`graph_catalog::element_id`,
  as Bolt and the graph output make them): a composite id's parts' texts
  joined by `|` (`value::element_id_sql`), a NULL part spelled `null` (not a
  NULL element id: Bolt blanked the row, review), in a node's `elementId`
  and a relationship's and its ends'. (The contract's text is not
  collision-free; identities never use it.)
- **A NULL element.** An element is NULL (an OPTIONAL MATCH's unmatched
  one, a NULL list element) when every part of a composite node id is
  (`Lowerer::presence`, `null_test`): a part of a real node's can be NULL,
  and the first-column test dropped such nodes from lists and counts
  (review). Other identities keep their first column (a label union's
  label, a relationship's, a path's start).
- **List elements.** A node's or relationship's element tuple holds a slot
  per id column (its id; its ends' ids and `edge_id`), so lists of
  composite nodes and relationships, of relationships with a composite
  `edge_id`, and a walk's carried tuples are lowered; `count(DISTINCT x)` of
  a list's element of several identity slots counts their tuple.
- **Not lowered:** `id(n)` of a composite node (one value).
- **Comparator note:** a relationship of several types returned whole is a
  value whose ends the comparator reads off the element id (`B|1`), where
  Neo4j's flattened ends are a list (`["B", "1"]`); the value is the
  contract's.

### 4.7 Label inference

Labels are inferred over the explicit pattern graph of each clause, with
carried variables bringing their already-inferred label sets. It is
constraint propagation:

```
label(v) := label(v) ∩ { from-labels of feasible (type, direction) for each incident edge }
```

It is iterated to a fixed point, with feasibility taken from the schema
catalog.
- A variable-length segment propagates through the transitive closure of
  feasible types.
- A `*0..` segment also allows end = start, so the end's set includes the
  start's.
- An empty set means the clause matches nothing:
  - in a MATCH this is the `WHERE false` plan, as today;
  - in an OPTIONAL MATCH it means every input record gets NULLs. This replaces the per-query `TypeInference` result for the
new path. Slice 1 includes a parity check against `TypeInference`'s label
sets over the corpus, and any disagreement is decided by checking against
Neo4j.

### 4.8 Filter placement: correct by default, pushdown only when proven

Every WHERE is first placed **exactly where its clause puts it**:
- MATCH: a filter over the clause's join;
- OPTIONAL MATCH: inside the `Optional`'s inner plan;
- WITH: a filter over the `Project` / `Aggregate`.

`pushdown.rs` then moves a conjunct `c` toward the leaves only when all of
these hold:
- (a) every `VarId` in `c` is produced by the target subtree;
- (b) the target is not on the NULL side of an `Optional` boundary that `c`
  sits outside;
- (c) the target is not past a `Project`, `Aggregate`, `Limit` or `Skip`
  boundary (except the standard push of a pure grouping-key predicate
  through `Aggregate`);
- (d) for a push into a `PathScan`'s base case, `c` references only the
  path's start variable, by property.

**shortestPath is not a pushdown case.** In a clause with a shortestPath, the
conjuncts that reference the path, its relationships or nodes, or the
in-path variables belong to `PathSpec.predicates`. They are evaluated
**before** the per-(start, end) pick, because that is required for
correctness (#1312), not as an optimization. The binder routes them there.
A predicate that cannot be evaluated inside the search is `Unsupported`.

Pushdown is never needed for correctness, and the test suite runs every query
with pushdown both enabled and disabled. Conjuncts cannot be named into the
wrong scope (#1304, #1308) or the wrong role (#1302), because a conjunct
carries `VarId`s and `Col`s, not names.

### 4.9 OPTIONAL MATCH as one unit

`Optional { input: I, inner: Q, correlation: C }`, where:
- `C` is the input variables the pattern shares, plus the input variables
  its WHERE references;
- `Q` is a MATCH clause's join over the pattern, with the WHERE inside it.

`Q` reads `C` from a **drive** relation `D = SELECT DISTINCT C-columns FROM I`.

Lowering: `I LEFT JOIN (Q over D) ON I.C = Q.C`.
- Pattern-shared variables join with plain equality. A NULL pattern variable
  cannot match, which is the correct Cypher behaviour.
- Variables referenced only by the WHERE join with
  `isNotDistinctFrom`, so that a NULL input value still reaches the WHERE.
  Verified on ClickHouse 26.7: a NULL key matches the NULL drive row.

**NULLs for every column type.** With `join_use_nulls = 1`, ClickHouse fills
unmatched `Array`, `Tuple` and `Map` columns with defaults (`[]`), not NULL.
This was verified, and Neo4j returns NULL for `r`, `size(r)` and `nodes(p)`
of an unmatched OPTIONAL path.
- So `Q` exports a `__matched` constant `1`, and the outer query reads every
  inner column of a non-nullable type as `if(__matched IS NULL, NULL, col)`.
- A composite identity tuple is exported as its component columns, each of
  which is Nullable.
- `nullable: true` on a binding is what tells the expression lowering to use
  these guarded reads.

**Cheaper forms when they are provably equivalent:**
- When `C` is the pattern's anchor alone, the WHERE references no other
  input variable, **and the anchor's relation is unique on its identity**,
  `D` is the anchor's own element relation and needs no `DISTINCT` (the #479
  form, generalized to any pattern).
  - An own-table node scan is unique on its identity.
  - A denormalized role column, or an `Alternatives` relation, is not, and
    keeps the `DISTINCT` drive.
- When the WHERE has conjuncts that reference only input variables, they may
  move into the `ON` clause instead of `Q`. ClickHouse 26.7 handles
  one-sided conditions in a LEFT JOIN ON correctly, which was checked.

This is correct for:
- multi-hop patterns (#1235);
- paths (#1306);
- union alternatives (#1249, one LEFT JOIN onto the union);
- WITH-carried anchors (#1190, #1305);
- coupled layouts (#504).

The #597, #611 and #614 provenance tags are not needed, because the WHERE is
never separated from its clause.

**Cost.** `I` becomes a CTE referenced twice. ClickHouse inlines CTEs, so `I`
is evaluated twice. The anchor-only form above avoids that in the common case.
The rest is measured in slice 3 against the legacy SQL on the social
benchmark and LDBC SF1 (§8).

Implemented in S5 (`Lowerer::optional_match`):
- `Q` is a CTE `optional_o{k}`, built in a segment of its own by the MATCH
  lowering, with the clause's WHERE inside it. The rows so far LEFT JOIN it
  on `C`, and the introduced variables are read from its columns.
- **Anchored form.** `C` is only nodes the pattern shares (any number,
  including none, which joins `ON 1 = 1`). `Q` holds the pattern's matches
  in the whole graph and no drive is built. That is used only when `Q` is
  bounded: one relationship (at most an edge table), no shared node, or a
  shared node restricted by the input's WHERE (below). Otherwise a
  multi-hop `Q` can be far larger than the result (every two-hop path of
  the graph; the review ran ClickHouse out of memory at 18 GiB), and the
  drive form is used. A shared node is in `I`, so it
  exists, and `Q` does not need its table: when `Q` reads nothing of it but
  its identity, it is read from the endpoint column of its first
  relationship in the pattern. A match whose endpoint is no input node joins
  no row.
- **Restriction from the input.** Some conjuncts of the input's WHERE read
  only a shared node's own columns, as an operator tree over columns,
  literals and parameters (no function call, so no `rand()`). Such a
  conjunct is copied into `Q`, which then scans the node's table. A match it
  removes has a shared node no input row has, so it joins no row.
- **Drive form.** It is used when `C` contains a relationship, or a variable
  that only the WHERE or the property maps read.
  - `I` becomes a pass-through CTE. A segment that is only a WITH's CTE is
    used as it is.
  - `D` is `optional_d{k}`, a `SELECT DISTINCT` of `C`'s identity and
    endpoint columns plus the properties the clause reads.
  - A relationship identified by its endpoints (no `edge_id`) can have
    parallel edges with other properties. It also joins on the properties
    `D` holds, NULL-safely; otherwise each parallel edge got the other's
    matches.
  - A value that only the WHERE reads joins NULL-safely, as
    `(x = y OR (x IS NULL AND y IS NULL))`, a form every dialect joins on.
    An element only the WHERE reads joins that way when its binding is
    nullable.
- **Matches nothing.** A pattern that cannot match (empty label inference, a
  label mismatch, a shared element that matches nothing) needs no join. The
  introduced variables are NULL. A variable that matches nothing and is only
  read by the WHERE is NULL inside `Q` (`WHERE b IS NULL` holds).
- **NaN.** A NaN value read only by the WHERE does not equal itself, so the
  NULL-safe key may not join it. The review saw this once in about 30 runs.
- **NULL elements.** For a nullable binding, what is constant for a matched
  element is guarded by its identity: `b:User`, `type(r)`, and `a = b` across
  labels give `CASE WHEN id IS NULL THEN NULL ELSE … END`. `labels(b)` is
  refused, because a ClickHouse array cannot be NULL.
- **Later clauses.**
  - A later MATCH of a nullable variable adds `id IS NOT NULL`, so `MATCH (b)`
    on its own drops the row.
  - A tie whose later relation is `Q`'s LEFT JOIN goes to WHERE, not to that
    ON.
- **List-typed properties.** ClickHouse fills an unmatched row's
  `Array`/`Map`/`Tuple` columns with defaults even under `join_use_nulls`.
  `Q` exports only identities, endpoints and properties, so an unmatched
  element's list-typed property reads `[]`, as on the legacy path. Without
  property types the lowering cannot tell which properties are lists. The
  `__matched` guard sketched above would produce a `Variant`, and only under
  `use_variant_as_common_type`.
- **Measured cost**, social benchmark at scale 100 (100K users, 10M
  follows), median of 5 runs:

  | Shape | Legacy | New |
  |---|---|---|
  | Anchor restricted by its own WHERE (`user_id < 100`, `= 42`), 1 or 2 hops | 16–51 ms | 5–32 ms |
  | Unselective anchor, 1 hop, 10M rows | 32 ms | 49 ms |
  | Two chained OPTIONALs, unselective | 71 ms | 109 ms |
  | WHERE on another variable than the anchor (`MATCH (a)-->(b) WHERE a…  OPTIONAL MATCH (b)-->(p)`) | 8 ms | 33 ms |
  | Two hops from a WITH-carried anchor (`… WITH x OPTIONAL MATCH (x)-->(b)-->(c)`, drive) | 43 ms | 87–97 ms |
  | Two hops after an earlier OPTIONAL (drive) | 69–80 ms | 111–139 ms |

  The last row is `Q` unrestricted: the input's restriction does not reach
  the anchor through its own columns. Restricting `Q` by `anchor IN (SELECT
  … FROM I)`, or using the drive, was slower in every measured shape (68 ms
  and 53 ms there), because `I` is evaluated again. Choosing the form from
  table statistics belongs to P-5. The legacy SQL is faster there because it
  is not a unit: it LEFT JOINs each hop, which is wrong for multi-hop
  patterns (#1235) and WHEREs over the optional variables.

### 4.10 WITH, aggregation and exports

`Project` / `Aggregate` lower to a CTE whose columns are **the output scope**.
The CTE's column list is the scope, so a reader cannot guess wrongly
(root A of P-4b).

A carried node exports its identity columns plus the properties that later
clauses actually use, found by a **demand pass** over the bound plan. The pass
walks backwards and collects `Ref(VarId, prop)`. A whole-entity RETURN or a
`properties()` call demands all properties.
- Exporting what is demanded, rather than joining the node table back, is
  required for denormalized nodes, which have no table of their own.
- Labels travel as part of the binding (static), and as a column only when
  the binding's label set has more than one member.

Aggregation keys are the non-aggregated items. An aggregated node or
relationship is grouped by its identity columns, never by a placeholder
(#1222).

### 4.11 Paths and shortestPath

`PathScan` lowers through the existing recursive-CTE generator
(`generate_vlp_cte_via_manager` and its strategies) behind one clean call. It
takes:
- the edge's `EdgeAccessStrategy`;
- the endpoints' `NodeAccessStrategy`s;
- the `PathSpec`;
- an optional pushed-down start predicate (§4.8 d).

It returns the column contract `start(Vec<Col>)`, `end(Vec<Col>)`,
`edges` (the identity list), `nodes`, `hop_count`, and the property arrays
the demand pass requests (`nodes.name`, `edges.weight`, …).
`PathSpec.predicates` (§4.8) are evaluated inside the search.

**The generator is not a clean call today, and slice 6 starts by fixing
that.** It has three side channels:
- `CteManager` sets `from_alias` from the query-wide `vlp_from_alias()`
  (`cte_manager/mod.rs:653, 2050`).
- It returns `outer_where_filters` that "must be applied in outer SELECT".
- `cte_extraction.rs:5666` registers `vlp_composite_id_components` for the
  printer to read.

Each becomes an explicit output of the call (an alias chosen by the caller,
filters returned as `BoundExpr` conjuncts, composite components in the
column contract), or the path is `Unsupported`.

The generator never sees the surrounding hops, the WITH CTE or the outer
filters. The filters that used to be pushed in by `categorize_filters` are
pushdown decisions (§4.8). shortestPath keeps the generator's pick, which is
partitioned by (start, end) (#1183).

S6 is split in four: S6a variable-length relationships and `length(p)`,
S6b shortestPath with in-search predicates (#1312), S6c path and list values
(`nodes(p)`, `relationships(p)`, a `-[r*]->` list, `RETURN p`, `WITH p`), S6d
a shortestPath's path as a value.

Implemented in S6a (`bound_plan/lower/path.rs`, `Lowerer::path_scan` /
`build_path`):
- **The call.** `path_cte` builds the `PatternSchemaContext` of the edge and
  its endpoints and calls `CteManager::generate_vlp_cte` with every input
  explicit: the hop range, the CTE name (`vlp_v{N}_path`, unique per
  relationship variable), the relationship identity, the start conjuncts
  (SQL over `start_node`), and the relationship's property map (SQL over
  `rel`). The relation is joined under the variable's own alias. Its column
  contract is `start_id`, `end_id`, `hop_count`, `path_edges`, `path_nodes`.
  The side channels are unused: the reported FROM alias is ignored,
  `outer_where_filters` is refused, and composite ids are refused (S8), so
  no composite components are registered. Lowered: one type whose edge
  joins one label to itself, standard layout, no `filter:` / view
  parameters / FINAL on the edge or node table, single-column ids,
  directed (undirected paths: S7a, §4.6 `Alternatives`).
- **Labels.** Every node of a path of one or more relationships has the
  edge's label. From a node of another label only the path of none is left
  (`(a:Post)-[:FOLLOWS*0..2]->(b)` is `a` itself): the relation is generated
  for `*0..0`. Label inference already empties the other cases.
- **Ties.** `start_id` and `end_id` are tied to the endpoint nodes like any
  element (§4.6.2), so a closed path `(a)-[*]->(a)`, a path after a fixed hop
  or a WITH, and two paths chained or fanned in need no special case
  (#1310, #1210, #1300 on the standard layout, #1177).
- **Where the walk starts.** At a restricted end, the left one when both
  are, ranked (without statistics, P-5): an end carried from a CTE (a WITH,
  the OPTIONAL drive); then one with conjuncts over its own columns; then
  one tied to the rows so far. From there it follows the relationships
  forward or backward (the generator walks the edge with `from_id`/`to_id`
  exchanged), so `start_id` is the walk's first node. Inside the walk's
  first step go:
  - the conjuncts of the clause and the segment that read only the first
    node's own columns (operator trees over columns, literals and
    parameters, as in §4.9), rewritten to `start_node`;
  - when the rows so far restrict the first node, `start_node.id IN (SELECT
    DISTINCT <its identity> FROM <rows> WHERE <their conjuncts>)`, where
    the rows are the relations tied, directly or through others, to the
    first node's (`Lowerer::rows_holding`). A cross-joined relation
    restricts nothing and is left out. Every value the result can have is
    in it, so it is a restriction, never a filter of results.

  Each stays in the outer query too. Without them every walk starts at
  every node: 7–8 s at scale 100, or out of memory, where the restricted
  walk takes 70–280 ms.
- **Unbounded.** A missing maximum (`*`, `*2..`) is unbounded. The generator
  is given a bound beyond any recursion (`i32::MAX`), so the walk ends when
  no trail extends, or ClickHouse fails at `max_recursive_cte_evaluation_depth`
  (the server's `max_cte_depth`). The legacy path cuts at 5 hops (the
  generator's `DEFAULT_MAX_HOPS`, 3 for `*0..`) and silently drops longer
  paths: `*1..` on `social_integration` returns 367 rows where Neo4j returns
  1821 (#1329).
- **Uniqueness (§4.6.3).** Per MATCH clause, between relationship-bearing
  elements of one edge table: a fixed relationship is not on a path
  (`NOT has(p.path_edges, <its identity>)`), and two paths share none
  (`NOT hasAny(p1.path_edges, p2.path_edges)`). A relationship's identity is
  its `edge_id`, else its stored `(from, to)` pair in that order whichever
  way a walk follows it: it is passed to the generator as the identity, and
  a hop's is spelled the same way (`spell_edge_identity`), so every element
  of a table spells a relationship alike. Within a path the generator's
  `NOT has(vp.path_edges, …)` keeps the walk a trail. A path of `*0..0` has
  no relationship.
- **A path as a value**: S6c, below.
- **`length(p)`.** The number of fixed relationships of `p` plus each path's
  `hop_count`. For an OPTIONAL `p` it is NULL when the clause did not match
  (guarded by the identities of its introduced elements, which the
  OPTIONAL's `Q` exports even when anonymous). `OPTIONAL MATCH p = …` now
  parses; the legacy planner refuses it loudly.
- **OPTIONAL MATCH.** A pattern with a path and a shared node uses the drive
  form: the anchored form's copied restriction need not reach the node the
  walk starts at (the review ran ClickHouse out of memory on one), and the
  drive holds every shared node.
- **Join order.** ClickHouse builds a hash table of every joined relation and
  streams the FROM rows through them. A path relation is joined first
  (`Lowerer::path_first`), the others after a relation they are tied to, and
  each ON conjunct moves to the later of its relations (§4.6.4: every order
  is correct because ties are equalities; only inner joins are reordered).
  Joined last, a path of 10⁸ rows took 9.4 s; first, 1.3 s.
- **Measured cost**, social benchmark at scale 100 (100K users, 10M
  follows), median of 5, with ClickHouse's cache of join sizes from earlier
  runs off (`collect_hash_table_stats_during_joins = 0`; with it, repeated
  runs of either path get faster):

  | Shape | Legacy | New |
  |---|---|---|
  | `(a {user_id: 1})-[*1..2]->(b)`, `*1..3`, `*0..2`, `length(p)`, ORDER BY / LIMIT, after a WITH | 50–122 ms | 65–131 ms |
  | `(a)-[*1..2]->(b) WHERE a.user_id < 10` | 63 ms | 76 ms |
  | after a fixed hop, backward, OPTIONAL (with or without a WHERE), restricted only at the right end | 6.9–8.0 s | 72–133 ms |
  | the start in an earlier clause, a WITH or a comma part; hop then path restricted at the end; backward path then hop | out of memory | 73–282 ms |
  | `WITH c MATCH (c)-->(a)-[*1..2]->(b)` (10⁸ paths) | 7.4 s | 1.4 s |
  | a path then a fixed hop / then a `*1..1` path | 82–89 ms | 128–249 ms |
  | `-[*2]->` from 3 starts | 9 ms | 67 ms |

  The second-to-last row: the later path's semi-join evaluates the rows so
  far again, the first path included (ClickHouse inlines CTEs). The last
  row: the legacy path writes an exact range as fixed hops. That expansion
  is future work (deferred until after S6).

Implemented in S6b (`path::search_cte`, `path::reached_cte`,
`path::pick_cte`, `Lowerer::shortest_relation`), checked against Neo4j 5.26:
- **The pattern.** One variable-length relationship between two node
  variables, as Neo4j requires. A fixed-length one (`shortestPath((a)-->(b))`)
  and a path from a node to itself (`(a)-[*]->(a)`, which Neo4j fails at
  run time) are refused.
- **Conditions in the search (§4.8, #1312).** The WHERE conjuncts that read
  the path's relation (its length, its relationships) hold of the paths the
  search picks from: the pick is the shortest path *that satisfies them*.
  They are found on the lowered conjuncts by the aliases they read, so a
  path read inside a function or a `CASE` counts; a conjunct with a part
  whose reads are not visible (a subquery, raw SQL) is refused. They may
  read only the path and its two ends: a pick for each row of another
  element would not be per pair of ends, so that is refused, as is an end
  carried from a CTE. Conjuncts over the ends only stay outside: for one
  pair of ends they hold of every path or of none.
- **Uniqueness.** Neo4j searches a shortest path on its own: another
  relationship of the same MATCH may be on it (`(a)-[r1]->(x),
  p = shortestPath((a)-[*]->(b))` gives the same rows as two MATCH clauses,
  and differs from adding `NOT r1 IN relationships(p)`). So the shortest
  path takes no part in §4.6.3.
- **The search.** A breadth-first search from each first node
  (`vlp_v{N}_bfs`). ClickHouse gives each step of a recursive CTE only the
  rows of the step before, so each step carries the nodes reached so far
  (`new = 0`) with those reached first at this depth (`new = 1`), and a node
  already reached is not reached again (`(start_id, node) NOT IN (…)`). It
  visits each node once per first node and ends when a depth adds none, or,
  when the last node is restricted (its own conjuncts, the rows that hold
  it), once a first node has reached every value the last node can have:
  on a 1,500-node chain the search from 0 to 3 is three steps where the
  whole chain would exceed ClickHouse's recursion limit (100 by default,
  the server's `max_cte_depth`), which is where an end more than about 98
  steps away still fails, loudly. A node is reached through a relationship
  that satisfies the property map, and exists in the node table. The first
  nodes are restricted as in S6a; an end whose identity equals a constant
  is preferred as the first node (it is one node).
  - `shortestPath`: one row per pair of ends, at its distance.
  - `allShortestPaths`: the search counts the shortest paths to each node
    (the sum over the relationships reaching it from one level nearer, in
    `UInt256`), and a pair's row is repeated that many times
    (`ARRAY JOIN range(accurateCast(paths, 'UInt64'))`: a count beyond
    `UInt64` fails, it does not wrap). The paths of a pair differ only in
    their nodes and relationships, which are values only in S6c. Walking
    back over the levels instead enumerates the paths, but ClickHouse
    evaluates the search again in every step of the walk: 1.2 s for one
    pair at scale 100, against 0.23 s counted. (S6d walks back inside one
    recursive CTE that reads the search once, and only when the paths are
    read.)
- **How the conditions apply.** In S6b a condition depends on a path only
  through its length, so:
  - a bound from above (`length(p) < k`, `<= k`, `= k`, either way round,
    `k` an integer) bounds the search; `=` stays a condition too, and a
    bound below the range leaves no path;
  - a lower bound of the range above 1 is the condition
    `length(p) >= min` (Neo4j rejects such a range; the legacy path takes
    it as the shortest path of at least that length, #1205);
  - a pair whose distance satisfies the conditions has its shortest paths
    (no path is shorter) (`vlp_v{N}_near`, then the first arm of
    `vlp_v{N}_shortest`);
  - for the other pairs (`NOT coalesce(<conditions>, false)`) the pick is
    among the trails of the range that satisfy them (the S6a relation,
    walked only from those pairs' first nodes): the shortest
    (`ROW_NUMBER`) or all of the shortest length (`MIN … OVER`), per
    `(start_id, end_id)`. A condition can make a trail that revisits a node
    the shortest that satisfies it (`length(p) >= 2` from 1 to 3 over
    1→2→5→1→3, as Neo4j answers; `*0..` with `length(p) > 0` from a node
    back to itself). That search is exhaustive, as Neo4j's fallback is:
    on a large graph it is as costly as the trails of those pairs are many.

  The ends a condition reads are joined to the pairs under their own
  aliases.
- **One node at both ends.** From 0 the path of none (or the shortest closed
  trail that satisfies the conditions). From 1, no path: Neo4j fails such a
  row with "The shortest path algorithm does not work when the start and end
  nodes are the same", and names `cypher.forbid_shortestpath_common_nodes`
  as the setting that accepts missing results for those rows instead. The
  new path has no run-time error for it and leaves the row out.
- **Relationships without `edge_id`.** A relationship's identity is its
  stored pair (S6a), so two rows with the same ends are one relationship to
  the trails, which no trail uses twice, while the search counts both. The
  legacy path does the same (#1331).
- **Dialect.** The spelling comes from `FunctionMapper::shortest_path_search`
  (ClickHouse); other dialects keep the legacy path.
- **Measured cost**, social benchmark at scale 100, as in S6a:

  | Shape | Legacy | New |
  |---|---|---|
  | both ends pinned, `length(p)`; with `length(p) < 10`; `> 1`; `*..5` and `> 1` | 329–348 ms (its own BFS, cut at 5 hops) | 128–203 ms |
  | both ends pinned, `*..3`; and with `length(p) > 1` | 165–185 ms | 122–205 ms |
  | both ends pinned, `a.name, b.name` | error (Code 47) | 116 ms |
  | one start to every end; three starts; starts from a WITH; 10K starts by a property to a pinned end | out of memory or error | 195–340 ms |
  | `allShortestPaths`, pinned pair / one start to every end | error | 178 / 274 ms |
  | `*..3` from one start with `length(p) > 1` (the direct pairs need trails) | 116 ms (wrong answer, #1312) | 424 ms |
  | OPTIONAL, both ends pinned; with `length(p) > 1` | refused | 173 / 243 ms |

Implemented in S6c (`bound_plan/lower/value.rs`, `Lowerer::graph_item`),
checked against Neo4j 5.26:
- **The form.** A value is in Neo4j's own JSON form (its Query API's): a
  node is `{elementId, labels, properties}`, a relationship `{elementId,
  startNodeElementId, endNodeElementId, type, properties}`, a path the list
  of its nodes and relationships in turn. The element ids are ClickGraph's
  (`Label:id-`, `TYPE:from->to-`), which the SQL and Bolt spell from one
  place (`graph_catalog::element_id`); the properties are the declared ones
  that are not NULL (Neo4j stores no NULL property).
  - HTTP rows carry the value as it is.
  - Bolt decodes it by the item's type (`ResultKind::Graph`: node,
    relationship, path, list) into Node, Relationship and Path structures
    (`bolt_protocol::graph_values`).
  - The graph output and embedded `query_graph` take its nodes and edges.

  On ClickHouse an element is a `Map(String, Dynamic)`, so one list holds
  nodes and relationships of any label (`FunctionMapper::graph_values`).
  Other dialects keep the legacy path.
- **Where a value comes from.** A fixed node or relationship's value is
  built from its columns (its table, or the CTE that carried it). A
  variable-length relationship's nodes and relationships are carried through
  its search: asked for them (`PathValues`), the generator accumulates
  `path_node_values` / `path_rel_values` step by step, as it does
  `path_nodes`, in the standard layout's arms.
  - The demand pass asks only for what a value reads: `nodes(p)` the nodes,
    `relationships(p)` and a `-[r*]->` list the relationships, `p` both. A
    WITH item that passes a path or list through reads nothing itself:
    only what later clauses read of it is asked for (`WITH p RETURN
    count(*)` builds no value).
  - The walk's order is reversed when the walk starts at the pattern's
    right end, or runs against the pattern's direction (a closed
    `(a)<-[*]-(a)`).
- **Where a value may appear.** `p`, `nodes(p)`, `relationships(p)` and a
  `-[r*]->` list may be RETURN and WITH items. `size()` of them is counted
  from the path, also in a WHERE, without its values: `length(p) + 1`,
  `length(p)`, `hop_count`. `size()` of a list a WITH carried is the
  list's length.
- **Identity.** ClickHouse refuses `Dynamic` in GROUP BY, and two values are
  equal when their elements are. So DISTINCT and grouping by a value go by
  one key (`Lowerer::path_key`), and the value is `any()` of its group
  (`Body::determined`). The key is one list of texts in path order, each
  with its label or type:
  - for `p`, its first node and every relationship (they determine the
    other nodes);
  - for `nodes(p)`, every node;
  - for `relationships(p)`, every relationship.

  One list, not a key per element: two variable-length parts can split
  one path in several ways (`*1..2` then `*1..2`), which Neo4j counts as
  one path. Two empty lists are equal. A `-[r*]->` list is keyed by its
  `path_edges`; a WITH that carries one is grouped by them. A WITH that
  carries a path and is DISTINCT or aggregates is grouped by the path's
  key, and the path's elements are `any()` of each group.
- **WITH.** A path variable carries its elements: their identities, every
  property of a fixed element, and the path relation's values. The next
  segment builds the path's value and length from them. A computed list
  (`WITH nodes(p) AS ns`) carries its value and its keys.
- **NULL.** The path of an OPTIONAL MATCH that did not match is NULL, and so
  are its lists and a `-[r*]->` list. The value is a `Dynamic` NULL, since
  an array cannot be NULL. Whether the clause matched is tested on a
  nullable element's identity, as for `length(p)`. All such rows have one
  NULL value: their key is that test and an empty list, so DISTINCT gives
  one NULL row, not one per input row. A carried `-[r*]->` list exports the
  test too, so NULL is not grouped with an empty list.
- **Properties.** A label or type with no declared properties has an
  empty property map, spelled `CAST(CAST(map(), 'Map(String, String)'),
  'Map(String, Dynamic)')`: ClickHouse cannot cast an empty `map()` straight
  to a `Dynamic` value type.
- **OPTIONAL drive.** When an OPTIONAL MATCH's WHERE reads a `-[r*]->` list,
  the clause's matches are joined on the list's identity: its first node and
  its relationships. They used to be joined on its ends and length, which two
  paths can share. The drive holds no values.
- **The legacy path** returns ids for `nodes(p)`, type names for
  `relationships(p)`, and for a variable-length `RETURN p` a Path of its two
  ends with no properties.
- **Not lowered:**
  - a list inside another expression (an index, `head()`, `IN`, a
    comprehension, `collect()`: S7 lists);
  - ORDER BY a value;
  - a carried list read by an OPTIONAL MATCH;
  - composite ids (S8);
  - other dialects.
- **Measured cost**, social benchmark at scale 100, as in S6a. The legacy
  path builds no values (ids, type names or a marker tuple), so its times
  are of less work:

  | Shape | Legacy | New |
  |---|---|---|
  | `(a {user_id: 1})-[*1..2]->(b)` (10K paths): `length(p)` / `nodes(p)` / `p` | 50 ms | 67 / 91 / 106 ms |
  | the same `*1..3` (1M paths): `length(p)` / `relationships(p)` / `p` | 92–99 ms | 108 / 239 / 1,066 ms |
  | `-[r*1..2]->` from 9 starts, `RETURN r` (100K lists) | 61 ms | 92 ms |
  | two fixed hops, `RETURN p` (10K) | 31 ms | 46 ms |
  | `*1..2` to a pinned end, `RETURN p` | out of memory | 177 ms |
  | `RETURN DISTINCT nodes(p)` / `WITH p RETURN p` (10K) | 141 / 53 ms | 185 / 111 ms |
  | OPTIONAL to a pinned end from 9 starts, `RETURN p` | refused | 131 ms |

  Most of the cost of the 1M case is building the `Dynamic` maps. Carrying
  typed tuples through the search and building the maps at the end takes
  0.88 s instead of 1.07 s; that is left for later.

  Large results cost memory. For `WHERE a.user_id < 3` with `*1..3` (2.9M
  paths), the peaks are:
  - `length(p)`, and `WITH p RETURN count(*)`: 0.2 s, 0.3 GiB;
  - `relationships(p)`: 0.6 s, 3.4 GiB;
  - `p`: 3.1 s, 10.2 GiB;
  - `DISTINCT nodes(p)`: 14.9 s, 33.3 GiB;
  - `DISTINCT p`: 22.3 s, 39.2 GiB.

  DISTINCT keeps `any()` of each group's value, while grouping by the keys
  alone takes 1.8 s. `LIMIT 1 BY <key>` would stream instead, but
  `RenderPlan` has no LIMIT BY (#1335). A LIMIT short-circuits all of
  these.
- **Parallel relationships.** A relationship's element id ignores its
  `edge_id` (#1334), so two parallel relationships share one: Bolt gives
  them one id, and the graph output keeps one. A Bolt Path keeps every
  relationship it steps over, as a path never repeats one.

Implemented in S6d (`path::walk_ctes`, `Lowerer::shortest_relation`),
checked against Neo4j 5.26:
- **When.** A shortest path's paths are recovered only when they are read:
  its values (`p`, `nodes(p)`, `relationships(p)`, its `-[r*]->` list, asked
  for as in S6c), or its identity (DISTINCT or grouping by the path or its
  list, in a RETURN or a WITH; the demand pass's `#key`). Otherwise the S6b
  relation is unchanged: `length(p)`, the ends, a `WITH p` that passes it
  through. Only a recovered relation has `path_nodes`; a WITH exported it
  anyway (`WITH p RETURN length(p)` failed with Code 47). A `shortestPath`
  has one path per pair, so its ends would identify it without a walk; it
  is walked anyway, for one key form with every other path (one start to
  every end, `WITH DISTINCT p`: about 1.4 s against 1.0 s).
- **The walk.** The search keeps each node's distance from each first
  node, its level. A shortest path to a node at distance `d` steps back to
  a node at `d - 1` with a relationship to it, and on to the first node.
  The walk (`vlp_{var}_walk`) is one recursive CTE that reads the search
  once: its first rows are the search's (`level = 1`), the paths' last
  nodes marked (`frontier = 1`); each step moves every path one level back,
  prepending the node and the relationship it leaves (their identities, and
  the values asked for), and carries the levels along while a path has more
  than one step left. A walk that read the search inside its steps would
  evaluate it once per step (S6b).
  - `shortestPath`: the search keeps a parent of each node (`min` of the
    nodes one level nearer with a relationship to it), and the walk follows
    the parents: one path per pair, and a value is built only for it. A
    walk through every relationship, picking one per step, built a value
    for every candidate first: over 60 s for one start to every end at scale
    100.
  - `allShortestPaths`: the search keeps each node's parents, once each
    (an array), and the walk follows every parent by every relationship from
    it: a row per path. The search then counts nothing and no row is
    repeated. Joining every relationship into a node and keeping those from
    the level before multiplied the paths by the in-degree first: the 940K
    paths of `*..3` from one start at scale 100 ran out of memory (56 GiB).
    `shortestPath` keeps a single parent, not an array: read through an
    `ARRAY JOIN`, it made a pinned pair's walk twice as slow.
  - Each step reads only the relationships into the frontier's nodes:
    joined to the whole edge table, one pair's walk cost about 1.6 times as
    much.
  - Each step's relationship satisfies the property map, as the search's
    did.
- **Which path.** Neo4j returns any one of a pair's shortest paths. The
  walk takes the least parent at each level and, of parallel relationships,
  the least identity, so a query gives the same path every time.
- **Conditions (§4.8).** The pairs whose distance satisfies the conditions
  are walked, and only they (`vlp_{var}_walked`). The trails of the others
  (S6b) carry their values as S6a's do, and the pick unions the two.
- **Identity.** A recovered path's `path_edges` holds each relationship's
  identity as a text (`toString` of its `edge_id` or of its stored pair):
  the walk's first rows hold an empty list, which needs a type, and a key
  reads the text anyway (`Lowerer::path_key`). The trails' are converted. A
  shortest path takes no part in uniqueness (S6b), so its identities meet
  no other relation's.
- **Not lowered:** lists in other expressions and undirected patterns
  (S7), the other S6c refusals.
- **Measured cost**, social benchmark at scale 100, as in S6a. The legacy
  path returns a path of the two ends without properties:

  | Shape | Legacy | New |
  |---|---|---|
  | both ends pinned: `length(p)` / `p` / `nodes(p)` | 292–297 ms (`p`: the two ends) | 114 / 390 / 359 ms |
  | `allShortestPaths`, both ends pinned (8 paths): `count(*)` / `p` / `DISTINCT nodes(p)` | out of memory | 137 / 462 / 466 ms |
  | one start to every end (100K pairs): `length(p)` / `WITH DISTINCT p` / `RETURN p LIMIT 10` | out of memory | 213 / 730 / 962 ms |
  | `allShortestPaths` of `*..3` from one start (940K paths): `WITH DISTINCT p` / `RETURN p LIMIT 1` | out of memory | 566 / 737 ms |
  | three starts to a pinned end, `RETURN p` | out of memory | 349 ms |
  | both ends pinned, `*..3` with `length(p) > 1`, `RETURN p` | 168 ms (wrong answer, #1312) | 724 ms |
  | OPTIONAL to a pinned end, `RETURN p` | refused | 361 ms |

  A LIMIT does not reach into the walk: `RETURN p LIMIT 10` from one start
  recovers all 100K paths. With conditions, the trails of the failing pairs
  build their values before the pick (724 ms, 205 ms without values), as
  S6c's do.
- **Relationships without `edge_id`.** Their identity is their stored pair
  (S6a), so DISTINCT counts two paths through parallel rows once:
  `WITH DISTINCT p` gives 10,376 of the 10,379 `allShortestPaths` of `*..2`
  from one start at scale 100 (#1331).

### 4.12 Subquery expressions

`Apply { kind, sub, correlation }`, where `sub` is a bound MATCH over the
correlation variables:

| Construct | Lowering |
|---|---|
| `EXISTS` / pattern predicate | semi-join (`IN` or `EXISTS` on the correlation identity) |
| `NOT` | anti-join |
| `size()` / `COUNT{}` | `LEFT JOIN` of a grouped count, with `coalesce(…, 0)` |
| pattern comprehension | `LEFT JOIN` of a grouped `groupArray` |

`Mark(VarId)` is the general form: a boolean column, for `EXISTS` used under
`OR`, `CASE` or in `RETURN`. Semi and Anti are its specializations when the
predicate is a top-level conjunct.

ClickHouse constraints, verified on 26.7, that the lowering must respect:
- **Decorrelate everything.** A doubly nested correlated subquery that
  reads the outermost table fails with `NOT_FOUND_COLUMN_IN_BLOCK` (Code
  10). So every `Apply` lowers to a join against a grouped or distinct
  relation keyed on the correlation identity, never to a correlated
  subquery.
- **Guard NULL correlations.** `NOT (k IN (…))` with a scalar NULL `k` is
  NULL and drops the row, whereas Neo4j's `NOT EXISTS {(x)-->()}` is true for
  a NULL `x`. The anti and mark lowerings guard a nullable correlation
  identity explicitly (`k IS NULL OR …`) and do not rely on
  `transform_null_in`.

The correlation set is explicit, so the alias guessing and the
"outer alias" task-local of today are not needed.

**Implemented in S9** (`bound_plan/expr.rs` `hoist_subqueries`,
`binder.rs` `bind_subquery`, `bound_plan/lower/subquery.rs`):
`EXISTS { [MATCH] pattern [WHERE …] }`, a pattern predicate, `NOT (pattern)`,
`exists(pattern)`, `size(pattern)` and `size([pattern …])`, pattern
comprehensions of values.
- **Binding.** Before an expression is converted, each subquery in it is
  taken out of the AST and bound as a MATCH over the expression's scope:
  a variable of the scope is bound before it, the others are its own. The
  expression keeps a placeholder call `__cg_subquery(<value>, <correlation>…)`
  whose arguments are the subquery's value binding and its **correlation**
  (the scope's variables it reads, nested subqueries' included), so every
  existing check of what an expression reads (grouping keys, DISTINCT /
  aggregation visibility, an OPTIONAL MATCH's WHERE-only variables) sees
  them. The bound statement lists its `Subquery`s (`types.rs`). As in
  Neo4j, a pattern predicate may not introduce a variable, and a
  subquery's variables are not visible outside it. `size(pattern)` is
  Cypher 4's (Neo4j 5 rejects it and spells it `COUNT { … }`); it is
  lowered as that count, as legacy does.
- **One form for every construct.** A subquery is a `Mark` in §4.5's terms:
  a value the expression reads. Before an expression is lowered, the
  subqueries it reads are applied to the current rows: `Q`, the subquery's
  MATCH with its WHERE inside (as an OPTIONAL MATCH's, §4.9), grouped by the
  correlation columns into `count(*)` or the list of its projection, LEFT
  JOINs the rows on the correlation. One group per key, so a row joins at
  most one: rows are neither multiplied nor dropped and keep their order.
  The value is a column of the group, one operand as it is (a comparison
  of it needs no parentheses): `exists` is computed in `Q` as a boolean
  (`count(*) > 0`, `Nullable(Bool)`) and read `coalesce(…, false)`, a count
  `coalesce(count, 0)`, a list as it is (a row that joins no group reads
  `[]`). A list keeps NULL elements
  (`groupArray(tuple(x))`, `Lists::collect_all`), unlike `collect()`. No
  semi / anti join and no correlated SQL subquery.
- **Correlation.** As an OPTIONAL MATCH's: anchored on the shared nodes
  (read from their own tables, or the endpoint column of the pattern's
  relationship) unless a relationship or a value is shared or a variable
  only the WHERE reads; then a drive `D`, the distinct correlation columns
  of the rows. Unlike an OPTIONAL MATCH's, `D` reads a **copy** of the rows
  so far and the segment stays open, so a subquery is applied in the
  middle of a MATCH (after its pattern, ties, conditions and uniqueness:
  its WHERE conjuncts that read a subquery are applied last), inside an
  OPTIONAL MATCH's `Q` and inside another subquery's `Q`. `D` holds the
  correlated elements' identity / endpoint columns and the properties `Q`
  reads (nested subqueries' reads included); a key that is never NULL joins
  by equality, the rest NULL-safely (a WHERE-only variable can be NULL and
  still decide the WHERE: `EXISTS { MATCH (c) WHERE b IS NULL }`), by the
  dialect's one-condition form (`FunctionMapper::null_safe_equal`,
  `isNotDistinctFrom`: ClickHouse keys no join of three or more relations
  on `a = b OR (a IS NULL AND b IS NULL)`, Code 403). The copy leaves out
  the subqueries applied before and the conditions reading them (more
  rows, the same keys): each drive would otherwise copy every earlier one,
  doubling the cost per subquery (review).
- **NULL.** A NULL shared node or relationship is in no match (`Q`'s MATCH
  requires it not NULL): false, 0, `[]`, as Neo4j. A shared element that
  matches nothing, or a pattern that cannot match, is the constant.
- **Positions.** A MATCH's or OPTIONAL MATCH's WHERE; a WITH / RETURN item
  (also inside an aggregate's argument or a CASE), applied to the input
  rows; the ORDER BY / WHERE of a projection that neither aggregates nor is
  DISTINCT, when it reads only input variables; a WITH's WHERE that reads a
  subquery and only projected names is bound as a `Filter` after the
  projection (after its SKIP / LIMIT, #1311); UNWIND; a free-standing
  ORDER BY; nested subqueries.
- **Not lowered yet:** a subquery inside a list comprehension, `reduce` or
  lambda (it could read the element); in a property map; in the ORDER BY /
  WHERE of a DISTINCT or aggregating projection reading an input variable,
  or in an ORDER BY reading a projected name; a shortestPath's WHERE
  conjunct reading both a subquery and the path (the search holds it); a
  subquery reading a path, a variable-length relationship's list or a
  carried list of nodes; a pattern comprehension of nodes, relationships,
  paths or maps.

### 4.13 Result shape

The final `Project`'s scope lowers to the SELECT list, using today's column
naming (`a.prop`, `r.from_id`, …) so that the HTTP and Bolt outputs do not
change. It also produces a `ResultShape`: per output column group, the kind
(Value, Node{labels}, Rel{type, start labels, end labels, direction}, Path).
Today `extract_return_metadata(LogicalPlan, PlanCtx)` reads the
`LogicalPlan` and the name-keyed registry. It has three callers:
- the Bolt handler (`bolt_protocol/handler.rs:3132`);
- the HTTP graph output (`server/graph_output.rs:31`);
- embedded (`clickgraph-embedded/src/graph_result.rs:159`).

All three take the `ResultShape` instead when the new path rendered the
query.

Implemented in S4c:
- The shape is `Vec<ResultColumn>` (`bound_plan::lower`), one per RETURN
  item: `Value`, `Node{label}`, `Rel{type, from label, to label}`,
  `NodeId{label}`. `ReadTranslation::return_metadata()` is the one seam the
  three callers read. It converts the shape into the result transformer's
  `ReturnItemMetadata` on the new path, and runs `extract_return_metadata`
  on the legacy one.
- A relationship's columns are its stored endpoint columns, so its shape
  says `Outgoing` whatever the pattern's direction.
- The legacy transformer swaps start and end for an `Incoming` pattern,
  which reverses `(a)<-[r]-(b) RETURN r` over Bolt. The new path does not.

### 4.14 Lowering target and emitter

Lowering produces a `RenderPlan`:
- CTEs are flat, so architectural rule 1 still holds;
- WITH bodies, drive relations and inner OPTIONAL plans become CTEs;
- path CTEs are `RawSql`, as today;
- the final SELECT joins relations by `RelId`-derived aliases.

Expressions lower to `RenderExpr` with concrete `table_alias.column`, and
print through the existing dialect-aware emitter, which keeps Databricks
support.

The new path calls a **plain** emitter entry (`render_plan_to_sql_plain`). It
runs `flatten_all_ctes` and printing, and none of the repair passes (Appendix
A, groups C and D). A ratchet test forbids `src/bound_plan/` from calling any
function that edits an assembled `RenderPlan`.

**Printing is not plain today either.** It reads alias-keyed task-local state
(`to_sql_query.rs`):
- `is_string_operand` reads `get_reduce_binder_type` (:63);
- `try_rewrite_in_cte_subquery` reads `get_cte_name_for_alias` (:350);
- `render_arg_is_collection` reads the variable registry to choose
  Databricks `size` (:1062);
- `rewrite_expr_for_vlp` reads `vlp_from_alias`, `path_fixed_hops` and
  `is_vlp_composite_id_component` (:2483–2665);
- `extract_fixed_path_info_from_plan` runs inside `render_plan_to_sql`
  (:6450).

So:
- the binder's types travel on the lowered `RenderExpr`: a type annotation
  on column references and lambda binders;
- the plain entry bypasses the VLP and fixed-path rewrites;
- in debug builds it **asserts that these task-local channels are empty**,
  so a hidden dependency fails loudly instead of resolving by name.

`ViewTableRef.source` is an `Arc<LogicalPlan>`, and the emitter matches
`LogicalPlan::ViewScan` (:3372, :4252). The new path builds table references
through a constructor that does not need a `LogicalPlan`.

### 4.15 ClickHouse settings the SQL depends on (`join_use_nulls = 1`)

ClickHouse's default `join_use_nulls = 0` fills the unmatched side of a LEFT
JOIN with **type defaults**, not NULL: `''` for String, `0` for numbers,
`[]` for arrays. OPTIONAL MATCH semantics depend on getting NULL, and so do
`IS NULL`, `count(x)`, `coalesce`, and any WHERE over an optional variable.
Verified on 2026-10-06, `test_integration`, with
`MATCH (u) WHERE u.user_id IN [20,21] OPTIONAL MATCH (u)-[:FOLLOWS]->(b)
RETURN b.name, b.name IS NULL, b.age`:

| Execution path | `b.name` | `b.name IS NULL` | `b.age` |
|---|---|---|---|
| server (`RoleConnectionPool::standard_options`) | NULL | true | NULL |
| `cg query` / embedded remote (same pool) | NULL | true | NULL |
| chdb embedded (`SET join_use_nulls = 1`) | NULL (by the same setting) | | |
| the same SQL sent to ClickHouse **without** the setting | `''` | **false** | `0` |

Rules for the new path:
1. **The setting belongs to the SQL contract, not to the connection.**
   - There is one list, `sql_generator::SEMANTIC_SESSION_SETTINGS`. The
     server connection pool, the `clickhouse` client and the chdb executor
     all apply it at session level.
   - **SQL handed to a user** carries the list in the statement itself:
     `sql_generator::portable_sql` appends `SETTINGS join_use_nulls = 1` for
     ClickHouse and leaves Databricks/Spark unchanged. This covers HTTP
     `sql_only`, `/query/sql`, and embedded `query_to_sql`, which serves
     `cg sql`, FFI, Go and Python.
   - Executed SQL stays exactly what the emitter produced (done in S0.5).
   - Verified: a trailing `SETTINGS join_use_nulls = 1` applies to the whole
     statement, including recursive and WITH CTEs.
   - That makes the SQL correct wherever it runs, including the documented
     SQL-only mode (`sql_only: true`, `cg sql`) whose output users execute
     outside ClickGraph.
   - **Today that output carries no setting** (#1314). Run externally, every
     OPTIONAL MATCH in it gives `''`/`0` and false `IS NULL` results, with
     no error.
   - Executors keep setting it at session level too. `Decision 0.7` (no
     `SETTINGS` at query time) applies to embedded *writes* only.
2. **The setting covers only Nullable-capable types.** `Array`, `Tuple`
   and `Map` still come back as defaults (§4.9), so the `__matched` guard is
   required with the setting. It is emitted through `Dialect` because Spark
   does not need it.
3. **Types change under the setting.** Right-side columns become
   `Nullable(T)`, which affects:
   - `UNION ALL` arms (the arms must agree; §4.6 casts each column to one
     declared type, Nullable when any arm is on a NULL-supplying side);
   - functions that reject Nullable arguments;
   - `GROUP BY` keys.

   The lowering computes nullability from `Binding.nullable` rather than
   discovering it from ClickHouse errors.
4. **The oracle harness and every probe run with the setting**, and with a
   deliberately unset session too, to prove the SQL carries it (rule 1).
   The prototype numbers in §1.3 were taken with `join_use_nulls = 1`. Those
   six queries are `count(*)` with no predicate over an optional column, so
   they do not depend on it; slice 1's harness covers the value-level cases.

## 5. Reused, replaced, deleted

| Component | New path |
|---|---|
| Parser expression grammar, AST expressions | reused |
| Parser query-part structure | **replaced** by the clause list (§4.2); the legacy AST is derived from it |
| `logical_expr/ast_conversion.rs` (operators, functions, literals) | reused by the binder |
| `PlanCtx`, `TableCtx`, `VariableRegistry`, `VariableScope`, `ScopeContext`, the alias-keyed `QueryContext` channels | not used; deleted at cutover |
| Analyzer passes (TypeInference, BidirectionalUnion, UnionDistribution, GraphJoinInference, FilterTagging, FilterIntoGraphRel, DuplicateScansRemoving, CteReferencePopulator, VariableResolver, CteColumnResolver, …) | not used; deleted at cutover |
| `graph_catalog` (`GraphSchema`, `PatternSchemaContext`, access strategies, `SchemaFilter`) | **reused**: the only way the new path reads layouts |
| Recursive path CTE generator (`cte_manager`, `variable_length_cte.rs`) | **reused** behind the `PathScan` call (§4.11) |
| Property mapping, `ViewTableRef` (parameterized views, FINAL), composite `Identifier` helpers | reused |
| `RenderExpr`, `FunctionMapper`, `Dialect`, SQL printing, `flatten_all_ctes` | reused |
| `with_to_cte`, `join_builder`, `from_builder`, `filter_builder`, `filter_pipeline`, `plan_optimizer` repair passes, `cte_extraction` composition, `vlp_rewrite`, `hop_vlp_uniqueness`, `variable_scope.rs`, `cte_export.rs` | not used; deleted at cutover |
| Write path (`write_plan_builder`), `CALL` procedures, `COPY TO` | out of scope; unchanged |

## 6. Verification

1. **Neo4j as the reference.**
   - `scripts/oracle/` gets:
     - a loader that builds each fixture database's *logical* graph from its
       schema YAML and tables, in Python, written directly from the YAML
       rules;
     - a cross-check of node, edge and per-label counts against simple
       ClickGraph scans and raw table counts;
     - a runner that executes Cypher on `neo4j:5-community` in Docker.
   - One logical graph is loaded per layout database: standard, FK-edge,
     denormalized, polymorphic, composite id, mixed access.
   - The Python loader is a second implementation of the layout rules.
     That is the "oracle encodes the engine's assumption" risk of #1287. It
     is kept small and declarative, and each layout's loaded graph is
     checked three ways:
     - per-label node counts and per-type edge counts against raw-table
       counts;
     - a hand-listed expected graph for the small fixtures;
     - **seeded integrity cases**: a dangling edge, a self-loop, parallel
       edges, and a node with no edges, so that rules like node-scan
       elision (§4.6) and self-loop handling (§4.6) are actually exercised.
   - **Comparator rules.** These are explicit and live in one file:
     - `id()` / `elementId()` values are compared through a mapping from
       Neo4j's internal ids to the loader's logical identity, never
       literally;
     - whole entities are compared as (labels, properties);
     - paths are compared as sequences of entities;
     - when there is no ORDER BY, rows are compared as multisets;
     - shortestPath with ties between equal-length paths compares the set
       of lengths and endpoints, not the chosen path;
     - floats are compared with a tolerance;
     - temporal formatting is normalized (#1055 #1068).
2. **Result goldens.** For every corpus query and layout, the expected rows
   from Neo4j are stored once (`tests/corpus/expected/<layout>/<id>.json`,
   compared as multisets, in order when there is an ORDER BY). The live suite
   compares ClickGraph's rows with them. The SQL-text goldens stay as
   change-detectors for the legacy path only.
3. **Differential runs per slice.** For each corpus query, compare new, legacy
   and Neo4j. A slice may make a query the new path handles go from
   wrong/error to correct. It may **never** go from correct to wrong or error.
4. **Generated sweeps.** The pattern-shape generators from this year's sweeps
   (`carry*_sweep.py`, `filter_sweep*.py`, `outside_sweep.py`) are retargeted
   at Neo4j as the oracle, so "the oracle encodes a known gap" (#1287) can no
   longer happen.
5. **Invariant tests.**
   - Ratchets: no name-keyed lookup after binding (no `HashMap<String, _>`
     keyed by variable name in `src/bound_plan/` outside the binder); no
     dependency on the analyzer or render composition modules.
   - I2 is checked on every lowered plan in debug builds.
   - Every query is also run with pushdown disabled.

## 7. Migration slices

Each slice is one PR with the standard gate: fmt, clippy, `cargo test
--no-fail-fast`, the live suite, an adversarial review, and the Neo4j
differential. The routing switch is
`CLICKGRAPH_BOUND_PLAN = off | shadow | on`:
- `shadow` translates with both paths and records differences in tests;
- `on` routes supported queries to the new path.

The default stayed `off` until slice 10, which made it `on`. Slice 11's
cutover (S11j) removed the switch: ClickHouse reads go through the bound
plan only, and what it does not lower is an error; other dialects keep the
legacy fallback until their coverage (S12).

| # | Slice | Acceptance | Closes |
|---|---|---|---|
| 0 | This doc + PRIORITIES P-4c | review | — |
| 0.5 | **One translate seam.** SQL handed to users carries `SETTINGS join_use_nulls = 1` (§4.15); executed SQL is unchanged. Today there are about ten entry points that each call `evaluate_read_statement` + `to_render_plan_with_ctx`: `server/handlers.rs:1459, 2109`, `sql_generation_handler.rs:338`, `bolt_protocol/handler.rs:2374, 2670, 3108`, and `sql_generator/emitters/clickhouse/mod.rs:82, 131, 165`, which serve embedded, FFI, Go, Python and `cg`. All of them go through one function that returns SQL plus `ResultShape`. The query cache key includes the route. `$param` templating, `USE`, multi-schema selection and the depth guards are applied in the seam. | executed SQL byte-identical (corpus and goldens unchanged); SQL-only output executed on a session without the setting returns NULLs | #1314 |
| 1 | Oracle harness (§6.1–6.2) + result goldens for the corpus on the standard layout | goldens generated; **existing engine scored** (gives the baseline list of wrong answers) | — |
| 2 | Clause-list parser (§4.2) with the legacy AST derived from it | legacy SQL goldens byte-identical; new shapes parse | parse gaps |
| 3 | Binder + scope + label inference, no SQL (`cg bind` debug output) | binds the whole corpus or reports `Unsupported`; label parity with TypeInference; name-resolution tests incl. re-binding after WITH, shadowing, ORDER BY visibility | — |
| 4 | Lowering, standard layout: node and edge scans, fixed hops, comma patterns, reused relationship variables, WHERE, WITH/RETURN incl. aggregation, DISTINCT, ORDER/SKIP/LIMIT/WHERE order, `shadow` mode | Neo4j-equal on every corpus query it supports; 0 correct→wrong | #1304 #933 #1263 #1089 #1311 |
| 5 | OPTIONAL MATCH (§4.9) | as 4 + timing vs legacy | #1235 #1305 #1306 #615 #1190 |
| 6 | Paths (§4.11): make the generator call clean first, then uniqueness (§4.6.3), path functions, list bindings, shortestPath with in-search predicates | as 4 | #1310 #1307 #1203 #1210 #1300 #1292 #1178 #1177 #1312 |
| 7 | UNWIND, Cypher UNION, `Alternatives` (undirected, unlabeled) | as 4 | #1249 (with 8) |
| 8 | Layouts through `PatternSchemaContext`: FK-edge, denormalized, polymorphic, composite, mixed access, coupled | Neo4j-equal per layout | #1302 #1189 #1149 #1160 #1155 #1106 #1186 #1184 #504 #627 #1007 #924 #927 #1157 #1259 |
| 9 | Subquery expressions + pattern comprehension (§4.12) | as 4 | #615.2 #640 #1105 #1062 (semantics) |
| 10 | Default `on`; the legacy path is reachable only for `Unsupported` | full live suite green on `on`; LDBC SF1 timing within budget | — |
| 11 | Delete the legacy read path in steps (analyzer passes, render composition, the 48 repairs, the name-keyed maps), one PR per group | coverage of the corpus = 100% on the new path | — |

While P-4c is open: no new allowlist widenings or per-shape repairs in the
legacy composition code. A new silently wrong bug in a family that P-4c covers
is fixed by refusing loudly in legacy and recording a corpus entry for the
slice that will handle it.

## 8. Risks and open questions

1. **Scale of the change.**
   - New code: the binder, lowering and pushdown. My estimate for the read
     path, excluding reused leaves, is 6–9k lines.
   - Code eventually deleted: well over 50k lines (analyzer joins and
     filters, plus render composition).
   - The strangler routing and the per-slice Neo4j acceptance are what keep
     this from being a big-bang rewrite. The risk that remains is running two
     paths for a long time. Slice 11 has a coverage exit criterion, and §7's
     freeze stops the legacy path growing in the meantime.
2. **Performance.**
   - Drive relations and inlined CTEs can evaluate an input twice (§4.9).
   - The plain left-deep join order may lose some selective-anchor reorders.
   - Slices 5 and 10 measure the social benchmark and LDBC SF1 against
     legacy. Any regression beyond a stated budget blocks the slice and is
     addressed in the lowering (anchor-only form, semi-join restriction of a
     drive), never by loosening semantics.
3. **Recursive CTEs** whose start is restricted by a drive or carried
   relation: the pushdown in §4.8 d is per-start-property. Restricting the
   start by a *join* (a semi-join of start ids) is an optimization to measure
   in slice 6.
4. **Parameterized views, FINAL, schema `filter:`** come through
   `ViewTableRef` and `SchemaFilter` per element. The verification must
   include the `data_security` and filtered-endpoint fixtures.
5. **Databricks.** Lowering reuses the dialect emitter. The path generator
   already has a Databricks form. The differential runs on Databricks too, if
   the fixtures are available (`scripts/load_databricks_fixtures.py`).
6. **Neo4j differences that are not bugs.** Ordering without ORDER BY, float
   formatting, and the temporal output format (#1055 #1068) are normalized by
   the comparator, and listed in the comparator, not hidden in goldens.

## 9. Non-goals

- Write clauses, `CALL`, `COPY TO`, and the schema-discovery and NL tools.
- New Cypher features beyond what the parser accepts today, other than
  clause-order freedom (§4.2).
- Changing result column naming or the Bolt and HTTP formats.

## 10. Checklist

- [x] S0 design doc + P-4c (#1313)
- [x] S0.5 one translate seam (`src/translate.rs`).
  - Every read entry point goes through `translate_read`: HTTP `/query`
    and `/query/sql`, Bolt, both `apoc.export`/`COPY TO` inner queries, the
    server export helper, embedded `cypher_to_sql*`, and the corpus and
    golden harnesses.
  - SQL-only outputs carry `SETTINGS join_use_nulls = 1` (#1314).
  - It fixed three defects found on the way:
    - The generated-alias and CTE counters were process-global and reset by
      every HTTP and Bolt request, so a concurrent request could rewind
      another query's counter mid-translation and re-issue the same `t{N}`.
      They are now per query.
    - `/query/sql` skipped the analyzer passes and the `id()` rewrite.
    - `/query/sql` shared `/query`'s cache key (same text, no tenant or
      view params), so its SQL could be served as `/query`'s cached SQL.
      It now has its own route-scoped key that includes the view parameters.
    - `/query/sql` and the HTTP `apoc.export` path translated with no query
      context, so their counters, schema, stats and dialect came from
      process-global state. They now set up a context like `/query` does,
      and `translate_read` opens one for any caller without one.
- [x] S1 Neo4j oracle + result goldens (standard): `scripts/oracle/`,
  `tests/corpus/expected/`, `tests/integration/test_neo4j_result_goldens.py`.
  - The corpus queries tagged `social_integration` and `standard` score
    343 correct and 101 known-wrong.
  - The 101 are categorized in `triage.json`; the new bugs are tracked in
    #1316.
  - The comparator is exact: ORDER BY key sequences are checked, a LIMIT
    answer must be a valid subset of Neo4j's full answer, relationship
    endpoints are compared, and integers compare exactly.
  - Every non-bug category is confirmed by re-running a rewritten query on
    Neo4j.
  - Findings that change later slices:
    - `#1181`'s hand oracle enforces less relationship uniqueness than
      Cypher (S6 must use Neo4j);
    - unknown labels and impossible patterns are refused where Cypher
      returns empty (decide in S3);
    - an undeclared UInt8 flag compared with `true` (needs
      `property_types`);
    - unmapped properties are read from table columns (decide in S3).
  - Other layouts (FK-edge, denormalized, polymorphic, composite) need
    loader rules, added with S8.
- [x] S2 clause-list parser (`open_cypher_parser/clause_list.rs`).
  - Every clause is in source order.
  - A WITH has its own ORDER BY / SKIP|OFFSET / LIMIT / WHERE in the fixed
    order, and out-of-order or post-clause ORDER BY / SKIP / LIMIT are
    free-standing clauses.
  - There is no free-standing WHERE. RETURN ends the query, and a query must
    end with RETURN, an update clause or CALL.
  - Each grammar rule has a positive and a negative test, checked against
    Neo4j 5.26.
  - It reuses the legacy clause and expression parsers; the legacy parser
    and pipeline are unchanged.
  - `clause_list_parity.rs` checks that both parsers agree on all 1484
    corpus queries the legacy parser accepts, comparing canonical forms.
    The tests fail on deliberately broken parsers.
  - Review: a first cut accepted clauses after RETURN and a free-standing
    WHERE, and modelled WITH modifiers as "written order", which gave wrong
    scope rules. All three were fixed before merge.
  - It accepts `WITH a MATCH .. MATCH ..`, several UNWINDs after a WITH, and
    MATCH after OPTIONAL MATCH after a WITH. It rejects every corpus query
    the legacy parser rejects, plus the 2 that end without RETURN.
  - Not done here: deriving the legacy AST from the clause list. It is not
    needed, because the legacy parser and planner are deleted together in
    S11.
- [x] S3 binder + scope + labels (`src/bound_plan/`).
  - `bind_statement` walks the clause list once and resolves every variable
    name against the current scope exactly once. Each binding gets a `VarId`
    (internal name `v{N}`), a kind (node, relationship, path, value), a
    nullability and its source clause.
  - WITH and RETURN outputs are new bindings. The projection's ORDER BY and
    WHERE see the input scope too, unless it aggregates or is DISTINCT; then
    an input expression is allowed only where it equals a projected one.
    Implicit grouping, UNION column names (matched by name), comprehension
    and reduce locals, and re-binding a name after WITH (#1304) all follow
    Neo4j 5.26.
  - Labels are inferred from the schema's (type, from, to) triples to a fixed
    point, including variable-length segments and closed patterns. Only
    variables the clause introduces are narrowed; a bound variable never is
    (an OPTIONAL MATCH must not drop input rows).
  - S1 finding decided: an unknown label or an impossible pattern binds to an
    empty label set and will match nothing, as in Cypher; it is not an error.
    The other S1 finding (unmapped properties read from table columns) is a
    property-to-column decision and moves to S4.
  - `binder_corpus.rs` binds the whole corpus: 1443 bound, 35 unsupported
    (graph patterns inside expressions, which fall back until S9), 24
    rejected by the clause-list parser (all invalid Cypher), 4 bind errors
    (all queries Neo4j also rejects, checked on Neo4j). No query in the
    Neo4j scorecard gets a bind error.
  - Review (checked against Neo4j) found nine defects, all fixed with tests
    that fail without the fix:
    - a variable-length list reused as one relationship was accepted;
    - ORDER BY resolved an alias before matching a projected expression
      (`RETURN a.age AS a ORDER BY a.age`);
    - `RETURN *` columns were not in name order;
    - aggregates were accepted in WHERE, pattern properties, UNWIND and
      non-aggregating ORDER BY;
    - comprehension and reduce variables counted as outer variables in the
      grouping check;
    - a property of a grouping-key variable was rejected;
    - value-typed variables used as nodes were rejected (now a fallback);
    - plus `RETURN *, a`, `reduce(x = 0, x IN ..)` and `ORDER BY *`.
  - A first run hung: a closed pattern `(a)-[r]->(a)` assigned each end's
    label set to the shared slot in turn and never converged. Inference now
    only intersects (so it always terminates), handles closed patterns, and
    stops a variable-length search when a frontier repeats.
  - Not bound yet (`BindError::Unsupported`, the caller falls back): CALL,
    updating clauses, graph patterns inside expressions, property-map
    parameters, re-matching a bound variable-length relationship list, the
    same relationship variable twice in one MATCH (Neo4j: no rows), and a
    list element or other value used as a node or relationship.
- [ ] S4 lowering: MATCH / WHERE / WITH / RETURN (standard). Split in two:
  - [x] **S4a: MATCH / WHERE / RETURN** (`src/bound_plan/lower/`).
    - Routing: `CLICKGRAPH_BOUND_PLAN=on` (default off) in
      `translate::translate_in_context`. A query goes to the bound-plan path
      when the caller passes its text (`ReadOptions.cypher`: HTTP `/query`
      except graph output, and `cypher_to_sql`) and it binds and lowers;
      anything else, including every bind error, goes to the legacy pipeline.
      Bolt, graph output and the metadata entry points stay legacy until the
      result shape exists.
    - Every pattern element is its own scan aliased `v{N}`; a variable seen
      again (same clause or earlier) is the same scan, tied by identity.
      Scans are joined node, relationship, node in path order, each tie in
      the ON of the later scan, so the printer needs no join sorting.
      Relationships of one MATCH sharing an edge table must differ (edge_id,
      else the stored endpoint tuple).
    - Scope: standard layout only, decided in `graph_catalog`
      (`NodeSchema::is_standard_own_table`,
      `RelationshipSchema::is_standard_edge_table`); one label per node and
      one type per relationship after inference; directed fixed hops; inline
      property maps; schema `filter:`; view parameters; FINAL on the FROM
      table; a RETURN of values with aggregation, DISTINCT, ORDER BY, SKIP,
      LIMIT; RETURN with no MATCH.
    - An element whose label or type set is empty, or a variable from an
      earlier clause written with a label it does not have, makes the query
      return no rows (`WHERE false`), as in Cypher. A property the schema does
      not map reads as NULL.
    - `render_plan_to_sql_plain` prints the plan: CTE flattening, alias
      scope for result typing, duplicate-alias disambiguation, and nothing
      else (no `optimize_plan`, no VLP or fixed-path rewrites, no join
      re-sorting). While it runs, `QueryContext::plain_render` makes column
      printing skip the name-keyed resolution (registry, multi-type VLP
      aliases, the `id` pseudo-property). Legacy printing is unchanged.
    - A node or relationship stands for its identity only in `count(a)`,
      `count(DISTINCT a)`, `a = b` / `a <> b` (different labels or types are
      never equal) and `a IS [NOT] NULL`. Anywhere else it is the entity's
      value, which needs the result shape, so it is not lowered.
    - An aggregate of the NULL literal (an unmapped property, an element that
      matches nothing) is folded to Cypher's value (`collect` → `[]`, `sum` /
      `count` → 0, `min` / `max` / `avg` → NULL) and kept an aggregate
      (`CASE WHEN count(*) >= 0 …`), so the query still returns one row (one
      per group). ClickHouse returns NULL for these (`Nullable(Nothing)`).
    - Constant ORDER BY keys are dropped (ClickHouse reads `ORDER BY 1` as a
      column position). Constant grouping keys are dropped too, with
      `HAVING count(*) > 0` when all keys were constant, so an empty input
      still gives no row.
    - `tenant_id` is merged into the view parameters as the legacy planner
      does (`PlanCtx::with_all_parameters`): tenant isolation of
      parameterized views.
    - Not lowered yet: whole-entity returns and `id()` (need the result
      shape and the server's id encoding), list comprehensions (the legacy
      converter prints lambda bodies early), composite identities used as one
      value, FINAL on a joined table, edge `constraints:`.
    - A test fails if `src/bound_plan/` uses the analyzer, `PlanCtx`, the
      render composition modules or task-local query state.
    - The binder now starts a reused relationship's inference from its bound
      types (it used the written ones, so `MATCH (a)-[r:FOLLOWS]->(b) MATCH
      (c)-[r]->(d)` left `c` and `d` unlabeled).
    - Review (checked on ClickHouse and Neo4j) found six defects, all fixed:
      tenant isolation dropped; an element that matches nothing gave a
      ClickHouse error when other scans existed; `collect` / `sum` of no
      values gave NULL; `ORDER BY` / `GROUP BY` of a constant read as a
      column position; identity comparison ignored labels; a node nested in
      an expression was returned as its id. A second review found three more,
      also fixed: relationships of one type in different tables compared
      equal (now: same edge definition); `count(x)` of an element that
      matches nothing gave no row; `sum` / `collect` of an expression that is
      always NULL (`sum(a.unmapped + 1)`) gave NULL (now `coalesce(sum(..), 0)`
      / `coalesce(collect(..), [])`). Pre-existing in both paths: `avg` /
      `stDev` of no values give `nan` in ClickHouse where Neo4j gives NULL.
    - Acceptance (Neo4j oracle, `social_integration` and `standard`, switch
      off vs on): 82 queries take the new path; 79 equal Neo4j, 2 are
      rejected by Neo4j (`exists(prop)`), 1 is the known UInt8-vs-`true`
      mismatch (needs `property_types`; wrong on the legacy path too).
      0 correct → wrong; 8 wrong → correct. 314 corpus queries lower in all.
    - Live suite with the switch on vs off: 17 tests differ, none a new-path
      defect. 7 Neo4j-golden entries are now correct (the goldens are keyed
      to the default path); 2 expose #1320 (legacy EXISTS over an impossible
      pattern is true for every row); 1 asserts the SQL text `INNER JOIN`.
      Six encode legacy behaviour that differs from Cypher and from the docs:
      an error for an unknown label, type or property (Cypher: no rows /
      NULL), and reading an unmapped column (`docs/wiki/Schema-Basics.md`:
      "Unmapped properties won't be accessible"). They are decided when the
      default flips (S10).
    - Decided (user, 2026-10-06): an undeclared property reads the same-named
      column, as on the legacy path; a missing column is a ClickHouse error
      ("not much harm"). It is NULL only when the element's columns were
      discovered (`closed_properties`; an excluded column is unknown) or in
      Neo4j-compat mode. The lowering follows this (`LowerOptions::neo4j_compat`),
      which removed three of the on/off differences (the unknown-property
      error and the two `u.score` tier tests).
  - [x] **S4b: WITH**, free-standing ORDER BY / SKIP / LIMIT, the WITH
    modifiers' fixed order (#1311) (`src/bound_plan/lower/`).
    - A WITH ends a segment. The rows so far become a CTE whose columns are
      exactly the WITH's output scope (§4.10), and the next clause reads from
      it, so no later reader can resolve a name against the wrong relation:
      - a carried node exports its identity columns; a relationship also
        exports its endpoint columns, so it can be matched again;
      - each also exports the properties later clauses read (the demand
        pass, carried back through pass-through items `WITH a AS b`);
      - a value exports its value; a constant needs no column (an aggregate
        of a carried NULL still folds to Cypher's value).
      Columns are named from the binding (`v{N}`, `v{N}__<physical>`,
      `p{len}_v{N}_<prop>`), so they cannot collide.
    - An element carried by a WITH is the same element: a later pattern ties
      to its exported identity, two elements of one CTE tie in WHERE, and a
      carried relationship takes part in the uniqueness of the MATCH that
      uses it again.
    - A CTE that the next pattern does not tie to is a cross join, so its
      rows still count (#1089 shape).
    - Modifiers run in the fixed order ORDER BY, SKIP, LIMIT, WHERE. A WHERE
      after a SKIP / LIMIT is computed per row in the CTE and applied by the
      next segment, so it filters the rows the LIMIT kept (#1311: Neo4j 0,
      legacy 5). Without SKIP / LIMIT it is a WHERE, or HAVING when the WITH
      aggregates. Aggregation groups by the carried elements' exported
      columns (identity first, #1222).
    - Rows keep an order until a MATCH. While they have one, it travels as
      exported key columns (`__o{i}`). A later SKIP / LIMIT, WITH or RETURN
      without its own ORDER BY reads the rows in that order, so `WITH n
      ORDER BY n WITH n LIMIT 2` keeps the first two. DISTINCT and
      aggregation keep their input's first-seen order in Neo4j, which the SQL
      does not; the order is then marked lost, and a later SKIP / LIMIT
      without its own ORDER BY is not lowered. `collect()` over ordered rows
      is not lowered either (the list would need the order kept inside the
      aggregate), whatever the projection's own ORDER BY.
    - An aggregating WITH whose grouping items leave no GROUP BY key (all
      constant, or an element that matches nothing) still has per-group
      semantics: no row on an empty input (`HAVING count(*) > 0`).
    - On Databricks, `size()` of a list carried by a WITH prints Spark
      `size` (Spark `length` is string-only): the plain printer reads the
      list-valued CTE columns off the lowered plan, which has no variable
      registry.
    - A free-standing SKIP / LIMIT ends a segment that exports the scope
      unchanged; a free-standing ORDER BY sets the rows' order.
    - `render_plan_to_sql_plain` prints the CTEs, each one plain SELECT with
      its own alias scope.
    - Not reached yet: every entry point parses with the legacy parser before
      routing, so syntax only the clause-list parser accepts (a free-standing
      clause after MATCH, `WITH a MATCH .. MATCH ..`) still gets the legacy
      parse error. The lowering handles it (unit tests, the ad-hoc oracle
      below); routing parse failures to the new path belongs to S10.
    - Acceptance:
      - Neo4j oracle, switch off vs on (`social_integration`, `standard`):
        0 correct → wrong, 10 wrong → correct. The 47 newly lowered queries
        with a WITH all equal Neo4j except 2 that Neo4j rejects (a UInt8 flag
        used as a predicate). The corpus lowers 371 queries (was 314); none
        stops on WITH.
      - An ad-hoc set of 102 WITH shapes (order travelling through WITHs,
        #1311, carried relationships matched again in both directions,
        uniqueness, DISTINCT, aggregation with HAVING, re-binding (#1304),
        free-standing clauses, constants, NULLs, impossible labels), lowered
        and run on ClickHouse, compared with Neo4j 5.26: every query with a
        unique answer equals Neo4j, including the row order where one is
        defined. The rest: two queries whose answer depends on ties or an
        unordered SKIP, one `int / int` (#847, legacy prints the same `/`),
        one refused by design (DISTINCT + LIMIT over ordered rows), and three
        oracle-comparator artifacts (Neo4j's `meta` is shorter than the row,
        so `neo_rows` drops a column; ClickGraph's rows are equal).
      - Review (about 230 more queries, each finding checked on ClickHouse
        and Neo4j) found three defects and a Databricks printing regression,
        all fixed with tests that fail without the fix: a group key that
        matches nothing made the WITH a global aggregate (1 row, Neo4j 0);
        `collect()` over ordered rows was lowered when the projection had its
        own ORDER BY; DISTINCT dropped the order, so a later LIMIT or
        `collect()` read unordered rows (the LIMIT case was right on legacy);
        Spark `length` for `size()` of a carried list. Re-run of all 397
        queries: 341 equal Neo4j, 38 are refused (Unsupported, or invalid
        Cypher), and every remaining difference is one of: ties or an
        unordered SKIP under LIMIT, a property the Neo4j loader does not
        store (`follow_id`, an edge id read as a column by the undeclared-
        property rule), `int / int` (#847), byte-based `size` / `reverse` of
        non-ASCII strings (identical on legacy), and the comparator artifact.
  - [x] **S4c: result shape** — whole-entity returns, `id()`, Bolt and graph
    output (§4.13).
    - A node or relationship RETURN item (`n`, `n AS x`, `n.*`) is its
      columns under the legacy names:
      - `x.<prop>` for every property, by name;
      - a relationship first has `x.from_id` / `x.to_id`, its stored endpoint
        columns (`from_id_1`, … when composite);
      - a WITH CTE exports every property of an element the final RETURN
        returns whole: the demand pass marks it `*`, carried back through
        pass-through items.
    - Grouping by a returned element also groups by its identity. DISTINCT
      over a relationship whose `edge_id` is not returned is a GROUP BY of
      the returned columns plus the identity, since rows equal in every
      returned column can be different relationships. With aggregation that
      case is refused.
    - `id(n)` as a RETURN item is the node's key column, as on the legacy
      path; Bolt encodes it from the shape (`IdMapper`). Still refused:
      - `id()` anywhere else (the HTTP / Bolt `id()` rewrite turns comparisons
        into key predicates in the AST, not in the text the new path parses);
      - `id()` of a relationship (the legacy value is its from column);
      - a composite id;
      - `ORDER BY` an `id()` item (a string key's encoded id is a hash, so it
        sorts differently);
      - `elementId()`.
    - Routing: Bolt and HTTP `format: Graph` now pass the query text, and
      embedded `query_graph` does too. A translation with `id() = N` label
      constraints still takes the legacy path.
    - Each shape item lists its own columns as (key, column). Lowering names
      result columns uniquely (the printer's `_2` rule), and the Bolt, graph
      and embedded transformers read only an item's listed columns
      (`ReturnItemMetadata::columns`, `entity_row`, `value`). The legacy
      convention instead collects every `x.`-prefixed column, which on both
      paths gave `RETURN n, n.name` a `name_2` property and
      `RETURN n, n.age + 1` an `age + 1` property. Legacy metadata keeps that
      convention (`columns: None`).
    - A returned element that matches nothing is one NULL column (no rows).
    - An element as a value (`collect(a)`, `[a]`, `CASE … a …`) and `v.*`
      outside a RETURN item are still refused.
    - Fixed: S4a lowered `RETURN n.*` to `v0.* AS "n.*"` (behind the switch).
    - Acceptance:
      - Neo4j oracle, switch off vs on (`social_integration`, `standard`):
        0 correct → wrong, 22 wrong → correct (12 of them whole-entity returns
        new here). The corpus lowers 405 queries (was 371).
      - Bolt and HTTP graph format, switch on vs off, and Bolt on vs Neo4j,
        over 25 whole-entity shapes: every Bolt result equals Neo4j (labels,
        properties, relationship type and endpoints); `n.*` is not Cypher. Every on/off difference
        is a legacy defect the new path does not have:
        - `(a)<-[r]-(b) RETURN r` has start and end swapped;
        - `RETURN n AS x` is named `n`, and its Bolt node is missing;
        - after `WITH n`, the properties gain `email_address` / `full_name`
          and a relationship loses `from_id` (so no Bolt relationship);
        - `RETURN b, count(*)` returns flat columns over Bolt;
        - graph output is empty after a WITH or for `n.*`;
        - `WITH r MATCH (x)-[r]->(y)` fails in ClickHouse.
      - Embedded `query_remote_graph`: identical nodes and edges where the
        legacy path works, and correct where it is empty or fails.
      - Live suite, switch on vs off: 41 tests differ (28 after S4b). The 13
        new ones are 12 Neo4j-golden entries recorded as wrong that are now
        correct (the goldens are keyed to the default path; the same 12 as
        the oracle), and one timing test (`test_filter_early_vs_late`).
    - Review (about 120 queries over HTTP JSON, Graph and Bolt, on vs off vs
      Neo4j) found no wrong rows on the new path. Two findings, both fixed:
      - HTTP `format: Graph` with `sql_only` extracted the result shape before
        returning the SQL, so with the switch off a query the legacy extractor
        cannot read gave 500 instead of its SQL. The shape is now read only
        after the `sql_only` return.
      - Columns collected by prefix, as above.
      Not changed: `translate.rs` imports the result transformer's metadata
      type from the Bolt module, and Bolt reports no fields for an empty
      all-scalar result, on both paths.

- [x] **S5: OPTIONAL MATCH as one unit** (§4.9, "Implemented in S5").
  - Lowered: an OPTIONAL MATCH over what S4 lowers: fixed-length directed
    hops on the standard layout, any number of parts and hops, its WHERE, and
    inline property maps. That covers leading, chained, after a WITH, and
    followed by MATCH / WITH / RETURN of its variables, including whole
    elements and `id()`. Still refused: what MATCH refuses (paths S6,
    undirected S7, other layouts S8, pattern predicates S9), and
    `labels()` of a nullable node.
  - Bolt:
    - An element whose columns are all NULL (unmatched) is NULL, and so is
      its `id()`. Before, Bolt failed on the missing id, or hashed `NULL`
      into an id.
    - Graph output and embedded `query_graph` skip such elements.
    - Fixed on both paths: the vendored PackStream serializer wrote nothing
      for a unit value, so every Bolt record holding a NULL (`RETURN null AS
      x`, `[1, null]`, an unmatched optional) failed in the client with
      "Nothing to unpack". It now writes `0xC0`.
  - Acceptance:
    - Neo4j oracle, switch on, compared with S4c on: 0 correct → wrong; 4
      wrong or error → correct; MATCH 365 → 369.
      - The 17 corpus OPTIONAL queries the new path lowers on the oracle
        schemas all equal Neo4j, except five `UNTYPED_BOOLEAN` entries
        (`is_active` is 1/0 in the Neo4j load). With `= 1` instead of
        `= true` on both sides, all five equal Neo4j.
      - `test_616_fold_optional_bid_plain_predicate` goes from wrong to an
        error: `b.id` is not declared, so it reads the missing column `id`
        (the undeclared-property rule). The legacy path answered with wrong
        rows.
      - The corpus lowers 460 queries (was 405).
    - 48 further OPTIONAL shapes (`social_integration`), on the oracle:
      - Switch on: 47 equal Neo4j. The one difference is a LIMIT cutting
        rows tied on the sort key; both answers are valid.
      - Switch off: 21 wrong and 6 errors. They include multi-hop OPTIONAL
        (#1235), a leading OPTIONAL MATCH, a WHERE reading another input
        variable or a WITH value (NULL included), a shared relationship, a
        later `MATCH (b)` of a NULL node, label and `type()` tests on NULL,
        and patterns that cannot match.
    - Bolt, switch on, over 12 whole-element and NULL shapes: every result
      equals Neo4j. The on/off differences are legacy defects (an empty or
      failing result, reversed endpoints, all-NULL anchor columns).
    - Timing: §4.9 table.
    - Live suite, switch on vs off: 46 tests differ (41 after S4c). The 5
      new ones are the Neo4j-golden entries above: 4 recorded as wrong that
      are now correct, and `test_616` (wrong rows → the undeclared-column
      error). The goldens are keyed to the default path, so they report it.
    - Review (about 220 generated OPTIONAL shapes against Neo4j, Bolt, and
      timing at scale 100). Three findings, all fixed, each with a unit
      test:
      - A variable that matches nothing and is only read by the WHERE
        emptied the clause (`… WHERE b IS NULL`: 3 rows instead of 9).
      - A drive over a relationship without `edge_id` joined parallel edges'
        matches to each other (92/92 instead of 68/24 on `social`).
      - An unrestricted multi-hop `Q` (anchor carried by a WITH or an
        earlier OPTIONAL) ran out of memory. It now uses the drive.

      After the fixes, the 279 shapes (the review's plus this slice's
      sweep) on the new path all equal Neo4j, except for `collect()` order
      without ORDER BY, a LIMIT over tied rows, and a comparator artifact
      for an empty list. The other differences take the legacy path. With
      the switch off nothing regressed: the PackStream change reaches only
      the two `to_bytes` calls in `connection.rs`, and HELLO, RUN and PULL
      still work with the Neo4j driver.
    - Mutation check: each of 21 rules broken in turn (the LEFT JOIN, tie
      placement, `IS NOT NULL`, NULL-safe keys, the conjunct allowlist, the
      NULL guards, the drive's DISTINCT and CTE reuse, the elision, the
      restriction copy, the impossible cases, the WHERE inside `Q`, the
      PackStream NULL, the Bolt NULL element) fails a unit test.
- [x] **S6a: variable-length relationships** (§4.11, "Implemented in S6a").
  - Lowered: `-[:T*a..b]->` / `<-[:T*a..b]-` (any range, including `*0..`,
    `*0..0` and unbounded) of one type joining one label to itself, on the
    standard layout, in MATCH and OPTIONAL MATCH, with property maps, chained
    and in comma parts with fixed hops and other paths; `length(p)`;
    `OPTIONAL MATCH p = …`. Still refused: shortestPath (S6b), a path or a
    `-[r*]->` list as a value (`nodes(p)`, `RETURN p`, `size(r)`, DISTINCT by
    `r`: S6c), undirected (S7), other layouts and composite ids (S8).
  - Acceptance:
    - Neo4j oracle, switch on, vs S5: 0 correct → wrong; 22 errors and 5
      wrong answers → correct; MATCH 369 → 396. The corpus lowers 656
      queries (was 460). (`collect()` without ORDER BY can come out in
      another order between runs; it did once, on a query with no path.)
    - About 400 generated path shapes per graph on three graphs (FOLLOWS
      with `edge_id`, without it, FRIENDS_WITH with a composite `edge_id`):
      every answer of the new path equals Neo4j's, except `collect()` order.
      The legacy path gets 16–58 of them wrong and errors on 80. They include
      the repro shapes of #1310, #1210, #1203, #1305, #1306, #1190, #1177,
      #1178 and the standard-layout analog of #1300.
    - Timing: §4.11 table.
    - Live suite, switch on vs off: 86 tests differ (46 after S5). The 40
      new ones:
      - 28 Neo4j-golden entries recorded as known-wrong now equal Neo4j, and
        5 `xfail` path tests of the pattern matrix pass;
      - 3 `test_chained_vlp_trailing_hop[std]` tests: their oracle leaves a
        hop and a path unconstrained against each other (the legacy
        contract, #1203); the new path's 94 / 76 / 123 rows equal a
        brute force with Cypher's uniqueness;
      - `AUTHORED*2` returns 0 rows, as Neo4j does, where the test expects
        the legacy refusal;
      - 3 shapes the legacy path refuses loudly are answered, and equal
        Neo4j.

      The goldens and these tests are keyed to the default (legacy) path.
    - Review (about 200 further shapes against Neo4j, and timing at scale
      100). Five findings, all fixed, each with a unit test:
      - `*0..N` from a node of another label than the edge's walked the
        edge table from colliding ids (29 rows where Neo4j has 3).
      - DISTINCT or grouping by a `-[r*]->` variable merged paths sharing
        their carried columns (50 vs 55): refused until S6c.
      - An anchored OPTIONAL whose input restriction did not reach the
        walk's first node ran out of memory: a path uses the drive.
      - A cross-joined end counted as restricted, so the walk started at
        every node (out of memory): the semi-join holds only the relations
        tied to the first node, and an end with conjuncts of its own wins.
      - Two paths of one table without `edge_id` were walked forward to
        spell their relationships alike, dropping a backward walk's
        restriction (out of memory): the identity is now the stored pair
        in every walk.

      The review's shapes and the repros, rerun: every answer of the new
      path equals Neo4j's, except `collect()` order; the out-of-memory
      shapes take 73–282 ms.
    - Mutation check: each of 29 rules broken in turn fails a unit test,
      except one that label inference makes unreachable (a path from a
      label the edge does not join that would need a relationship).
- [x] **S6b: shortestPath / allShortestPaths** (§4.11, "Implemented in S6b";
  #1312).
  - Lowered: `shortestPath` / `allShortestPaths` over one variable-length
    relationship as S6a lowers it, in MATCH and OPTIONAL MATCH, with WHERE
    conditions on the path (its length, with its ends) applied before the
    pick. Still refused: a fixed-length relationship in it, a path from a
    node to itself, a condition reading another variable or a carried value,
    the path as a value (S6c), other dialects.
  - Acceptance:
    - Neo4j oracle, switch on, vs S6a: 0 correct → wrong; 2 errors → correct; MATCH 396 → 398. The corpus lowers 688 queries (was 656).
    - 236 generated shortestPath shapes per graph (social_integration
      FOLLOWS, social_standard FOLLOWS and FRIENDS_WITH): each of the 216
      that Neo4j answers equals Neo4j on the new path; the legacy path gets
      76 / 84 / 1 wrong and errors on 20. The other 20 are ones Neo4j
      rejects (a lower bound above 1) or LIMIT ties. On a 16-node graph of
      diamonds, parallel edges and cycles (open ranges bounded at 6, as
      both engines' exhaustive searches explode there unbounded) the same:
      216 equal Neo4j, the legacy path gets 87 wrong.
    - The 16 shapes of `test_shortest_path_pairs`'s graph (ranges, lower
      bounds, `*0..`): every answer equals Neo4j's.
    - Live suite, switch on vs off: 102 tests differ (86 after S6a). The 17
      new ones: 3 `test_pattern_matrix` shortest-path `xfail`s pass; 2
      Neo4j goldens recorded as known-wrong are now correct; 2
      `test_where_on_length_matches_the_length_histogram` shortestPath
      cases expect the condition to filter the picked paths (#1312);
      10 `test_lower_bound_is_applied_before_the_shortest_pick_1205` cases
      expect simple paths where Neo4j's search allows trails (verified on
      their graph: Neo4j has the same three extra pairs). They are keyed to
      the legacy path.
    - Review (about 90 shapes on crafted graphs, timing at scale 100).
      Findings, fixed with a unit test each: a near end of a deep graph hit
      the recursion limit (the search now stops at the last node's values);
      any condition on `length(p)` searched every trail and ran out of
      memory at scale 100 (bounds from above now bound the search, and a
      pair whose distance satisfies the conditions needs no trail); a
      condition reading a WITH-carried value failed in ClickHouse (now
      refused); `*0..0` from a label the edge does not join failed on
      types; a start chosen by a property over a pinned end ran out of
      memory (an end pinned by identity is now the first node). Parallel
      rows of a table without `edge_id` are one relationship to the trails
      (shared with legacy and S6a: #1331).
    - Mutation check: 33 rules broken in turn; 32 fail a unit test. The
      other (shortest relationships left out of uniqueness) also holds by
      construction: their relation has no `path_edges`.
- [x] **S6c: a path or a list of nodes or relationships as a value** (§4.11,
  "Implemented in S6c").
  - Lowered: `p`, `nodes(p)`, `relationships(p)` and a `-[r*]->` list as
    RETURN and WITH items (also DISTINCT, grouped, OPTIONAL) for paths of
    fixed hops and S6a variable-length parts; `size()` of them. Still
    refused: a shortestPath's value (S6d), lists in other expressions (S7),
    ORDER BY a value, composite ids (S8), other dialects.
  - Acceptance:
    - Neo4j oracle, switch on, vs S6b, both with the new comparator: 0
      correct → wrong; 3 wrong answers and 1 error → correct; MATCH 398 →
      402. The corpus lowers 701 queries (was 688). The comparator now
      compares values in Neo4j's Query API form (it asks Neo4j's Query API,
      as the tx API's `meta` flattens lists) and keeps an empty list's
      column (S4c's comparator artifact). Rerun on S6b, it changes only 5
      path queries, from INCOMPARABLE to MISMATCH (the legacy values).
    - Generated shapes on four graphs (social_integration FOLLOWS and
      LIKED; social_standard FOLLOWS without `edge_id`, FRIENDS_WITH with a
      composite `edge_id`; a 16-node graph of diamonds, parallel edges and
      cycles). They cover fixed and variable-length paths, every range,
      both directions, walks from either end, closed, chained and mixed-label
      paths, DISTINCT, grouping, WITH and OPTIONAL. The new path answers 184–192
      shapes per graph and each equals Neo4j. The legacy path gets 104–117
      of them wrong and errors on 64–69.
    - Bolt: 19 queries read through the Neo4j Python driver equal Neo4j's
      answers (node order, relationship direction and endpoints, labels,
      properties).
    - Live suite, switch on vs off: 110 tests differ (104 by the same
      count after S6b). The 6 new ones:
      - a Neo4j golden recorded as known-wrong is now correct;
      - 5 tests expect the legacy refusal of a path made of a hop and a
        variable-length part (`nodes(p)`, `relationships(p)`, `p`,
        `size(nodes(p))`, and `size(nodes(p)) > 3` in a WHERE). The new
        path answers them, equal to Neo4j.

      They are keyed to the legacy path.
    - Review (about 150 further shapes against Neo4j on four graphs, Bolt,
      timing at scale 100). Findings, each fixed with a unit test:
      - DISTINCT and grouping by a path with two variable-length parts
        counted two splits of one path as two paths (161 vs 131 rows):
        one key of the path's elements in order;
      - an OPTIONAL path that did not match gave one NULL per input row
        under DISTINCT (3 vs 1);
      - a type or label without properties failed in ClickHouse (empty
        `map()` cast);
      - a WITH passing a path through built its values unread (3.9 s,
        9.8 GiB for `WITH p RETURN count(*)`; now 0.18 s), and made
        Databricks fall back for it;
      - Bolt merged two parallel relationships of a path (they share an
        element id, #1334).

      Filed: #1334 (relationship element ids ignore `edge_id`), #1335
      (DISTINCT over millions of large values). The review's shapes and
      repros, rerun: every answer of the new path equals Neo4j.
    - Mutation check: 36 rules broken in turn; 35 fail a unit test. The
      other one, asking a shortest path's search for no values, cannot be
      observed: its value is refused afterwards.
- [x] **S6d: a shortestPath's path as a value** (§4.11, "Implemented in
  S6d").
  - Lowered: the values and identity of a `shortestPath` /
    `allShortestPaths` path (`p`, `nodes(p)`, `relationships(p)`, its
    `-[r*]->` list) wherever S6c lowers a variable-length path's, with
    conditions and OPTIONAL. Fixed on the way: `WITH p` over a shortest path
    failed in ClickHouse (Code 47: it exported a `path_nodes` column the
    relation does not have), and so did `WITH DISTINCT p` and grouping by
    `p`.
  - Acceptance:
    - Neo4j oracle, switch on, vs S6c: no query changes (MATCH 402); the
      corpus has no shortest path read as a value.
    - Generated shapes on the four S6c graphs (250 each: every range, both
      directions, pinned ends, conditions, WITH, DISTINCT, grouping, other
      parts, OPTIONAL). Neo4j answers 218 and the new path equals it on all
      of them, on every graph. For `shortestPath`, which picks any one of a
      pair's shortest paths, the columns other than paths and lists equal
      Neo4j's and every row is one of Neo4j's `allShortestPaths` rows. Neo4j
      refuses the other 32 (a pair of one node with a range from 1, a lower
      bound above 1).
    - Bolt: 9 queries through the Neo4j Python driver equal Neo4j.
    - Live suite, switch on: the same as S6c but one timing-sensitive test.
    - Review (about 50 further shapes against Neo4j, scratch schemas with
      mixed id types and empty tables, a 1,500-node chain, scale 100): no
      wrong answer. One finding, fixed: `allShortestPaths` with many paths
      ran out of memory (940K paths from one start: 56 GiB), as the walk
      joined every relationship into a node before keeping those from the
      level before; it now follows the parents the search keeps (0.6–0.7 s).
    - Mutation check, by the sweep on the 16-node graph (and, for the
      shapes Neo4j refuses or property maps, which Neo4j does not take in a
      shortest path, the unmutated answers): 18 rules broken in turn; 16
      change answers. Of the other two, the greatest parent instead of the
      least picks another shortest path (only the unmutated answers tell);
      a walk that ignores the property map differs only on parallel
      relationships with different properties, which the graph lacks (a
      unit test pins it).
- [x] S7 UNWIND / UNION / alternatives, in sub-slices (all done; reading a
  map's value is left to a later slice: S7e3): S7a undirected
  relationships, S7b several labels / types (with the two-schema and
  bound-relationship cases S7a refuses), S7c UNWIND (done), S7d UNION
  (done), S7e lists.
  S7e is three: S7e1 lists of values (comprehensions, lists of booleans,
  `collect()` over ordered rows), S7e2 lists of nodes, relationships and
  paths (`collect(n)`, `[n]`, `nodes(p)` / `relationships(p)` / `r*` in
  expressions, UNWIND of them, their elements' properties), S7e3 maps (a
  map literal's value types, `m.k`).
  S7b is itself three: S7b1 a node of several labels, S7b2 a relationship of
  several types or joining several label pairs (with S7a's refusals), S7b3
  variable-length relationships over them (S7b3a walks, S7b3b shortest
  paths).
  - [x] **S7a: undirected relationships** (§4.6 `Alternatives`,
    "Implemented in S7a"): fixed hops, variable-length and shortest paths,
    standard layout, as values too.
    - Acceptance:
      - Neo4j oracle, switch on, vs S6d: 0 correct → wrong, 3 errors →
        correct (MATCH 402 → 405). The corpus lowers 744 queries (was 701):
        of the 28 new ones on the oracle schemas, 27 equal Neo4j, and the
        other is the UNTYPED_BOOLEAN artifact (#1316).
      - Generated shapes on four graphs (113–119 each: hops, closed and
        chained hops, mixed with directed hops, comma parts, WITH and
        re-matching directed, OPTIONAL, path values, one-way types, every
        range, shortest paths with conditions, property maps). Every shape
        the new path lowers equals Neo4j (115, 114, 108, 107); the legacy path is
        wrong on 40–77 and errors on 14–16 per graph. Neo4j refuses or times
        out on 4–8.
      - Live suite, switch on, vs S6d: new failures are tests pinning the
        legacy path (two refusals, a 4-branch SQL shape), the timing test,
        and an `EXISTS` / `NOT` pattern predicate over an impossible pattern
        on the FK-edge layout that legacy answers with every node, exposed
        now that the MATCH oracle in the test returns its (empty) answer;
        known-wrong goldens that are now correct, and the #583 xfail now
        passing. The two shapes the legacy guard refused equal a trail
        enumeration (204) and Neo4j's `shortestPath` rule (210).
      - Review (scratch schemas with self-loops, parallel and mutual edges,
        tables without `edge_id`, composite ids, ALIAS / MATERIALIZED
        columns, a schema `filter:`, against Neo4j): two findings, fixed. A
        variable-length relationship whose type has several schemas matched
        nothing for a pair of labels the type does not join (now refused,
        S7b; also the directed form); the two-direction CTE's `SELECT *`
        left out ALIAS / MATERIALIZED columns (now named columns). Naming
        them first dropped `RETURN r` (the demand pass's `*` marker taken for
        a column), caught by the oracle and fixed.
      - Mutation check on three graphs (Neo4j's answers, else the
        unmutated build's): 9 rules broken in turn; 8 change answers
        (13 to 290 of 349 checks each, over the 3 the unmutated build's arbitrary pick among equal shortest paths fails). The other (walking an undirected path backward) cannot
        change one: the two-direction relation is the same either way.
  - [x] **S7b1: a node of several possible labels** (§4.6 "Implemented in
    S7b1"): one relation of its labels' tables, identity (label, id), a
    relationship of one type at it, label tests, values.
    - Acceptance:
      - Neo4j oracle, switch on, vs S7a: 0 correct → wrong; one match →
        error, `MATCH (n) WHERE n.nonexistent_xyz_999 = 123`: a property no
        label declares reads the same-named column, as on a node of one label
        (the undeclared-property rule). The corpus lowers 754 queries (was
        744).
      - Generated shapes on a scratch graph (five labels, ids shared across
        labels, two labels of one table split by `filter:`; 83 shapes:
        unlabeled nodes, label tests and alternatives, identity comparisons,
        WITH and re-matching, OPTIONAL, path values, aggregation, grouping and
        ordering by a node returned whole). With one property typed
        differently on two labels, every shape that does not read it equals
        Neo4j and every one that does is an error; with none, every shape
        lowered equals Neo4j but one reading an undeclared property (the
        rule). Legacy is wrong on 35–38 and errors on 16–19. S7a's four
        undirected sweeps are unchanged (every lowered shape equals Neo4j).
      - Review (scratch graphs with parallel edges, composite ids, a number
        typed differently on two labels, FINAL, an expression mapping, a
        `filter:`, a parameterized view; about 230 queries against Neo4j):
        three findings. A property typed differently on two labels answered
        differently (a ClickHouse `Variant`: its NULLs counted, `8` and `8.0`
        distinct), now an error (`one_type_guard`); `RETURN DISTINCT n ORDER
        BY n.name` was Code 215, now grouped by the node's properties too; a
        label's undeclared same-named column reads NULL where another label
        declares the property (kept, user decision, above).
      - Mutation check: 11 rules broken in turn; 10 change answers, the
        other (taking the first of several label pairs) is a refusal the unit
        tests pin.
      - Timing (scale 100): unlabeled `count(*)` 3 ms, a node by property
        2 ms, `RETURN n LIMIT 25` 68 ms (legacy 11 ms, with no labels),
        `count(DISTINCT n)` 169 ms (legacy 167 ms), a carried node re-matched
        to a relationship 145 ms (legacy: no SQL), `a = b` 96 ms (legacy:
        Code 47).
  - [x] **S7e3: map literals as values** (§4.4 "Implemented in S7e3"):
    `Map(String, Dynamic)` literals (typed output), `keys()`, grouping,
    DISTINCT, UNION, `collect` of maps; reading a map's value is refused.
    - Acceptance:
      - Map shapes on the S7d graph (24: typed values of every column kind,
        booleans, `{}`, nested maps, DISTINCT / grouping, `collect` in
        order, maps of aggregates, CASE and lists of maps of different
        keys, UNION / UNION ALL, `keys()`): 23 equal Neo4j; the other is
        `duration`'s output format (#1068, older).
      - The S7d UNION sweep: 145 of 149 equal Neo4j (was 143: its two
        map-literal shapes). S7e1 / S7e2a / S7e2b sweeps: no shape worse than
        legacy; the S7e1 review set 323 (was 319).
      - Mutation check (24 map shapes): 7 rules broken in turn, 5 change
        answers, among them values not cast to `Dynamic`, which fails only on
        ClickHouse 25.8 (checked there by hand; the harness runs 26.7). The
        constructors' map form and the refusal of map comparisons change no
        answer (legacy, where a refusal goes, is wrong on those shapes too);
        unit tests pin both.
      - Neo4j oracle, switch on, vs S7e2b: unchanged (MATCH 409). Live suite:
        unchanged but the timing-flaky test.
      - Two adversarial review rounds (about 600 probes, ClickHouse 26.7
        and 25.8, branch and `main`, against Neo4j). The first found the
        named-tuple design unsound (positional comparison and merging,
        converted value types, non-constant aggregate types); the second
        found every read of a `Dynamic` value wrong somewhere (25.8 joins of
        `UInt8` with `Int64` match nothing, booleans equal numbers, `count`
        counts a NULL `Dynamic`, lists sort as text). Reads are refused; on
        the final build no new-path answer differs from Neo4j where `main`'s
        did not, and `keys()` is as `main` gave it.
  - [x] **S7e2b: a walk's nodes and relationships as lists** (§4.4
    "Implemented in S7e2b"): walks of one definition carry element tuples
    when read as a list's elements; `nodes(p)` / `relationships(p)` of paths
    with walks, and `-[r*]->` lists.
    - Acceptance:
      - Generated shapes on the S7d graph (36 answered by Neo4j: walks of
        every direction and range incl. `*0..`, pinned at either end, mixed
        with fixed hops, two walks in a path, shortest / all shortest paths,
        comprehensions, UNWIND, WITH, MATCH conditions): every lowered shape
        equals Neo4j but parallel relationships without `edge_id` (older,
        above), a shortestPath that picks another of two equally short
        paths, and an unordered `collect`.
      - The S7e1 / S7e2a sweeps and the S7e2a review's shapes: no shape
        worse than legacy (but S7e2a's parallel-equal DISTINCT, above).
      - Mutation check: 7 rules broken in turn, all change answers (one
        shape added for walks from the right end).
      - Neo4j oracle, switch on, vs S7e2a: unchanged (MATCH 409). Corpus
        lowers 793 (was 788).
      - Live suite, switch on, vs S7e2a: unchanged. (A first run lowered
        `length()` of a list — a test expecting an error for `length(r)`
        failed, and two expected failures on `length(nodes(p))` passed —
        but Neo4j refuses `length()` of a list: now refused.)
      - Adversarial review (about 340 Neo4j-compared shapes on two graphs,
        a typed schema on ClickHouse 26.7 and 25.8, 2,474 earlier queries
        diffed against `main`): one regression, fixed. A walk over a type
        with a composite `edge_id` failed to build tuples a WITH item asked
        for, and the whole query fell back to legacy (type names for `WITH
        relationships(p)`); such a walk now carries none, and its values
        lower as before. Performance on a million paths: unchanged within
        noise; a WITH or UNWIND of a walk's list carries both its values and
        its tuples (redundant, cheap).
  - [x] **S7e2a: lists of nodes and relationships** (§4.4 "Implemented in
    S7e2a"): `collect(n)`, `[a, b]`, fixed paths' `nodes(p)` /
    `relationships(p)` as arrays of typed tuples; their elements' properties,
    identity, `type()`, label tests, `count` / `collect`; returned as values.
    - Acceptance:
      - Generated shapes on the S7d graph (99 answered by Neo4j: collect /
        UNWIND / comprehensions over nodes and relationships with and
        without `edge_id`, NULL elements from OPTIONAL MATCH, list literals,
        fixed paths' nodes and relationships, WITH chains, DISTINCT,
        grouping, UNION arms, equality with matched nodes): every lowered
        shape equals Neo4j but the order of a `collect` of rows in no order
        (Cypher defines none).
      - The review's shapes (about 400 on the S7d graph, a graph of renamed
        properties, String ids, a boolean declared on a `UInt8` column,
        parallel equal relationships, empty labels; the S7e1 review graph;
        FK-edge, denormalized and polymorphic copies, all refused): with
        the fixes, 258 lowered on the S7d graph, none worse than legacy;
        the remaining differences are unordered `collect` orders and the
        known gaps above.
      - The S7e1 sweeps: unchanged (a `collect` after a MATCH, whose order
        Cypher does not define, matched by chance before).
      - Mutation check: 10 rules broken in turn, all change answers (one
        first survived for want of a parseable shape; two shapes added);
        the review fixes' gates are pinned by unit tests, each checked to
        fail with its gate removed.
      - Neo4j oracle, switch on, vs S7e1: unchanged (MATCH 409). Corpus
        lowers 788 (was 784: the `collect(u) … UNWIND` tests).
      - Live suite, switch on, vs S7e1: unchanged.
      - Adversarial review: three wrong answers, fixed. A list mixing
        labels, types or values (`[p, c]`, `[p, 1]`, `collect([p, c])`)
        returned raw tuples (the guards keyed on a list's kind, which mixing
        made unknown): now refused unless every element is of one shape. A
        boolean declared on an integer column read 1 / 0 through an element:
        now cast in the tuple. `size(collect(p) + collect(c))` and `size([p,
        c])` (25.8) failed where `main` answered: refused, so legacy answers.
        A DISTINCT of parallel equal relationships with no `edge_id` is the
        older policy (above).
  - [x] **S7e1: lists of values** (§4.4 "Implemented in S7e1"):
    comprehensions over values, lists of booleans, `collect()` over ordered
    rows, `+` with a list.
    - Acceptance:
      - Generated shapes on the S7d scratch graph (138: comprehensions over
        literals, NULL and empty lists, nested, over list properties, with
        outer columns, aggregates in the list, in WHERE / ORDER BY / UNION
        arms / UNWIND; boolean lists; `collect()` after ORDER BY with NULLs,
        ties, DESC, grouping, DISTINCT, SKIP / LIMIT, UNWIND, OPTIONAL,
        WITH chains; `+` of lists). 133 equal Neo4j (legacy 103). The other
        5: `reduce` over a comprehension does not parse (both paths), `+`
        of strings of unknown type (loud, S7c), a list with a NULL list in
        it (loud), `+` of a list and a value that may be NULL (not lowered;
        legacy loud), and a LIMIT the harness cannot verify.
      - The review's shapes (344, on a graph with Decimal, Date,
        LowCardinality, `UInt8` declared boolean and arrays, a 5,000-row
        table and an empty one): 316 equal Neo4j (legacy 214), none worse
        than legacy. The rest are known gaps: map values (S4, S7e3),
        integer `/` (#847), mixed-type lists, NULL lists, a list property
        of an OPTIONAL MATCH's unmatched node read as `[]`, parse gaps
        (quantifiers, `DISTINCT NOT`), lists of nodes (S7e2).
      - Mutation check (the 138 shapes): 14 rules broken in turn, 13
        change answers. The other, an ordered `collect` that does not sort
        by the rows' number, gives the same answers on ClickHouse 26.7 (the
        window's output reaches `groupArray` in order, at 300,000 rows and
        grouped too), which ClickHouse does not promise; unit
        tests pin the sort.
      - Neo4j oracle, switch on, vs S7d: unchanged (MATCH 409; the corpus
        has no comprehension over values or `collect()` over ordered rows).
      - Live suite, switch on, vs S7d: unchanged but
        `test_filter_early_vs_late` (timing; flaky on either build). An
        earlier run also changed a scorecard entry of a LIMIT without ORDER
        BY whose SQL is identical (rows differ run to run).
      - Adversarial review: two wrong answers, both fixed. `+` with a
        comprehension or an ordered `collect` added the arrays element by
        element (the emitter's list test did not know them); `max(DISTINCT
        a > 1)` showed 1 / 0 after the DISTINCT moved (the type classifier
        read `DISTINCT x` as untyped). Loud errors fixed: comparing two
        comprehension parameters (`x.id = y.id`), `list + 3`. An aggregate
        in a comprehension's body is now an error, as in Neo4j.
  - [x] **S7d: Cypher UNION** (§4.4 "Implemented in S7d"): each arm a CTE
    of its columns in the UNION's order, a `UNION ALL` of them, values
    read as `Dynamic`; UNION ALL ordered by arm and row when an arm is
    ordered, UNION grouped by Cypher's equality and the elements' identity.
    - Acceptance:
      - Generated shapes on a scratch graph with a `Bool`, a `Float64`, a
        `LowCardinality(String)`, a list property and a relationship whose
        `edge_id` is not a property (152: literals and values of mixed
        types, booleans against numbers, strings against numbers, lists,
        columns matched by name, aggregates, nodes, relationships, OPTIONAL
        nulls, paths, nodes of several labels, relationships of several
        types, per-arm ORDER BY / SKIP / LIMIT, arms ordered by a list,
        WITH / UNWIND / VLP / shortest paths in arms, three arms; 26 checked
        row for row). 145 are lowered: 143 equal Neo4j; the other 2 differ
        by a map literal's value type (S4). Legacy is wrong on 42 of them,
        errors on 53 and orders 2 differently. Not lowered: a node against a
        value, a node of another label or a relationship of another kind
        (legacy wrong on 1, errors on 3), and the 3 Neo4j refuses.
      - Neo4j oracle, switch on, vs S7c: MATCH 408 → 409 (a UNION whose arm
        has two undirected OPTIONAL MATCHes, an error before, equals Neo4j;
        `test_914`, `collect()` over an unordered MATCH, flips as in S7c).
      - Live suite, switch on, vs S7c: unchanged but for the scorecard
        entry of that UNION, now correct (`known-wrong entry is now
        CORRECT`). An earlier run also failed `test_filter_early_vs_late`,
        the timing test.
      - Mutation check: 19 rules broken in turn, and the sweeps catch all
        19:
        - the `Dynamic` cast; the exported identities;
        - the columns' order by name; an arm's ORDER BY under its LIMIT;
        - the sort keys exported; the row numbering, its direction, and the
          order by arm;
        - the arms' demand; UNION read as UNION ALL;
        - a graph value's and a several-label element's identity;
        - values grouped as they are; an element's columns grouped;
        - in the key: the string kind, the number kind, the list kind,
          `-0`, FixedString padding, and FixedString / Enum as strings.

        A boolean kind in an earlier key survived because it was redundant
        (a boolean's text is never a number's), and was removed.
      - Adversarial review (about 250 queries beyond the sweep): no wrong
        row count from the arms, ordering, identity or column matching.
        Fixed from it:
        - the DISTINCT key took only `String` as a string, so FixedString,
          Enum, UUID and dates merged with numbers or booleans of the same
          text, or stayed apart from equal strings (now kinds by type, with
          FixedString padding);
        - `-0.0` ≠ `0`;
        - map literals of different key order were different;
        - a node with a `Dynamic` column failed (Code 44, now grouped
          through the key).

        A sweep of the review's types (24 shapes) equals Neo4j but for
        S4's map values and a harness artifact. Left as found: native map
        columns' key order, and parallel relationships with no `edge_id`
        (both older).
  - [x] **S7c: UNWIND** (§4.4 "Implemented in S7c"): a list of values;
    the rows so far become a CTE that repeats each row per element
    (`ARRAY JOIN`), keeping the order Neo4j keeps.
    - Acceptance:
      - Generated shapes on a scratch graph with list properties (138:
        literals, empty / NULL / nested lists, `range`, `split`, `keys`,
        slices, CASE, `coalesce`, list properties, declared and undeclared
        scalar properties, booleans, UNWIND first, after MATCH, OPTIONAL,
        WITH, aggregation, ORDER BY with ties, SKIP / LIMIT, several UNWINDs,
        then MATCH / OPTIONAL MATCH, paths, shortest paths, undirected;
        38 checked row for row). 120 are lowered: 117 equal Neo4j, a list of
        map literals differs by its values' type (S4's map literal), and 2 are
        loud (an undeclared string property; `+` of strings from an
        UNWIND). Legacy is wrong on 14 of them and errors on 25.
      - Neo4j oracle, switch on, vs S7b3b: unchanged (MATCH 408; the
        corpus's 13 UNWIND queries keep their verdicts). One run showed 407:
        `collect()` over an unordered MATCH (no UNWIND) listed the same ages
        in another order, as it may.
      - Live suite, switch on, vs S7b3b: unchanged but for
        `test_filter_early_vs_late` (the timing test, passing this time).
      - Mutation check (13 rules broken in turn: the empty list, a NULL
        value, the numbering of ordered rows, the positions' direction, the
        one-row rule after an UNWIND and after an aggregation, rows in no
        order, the projected values' kinds, a declared property, the zipped
        ARRAY JOIN, a list's elements, the numbering's direction, the NULL
        literal): the sweep catches 10. The other 3 only change an order
        ClickHouse keeps anyway on small inputs, or refuse shapes the
        legacy path answers right; the unit tests catch them.
      - Adversarial review (about 280 shapes on three graphs: declared
        types, two labels and two types with `filter:`, FK edges, FINAL):
        no wrong row count. Fixed from it: a boolean element showed 1 / 0
        (now cast to Bool), `split()` of NULL gave a row of `''`, and the
        NULL test of a value was the `IS NULL` operator, which reads `NOT a
        IS NULL` as `NOT (a IS NULL)` (found while fixing: `UNWIND NOT
        (p.id > 2)` dropped every row; now `isNull()`). Also made to answer
        instead of failing: a list that is NULL by the schema, and lists
        whose elements are all NULL (`WITH [] AS l UNWIND l`). Left as found
        (older, both paths): the value gaps listed in §4.4.
  - [x] **S7b3b: `shortestPath` / `allShortestPaths` over several types,
    definitions or labels** (§4.6 "Implemented in S7b3b"): the search of
    S6b / S6d over a union walk's relations, a node identified by its
    identity as a text; the pairs' ends recovered by label and id.
    - Acceptance:
      - Generated shapes on the S7b2 scratch graph, on three property
        typings (452 each: both functions; several types, a type of several
        definitions, untyped; ends of one, several or no labels; every
        direction and range; conditions on the length, alone and with an
        end; values (`p`, `nodes(p)`, `relationships(p)`, path identity);
        pinned ends, WITH, OPTIONAL, other parts, aggregates, unknown
        types). 426 are lowered: 424 equal Neo4j (for `shortestPath`, the
        rows equal Neo4j's or, with paths, each is one of
        `allShortestPaths`'); legacy is wrong on 306 of them and errors on
        98. The other 2, OPTIONAL with a condition, failed (Code 47: a
        condition there read the path's identity for NULL through
        `start_label`, which the search's relations lack; a union path's
        identity now has `start_id` first) and now equal Neo4j's rows except
        where Neo4j is wrong: Neo4j 5.26.31 drops a value of the incoming
        row (`a.id`, `id(a)`, or a value a WITH projected before the
        OPTIONAL MATCH) in the rows its exhaustive fallback produces under
        OPTIONAL MATCH, for a walk of one type too. Not lowered: 12
        conditions that read `b.id` of an end of several labels (the raw
        guard; legacy wrong on 8, errors on 4); 8 list comprehensions over
        `nodes(p)` / `relationships(p)` (S7e; both pipelines error). Neo4j rejects 6 (a property map in
        `shortestPath`, a start that is the end, 2 timeouts).
      - Walks from the right end read in the path's order (`p`, from a
        pinned last node) and `WITH DISTINCT p`: 14 more shapes, each equal
        to Neo4j (added when the mutation check showed the set lacked
        them).
      - Conditions reading an end of several labels (by a property one label
        declares): 4 shapes equal Neo4j; a fifth compares a string with a
        number (`coalesce(a.code, 1)`), a ClickHouse type error as any such
        comparison is.
      - Neo4j oracle, switch on, vs S7b3a: unchanged (MATCH 408; the corpus
        has no shortest path over several labels or types).
      - Live suite, switch on, vs S7b3a: unchanged but for
        `test_filter_early_vs_late`, which times one query twice and fails
        either build at random (S7b3a's: 3 of 5 runs).
      - Mutation check (224 of the shapes): 10 rules broken in turn (the
        relationships' node keys, the pairs' ends, the keyed trails, the
        walk's relationship spelling, a condition's end join, the path's
        order, the vacuous label condition, the property map in the search,
        a several-label end's label, the last end's restriction), 10 change
        answers. Two first survived: the set read no shortest path's
        identity and no right-end walk's order; 14 shapes were added for
        them. A cross-check keeps every answer (the search reaching every
        node: the ties restrict the ends anyway).
      - Adversarial review (about 250 shapes on six graphs: ids shared
        across labels, dangling and parallel relationships across tables,
        FINAL, filters, labels on one table, overlapping label sets; one
        table's SQL byte-identical to `main`'s; S7b3a's review set
        unchanged): one silent wrong answer, fixed. The pick joined a
        one-label end a condition reads to its raw table, without its
        `filter:` and FINAL, so a stale or filtered-out version of a node
        joined once more: `allShortestPaths` counted paths twice, and
        `shortestPath ... WHERE length(p) >= 3 OR a.name = 'a1'` read a
        stale name (14 rows against Neo4j's 9). The end is now its label's
        relation (`Lowerer::node_relation_sql`); the reviewer's shapes equal
        Neo4j.
  - [x] **S7b3a: variable-length relationships of several types,
    definitions or labels** (§4.6 "Implemented in S7b3a"): a walk of the
    union of their definitions between nodes keyed by label and id, pruned
    to the definitions a path between its ends can use; one spelling of a
    relationship's identity for every scan. `shortestPath` over them is
    S7b3b (above).
    - Acceptance:
      - Neo4j oracle, switch on, vs S7b2: 0 correct → wrong, 4 → correct
        (MATCH 404 → 408: `(a)-[r*1..2]->(a)` and a `*0..1` closed path, both
        legacy errors, an untyped `*1..3` with `nodes(p)` under a `LIMIT`,
        and a collect order).
      - Generated shapes on the S7b2 scratch graph (73 shapes: untyped,
        alternative types, a type of several definitions, ends of several
        labels, every direction and range, pinned at either end, path values,
        property maps, uniqueness against hops and walks of one or several
        definitions, adjacent `*0..1` parts, WITH, OPTIONAL), on three
        property typings, and on the S7b1 label graph (17 shapes: labels of
        one table split by `filter:`, unlabeled ends): every lowered shape
        equals Neo4j, except those reading a property typed differently on
        two labels or types (an error, `one_type_guard`). Legacy errors on 51
        and is wrong on 19 of the first. S7a's four, S7b1's two and S7b2's
        three sweeps are unchanged (one shape legacy errored on now lowered
        and correct).
      - Found by the sweep, fixed: S7b2's union hop spelled a relationship's
        identity unlike a walk of one table, so `DISTINCT p` over
        `[:KNOWS*0..1]->()-[r:KNOWS|FOLLOWS]->()-[:KNOWS*0..1]` counted 19
        paths against Neo4j's 16.
      - Live suite, switch on, vs S7b2: three GraphRAG parameterized-view
        walks now run and pass; two known-wrong goldens are now correct (the
        scorecard keeps the legacy answers until S10). Found by it, fixed: a
        walk over a polymorphic edge (`$any` end) matched nothing, as its
        declared labels pruned it away; any definition whose rows carry their
        own labels is now refused (`RelationshipSchema::
        has_fixed_endpoint_labels`).
      - Mutation check (both graphs): 14 rules broken in turn, 14 change
        answers; two cross-checks keep every answer (every path of one table
        walked as a union; every union walk started at its right end).
      - Adversarial review (own graphs: definitions of one type between
        several label pairs, FK and FINAL tables, view parameters, cycles):
        one regression, fixed: a type the schema doesn't have at a zero lower
        bound (`[:NOPE*0..2]`, also `RETURN p`, OPTIONAL) matched nothing,
        where Neo4j matches the path of none. The binder drops the type, and
        an empty set of types read as none written. Not from this slice,
        left as found: a path key over an impossible end is an internal
        error that falls back to legacy (wrong: 10 vs 0; on main too).
  - [x] **S7b2: a relationship of several possible types or label pairs**
    (§4.6 "Implemented in S7b2"): one relation of its definitions' tables
    in their orientations, identity (definition, id), ties to its ends'
    labels, a relationship bound before matched undirected, values.
    - Acceptance:
      - Neo4j oracle, switch on, vs S7b1: 0 correct → wrong, 1 wrong →
        correct (MATCH 404 → 405). The corpus lowers 759 queries (was 754).
      - Generated shapes on a scratch graph (three labels, ids shared across
        labels, a type of two tables, a type both ways between two labels as
        two tables, an `edge_id` type beside `(from, to)` ones, parallel edges,
        self-loops, a relationship `filter:`; 103 shapes: untyped and
        alternative types in every direction, label pairs, properties,
        identity and uniqueness, WITH and re-matching (directed and
        undirected), OPTIONAL, path values, a variable-length relationship
        beside a union), on three property typings (one type per property;
        a number of two widths; a string beside a number): every lowered
        shape equals Neo4j, except those reading the string-and-number
        property (an error, `one_type_guard`) and an undeclared property (the
        rule). Legacy errors on 51 and is wrong on 42–43 of 99. S7a's four
        undirected sweeps and S7b1's two label sweeps are unchanged, plus two
        shapes legacy answered wrong (`MATCH (n) WITH n MATCH
        (n)-[:WORKS_AT]-(m)`: 4 rows against 8) now lowered and correct.
      - Mutation check: 17 rules broken in turn; 17 change answers (two
        needed new shapes: a written type on a carried relationship whose
        ends do not narrow it, and a carried relationship re-matched between
        two bound nodes of other labels).
      - Review (five scratch graphs, about 300 shapes against Neo4j: composite
        node and edge ids, identities of different widths, parallel edges,
        self-loops, carried relationships through WITH / DISTINCT /
        aggregation, OPTIONAL, paths, unions beside variable-length paths):
        two findings, fixed. A relationship bound before matched undirected
        in a MATCH with a variable-length relationship was Code 47
        (`path_first` placed a tie, read inside a `CASE`, before the
        orientation join; `table_aliases` now reads every expression); a
        property whose definitions' column types have a common type was
        converted (`Bool` read as `UInt8`, `WHERE r.flag = 2` matching
        nothing, `Float32` as `Float64`, `Date` as `DateTime`), also on nodes
        of several labels: now one column per definition and an error unless
        the types agree up to width, representation and nullability.
        Mixed string / number ids across definitions are loud errors (known
        gaps, above).
      - Live suite, switch on, vs S7b1: one known-wrong golden now correct
        (`MATCH ()-[r:LIKED]-() RETURN r LIMIT 25`), nothing else changed.
      - Timing: above (§4.6).
- [x] S8 layouts, in sub-slices: S8a FK edges (done), S8b denormalized
  nodes (done; a label both embedded and in its own tables later), S8c
  polymorphic edges and node tables (done), S8d composite ids: S8d1 walks
  of one definition (done), S8d2a shortest paths (done), S8d2b union walks
  (done), S8d2c values and lists (done).
  - [x] **S8a: FK edges** (§4.6 "Implemented in S8a"): a relationship whose
    table is one of its ends' node tables is read as a table of
    relationships; a self-referencing one's ends as legacy reads them
    (#632); its table read with its node's view parameters and FINAL;
    crossed composite ends refused.
    - Acceptance:
      - Neo4j oracle, switch on, vs S7e3: social_integration + standard
        MATCH 409 → 425 (16 wrong, erroring or incomparable answers now
        equal Neo4j); one correct → error, `WITH a, p, p.likes AS
        like_count`: `likes` is undeclared on `Post` and `posts_test` has no
        such column, a ClickHouse error under the undeclared-property rule
        (legacy drops the unused item); sqlgen_fk_edge 9 of 9 unchanged. The
        corpus lowers 925 queries (was 787).
      - Generated shapes against Neo4j (the oracle loader reads a
        self-referencing FK edge as legacy does): three scratch graphs (a
        self-referencing FK edge in both spellings with NULL, dangling,
        self-loop and cyclic references; an FK edge on the to-node table with
        a node `filter:`; one on the from-node table with properties; a
        separate edge table beside them; composite FK edges), the FK-edge
        wiki's file tree, and the review's graphs (an FK edge beside an edge
        table, one table as two labels by `filter:`, two FK edges on one
        row): every lowered shape equals Neo4j but unordered `collect()`
        orders; legacy is wrong or errors on about half of them.
      - Mutation check: 6 rules broken in turn, all caught: 3 by the sweep
        (FK edges refused; the generator's FK-edge walk, which errors or
        differs on 10 shapes; self-referencing ends as written, 51 shapes);
        3 by unit tests (crossed ends, view parameters, FINAL), which the
        loader cannot show (it pairs columns as written and reads no table
        options).
      - Adversarial review (about 110 shapes, own graphs, ClickHouse 26.7):
        three findings, fixed. A self-referencing FK edge ran backwards
        (`from_id: parent_id, to_id: object_id` read as parent to child,
        where the wiki, the examples, the goldens and legacy have child to
        parent; the first oracle loader read it the same wrong way, so the
        sweep agreed); a node on a parameterized view with an FK edge
        declaring none read the bare view (correct → ClickHouse error); a
        node's FINAL was not applied to its FK edge's read (stale versions;
        now not lowered, as a joined read prints no FINAL). Not from this
        slice: legacy's crossed-composite guard misses some shapes, which a
        refusal falls back to (#1348).
      - Live suite, switch on, vs S7e3: SQL-text tests of legacy CTE names
        (5), a test encoding `labels(x)[1]` as 1-based (Neo4j: NULL, as the
        new path answers), the undeclared `p.likes` above, and result goldens
        whose recorded wrong outcome is now correct; nothing else.
  - [x] **S8b: nodes embedded in edge tables** (§4.6 "Implemented in
    S8b"): an embedded label is the relation of its roles (two CTEs, built
    when first read); a node of the clause is read from its first
    relationship's row when that row holds all it is read for; a foreign
    denormalized label is its own table; uniqueness compares one
    definition's relationships; coupled edges are tied through their
    shared node; mixed labels (embedded and own tables) are not lowered.
    - Acceptance:
      - Neo4j oracle, switch on vs off, 13 schemas (the ten denormalized
        and coupled-edge fixtures, social_integration, standard,
        sqlgen_fk_edge): 159 answers wrong or erroring under legacy now
        equal Neo4j; 4 the other way, all known: two undeclared properties
        read under the undeclared-property rule (ClickHouse error, as in
        S8a), two zeek reads of `IP.port`, which differs across one IP's
        rows (no single value). Switch on, vs S8a: social_integration and
        standard unchanged. The corpus lowers 1237 queries (was 925).
      - Generated shapes against Neo4j on scratch graphs (embedded labels
        in one and several tables, in both roles, with a node `filter:`,
        NULL and dangling ends, composite ids in both orders, foreign and
        mixed-access labels, coupled DNS edges) and the review's graphs:
        every lowered shape equals Neo4j but unordered `collect()` orders
        and reads of a property the rows of one id disagree on.
      - Mutation check: 14 rules broken in turn, all caught: 13 by the
        sweep (uniqueness across definitions and foreign labels also by unit
        tests), one by a unit test only (no elision: the same answers, pinned
        by the SQL). A guard against eliding an emitted node was found
        redundant (only an earlier clause's nodes are emitted then, excluded
        since the review) and removed. The sweep's one non-match is a mixed
        label, not lowered: legacy's answer, with the switch on or off.
      - Adversarial review: four findings, fixed: composite ids listed in
        another order on the edge (0 rows); a label embedded in some tables
        and its own in others read as one of them; an OPTIONAL MATCH after
        a filtered node elided that node (ClickHouse error); NULL in some
        rows of an id (now inconsistent data, as above).
      - Live suite, switch on, vs S8a: 22 tests changed, none to a wrong
        answer. SQL-text tests of legacy's joins and branches (7); legacy
        refusals now answered, each equal to Neo4j (#1155 chained
        undirected paths, #1158 mixed-access shapes, a path off a carried
        node in a WITH body: 10); tests encoding legacy's contract where
        the new path gives Neo4j's answer (3: a fixed hop is now
        edge-unique against the paths before it, #1203; 1: a `WHERE
        length(p)` on shortestPath is applied during the search, all 19
        comparisons equal Neo4j); one strict xfail of #1203 now passes.
  - [x] **S8c: polymorphic edges and label-column node tables** (§4.6
    "Implemented in S8c"): type and label discriminators as row filters of
    fixed-label definitions; closed `*_label_values`; walks over filtered
    tables by the walk of definitions.
    - Acceptance:
      - Neo4j oracle, switch on vs off, the four polymorphic schemas
        (polymorphic, social_polymorphic, sqlgen_polymorphic, data_security):
        MATCH 144 → 170 (legacy 144); the non-matches left are legacy's
        (pattern comprehensions, S9; `collect` order; undeclared properties;
        a parse error). The 13 earlier schemas unchanged. The corpus lowers
        1380 queries (was 1237).
      - Generated shapes against Neo4j on four scratch graphs (both ends
        open with two labels of overlapping ids, an unknown label, a NULL
        label, a type outside `type_values`, dangling ids, self-loops and
        parallel edges with a composite `edge_id`; a closed end with a fixed
        `to_node` and a `filter:`; labels sharing a table with a polymorphic
        edge on that table; fixed-end types and a standard edge with a
        `filter:` walked alone): every lowered shape equals Neo4j (95 of 97; a
        walk's relationship list over a composite `edge_id` and a pattern
        predicate are not lowered); legacy is wrong or errors on 59.
      - Mutation check: 14 rules broken in turn, all caught: 12 by the
        sweep (with the review's FK graph), 2 by a unit test (a pair another
        definition joins). The view
        and label inference each close an end to its label values: either
        alone keeps every answer, both broken are caught.
      - Adversarial review (own graphs: ids repeated across labels of one
        table, lowercase / spaced / NULL discriminators, a type named like a
        label, ORed filters, keyword column names, composite ids, a
        standard and a polymorphic definition of one type, standard edges
        with filters, view parameters and FINAL walked as unions, S8b
        embedded nodes beside a polymorphic edge, the repo's schemas): one
        finding, fixed: FK edges on a table labels share read every label's
        rows (above; legacy is worse). Not from this slice: a union walk over
        `edge_id`s of different types fails (#1352); rows repeated without an
        `edge_id` are one relationship in walks (as legacy).
      - Live suite, switch on, vs S8b: 10 polymorphic walk and shortestPath
        tests now pass; legacy #1244 refusals now answered (3, each equal
        to Neo4j's count); one SQL-text test of legacy's CTE name (its query
        equals Neo4j); a timing test (passes alone).
  - [x] **S8d1: walks between composite ids** (§4.6 "Implemented in
    S8d1"): a walk of one definition between composite ids, joined, tied and
    restricted by the ids' columns; composite relationships' identities.
    - Acceptance:
      - Neo4j oracle: composite_id 12 → 15 (three legacy refusals of a hop
        beside a composite walk now equal Neo4j); 19 other schemas
        unchanged (1081 verdicts). The corpus lowers 1398 queries (was
        1380).
      - Generated shapes against Neo4j on two scratch graphs (composite
        accounts whose ids share a pipe text, cycles, a self-loop, an
        `edge_id` type with parallel rows; single ids with a composite
        `edge_id`): every lowered shape equals Neo4j (30); legacy is wrong
        or errors on 22 of them (its pipe-text joins conflate the two
        accounts).
      - Mutation check: 5 rules broken in turn: 4 caught by the sweep (steps
        by the texts, ends tied by one column, the hop not unique against
        the walk, composite walks refused); restricting the first node by
        one column keeps every answer (a pushdown: the tie still holds), a
        unit test pins it. A composite path identity by its columns was
        unreachable (no lowered shape groups by such a path) and is left to
        S8d2.
      - Live suite, switch on, vs S8c: unchanged.
      - Adversarial review (three-part ids of String, UInt32 and Nullable
        columns, pipe-sharing ids, composite FK and embedded nodes, every
        walk form, walks with hops, OPTIONAL / WITH / UNION, shortest paths
        over a composite `edge_id`): no wrong answer of this slice's. Two
        shapes it newly took now refused: an end with fewer columns than its
        node's id, an id column named `id` (a ClickHouse error). Filed: NULL
        id parts dropped from zero-length walks (#1354); a hop of several
        types beside a walk failing on `edge_id` types (#1352, as union
        walks).
  - [x] **S8d2a: shortest paths between composite ids** (§4.6
    "Implemented in S8d2a"): the search keys a node by the text of its id's
    tuple; conditions it cannot hold pick among the trails keyed alike.
    - Acceptance:
      - Generated shapes against Neo4j on the composite scratch graph
        (accounts whose ids share a pipe text, cycles, a self-loop, an
        `edge_id` type and one without): every lowered shape equals Neo4j
        (20: directed, reversed, undirected, `allShortestPaths`, bounded,
        from a pipe-sharing id, OPTIONAL, conditions on the length and on an
        end), and the review's 49 rerun after its fixes (all equal but the
        older gaps below); legacy errors or answers wrong on all 20 (its pipe-text keys
        conflate the accounts). Not lowered: a path read as a value (S8d2c).
      - Neo4j oracle, 20 schemas: unchanged (no corpus query has a
        composite shortest path); the corpus lowers 1420.
      - Mutation check (with the review's NULL-part graph): 6 rules broken
        in turn, 5 caught by the sweep (keys by the `|`-joined text, ends
        tied by one column, the trails' ends keyed alike, a condition's end
        keyed by one column, NULL parts relating nodes); restricting the
        trails to the failing pairs is a pushdown that keeps every answer,
        pinned by a unit test.
      - Adversarial review (three-part ids of String / LowCardinality /
        UInt32 / Nullable parts, strings with quotes, backslashes, commas,
        parentheses and `|`, edge columns typed unlike the node's, FK and
        mapped ids, every range, OPTIONAL, after WITH, conditions on lengths
        and ends, 5k nodes): two findings, fixed: a NULL id part related
        nodes through the key's text (wrong lengths; legacy was right), and
        a property in a MATERIALIZED / ALIAS column was a ClickHouse error
        (the keyed subqueries' `*`). Not of this slice: the representation
        gap above; undirected walks over columns of other signedness fail
        (#1358); the trails take parallel rows without `edge_id` for one
        (#1331); the trails ran out of memory at 5k nodes (as single ids).
  - [x] **S8d2b: walks of several definitions between composite ids** (§4.6
    "Implemented in S8d2b"): the walk goes by keys (each node's and
    relationship end's identity as a text).
    - Acceptance:
      - Generated shapes against Neo4j on a scratch graph (two composite
        labels sharing id tuples, a node with a NULL id part and rows to and
        from it, pipe-sharing ids, an `edge_id` type and three without, a
        self-loop): every lowered shape equals Neo4j (25: typed, untyped and
        label-crossing walks, every direction, `*0..`, closed walks, walks
        beside hops, OPTIONAL, after WITH, `size(r)`, `length(p)`, shortest
        and all shortest paths with conditions); legacy errors or answers
        wrong on 20. Not lowered: a path read as a value (S8d2c).
      - Neo4j oracle, 20 schemas: unchanged; the corpus lowers 1420.
      - Mutation check: 6 rules broken in turn, all caught (keys of the
        `|`-joined parts, NULL parts relating nodes, ends not tied by key,
        the first node restricted by label only, a row's ends keyed the
        other way, a key tie's join moved by `path_first`).
      - Adversarial review (80+ shapes: labels sharing id tuples, three-part
        ids, quotes / commas / parentheses / `|` / `:` / unicode in ids,
        every direction and range, closed walks, beside hops, OPTIONAL,
        after WITH, UNION, EXISTS, shortest paths, 5k nodes): two findings,
        fixed: a composite `to` end after a single-id `from` was not
        NULL-filtered (rows through a NULL part), and two walks in one
        relation, one by keys, failed (Code 47: a key tie moved before its
        node). Filed: text keys miss equal values of differently typed
        columns (#1360, known gap above). A union of definitions of other
        end arities is still refused (`rel_union_shape`).
  - [x] **S8d2c: composite ids in values and lists** (§4.6 "Implemented in
    S8d2c"): the generator spells composite ids as their tuple's text under
    the lowering's flag; element ids join parts with `|`; list tuples hold
    a slot per id column.
    - Acceptance:
      - Generated shapes against Neo4j on the composite graphs (pipe-sharing
        ids, a NULL-part node, two composite labels sharing id tuples,
        composite and non-composite `edge_id`s): 45 shapes (path values,
        `nodes(p)` / `relationships(p)`, `DISTINCT p` incl. zero-length
        paths, `RETURN DISTINCT nodes(p)`, lists, `collect` / `UNWIND` /
        `count(DISTINCT)` of composite elements, union-walk and shortest-path
        values, with conditions): every lowered shape equals Neo4j but
        `collect` order, a shortest path's pick among equals (each checked
        among Neo4j's `allShortestPaths`) and the comparator note above.
      - Neo4j oracle, 20 schemas: unchanged; the corpus lowers 1420.
      - Mutation check: 5 rules broken in turn, all caught (walks' ids by
        the `|`-joined text, element ids of one part, identities by the
        element id's text, a list node's identity of one slot, a composite
        node NULL when its first part is).
      - Live suite: unchanged.
      - Adversarial review (about 230 shapes in 10 sweeps on 6 schemas,
        and Bolt): two findings, fixed: a node whose first composite id
        part is NULL was dropped from lists and counts (`size(collect(a))`
        5 for 6), and a NULL part made the element id NULL (Bolt blanked the
        row). Not of this slice: zero-length walks drop nodes with a NULL id
        part (#1354, S8d1); a UNION of lists of nodes built as tuples and as
        values fails (#1362, single ids too).
- [x] S9 subquery expressions (§4.12 "Implemented in S9"): `EXISTS`,
  pattern predicates, `exists(pattern)`, `size(pattern)` /
  `size([pattern …])`, pattern comprehensions of values; bound as MATCHes
  over the expression's scope, lowered as a LEFT JOIN of their matches
  grouped by the correlation.
  - Acceptance:
    - Neo4j oracle, all 20 loadable corpus schemas, switch on vs S8d1: 9
      queries wrong or erroring become Neo4j-equal (pattern comprehensions,
      EXISTS with an inner WHERE, multi-table correlation, `NOT` over a
      variable-length pattern), none the other way; one `size(pattern)`
      query that Neo4j 5 rejects now errors on both (an undeclared `id`
      column, by the undeclared-property rule). The corpus lowers 1420
      queries (was 1398); the 11 subquery refusals left read a list's
      integer as a node (Neo4j rejects them).
    - Generated shapes against Neo4j: 105 lowered on a scratch graph
      (cycles, a self-loop, parallel relationships with and without
      `edge_id`, NULL properties, an isolated node; every position, NULL
      correlations, nesting, the review's repros) and 39 on the FK-edge,
      embedded-node, polymorphic and composite-id graphs: every one equals
      Neo4j except 5 undirected walks over parallel rows without `edge_id`
      (the older gap, also on a plain MATCH); legacy is wrong or errors on
      60 + 32 of them. The review's 400 shapes, rerun after its fixes: every
      difference left is a filed or listed older defect (#1355, #1356,
      `IN` of a list holding NULL, string `+`, list supertypes), list order,
      or a probe asking Neo4j another question.
    - Mutation check: 8 rules broken in turn (NULL-safe drive joins, NULLs
      kept in a comprehension, a nullable WHERE-only key, the LEFT JOIN,
      the count's default, the grouping, nested subqueries' reads, the
      WHERE-only variables of the drive), all caught by the sweep.
    - Adversarial review (about 300 shapes on five schemas, overlapping
      ids across labels and types, dangling FK rows, a 300k-node table):
      no crash; two findings of this slice, fixed: a NULL-safe join of only nullable keys
      past two relations was a ClickHouse error (now
      `null_safe_equal`), and an `EXISTS` compared with `=` / `<>` read
      `… > 0 = … > 0` (now a boolean column); the drive's cost doubled per
      subquery (fixed). Filed, not of this slice: parenthesized comparisons
      parsed as a chained comparison (#1355); fixed-length uniqueness over
      parallel rows without `edge_id` (#1356).
    - Live suite, switch on, vs S8d1: 4 FK pattern-predicate oracle tests
      now pass; 6 known-wrong goldens now Neo4j-equal (scorecard records
      legacy); 3 tests of legacy's loud refusals (#574 / #588 two-hop,
      bounded and OR'd pattern predicates, a polymorphic multi-type EXISTS)
      now answered, each checked against the tables.
- [x] S10 default on, in two parts:
  - [x] **S10a: the default flips** (`translate::BoundPlanMode::from_env`:
    on unless `CLICKGRAPH_BOUND_PLAN=off`).
    - A bind error other than `Unsupported` is the query's error
      (`TranslateError::Invalid`: HTTP 400 "Query error", Bolt "Invalid
      query"), not a reason to try the legacy path. These are queries Neo4j
      also rejects (S3: every bind error in the corpus checked on Neo4j).
      `Unsupported`, a clause-list parse failure and a lowering error still
      fall back.
    - Every entry point that has the query text passes it: `/query/sql` (so
      it returns the SQL `/query` runs) and the inner queries of
      `apoc.export.*.query` and `COPY TO` (HTTP and Bolt) were added. The
      library path that drops write clauses stays legacy, because its
      statement no longer matches the text.
    - Render-plan unit tests of legacy internals call
      `cypher_to_sql_legacy` (test-only, the route pinned off).
    - Budget: §8 asked for one but none was stated before; this slice set
      it after measuring, so it is open to review: the LDBC SF1 total within
      5% of legacy, and no query 2× or more slower where both paths give the
      same answer.
    - Acceptance:
      - Neo4j scorecard regenerated on the default (`social_integration`,
        `standard`): 57 CG_ERROR → MATCH, 22 MISMATCH → MATCH, 11 new
        entries MATCH; 431 MATCH, 18 MISMATCH (UNTYPED_BOOLEAN /
        EXTENSION), 8 CG_ERROR. 4 entries became CG_ERROR, each a read of a
        property the schema does not declare (the user's rule, S4a: it reads
        the same-named column; ClickHouse rejects the missing column). The
        triage rule for them (EXTENSION) matches the property names, not the
        SQL alias.
      - Neo4j oracle, switch off vs on, 20 schemas: 197 wrong → right; 5
        legacy-right → not equal, each accepted (`collect` order, two
        undeclared-property errors, two zeek port cases known since S8b).
      - Live suite on the default: 4825 passed; every remaining failure
        predates the slice (9 multi-tenant tests whose parameterized views
        the test database lacks, a timing test that compares a cold query
        with a cached one and also fails with the switch off, 2 collection
        errors). 67 tests that pinned legacy behaviour were rewritten, each
        against Neo4j or a brute-force oracle over the fixture (former
        refusals now compare rows; SQL-text checks now compare results;
        trail oracles for relationship uniqueness; 0-based lists). A strict
        xfail (#583, undirected OPTIONAL count) now passes and asserts the
        count. `test_denormalized_edges` replaced the shared
        `denormalized_flights` schema for every later test file; it now
        registers its own name.
      - LDBC SF1, 41 official queries, 5 runs, median: total 55.2 s → 57.1 s
        (+3.5%). Slower: short-2 3.4× (legacy's answer is wrong: 4 rows of
        10), bi-5 1.6×, bi-6 1.5× (legacy's answer is wrong: every score +1,
        a NULL tuple counted by `count(DISTINCT)`), bi-18 1.25×. Ground truth
        by hand-written ClickHouse SQL: no query legacy answered right is
        answered wrong; bi-6, complex-4 and short-7 are now right. Eight
        queries are empty or short because the LDBC schema cannot say a Post
        is a Message or declare City→Country edges (#1366); the patterns are
        impossible in the declared schema, so no rows is its answer (S1).
        Filed: #1364 (`collect` keeps a NULL list from a CASE as `[]`, both
        paths), #1365 (a mixed-type list as a grouping key, ClickHouse Code
        44).
      - Translation time (release, simple hop): 0.17 ms cold vs legacy's
        0.41 ms.
      - TCK (chdb) aborts at the same scenario with the switch off and on.
      - Adversarial review (the corpus through both routes, about 870
        queries on 22 schemas against Neo4j; about 170 scoping / error edge
        cases; Bolt, graph output, tenants, Databricks): no answer changed
        for the worse but the known zeek port and `collect` order cases;
        every `Invalid` is a query Neo4j's EXPLAIN rejects; the bound plan
        parameterizes every table for a tenant (legacy missed some in walks,
        unlabeled patterns and subqueries). Fixed: `/query/sql` answered an
        `Invalid` with 500 (now 400, as `/query`), the library doubled the
        "Query error" prefix, and the switch now also reads `false` / `0` /
        `no` as off. Left as found: the SQL golden and corpus-sweep tests
        translate without the text, so they lock the legacy route only
        (retired with it in S11); Bolt failures reach Python driver 6 as
        "unknown error" on both routes; a node table with duplicate ids
        multiplies walk rows (zeek `dns_log`: a schema whose `node_id` is not
        unique).
  - [x] **S10b: text only the clause-list parser reads** reaches the bound
    plan (`translate::translate_text_read`, `clause_list_use_name`). Every
    entry point parses with the legacy parser first; when that fails and the
    clause-list parser reads the text, the query is a read query translated
    by the bound plan alone (HTTP `/query`, `/query/sql`, Bolt, and the
    library's `cypher_to_sql` / `cypher_to_sql_with_metadata`, so embedded,
    FFI and `cg`). Its `USE` names the schema. When the bound plan does not
    translate it (`Unsupported`, an updating clause, `id()` outside a RETURN
    item, the route off), the answer is the legacy parse error, as before; a
    query Neo4j rejects is `Invalid`.
    - Shapes: `WITH a MATCH .. MATCH ..`, several UNWINDs after a WITH, a
      MATCH after an OPTIONAL MATCH after a WITH, free-standing ORDER BY /
      SKIP / LIMIT (all compiled by Neo4j 5.26). The corpus has none (S2:
      the clause-list parser rejects every corpus query the legacy parser
      rejects), so the scorecard is unchanged.
    - Bolt keeps only the parse error across its awaits (a parsed statement
      is not `Send`) and parses again where it consumes the statement.
    - Acceptance:
      - 15 generated shapes on `social_integration` (the families above,
        with WHERE, aggregates, OPTIONAL, ordered pages, `USE`): every one
        equal to Neo4j; legacy: a parse error for each.
      - Routes: HTTP `/query`, `/query/sql`, Bolt and `cg` give the same
        answers; `id()` / updating / misspelled text keeps its parse error.
      - Tests: unit tests of `translate_text_read` (translated, not
        translated, route off, `Invalid`) and the library; the endpoint
        test compares `/query/sql` with `/query` on two text-only shapes; a
        Bolt test that such text gets past the parse. The endpoint and Bolt
        tests fail with the routing disabled (checked).
      - Live suite: 4826 passed; the remaining failures are the 9
        multi-tenant tests that predate S10a.
      - Adversarial review (about 110 text-only queries against Neo4j on
        HTTP, Bolt and `cg`, a main-branch server beside it; writes, CALL,
        COPY TO, USE / cache, Databricks): no wrong answer; no write or
        procedure reaches the new route. Fixed: a text-only query over a
        schema that does not exist answered "Schema not found" before the
        syntax error (now the syntax error, as before); the message text
        of an untranslated one is the old "Query syntax error"; the inner
        queries of COPY TO and `apoc.export.*.query` (HTTP and Bolt) now
        translate as `/query` does (`translate::translate_inner_read`).
        Filed, older than S10: a USE in a later UNION arm is ignored
        (#1370); `CYPHER replan=force` drops a following USE (#1371); a
        Bolt session without a database gets an arbitrary schema (#1372);
        library translation overflows the stack on thousands of clauses
        (#1373).
- [ ] S11 legacy deletion. First the corpus is covered: after S10 the bound
  plan lowers 1420 corpus queries; the rest fall back. Of those, the 24
  the clause-list parser rejects are invalid Cypher or end without RETURN
  (S2), and 6 are bind errors Neo4j also gives. The others, in sub-slices:
  path list functions (S11a); `count()` of a carried list (S11b, #1375);
  lists of lists of nodes / relationships (`collect(nodes(p))`); `label()`, `id()` outside a RETURN item and
  ORDER BY an `id()` item; ordered SKIP / LIMIT after DISTINCT, aggregation
  or UNWIND; undeclared properties of a node embedded in edge tables;
  pattern predicates inside list comprehensions (#629); and errors where
  Neo4j refuses (a property of a variable-length relationship's list, a
  shortestPath from a node to itself, composite ids paired in another
  column order). Then the deletion PRs (§7 row 11).
  - [x] **S11a: path list functions.** `count(p)` / `count(DISTINCT p)` of a
    path or a path's list count the rows where it is not NULL, or its
    distinct values by its elements' identities (`count_graph_value`; a
    path that matches nothing counts 0 in one row, an aggregate);
    `length()` of a path's nodes / relationships is their size (Neo4j 4's
    reading, which the legacy pipeline answers; Neo4j 5 refuses it).
    `length()` of a variable-length relationship's list stays refused: the
    legacy pipeline fails on it and Neo4j refuses it. `size()` of a carried
    list an OPTIONAL MATCH may leave NULL is NULL there (its null-condition
    key), and signed (Int64) like every size. `count()` builds no value: a
    plain count reads only the NULL condition, `count(DISTINCT)` the
    identities (the demand pass asks a walk for its key only then,
    `PATH_KEY`).
    - Not lowered yet: `count()` of a carried list of nodes or
      relationships (carried as tuples). The legacy fallback miscounts
      `count(DISTINCT)` of one (#1375); next in the coverage list.
  - [x] **S11b: `count()` of a list of nodes or relationships** (#1375):
    a list of element tuples is never NULL (a path an OPTIONAL MATCH may
    leave NULL carries a value, not tuples), so `count(l)` counts rows and
    `count(DISTINCT l)` the distinct lists by their elements' whole tuples
    in order, as DISTINCT compares them (identities alone would merge
    parallel relationships of a type without `edge_id`, which Neo4j tells
    apart). Only a list built never NULL is counted (`never_null_element_
    list`: a path's nodes / relationships, a variable-length relationship's
    list, `collect()`, a list literal, their concatenation, or a WITH item
    of one); others fall back.
    - Acceptance: generated shapes against Neo4j (carried `nodes(p)` /
      `relationships(p)` of fixed, variable-length, undirected and union
      walks, `collect()`ed lists, list literals, grouped, nested WITH,
      ordered): every lowered shape equal to Neo4j. Corpus unchanged (1428;
      no corpus query counts a list). Scorecard unchanged. Live suite: the
      9 multi-tenant failures that predate S10a.
    - Still falls back: `collect()` of lists of nodes or relationships (a
      list of lists), which brings a query with it to the legacy pipeline,
      where `count(DISTINCT relationships(p))` in the same query is
      miscounted (#1375).
    - Adversarial review (about 120 shapes on five schemas incl. parallel
      relationships without `edge_id`): two findings, fixed: identities
      merged parallel relationships (7 vs 6; now whole tuples), and a list
      comprehension over `split(NULL)` (an array here, NULL in Cypher) was
      counted (now falls back; #1377). Filed: legacy fallbacks that answer
      wrongly for list-of-element shapes the bound plan refuses (#1378);
      the parallel-relationship identity contract gathered on #1356.
  - [x] **S11c: lists of lists of nodes or relationships.** `collect()` of a
    list that is never NULL (S11b's `never_null_element_list`) is an array
    of those lists; a returned list of lists maps each level to its
    elements' values, a lambda parameter per depth (`__cg_e` at the outer
    level as before, `__cg_e1`… inside: single-level SQL byte-identical).
    Bolt and graph output already recurse through nested `GraphType::List`.
    Retires the last #1375 repro (it fell back to the legacy miscount).
    - Acceptance: generated shapes against Neo4j (`collect(nodes(p))` /
      `collect(relationships(p))` of fixed and variable-length paths,
      `collect()` of collected lists incl. over OPTIONAL rows, nested
      literals, UNWIND of lists of lists, sizes, `collect(DISTINCT
      relationships(p))`): every lowered shape equal to Neo4j, list orders
      compared as multisets where they follow `collect()`. Corpus and
      scorecard unchanged; live suite unchanged.
      - Adversarial review (about 300 shapes on six schemas incl. parallel
        relationships, composite ids, FK edges, polymorphic and denormalized
        tables; HTTP, Bolt and graph output; depth-3 lists, inner NULLs,
        shadowed comprehensions, ordered collects): no defect; the
        never-NULL gate held. Filed, older than S11c: lists mixing labels
        fall back to legacy bare ids (#1380); more list-of-list legacy
        fallbacks (#1378); `reduce()` with an `[]` accumulator and parser
        gaps (#1381). Returning millions of node values is slow
        (serialization; the flat `RETURN nodes(p)` costs the same).
  - [x] **S11d: `label()`, and `id()` of what matches nothing.** `label(n)`
    (a ClickGraph function: Neo4j has `labels()` only) is a node's one
    label as a string, as the legacy pipeline reads it: the label of a
    node of one label, the label column of a node of several (a union of
    labels), NULL where an OPTIONAL MATCH left it NULL; of a list's node
    element too. `label()` of a relationship stays refused (the legacy
    pipeline has no reading of it). A function of an element that matches
    nothing is NULL (a MATCH of it has no rows; an OPTIONAL MATCH leaves it
    NULL) before the `id()` refusal, so `RETURN id(a)` over an impossible
    pattern lowers; its `valueType()` is "NULL", as in Neo4j.
    - Not yet: `id()` comparisons in a WHERE and ORDER BY an `id()` item
      (the server's encoded id: the HTTP / Bolt rewrite turns comparisons
      into key predicates, which the bound plan does not read yet). Done in
      S11e.
    - Acceptance: `label(x)` against Neo4j's `labels(x)[0]` (`neo ||| cg`)
      on VLP and union-walk ends, unlabeled nodes, DISTINCT / grouped /
      filtered, OPTIONAL, list elements: every shape equal. Corpus 1437
      lowered (was 1428). Scorecard: two CG_ERROR entries now run (their
      `id()` makes them incomparable with Neo4j). Live suite unchanged.
      - Adversarial review (about 260 comparisons on eight layouts incl. a
        label-column node table, where `label()` reads the row's label and
        the legacy pipeline is wrong on 13 shapes; OPTIONAL / EXISTS /
        pattern comprehensions / UNION arms over impossible elements): no
        defect. Fixed alongside: `valueType()` of an element that matches
        nothing is "NULL" (was null). Filed: Bolt UNION `id()` returns raw
        keys, and loud gaps (#1383).
  - [x] **S11e: `id()` compared with integers, and ORDER BY `id()`.**
    `id(n) = N`, `id(n) <> N`, `id(n) [NOT] IN [N, …]` of a node with
    integer literals (Neo4j Browser's expand and fetch queries; Bolt inlines
    their parameters) are lowered as the `id()` rewrite reads them: on the
    server (HTTP, Bolt, `/query/sql`, export inner queries) an `N` is one of
    the server's ids (`ReadOptions::id_mapper`; the session's cache, another
    session's of the same scope, else the bit pattern of a label code and a
    small integer key: `id_function::resolve_node_id`, shared with the
    rewrite), the node of that label and key or none; in the library
    (embedded, `cg`) `N` is compared with the key, as `id(n)` is the key
    there. `<>` reads the same ids (legacy compared `<>` with the raw key, so
    it excluded nothing). An `id()` RETURN item, and `ORDER BY id(n)`, sort
    by the key, as the legacy pipeline does: the order of the id HTTP
    returns, and of Bolt's encoded ids for a node of one label with a
    non-negative integer key (a string key's encoded id is a hash, which
    has no meaningful order). Other uses of `id()` (`id(n) > 3`, `id(r)`,
    `id(n) + 1`) stay refused. The Bolt guard that sent queries with `id()`
    constraints to the legacy pipeline is gone.
    - Fixed alongside: a Bolt-cached id decoded on HTTP kept Neo4j
      Browser's trailing `-` (`IdMapper::decode_for_query`: `'2-'` against
      an integer key, ClickHouse Code 53, on both pipelines).
    - Acceptance: live HTTP and Bolt, bound plan on vs off, with ids the
      servers returned (`social_integration`, composite ids): every shape
      equal or the legacy pipeline wrong (a `(a:Post)` matched a User's id;
      `<>` excluded nothing; `NOT id(b) IN [..]` of an unmatched `b` was
      true, NULL in Neo4j; a NULL `id(b)` was encoded as a number on Bolt;
      a composite `IN` split into two rows). Corpus 1439 lowered (was 1437:
      the two ORDER BY `id()` queries). Scorecard unchanged; live suite
      unchanged.
    - Still falls back: the corpus's expand query is a UNION whose arms
      return `o` of different label sets (S7d).
    - Adversarial review (about 210 comparisons, on vs off vs main, HTTP /
      Bolt / library, on social, composite, FK-edge, denormalized,
      polymorphic and a mixed-key schema: labels with UInt64, String and
      Int64 keys): three findings, fixed. A several-labels node's key (a
      `Variant` when its labels' key types differ) is compared as text
      (`'x'` against the Variant was Code 53); the library compares the key
      with the integer's text, read as the column's type (a String key
      compared with an integer was Code 386); a one-column key is the id's
      whole text (a key `p|q` was split as a composite's parts). The
      library's `id(n) = N` of a several-labels node matches every label's
      node with that key, as `RETURN id(n)` is the key there. Filed: a
      several-labels node of mixed key types carried through WITH and
      joined fails (#1390, older; such a query with an `id()` WHERE ran on
      the legacy pipeline before); composite ids on Bolt (#1387); id
      decoding across scopes (#1388). Older id gaps the review listed
      (HTTP `id()` parameters are not decoded; keys 0 and 1 share an
      encoded id; HTTP caches id-dependent SQL by text) predate S11e.
  - [x] **S11f: UNION ALL columns of nodes of different labels, or
    relationships of different definitions.** Under UNION ALL a column
    whose arms hold nodes of different labels (one label, or several: an
    unlabeled node), or relationships of different types or label pairs, is
    a value in every arm (`Query::as_values`; the arms are lowered again
    with it so), as a node of several labels returned alone is. Refused:
    such a column under UNION (not ALL), which must tell the same element
    in different arms, whose identities the arms spell differently (a type
    union pads its ids with NULLs, a list's element is its tuple, a
    several-label key is a `Variant`: the review found duplicates from
    each); `v.*` of such a column; a node in one arm and a relationship or
    a value in another (the legacy pipeline errors on all three, or
    answers wrongly).
    - Acceptance: generated shapes against Neo4j on `social_integration`
      (User and Post, a node's neighbours in both directions, relationships
      of different types and of a type union, with values, ordered arms
      with LIMIT, grouped and DISTINCT arms, NULL nodes and relationships
      of OPTIONAL MATCHes against several-label arms): every lowered shape
      equal to Neo4j (the legacy pipeline errors on every one). Bolt
      returns them as nodes and relationships. Corpus 1440 lowered (was
      1439: the browser-expand UNION ALL). Scorecard and live suite
      unchanged.
    - Adversarial review (about 85 shapes on six schemas, incl. a mixed-key
      schema): every UNION ALL shape equal to Neo4j; three UNION (not ALL)
      duplicates and `v.*` changing meaning, fixed by refusing them. Filed,
      older: a several-label node's String key `'01'` reads as `1` when its
      labels' key types differ (#1393); DISTINCT of a node value with an
      aggregate fails (Code 184).
  - [x] **S11g: SKIP / LIMIT and `collect()` after an UNWIND of rows in no
    order.** Neo4j returns each row's elements together, in list order,
    the rows in no particular order; the SQL keeps neither. An UNWIND whose
    order a later clause relies on with no ORDER BY between (a SKIP /
    LIMIT, a `collect()`: `ordered_unwinds`, a pass over the plan) numbers
    the rows first in any order (`Unwind::any_row_number`, `row_number()
    OVER ()`), and its rows are ordered by that number and the element's
    position, as after ordered rows. Other UNWINDs are unchanged (no
    window), as are UNWINDs followed by a MATCH (which leaves its rows in
    no order). `collect()` sorts by the lowering's own numbers directly
    (`Lowerer::numbers`), with no second numbering. Still refused:
    SKIP / LIMIT or `collect()` relying on the first-seen order of a
    DISTINCT or an aggregation of ordered rows (no corpus query); and a
    plan that reads more than once a CTE whose rows depend on that
    numbering (`unwind::reject_rereads_of_numbered_rows`: ClickHouse numbers
    the rows anew on each read, so an OPTIONAL MATCH's or a subquery's
    drive or a path's seed over rows a LIMIT took, or a `collect()` listed,
    would disagree with the other read). Those fall back to legacy, as
    before S11g; the general class (an unordered LIMIT, tied ORDER BY or
    `collect()` order read twice) is #1395.
    - Acceptance: generated shapes against Neo4j whose answer is the same
      for every order Neo4j may return (each user's list `[10k+1, 10k+2]`:
      a LIMIT's rows are one list's prefix, `collect()` per row keeps list
      order, SKIP + LIMIT spans lists contiguously, nested UNWINDs): every
      lowered shape equal to Neo4j (legacy errors or answers wrongly on
      six). Corpus 1442 lowered (was 1440: the two `collect` / UNWIND /
      LIMIT queries). Scorecard and live suite unchanged.
    - Adversarial review (validity checker of 26 shapes on 60 / 40k / 3M
      rows under several thread and block settings; 78 shapes against
      Neo4j): found that `row_number() OVER ()` re-read through a drive or
      seed after `WITH … LIMIT` gave silently wrong rows from 40k rows up
      (OPTIONAL MATCH all NULL in 19 of 20 runs; variable-length and
      shortestPath counts 0, which legacy answered). Fixed by the refusal
      above (a unit test fails without it); the checker is valid on all 26
      shapes on 60 and 3M rows after the fix. Also fixed: MATCH between
      the UNWIND and the paging no longer numbers the rows, and grouped
      `collect()` after an UNWIND no longer sorts every row by a second
      window (60M elements: 2.6 s → 0.5 s; memory 6 → 4.5 GB, the
      per-element keys, #1397). Not fixed: a list of `UInt64` and `Int64`
      columns is a `Variant`, so `collect(DISTINCT x)` over it now counts
      equal integers twice, as `count(DISTINCT x)` already did (#1396).
  - **Before the deletion (2026-10-11, user: cutover first).** A trial of
    the ClickHouse cutover (what the bound plan does not lower becomes an
    error, parked on `feature/p4c-cutover-clickhouse`) failed 34 live
    tests the corpus census does not see (their schemas are not in the
    census's map): edge `constraints:` (19, S11h), shortestPath over one
    fixed-length relationship (10, S11i), shortestPath to self (2, agreed
    errors), a query without RETURN (2: a ClickGraph extension that
    returns the matched nodes; Neo4j refuses it). On Databricks the bound
    plan lowers 1135 corpus queries (1442 on ClickHouse): 320 more still
    reach the legacy pipeline there (nodes embedded in edge tables 161,
    path values / element lists 106, shortestPath 29, UNION 10, UNWIND 8),
    and the embedded write path plans its read part with the legacy
    analyzer. Order: S11h, S11i, the ClickHouse cutover (Databricks and
    writes keep the fallback), Databricks coverage (S12), writes, then the
    deletion PRs.
  - [x] **S11h: edge `constraints:`.** A relationship of a definition with
    a `constraints:` expression exists only where it holds of its two
    nodes (`from`: the node it leaves as stored). The expression is bound
    as Cypher over the hop's two nodes (`expr::bind_edge_constraint`) and
    read of their declared column properties (as the generator compiles
    it: anything else is `Unsupported`, never dropped); the demand pass
    reads them of the ends (`constraint_reads`), so a carried node exports
    them.
    - One definition, one direction: a condition on the hop's nodes; both
      directions (`both_directions`): each row says which way it was
      stored (`__cg_forward`), the condition picks `from` by it.
    - Several definitions read together (`rel_union_of`): each
      constrained definition's arm joins its two node tables and keeps
      the rows the constraint holds of (`constrained_rows`), so single
      hops, union walks and their shortest-path searches all read only
      the relationships there are.
    - A directed walk of one definition: the generator's constraint (base
      and recursive steps), the walk forced to start at the stored `from`
      end (the generator reads `from` at the node a step leaves; a
      backward walk would invert it) and carrying what the next step reads
      of that node (`end_<property>`); undirected or a shortest path: the
      union walk above.
    - An OPTIONAL MATCH reads the node it shares from its table (not the
      edge's endpoint columns) when the edge is constrained, and a drive
      exports what a constraint reads of it (the clause's `read_props`).
    - Acceptance: 43 generated shapes on a scratch graph (FOLLOWS / WROTE
      constrained, KNOWS not, self-loops) equal to Neo4j, whose queries
      state the constraint explicitly (`all(r IN relationships(p) WHERE
      …)`); legacy wrong on 19, errors on 16. With the cutover applied, the
      19 live tests pass (three asserted legacy's SQL text and now check
      rows or alias-free SQL). Corpus 1442, scorecard and live suite
      unchanged.
    - Adversarial review (about 140 shapes on four scratch schemas incl.
      `OR` / `NOT` / arithmetic / NULL constraints, FK, polymorphic,
      composite and denormalized layouts): fixed an `OR` constraint the
      generator spliced unparenthesized into each step (`(c1 AND …) OR c2`
      escaped the hop limit and uniqueness: 26 rows vs 4, runaway
      recursion with cycles; legacy too); the OPTIONAL MATCH guard checked
      only the first relationship at the shared node (now: whatever any
      constraint reads joins the clause's `read_props`); a walk restricted
      only at its `to` end, forced forward, expanded from every node (9.3
      s at `*1..5` on 100k nodes; now the union walk, 0.2 s); carried
      constraint properties named `end_<prop>` met the generator's own
      `end_id` (now `end_cg_<prop>`). The 138 shapes rerun equal to Neo4j
      but for `/` (integer division, pre-existing). The text paths' other
      readings of a constraint (string literals, Cypher-only syntax) are
      #1402.
  - [x] **S11i: shortestPath over one fixed-length relationship.**
    `shortestPath((a)-[:R]->(b))` is the shortest path of exactly one
    relationship: the binder binds a relationship of a shortest-path
    pattern written without a length as `*1..1` (the same paths; the SQL
    is identical, a unit test checks it). Named (`[r:R]`), it is that
    list, as Neo4j binds it (review: `size(r)` is 1 there, `type(r)` a
    type mismatch). Found by a trial of the ClickHouse
    cutover: the 10 live tests of #1205's bare-hop form, which pass with
    it applied; 6 generated shapes on `social_integration` equal to Neo4j
    (one more, `nodes(p)` of an impossible path, fails on both pipelines
    as with `*1..1`).
  - [x] **S11k: FINAL on a joined table.** A table read with FINAL
    (`use_final`, or a ReplacingMergeTree-family engine the server
    detects) that is not the FROM table is joined with it: `JOIN t AS a
    FINAL` (`Join::use_final`, printed where the dialect has FINAL). Each
    relation keeps its FINAL where `path_first` / `rows_holding` reorder
    the joins (`path_first` no longer keeps a FINAL FROM first). It was
    refused, and the legacy pipeline read joined tables without FINAL
    (stale row versions). Found by the cutover's review: on such engines
    every traversal would have been an error. Review: a first cut joined
    `(SELECT * FROM t FINAL)`, which drops ALIAS / MATERIALIZED columns
    (`*` leaves them out: Code 47 with auto-discovered schemas); the node
    relations of walks and searches, still subqueries, now read them
    (`FunctionMapper::all_columns_settings`) and take FINAL from the
    dialect (`final_modifier`).
    - Acceptance: a differential of 20 shapes (hops each way and
      undirected, two hops, walks, shortestPath, OPTIONAL MATCH, pattern
      predicates and comprehensions, carried nodes, aggregation, UNION)
      over ReplacingMergeTree tables holding several versions of 15
      people and 40 relationships, with `use_final`, against the same
      graph's latest versions in plain MergeTree tables: equal rows
      (a comprehension's list in another element order).
  - [x] **S11l: `id()` as a value** (user 2026-10-11). A node's `id(n)`
    as a value is its key; `id(a) = / <> id(b)` compares the elements
    (`identity_comparison`: different labels never equal); a WITH item
    `id(n)` returned later keeps `ResultKind::NodeId` (`Lowerer::id_values`)
    so Bolt encodes it. `id(r)` of a relationship is Bolt's number for it
    (`IdMapper::compute_deterministic_id` of its element id, which only
    Rust computes: a per-process label code and `DefaultHasher`): a RETURN
    item (`ResultKind::RelId`) holds the element id text (`value::
    rel_element_id`) that Bolt, HTTP JSON (`execute_cte_queries`, such a
    query is not cached) and the embedded library
    (`cypher_to_sql_with_id_columns`) turn into the number; HTTP text
    formats and SQL-only output keep the text. Other `id(r)` uses and
    `elementId()` stay refused. Found by the cutover's review (legacy
    answered them, `id(r)` with a different number than `r`'s).
    - Acceptance: live over Bolt and HTTP (social_integration): `id(r)`
      equals the returned `r`'s id on every row; HTTP returns the same
      numbers; a carried `id(a)` equals `RETURN id(a)` over Bolt and is the
      key over HTTP; `id(a) = id(b)` of all user pairs counts the users; an
      unmatched `id(r)` is NULL; `cg query` returns the library's number.
    - Adversarial review (ids against Bolt's `r` / paths on six schemas,
      the cache, every output format): fixed Bolt returning the element id
      text when no graph item was beside `id(r)` (its rows skipped the
      transform), and `ORDER BY id(r)` sorting by that text (now refused:
      the number exists after the query). Documented: an id carried as a
      value is the key alone (labels lost). Filed: parallel relationships
      share Bolt's id, as Bolt's `r` does (#1409); `WHERE id(r) = <Bolt
      id>` (#1410).
  - [x] **S11m: a map's keys.** A map literal a WITH projects also
    exports each key's value as its own column (`{v}__k{i}`, a constant
    kept as it is: `export_map_keys`), carried through later segments
    (`Exports::maps`, `Lowerer::map_keys`, re-exported by `export_scope`);
    `m.k` / `m['k']` read it in its own type, a key the map does not have
    is NULL, and the projection's own WHERE / ORDER BY read the keys over
    its input. A map whose keys are not known where it is read (a
    parameter, a list's element, a computed key) stays refused. Found by
    the cutover's review (legacy answered `m.n`, and `m.k` of an integer as
    a string).
    - Acceptance: 12 generated shapes on `social_integration` equal to
      Neo4j (typed keys, missing keys, WHERE / ORDER BY / DISTINCT /
      aggregation over keys, NULL values, two WITHs, OPTIONAL MATCH
      values, UNWIND); legacy errors on 5, wrong on 5.
    - Adversarial review (about 110 shapes): fixed `m.a.b` reading NULL
      (the dotted text was looked up as one key: now refused), and a key
      read in an aggregating WITH's own WHERE / ORDER BY reading the key
      ungrouped (ClickHouse Code 215: now `any()` of it, as the map is
      read). Rerun: 18 shapes equal to Neo4j.
  - [x] **S11n: lambdas of higher-order functions.** A lambda written as
    a function's argument (`ch.arrayFilter(x -> x > 5, l)`, the documented
    ClickHouse passthrough) is the dialect's lambda
    (`FunctionMapper::lambda`) over the binder's local parameters (which
    shadow variables of the same name). It was refused; the legacy
    pipeline answered the wiki's examples (found by a census of the wiki's
    Cypher examples before the cutover).
    - Acceptance: the wiki's 18 `ch.*` lambda examples give the legacy
      pipeline's results (the ones over properties the benchmark schema
      lacks fail on both).
    - Adversarial review (about 230 shapes against legacy on three
      schemas, the server, Databricks): fixed a body giving a node,
      relationship or map (read as its internal tuple: now refused, a list
      comprehension returns them) and repeated parameters (accepted, the
      last shadowing: now "already declared", as ClickHouse).
  - [x] **S11j: the ClickHouse cutover.** A ClickHouse read the bound plan
    does not lower is an error (`TranslateError::Unsupported`: HTTP 400
    "Not supported: …", Bolt a client error, the library "Not supported:
    …"), never the legacy pipeline; text the clause-list parser rejects
    is a syntax error on every dialect (a query without RETURN, which the
    legacy pipeline answered with the matched nodes: user 2026-10-11). The
    switch `CLICKGRAPH_BOUND_PLAN` is gone; `ReadOptions::bound_plan =
    Off` reaches the legacy pipeline for its own tests until it is
    deleted. What still reaches it: other dialects
    (`SqlDialect::reads_fall_back_to_legacy`) and the embedded write path's
    read part (no query text). Entry points answer standalone procedures
    and reject SET / DELETE before translating; a statement that is not a
    read reaching it (`BindError::NotARead`, or text only the legacy parser
    reads: a CALL inside a query or a UNION, CREATE) is "Not supported" on
    ClickHouse (`translate::not_a_read`).
    - Acceptance: full live suite on the cutover build: 4827 passed (the 9
      multi-tenant failures predate S10a). Tests changed to the new
      answers: a query without RETURN, shortestPath from a node to itself
      (errors, user 2026-10-11). Corpus 1442 lowered; scorecard verdicts
      unchanged (4 queries Neo4j rejects that legacy answered now error on
      ClickGraph too: NEO_ERROR 15 → 11, BOTH_ERROR 14 → 18). Embedded /
      tool / client crates, the embedded chdb e2e tests and the Python
      bindings pass but for failures that main has too.
    - Review polish: a refusal names the construct (`not_translated`
      drops the plan's wording and slice tags; Bolt
      `Neo.ClientError.Statement.FeatureNotSupported`), a broken invariant
      is `TranslateError::Internal` (HTTP 500, Bolt a database error); a
      CALL / updating statement the clause-list parser does not read
      (`CALL p() YIELD x WHERE …`) is not a syntax error: the server answers
      it as a standalone procedure (found by a census of the wiki's 815
      Cypher examples; the procedure route ignoring the WHERE predates it:
      #1411). Second review: a CALL inside a query or a UNION and CREATE
      reached the legacy pipeline (the CALL dropped: `MATCH (u) CALL
      db.labels() YIELD label RETURN count(*)` gave 30, not 60; a library
      CALL gave `WHERE false`; CREATE a 500): now "Not supported" on
      ClickHouse. Also: `id(r)` over several types names that limit; an
      inner export query's internal error is 500; `not_translated` keeps
      backticked text and nested `internal:` reasons. Bolt failure codes
      reach 5.7+ drivers once they carry GQL fields (#1368, #1389). Rerun on
      main with S11k–S11n: live suite 4826 passed (the 9 multi-tenant
      failures, plus `test_filter_early_vs_late`, a timing ratio that is
      flaky on main too); corpus 1442; scorecard verdicts unchanged.
  - **User decisions (2026-10-11)** on what S11 does not lower: a
    relationship without `edge_id` keeps its ends as its identity (#1356;
    define `edge_id` to tell parallel rows apart); an undeclared property
    of a node embedded in edge tables, and the shapes Neo4j refuses (a
    property of a variable-length relationship's list, shortestPath from a
    node to itself, composite ids paired in another column order, a
    pattern predicate over an integer list's element as a node, #629's 11
    corpus queries: Neo4j's "Type mismatch"), become errors at the legacy
    deletion; `length(nodes(p))` keeps answering.
    - Acceptance:
      - Generated shapes against Neo4j on `social_integration` (`count(p)`
        grouped, DISTINCT, OPTIONAL, union walks, zero-length paths,
        undirected, carried; `length()` of `nodes(p)` / `relationships(p)`
        against Neo4j's `size()`; carried lists an OPTIONAL MATCH left
        NULL): every lowered shape equal to Neo4j.
      - Corpus: 1428 lowered (was 1420). Scorecard: one CG_ERROR → MATCH
        (a carried relationship list over an impossible pattern).
      - Live suite: only the 9 multi-tenant failures that predate S10a.
      - Adversarial review (about 330 shapes on seven schemas incl.
        parallel relationships with `edge_id`, self-loops and composite ids
        with a NULL part; rerun after the fixes on six): every lowered shape
        equal to Neo4j. Fixed: `count(p)` of a walk built every element's
        value inside the search (7.6 s vs legacy's 0.38 s on the social
        benchmark, now 0.36 s; `count(DISTINCT p)` 0.72 s; a unit test
        checks no value is built), and a NULL-able unsigned size mixed with
        a negative number failed in a grouping key (Code 44; now signed).
        Filed: the legacy fallback's `count(DISTINCT)` of a list of
        relationships (#1375).
- [ ] **S12: Databricks coverage.** The bound plan lowers on Databricks what
  it lowers on ClickHouse, so the legacy pipeline can go on every dialect.
  Each `FunctionMapper` spelling that is `None` on Databricks (`unwind`,
  `one_type_guard`, `lists`, `graph_values`, `cypher_union`,
  `shortest_path_search`) gets a Spark one, checked by running each
  corpus query's bound-plan SQL on both engines over the same data
  (`scripts/oracle/spark/`: the ClickHouse SQL on ClickHouse, the
  Databricks SQL on Spark 4.1 / Delta, rows compared). Census:
  `cargo test --test integration the_corpus_lowers_on_databricks`
  (`CENSUS_SQL_OUT` writes each dialect's SQL, `CENSUS_CORPUS` reads other
  queries). At the start: 1135 lowered on Databricks of ClickHouse's 1442;
  the differential over them matched every comparable query (763; 20 in
  another order under ties).
  - [x] **S12a: UNWIND and nodes embedded in edge tables.** Spark has no
    ARRAY JOIN: an UNWIND is a `LATERAL VIEW explode`, with its positions
    `posexplode` (`ArrayJoin::position`, `FunctionMapper::array_join_clause`,
    which the plain printer calls: the list read once), lists read in step
    zipped, rows numbered over `ORDER BY 1` (Spark numbers ordered windows
    only: one partition, a cost on large unordered inputs), one row
    `(SELECT 1)`. A property several sources hold is their `coalesce`
    under a `typeof` guard that raises an error when the types differ
    beyond integer width and string representation (`one_type_guard`;
    ClickHouse fails such a query with no rows too, Spark on a row read);
    any of the rows' values is `any_value(…, true)` (`any_non_null`: CH
    `any` skips NULLs, Spark's needs it). Also: Spark `split` reads its
    delimiter as a regex (`split('a.b', '.')` gave four empty strings):
    quoted as `Pattern.quote` does (an empty one kept: characters); Spark
    `sequence` is never empty: `range` is empty when its step leads away
    from its end, each argument bound once; `size()` of a list expression
    is `FunctionMapper::list_size` (Spark `array_size`: `length` is of a
    string only, `size(NULL)` is -1 unless ANSI; a property holding an
    array is not known as a list yet). A list of values of different types
    (`[u.name, u.age]`) fails on Spark (one element type: VARIANT, with
    graph values).
    - Acceptance: Databricks lowers 1289 (was 1135). Differential over
      every comparable query: 1062 equal, 20 in another order under ties;
      the rest are ClickHouse expressions in a schema's YAML
      (`property_expressions`, 16), errors on both, or filed: a CASE WHEN
      NULL ClickHouse matches (#1412), `count()` accepted (#1413),
      Databricks `WHERE NULL` (#1414), ClickHouse `UNWIND split(null)`
      (#1415), Databricks ORDER BY an aggregate of an impossible pattern
      (#1416). 46 generated UNWIND / split / size shapes: every one both
      dialects lower equal.
    - Adversarial review: fixed a list read twice for its positions
      (`transform`: a `rand()` list gave a NULL element; now `posexplode`),
      `split` with an empty delimiter (a trailing ""), `range` with a step
      away from its end (Spark `sequence` never empty), the type guard's
      regex escaping (and collated strings), and the harness: queries that
      read no table compared, a LIMIT subset only without ORDER BY
      (`TIES_LIMIT` with it), `collect()` lists sorted only before any ORDER
      BY. Rerun over the corpus (literal queries now compared): 1128 equal,
      43 in another order and 12 of other rows at a LIMIT, all ties; an
      embedded node whose sources disagree on a property (zeek's IP port)
      gives either engine's `any` pick. Filed: ClickHouse `split` with an
      empty or multi-character delimiter (#1418), ClickHouse UNWIND gaps
      (#1419).
  - [x] **S12b: lists.** `FunctionMapper::lists` on Spark: comprehensions
    `filter` / `transform`; an ordered `collect()` sorts `(key, value)`
    structs by the key alone (`array_sort` with a comparator: a value need
    not be orderable); `collect_list` skips the absent rows (`CASE WHEN`);
    a list's node or relationship is `Lists::tuple`, a `named_struct` of
    positional fields `_1`, `_2`, … (Spark's `struct(a.x, …)` names a field
    after its column: none can be read by position), read as `(t)._i`; the
    empty list of a walk's tuples is a `collect_list` over no row;
    `array_distinct` keeps first occurrences; a map is a
    `MAP<STRING, VARIANT>` (`map_keys` in key order). A pattern
    comprehension that matches nothing reads `[]` through `Lists::or_empty`
    (Spark's unmatched LEFT JOIN column is NULL; ClickHouse fills an
    `Array` column's default, so `None` there). A map that is neither
    grouped nor made distinct needs no `cypher_union` key: Spark refuses a
    map in GROUP BY / DISTINCT, so those stay refused ("a grouped or
    DISTINCT map", with UNION: S12d). `nodes(p)` / `relationships(p)` of a
    path read through the graph-value spelling: S12c.
    - Acceptance: Databricks lowers 1294 (was 1289; 4 path
      queries now stop at graph values). Ad hoc differential (88 queries
      over social_integration and composite_id: comprehensions nested and
      over NULL lists, ordered and DISTINCT collect, lists of nodes and
      relationships read by property, UNWIND of them, variable-length
      relationship lists incl. `*0..`, pattern comprehensions empty or
      not, map literals and keys): all 69 that both dialects lower equal.
      Corpus differential: 1132 equal (was 1128: the 5 newly lowered,
      one in another order under a tie), the rest as at S12a. Filed: list predicates `all()` / `any()` / `none()` /
      `single()` do not parse on any dialect (#1420, documented as
      supported), nor do backtick-quoted map keys (#1421).
    - Adversarial review (135 more differential queries, no wrong answer;
      ClickHouse SQL byte-identical over the corpus): fixed `x IN <list
      expression>` (Spark's `IN` takes a parenthesized list only:
      `FunctionMapper::in_list_expression`, `array_contains`, at all three
      operator renderers), a list index / `head` / `last` past the end
      failing under ANSI mode (Spark 4's and Databricks' default:
      `try_element_at`; also the legacy path's, 22 Databricks goldens), a
      map value that is a map (`to_variant_object`: a cast refuses it) and
      an empty list in a map refused (`Lists::map_of` takes `MapEntry`s,
      `None`: no VARIANT holds a list of no type). Filed: ClickHouse
      `x IN <list expression>` with a NULL element answers false (#1423;
      Spark's NULL is Cypher's), a pattern comprehension from a NULL node
      reads `[]` on both (#1424, to check against Neo4j), `collect(f) + [u]`
      in an aggregation not grouping `u`'s columns (#1425, both dialects).
  - [x] **S12c: graph values.** `FunctionMapper::graph_values` on Spark: an
    element is a VARIANT object (`to_variant_object` of a
    `MAP<STRING, VARIANT>`; a value that is an object or a list is a VARIANT
    already or casts to one), a path or a list of elements an
    `ARRAY<VARIANT>` (an empty one typed); properties without NULLs by
    `map_filter`; `tail` by `filter` on the index, `interleave` by
    `flatten(transform(…))`, `length` `array_size`, `any` `any_value(…,
    true)`. A composite identity's key text is the new
    `GraphValues::tuple_text` (`value::identity_text`, at every key: a
    relationship's, a walk's node, a shortest-path search's, a union walk's
    edges): ClickHouse `toString(tuple(…))` as before, Spark a JSON object
    of the parts' texts by position, NULLs kept (`CAST(struct AS STRING)`
    quotes no part: `('a, b')` and `('a', 'b')` would read alike). The Databricks executor reads a
    `VARIANT` / `MAP` / `STRUCT` column's JSON text as the object or array
    it is (a scalar left as it is: a VARIANT string `"123"` is no number);
    to verify against a live workspace. A datetime inside a value is Spark's
    text (`2024-01-01 10:00:00+00:00`), as ClickHouse's is its own (#1068).
    Walks over several labels or types also need the shortest-path search
    spelling (S12e).
    - Acceptance: Databricks lowers 1324 (was 1294). Corpus differential
      1158 equal (was 1132; the 30 newly lowered: 26 equal, 2 the zeek
      embedded IP's `any` pick and #1426, 1 an error on both, 1 not
      lowered on ClickHouse). Ad hoc (21 queries: paths
      fixed, variable-length, undirected, incoming, zero-hop, OPTIONAL
      NULL; `nodes` / `relationships`; collected nodes and relationships,
      ordered or not; nodes of several labels and relationships of several
      types): every one both dialects lower equal (one under a tie). Filed:
      ClickHouse reads an embedded node's array property as NULL when one
      source lacks it (#1426, `any` over a `Variant`; Spark right), an
      internal error ordering a carried path by `length(p)` (#1427).
    - Adversarial review: fixed DISTINCT paths overcounted on Spark with
      composite identities (16 vs 22 on test_fixtures, 24 vs 32 on
      denormalized flights): a walk's typed tuples (`path_edges`, a
      composite `path_nodes`) were texted by `CAST(struct AS STRING)`, a
      fixed hop's by `tuple_text`, so one path cut two ways was two keys.
      `GraphValues::tuple_value_text` texts a tuple value as `tuple_text`
      its parts (Spark: cast to `STRUCT<_1: STRING, …>`, `to_json`), and
      `prefixed_texts` takes the elements' arity (`TrailEdges::Typed`
      carries it). The executor recognizes a container by its type text
      too, and an array's container elements delivered as JSON text. Noted:
      a float or `UInt8` boolean id's element id reads as each engine
      texts it (`1.0` / `1`); a `Map`-typed property fails on Spark
      (`CAST(MAP AS VARIANT)`), loudly.
  - [x] **S12d: UNION and grouped maps.** `FunctionMapper::cypher_union` on
    Spark: a column of values is a VARIANT (`CAST(… AS VARIANT)`), grouped
    by `distinct_key`, a text of the value's kind and value (Spark groups,
    compares and sorts no VARIANT, `variant_key`): `n:` a number as a
    Cypher number (an integral one within BIGINT as its integer, any other
    as its double: `1` = `1.0` = decimal `1.0`, `-0.0` = `0`, `0.0001` either
    way), `b:` a boolean, any other scalar by its type and text (a date is
    no string), `l:` / `m:` a list or map by its elements' keys (two levels,
    then JSON), `:` NULL; `any_value` of a group (which of equal values, as
    ClickHouse's `any`).
    A map literal is now a VARIANT object (`to_variant_object`, S12b's
    `MAP<STRING, VARIANT>` could not be a VARIANT: a UNION's or a map's
    value), so a map nested in a map needs no special case
    (`MapEntry::holds_maps` gone); `keys()` reads it as a map
    (`CAST(… AS MAP<STRING, VARIANT>)`, keys sorted as a VARIANT object
    holds them). With the key, a grouped or DISTINCT map lowers on Spark.
    The Databricks executor reads a VARIANT column's JSON text whatever it
    holds (Databricks renders a VARIANT as JSON: a string with its quotes),
    a MAP / STRUCT column's object; to verify against a live workspace.
    - Acceptance: Databricks lowers 1334 (was 1324). Corpus differential
      1166 equal (was 1158; of the 10 newly lowered UNIONs 8 equal, 2 are
      arms with LIMIT and no ORDER BY: any rows, as many). Ad hoc (17:
      `1` / `1.0` / `true` / `'1'` / NULL in one column, lists, maps,
      ordered arms, nodes and paths, grouped and DISTINCT maps, a boolean
      with an integer): every one equal; S12b's 61 list and map queries
      rerun with maps as VARIANT objects: every one equal (63 now lower).
    - Adversarial review (Neo4j oracle): fixed the key splitting a decimal
      and a double of one value (`CAST(double AS STRING)` is scientific),
      merging decimals past 18 digits and failing past 10^20 (a
      `DECIMAL(38, 18)` cast; now as Cypher's double), lists and maps
      compared by JSON (`[1]` ≠ `[1.0]`; now by their elements' keys), and a
      date merged with its text (now by type). Rerun: every repro equal to
      Neo4j; corpus and S12b unchanged. Filed: ClickHouse merges a date
      with its text (#1432). Noted: a list of NULLs only (`[null]`,
      `ARRAY<VOID>`) in a UNION fails on Spark, loudly; a list of mixed
      element types fails on Spark (S12a).
  - [x] **S12e: shortestPath and walks over several labels or types.**
    `FunctionMapper::shortest_path_search` on Spark. Spark gives each step
    of a recursive CTE the rows of the step before, as ClickHouse does, but
    takes one recursive term, no recursive reference in an aggregate,
    window or sort, and no recursive CTE read by another's subquery; the
    spelling says so: `recursive_steps` (the search's expand-and-carry
    steps one parenthesized `UNION ALL`), `stops_early` (no "every end
    reached" cut: the search ends when a depth adds no node, the same
    paths), `grouped_steps` (no step aggregates: `allShortestPaths` and a
    shortest path's relationships or nodes are not lowered on Spark: 6
    corpus queries), `nested_recursion` (the trails of a shortestPath
    condition start at every first node, not only at the pairs that need
    them: the pick keeps the same pairs). Types: `INT` depth, a
    `DECIMAL(38, 0)` path count (overflow fails). A walk over several
    labels or types (`union_path_ctes`, one linear recursion) needed only
    the depth type.
    - Acceptance: Databricks lowers 1436 of ClickHouse's 1442 (was 1334;
      the rest: the 6 above). Corpus differential 1261 equal (was 1166;
      the 102 newly lowered: 95 equal, 2 under ties, 5 without data). Ad
      hoc (14: unbounded, `*2..`, undirected, several starts and ends, all
      pairs, a length condition, carried ends, several types to a label,
      unlabeled walks): every lowered one equal; `allShortestPaths` and
      `nodes(p)` of a shortest path refused on Spark. Found:
      `RETURN p ORDER BY length(p)` is an internal error on both dialects
      (#1427, broadened).
    - Adversarial review: fixed a node row with a NULL id emptying every
      Spark search (it seeded `(NULL, NULL)`, and Spark's `(a, b) NOT IN` of
      a row holding NULL rejects every pair: 0 rows for 98); a search now
      starts only at non-NULL ids, on every dialect. Checked on a synthetic
      graph of cycles, a diamond, self-loops and parallel edges (13 shapes)
      and the polymorphic schema (12): every one equal. Limits: without the
      early stop a Spark search explores all a first node reaches, and
      Spark caps a recursive CTE at 100 levels and 1,000,000 rows
      (`spark.sql.cteRecursionLevelLimit` / `cteRecursionRowLimit`): a
      deep or wide graph fails there, loudly, where ClickHouse answers.

- [ ] **S13: the embedded write path.** Embedded (chdb) mode runs Cypher
  writes, which planned on the legacy pipeline: the statement as a
  `LogicalPlan`, `write_guard`, `write_plan_builder` (each UPDATE / DELETE's
  ids a legacy `RenderPlan`), write + RETURN re-read through
  `translate_read` without text (legacy on every dialect). The legacy
  pipeline cannot be deleted while they do. The output model
  (`WriteRenderPlan`: INSERT / lightweight UPDATE / DELETE, `write_to_sql`,
  the count probes) stays.
  - [x] **S13a: SET / REMOVE / DELETE.** `bind_write_statement`: the read
    part bound as a read's (no RETURN), then each updating clause's
    operations in its scope (`BoundWrite`: `v.prop = value`, REMOVE,
    `[DETACH] DELETE v` of a node; a relationship target, CREATE, a
    RETURN or reading clause after an update, a UNION: `Unsupported`, the
    legacy planner's until S13b/S13c). `bound_plan::write::plan_write`: one
    UPDATE / DELETE per label the node can have, by its identities in the
    read part's records (`lower::write::lower_identities`: the read part
    lowered as a relation, its node's identity columns DISTINCT; a node of
    several labels by its rows' label column, so a property another label
    declares reads NULL, as the read's union does). A SET value is computed
    from the row it updates (`lower_write_value`: its node scanned at the
    UPDATE's table, qualified by the table's name, which ClickHouse's UPDATE
    resolves; another variable or a pattern: refused, the legacy SQL
    `SET age = a.age + 1` failed with UNKNOWN_IDENTIFIER). REMOVE: NULL; a
    property assigned twice: the last. ClickHouse's lightweight DELETE
    resolves its subquery's tables in its target's database, so a CTE there
    is unknown (an UPDATE and a SELECT read it): such a DELETE's ids
    (`RowSource::Read`: the union of an untyped node's labels, a WITH, a
    variable-length pattern) are read first by the embedded executor and
    deleted by literal (`RowSource::Ids`, at most 5,000 per statement; no
    ids, no DELETE). Admission as the legacy guard's: chdb only, a
    source-backed label read-only, a node in its own table, a
    single-column identity; DETACH DELETE deletes the relationships of each
    plain edge table with fixed end labels at the label (an FK edge's go
    with the node; a polymorphic or node-property-embedding table's cannot
    be told apart by the id: refused, where the legacy planner deleted by
    id alone).
    - Acceptance: the embedded crate's write tests (152, the capturing
      executor's SQL) pass, `__cg_write_ids` showing the bound plan's;
      chdb end to end (`writes_e2e`) as on main: SET, REMOVE, DELETE
      executed (2 failures on main and here: an Int64 property reads back
      as a string, #1436). Executed on ClickHouse against copies of the
      test tables: SET with a value of the row's own properties, DETACH
      DELETE of a user (its follows and likes, then the node).
    - Adversarial review (executed on ClickHouse): a write of several
      statements re-ran its MATCH in each after the earlier ones wrote
      (`MATCH (u)-[:FOLLOWS]->(f) … DETACH DELETE u`: its relationships
      deleted, `u` was no longer matched; `SET … DELETE`, `DELETE a, b`
      likewise), counters wrong (every DELETE of a sequence but the last
      counted as a relationship's, a shared relationship twice). Now a
      write of several statements reads every operation's ids before any
      runs (`RowSource::Read`), and the executor runs the checks, then
      each statement after probing its count (`DeleteOp::relationships`
      says which counter). One UPDATE per SET clause, in clause order (a
      later clause reads what an earlier one wrote). A SET value reads its
      row's bare columns (chdb resolves no table-qualified column in an
      UPDATE). Neo4j's rules: a plain DELETE of a node with relationships
      fails (`WriteRenderPlan::Check`, each plain edge table at the label,
      before anything runs; the legacy path left them dangling); a node of
      several labels whose other label lacks a SET property fails only
      when such a node matches. The embedded dispatcher routes a write the
      legacy parser cannot read (two SET clauses). Five chdb end-to-end
      tests of these shapes pass; #1436 re-diagnosed (the schema declares
      no property types: its columns are strings).

## Appendix A — post-assembly repair passes (48, plus CTE flattening, at `e1241a9c`)

**A. WITH finalization (`with_to_cte/mod.rs` unless noted)**
1. `prune_joins_covered_by_last_cte` 4680
2. `clear_stale_joins_for_cte_aliases` `plan_builder_utils.rs:3974`
3. `reattach_unwind_array_joins` 8890
4. `rewrite_cte_join_conditions_and_prune_orphans` 6920
5. `fix_composite_alias_refs_and_augment_scope` 7289
6. `tie_or_reject_cross_joined_pattern_endpoint` 7181
7. `restructure_post_with_optional_match` 6290
8. `apply_passthrough_cte_name_remappings` 3158
9. `reconcile_stale_cte_name_references` 3062
10. `resolve_final_from_against_cte` 3340
11. `resolve_cross_table_with_cte_joins` 4101
12. `extract_cte_join_condition_from_filter` `plan_builder_utils.rs:379`
13. `generate_vlp_with_cte_join_conditions` 3427
14. `restructure_post_with_optional_or_insert_cte_join` 3665
15. `apply_hop_uniqueness_after_with` 3949
16. `add_cte_cross_joins_to_union_branches` 3185
17. `apply_final_outer_scope_passes` 4645
18. `fix_orphan_table_aliases` `variable_scope.rs:1397`
19. `reject_with_cte_joined_twice_under_vlp` 4076
20. `apply_weighted_shortest_path_restructure` 3252

**B. Main render path**
21. `rewrite_vlp_union_branch_aliases` `plan_builder_utils.rs:1740`
22. `rewrite_vlp_aggregate_aliases` `vlp_rewrite.rs:46`
23. `rewrite_render_plan_with_scope` `variable_scope.rs:530`
24. `remap_coupled_rel_vars_in_filter` `plan_builder_helpers.rs:3304`
25. `from_marker_pre_filter` `plan_builder.rs:169`
26. `apply_anylast_wrapping_for_group_by` `plan_builder.rs:57`
27. `rewrite_denorm_optional_vlp_anchor_scan` `plan_builder_helpers.rs:691`
28. Pattern-comprehension join injection `plan_builder.rs:1523,5843`
29. Optional-denorm `rewrite_denorm_refs` `plan_builder.rs:4787`
30. `deduplicate_join_aliases` `join_deduplicator.rs:16`
31. `inject_own_table_joins` `join_builder.rs:172`
32. `apply_optional_node_pre_filters` `join_builder.rs:312`
33. The #583 edge-target swap `join_builder.rs:~2192`

**C. `plan_optimizer::optimize_plan`**
34. `remove_dead_ctes` 123
35. `prune_vlp_columns` 239
36. `prune_cte_columns` 340
37. `drop_from_alias_join_entries` 2102
38. `fold_optional_edge_node_join_with_predicate` 1596
39. `remove_unreferenced_joins` 2324
40. `eliminate_bridge_nodes_in_plan` 2442
41. `remove_redundant_edge_self_joins` 1935
42. `reorder_from_for_selective_predicate` 4485

**D. Emitter `render_plan_to_sql` (`to_sql_query.rs`)**
43. `flatten_all_ctes` 6262 (kept: this is printing, not a repair)
44. `short_circuit_reduce_over_empty_list` 6353
45. `rewrite_vlp_in_cte_bodies` 5984
46. `rewrite_vlp_select_aliases` 1453
47. `drop_disconnected_vlp_joins` 1334
48. `unify_mixed_anchor_branch_selects` 1394
49. `sort_joins_by_dependency` `plan_builder_helpers.rs:5662`

## Appendix B — prototype

The SQL below is the lowering of §4 written by hand. `VLP(p, n)` is the
existing recursive CTE shape (base case plus recursive step with
`NOT has(path_edges, …)`), unchanged. Run with `join_use_nulls = 1`. The
harness (`proto.py` and `neo.py`) is kept with the slice 1 oracle work.

**#1305**: a WITH, then a hop and a path, then OPTIONAL with a WHERE on a
variable from the required part:

```sql
WITH RECURSIVE
  w1 AS (SELECT c.user_id AS c_id FROM users z JOIN follows r ON r.follower_id = z.user_id
         JOIN users c ON c.user_id = r.followed_id),                         -- WITH c : scope {c}
  p1 AS VLP(…, 2),
  m2 AS (SELECT w1.c_id, a.user_id AS a_id, p1.end_id AS b_id               -- MATCH: scope {c, t1, a, b}
         FROM w1 JOIN follows t1 ON t1.follower_id = w1.c_id
         JOIN users a ON a.user_id = t1.followed_id
         JOIN p1 ON p1.start_id = a.user_id JOIN users b ON b.user_id = p1.end_id
         WHERE NOT has(p1.path_edges, tuple(t1.follower_id, t1.followed_id))),  -- clause uniqueness
  drive AS (SELECT DISTINCT b_id, a_id FROM m2),                             -- correlation {b, a}
  opt AS (SELECT drive.b_id AS k_b, drive.a_id AS k_a, d.user_id AS d_id
          FROM drive JOIN follows t3 ON t3.follower_id = drive.b_id
          JOIN users d ON d.user_id = t3.followed_id
          WHERE drive.a_id = 2)                                              -- WHERE stays in its clause
SELECT count(*) FROM m2 LEFT JOIN opt ON opt.k_b = m2.b_id AND opt.k_a = m2.a_id
-- 272 = Neo4j (engine: 125)
```

**#1310**: closed path next to a hop. Two ties on `a` and nothing else:

```sql
SELECT count(*) FROM users z JOIN follows t1 ON t1.follower_id = z.user_id
JOIN users a ON a.user_id = t1.followed_id
JOIN p1 ON p1.start_id = a.user_id AND p1.end_id = a.user_id
WHERE NOT has(p1.path_edges, tuple(t1.follower_id, t1.followed_id))
-- 14 = Neo4j (engine: 77)
```
