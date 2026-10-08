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
- **UNION.** Each arm is bound from an empty scope. Output names must match.
- **Subquery expressions.** A child scope whose parent is the current scope.
  The parent's variables used inside become the subquery's **correlation
  set**, an explicit `Vec<VarId>`.
- **Lambdas** (comprehensions, `reduce`, quantifiers). An expression-local
  scope; the binder resolves the shadowing.
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
  directed. Undirected paths are S7.
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
    pair at scale 100, against 0.23 s counted.
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
    `relationships(p)` and a `-[r*]->` list the relationships, `p` both.
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
  its elements' identities, and the value is `any()` of its group
  (`Body::determined`). The identities are node ids, relationship
  identities, and a path relation's `path_nodes` (for its nodes) or
  `path_edges` (for its relationships), so two empty lists are equal. A
  `-[r*]->` list a WITH carries is grouped by its relationships.
- **WITH.** A path variable carries its elements: their identities, every
  property of a fixed element, and the path relation's values. The next
  segment builds the path's value and length from them. A computed list
  (`WITH nodes(p) AS ns`) carries its value and its keys.
- **NULL.** The path of an OPTIONAL MATCH that did not match is NULL, and so
  are its lists and a `-[r*]->` list. The value is a `Dynamic` NULL, since
  an array cannot be NULL. Whether the clause matched is tested on a
  nullable element's identity, as for `length(p)`.
- **OPTIONAL drive.** When an OPTIONAL MATCH's WHERE reads a `-[r*]->` list,
  the clause's matches are joined on the list's identity: its first node and
  its relationships. They used to be joined on its ends and length, which two
  paths can share. The drive holds no values.
- **The legacy path** returns ids for `nodes(p)`, type names for
  `relationships(p)`, and for a variable-length `RETURN p` a Path of its two
  ends with no properties.
- **Not lowered:**
  - a shortestPath's path as a value (S6d: its search keeps distances, not
    paths);
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

The default stays `off` until slice 3 is accepted, then becomes `on`.

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
      paths, DISTINCT, grouping, WITH and OPTIONAL. The new path answers 178–186
      shapes per graph and each equals Neo4j. The legacy path gets 104–117
      of them wrong and errors on 58–63.
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
    - Mutation check: 27 rules broken in turn; 26 fail a unit test. The
      other one, asking a shortest path's search for no values, cannot be
      observed: its value is refused afterwards.
- [ ] S6d a shortestPath's path as a value
- [ ] S7 UNWIND / UNION / alternatives
- [ ] S8 layouts
- [ ] S9 subquery expressions
- [ ] S10 default on
- [ ] S11 legacy deletion

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
