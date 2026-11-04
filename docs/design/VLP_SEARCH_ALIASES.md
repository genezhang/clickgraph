# Spec: The BFS search's local aliases are namespaced (#1515)

Status: DRAFT v1 — committed with its implementation (one slice; the spec ships
in the PR that closes it, like every wave-1 slice of OPEN_ISSUE_PLAN.md)
Owner: hari2353
Grounding: codebase at `55112883`; issue #1515 (found by the review of #1358);
the BFS search generators `bound_plan/lower/path.rs` §`search_cte` /
§`path_search_cte`; the `Search` construction (`PathCall::search`); the #1358
composite-id work that fixed every search key.

---

## Objective

A directed `shortestPath` / `allShortestPaths` over an edge table **named `f`**
fails at ClickHouse with Code 47: `Identifier 'f.node' cannot be resolved from
table with name rel`. The BFS search's CTE templates alias their frontier with
the fixed short name `f` (`FROM {bfs} AS f JOIN {edge_table} AS rel ON
{rel_from} = f.node`); when a physical table of the query is itself named `f`,
ClickHouse resolves the bare name into the scope and the frontier's alias and
the table name fight.

**Who is the user:** anyone whose schema names edge tables in one or two
letters — a scratch LDBC-schema table `f` was the finder; `f`, `r`, `e`, `n`,
`p` are ordinary table names on ClickHouse.

The issue's fix direction: the search's aliases must not be fixed short names;
they come from the lowering's own allocation so no in-query name shadows them.

**Success looks like:** the issue's repro lowers to SQL whose search CTE has
no alias equal to any bare table name the same body reads; the search steps
run and the oracle corpus answers as before. `edge_table AS rel` etc. keep
their spellings — the fix reaches the aliases that are the *templates' own*
text, not the ones conjuncts are baked against.

## Facts (from the templates as written at `55112883`)

1. `search_cte` (ClickHouse `shortestPath` / `allShortestPaths` that the
   search's own step can group) names: the frontier alias `f`, the edge rows
   alias `rel`, the ends' aliases `start_node` / `end_node`. Every `f.`
   reference — `f.new`, `f.depth`, `f.start_id`, `f.node`, `f.paths`,
   `f.var-carry` in the concat, the GROUP BY `f.start_id` — is text inside
   that one function (and its sibling `path_search_cte`); no conjunct or
   later CTE reads the frontier by `f` (both conditions and the walkers read
   the search's exported columns).
2. `path_search_cte` (the Spark step, and the searches whose paths are carried
   row-wise) names the same frontier `f` plus — in parts of the same body —
   the seed subquery alias `s` and the kept-step subquery alias `reached`.
3. The other aliases are **baked** into `RenderExpr`s and value objects at
   `Search` construction: the relationship conditions render `rel.` (the
   composite-id `IS NOT NULL` guards too), the start / end conditions render
   `start_node.` / `end_node.`, and the path's carried values (`table_rel_object`,
   `walk_tuples`, `table_node_object`, the edge identity) take
   `START` / `END` / `REL` as their alias parameters. Renaming those is a
   construction-side change — deliberately **out of this slice's scope**
   (boundaries).
4. The search CTE itself is namespaced `vlp_{var}_bfs` (var is the lowered
   binding name `v{N}`), and the lowering's other allocator,
   `next_cte_alias()` = `w{len + 1}`, is count-derived — *neither guarantees
   no collision with a schema-chosen table name either*; namespacing the
   templates' own aliases the way the CTE name is namespaced does.

## Design

Inside the two templates, the aliases that are their own text take the search's
namespacing — the same device the CTE name already uses, derived from `var`,
deterministic, and un-inventable as a table name in any schema of ordinary
sense:

- frontier `f` → `vlp_{var}_frontier`
- seed subquery alias `s` → `vlp_{var}_seed`
- kept-step subquery alias `reached` → `vlp_{var}_reached`

Single responsibility (the templates' own aliasing), no call-site or
construction change (`Search` already carries `var`), no new state; the same
rename applies to both dialect spellings of the step (`recursive_steps` keeps
its argument semantics).

Two searches in one query (two VLP walk variables) stay apart — each var's
frontier is its own; the walkers read columns, not these aliases.

Backward compat: queries whose tables are not named `f` lower to SQL of the
same shape with new alias text — every fixed alias whose fate the goldens
pin changes once, and the PR carries the regenerated goldens with that one
justification. Future compat: the baked `START` / `END` / `REL` aliases, when
their construction ever allocates, take the same device; a schema table named
`rel` (et al.) gets its own follow-up issue when proven broken (the #1515
repro is the frontier).

## Testing

- Failing first: a schema over an edge table named `f`, the issue's repro
  (directed `shortestPath`, `RETURN length(p)`), and a carried-paths sibling
  (`RETURN p` / `allShortestPaths`) — each asserts the lowered search CTE
  contains no `AS f` and no bare-the-table-name alias collision
  (`vlp_…_frontier` is pinned by fragment), and that the frontier's reads
  (`{frontier}.node`) and the edge join agree on it. The present SQL fails
  these (it spells ` AS f` and `JOIN f AS rel`).
- The same queries on Databricks (`databricks_sql`) — `path_search_cte` is
  the Spark step, and `s` / `reached` ride its template.
- Unchanged accepts: the oracle corpus (`the_corpus_lowers`) and the ratchet
  after the one justified golden regenerate; the unit battery of the VLP
  family updated where it pins the old alias text.
- Gate: fmt / clippy `-D warnings` / full lib suite; live tier of the repro
  over a scratch table `f` is CI's job (no local ClickHouse), as is e2e.

## Boundaries

- The baked aliases (`rel`, `start_node`, `end_node`) keep their names. A
  table named `rel` joins as `JOIN {edge_table} AS rel` — ClickHouse binds
  rows of the table to the alias, and `rel.` reads it; whether the composite
  guards' `{REL}.` then fails is a probe of its own, filed separately if so.
  This slice refuses to touch `Search` construction (P-4c freeze discipline:
  one concern per slice, the repro's shape fixed, the generalization is
  named, not smuggled).
- The frontier's new text is applied inside the two generators only; any
  dialect-side or generator-side static that mentions `f` would ship a
  wrong alias again — the templates are the single source (asserted by the
  tests above, not by convention).
- A schema table literally named `vlp_…_frontier` would collide as today's
  table named `f` does; inventing such a name is out of ordinary practice,
  and the ratchet / oracle would flag it. The var under every `vlp_{var}_…`
  name is the lowering's own allocation (`VarId::name` = `v{N}`, a counter,
  never the query's text), so the templates' identifiers stay bounded
  [a-zA-Z0-9_] shapes and stay apart for two walks — the existing pins
  (which assert `vlp_v1_frontier` literally) notice any drift.

## End state (#1542)

Every FROM alias the search bakes takes the same `vlp_{var}_*` scheme in the
follow-up (`rel`, `start_node` / `end_node`, and the carried values' START /
END / REL alias parameters), so the collision class closes once — and the
#1085 gate on a VLP endpoint named `t` (rejected today, `TODO(multi-vlp)`)
folds into the same namespacing instead of rejecting the query.

## Success criteria

1. The issue's repro over an edge table `f` lowers to SQL whose search body
   has no alias colliding with a bare table name, on ClickHouse and Spark.
2. Neo4j 5.26's answer agrees with ClickHouse's on the live tier (CI).
3. Corpus goldens regenerate once with the alias text as sole justification;
   the ratchet's axis tokens stay where they were.
4. The gate (fmt / clippy / lib suite / ratchet / corpus) is green.
