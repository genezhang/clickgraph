# query_planner — what remains of the legacy planner

The legacy planner (analyzer, optimizer, `PlanCtx`, the clause builders) was
deleted in P-4c S14c3: reads and writes are bound and lowered by
`src/bound_plan/` (`docs/design/EXPLICIT_SCOPE.md`). What remains:

| Module | What | Until |
|---|---|---|
| `logical_expr/` | `LogicalExpr`, the binder's expression IR, and its conversion from the parser's AST (`ast_conversion.rs`) | moves to `bound_plan` (S14c6) |
| `logical_plan/` | `LogicalPlan` and its node types: still referenced by shared structs (`ViewTableRef.source`, `LogicalExpr`'s subquery variants, PageRank's CALL) and `VariableLengthSpec` / `ShortestPathMode` (path generator) | S14c6 |
| `ast_transform/` | the `id()` rewrite (`resolve_node_id`, used by the bound plan's id lookup) | `transform_id_functions` goes in S14c7 |
| `types.rs` | `QueryType`, statement classification (`get_query_type`) | stays |
| `join_context.rs` | the VLP CTE naming constants | stays (moves with the generator) |
| `transformed.rs`, `errors.rs` | types `LogicalPlan` still names | S14c6 |

`evaluate_call_query` (PageRank) still builds a `LogicalPlan::PageRank`.
