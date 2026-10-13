# query_planner — what remains of the legacy planner

The legacy planner (analyzer, optimizer, `PlanCtx`, the clause builders) was
deleted in P-4c S14c3 and `LogicalPlan` in S14c6: reads and writes are bound
and lowered by `src/bound_plan/` (`docs/design/EXPLICIT_SCOPE.md`). What
remains:

| Module | What |
|---|---|
| `logical_expr/` | `LogicalExpr`, the binder's expression IR, and its conversion from the parser's AST (`ast_conversion.rs`; the binder takes subquery expressions out first) |
| `ast_transform/` | the `id()` rewrite (`resolve_node_id`, used by the bound plan's id lookup); `transform_id_functions` goes in S14c7 |
| `types.rs` | `QueryType`, statement classification (`get_query_type`) |
| `join_context.rs` | the VLP CTE naming constants |
| `mod.rs`, `errors.rs` | `evaluate_call_query`: `CALL pagerank(...)`'s arguments (`PageRank`) |
