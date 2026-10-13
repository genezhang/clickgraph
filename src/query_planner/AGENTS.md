# query_planner — what remains of the legacy planner

The legacy planner (analyzer, optimizer, `PlanCtx`, the clause builders) was
deleted in P-4c S14c3, `LogicalPlan` in S14c6 and the `id()` AST rewrite in
S14c7 (the bound plan reads ids itself, `id_mapper::resolve_node_id`): reads and writes are bound
and lowered by `src/bound_plan/` (`docs/design/EXPLICIT_SCOPE.md`). What
remains:

| Module | What |
|---|---|
| `logical_expr/` | `LogicalExpr`, the binder's expression IR, and its conversion from the parser's AST (`ast_conversion.rs`; the binder takes subquery expressions out first) |
| `types.rs` | `QueryType`, statement classification (`get_query_type`) |
| `join_context.rs` | the VLP CTE naming constants |
| `mod.rs`, `errors.rs` | `evaluate_call_query`: `CALL pagerank(...)`'s arguments (`PageRank`) |
