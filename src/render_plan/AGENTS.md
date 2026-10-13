# render_plan — the SQL IR

Since P-4c S14c3 this directory holds only the intermediate representation the
bound plan lowers to and its helpers; the legacy LogicalPlan→RenderPlan
composition (plan builders, WITH→CTE, the 48 repair passes) was deleted.

| File | What |
|---|---|
| `mod.rs` | `RenderPlan`, `Cte`, `CteContent`, `Join`, `SelectItem(s)`, `FilterItems`, `OrderByItem(s)`, `Union`, `ArrayJoin`… — built by `bound_plan/lower` |
| `render_expr.rs` | `RenderExpr` and its map/visit helpers |
| `view_table_ref.rs` | a table reference (`parameterized_name` for view parameters, FINAL) |
| `write_render.rs` | `WriteRenderPlan` (INSERT / UPDATE / DELETE), built by `bound_plan/write.rs` |
| `cte_manager/`, `cte_generation.rs`, `filter_pipeline.rs`, `filter_sql.rs` | the context the variable-length path generator (`sql_generator/emitters/clickhouse/variable_length_cte.rs`) reads when `bound_plan/lower/path.rs` builds a walk |
| `expression_utils.rs`, `from_table.rs`, `errors.rs` | small helpers |

The SQL text is printed by `sql_generator/emitters/clickhouse/to_sql_query.rs`
(`render_plan_to_sql_plain`). Fields kept only for the deleted pipeline
(`variable_registry`, the VLP metadata on `RenderPlan` / `Cte` / `Join`) go in
S14c5 (`docs/design/EXPLICIT_SCOPE.md` S14).
