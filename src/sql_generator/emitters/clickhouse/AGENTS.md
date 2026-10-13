# emitters/clickhouse — SQL printing

Prints the bound plan's `RenderPlan` (`bound_plan/lower`) as SQL; the dialect
(ClickHouse, Databricks) comes from the task-local `QueryContext` through
`FunctionMapper` (`sql_generator/function_mapper`). The legacy renderer
(`render_plan_to_sql` and its repair passes, `multi_type_vlp_joins`,
`json_builder`) was deleted in P-4c S14c3.

| File | What |
|---|---|
| `to_sql_query.rs` | The plain printer: `render_plan_to_sql_plain` / `render_plan_parts_plain` (a lowered plan's CTEs in order, then its SELECT), `render_expr_to_sql_plain`, `order_keys_to_sql_plain`; the `ToSql` impls of the clauses and of `RenderExpr`; `flatten_all_ctes` (one `WITH RECURSIVE`, every CTE top-level, same-name CTEs renamed) |
| `variable_length_cte.rs` | The recursive CTE of a variable-length path (`VariableLengthCteGenerator`), called through `render_plan/cte_manager` by `bound_plan/lower/path.rs` |
| `write_to_sql.rs` | `WriteRenderPlan` (embedded writes, `bound_plan/write.rs`) → INSERT / lightweight UPDATE / DELETE |
| `function_registry.rs`, `function_translator.rs` | Cypher function → SQL function mapping (per dialect) |
| `type_inference.rs` | The render-site type classifier (string `+`, temporal components, `toInteger` of strings) |
| `common.rs` | Identifier quoting, predicate spellings (STARTS WITH, regex), reduce / list helpers |
| `pagerank.rs` | The PageRank `CALL`'s SQL |
| `id_gen.rs`, `errors.rs` | Id generation for CREATE; errors |
| `mod.rs` | The library entry points `cypher_to_sql`, `cypher_to_sql_with_id_columns`, `cypher_to_sql_with_metadata` (the read seam, `translate.rs`) |

Rules: CTEs are flat (`flatten_all_ctes`); dialect differences go through
`FunctionMapper`, never inline branching (the ratchet test,
`tests/rust/ratchet/`).
