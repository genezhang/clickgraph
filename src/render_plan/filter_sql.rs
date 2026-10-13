//! SQL text of the conditions a variable-length path's search reads
//! (`cte_manager`): a `RenderExpr` as SQL with its aliases mapped. Kept from
//! the legacy `cte_extraction` (P-4c S14c3), whose other code went with it.

use super::expression_utils::{flatten_addition_operands, has_string_operand};
use super::render_expr::{Literal, Operator, RenderExpr};
use crate::sql_generator::function_mapper::current_function_mapper;

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
pub fn render_expr_to_sql_string(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> String {
    match expr {
        RenderExpr::Column(col) => col.raw().to_string(),
        RenderExpr::TableAlias(alias) => alias.0.clone(),
        RenderExpr::ColumnAlias(alias) => alias.0.clone(),
        RenderExpr::Literal(lit) => match lit {
            super::render_expr::Literal::String(s) => {
                crate::utils::sql_literal::cypher_string_to_sql_literal(s)
            }
            super::render_expr::Literal::Integer(i) => i.to_string(),
            super::render_expr::Literal::Float(f) => f.to_string(),
            super::render_expr::Literal::Boolean(b) => b.to_string(),
            super::render_expr::Literal::Null => "NULL".to_string(),
        },
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            let table_alias = alias_mapping
                .iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
            // Quote column name if it contains dots or special characters
            let quoted_column =
                crate::clickhouse_query_generator::quote_identifier(prop.column.raw());
            format!("{}.{}", table_alias, quoted_column)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Special handling for IS NULL / IS NOT NULL with wildcard property access (e.g., r.*)
            // Convert r.* to r.from_id for null checks (LEFT JOIN produces NULL for all columns)
            let operands: Vec<String> = if matches!(
                op.operator,
                Operator::IsNull | Operator::IsNotNull
            ) && op.operands.len() == 1
                && matches!(&op.operands[0], RenderExpr::PropertyAccessExp(prop) if prop.column.raw() == "*")
            {
                // Extract the relationship alias and use from_id column instead of wildcard
                if let RenderExpr::PropertyAccessExp(prop) = &op.operands[0] {
                    let table_alias = alias_mapping
                        .iter()
                        .find(|(cypher, _)| *cypher == prop.table_alias.0)
                        .map(|(_, cte)| cte.clone())
                        .unwrap_or_else(|| prop.table_alias.0.clone());

                    // Use from_id as the representative column for null check
                    // (LEFT JOIN makes all columns NULL together, so checking one is sufficient)
                    // Quote column name in case it contains dots
                    vec![format!("{}.`from_id`", table_alias)]
                } else {
                    op.operands
                        .iter()
                        .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                        .collect()
                }
            } else {
                op.operands
                    .iter()
                    .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                    .collect()
            };

            // Parenthesize arithmetic operands to preserve grouping — Path C
            // previously emitted bare `{a} OP {b}`, dropping parens on BOTH sides
            // (`(1+2)*3` → `1 + 2 * 3`, `10-(2+3)` → `10 - 2 + 3`). Uses the same
            // precedence rules as Path A (`needs_left_parens`/`needs_right_parens`).
            let arith_lhs = || {
                if op.operands.len() == 2
                    && crate::render_plan::render_expr::needs_left_parens(
                        op.operator,
                        &op.operands[0],
                    )
                {
                    format!("({})", operands[0])
                } else {
                    operands[0].clone()
                }
            };
            let arith_rhs = || {
                if op.operands.len() == 2
                    && crate::render_plan::render_expr::needs_right_parens(
                        op.operator,
                        &op.operands[1],
                    )
                {
                    format!("({})", operands[1])
                } else {
                    operands[1].clone()
                }
            };

            match op.operator {
                Operator::Equal => format!("{} = {}", operands[0], operands[1]),
                Operator::NotEqual => format!("{} != {}", operands[0], operands[1]),
                Operator::LessThan => format!("{} < {}", operands[0], operands[1]),
                Operator::GreaterThan => format!("{} > {}", operands[0], operands[1]),
                Operator::LessThanEqual => format!("{} <= {}", operands[0], operands[1]),
                Operator::GreaterThanEqual => format!("{} >= {}", operands[0], operands[1]),
                Operator::And => format!("({})", operands.join(" AND ")),
                Operator::Or => format!("({})", operands.join(" OR ")),
                Operator::Not => format!("NOT ({})", operands[0]),
                Operator::Addition => {
                    // Use concat() for string concatenation
                    // Flatten nested + operations for cases like: a + ' - ' + b
                    if has_string_operand(&op.operands) {
                        let flattened: Vec<String> = op
                            .operands
                            .iter()
                            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
                            .collect();
                        format!("concat({})", flattened.join(", "))
                    } else {
                        format!("{} + {}", arith_lhs(), arith_rhs())
                    }
                }
                Operator::Subtraction => format!("{} - {}", arith_lhs(), arith_rhs()),
                Operator::Multiplication => format!("{} * {}", arith_lhs(), arith_rhs()),
                Operator::Division => {
                    // Cypher integer-constant division truncates toward zero
                    // (`7/2 = 3`); CH/Spark `/` is float division. Mirror Path A
                    // (`render_integer_division`): both operands integer constants
                    // → `intDiv(a, b)` (CH) / `div(a, b)` (Spark). Column operands
                    // aren't typeable here so stay `/` (tracked in #847).
                    let both_int_consts = op.operands.len() == 2
                        && op.operands.iter().all(
                            crate::sql_generator::emitters::clickhouse::to_sql_query::is_integer_constant,
                        );
                    if both_int_consts {
                        let int_div =
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .integer_division();
                        format!("{}({}, {})", int_div, operands[0], operands[1])
                    } else {
                        format!("{} / {}", arith_lhs(), arith_rhs())
                    }
                }
                Operator::ModuloDivision => format!("{} % {}", arith_lhs(), arith_rhs()),
                Operator::Exponentiation => format!("POWER({}, {})", operands[0], operands[1]),
                Operator::In => {
                    // Cypher: x IN array_property → CH: has(arr, x), Spark: array_contains(arr, x)
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        let contains =
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .array_contains();
                        format!("{}({}, {})", contains, operands[1], operands[0])
                    } else if let Some(in_sql) =
                        render_in_list_rhs(&op.operands[1], &operands[0], false, alias_mapping)
                    {
                        in_sql
                    } else {
                        format!("{} IN {}", operands[0], operands[1])
                    }
                }
                Operator::NotIn => {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        let contains =
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .array_contains();
                        format!("NOT {}({}, {})", contains, operands[1], operands[0])
                    } else if let Some(in_sql) =
                        render_in_list_rhs(&op.operands[1], &operands[0], true, alias_mapping)
                    {
                        in_sql
                    } else {
                        format!("{} NOT IN {}", operands[0], operands[1])
                    }
                }
                Operator::StartsWith => crate::clickhouse_query_generator::starts_with_predicate(
                    &operands[0],
                    &operands[1],
                ),
                Operator::EndsWith => crate::clickhouse_query_generator::ends_with_predicate(
                    &operands[0],
                    &operands[1],
                ),
                Operator::Contains => {
                    // Dialect-aware: Spark's position(substr, str) reverses CH's arg order.
                    crate::clickhouse_query_generator::contains_predicate(
                        &operands[0],
                        &operands[1],
                    )
                }
                Operator::IsNull => format!("{} IS NULL", operands[0]),
                Operator::IsNotNull => format!("{} IS NOT NULL", operands[0]),
                Operator::Distinct => format!("{} IS DISTINCT FROM {}", operands[0], operands[1]),
                Operator::RegexMatch => crate::clickhouse_query_generator::regex_match_predicate(
                    &operands[0],
                    &operands[1],
                ),
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            // Cypher round() follows Neo4j's two-branch semantics, neither of
            // which matches CH's native HALF_EVEN round. Route the 1-arg/2-arg
            // forms through the dialect helper so a round() reachable via the
            // CTE-build path (Path C) matches the final-SELECT renderer (Path A).
            // The helper keeps Spark's already-faithful round() a plain call.
            if func.name.eq_ignore_ascii_case("round") {
                if let Some(sql) = crate::clickhouse_query_generator::round_half_up_sql(&args) {
                    return sql;
                }
            }
            // #880: toInteger/toFloat on a string-typed arg must dispatch to the
            // dialect's parse-or-null cast (CH toInt64OrNull/toFloat64OrNull;
            // Spark bigint/double already null-on-failure) so an unparseable
            // string yields NULL instead of throwing Code 6. Mirrors the Path-A
            // renderer (to_sql_query.rs); gated on the classifier so numeric/
            // unknown args keep the plain cast (conservative-None). This Path-C
            // renderer backs VLP bound-node / pattern-comprehension WHERE filters.
            let fn_lower = func.name.to_lowercase();
            if (fn_lower == "tointeger" || fn_lower == "tofloat")
                && func.args.len() == 1
                && args.len() == 1
                && crate::sql_generator::emitters::clickhouse::type_inference::infer_render_type(
                    &func.args[0],
                ) == Some(
                    crate::sql_generator::emitters::clickhouse::type_inference::RenderType::String,
                )
            {
                let mapper = crate::sql_generator::function_mapper::current_function_mapper();
                return if fn_lower == "tointeger" {
                    mapper.cast_int64_or_null(&args[0])
                } else {
                    mapper.cast_float64_or_null(&args[0])
                };
            }
            // #1055: toString on a PROVEN Float arg keeps a whole-valued float's
            // trailing `.0` (`toString(3.0)` → "3.0"). Mirrors the Path-A renderer
            // (to_sql_query.rs); this Path-C renderer backs VLP bound-node /
            // pattern-comprehension WHERE filters. Gated on the classifier so an
            // Integer/Number/unknown arg keeps the plain cast (conservative-None).
            if fn_lower == "tostring"
                && func.args.len() == 1
                && args.len() == 1
                && crate::sql_generator::emitters::clickhouse::type_inference::infer_render_type(
                    &func.args[0],
                ) == Some(
                    crate::sql_generator::emitters::clickhouse::type_inference::RenderType::Float,
                )
            {
                let mapper = crate::sql_generator::function_mapper::current_function_mapper();
                return mapper.to_string_float(&args[0]);
            }
            // #866: arrayFilter/arrayMap (list-comprehension lowering) need the
            // dialect name + arg swap on this Path-C renderer too. Spark's
            // filter/transform take (collection, lambda); CH's arrayFilter/
            // arrayMap take (lambda, collection). `dialect_function_name` below
            // deliberately SKIPS registry entries that carry an `arg_transform`
            // (it can't rewrite args), so without this a list comprehension
            // reaching Path C — e.g. `size([x IN l WHERE p])` inside a VLP WHERE
            // filter — would emit the raw CH name + CH arg order on Spark
            // (invalid). The shared helper applies BOTH, matching Path A
            // (to_sql_query.rs), routing through the registry (no inline dialect
            // branch here — Rule #7).
            if let Some(sql) =
                crate::clickhouse_query_generator::list_higher_order_fn_sql(&fn_lower, &args)
            {
                return sql;
            }
            // #962: on ClickHouse, `size`/`reverse` over a PROVEN string arg
            // upgrade to the codepoint-based `lengthUTF8`/`reverseUTF8` (the
            // byte-based `length`/`reverse` mangle multi-byte UTF-8). Array /
            // unknown args keep the plain name (correct for arrays, byte-
            // identical for unknowns). Mirrors the Path A renderer
            // (to_sql_query.rs); this Path-C renderer backs VLP bound-node /
            // pattern-comprehension WHERE filters.
            if fn_lower == "size" || fn_lower == "reverse" {
                let dialect = crate::server::query_context::get_current_dialect();
                if let Some(utf8) =
                    crate::sql_generator::emitters::clickhouse::to_sql_query::clickhouse_utf8_string_fn(
                        &fn_lower,
                        func.args.first(),
                        dialect,
                    )
                {
                    return format!("{}({})", utf8, args.join(", "));
                }
            }
            // Map dialect-divergent names (e.g. tuple -> Spark struct) via the registry.
            let name = crate::clickhouse_query_generator::dialect_function_name(&func.name);
            format!("{}({})", name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        RenderExpr::List(list) => {
            let items: Vec<String> = list
                .iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            // Render as an ARRAY literal (CH `[...]` / Spark `array(...)`), matching
            // the canonical Path A (`RenderExpr::to_sql`). A bare `(a, b, c)` is a
            // TUPLE in ClickHouse — `arrayElement`/`length`/`has` all reject it
            // (Code 43) — and only coincidentally worked for `x IN (...)`. CH
            // accepts `x IN [array]` identically, so the array form is correct in
            // every context this CTE-body renderer reaches.
            crate::sql_generator::function_mapper::current_function_mapper()
                .array_literal(&items.join(", "))
        }
        RenderExpr::Case(case) => {
            let when_clauses: Vec<String> = case
                .when_then
                .iter()
                .map(|(condition, result)| {
                    format!(
                        "WHEN {} THEN {}",
                        render_expr_to_sql_string(condition, alias_mapping),
                        render_expr_to_sql_string(result, alias_mapping)
                    )
                })
                .collect();
            let else_clause = case
                .else_expr
                .as_ref()
                .map(|expr| format!(" ELSE {}", render_expr_to_sql_string(expr, alias_mapping)))
                .unwrap_or_default();
            format!(
                "CASE {} {} END",
                case.expr
                    .as_ref()
                    .map(|e| render_expr_to_sql_string(e, alias_mapping))
                    .unwrap_or_default(),
                when_clauses.join(" ") + &else_clause
            )
        }
        RenderExpr::ReduceExpr(reduce) => {
            // Route reduce() through the dialect helper so the fold spelling
            // follows the active dialect (CH `arrayFold(...)` / Spark `aggregate(...)`)
            // instead of hardcoding ClickHouse here. Cast numeric init to Int64 to
            // prevent type mismatch issues (mirrors Path A's `RenderExpr::to_sql`
            // ReduceExpr arm exactly).
            let init_sql = render_expr_to_sql_string(&reduce.initial_value, alias_mapping);
            let list_sql = render_expr_to_sql_string(&reduce.list, alias_mapping);
            // #969: install the lambda binder types around the BODY render only,
            // so the string-`+`→`concat` detector sees `s + x` as a string
            // concatenation (mirrors Path A). initial_value/list are outer-scope
            // (rendered above). Save/restore for nested reduces.
            let binder_types = crate::clickhouse_query_generator::reduce_binder_type_map(
                &reduce.accumulator,
                &reduce.initial_value,
                &reduce.variable,
                &reduce.list,
            );
            let prev_binders = crate::server::query_context::push_reduce_binder_types(binder_types);
            let expr_sql = render_expr_to_sql_string(&reduce.expression, alias_mapping);
            crate::server::query_context::restore_reduce_binder_types(prev_binders);

            // Wrap a numeric init literal in the matching 64-bit cast to prevent
            // a Code 53 type mismatch (mirrors Path A: #955 Integer, #971 Float).
            let init_cast = match *reduce.initial_value {
                RenderExpr::Literal(Literal::Integer(_)) => {
                    format!("{}({})", current_function_mapper().cast_int64(), init_sql)
                }
                RenderExpr::Literal(Literal::Float(_)) => {
                    format!("{}({})", current_function_mapper().cast_float64(), init_sql)
                }
                _ => init_sql,
            };

            // #955: a fold over an EMPTY list literal does zero iterations, so
            // the result is exactly the initial accumulator. The fold would fail
            // on ClickHouse with Code 53 (the bare `[]` is `Array(Nothing)`, so
            // the lambda return type can't unify with the accumulator). Mirrors
            // the Path A (`RenderExpr::to_sql`) short-circuit exactly.
            if matches!(reduce.list.as_ref(), RenderExpr::List(items) if items.is_empty()) {
                return init_cast;
            }

            crate::clickhouse_query_generator::reduce_fold_sql(
                &reduce.variable,
                &reduce.accumulator,
                &expr_sql,
                &list_sql,
                &init_cast,
            )
        }
        RenderExpr::ArraySubscript { array, index } => {
            let array_sql = render_expr_to_sql_string(array, alias_mapping);
            // Mirror the canonical Path A (`RenderExpr::to_sql`) exactly:
            //   * a string-literal index is a MAP-KEY access (`m['key']`) — left
            //     verbatim, no offset, no null-safe accessor;
            //   * a numeric index is Cypher 0-based, so it needs +1 for CH/Spark
            //     1-based element access, with negative (from-the-end) indices
            //     passed through unchanged (`-1` = last), and the whole thing
            //     routed through `array_element_or_null` so an out-of-bounds index
            //     yields NULL (openCypher) rather than the element type default.
            // The previous raw `{array}[{index}]` was doubly wrong here: no +1
            // offset (off-by-one vs Cypher) and, combined with the List arm's old
            // tuple output, produced `(1,2,3)[0]` — a Code 43 on ClickHouse.
            match index.as_ref() {
                RenderExpr::Literal(Literal::String(_)) => {
                    let index_sql = render_expr_to_sql_string(index, alias_mapping);
                    format!("{}[{}]", array_sql, index_sql)
                }
                _ => {
                    let idx_1based = match index.as_ref() {
                        RenderExpr::Literal(Literal::Integer(n)) if *n >= 0 => {
                            format!("{}", n + 1)
                        }
                        _ => {
                            let i = render_expr_to_sql_string(index, alias_mapping);
                            format!("if(({i}) >= 0, ({i})+1, ({i}))")
                        }
                    };
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .array_element_or_null(&array_sql, &idx_1based)
                }
            }
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            // Use to_sql method which handles ClickHouse arraySlice generation
            // This is a temporary delegation - ideally we'd use alias_mapping here too
            // but to_sql() doesn't support it. For now, array slicing shouldn't have
            // complex alias references that need mapping.
            let array_sql = render_expr_to_sql_string(array, alias_mapping);
            let mapper = crate::sql_generator::function_mapper::current_function_mapper();
            use crate::sql_generator::emitters::clickhouse::to_sql_query::{
                is_nonneg_int_literal, normalize_slice_bound,
            };
            // Cypher list ranges are 0-based and HALF-OPEN: `list[from..to]` yields
            // indices [from, to), i.e. `to - from` elements. offset = from + 1.
            // Negative bounds count from the end and are normalized first (the
            // same rewrite Path A applies); non-negative integer literals stay
            // verbatim so positive slices render identically.
            match (from, to) {
                (Some(from_expr), Some(to_expr)) => {
                    let nf = normalize_slice_bound(
                        &render_expr_to_sql_string(from_expr, alias_mapping),
                        &array_sql,
                        is_nonneg_int_literal(from_expr),
                    );
                    let nt = normalize_slice_bound(
                        &render_expr_to_sql_string(to_expr, alias_mapping),
                        &array_sql,
                        is_nonneg_int_literal(to_expr),
                    );
                    // Floor the length at 0 so from >= to yields an empty slice (a
                    // negative length is silently wrong in one dialect's slice and
                    // an error in another's).
                    mapper.array_slice(
                        &array_sql,
                        &format!("{} + 1", nf),
                        Some(&format!("greatest({} - {}, 0)", nt, nf)),
                    )
                }
                (Some(from_expr), None) => {
                    let nf = normalize_slice_bound(
                        &render_expr_to_sql_string(from_expr, alias_mapping),
                        &array_sql,
                        is_nonneg_int_literal(from_expr),
                    );
                    mapper.array_slice(&array_sql, &format!("{} + 1", nf), None)
                }
                (None, Some(to_expr)) => {
                    let nt = normalize_slice_bound(
                        &render_expr_to_sql_string(to_expr, alias_mapping),
                        &array_sql,
                        is_nonneg_int_literal(to_expr),
                    );
                    mapper.array_slice(&array_sql, "1", Some(&nt))
                }
                (None, None) => array_sql,
            }
        }
        RenderExpr::Star => "*".to_string(),
        RenderExpr::Parameter(param) => format!("${}", param),
        RenderExpr::MapLiteral(entries) => {
            // Map literals handled specially for duration(), point(), etc.
            let pairs: Vec<String> = entries
                .iter()
                .map(|(k, v)| {
                    let val_sql = render_expr_to_sql_string(v, alias_mapping);
                    format!("'{}': {}", k, val_sql)
                })
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

/// Render the RHS of an `x IN <list>` / `x NOT IN <list>` predicate for the CTE
/// body. Returns `None` when the RHS is not a list literal (e.g. an array-valued
/// property or subquery), so the caller falls through to its own handling.
///
/// The `List` arm renders a list literal as an ARRAY (`[...]` / `array(...)`),
/// which is correct for scalar/subscript/length contexts but WRONG inside an
/// `IN`: on ClickHouse a heterogeneous array literal (an id column `toString`-
/// wrapped next to another type) fails `NO_COMMON_TYPE` (Code 386), and on Spark
/// `x IN array(...)` compares a scalar to one array value. So an `IN` list must
/// render as a paren value-list `x IN (a, b)`. That decision lives in
/// `FunctionMapper::in_list_predicate` (the canonical dialect layer, Rule #7);
/// this helper only renders the items from the RenderExpr.
fn render_in_list_rhs(
    rhs: &RenderExpr,
    lhs_sql: &str,
    negate: bool,
    alias_mapping: &[(String, String)],
) -> Option<String> {
    let RenderExpr::List(items) = rhs else {
        return None;
    };
    let item_sqls: Vec<String> = items
        .iter()
        .map(|item| render_expr_to_sql_string(item, alias_mapping))
        .collect();
    // An explicit null element in the list requires three-valued IN/NOT IN
    // semantics (#855): `x IN [1,2,null]` with no non-null match is null, not
    // false. The `IN (…)` value-list collapses that to a non-match, so expand
    // to element-wise OR/AND where a `x = null` / `x <> null` term propagates
    // the unknown. A null-free list keeps the byte-stable value-list form.
    let has_null = items
        .iter()
        .any(|i| matches!(i, RenderExpr::Literal(super::render_expr::Literal::Null)));
    if has_null && !item_sqls.is_empty() {
        let (cmp, joiner) = if negate {
            ("<>", " AND ")
        } else {
            ("=", " OR ")
        };
        let clauses: Vec<String> = item_sqls
            .iter()
            .map(|rhs| format!("{} {} {}", lhs_sql, cmp, rhs))
            .collect();
        return Some(format!("({})", clauses.join(joiner)));
    }
    Some(
        crate::sql_generator::function_mapper::current_function_mapper()
            .in_list_predicate(lhs_sql, &item_sqls, negate),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_in_list_rhs_with_null_expands_for_three_valued_logic_855() {
        use crate::render_plan::render_expr::Literal as RLit;
        // `x IN [1, 2, null]` on the CTE-body path must expand element-wise so
        // three-valued IN holds (#855): a plain `x IN (…)` value-list would
        // collapse the null to a non-match. `x` here is a pre-rendered LHS.
        let list = RenderExpr::List(vec![
            RenderExpr::Literal(RLit::Integer(1)),
            RenderExpr::Literal(RLit::Integer(2)),
            RenderExpr::Literal(RLit::Null),
        ]);
        assert_eq!(
            render_in_list_rhs(&list, "x", false, &[]).unwrap(),
            "(x = 1 OR x = 2 OR x = NULL)"
        );
        assert_eq!(
            render_in_list_rhs(&list, "x", true, &[]).unwrap(),
            "(x <> 1 AND x <> 2 AND x <> NULL)"
        );
        // A null-free list keeps the byte-stable value-list form (0 churn).
        let nn = RenderExpr::List(vec![
            RenderExpr::Literal(RLit::Integer(1)),
            RenderExpr::Literal(RLit::Integer(2)),
        ]);
        assert_eq!(
            render_in_list_rhs(&nn, "x", false, &[]).unwrap(),
            "x IN (1, 2)"
        );
    }
}
