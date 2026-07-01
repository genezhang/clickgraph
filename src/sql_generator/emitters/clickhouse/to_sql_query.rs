use crate::{
    query_planner::join_context::{VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN},
    query_planner::logical_plan::LogicalPlan,
    render_plan::{
        render_expr::{
            self, AggregateFnCall, Column, ColumnAlias, InSubquery, Literal, Operator,
            OperatorApplication, PropertyAccess, RenderCase, RenderExpr, ScalarFnCall, TableAlias,
        },
        ViewTableRef,
        {
            ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem,
            GroupByExpressions, Join, JoinItems, JoinType, OrderByItems, OrderByOrder, RenderPlan,
            SelectItem, SelectItems, ToSql, UnionItems, UnionType,
        },
    },
    server::query_context::{
        clear_all_render_contexts, get_cte_property_mapping, get_relationship_columns,
        is_multi_type_vlp_alias, restore_branch_context, set_alias_label_map,
        set_all_render_contexts, set_multi_type_vlp_aliases, snapshot_branch_context,
    },
    utils::cte_naming::is_generated_cte_name,
};
use std::collections::HashMap;
use std::collections::HashSet;

// Import function translator for Neo4j -> ClickHouse function mappings
use super::function_registry::get_function_mapping;

// ============================================================================
// RENDER CONTEXT ACCESSORS (delegating to unified query_context)
// ============================================================================

/// Get relationship columns for IS NULL checks
fn get_relationship_columns_from_context(alias: &str) -> Option<(String, String)> {
    get_relationship_columns(alias)
}

/// Get CTE property mapping
fn get_cte_property_from_context(cte_alias: &str, property: &str) -> Option<String> {
    get_cte_property_mapping(cte_alias, property)
}

/// Check if alias is a multi-type VLP endpoint
fn is_multi_type_vlp_alias_from_context(alias: &str) -> bool {
    is_multi_type_vlp_alias(alias)
}

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(contains_string_literal)
        }
        _ => false,
    }
}

/// Check if any operand in the expression contains a string
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(contains_string_literal)
}

/// Ternary result for Cypher literal equality evaluation.
#[derive(Debug, PartialEq)]
enum CypherTriBool {
    True,
    False,
    Null, // unknown
}

impl CypherTriBool {
    fn negate(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
        }
    }
    fn sql_str(&self) -> &'static str {
        match self {
            Self::True => "true",
            Self::False => "false",
            Self::Null => "null",
        }
    }
}

/// If expr is `toString(inner)`, return `inner`; otherwise return `expr` unchanged.
/// Used to look through the toString() wrapping that mixed-type ClickHouse arrays add.
fn unwrap_tostring(expr: &RenderExpr) -> &RenderExpr {
    if let RenderExpr::ScalarFnCall(fn_call) = expr {
        if fn_call.name.eq_ignore_ascii_case("toString") && fn_call.args.len() == 1 {
            return &fn_call.args[0];
        }
    }
    expr
}

/// Statically evaluate Cypher equality between two literal RenderExpr values.
///
/// Implements Cypher's three-valued logic:
/// - Different types: false (no coercion)
/// - Either side is null: null (unknown)
/// - List/map with null element: null if no definite mismatch, false if definite mismatch
/// - Returns None when the result cannot be determined statically.
fn cypher_literal_eq(lhs: &RenderExpr, rhs: &RenderExpr) -> Option<CypherTriBool> {
    match (lhs, rhs) {
        // null = anything → NULL (unknown)
        (RenderExpr::Literal(Literal::Null), _) | (_, RenderExpr::Literal(Literal::Null)) => {
            Some(CypherTriBool::Null)
        }

        // Cross-type literal comparisons → false (no type coercion in Cypher)
        (
            RenderExpr::Literal(Literal::String(_)),
            RenderExpr::Literal(Literal::Integer(_) | Literal::Float(_) | Literal::Boolean(_)),
        )
        | (
            RenderExpr::Literal(Literal::Integer(_) | Literal::Float(_) | Literal::Boolean(_)),
            RenderExpr::Literal(Literal::String(_)),
        )
        | (
            RenderExpr::Literal(Literal::Boolean(_)),
            RenderExpr::Literal(Literal::Integer(_) | Literal::Float(_)),
        )
        | (
            RenderExpr::Literal(Literal::Integer(_) | Literal::Float(_)),
            RenderExpr::Literal(Literal::Boolean(_)),
        ) => Some(CypherTriBool::False),

        // Same scalar types: evaluate directly (needed for null propagation in collections)
        (RenderExpr::Literal(Literal::Integer(a)), RenderExpr::Literal(Literal::Integer(b))) => {
            Some(if a == b {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }
        (RenderExpr::Literal(Literal::Float(a)), RenderExpr::Literal(Literal::Float(b))) => {
            // NaN != NaN (IEEE 754), consistent with Cypher scenario [8]
            Some(if a == b {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }
        // Integer ↔ Float: Cypher treats 1 = 1.0 as true
        (RenderExpr::Literal(Literal::Integer(a)), RenderExpr::Literal(Literal::Float(b))) => {
            Some(if (*a as f64) == *b {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }
        (RenderExpr::Literal(Literal::Float(a)), RenderExpr::Literal(Literal::Integer(b))) => {
            Some(if *a == (*b as f64) {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }
        (RenderExpr::Literal(Literal::String(a)), RenderExpr::Literal(Literal::String(b))) => {
            Some(if a == b {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }
        (RenderExpr::Literal(Literal::Boolean(a)), RenderExpr::Literal(Literal::Boolean(b))) => {
            Some(if a == b {
                CypherTriBool::True
            } else {
                CypherTriBool::False
            })
        }

        // List comparison
        (RenderExpr::List(lhs_items), RenderExpr::List(rhs_items)) => {
            if lhs_items.len() != rhs_items.len() {
                return Some(CypherTriBool::False);
            }
            let mut has_null = false;
            for (l, r) in lhs_items.iter().zip(rhs_items.iter()) {
                // Unwrap toString() wrapping added for mixed-type ClickHouse arrays.
                // e.g. [[1],[2]] becomes [toString([1]), toString([2])] at render time.
                let l_inner = unwrap_tostring(l);
                let r_inner = unwrap_tostring(r);
                match cypher_literal_eq(l_inner, r_inner) {
                    Some(CypherTriBool::False) => return Some(CypherTriBool::False),
                    Some(CypherTriBool::Null) => has_null = true,
                    Some(CypherTriBool::True) => {}
                    None => return None, // Can't determine statically
                }
            }
            Some(if has_null {
                CypherTriBool::Null
            } else {
                CypherTriBool::True
            })
        }

        // Map comparison
        (RenderExpr::MapLiteral(lhs_entries), RenderExpr::MapLiteral(rhs_entries)) => {
            if lhs_entries.len() != rhs_entries.len() {
                return Some(CypherTriBool::False);
            }
            let mut has_null = false;
            for (lkey, lval) in lhs_entries {
                match rhs_entries.iter().find(|(k, _)| k == lkey) {
                    None => return Some(CypherTriBool::False), // key not in rhs
                    Some((_, rv)) => match cypher_literal_eq(lval, rv) {
                        Some(CypherTriBool::False) => return Some(CypherTriBool::False),
                        Some(CypherTriBool::Null) => has_null = true,
                        Some(CypherTriBool::True) => {}
                        None => return None,
                    },
                }
            }
            Some(if has_null {
                CypherTriBool::Null
            } else {
                CypherTriBool::True
            })
        }

        _ => None, // Non-literal or mixed: can't determine statically
    }
}

/// Flatten nested + operations into a list of operands for concat()
fn flatten_addition_operands(expr: &RenderExpr) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .flat_map(flatten_addition_operands)
            .collect(),
        _ => vec![expr.to_sql()],
    }
}

/// Check if a RenderExpr is a list/array expression (for arrayConcat detection).
/// Returns true for: groupArray(), arrayConcat(), arraySort(), List literals,
/// and recursive Addition of list expressions (list + list).
fn is_list_expr(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::AggregateFnCall(agg) => {
            agg.name.eq_ignore_ascii_case("groupArray")
                || agg.name.eq_ignore_ascii_case("collect")
                || agg.name.eq_ignore_ascii_case("arrayConcat")
        }
        RenderExpr::ScalarFnCall(f) => {
            f.name.eq_ignore_ascii_case("arrayConcat")
                || f.name.eq_ignore_ascii_case("arraySort")
                || f.name.eq_ignore_ascii_case("arrayDistinct")
                || f.name.eq_ignore_ascii_case("arrayFilter")
                || f.name.eq_ignore_ascii_case("arrayMap")
        }
        RenderExpr::List(_) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(is_list_expr)
        }
        _ => false,
    }
}

/// Flatten nested + operations for arrayConcat (list concatenation).
/// Known scalar literals are wrapped as `[scalar]` so that `list + scalar`
/// produces valid `arrayConcat(list, [scalar])` (ClickHouse requires array args).
/// Ambiguous expressions (PropertyAccessExp, ColumnAlias, etc.) are left as-is
/// since they may already hold arrays (e.g., CTE columns from groupArray).
fn flatten_list_addition_operands(expr: &RenderExpr) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .flat_map(flatten_list_addition_operands)
            .collect(),
        _ => {
            let sql = expr.to_sql();
            if is_known_scalar(expr) {
                // Wrap scalar as single-element array for arrayConcat/concat compatibility
                vec![
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .array_literal(&sql),
                ]
            } else {
                vec![sql]
            }
        }
    }
}

/// Returns true if the expression is definitely a scalar (not an array).
/// Conservative: returns false for ambiguous types (PropertyAccessExp, ColumnAlias, etc.)
/// since those may hold array values from CTE columns.
fn is_known_scalar(expr: &RenderExpr) -> bool {
    matches!(
        expr,
        RenderExpr::Literal(Literal::Integer(_))
            | RenderExpr::Literal(Literal::Float(_))
            | RenderExpr::Literal(Literal::String(_))
            | RenderExpr::Literal(Literal::Boolean(_))
            | RenderExpr::Literal(Literal::Null)
            | RenderExpr::Parameter(_)
    )
}

/// Rewrite `x IN cte.p{N}_col` / `x NOT IN cte.p{N}_col` to a subquery form:
/// `x IN (SELECT col FROM cte_name)`. Returns `Some(sql)` if rewritten.
///
/// After CollectUnwindElimination, `x IN collected_list` becomes
/// `x IN cte.p{N}_{alias}_{property}`. CTE entity columns are scalar, not arrays,
/// so we must expand to a subquery. Only matches CTE-format columns (is_cte_column)
/// to avoid converting legitimate array column references.
fn try_rewrite_in_cte_subquery(
    operator: &Operator,
    lhs_sql: &str,
    rhs_expr: &RenderExpr,
) -> Option<String> {
    if !matches!(operator, Operator::In | Operator::NotIn) {
        return None;
    }
    if let RenderExpr::PropertyAccessExp(ref prop) = rhs_expr {
        let col_name = prop.column.to_sql_column_only();
        if crate::utils::cte_column_naming::is_cte_column(&col_name) {
            let table_alias = &prop.table_alias.0;
            if let Some(cte_name) =
                crate::server::query_context::get_cte_name_for_alias(table_alias)
            {
                let op_word = if *operator == Operator::In {
                    "IN"
                } else {
                    "NOT IN"
                };
                return Some(format!(
                    "{} {} (SELECT {} FROM {})",
                    lhs_sql, op_word, col_name, cte_name
                ));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Shared OperatorApplication sub-renderers.
//
// These three Addition/Subtraction special cases were copy-pasted across the
// three OperatorApplication render paths (`RenderExpr::to_sql`,
// `to_sql_without_table_alias`, and `impl ToSql for OperatorApplication`),
// which is why dialect leaf-fixes previously needed editing in three places.
// Extracted verbatim so each path calls one source of truth; output is
// byte-identical. The interval case is now dialect-aware (see
// `render_interval_arithmetic` + the `FunctionMapper` epoch/timestamp methods).
// ---------------------------------------------------------------------------

/// `list1 + list2` -> dialect array concat. Operands flatten via `to_sql()`
/// (always qualified), so this is independent of the calling path's aliasing.
fn render_list_addition(op: &OperatorApplication) -> Option<String> {
    if op.operator == Operator::Addition && op.operands.iter().any(is_list_expr) {
        let flattened: Vec<String> = op
            .operands
            .iter()
            .flat_map(flatten_list_addition_operands)
            .collect();
        return Some(format!(
            "{}({})",
            crate::sql_generator::function_mapper::current_function_mapper().array_concat(),
            flattened.join(", ")
        ));
    }
    None
}

/// String `+` -> `concat(...)` (ClickHouse has no `+` for strings).
fn render_string_addition(op: &OperatorApplication) -> Option<String> {
    if op.operator == Operator::Addition && has_string_operand(&op.operands) {
        let flattened: Vec<String> = op
            .operands
            .iter()
            .flat_map(flatten_addition_operands)
            .collect();
        return Some(format!("concat({})", flattened.join(", ")));
    }
    None
}

/// Interval arithmetic on epoch-millis: wrap non-interval operands as a
/// timestamp, do the `+`/`-`, and convert the result back to epoch-millis.
/// Dialect-aware via the function mapper — ClickHouse:
/// `toUnixTimestamp64Milli(fromUnixTimestamp64Milli(x) + toIntervalDay(n))`;
/// Databricks: `unix_millis(timestamp_millis(x) + make_dt_interval(n,0,0,0))`.
/// `rendered` is the path's pre-rendered operands; one of them is the interval
/// (produced by the `duration()` translation).
fn render_interval_arithmetic(op: &OperatorApplication, rendered: &[String]) -> Option<String> {
    use crate::server::query_context::get_current_dialect;
    use crate::sql_generator::SqlDialect;

    let dialect = get_current_dialect();
    // An operand is the interval term when it is a dialect interval constructor.
    // Databricks markers are anchored on the call `(` so a column whose name
    // merely contains the token isn't mistaken for an interval. (The CH `(`-less
    // `toInterval` check is kept verbatim to preserve byte-identical CH output.)
    let is_interval = |r: &str| match dialect {
        SqlDialect::Databricks => {
            r.contains("make_dt_interval(") || r.contains("make_ym_interval(")
        }
        _ => r.contains("toInterval"),
    };

    if (op.operator == Operator::Addition || op.operator == Operator::Subtraction)
        && rendered.len() == 2
        && rendered.iter().any(|r| is_interval(r))
    {
        // An operand that is already a timestamp expression must not be re-wrapped.
        // Databricks function markers are anchored on the call `(`; `current_timestamp`
        // is intentionally bare (Spark allows it as a keyword without parens). The CH
        // arm is kept verbatim to preserve byte-identical CH output.
        let already_timestamp = |r: &str| match dialect {
            SqlDialect::Databricks => {
                r.contains("timestamp_millis(")
                    || r.contains("to_timestamp(")
                    || r.contains("from_unixtime(")
                    || r.contains("current_timestamp")
            }
            _ => {
                r.contains("fromUnixTimestamp64Milli")
                    || r.contains("parseDateTime64BestEffort")
                    || r.contains("toDateTime")
                    || r.contains("now64")
                    || r.contains("now()")
            }
        };
        let mapper = crate::sql_generator::function_mapper::current_function_mapper();
        let wrapped: Vec<String> = rendered
            .iter()
            .map(|r| {
                if is_interval(r) || already_timestamp(r) {
                    r.clone()
                } else {
                    mapper.epoch_millis_to_timestamp(r)
                }
            })
            .collect();
        let sql_op = if op.operator == Operator::Addition {
            "+"
        } else {
            "-"
        };
        return Some(
            mapper
                .timestamp_to_epoch_millis(&format!("{} {} {}", &wrapped[0], sql_op, &wrapped[1])),
        );
    }
    None
}

/// `x IN [const, const, ...]` on Databricks/Spark, where the array-literal form
/// (`x IN array(...)`) is invalid — Spark needs a paren value-list `x IN (a, b)`.
/// Returns `None` on ClickHouse (its `x IN [array]` form is kept byte-stable) and
/// for non-constant lists (those are expanded to OR/AND by the caller first).
/// `rendered[0]` is the path-rendered LHS; list items are constants so they
/// render identically regardless of aliasing.
fn render_constant_in_list(op: &OperatorApplication, rendered: &[String]) -> Option<String> {
    use crate::server::query_context::get_current_dialect;
    use crate::sql_generator::SqlDialect;
    if !matches!(get_current_dialect(), SqlDialect::Databricks)
        || !matches!(op.operator, Operator::In | Operator::NotIn)
        || rendered.len() != 2
    {
        return None;
    }
    if let RenderExpr::List(items) = &op.operands[1] {
        // Empty list: Spark `IN ()` is a syntax error. `IN []` is always false,
        // `NOT IN []` always true — emit the constant predicate directly.
        if items.is_empty() {
            return Some(
                if op.operator == Operator::In {
                    "FALSE"
                } else {
                    "TRUE"
                }
                .to_string(),
            );
        }
        if items
            .iter()
            .all(|i| matches!(i, RenderExpr::Literal(_) | RenderExpr::Parameter(_)))
        {
            let rhs: Vec<String> = items.iter().map(|i| i.to_sql()).collect();
            let kw = if op.operator == Operator::In {
                "IN"
            } else {
                "NOT IN"
            };
            return Some(format!("{} {} ({})", rendered[0], kw, rhs.join(", ")));
        }
    }
    None
}

/// Spark/Databricks (unlike ClickHouse) does not allow a WHERE clause to
/// reference a SELECT-list alias defined in the same query: the bare name is
/// resolved against the FROM tables only, so it is either unresolved or — when
/// more than one joined table carries that column — an AMBIGUOUS_REFERENCE
/// error. LDBC interactive Q10 hits this: `WITH person, city, friend,
/// datetime({epochMillis: friend.birthday}) AS birthday WHERE birthday.month=...`
/// renders to a CTE projecting `friend.birthday AS birthday` whose WHERE reads
/// `month(timestamp_millis(birthday))`, and both `friend` and `person` (Person
/// self-join) expose a `birthday` column.
///
/// Inline each WHERE reference to a same-scope SELECT alias with that alias's
/// source expression, restoring ClickHouse/Neo4j semantics (the post-WITH WHERE
/// filters the projected value). Databricks-only — ClickHouse keeps
/// alias-in-WHERE and stays byte-identical (golden snapshots).
fn inline_where_alias_refs_for_spark(plan: &mut RenderPlan) {
    use crate::server::query_context::get_current_dialect;
    use crate::sql_generator::SqlDialect;
    if !matches!(get_current_dialect(), SqlDialect::Databricks) {
        return;
    }
    inline_where_alias_refs_recursive(plan);
}

/// Recursively inline WHERE alias references using THIS scope's primary SELECT
/// projection (`plan.select`).
///
/// The primary map is applied to the primary filter and, for a UNION, to each
/// branch filter — because at emit a branch's own SELECT may be a nested
/// whole-node expansion whose bare columns spuriously collide (an undirected
/// internal union's reverse arm carries a bare `person.birthday` that shadows
/// the WITH variable, while the projection that is actually emitted binds
/// `friend.birthday`). Matching the ClickHouse binding requires the primary
/// (shared) projection, not the branch's raw one.
///
/// To stay sound for *genuine* user `UNION`s — whose arms legitimately bind the
/// same alias name to different, per-branch sources — a branch only receives the
/// primary entries whose source expression references table aliases that are all
/// present in that branch's own FROM/JOINs. So `birthday => friend.birthday` is
/// inlined into the reverse arm (which joins `friend`), but `score => a.age`
/// would be skipped for a UNION arm that selects from `m` only, never producing
/// `WHERE a.age` against a table the branch lacks.
fn inline_where_alias_refs_recursive(plan: &mut RenderPlan) {
    // Build alias -> source-expression map from THIS scope's primary SELECT.
    // Skip aggregate-bearing sources (an aggregate is illegal in WHERE; such a
    // predicate belongs in HAVING, which we never touch).
    let mut alias_map: HashMap<String, RenderExpr> = HashMap::new();
    for item in &plan.select.items {
        if let Some(ca) = &item.col_alias {
            if source_contains_aggregate(&item.expression) {
                continue;
            }
            alias_map.insert(ca.0.clone(), item.expression.clone());
        }
    }
    if !alias_map.is_empty() {
        if let Some(filter) = plan.filters.0.as_mut() {
            substitute_alias_refs_in_expr(filter, &alias_map);
        }
        if let Some(union) = plan.union.0.as_mut() {
            for branch in union.input.iter_mut() {
                let branch_tables = collect_scope_table_aliases(branch);
                let branch_map: HashMap<String, RenderExpr> = alias_map
                    .iter()
                    .filter(|(_, src)| match source_table_aliases(src) {
                        // Only inline a source whose required tables are all
                        // present in this branch. `None` means the source has a
                        // node whose table refs can't be determined — fail
                        // closed (do not inline) rather than risk emitting a
                        // column against a table the branch lacks.
                        Some(tables) => tables.is_subset(&branch_tables),
                        None => false,
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if !branch_map.is_empty() {
                    if let Some(filter) = branch.filters.0.as_mut() {
                        substitute_alias_refs_in_expr(filter, &branch_map);
                    }
                }
            }
        }
    }
    for cte in plan.ctes.0.iter_mut() {
        if let CteContent::Structured(cte_plan) = &mut cte.content {
            inline_where_alias_refs_recursive(cte_plan);
        }
    }
}

/// Table aliases available in a scope: its FROM table plus every JOINed table.
fn collect_scope_table_aliases(plan: &RenderPlan) -> HashSet<String> {
    let mut aliases = HashSet::new();
    if let Some(from) = plan.from.0.as_ref() {
        if let Some(alias) = from.alias.as_ref() {
            aliases.insert(alias.clone());
        }
    }
    for join in &plan.joins.0 {
        aliases.insert(join.table_alias.clone());
    }
    aliases
}

/// The set of table aliases a SELECT-source expression references, or `None`
/// when the expression contains a node whose table references cannot be fully
/// determined (so it is unsafe to inline into a foreign UNION branch).
///
/// Because `substitute_alias_refs_in_expr` injects the source WHOLESALE into a
/// branch filter, the guard must know EVERY table the source needs; an
/// incomplete count could wrongly pass the subset check. So this is fail-closed:
/// it fully walks every compound variant that can nest column references
/// (functions, operators, `Case`, `List`, `MapLiteral`, array subscript/slice),
/// records each `PropertyAccessExp`/qualified `Column` table, treats
/// `Literal`/`Parameter`/`Star` as table-free, and returns `None` for anything
/// whose tables cannot be proven — bare unqualified columns, alias refs, `Raw`
/// SQL, sub-queries, `ReduceExpr`, `PatternCount`, `CteEntityRef`. `None` → the
/// caller skips inlining. Over-skipping only leaves a rare Spark alias-in-WHERE
/// unfixed; it never emits wrong SQL.
fn source_table_aliases(expr: &RenderExpr) -> Option<HashSet<String>> {
    let mut out = HashSet::new();
    if collect_source_table_aliases(expr, &mut out) {
        Some(out)
    } else {
        None
    }
}

/// Returns false if an opaque/undeterminable node was encountered.
fn collect_source_table_aliases(expr: &RenderExpr, out: &mut HashSet<String>) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            out.insert(pa.table_alias.0.clone());
            true
        }
        // A `Column` carries its raw text; a qualified `alias.col` determines its
        // table (the qualifier), an unqualified bare column does not → fail closed.
        RenderExpr::Column(col) => match col.raw().split_once('.') {
            Some((table, _)) => {
                out.insert(table.to_string());
                true
            }
            None => false,
        },
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) | RenderExpr::Star => true,
        RenderExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .all(|o| collect_source_table_aliases(o, out)),
        RenderExpr::ScalarFnCall(f) => f.args.iter().all(|a| collect_source_table_aliases(a, out)),
        RenderExpr::AggregateFnCall(a) => {
            a.args.iter().all(|x| collect_source_table_aliases(x, out))
        }
        RenderExpr::Case(c) => {
            c.expr
                .as_deref()
                .is_none_or(|e| collect_source_table_aliases(e, out))
                && c.when_then.iter().all(|(w, t)| {
                    collect_source_table_aliases(w, out) && collect_source_table_aliases(t, out)
                })
                && c.else_expr
                    .as_deref()
                    .is_none_or(|e| collect_source_table_aliases(e, out))
        }
        RenderExpr::List(items) => items.iter().all(|i| collect_source_table_aliases(i, out)),
        RenderExpr::ArraySubscript { array, index } => {
            collect_source_table_aliases(array, out) && collect_source_table_aliases(index, out)
        }
        RenderExpr::MapLiteral(entries) => entries
            .iter()
            .all(|(_, v)| collect_source_table_aliases(v, out)),
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_source_table_aliases(array, out)
                && from
                    .as_deref()
                    .is_none_or(|e| collect_source_table_aliases(e, out))
                && to
                    .as_deref()
                    .is_none_or(|e| collect_source_table_aliases(e, out))
        }
        // Bare column/alias refs and every variant we cannot fully introspect:
        // table set is undeterminable → fail closed.
        _ => false,
    }
}

/// Aggregate detection for SELECT-source expressions, covering every nesting
/// variant the inline pass can carry (including `MapLiteral`/`ArraySlicing` that
/// the shared `render_expr_contains_aggregate` does not). Used to keep an
/// aggregate-bearing source out of the alias map entirely — an aggregate is
/// illegal in WHERE, so it must never be inlined there.
fn source_contains_aggregate(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::AggregateFnCall(_) => true,
        RenderExpr::ScalarFnCall(f) => f.args.iter().any(source_contains_aggregate),
        RenderExpr::OperatorApplicationExp(op) => op.operands.iter().any(source_contains_aggregate),
        RenderExpr::Case(c) => {
            c.expr.as_deref().is_some_and(source_contains_aggregate)
                || c.when_then
                    .iter()
                    .any(|(w, t)| source_contains_aggregate(w) || source_contains_aggregate(t))
                || c.else_expr
                    .as_deref()
                    .is_some_and(source_contains_aggregate)
        }
        RenderExpr::List(items) => items.iter().any(source_contains_aggregate),
        RenderExpr::MapLiteral(entries) => {
            entries.iter().any(|(_, v)| source_contains_aggregate(v))
        }
        RenderExpr::ArraySubscript { array, index } => {
            source_contains_aggregate(array) || source_contains_aggregate(index)
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            source_contains_aggregate(array)
                || from.as_deref().is_some_and(source_contains_aggregate)
                || to.as_deref().is_some_and(source_contains_aggregate)
        }
        _ => false,
    }
}

/// Replace, in place, any leaf that names a SELECT alias (`ColumnAlias`,
/// `TableAlias`, or a bare `Column`) with that alias's source expression, walking
/// the same compound variants as `collect_bare_aliases_from_expr` in
/// plan_optimizer. `RenderExpr::Raw` is deliberately NOT handled: blindly
/// string-substituting an alias name inside opaque raw SQL is unsafe (it could
/// hit a substring or a quoted literal), so a raw predicate referencing an alias
/// is left as-is.
fn substitute_alias_refs_in_expr(expr: &mut RenderExpr, alias_map: &HashMap<String, RenderExpr>) {
    match expr {
        RenderExpr::ColumnAlias(ca) => {
            if let Some(src) = alias_map.get(&ca.0) {
                *expr = src.clone();
            }
        }
        RenderExpr::TableAlias(ta) => {
            if let Some(src) = alias_map.get(&ta.0) {
                *expr = src.clone();
            }
        }
        RenderExpr::Column(col) => {
            if let Some(src) = alias_map.get(col.raw()) {
                *expr = src.clone();
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                substitute_alias_refs_in_expr(operand, alias_map);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in func.args.iter_mut() {
                substitute_alias_refs_in_expr(arg, alias_map);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in agg.args.iter_mut() {
                substitute_alias_refs_in_expr(arg, alias_map);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(e) = case.expr.as_mut() {
                substitute_alias_refs_in_expr(e, alias_map);
            }
            for (when, then) in case.when_then.iter_mut() {
                substitute_alias_refs_in_expr(when, alias_map);
                substitute_alias_refs_in_expr(then, alias_map);
            }
            if let Some(e) = case.else_expr.as_mut() {
                substitute_alias_refs_in_expr(e, alias_map);
            }
        }
        RenderExpr::List(items) => {
            for item in items.iter_mut() {
                substitute_alias_refs_in_expr(item, alias_map);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            substitute_alias_refs_in_expr(array, alias_map);
            substitute_alias_refs_in_expr(index, alias_map);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            substitute_alias_refs_in_expr(array, alias_map);
            if let Some(f) = from.as_mut() {
                substitute_alias_refs_in_expr(f, alias_map);
            }
            if let Some(t) = to.as_mut() {
                substitute_alias_refs_in_expr(t, alias_map);
            }
        }
        RenderExpr::InSubquery(subq) => {
            substitute_alias_refs_in_expr(&mut subq.expr, alias_map);
        }
        _ => {}
    }
}

/// Resolve Cypher `size(arg)` to a Spark/Databricks function name, or `None`
/// when no dialect-specific override applies (caller falls back to the registry
/// default, `length`).
///
/// CH `length` is overloaded for strings and arrays; Spark is not — `size` is
/// collection-only and `length` string-only. The static registry name can't be
/// right for both, and the argument's type is not always inferable from the
/// Cypher text (a bare `posts` and a bare `name` both render as a column ref).
/// So the default stays the string-safe `length` and this returns `Some("size")`
/// only when the argument is a *detected* collection: an inline list literal, a
/// `collect`/`groupArray` aggregate, or a variable the registry typed as a
/// collection (e.g. `WITH collect(post) AS posts`). Schemas that need `size()`
/// over a non-obvious collection should declare the column's array type so the
/// registry resolves it. Returns `None` outside Databricks (CH unchanged).
fn databricks_size_name(
    arg: Option<&RenderExpr>,
    dialect: crate::sql_generator::SqlDialect,
) -> Option<&'static str> {
    use crate::sql_generator::SqlDialect;
    if dialect != SqlDialect::Databricks {
        return None;
    }
    arg.filter(|a| render_arg_is_collection(a)).map(|_| "size")
}

/// True when a `size()` argument is recognizably a collection (vs a string).
fn render_arg_is_collection(arg: &RenderExpr) -> bool {
    fn name_is_collection(name: &str) -> bool {
        // Variable registry (collection-typed vars) OR the array-CTE-column set
        // collected from the plan (carried-forward collect()/groupArray columns
        // the registry types only as scalars).
        crate::server::query_context::get_current_variable_registry()
            .and_then(|r| r.lookup(name).map(|v| v.is_collection()))
            .unwrap_or(false)
            || crate::server::query_context::is_array_cte_column(name)
    }
    match arg {
        RenderExpr::List(_) => true,
        RenderExpr::AggregateFnCall(a) => is_collection_aggregate(&a.name),
        RenderExpr::TableAlias(ta) => name_is_collection(&ta.0),
        RenderExpr::Column(c) => name_is_collection(c.raw()),
        // A carried-forward CTE column keeps its producing variable's name as the
        // column (e.g. `posts`); match on that. The table alias is intentionally
        // not consulted — property access on a collection isn't valid Cypher, so
        // checking it would only widen the false-positive surface.
        RenderExpr::PropertyAccessExp(pa) => name_is_collection(pa.column.raw()),
        _ => false,
    }
}

/// True for aggregates that produce an array value.
fn is_collection_aggregate(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "collect" | "collect_list" | "collect_set" | "grouparray" | "array_agg"
    )
}

/// Collect the names of CTE output columns that hold an array/collection value:
/// a SELECT item whose source expression is a collection aggregate
/// (`collect`/`groupArray`/…) or a list literal. Walks every structured CTE and
/// UNION branch. Name-keyed (carried-forward columns keep the producing alias),
/// which is unambiguous within a single query plan.
fn collect_array_cte_columns(plan: &RenderPlan) -> HashSet<String> {
    fn expr_is_collection(expr: &RenderExpr) -> bool {
        match expr {
            RenderExpr::List(_) => true,
            RenderExpr::AggregateFnCall(a) => is_collection_aggregate(&a.name),
            _ => false,
        }
    }
    fn walk(plan: &RenderPlan, out: &mut HashSet<String>) {
        for item in &plan.select.items {
            if let Some(ca) = &item.col_alias {
                if expr_is_collection(&item.expression) {
                    out.insert(ca.0.clone());
                }
            }
        }
        if let Some(union) = plan.union.0.as_ref() {
            for branch in &union.input {
                walk(branch, out);
            }
        }
        for cte in &plan.ctes.0 {
            if let CteContent::Structured(cte_plan) = &cte.content {
                walk(cte_plan, out);
            }
        }
    }
    let mut out = HashSet::new();
    walk(plan, &mut out);
    out
}

/// Render the SKIP/LIMIT clause, dialect-aware (no trailing newline; empty when
/// neither is set). ClickHouse uses the MySQL-style `LIMIT offset, count` and
/// requires a count when offsetting (so SKIP-only emits a huge upper bound);
/// Spark/Databricks uses standard `LIMIT count OFFSET offset` and supports a
/// bare `OFFSET`. Replaces the same logic previously copy-pasted across the
/// union-branch, main-query, and CTE-body emission sites.
fn limit_offset_clause(skip: Option<i64>, limit: Option<i64>) -> String {
    use crate::server::query_context::get_current_dialect;
    use crate::sql_generator::SqlDialect;
    let databricks = matches!(get_current_dialect(), SqlDialect::Databricks);
    match (skip, limit) {
        (None, None) => String::new(),
        (None, Some(l)) => format!("LIMIT {l}"),
        (Some(s), Some(l)) => {
            if databricks {
                format!("LIMIT {l} OFFSET {s}")
            } else {
                format!("LIMIT {s}, {l}")
            }
        }
        (Some(s), None) => {
            if databricks {
                format!("OFFSET {s}")
            } else {
                // ClickHouse requires a count with an offset; use a huge upper bound.
                format!("LIMIT {s}, 18446744073709551615")
            }
        }
    }
}

/// Build the relationship columns mapping from a RenderPlan (for collecting data)
/// Returns the mapping of alias → (from_id_column, to_id_column)
fn build_relationship_columns_from_plan(plan: &RenderPlan) -> HashMap<String, (String, String)> {
    let mut map = HashMap::new();

    // Add joins from main plan - extract column from joining_on conditions
    for join in &plan.joins.0 {
        if let Some(from_col) = join.get_relationship_id_column() {
            // For now, just store from_col for both (we only need one for NULL checks)
            map.insert(join.table_alias.clone(), (from_col.clone(), from_col));
        }
    }

    // Also process unions (each branch has its own joins)
    if let Some(ref union) = plan.union.0 {
        for union_plan in &union.input {
            for join in &union_plan.joins.0 {
                if let Some(from_col) = join.get_relationship_id_column() {
                    map.insert(join.table_alias.clone(), (from_col.clone(), from_col));
                }
            }
        }
    }

    // Process CTEs recursively and merge results
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref cte_plan) = cte.content {
            let cte_map = build_relationship_columns_from_plan(cte_plan);
            map.extend(cte_map);
        }
    }

    map
}

/// Build CTE property mappings from RenderPlan CTEs (for collecting data)
/// Returns mapping of CTE alias → (property → column name)
fn build_cte_property_mappings(plan: &RenderPlan) -> HashMap<String, HashMap<String, String>> {
    let mut map = HashMap::new();

    // Process each CTE in the plan
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref cte_plan) = cte.content {
            let mut property_map: HashMap<String, String> = HashMap::new();

            // Build property mapping from SELECT items
            // Format: "property_name" → "cte_column_name"
            //
            // IMPORTANT: We use the FULL column name as the property name (e.g., "user_id" → "user_id")
            // because the column names in CTEs already come from ViewScan.property_mapping.
            //
            // Previous behavior: Used underscore/dot parsing to extract suffix (e.g., "user_id" → "id")
            // This broke auto-discovery schemas where property names include underscores.
            // Example bug: node_id=user_id with auto_discover_columns should expose property "user_id",
            // not "id" (which doesn't exist in the database).
            for select_item in &cte_plan.select.items {
                if let Some(ref col_alias) = select_item.col_alias {
                    let cte_col = col_alias.0.as_str();

                    // Identity mapping: property name = column name
                    property_map.insert(cte_col.to_string(), cte_col.to_string());
                }
            }

            if !property_map.is_empty() {
                log::debug!(
                    "🗺️  CTE '{}' property mapping: {:?}",
                    cte.cte_name,
                    property_map
                );
                map.insert(cte.cte_name.clone(), property_map.clone());
            }
        }
    }

    // CRITICAL: Also scan main plan's FROM clause to map CTE aliases
    // Example: FROM with_cnt_friend_cte_1 AS cnt_friend
    // We need to map BOTH "with_cnt_friend_cte_1" AND "cnt_friend" to the same property mapping
    if let Some(ref from_table) = plan.from.0 {
        let table_name = &from_table.name;
        let alias = from_table.alias.as_ref().unwrap_or(table_name);

        // If this FROM references a CTE (name starts with "with_" or matches a CTE name)
        if let Some(cte_mapping) = map.get(table_name).cloned() {
            if alias != table_name {
                log::debug!(
                    "🔗 Aliasing CTE '{}' as '{}' with same property mapping",
                    table_name,
                    alias
                );
                map.insert(alias.clone(), cte_mapping);
            }
        }
    }

    map
}

/// Build CTE alias → CTE name mapping for a specific RenderPlan scope.
/// Scans FROM/JOINs for references to known CTEs. Used per-scope (per CTE body
/// or main plan) to correctly resolve `IN alias.column` → `IN (SELECT col FROM cte)`.
fn build_cte_alias_mapping_for_scope(
    scope: &RenderPlan,
    cte_names: &HashSet<String>,
) -> HashMap<String, String> {
    let mut mapping = HashMap::new();

    // Map CTE names to themselves (for direct references like `with_x.col`)
    for name in cte_names {
        mapping.insert(name.clone(), name.clone());
    }

    // Check FROM clause
    if let Some(ref from_table) = scope.from.0 {
        if cte_names.contains(&from_table.name) {
            let alias = from_table.alias.as_ref().unwrap_or(&from_table.name);
            mapping.insert(alias.clone(), from_table.name.clone());
        }
    }
    // Check JOINs
    for join in &scope.joins.0 {
        if cte_names.contains(&join.table_name) {
            mapping.insert(join.table_alias.clone(), join.table_name.clone());
        }
    }
    // Check UNION branch FROM/JOINs
    if let Some(ref union) = scope.union.0 {
        for branch in &union.input {
            if let Some(ref from_table) = branch.from.0 {
                if cte_names.contains(&from_table.name) {
                    let alias = from_table.alias.as_ref().unwrap_or(&from_table.name);
                    mapping.insert(alias.clone(), from_table.name.clone());
                }
            }
            for join in &branch.joins.0 {
                if cte_names.contains(&join.table_name) {
                    mapping.insert(join.table_alias.clone(), join.table_name.clone());
                }
            }
        }
    }

    mapping
}

/// Build multi-type VLP aliases tracking from RenderPlan
/// Returns mapping of Cypher alias → CTE name for multi-type VLP queries
fn build_multi_type_vlp_aliases(plan: &RenderPlan) -> HashMap<String, String> {
    let mut aliases = HashMap::new();

    // Collect WITH CTE aliases to avoid conflicts
    // WITH CTEs (e.g., with_a_cte_0) export aliases that access base tables directly,
    // NOT through VLP JSON properties. We must not register these as VLP aliases.
    let mut with_cte_aliases: HashSet<String> = HashSet::new();
    for cte in &plan.ctes.0 {
        if cte.cte_name.starts_with("with_") {
            // Extract the alias from CTE name (e.g., "with_a_cte_0" → "a")
            // Also handle compound names like "with_a_allNeighboursCount_cte_0" → "a"
            if let Some(rest) = cte.cte_name.strip_prefix("with_") {
                if let Some(alias) = rest.split("_cte").next() {
                    with_cte_aliases.insert(alias.to_string());
                    // Also insert the first segment for compound aliases
                    // e.g., "a_allNeighboursCount" → also insert "a"
                    if let Some(first) = alias.split('_').next() {
                        with_cte_aliases.insert(first.to_string());
                    }
                }
            }
        }
    }

    // Track multi-type VLP aliases for JSON property extraction
    // Multi-type VLP CTEs have names like "vlp_multi_type_u_x"
    // and their end_properties column contains JSON with node properties
    for cte in &plan.ctes.0 {
        if cte.cte_name.starts_with("vlp_multi_type_") {
            // Extract Cypher alias from CTE metadata if available
            if let Some(ref cypher_end_alias) = cte.vlp_cypher_end_alias {
                // Skip if this alias is also a WITH CTE alias — WITH CTEs access base tables
                if with_cte_aliases.contains(cypher_end_alias.as_str()) {
                    log::info!(
                        "🎯 Skipping VLP alias '{}' — conflicts with WITH CTE alias",
                        cypher_end_alias
                    );
                    continue;
                }
                aliases.insert(cypher_end_alias.clone(), cte.cte_name.clone());
                log::info!(
                    "🎯 Tracked multi-type VLP alias: '{}' → CTE '{}'",
                    cypher_end_alias,
                    cte.cte_name
                );
            }
        }
    }

    aliases
}

/// Rewrite property access in SELECT, GROUP BY items for VLP queries
/// Maps Cypher aliases (a, b) to CTE column names (start_xxx, end_xxx)
/// For VLP, the CTE includes properties named using the Cypher property name: start_email, start_name, etc.
fn rewrite_vlp_select_aliases(mut plan: RenderPlan) -> RenderPlan {
    log::debug!("🔍 TRACING: rewrite_vlp_select_aliases called - checking for VLP CTEs");
    // 🔧 FIX: If FROM references a WITH CTE (not the raw VLP CTE), skip this rewriting
    // The WITH CTE has already transformed the columns, and the SELECT items reference
    // the WITH CTE columns, not the raw VLP CTE columns.
    if let Some(from_ref) = &plan.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            log::debug!(
                "🔧 VLP: FROM uses WITH CTE '{}' - skipping VLP SELECT rewriting",
                from_ref.name
            );
            return plan;
        }
    }

    // ── Fan-in detection ────────────────────────────────────────────────────
    // When multiple VLP CTEs share the same vlp_cypher_end_alias (e.g. "x"),
    // each CTE represents one "inbound edge" fan-in constraint
    //   (a)-->(x), (b)-->(x), (c)-->(x)
    // The generated plan picks the outermost CTE as FROM and ignores the rest.
    // Fix: use the first CTE as FROM, JOIN the others on end_id.
    {
        // Collect VLP CTEs grouped by end_alias
        let fan_in_ctes: Vec<&Cte> = plan
            .ctes
            .0
            .iter()
            .filter(|c| c.vlp_cypher_start_alias.is_some() && c.vlp_cypher_end_alias.is_some())
            .collect();

        // Group by end_alias; fan-in only when all share the same end
        let first_end = fan_in_ctes
            .first()
            .and_then(|c| c.vlp_cypher_end_alias.as_deref());
        let all_same_end = first_end.is_some()
            && fan_in_ctes
                .iter()
                .all(|c| c.vlp_cypher_end_alias.as_deref() == first_end);

        if fan_in_ctes.len() > 1 && all_same_end {
            log::info!(
                "🔀 Fan-in VLP detected: {} CTEs all targeting '{}'",
                fan_in_ctes.len(),
                first_end.unwrap_or("")
            );

            let first = fan_in_ctes[0];

            // Set FROM to the first VLP CTE with the standard alias "t"
            plan.from = FromTableItem(Some(ViewTableRef {
                source: std::sync::Arc::new(LogicalPlan::Empty),
                name: first.cte_name.clone(),
                alias: Some(VLP_CTE_FROM_ALIAS.to_string()),
                use_final: false,
            }));

            // Add INNER JOINs for the remaining CTEs, joining on end_id
            for (i, other) in fan_in_ctes[1..].iter().enumerate() {
                let other_alias = format!("t_fi_{}", i);
                let join = Join {
                    table_name: other.cte_name.clone(),
                    table_alias: other_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(other_alias),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        VLP_END_ID_COLUMN.to_string(),
                                    ),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(VLP_CTE_FROM_ALIAS.to_string()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        VLP_END_ID_COLUMN.to_string(),
                                    ),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                };
                plan.joins.0.push(join);
            }

            // Each VLP CTE already has its own start-node filter (WHERE c_1.name = '...').
            // The outer WHERE referencing start aliases (e.g. t.start_name = 'C') is
            // redundant and incorrect after FROM is changed. Clear it.
            plan.filters = FilterItems(None);

            // The SELECT already references t.end_* (from the old outermost CTE).
            // Since we now use the first CTE as FROM with the same alias "t", no
            // SELECT rewriting is needed — return immediately.
            return plan;
        }
    }

    // Check if any CTE is a VLP CTE
    let vlp_cte = plan
        .ctes
        .0
        .iter()
        .find(|cte| cte.vlp_cypher_start_alias.is_some());

    log::debug!(
        "🔍 TRACING: Checking for VLP CTEs. Found {} CTEs",
        plan.ctes.0.len()
    );
    for (i, cte) in plan.ctes.0.iter().enumerate() {
        log::debug!(
            "🔍 TRACING: CTE {}: name={}, vlp_start_alias={:?}",
            i,
            cte.cte_name,
            cte.vlp_cypher_start_alias
        );
    }

    if let Some(vlp_cte) = vlp_cte {
        // 🔧 FIX: For OPTIONAL MATCH + VLP, FROM uses the anchor node table (not the VLP CTE),
        // and the VLP CTE is added as a LEFT JOIN. In this case, we should NOT rewrite
        // expressions because:
        // - FROM is: users AS a (anchor node)
        // - SELECT should reference: a.name (from anchor), COUNT(DISTINCT t.end_id) (from VLP CTE)
        // - VLP CTE is: LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
        //
        // Detection: If FROM uses a regular table (not the VLP CTE), skip rewriting
        log::debug!("🔍 TRACING: VLP CTE detected: {}", vlp_cte.cte_name);
        if let Some(from_ref) = &plan.from.0 {
            log::debug!(
                "🔍 TRACING: FROM ref name: '{}', starts_with vlp_: {}",
                from_ref.name,
                from_ref.name.starts_with("vlp_")
            );
            if !from_ref.name.starts_with("vlp_") {
                log::debug!(
                    "🔍 TRACING: OPTIONAL VLP detected - FROM uses anchor table '{}' - SKIPPING VLP SELECT rewriting",
                    from_ref.name
                );
                log::info!(
                    "   Anchor properties will be accessed directly (e.g., a.name), VLP CTE ({}) used via LEFT JOIN",
                    vlp_cte.cte_name
                );
                // Still rewrite UNION branches — they may use a VLP CTE directly
                // even when the main plan's FROM is an anchor table (e.g., UNION ALL of a
                // non-VLP FOLLOWS branch and a multi-type VLP AUTHORED/LIKED branch).
                let parent_ctes_snap = plan.ctes.0.clone();
                if let Some(ref mut union) = plan.union.0 {
                    for branch in union.input.iter_mut() {
                        rewrite_vlp_branch_select(branch, &parent_ctes_snap);
                    }
                }
                return plan;
            } else {
                log::debug!(
                    "🔍 TRACING: NOT optional VLP - FROM uses VLP CTE - proceeding with rewriting"
                );
            }
        } else {
            // FROM is None — likely a Union shell where branches have their own FROM.
            // Check if any Union branch FROM uses the VLP CTE. If not, the VLP CTE
            // is consumed by a WITH CTE (not by the main query) — skip rewriting.
            let any_branch_uses_vlp = plan.union.0.as_ref().is_some_and(|union| {
                union.input.iter().any(|branch| {
                    branch
                        .from
                        .0
                        .as_ref()
                        .is_some_and(|f| f.name.starts_with("vlp_"))
                })
            });
            if !any_branch_uses_vlp {
                log::info!(
                    "🔍 VLP rewriting: FROM=None and no Union branch uses VLP CTE - skipping rewriting"
                );
                return plan;
            }
            log::debug!("🔍 TRACING: No FROM ref found but Union branches use VLP");
        }

        let mut start_alias = vlp_cte.vlp_cypher_start_alias.clone();
        let mut end_alias = vlp_cte.vlp_cypher_end_alias.clone();
        let path_variable = vlp_cte.vlp_path_variable.clone();
        // Non-OPTIONAL VLP: always rewrite start alias (we return early for OPTIONAL VLP)
        let is_optional_vlp = false;

        // Skip rewriting aliases that are covered by WITH CTE JOINs
        // These aliases reference WITH CTE columns, not VLP CTE columns
        for join in &plan.joins.0 {
            if join.table_name.starts_with("with_") {
                if start_alias.as_deref() == Some(join.table_alias.as_str()) {
                    log::info!(
                        "🔧 VLP top-level: Skipping start alias '{}' rewrite (covered by WITH CTE '{}')",
                        join.table_alias, join.table_name
                    );
                    start_alias = None;
                }
                if end_alias.as_deref() == Some(join.table_alias.as_str()) {
                    log::info!(
                        "🔧 VLP top-level: Skipping end alias '{}' rewrite (covered by WITH CTE '{}')",
                        join.table_alias, join.table_name
                    );
                    end_alias = None;
                }
            }
        }
        // Also check Union branches for WITH CTE JOINs
        if let Some(ref union) = plan.union.0 {
            for branch in &union.input {
                for join in &branch.joins.0 {
                    if join.table_name.starts_with("with_") {
                        if start_alias.as_deref() == Some(join.table_alias.as_str()) {
                            log::info!(
                                "🔧 VLP top-level: Skipping start alias '{}' rewrite (covered by WITH CTE in branch)",
                                join.table_alias
                            );
                            start_alias = None;
                        }
                        if end_alias.as_deref() == Some(join.table_alias.as_str()) {
                            log::info!(
                                "🔧 VLP top-level: Skipping end alias '{}' rewrite (covered by WITH CTE in branch)",
                                join.table_alias
                            );
                            end_alias = None;
                        }
                    }
                }
            }
        }

        log::info!(
            "🔧 VLP SELECT rewriting: start_alias={:?}, end_alias={:?}, path_variable={:?}",
            start_alias,
            end_alias,
            path_variable
        );
        log::info!("🔧 SELECT has {} items", plan.select.items.len());

        // Rewrite each SELECT item's expressions
        for (idx, item) in plan.select.items.iter_mut().enumerate() {
            log::info!("🔧 Item {}: {:?}", idx, item.expression);
            let before = format!("{:?}", item.expression);
            item.expression = rewrite_expr_for_vlp(
                &item.expression,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", item.expression);
            if before != after {
                log::info!("🔧   Rewritten from: {} → {}", before, after);
            }
        }

        // Inject schema-natural relationship FK projections for 1-hop multi-type
        // VLP queries. The CTE projects `r_from_id` / `r_to_id` columns whose
        // values are the natural from→to direction regardless of which CTE
        // branch (Outgoing/Incoming) produced the row. result_transformer uses
        // these to construct a canonical relationship element_id, which fixes
        // duplicate edges in Browser when the same edge appears in two
        // expansions from different endpoints (the same-label-relationship
        // direction issue that label-matching reversal detection can't
        // distinguish).
        //
        // We detect "this is a 1-hop relationship-VLP query" by finding a
        // SELECT item whose col_alias starts with "<relvar>." and whose
        // rewritten expression is `t.start_id` — i.e., a relationship's
        // start_id projection. The relvar name comes from the col_alias prefix.
        let mut rel_var_name: Option<String> = None;
        {
            use crate::graph_catalog::expression_parser::PropertyValue;
            for item in plan.select.items.iter() {
                if let Some(ref col_alias) = item.col_alias {
                    if col_alias.0.ends_with(".start_id") {
                        if let RenderExpr::Column(Column(PropertyValue::Column(col))) =
                            &item.expression
                        {
                            if col == "t.start_id" {
                                let rv = col_alias.0.trim_end_matches(".start_id").to_string();
                                if !rv.is_empty() {
                                    rel_var_name = Some(rv);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(rv) = rel_var_name {
            use crate::graph_catalog::expression_parser::PropertyValue;
            use crate::render_plan::render_expr::ColumnAlias;
            use crate::render_plan::SelectItem;
            // Only inject if the CTE actually exposes these columns (1-hop case).
            // The multi-type VLP CTE projects them when hop_count == 1; check by
            // looking at the FROM CTE name pattern.
            let from_is_multi_type_vlp = plan
                .from
                .0
                .as_ref()
                .is_some_and(|f| f.name.starts_with("vlp_multi_type_"));
            if from_is_multi_type_vlp {
                let already_injected = plan.select.items.iter().any(|item| {
                    item.col_alias
                        .as_ref()
                        .is_some_and(|ca| ca.0 == format!("{}.r_from_id", rv))
                });
                if !already_injected {
                    plan.select.items.push(SelectItem {
                        expression: RenderExpr::Column(Column(PropertyValue::Column(
                            "t.r_from_id".to_string(),
                        ))),
                        col_alias: Some(ColumnAlias(format!("{}.r_from_id", rv))),
                    });
                    plan.select.items.push(SelectItem {
                        expression: RenderExpr::Column(Column(PropertyValue::Column(
                            "t.r_to_id".to_string(),
                        ))),
                        col_alias: Some(ColumnAlias(format!("{}.r_to_id", rv))),
                    });
                    log::info!("🔧 Injected r_from_id/r_to_id projections for rel '{}'", rv);
                }
            }
        }

        // 🔧 BUG FIX: Also rewrite GROUP BY expressions for VLP queries
        // The GROUP BY clause may contain Cypher aliases (e.g., a.full_name)
        // that need to be rewritten to use VLP CTE columns (e.g., t.start_name)
        log::info!("🔧 VLP GROUP BY rewriting: {} items", plan.group_by.0.len());
        for (idx, group_expr) in plan.group_by.0.iter_mut().enumerate() {
            log::info!("🔧 GROUP BY {}: {:?}", idx, group_expr);
            let before = format!("{:?}", group_expr);
            *group_expr = rewrite_expr_for_vlp(
                group_expr,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", group_expr);
            if before != after {
                log::info!("🔧   GROUP BY rewritten from: {} → {}", before, after);
            }
        }

        // 🔧 BUG FIX: Also rewrite ORDER BY expressions for VLP queries
        // The ORDER BY clause may contain Cypher aliases (e.g., b.name)
        // that need to be rewritten to use VLP CTE columns (e.g., t.end_name)
        log::info!("🔧 VLP ORDER BY rewriting: {} items", plan.order_by.0.len());
        for (idx, order_item) in plan.order_by.0.iter_mut().enumerate() {
            log::info!("🔧 ORDER BY {}: {:?}", idx, order_item.expression);
            let before = format!("{:?}", order_item.expression);
            order_item.expression = rewrite_expr_for_vlp(
                &order_item.expression,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", order_item.expression);
            if before != after {
                log::info!("🔧   ORDER BY rewritten from: {} → {}", before, after);
            }
        }

        // Also rewrite WHERE clause for VLP queries
        // The WHERE may reference Cypher node aliases (e.g., o.user_id) that need
        // to be rewritten to VLP CTE column references (e.g., t.end_user_id)
        if let Some(ref filter_expr) = plan.filters.0 {
            let before = format!("{:?}", filter_expr);
            let rewritten = rewrite_expr_for_vlp(
                filter_expr,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", rewritten);
            if before != after {
                log::info!("🔧   WHERE rewritten from: {} → {}", before, after);
            }
            plan.filters = FilterItems(Some(rewritten));
        }

        // 🔧 CRITICAL FIX: Also rewrite JOIN conditions for VLP queries
        // JOIN conditions may reference Cypher node aliases (e.g., p.id, b.user_id) that need
        // to be rewritten to VLP CTE column references (e.g., t.end_id, t.end_user_id)
        //
        // Root cause: JOINs are built during logical plan → render plan conversion using
        // original Cypher variable names. After VLP CTE is created, these references must
        // be rewritten to use the CTE's start_/end_ columns.
        //
        // This was an oversight - we were rewriting SELECT/WHERE/GROUP BY/ORDER BY but not JOINs.
        log::info!("🔧 VLP JOIN rewriting: {} items", plan.joins.0.len());
        for (idx, join) in plan.joins.0.iter_mut().enumerate() {
            log::info!(
                "🔧 JOIN {}: table={}, alias={}",
                idx,
                join.table_name,
                join.table_alias
            );

            // Rewrite each condition in joining_on
            for (cond_idx, condition) in join.joining_on.iter_mut().enumerate() {
                let before = format!("{:?}", condition);

                // Rewrite left operand
                condition.operands[0] = rewrite_expr_for_vlp(
                    &condition.operands[0],
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );

                // Rewrite right operand
                condition.operands[1] = rewrite_expr_for_vlp(
                    &condition.operands[1],
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );

                let after = format!("{:?}", condition);
                if before != after {
                    log::info!(
                        "🔧   JOIN[{}] condition[{}] rewritten from: {} → {}",
                        idx,
                        cond_idx,
                        before,
                        after
                    );
                }
            }

            // Also rewrite pre_filter if present
            if let Some(ref filter_expr) = join.pre_filter {
                let before = format!("{:?}", filter_expr);
                let rewritten = rewrite_expr_for_vlp(
                    filter_expr,
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );
                let after = format!("{:?}", rewritten);
                if before != after {
                    log::info!(
                        "🔧   JOIN[{}] pre_filter rewritten from: {} → {}",
                        idx,
                        before,
                        after
                    );
                }
                join.pre_filter = Some(rewritten);
            }
        }
    }

    // Remove spurious JOINs and metadata SELECT items from the main plan
    // when FROM is a VLP CTE for shortestPath queries. Only shortestPath patterns
    // (with path_variable) produce these spurious artifacts from multi-pattern MATCH.
    // Other VLP queries have legitimate JOINs to non-VLP tables.
    if let Some(from_ref) = &plan.from.0 {
        if from_ref.name.starts_with("vlp_") {
            let is_shortest_path = plan
                .ctes
                .0
                .iter()
                .any(|cte| cte.vlp_path_variable.is_some());
            if is_shortest_path {
                plan.joins.0.retain(|join| {
                    join.table_name.starts_with("vlp_") || join.table_name.starts_with("with_")
                });
                plan.select.items.retain(|item| {
                    if let Some(ref col_alias) = item.col_alias {
                        !matches!(
                            col_alias.0.as_str(),
                            "_rel_properties"
                                | "__rel_type__"
                                | "__start_label__"
                                | "__end_label__"
                        )
                    } else {
                        true
                    }
                });
            }
        }
    }

    // Also rewrite UNION branches — each may have its own VLP CTE
    // (e.g., undirected patterns create separate CTEs for each direction)
    // Pass parent CTEs so branches can find VLP CTE info (path_variable, start/end aliases)
    // when their own branch.ctes is empty (VLP CTEs live in the parent plan)
    let parent_ctes = plan.ctes.0.clone();
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            rewrite_vlp_branch_select(branch, &parent_ctes);
        }
    }

    plan
}

/// Rewrite VLP SELECT aliases for a single UNION branch RenderPlan.
/// Same logic as the main rewrite_vlp_select_aliases but operates on a branch.
/// `parent_ctes` provides VLP CTE info from the parent plan when the branch has none.
fn rewrite_vlp_branch_select(branch: &mut RenderPlan, parent_ctes: &[crate::render_plan::Cte]) {
    // Skip if FROM is a generated CTE (WITH clause)
    if let Some(from_ref) = &branch.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            return;
        }
    }

    // Check if FROM references a VLP CTE (starts with "vlp_")
    // The VLP CTE is defined at the parent level, not in branch.ctes
    let from_is_vlp = branch
        .from
        .0
        .as_ref()
        .is_some_and(|f| f.name.starts_with("vlp_"));

    if !from_is_vlp {
        return;
    }

    // Find VLP CTE info from branch's own CTEs (may be empty for child branches)
    // Fall back to parent CTEs when branch has none (VLP CTEs live at parent level)
    let vlp_cte = branch
        .ctes
        .0
        .iter()
        .find(|cte| cte.vlp_cypher_start_alias.is_some());

    let (mut start_alias, mut end_alias, path_variable) = if let Some(vlp_cte) = vlp_cte {
        (
            vlp_cte.vlp_cypher_start_alias.clone(),
            vlp_cte.vlp_cypher_end_alias.clone(),
            vlp_cte.vlp_path_variable.clone(),
        )
    } else {
        // No VLP CTE in branch.ctes - look up from parent CTEs using the branch's FROM name
        let from_name = branch
            .from
            .0
            .as_ref()
            .map(|f| f.name.as_str())
            .unwrap_or("");
        let parent_vlp = parent_ctes
            .iter()
            .find(|cte| cte.cte_name == from_name && cte.vlp_cypher_start_alias.is_some());
        if let Some(parent_cte) = parent_vlp {
            // The parent VLP CTE has the correct aliases for this branch's direction
            (
                parent_cte.vlp_cypher_start_alias.clone(),
                parent_cte.vlp_cypher_end_alias.clone(),
                parent_cte.vlp_path_variable.clone(),
            )
        } else {
            // Last resort: infer from filter expressions
            let start_alias = if let Some(ref filter) = branch.filters.0 {
                extract_alias_from_filter(filter)
            } else {
                None
            };
            (start_alias, None, None)
        }
    };

    // Skip rewriting if we couldn't determine start_alias
    let Some(_) = start_alias else {
        return;
    };

    // Skip rewriting aliases that are covered by WITH CTE JOINs
    // These aliases reference WITH CTE columns, not VLP CTE columns
    for join in &branch.joins.0 {
        if join.table_name.starts_with("with_") {
            if start_alias.as_deref() == Some(&join.table_alias) {
                log::info!(
                    "🔧 VLP branch: Skipping start alias '{}' rewrite (covered by WITH CTE '{}')",
                    join.table_alias,
                    join.table_name
                );
                start_alias = None;
            }
            if end_alias.as_deref() == Some(&join.table_alias) {
                log::info!(
                    "🔧 VLP branch: Skipping end alias '{}' rewrite (covered by WITH CTE '{}')",
                    join.table_alias,
                    join.table_name
                );
                end_alias = None;
            }
        }
    }

    log::info!(
        "🔧 VLP UNION branch rewriting: start={:?}, end={:?}",
        start_alias,
        end_alias
    );

    for item in branch.select.items.iter_mut() {
        item.expression = rewrite_expr_for_vlp(
            &item.expression,
            &start_alias,
            &end_alias,
            &path_variable,
            false,
        );
    }
    for group_expr in branch.group_by.0.iter_mut() {
        *group_expr =
            rewrite_expr_for_vlp(group_expr, &start_alias, &end_alias, &path_variable, false);
    }
    for order_item in branch.order_by.0.iter_mut() {
        order_item.expression = rewrite_expr_for_vlp(
            &order_item.expression,
            &start_alias,
            &end_alias,
            &path_variable,
            false,
        );
    }
    // 🔧 FIX: Also rewrite WHERE clause (filters) for VLP UNION branches
    // Without this, branches with LIMIT get wrapped in subqueries with unrewritten WHERE clauses
    if let Some(ref filter_expr) = branch.filters.0 {
        let rewritten =
            rewrite_expr_for_vlp(filter_expr, &start_alias, &end_alias, &path_variable, false);
        branch.filters.0 = Some(rewritten);
    }

    // 🔧 FIX: Also rewrite JOIN conditions for VLP UNION branches.
    // JOINs are built during logical plan → render plan conversion using original Cypher
    // variable names (e.g., u2.user_id). After VLP CTE creation, these must be rewritten
    // to use VLP CTE columns (e.g., t.end_id). Without this, post-VLP relationship JOINs
    // (e.g., VLP endpoint -> AUTHORED -> Post) reference non-existent aliases.
    for join in branch.joins.0.iter_mut() {
        for condition in join.joining_on.iter_mut() {
            for operand in condition.operands.iter_mut() {
                *operand =
                    rewrite_expr_for_vlp(operand, &start_alias, &end_alias, &path_variable, false);
            }
        }
        if let Some(ref filter_expr) = join.pre_filter {
            join.pre_filter = Some(rewrite_expr_for_vlp(
                filter_expr,
                &start_alias,
                &end_alias,
                &path_variable,
                false,
            ));
        }
    }

    // JOIN ordering for VLP branches is handled later by the global
    // `sort_joins_by_dependency` pass in `render_plan_to_sql()`.

    // 🔧 FIX: Remove spurious JOINs from VLP branches in multi-pattern MATCH.
    // Only for shortestPath queries (path_variable is set): JOINs to regular tables
    // are redundant because the VLP CTE already encodes the full traversal with
    // endpoint filters. For non-shortestPath VLPs, chained JOINs are legitimate
    // (e.g., VLP endpoint -> AUTHORED -> Post).
    if from_is_vlp && path_variable.is_some() {
        let before_count = branch.joins.0.len();
        branch.joins.0.retain(|join| {
            // Keep JOINs to VLP CTEs or WITH CTEs
            if join.table_name.starts_with("vlp_") || join.table_name.starts_with("with_") {
                return true;
            }
            // Remove JOINs to regular tables (spurious from multi-pattern MATCH)
            log::info!(
                "🔧 VLP branch cleanup: removing spurious JOIN to {} AS {}",
                join.table_name,
                join.table_alias
            );
            false
        });
        if branch.joins.0.len() < before_count {
            log::info!(
                "🔧 VLP branch cleanup: removed {} spurious JOINs",
                before_count - branch.joins.0.len()
            );
        }

        // Remove metadata SELECT items (relationship metadata not needed for path queries)
        branch.select.items.retain(|item| {
            if let Some(ref col_alias) = item.col_alias {
                // Keep user-defined aliases, remove internal metadata
                !matches!(
                    col_alias.0.as_str(),
                    "_rel_properties" | "__rel_type__" | "__start_label__" | "__end_label__"
                )
            } else {
                true
            }
        });
    }
}

/// Extract table alias from a filter expression (e.g., "u.user_id" -> "u")
fn extract_alias_from_filter(expr: &RenderExpr) -> Option<String> {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => Some(prop.table_alias.0.clone()),
        RenderExpr::OperatorApplicationExp(op) => {
            // Check first operand
            for operand in &op.operands {
                if let Some(alias) = extract_alias_from_filter(operand) {
                    return Some(alias);
                }
            }
            None
        }
        _ => None,
    }
}

/// Recursively rewrite expressions to map VLP Cypher aliases to CTE column names
/// When we encounter PropertyAccess(a, xxx), we need to look up the Cypher property name
/// and create Column("start_xxx") using that Cypher property name (not the DB column name)
///
/// The challenge: at this point, we only have the DB column name from PropertyAccess.
/// The CTE was created with: `start_node.db_column AS start_cypher_property_name`
/// But the SELECT has: PropertyAccess(a, db_column_name)
///
/// To fix this, we need to NOT try to extract the property name from PropertyAccess,
/// but instead rely on the fact that properties are expanded at the render level.
/// The SELECT items should already have the Cypher property names as aliases,
/// and we just need to use those CTE column names directly.
///
/// Also handles path function rewriting:
/// - length(p) → t.hop_count
/// - nodes(p) → t.path_nodes  
/// - relationships(p) → t.path_relationships
pub(crate) fn rewrite_expr_for_vlp(
    expr: &RenderExpr,
    start_alias: &Option<String>,
    end_alias: &Option<String>,
    path_variable: &Option<String>,
    skip_start_alias: bool,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    match expr {
        RenderExpr::TableAlias(alias) => {
            // For VLP, TableAlias references to VLP endpoints should be rewritten to CTE columns
            if let Some(start) = start_alias {
                if &alias.0 == start {
                    if skip_start_alias {
                        return expr.clone();
                    }
                    return RenderExpr::Column(Column(PropertyValue::Column(
                        "t.start_id".to_string(),
                    )));
                }
            }
            if let Some(end) = end_alias {
                if &alias.0 == end {
                    return RenderExpr::Column(Column(PropertyValue::Column(
                        "t.end_id".to_string(),
                    )));
                }
            }
            // Handle bare path variable: p -> tuple(t.path_nodes, t.path_relationships, t.hop_count)
            // When RETURN p is used for a path variable, expand it to a tuple of path components
            if path_variable.as_ref() == Some(&alias.0) {
                log::info!(
                    "VLP path variable expansion: {} -> tuple({}.path_nodes, ...)",
                    alias.0,
                    VLP_CTE_FROM_ALIAS,
                );
                return RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: vec![
                        RenderExpr::Column(Column(PropertyValue::Column(format!(
                            "{}.path_nodes",
                            VLP_CTE_FROM_ALIAS
                        )))),
                        RenderExpr::Column(Column(PropertyValue::Column(format!(
                            "{}.path_relationships",
                            VLP_CTE_FROM_ALIAS
                        )))),
                        RenderExpr::Column(Column(PropertyValue::Column(format!(
                            "{}.hop_count",
                            VLP_CTE_FROM_ALIAS
                        )))),
                    ],
                });
            }
            expr.clone()
        }

        // Handle path functions: length(p), nodes(p), relationships(p)
        RenderExpr::ScalarFnCall(func) => {
            // Check if this is a path function with the path variable as argument
            if let Some(path_var) = path_variable {
                if func.args.len() == 1 {
                    if let RenderExpr::TableAlias(alias) = &func.args[0] {
                        if &alias.0 == path_var {
                            // This is a path function call: length(p), nodes(p), relationships(p)
                            let cte_column = match func.name.as_str() {
                                "length" => Some("hop_count"),
                                "nodes" => Some("path_nodes"),
                                "relationships" => Some("path_relationships"),
                                "cost" => Some("total_weight"),
                                _ => None,
                            };

                            if let Some(col_name) = cte_column {
                                log::info!(
                                    "🔧 VLP path function: {}({}) → t.{}",
                                    func.name,
                                    path_var,
                                    col_name
                                );
                                return RenderExpr::Column(Column(PropertyValue::Column(format!(
                                    "t.{}",
                                    col_name
                                ))));
                            }
                        }
                    }
                }
            }

            // Not a path function - recursively rewrite arguments
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| {
                        rewrite_expr_for_vlp(
                            a,
                            start_alias,
                            end_alias,
                            path_variable,
                            skip_start_alias,
                        )
                    })
                    .collect(),
            })
        }

        // Rewrite PropertyAccess for VLP aliases
        // PropertyAccess(a, email_address) should NOT be changed by us -
        // it's handled at expansion level. But if we encounter it here,
        // convert to Column with the CTE column name format.
        //
        // The CTE columns are: start_email, start_name, etc. (using Cypher property names)
        // But PropertyAccess gives us database names like email_address, full_name
        // We need to match these by deriving the property name.
        //
        // Special case: For ID columns (e.g., "id.orig_h"), use t.start_id or t.end_id directly
        // since the CTE has "start_id" column containing the full ID value.
        RenderExpr::PropertyAccessExp(prop) => {
            log::trace!(
                "🔧 rewrite_expr_for_vlp: Processing PropertyAccessExp {}.{}",
                prop.table_alias.0,
                prop.column.raw()
            );
            if let Some(start) = start_alias {
                if &prop.table_alias.0 == start {
                    if skip_start_alias {
                        log::debug!("🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' but skipping for OPTIONAL VLP", start);
                        return expr.clone();
                    }
                    log::debug!("🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' - rewriting to t.start_xxx", start);

                    // Check if this is the ID column (contains "id" or matches known ID column patterns)
                    let col_raw = prop.column.raw();
                    if col_raw == "id"
                        || col_raw.starts_with("id.")
                        || col_raw.ends_with("_id")
                        || col_raw.contains(".orig_")
                        || col_raw.contains(".resp_")
                    {
                        // This is the ID column - use t.start_id directly
                        return RenderExpr::Column(Column(PropertyValue::Column(
                            "t.start_id".to_string(),
                        )));
                    }

                    // This is accessing start node property
                    // Create Column with the full table.column format to prevent heuristic inference
                    // The FROM clause has the CTE aliased as 't', so use t.start_xxx
                    let prop_name = derive_cypher_property_name(col_raw);
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.start_{}",
                        prop_name
                    ))));
                }
            }

            if let Some(end) = end_alias {
                if &prop.table_alias.0 == end {
                    // Check if this is the ID column
                    let col_raw = prop.column.raw();
                    if col_raw == "id"
                        || col_raw.starts_with("id.")
                        || col_raw.ends_with("_id")
                        || col_raw.contains(".orig_")
                        || col_raw.contains(".resp_")
                    {
                        // This is the ID column - use t.end_id directly
                        return RenderExpr::Column(Column(PropertyValue::Column(
                            "t.end_id".to_string(),
                        )));
                    }

                    // This is accessing end node property
                    let prop_name = derive_cypher_property_name(col_raw);
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.end_{}",
                        prop_name
                    ))));
                }
            }

            // Not a start or end alias - check for VLP CTE columns accessed
            // via the relationship alias (e.g., r.path_relationships → t.path_relationships)
            let col_name = prop.column.raw();
            if matches!(
                col_name,
                "path_relationships"
                    | "rel_properties"
                    | "hop_count"
                    | "path_nodes"
                    | "start_id"
                    | "end_id"
                    | "end_type"
            ) {
                return RenderExpr::Column(Column(PropertyValue::Column(format!(
                    "t.{}",
                    col_name
                ))));
            }

            // Not a VLP alias - leave unchanged
            expr.clone()
        }

        // Recursively rewrite operands in operator applications
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| {
                        rewrite_expr_for_vlp(
                            o,
                            start_alias,
                            end_alias,
                            path_variable,
                            skip_start_alias,
                        )
                    })
                    .collect(),
            })
        }

        RenderExpr::AggregateFnCall(agg) => {
            // COUNT(path_variable) → COUNT(*) since each row represents a path
            if let Some(path_var) = path_variable {
                if agg.args.len() == 1 && agg.name.to_lowercase() == "count" {
                    if let RenderExpr::TableAlias(alias) = &agg.args[0] {
                        if &alias.0 == path_var {
                            log::info!("🔧 VLP path aggregate: count({}) → count(*)", path_var);
                            return RenderExpr::AggregateFnCall(AggregateFnCall {
                                name: agg.name.clone(),
                                args: vec![RenderExpr::Star],
                            });
                        }
                    }
                }
            }
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: agg
                    .args
                    .iter()
                    .map(|a| {
                        rewrite_expr_for_vlp(
                            a,
                            start_alias,
                            end_alias,
                            path_variable,
                            skip_start_alias,
                        )
                    })
                    .collect(),
            })
        }

        RenderExpr::ColumnAlias(ColumnAlias(alias_str))
            if path_variable.as_ref() == Some(alias_str) =>
        {
            log::info!(
                "🔧 VLP path variable expansion (ColumnAlias): {} → tuple({}.path_nodes, ...)",
                alias_str,
                VLP_CTE_FROM_ALIAS,
            );
            // Expand to tuple of path components using VLP_CTE_FROM_ALIAS constant
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "tuple".to_string(),
                args: vec![
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_nodes",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_relationships",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.hop_count",
                        VLP_CTE_FROM_ALIAS
                    )))),
                ],
            })
        }

        // Handle ArraySubscript: rewrite inner expressions
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(rewrite_expr_for_vlp(
                array,
                start_alias,
                end_alias,
                path_variable,
                skip_start_alias,
            )),
            index: Box::new(rewrite_expr_for_vlp(
                index,
                start_alias,
                end_alias,
                path_variable,
                skip_start_alias,
            )),
        },

        // Handle CASE expressions - rewrite VLP references in all sub-expressions
        RenderExpr::Case(case) => {
            // Special pattern: CASE path IS NULL WHEN true THEN -1 ELSE length(path) END
            // Rewrite to: ifNull(t.hop_count, toInt64(-1))
            if let Some(ref case_expr) = case.expr {
                if is_vlp_path_is_null(case_expr, path_variable) {
                    // Use the NULL-on-empty min aggregate so we always get exactly one row:
                    // - No path: VLP CTE returns 0 rows → returns NULL → ifNull gives -1
                    // - Path exists: VLP CTE returns 1 row → returns hop_count
                    // CH: `minOrNull` is required because CH's plain `min` returns 0 for
                    // empty input; Spark's `min` already returns NULL for empty input,
                    // so the FunctionMapper resolves this name per dialect.
                    let fmap = crate::sql_generator::function_mapper::current_function_mapper();
                    let cast64 = fmap.cast_int64().to_string();
                    let min_or_null = fmap.min_or_null().to_string();
                    return RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: "ifNull".to_string(),
                        args: vec![
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: cast64.clone(),
                                args: vec![RenderExpr::AggregateFnCall(AggregateFnCall {
                                    name: min_or_null,
                                    args: vec![RenderExpr::Column(Column(PropertyValue::Column(
                                        "t.hop_count".to_string(),
                                    )))],
                                })],
                            }),
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: cast64,
                                args: vec![RenderExpr::Literal(Literal::Integer(-1))],
                            }),
                        ],
                    });
                }
            }
            // Generic case: recursively rewrite all sub-expressions
            RenderExpr::Case(RenderCase {
                expr: case.expr.as_ref().map(|e| {
                    Box::new(rewrite_expr_for_vlp(
                        e,
                        start_alias,
                        end_alias,
                        path_variable,
                        skip_start_alias,
                    ))
                }),
                when_then: case
                    .when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            rewrite_expr_for_vlp(
                                w,
                                start_alias,
                                end_alias,
                                path_variable,
                                skip_start_alias,
                            ),
                            rewrite_expr_for_vlp(
                                t,
                                start_alias,
                                end_alias,
                                path_variable,
                                skip_start_alias,
                            ),
                        )
                    })
                    .collect(),
                else_expr: case.else_expr.as_ref().map(|e| {
                    Box::new(rewrite_expr_for_vlp(
                        e,
                        start_alias,
                        end_alias,
                        path_variable,
                        skip_start_alias,
                    ))
                }),
            })
        }

        // Leave other expressions unchanged
        other => other.clone(),
    }
}

/// Check if an expression is `path IS NULL` where path is the VLP path variable
fn is_vlp_path_is_null(expr: &RenderExpr, path_variable: &Option<String>) -> bool {
    if let Some(path_var) = path_variable {
        if let RenderExpr::OperatorApplicationExp(op) = expr {
            if op.operator == Operator::IsNull && op.operands.len() == 1 {
                return matches!(&op.operands[0], RenderExpr::TableAlias(alias) if alias.0 == *path_var)
                    || matches!(&op.operands[0], RenderExpr::ColumnAlias(ColumnAlias(a)) if a == path_var);
            }
        }
    }
    false
}

/// Derive Cypher property name from database column name
///
/// ⚠️ TECHNICAL DEBT: This uses hardcoded mappings for common schema patterns.
/// This is a workaround that should eventually be replaced with schema-aware resolution.
///
/// Current mappings:
/// - full_name → name (in social_benchmark, "name" is the Cypher property, "full_name" is the DB column)
/// - email_address → email (same pattern)
/// - user_id → id (user_id is the DB column, but Cypher uses "id" for the property)
/// - object_type → type (filesystem schema)
/// - size_bytes → size (filesystem schema)
/// - owner_id → owner (filesystem schema)
///
/// TODO: Pass schema context to this function to enable schema-aware property mapping.
/// This would allow proper handling of arbitrary schema variations without hardcoding.
///
/// FUTURE: Consider caching property mapping results to improve performance for repeated queries.
fn derive_cypher_property_name(db_column: &str) -> String {
    // Common mappings for various schemas
    // Social benchmark schema
    match db_column {
        "full_name" => "name".to_string(),
        "email_address" => "email".to_string(),
        "user_id" => "id".to_string(),
        // Filesystem schema
        "object_type" => "type".to_string(),
        "size_bytes" => "size".to_string(),
        "owner_id" => "owner".to_string(),
        // Default: use the column name as-is
        _ => db_column.to_string(),
    }
}

/// Extract fixed path information from a RenderPlan by analyzing SELECT items and JOINs
/// Returns FixedPathMetadata if the plan contains a path function call that can be resolved
fn extract_fixed_path_info_from_plan(
    plan: &RenderPlan,
) -> Option<crate::render_plan::FixedPathMetadata> {
    // Look for path function calls in SELECT items
    for item in &plan.select.items {
        if let Some(path_var) = find_path_function_argument(&item.expression) {
            // Found a path function with argument path_var
            // Infer hop count from the number of JOINs
            // For a path (a)-[:T]->(b), we have 2 JOINs (relationship + end node) = 1 hop
            // For a path (a)-[:T1]->(b)-[:T2]->(c), we have 4 JOINs = 2 hops
            // Formula: hops = JOINs / 2 (integer division)
            let hop_count = plan.joins.0.len() as u32 / 2;

            log::info!(
                "🔧 Detected fixed path: path_variable={}, hop_count={} (from {} JOINs)",
                path_var,
                hop_count,
                plan.joins.0.len()
            );

            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    // Also check GROUP BY and ORDER BY expressions
    for expr in &plan.group_by.0 {
        if let Some(path_var) = find_path_function_argument(expr) {
            let hop_count = plan.joins.0.len() as u32 / 2;
            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    for item in &plan.order_by.0 {
        if let Some(path_var) = find_path_function_argument(&item.expression) {
            let hop_count = plan.joins.0.len() as u32 / 2;
            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    None
}

/// Find a path function argument (e.g., the 'p' in length(p))
/// Returns the variable name if found
fn find_path_function_argument(expr: &RenderExpr) -> Option<String> {
    match expr {
        RenderExpr::ScalarFnCall(func) => {
            // Check for path functions
            if matches!(
                func.name.to_lowercase().as_str(),
                "length" | "nodes" | "relationships"
            ) && func.args.len() == 1
            {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    return Some(alias.0.clone());
                }
            }

            // Recursively check arguments
            for arg in &func.args {
                if let Some(var) = find_path_function_argument(arg) {
                    return Some(var);
                }
            }
            None
        }

        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                if let Some(var) = find_path_function_argument(operand) {
                    return Some(var);
                }
            }
            None
        }

        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                if let Some(var) = find_path_function_argument(arg) {
                    return Some(var);
                }
            }
            None
        }

        _ => None,
    }
}

/// Rewrite path function calls for fixed (non-VLP) path patterns
/// Converts:
/// - length(p) → 1 (literal hop count)
/// - nodes(p) → array of node IDs
/// - relationships(p) → array of relationship IDs
fn rewrite_fixed_path_functions(mut plan: RenderPlan) -> RenderPlan {
    if let Some(ref fixed_path_info) = plan.fixed_path_info {
        let path_var = fixed_path_info.path_variable.clone();
        let hop_count = fixed_path_info.hop_count;

        log::info!(
            "🔧 Fixed path rewriting: path_variable={}, hop_count={}",
            path_var,
            hop_count
        );
        log::info!("🔧 SELECT has {} items", plan.select.items.len());

        // Rewrite each SELECT item's expressions
        for item in plan.select.items.iter_mut() {
            let before = format!("{:?}", item.expression);
            item.expression = rewrite_expr_for_fixed_path(&item.expression, &path_var, hop_count);
            let after = format!("{:?}", item.expression);
            if before != after {
                log::info!("🔧   Rewritten from: {} → {}", before, after);
            }
        }

        // Also rewrite GROUP BY expressions
        log::info!(
            "🔧 Fixed path GROUP BY rewriting: {} items",
            plan.group_by.0.len()
        );
        for group_expr in &mut plan.group_by.0 {
            *group_expr = rewrite_expr_for_fixed_path(group_expr, &path_var, hop_count);
        }

        // Also rewrite ORDER BY expressions
        log::info!(
            "🔧 Fixed path ORDER BY rewriting: {} items",
            plan.order_by.0.len()
        );
        for order_item in &mut plan.order_by.0 {
            order_item.expression =
                rewrite_expr_for_fixed_path(&order_item.expression, &path_var, hop_count);
        }
    }

    plan
}

/// Recursively rewrite expressions to handle path function calls on fixed paths
/// Converts:
/// - length(p) → literal(hop_count)
/// - nodes(p) → [node_ids in order] (future enhancement)
/// - relationships(p) → [rel_ids in order] (future enhancement)
fn rewrite_expr_for_fixed_path(
    expr: &RenderExpr,
    path_variable: &str,
    hop_count: u32,
) -> RenderExpr {
    match expr {
        // Handle path functions: length(p)
        RenderExpr::ScalarFnCall(func) => {
            if func.args.len() == 1 {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    if alias.0 == *path_variable {
                        match func.name.to_lowercase().as_str() {
                            "length" => {
                                log::info!(
                                    "🔧 Fixed path function: length({}) → {}",
                                    path_variable,
                                    hop_count
                                );
                                return RenderExpr::Literal(Literal::Integer(hop_count as i64));
                            }
                            "nodes" => {
                                log::debug!(
                                    "🔧 Fixed path function: nodes({}) not yet implemented for fixed paths",
                                    path_variable
                                );
                                // TODO: Return array of node IDs
                                return expr.clone();
                            }
                            "relationships" => {
                                log::debug!(
                                    "🔧 Fixed path function: relationships({}) not yet implemented for fixed paths",
                                    path_variable
                                );
                                // TODO: Return array of relationship IDs
                                return expr.clone();
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Not a path function - recursively rewrite arguments
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| rewrite_expr_for_fixed_path(a, path_variable, hop_count))
                    .collect(),
            })
        }

        // Recursively rewrite operands in operator applications
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| rewrite_expr_for_fixed_path(o, path_variable, hop_count))
                    .collect(),
            })
        }

        RenderExpr::AggregateFnCall(agg) => {
            // COUNT(path_variable) → COUNT(*) since each row represents a path
            if agg.args.len() == 1 {
                if let RenderExpr::TableAlias(alias) = &agg.args[0] {
                    if alias.0 == *path_variable && agg.name.to_lowercase() == "count" {
                        log::info!(
                            "🔧 Fixed path aggregate: count({}) → count(*)",
                            path_variable
                        );
                        return RenderExpr::AggregateFnCall(AggregateFnCall {
                            name: agg.name.clone(),
                            args: vec![RenderExpr::Star],
                        });
                    }
                }
            }
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: agg
                    .args
                    .iter()
                    .map(|a| rewrite_expr_for_fixed_path(a, path_variable, hop_count))
                    .collect(),
            })
        }

        // Leave other expressions unchanged
        other => other.clone(),
    }
}

/// Extract column references from ORDER BY expressions for UNION queries
/// Returns (original_expr, union_column_alias) pairs
/// Returns empty if any expression contains id() function (not supported in UNION)
fn extract_order_by_columns_for_union(order_by: &OrderByItems) -> Vec<(RenderExpr, String)> {
    let mut columns = Vec::new();

    for (idx, item) in order_by.0.iter().enumerate() {
        // Log unsupported expressions but still include them — don't silently drop ORDER BY
        if matches!(&item.expression, RenderExpr::ScalarFnCall(f) if f.name == "id") {
            log::warn!("⚠️  ORDER BY id() may not work correctly in UNION queries");
        }

        // Skip unresolvable "id" pseudo-property in UNION branches.
        // This arises from ORDER BY id(x) where x is an unlabeled node in a
        // multi-type pattern; the id() AST transform produces x.id but no
        // actual "id" column exists in the tables.
        if let RenderExpr::PropertyAccessExp(pa) = &item.expression {
            if pa.column.raw() == "id" {
                log::warn!(
                    "⚠️  Dropping ORDER BY {}.id from UNION (unresolvable pseudo-property)",
                    pa.table_alias.0
                );
                continue;
            }
        }

        if matches!(&item.expression, RenderExpr::PropertyAccessExp(_)) {
            log::warn!("⚠️  ORDER BY property access may not work correctly with PatternResolver UNION CTEs");
        }

        // Generate a unique alias for this ORDER BY column
        let col_alias = format!("__order_col_{}", idx);
        columns.push((item.expression.clone(), col_alias));
    }

    columns
}

/// Add ORDER BY columns to a RenderPlan's SELECT (for UNION branches)
/// For denormalized schemas, resolves virtual node property references
/// (e.g., `o.code`) to actual edge table columns (e.g., `t1.dest_code`)
/// by examining the branch's path tuple direction and schema properties.
fn add_order_by_columns_to_select(
    mut plan: RenderPlan,
    order_columns: &[(RenderExpr, String)],
) -> RenderPlan {
    use crate::render_plan::render_expr::ColumnAlias;
    use crate::render_plan::SelectItem;

    // Build context for denormalized virtual node resolution:
    // Parse the path tuple to find which aliases are start/end and the rel alias
    let path_context = extract_path_context_from_select(&plan.select);

    for (expr, alias) in order_columns {
        let resolved_expr = if let Some(ref ctx) = path_context {
            resolve_denormalized_order_by_expr(expr, ctx)
        } else {
            // No path context (e.g., standalone node UNION scan).
            // Try to resolve by matching against existing SELECT items:
            // if SELECT already has `n."id.orig_h" AS "n.ip_address"` and
            // ORDER BY is `n.ip_address`, reuse the mapped expression.
            resolve_order_by_from_existing_select(expr, &plan.select)
        };

        plan.select.items.push(SelectItem {
            expression: resolved_expr,
            col_alias: Some(ColumnAlias(alias.clone())),
        });
    }

    plan
}

/// Resolve ORDER BY expression by finding a matching SELECT item.
/// For standalone UNION scans (no path context), if ORDER BY references
/// `n.ip_address` and SELECT already has `n."id.orig_h" AS "n.ip_address"`,
/// reuse the mapped expression `n."id.orig_h"`.
fn resolve_order_by_from_existing_select(expr: &RenderExpr, select: &SelectItems) -> RenderExpr {
    if let RenderExpr::PropertyAccessExp(pa) = expr {
        let target_alias = format!("{}.{}", pa.table_alias.0, pa.column.raw());
        // Look for a SELECT item whose output alias matches this property access
        for item in &select.items {
            if let Some(ref col_alias) = item.col_alias {
                if col_alias.0 == target_alias {
                    // Found matching SELECT item — reuse its (already mapped) expression
                    log::info!(
                        "ORDER BY: Resolved {}.{} via existing SELECT alias '{}'",
                        pa.table_alias.0,
                        pa.column.raw(),
                        col_alias.0
                    );
                    return item.expression.clone();
                }
            }
        }
    }
    expr.clone()
}

/// Path context extracted from a branch's SELECT items
struct PathBranchContext {
    start_alias: String,
    end_alias: String,
    rel_alias: String,
}

/// Extract path context (start/end/rel aliases) from SELECT items' path tuple
fn extract_path_context_from_select(select: &SelectItems) -> Option<PathBranchContext> {
    for item in &select.items {
        if let Some(ref ca) = item.col_alias {
            if ca.0 == "path" {
                if let RenderExpr::ScalarFnCall(func) = &item.expression {
                    if func.name == "tuple" && func.args.len() >= 4 {
                        let get_str = |idx: usize| -> Option<String> {
                            if let RenderExpr::Literal(Literal::String(s)) = &func.args[idx] {
                                Some(s.clone())
                            } else {
                                None
                            }
                        };
                        if let (Some(start), Some(end), Some(rel)) =
                            (get_str(1), get_str(2), get_str(3))
                        {
                            return Some(PathBranchContext {
                                start_alias: start,
                                end_alias: end,
                                rel_alias: rel,
                            });
                        }
                    }
                }
            }
        }
    }
    None
}

/// Resolve denormalized virtual node references in ORDER BY expressions.
/// Maps `o.code` → `t1.dest_code` (outgoing) or `t1.origin_code` (incoming)
/// by checking node position in path and schema from_node/to_node properties.
fn resolve_denormalized_order_by_expr(expr: &RenderExpr, ctx: &PathBranchContext) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            let alias = &pa.table_alias.0;
            let prop_name = pa.column.raw();

            // Check if this alias is a virtual denormalized node (start or end of path)
            // and NOT the relationship alias (which is a real table)
            if alias == &ctx.rel_alias {
                return expr.clone(); // Real table alias, no resolution needed
            }

            // Determine if this alias is start_node or end_node
            let is_start = alias == &ctx.start_alias;
            let is_end = alias == &ctx.end_alias;

            if !is_start && !is_end {
                return expr.clone(); // Not a path node, leave as-is
            }

            // Look up denormalized property mapping from schema
            // For "id" property (from id() function transformation), resolve to node_id first
            let effective_prop_name = if prop_name == "id" {
                lookup_denorm_node_id_property().unwrap_or_else(|| prop_name.to_string())
            } else {
                prop_name.to_string()
            };

            if let Some(resolved_col) = resolve_denorm_property_from_schema(
                &effective_prop_name,
                is_start, // if start, use from_node_properties; if end, use to_node_properties
            ) {
                log::info!(
                    "🔧 ORDER BY: Resolved denorm {}.{} → {}.{}",
                    alias,
                    prop_name,
                    ctx.rel_alias,
                    resolved_col
                );
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(ctx.rel_alias.clone()),
                    column: PropertyValue::Column(resolved_col),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            // Special handling for id(alias) — resolve to the node's ID column
            if func.name.eq_ignore_ascii_case("id") && func.args.len() == 1 {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    let alias_name = &alias.0;
                    let is_start = alias_name == &ctx.start_alias;
                    let is_end = alias_name == &ctx.end_alias;
                    if is_start || is_end {
                        // Look up the node_id property name from schema, then resolve it
                        if let Some(id_prop) = lookup_denorm_node_id_property() {
                            if let Some(resolved_col) =
                                resolve_denorm_property_from_schema(&id_prop, is_start)
                            {
                                log::info!(
                                    "🔧 ORDER BY: Resolved denorm id({}) → {}.{}",
                                    alias_name,
                                    ctx.rel_alias,
                                    resolved_col
                                );
                                return RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(ctx.rel_alias.clone()),
                                    column: PropertyValue::Column(resolved_col),
                                });
                            }
                        }
                    }
                }
            }
            let new_args: Vec<_> = func
                .args
                .iter()
                .map(|a| resolve_denormalized_order_by_expr(a, ctx))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: new_args,
            })
        }
        other => other.clone(),
    }
}

/// Look up a denormalized property from the active query's schema edge definitions.
/// Uses the task-local schema; falls back to GLOBAL_SCHEMAS["default"] if no context.
/// `is_from_node`: true = look in from_node_properties, false = look in to_node_properties
fn resolve_denorm_property_from_schema(prop_name: &str, is_from_node: bool) -> Option<String> {
    use crate::server::query_context::get_current_schema;

    let schema = get_current_schema()?;

    for rel_schema in schema.get_relationships_schemas().values() {
        let props: Option<&std::collections::HashMap<String, String>> = if is_from_node {
            rel_schema.from_node_properties.as_ref()
        } else {
            rel_schema.to_node_properties.as_ref()
        };
        if let Some(prop_map) = props {
            if let Some(col_name) = prop_map.get(prop_name) {
                return Some(col_name.clone());
            }
        }
    }
    None
}

/// Look up the node_id property name from the active query's schema.
/// Uses the task-local schema; falls back to GLOBAL_SCHEMAS["default"] if no context.
/// Returns the logical property name (e.g., "code") used for id() resolution.
fn lookup_denorm_node_id_property() -> Option<String> {
    use crate::server::query_context::get_current_schema;

    let schema = get_current_schema()?;

    for node_schema in schema.all_node_schemas().values() {
        if node_schema.is_denormalized {
            let columns = node_schema.node_id.id.columns();
            if let Some(first_col) = columns.first() {
                return Some(first_col.to_string());
            }
        }
    }
    None
}

/// Recursively check if a RenderExpr contains an aggregate function call
fn render_expr_contains_aggregate(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::AggregateFnCall(_) => true,
        RenderExpr::ScalarFnCall(f) => f.args.iter().any(render_expr_contains_aggregate),
        RenderExpr::Case(c) => {
            c.when_then.iter().any(|(cond, val)| {
                render_expr_contains_aggregate(cond) || render_expr_contains_aggregate(val)
            }) || c
                .else_expr
                .as_ref()
                .is_some_and(|e| render_expr_contains_aggregate(e))
        }
        RenderExpr::OperatorApplicationExp(op) => {
            op.operands.iter().any(render_expr_contains_aggregate)
        }
        RenderExpr::List(items) => items.iter().any(render_expr_contains_aggregate),
        RenderExpr::ArraySubscript { array, index } => {
            render_expr_contains_aggregate(array) || render_expr_contains_aggregate(index)
        }
        _ => false,
    }
}

/// Recursively collect property-access SQL from aggregate function arguments,
/// including aggregates nested inside Case, ScalarFnCall, etc.
fn collect_nested_aggregate_args(expr: &RenderExpr, agg_arg_cols: &mut Vec<String>) {
    match expr {
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_property_access_sql(arg, agg_arg_cols);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_nested_aggregate_args(arg, agg_arg_cols);
            }
        }
        RenderExpr::Case(c) => {
            for (cond, val) in &c.when_then {
                collect_nested_aggregate_args(cond, agg_arg_cols);
                collect_nested_aggregate_args(val, agg_arg_cols);
            }
            if let Some(e) = &c.else_expr {
                collect_nested_aggregate_args(e, agg_arg_cols);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_nested_aggregate_args(operand, agg_arg_cols);
            }
        }
        _ => {}
    }
}

/// Path-materialization metadata column aliases.
///
/// These are constants emitted by VLP UNION branches so the Bolt result
/// transformer can reconstruct a Path. When a user query mixes path
/// projection with aggregation (`RETURN p, COUNT(*)`), these belong in
/// GROUP BY and the SELECT — Cypher's implicit grouping carries them.
/// But for `RETURN COUNT(*)` / `RETURN COUNT(p)` (no path projected,
/// no GROUP BY), they leak into the outer SELECT alongside the aggregate
/// without grouping, violating ClickHouse's "non-aggregate column not in
/// GROUP BY" rule (Code 215).
fn is_path_metadata_alias(alias: &str) -> bool {
    matches!(
        alias,
        "_rel_properties"
            | "_start_properties"
            | "_end_properties"
            | "__rel_type__"
            | "__start_label__"
            | "__end_label__"
            | "__start_id__"
            | "__end_id__"
    )
}

/// Build a SELECT clause for UNION inner branches in the aggregation case.
/// Returns (inner_select_sql, agg_arg_columns) where agg_arg_columns lists
/// the SQL text of property-access expressions extracted from aggregate arguments.
/// The outer SELECT should backtick-escape these references in its aggregates.
///
/// When `drop_path_metadata` is true, path-materialization metadata aliases
/// (constants emitted by VLP branches for Bolt path reconstruction) are
/// excluded from the inner SELECT. This is set when the outer aggregate has
/// no GROUP BY — in that case the metadata columns would trip Code 215.
/// For `RETURN p, COUNT(*)` (implicit grouping by `p`), GROUP BY is non-empty
/// and the metadata columns must survive so the path can be rebuilt.
fn build_union_inner_select(
    select: &SelectItems,
    drop_path_metadata: bool,
) -> (String, Vec<String>) {
    let non_agg_items: Vec<&SelectItem> = select
        .items
        .iter()
        .filter(|item| {
            if render_expr_contains_aggregate(&item.expression) {
                return false;
            }
            // Skip ALL __order_col items: ORDER BY is handled by outer query
            if let Some(alias) = &item.col_alias {
                if alias.0.starts_with("__order_col") {
                    return false;
                }
                if drop_path_metadata && is_path_metadata_alias(&alias.0) {
                    return false;
                }
            }
            true
        })
        .collect();

    // Extract property-access expressions from aggregate arguments (recursively)
    let mut agg_arg_cols: Vec<String> = Vec::new();
    for item in &select.items {
        collect_nested_aggregate_args(&item.expression, &mut agg_arg_cols);
    }
    agg_arg_cols.sort();
    agg_arg_cols.dedup();

    // Remove agg_arg_cols that are already covered by non_agg_items.
    // Two coverage forms:
    // 1. Expression-matches-alias: e.g., `tag.name AS "tag.name"` — alias == expression SQL.
    //    The outer aggregate uses the alias, which equals the original expression.
    // 2. Alias-matches-col: e.g., `n.Origin AS "n.code"` covers agg_arg_col "n.code".
    //    The non-agg item already exposes the value under the exact alias the outer
    //    aggregate references, so no redundant `n.code AS "n.code"` should be added
    //    (which would fail if n.code doesn't exist as a DB column).
    let existing_with_matching_alias: std::collections::HashSet<String> = non_agg_items
        .iter()
        .filter_map(|item| {
            let expr_sql = item.expression.to_sql();
            // Item covers the agg_arg_col if BOTH expression matches AND
            // alias matches the dotted form (or no alias = expression IS the alias)
            if let Some(ref alias) = item.col_alias {
                if alias.0 == expr_sql {
                    Some(expr_sql) // alias matches expression (e.g., tag.name AS "tag.name")
                } else {
                    None // alias differs — check by alias below
                }
            } else {
                Some(expr_sql) // no alias — expression is the column name
            }
        })
        .collect();
    // Collect aliases from non-agg items to cover mapped-property cases like
    // `n.Origin AS "n.code"` which exposes "n.code" under the correct alias.
    let existing_by_alias: std::collections::HashSet<String> = non_agg_items
        .iter()
        .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
        .collect();
    agg_arg_cols.retain(|col| {
        !existing_with_matching_alias.contains(col) && !existing_by_alias.contains(col)
    });

    if non_agg_items.is_empty() && agg_arg_cols.is_empty() {
        return ("SELECT 1 AS __dummy\n".to_string(), vec![]);
    }

    let mut sql = if select.distinct {
        "SELECT DISTINCT \n".to_string()
    } else {
        "SELECT \n".to_string()
    };

    let total_items = non_agg_items.len() + agg_arg_cols.len();
    let mut idx = 0;

    for item in &non_agg_items {
        sql.push_str("      ");
        sql.push_str(&item.expression.to_sql());
        if let Some(alias) = &item.col_alias {
            sql.push_str(&format!(
                " AS {}",
                crate::sql_generator::function_mapper::current_function_mapper()
                    .quote_alias(&alias.0)
            ));
        }
        idx += 1;
        if idx < total_items {
            sql.push(',');
        }
        sql.push('\n');
    }

    // Add aggregate argument columns with their SQL as alias.
    // For qualified refs like "n.code" where the property part ("code") already
    // has a non-agg item with that unqualified alias (e.g., n.Origin AS "code"),
    // use the mapped DB column expression instead of the Cypher property name.
    // This fixes denormalized schemas where DB column ≠ Cypher property name
    // (e.g., Airport.code → flights.Origin/Dest).
    for col_sql in &agg_arg_cols {
        let expr_sql = if let Some(dot_pos) = col_sql.rfind('.') {
            let property_part = &col_sql[dot_pos + 1..];
            non_agg_items
                .iter()
                .find(|i| i.col_alias.as_ref().is_some_and(|a| a.0 == property_part))
                .map(|item| item.expression.to_sql())
                .unwrap_or_else(|| col_sql.clone())
        } else {
            col_sql.clone()
        };
        sql.push_str(&format!(
            "      {} AS {}",
            expr_sql,
            crate::sql_generator::function_mapper::current_function_mapper().quote_alias(col_sql)
        ));
        idx += 1;
        if idx < total_items {
            sql.push(',');
        }
        sql.push('\n');
    }

    (sql, agg_arg_cols)
}

/// Recursively collect property-access expression SQL from a RenderExpr tree.
fn collect_property_access_sql(expr: &RenderExpr, out: &mut Vec<String>) {
    match expr {
        RenderExpr::PropertyAccessExp(_) => {
            out.push(expr.to_sql());
        }
        // After VLP rewriting, PropertyAccessExp may become Column("t.end_id")
        // or remain as TableAlias("u2"). These need to be included in the inner
        // SELECT so the outer aggregate can reference them.
        RenderExpr::Column(col) => {
            let col_str = col.raw();
            // Only include qualified column references (e.g., t.end_id), not bare column names
            if col_str.contains('.') {
                out.push(col_str.to_string());
            }
        }
        RenderExpr::TableAlias(alias) => {
            // Bare node references in aggregates (e.g., COUNT(DISTINCT u2))
            // need the node's ID column in the inner SELECT
            out.push(alias.0.clone());
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_property_access_sql(operand, out);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_property_access_sql(arg, out);
            }
        }
        _ => {}
    }
}

/// Build the outer SELECT for UNION with aggregation.
///
/// Non-aggregate items reference their inner-branch alias via backticks.
/// Aggregate items rewrite property-access arguments to backtick-escaped
/// column aliases so they reference the inner projection.
///
/// `drop_path_metadata` mirrors [`build_union_inner_select`] — set when the
/// outer aggregate has no GROUP BY, so VLP path-materialization constants
/// don't leak into the outer projection and trigger Code 215.
fn build_outer_aggregate_select(
    select: &SelectItems,
    agg_arg_cols: &[String],
    drop_path_metadata: bool,
) -> String {
    // Build expression→alias map for non-aggregate SELECT items.
    // This maps raw expression SQL (e.g., "n.answers") to the output alias
    // (e.g., "n.resolved_ip") so aggregate expressions can reference the correct
    // UNION output column when the raw DB column name differs from the alias.
    let expr_to_alias: std::collections::BTreeMap<String, String> = select
        .items
        .iter()
        .filter(|item| !render_expr_contains_aggregate(&item.expression))
        .filter_map(|item| {
            item.col_alias
                .as_ref()
                .map(|a| (item.expression.to_sql(), a.0.clone()))
        })
        .collect();

    let items: Vec<String> = select
        .items
        .iter()
        .filter(|item| {
            if let Some(alias) = &item.col_alias {
                if alias.0.starts_with("__order_col") {
                    return false;
                }
                if drop_path_metadata && is_path_metadata_alias(&alias.0) {
                    return false;
                }
            }
            true
        })
        .map(|item| {
            let alias_str = item
                .col_alias
                .as_ref()
                .map(|a| a.0.clone())
                .unwrap_or_else(|| "result".to_string());
            if render_expr_contains_aggregate(&item.expression) {
                let mut agg_sql = item.expression.to_sql();
                // First, rewrite column references that are covered by non-agg
                // SELECT items with different aliases (e.g., n.answers → n.resolved_ip)
                for (expr_sql, col_alias) in &expr_to_alias {
                    if expr_sql != col_alias && agg_sql.contains(expr_sql.as_str()) {
                        agg_sql = agg_sql.replace(expr_sql, &format!("`{}`", col_alias));
                    }
                }
                // Handle agg_arg_cols: columns that aggregates reference.
                // Items in agg_arg_cols are projected verbatim as `col_ref AS "col_ref"`
                // by the inner SELECT, so they are always available under their own name.
                // Just backtick-wrap them for the outer aggregate reference.
                for col_ref in agg_arg_cols {
                    if agg_sql.contains(col_ref.as_str()) {
                        agg_sql = agg_sql.replace(col_ref, &format!("`{}`", col_ref));
                    }
                }
                format!(
                    "{} AS {}",
                    agg_sql,
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .quote_alias(&alias_str)
                )
            } else {
                format!(
                    "`{}` AS {}",
                    alias_str,
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .quote_alias(&alias_str)
                )
            }
        })
        .collect();
    items.join(", ")
}

/// Build GROUP BY clause with aliased column references for UNION subqueries.
///
/// Maps each GROUP BY expression to its SELECT column alias (backtick-escaped)
/// when available, falling back to the raw expression otherwise.
fn build_aliased_group_by(group_by: &GroupByExpressions, select: &SelectItems) -> String {
    if group_by.0.is_empty() {
        return String::new();
    }
    let expr_to_alias: std::collections::HashMap<String, String> = select
        .items
        .iter()
        .filter_map(|item| {
            item.col_alias
                .as_ref()
                .map(|a| (item.expression.to_sql(), a.0.clone()))
        })
        .collect();

    let mut sql = "GROUP BY ".to_string();
    for (i, expr) in group_by.0.iter().enumerate() {
        let expr_sql = RenderExpr::to_sql(expr);
        if let Some(alias) = expr_to_alias.get(&expr_sql) {
            sql.push_str(&format!("`{}`", alias));
        } else {
            sql.push_str(&expr_sql);
        }
        if i + 1 < group_by.0.len() {
            sql.push_str(", ");
        }
    }
    sql.push('\n');
    sql
}

/// Build alias→label map from a SQL scope's FROM clause and JOINs.
/// Maps each SQL alias to the graph node label that owns the underlying table.
/// This is ground truth: if `b` joins as `social.users`, then `b → User`.
fn build_alias_label_map_from_scope(
    from: &FromTableItem,
    joins: &JoinItems,
) -> HashMap<String, String> {
    use crate::server::query_context::get_current_schema;
    let schema = match get_current_schema() {
        Some(s) => s,
        None => return HashMap::new(),
    };
    let mut table_to_label: HashMap<String, String> = HashMap::new();
    for (label, ns) in schema.all_node_schemas() {
        let qualified = format!("{}.{}", ns.database, ns.table_name);
        table_to_label
            .entry(qualified)
            .or_insert_with(|| label.clone());
    }
    let mut map = HashMap::new();
    if let Some(ref vtr) = from.0 {
        if let Some(ref alias) = vtr.alias {
            if let Some(label) = table_to_label.get(&vtr.name) {
                map.insert(alias.clone(), label.clone());
            }
        }
    }
    for join in &joins.0 {
        if let Some(label) = table_to_label.get(&join.table_name) {
            map.insert(join.table_alias.clone(), label.clone());
        }
    }
    map
}

/// Activate branch-local rendering context for a SQL scope.
///
/// Must be called (preceded by snapshot_branch_context()) at EVERY SQL branch boundary:
/// - Each UNION branch (before rendering its FROM/JOINs/filters)
/// - Each CTE body (before rendering its FROM/JOINs/filters)
/// - The outer SELECT (after CTEs have rendered, before outer FROM/JOINs)
///
/// Two context fields are scoped to this branch:
///
/// 1. `alias_label_map` — rebuilt from this scope's actual FROM/JOIN table names.
///    Ground truth for `n.id` pseudo-property resolution. Prevents stale VLP-context
///    labels (e.g., `b → Post`) from leaking into non-VLP branches (where `b → User`).
///
/// 2. `multi_type_vlp_aliases` — filtered to only aliases VLP-backed in this scope.
///    An alias is VLP-backed only if its table name starts with `vlp_` in this scope's
///    FROM or JOINs. Prevents JSON_VALUE property rewriting from leaking into branches
///    where the alias references a direct node table, not a VLP CTE.
///
/// These two invariants together ensure each SQL branch gets correct property resolution
/// regardless of what other branches in the same query plan.
fn activate_scope_context(from: &FromTableItem, joins: &JoinItems) {
    // 1. Rebuild alias_label_map from this scope's actual FROM/JOIN table names.
    let alias_label_map = build_alias_label_map_from_scope(from, joins);
    set_alias_label_map(alias_label_map);

    // 2. Filter multi_type_vlp_aliases to only aliases that are VLP-backed in this scope.
    let vlp_backed = vlp_backed_aliases_from_from_joins(from, joins);
    let full_vlp = crate::server::query_context::get_multi_type_vlp_aliases();
    let scoped_vlp: HashMap<String, String> = full_vlp
        .into_iter()
        .filter(|(k, _)| vlp_backed.contains(k.as_str()))
        .collect();
    set_multi_type_vlp_aliases(scoped_vlp);
}

/// Returns VLP-backed aliases from explicit FROM + JOINs (shared by branch and outer-plan rendering).
fn vlp_backed_aliases_from_from_joins(from: &FromTableItem, joins: &JoinItems) -> HashSet<String> {
    let mut vlp_backed = HashSet::new();
    if let Some(ref vtr) = from.0 {
        if vtr.name.starts_with("vlp_") {
            if let Some(ref alias) = vtr.alias {
                vlp_backed.insert(alias.clone());
            }
        }
    }
    for join in &joins.0 {
        if join.table_name.starts_with("vlp_") {
            vlp_backed.insert(join.table_alias.clone());
        }
    }
    vlp_backed
}

fn render_union_branch_sql(branch: &RenderPlan) -> String {
    // Save branch-scoped context and activate for this branch's FROM/JOINs.
    // Each UNION branch gets its own isolated context so VLP aliases from one
    // branch (e.g., AUTHORED/LIKED) don't contaminate another (e.g., FOLLOWS).
    let snapshot = snapshot_branch_context();
    activate_scope_context(&branch.from, &branch.joins);

    let has_inner_union = branch.union.0.is_some();
    let has_limit = branch.limit.0.is_some();
    let has_skip = branch.skip.0.is_some();
    let has_order_by = !branch.order_by.0.is_empty();

    let bsql = if !has_inner_union && !has_limit && !has_skip && !has_order_by {
        // Simple branch: select + from + joins + filters
        let mut bsql = String::new();
        bsql.push_str(&branch.select.to_sql());
        bsql.push_str(&branch.from.to_sql());
        bsql.push_str(&branch.joins.to_sql());
        bsql.push_str(&branch.filters.to_sql());
        bsql
    } else {
        // Complex branch: wrap in subselect to preserve inner union/limit semantics
        let mut bsql = String::new();
        bsql.push_str("SELECT * FROM (\n");

        // First inner branch
        bsql.push_str(&branch.select.to_sql());
        bsql.push_str(&branch.from.to_sql());
        bsql.push_str(&branch.joins.to_sql());
        bsql.push_str(&branch.filters.to_sql());

        // Inner union branches
        if let Some(inner_union) = &branch.union.0 {
            let inner_union_type = match inner_union.union_type {
                UnionType::Distinct => "UNION DISTINCT \n",
                UnionType::All => "UNION ALL \n",
            };
            for inner_branch in &inner_union.input {
                bsql.push_str(inner_union_type);
                bsql.push_str(&render_union_branch_sql(inner_branch));
            }
        }

        bsql.push_str(")\n");

        // Add ORDER BY, LIMIT, SKIP
        if has_order_by {
            bsql.push_str(&branch.order_by.to_sql());
        }
        let clause = limit_offset_clause(branch.skip.0, branch.limit.0);
        if !clause.is_empty() {
            bsql.push_str(&clause);
            bsql.push('\n');
        }

        bsql
    };

    // Restore context for the parent scope.
    restore_branch_context(snapshot);

    bsql
}

/// Ensure a table name has a database prefix for base table references.
/// CTE references (names starting with `with_`, `vlp_`, `pattern_`, `rel_`, `__`)
/// are returned as-is. Base table names that are missing the `db.` prefix get it
/// by looking up the table in the current schema's node/relationship definitions.
fn ensure_database_prefix(table_name: &str) -> String {
    // Already has database prefix
    if table_name.contains('.') {
        return table_name.to_string();
    }

    // CTE references don't need database prefix
    if table_name.starts_with("with_")
        || table_name.starts_with("vlp_")
        || table_name.starts_with("pattern_")
        || table_name.starts_with("rel_")
        || table_name.starts_with("__")
        || table_name.starts_with("multi_type_vlp")
    {
        return table_name.to_string();
    }

    // Look up the table in the schema to find its database
    if let Some(schema) = crate::server::query_context::get_current_schema_with_fallback() {
        // Search node schemas for a matching table_name
        for node_schema in schema.all_node_schemas().values() {
            if node_schema.table_name == table_name && !node_schema.database.is_empty() {
                log::debug!(
                    "🔧 ensure_database_prefix: '{}' → '{}.{}' (from node schema)",
                    table_name,
                    node_schema.database,
                    table_name
                );
                return format!("{}.{}", node_schema.database, table_name);
            }
        }
        // Search relationship schemas for a matching table_name
        for rel_schema in schema.get_relationships_schemas().values() {
            if rel_schema.table_name == table_name && !rel_schema.database.is_empty() {
                log::debug!(
                    "🔧 ensure_database_prefix: '{}' → '{}.{}' (from relationship schema)",
                    table_name,
                    rel_schema.database,
                    table_name
                );
                return format!("{}.{}", rel_schema.database, table_name);
            }
        }
    }

    // Fallback: return as-is
    table_name.to_string()
}

/// Per-VLP-CTE alias info: `cte_name → (start_alias, end_alias, path_variable)`.
type VlpAliasInfo =
    std::collections::HashMap<String, (Option<String>, Option<String>, Option<String>)>;

/// Rewrite VLP variable references inside CTE bodies.
///
/// When a WITH CTE body references a VLP CTE (e.g., FROM vlp_person_friend),
/// its WHERE and JOIN expressions may still use original Cypher variable names
/// (e.g., friend.id, person.id). This rewrites them to VLP column names
/// (e.g., t.end_id, t.start_id).
///
/// For undirected VLP (base FROM + union branches), also clones filters and
/// JOINs to each union branch, rewriting with the correct VLP alias mapping
/// for that direction.
fn rewrite_vlp_in_cte_bodies(plan: &mut RenderPlan) {
    // Collect VLP CTE alias info: cte_name → (start_alias, end_alias, path_variable)
    let vlp_info: VlpAliasInfo = plan
        .ctes
        .0
        .iter()
        .filter(|cte| cte.vlp_cypher_start_alias.is_some())
        .map(|cte| {
            (
                cte.cte_name.clone(),
                (
                    cte.vlp_cypher_start_alias.clone(),
                    cte.vlp_cypher_end_alias.clone(),
                    cte.vlp_path_variable.clone(),
                ),
            )
        })
        .collect();

    if vlp_info.is_empty() {
        return;
    }

    // Process each Structured CTE body
    for cte in &mut plan.ctes.0 {
        if let CteContent::Structured(ref mut inner) = cte.content {
            rewrite_cte_body_vlp_refs(inner, &vlp_info);
        }
    }

    // Also process the outer plan itself — when an undirected VLP generates UNION
    // branches in the outer query, the chained JOINs and filters from the base plan
    // must be cloned to each branch (same logic as CTE bodies).
    rewrite_cte_body_vlp_refs(plan, &vlp_info);
}

/// Rewrite VLP references in a single CTE body's RenderPlan.
/// If the body's FROM is a VLP CTE, rewrites filters and JOIN conditions.
/// For undirected VLP (with union branches), clones filters/JOINs to each branch.
fn rewrite_cte_body_vlp_refs(plan: &mut RenderPlan, vlp_info: &VlpAliasInfo) {
    let from_name = match plan.from.0.as_ref() {
        Some(f) => f.name.clone(),
        None => return,
    };

    let forward_aliases = match vlp_info.get(&from_name) {
        Some(aliases) => aliases.clone(),
        None => return,
    };

    // Save original filters and joins before rewriting (needed for cloning to reverse branches)
    let original_filters = plan.filters.0.clone();
    let original_joins = plan.joins.0.clone();

    // Rewrite forward branch's filters
    if let Some(ref filter) = original_filters {
        plan.filters = FilterItems(Some(rewrite_expr_for_vlp(
            filter,
            &forward_aliases.0,
            &forward_aliases.1,
            &forward_aliases.2,
            false,
        )));
    }

    // Rewrite forward branch's JOIN conditions
    rewrite_joins_for_vlp(&mut plan.joins.0, &forward_aliases);

    // For undirected VLP: clone filters and JOINs to each reverse union branch
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            let branch_from_name = match branch.from.0.as_ref() {
                Some(f) => f.name.clone(),
                None => continue,
            };
            let reverse_aliases = match vlp_info.get(&branch_from_name) {
                Some(aliases) => aliases.clone(),
                None => continue,
            };

            // Clone and rewrite filters for reverse branch
            if let Some(ref filter) = original_filters {
                branch.filters = FilterItems(Some(rewrite_expr_for_vlp(
                    filter,
                    &reverse_aliases.0,
                    &reverse_aliases.1,
                    &reverse_aliases.2,
                    false,
                )));
            }

            // Clone and rewrite JOINs for reverse branch
            if !original_joins.is_empty() {
                branch.joins = JoinItems(original_joins.clone());
                rewrite_joins_for_vlp(&mut branch.joins.0, &reverse_aliases);
            }
        }
    }
}

/// Rewrite JOIN conditions using VLP alias mappings.
fn rewrite_joins_for_vlp(
    joins: &mut [Join],
    aliases: &(Option<String>, Option<String>, Option<String>),
) {
    for join in joins.iter_mut() {
        for cond in &mut join.joining_on {
            for operand in &mut cond.operands {
                *operand = rewrite_expr_for_vlp(operand, &aliases.0, &aliases.1, &aliases.2, false);
            }
        }
        if let Some(ref filter) = join.pre_filter {
            join.pre_filter = Some(rewrite_expr_for_vlp(
                filter, &aliases.0, &aliases.1, &aliases.2, false,
            ));
        }
    }
}

/// Swap `t.start_*` ↔ `t.end_*` column references in a SQL string.
/// Used for reverse VLP UNION branches where the direction is swapped.
/// Uses placeholder-based approach to avoid double-swap issues.
fn swap_vlp_start_end(sql: &str) -> String {
    // Phase 1: Replace all t.start_* with placeholder
    let placeholder = "__VLP_SWAP_PLACEHOLDER_";
    let result = sql.replace("t.start_", &format!("{}start_", placeholder));
    // Phase 2: Replace all t.end_* with t.start_*
    let result = result.replace("t.end_", "t.start_");
    // Phase 3: Replace placeholders with t.end_*
    result.replace(&format!("{}start_", placeholder), "t.end_")
}

/// Recursively collect all CTE definitions from a RenderPlan tree,
/// removing them from their nested locations (union branches, CTE content, etc.).
fn collect_nested_ctes(plan: &mut RenderPlan, collected: &mut Vec<Cte>) {
    // Take CTEs from this plan level
    let ctes = std::mem::take(&mut plan.ctes.0);
    for mut cte in ctes {
        // Recursively flatten CTEs inside Structured CTE content
        if let CteContent::Structured(ref mut inner_plan) = cte.content {
            collect_nested_ctes(inner_plan, collected);
        }
        collected.push(cte);
    }

    // Recurse into union branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            collect_nested_ctes(branch, collected);
        }
    }
}

/// Flatten all CTEs from the entire RenderPlan tree to the top level.
/// After this call, `plan.ctes` contains ALL CTEs in sequential dependency order
/// and no nested CTEs remain anywhere.
///
/// `collect_nested_ctes` walks depth-first: inner CTEs (dependencies) are collected
/// before the outer CTEs that reference them. This naturally produces the correct
/// dependency order — no additional sorting needed.
fn flatten_all_ctes(plan: &mut RenderPlan) {
    let mut collected = Vec::new();
    collect_nested_ctes(plan, &mut collected);

    if collected.is_empty() {
        return;
    }

    // Deduplicate by name (keep first occurrence — the dependency-order one)
    let mut seen = std::collections::HashSet::new();
    collected.retain(|cte| seen.insert(cte.cte_name.clone()));

    plan.ctes.0 = collected;
}

pub fn render_plan_to_sql(mut plan: RenderPlan, _max_cte_depth: u32) -> String {
    log::trace!(
        "render_plan_to_sql: from={:?}, joins={}, union={}, ctes={}",
        plan.from.0.as_ref().map(|f| &f.name),
        plan.joins.0.len(),
        plan.union.0.is_some(),
        plan.ctes.0.len()
    );
    // STEP 0: Flatten ALL CTEs to top level in dependency order.
    // CTEs are always a flat, linear chain — never nested inside other CTEs or union branches.
    flatten_all_ctes(&mut plan);

    // STEP 0.5: Rewrite VLP variable references inside CTE bodies.
    // When a WITH CTE body reads FROM a VLP CTE, its WHERE/JOIN expressions may still
    // use original Cypher variable names (e.g., friend.id). Rewrite them to VLP column
    // names (e.g., t.end_id). For undirected VLP, also clone filters/JOINs to reverse branches.
    rewrite_vlp_in_cte_bodies(&mut plan);

    // Extract fixed path information if not already set
    // This looks at the RenderPlan structure to infer path variable info
    if plan.fixed_path_info.is_none() {
        plan.fixed_path_info = extract_fixed_path_info_from_plan(&plan);
    }

    // Rewrite VLP SELECT aliases before SQL generation
    // Maps Cypher aliases (a, b) to CTE column prefixes (start_, end_)
    plan = rewrite_vlp_select_aliases(plan);

    // 🔧 CRITICAL FIX: Sort JOINs by dependency to ensure correct SQL ordering
    // Topological sort ensures that if JOIN A references table B in its ON clause,
    // then B appears before A in the FROM/JOIN sequence.
    //
    // This prevents errors like: "Unknown identifier t1" when t1 is used before defined.
    // The sort function existed but was never called - this fixes it once for all queries!
    //
    // Root cause: JOINs were generated in arbitrary order during planning, but SQL
    // requires strict dependency order. This fix applies topological sorting centrally.
    plan.joins.0 = {
        use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
        use crate::render_plan::FromTable;

        // Convert plan.from to the format expected by sort_joins_by_dependency
        let from_table = plan.from.0.as_ref().map(|table_ref| FromTable {
            table: Some(table_ref.clone()),
            joins: vec![],
        });

        sort_joins_by_dependency(plan.joins.0, from_table.as_ref())
    };
    log::trace!(
        "render_plan_to_sql after sort_joins: joins={}",
        plan.joins.0.len()
    );

    // Also sort JOINs in UNION branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
            use crate::render_plan::FromTable;

            let from_table = branch.from.0.as_ref().map(|table_ref| FromTable {
                table: Some(table_ref.clone()),
                joins: vec![],
            });
            branch.joins.0 =
                sort_joins_by_dependency(std::mem::take(&mut branch.joins.0), from_table.as_ref());
        }
    }

    // Also sort JOINs inside CTE plans (WITH clause CTEs have their own JOINs)
    for cte in plan.ctes.0.iter_mut() {
        if let CteContent::Structured(ref mut cte_plan) = cte.content {
            use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
            use crate::render_plan::FromTable;

            let from_table = cte_plan.from.0.as_ref().map(|table_ref| FromTable {
                table: Some(table_ref.clone()),
                joins: vec![],
            });
            cte_plan.joins.0 = sort_joins_by_dependency(
                std::mem::take(&mut cte_plan.joins.0),
                from_table.as_ref(),
            );

            // Sort UNION branch JOINs inside CTEs too
            if let Some(ref mut union) = cte_plan.union.0 {
                for branch in union.input.iter_mut() {
                    let branch_from = branch.from.0.as_ref().map(|table_ref| FromTable {
                        table: Some(table_ref.clone()),
                        joins: vec![],
                    });
                    branch.joins.0 = sort_joins_by_dependency(
                        std::mem::take(&mut branch.joins.0),
                        branch_from.as_ref(),
                    );
                }
            }
        }
    }

    // STEP: Post-hoc plan optimizations
    // 1. Dead CTE elimination — removes CTEs never referenced downstream
    // 2. VLP column pruning — removes unused property columns from recursive VLP CTEs
    // 3. CTE column pruning — backward dataflow removes unused carry-forward columns
    // 4. Unreferenced join elimination — removes JOINs whose alias is unused
    // 5. Bridge node elimination — removes FK-bridge node JOINs, rewrites ON conditions
    crate::render_plan::plan_optimizer::optimize_plan(&mut plan);

    // Rewrite path function calls for fixed (non-VLP) path patterns
    // Converts length(p) → hop_count, etc.
    plan = rewrite_fixed_path_functions(plan);

    // Build ALL rendering contexts (CTE registry, relationship columns, CTE mappings, multi-type aliases)
    let relationship_columns = build_relationship_columns_from_plan(&plan);
    let cte_mappings = build_cte_property_mappings(&plan);
    let multi_type_aliases = build_multi_type_vlp_aliases(&plan);

    // Collect all CTE names for scope-specific alias resolution in Cte::to_sql()
    let all_cte_names: HashSet<String> = plan.ctes.0.iter().map(|c| c.cte_name.clone()).collect();

    // Build main plan's CTE alias mapping
    let main_plan_alias_mapping = build_cte_alias_mapping_for_scope(&plan, &all_cte_names);

    // TASK-LOCAL: Set ALL contexts for this async task's rendering context
    set_all_render_contexts(
        relationship_columns,
        cte_mappings,
        multi_type_aliases,
        main_plan_alias_mapping,
    );
    // Store all CTE names for per-scope mapping in Cte::to_sql()
    crate::server::query_context::set_all_cte_names(all_cte_names);

    // Set the variable registry from the outer render plan for property resolution
    if let Some(ref registry) = plan.variable_registry {
        crate::server::query_context::set_current_variable_registry(registry.clone());
    }

    // Activate outer scope context: rebuild alias_label_map and scope multi_type_vlp_aliases
    // to this plan's FROM/JOINs. CTE bodies will snapshot/restore around their own context.
    activate_scope_context(&plan.from, &plan.joins);

    // Disambiguate duplicate SELECT aliases. When multiple nodes share property
    // names (creationDate, id), the inner SELECT has duplicate aliases which
    // chdb rejects (Code 179). Suffix duplicates with _2, _3, etc.
    // The outer SELECT references specific named aliases (personId, etc.)
    // which are never duplicated — only the node property expansions collide.
    {
        fn disambiguate_select_aliases(items: &mut [crate::render_plan::SelectItem]) {
            let mut counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for item in items.iter_mut() {
                if let Some(ref mut alias) = item.col_alias {
                    let count = counts.entry(alias.0.clone()).or_insert(0);
                    *count += 1;
                    if *count > 1 {
                        alias.0 = format!("{}_{}", alias.0, count);
                    }
                }
            }
        }
        disambiguate_select_aliases(&mut plan.select.items);
        if let Some(ref mut union) = plan.union.0 {
            for branch in &mut union.input {
                disambiguate_select_aliases(&mut branch.select.items);
            }
        }
    }

    // Spark/Databricks: inline WHERE references to same-scope SELECT aliases.
    // Runs LAST — after every VLP/undirected/disambiguation rewrite — so each
    // scope's SELECT projection is final and its alias map reflects exactly what
    // the branch will emit (the reverse undirected arm now binds `friend`, not a
    // stale whole-node `person`). ClickHouse path is untouched (gated).
    inline_where_alias_refs_for_spark(&mut plan);

    // Record which CTE columns are array/collection-valued so the Databricks
    // `size()` render can pick Spark `size` vs `length` (see databricks_size_name).
    // `render_plan_to_sql` is re-entrant (scalar-subquery RenderExprs render their
    // own sub-plan), so save the parent's set and restore it on every exit — a
    // nested sub-plan must not clobber the outer scope's array columns.
    struct ArrayColsGuard(HashSet<String>);
    impl Drop for ArrayColsGuard {
        fn drop(&mut self) {
            crate::server::query_context::set_array_cte_columns(std::mem::take(&mut self.0));
        }
    }
    let _array_cols_guard = ArrayColsGuard(crate::server::query_context::get_array_cte_columns());
    crate::server::query_context::set_array_cte_columns(collect_array_cte_columns(&plan));

    let mut sql = String::new();

    // If there's a Union, wrap it in a subquery for correct ClickHouse behavior.
    // ClickHouse has a quirk where LIMIT/ORDER BY on bare UNION ALL only applies to
    // the last branch, not the combined result. Wrapping in a subquery fixes this.
    if plan.union.0.is_some() {
        sql.push_str(&plan.ctes.to_sql());

        // Extract ORDER BY columns that need to be added to UNION branches
        let order_by_columns = if !plan.order_by.0.is_empty() {
            extract_order_by_columns_for_union(&plan.order_by)
        } else {
            Vec::new()
        };

        // If we have ORDER BY, add those columns to all UNION branches
        let mut modified_plan = plan.clone();
        if !order_by_columns.is_empty() {
            log::info!(
                "🔄 UNION with ORDER BY: Adding {} ordering columns to branches",
                order_by_columns.len()
            );

            // Add to the first branch (which is the base plan)
            modified_plan = add_order_by_columns_to_select(modified_plan, &order_by_columns);

            // Add to remaining branches
            if let Some(union) = &mut modified_plan.union.0 {
                union.input = union
                    .input
                    .iter()
                    .map(|branch| add_order_by_columns_to_select(branch.clone(), &order_by_columns))
                    .collect();
            }
        }

        // Use the modified plan for SQL generation
        plan = modified_plan;

        // Check if SELECT items contain aggregation (e.g., count(*), sum(), etc.)
        // Uses recursive check to detect aggregates nested in CASE, function calls, etc.
        let has_aggregation = plan
            .select
            .items
            .iter()
            .any(|item| render_expr_contains_aggregate(&item.expression));

        // Aggregate without GROUP BY = no implicit grouping by path → VLP
        // path-materialization metadata constants must not appear in the
        // inner/outer SELECT (would trip ClickHouse Code 215). With GROUP BY,
        // the user is grouping (e.g. `RETURN p, COUNT(*)`) and metadata
        // columns must survive so Bolt can reconstruct the path.
        let drop_path_metadata = has_aggregation && plan.group_by.0.is_empty();

        // Pre-compute inner SELECT and aggregate arg columns for aggregation+UNION case
        let (inner_select_sql, agg_arg_cols) = if has_aggregation {
            let (sql, cols) = build_union_inner_select(&plan.select, drop_path_metadata);
            (Some(sql), cols)
        } else {
            (None, vec![])
        };

        log::debug!(
            "UNION rendering: has_aggregation={}, select_items={}, agg_arg_cols={:?}",
            has_aggregation,
            plan.select.items.len(),
            agg_arg_cols
        );
        // Check if we need the subquery wrapper (when there's ORDER BY, LIMIT, GROUP BY, or aggregation)
        let needs_subquery = !plan.order_by.0.is_empty()
            || plan.limit.0.is_some()
            || plan.skip.0.is_some()
            || !plan.group_by.0.is_empty()
            || has_aggregation;

        log::debug!("UNION rendering: needs_subquery={}", needs_subquery);

        if needs_subquery {
            // Wrap UNION in a subquery
            // If there are specific SELECT items (aggregation case), use them
            // Otherwise default to SELECT *
            // For UNION with ordering/limiting, wrap in subquery and apply ORDER BY/LIMIT to outer query
            sql.push_str("SELECT ");

            if let Some(_union) = &plan.union.0 {
                if has_aggregation {
                    // Collect aggregate aliases to detect dependent order columns
                    let _agg_aliases: std::collections::HashSet<String> = plan
                        .select
                        .items
                        .iter()
                        .filter(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                        .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                        .collect();

                    sql.push_str(&build_outer_aggregate_select(
                        &plan.select,
                        &agg_arg_cols,
                        drop_path_metadata,
                    ));
                } else {
                    // Without aggregation: select column aliases from the subquery.
                    // Exclude ORDER BY helper columns (__order_col_N) from the outer
                    // SELECT — they exist in the __union subquery for ORDER BY use,
                    // but must not appear as result columns.
                    let alias_select = plan
                        .select
                        .items
                        .iter()
                        .filter(|item| {
                            if let Some(alias) = &item.col_alias {
                                !alias.0.starts_with("__order_col_")
                            } else {
                                true
                            }
                        })
                        .map(|item| {
                            if let Some(col_alias) = &item.col_alias {
                                format!("`{}` AS `{}`", col_alias.0, col_alias.0)
                            } else {
                                // Fallback to the expression
                                item.expression.to_sql()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    // A whole-node `RETURN n` over a heterogeneous denormalized
                    // union carries no explicit outer SELECT items (the aligned
                    // node columns live in each union branch). Emitting an empty
                    // projection produces invalid `SELECT  FROM (...)` (ClickHouse
                    // Code 62). Pass the union's aligned columns straight through
                    // with `*` in that case.
                    if alias_select.is_empty() {
                        sql.push('*');
                    } else {
                        sql.push_str(&alias_select);
                    }
                }
            } else if !plan.select.items.is_empty() {
                sql.push_str(&plan.select.to_sql());
            } else {
                sql.push('*');
            }

            sql.push_str(" FROM (\n");

            // Generate UNION branch SQL.
            // When has_aggregation is true, all branches are stored in union.input
            // (extract_union moved the first branch there), so skip the base plan.
            // Otherwise, the base plan (select/from/joins/filters) IS the first branch.
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };

                // With aggregation: extract_union already put all branches in union.input,
                // so don't also render the base plan as first branch.
                //
                // The `plan.from.0.is_some()` guard handles literal-only aggregations
                // (e.g., `RETURN 'test' AS label, count(*) AS cnt`) where extract_union
                // moved all branches into union.input and left plan.from empty. When
                // plan.from is None, the base plan is not a separate branch, so we must
                // fall through to the else branch that iterates only over union.input.
                if !has_aggregation && plan.from.0.is_some() {
                    // Activate scope context for the first (outer-plan) UNION branch,
                    // scoping alias_label_map and multi_type_vlp_aliases to this scope.
                    activate_scope_context(&plan.from, &plan.joins);

                    let mut first_branch_sql = String::new();
                    first_branch_sql.push_str(&plan.select.to_sql());
                    first_branch_sql.push_str(&plan.from.to_sql());
                    first_branch_sql.push_str(&plan.joins.to_sql());
                    first_branch_sql.push_str(&plan.filters.to_sql());
                    sql.push_str(&first_branch_sql);

                    for union_branch in &union.input {
                        sql.push_str(union_type_str);
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                } else if has_aggregation {
                    // For aggregation: use pre-computed inner SELECT that includes
                    // non-aggregate columns plus aggregate argument columns.
                    let inner_sql = inner_select_sql.as_ref().unwrap();

                    // For VLP+aggregation+UNION, detect when reverse branches need
                    // start↔end swapping. The inner_select_sql was computed from
                    // the first VLP CTE's perspective. Reverse branches have the
                    // Cypher aliases swapped (start=end, end=start), so t.start_id
                    // and t.end_id references need to be swapped.
                    //
                    // Derive the baseline start/end aliases from the VLP CTE backing
                    // the first UNION branch (match the branch's `from` CTE name),
                    // so we don't accidentally pick an unrelated VLP CTE.
                    let (first_start, first_end) = if let Some(first_branch) = union.input.first() {
                        let first_from_name = first_branch
                            .from
                            .0
                            .as_ref()
                            .map(|f| f.name.as_str())
                            .unwrap_or("");
                        let first_vlp_cte = plan.ctes.0.iter().find(|c| {
                            c.cte_name == first_from_name && c.vlp_cypher_start_alias.is_some()
                        });
                        (
                            first_vlp_cte.and_then(|c| c.vlp_cypher_start_alias.clone()),
                            first_vlp_cte.and_then(|c| c.vlp_cypher_end_alias.clone()),
                        )
                    } else {
                        (None, None)
                    };

                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        let mut branch_sql = String::new();

                        // Check if this branch uses a reverse VLP CTE (aliases swapped)
                        let branch_from_name = union_branch
                            .from
                            .0
                            .as_ref()
                            .map(|f| f.name.as_str())
                            .unwrap_or("");
                        let branch_vlp_cte = plan.ctes.0.iter().find(|c| {
                            c.cte_name == branch_from_name && c.vlp_cypher_start_alias.is_some()
                        });
                        let needs_swap = if let (Some(bvlp), Some(ref fs), Some(ref fe)) =
                            (branch_vlp_cte, &first_start, &first_end)
                        {
                            // Reverse branch has start/end swapped compared to first CTE
                            bvlp.vlp_cypher_start_alias.as_deref() == Some(fe.as_str())
                                && bvlp.vlp_cypher_end_alias.as_deref() == Some(fs.as_str())
                        } else {
                            false
                        };

                        // For non-VLP branches with their own SELECT items (e.g., coupled
                        // schema UNION), use the branch's SELECT which has correctly mapped
                        // DB column names. The pre-computed inner_sql from the outer plan
                        // may have unmapped Cypher property names.
                        let branch_has_own_select =
                            !union_branch.select.items.is_empty() && branch_vlp_cte.is_none();

                        if branch_has_own_select {
                            // Build inner SELECT from branch's own items (correctly mapped)
                            // Filter out aggregate items and add agg arg columns.
                            // Also include outer plan's non-aggregate aliased columns
                            // (RETURN aliases like personId → friend.id) that aren't
                            // already in the branch SELECT — the outer SELECT references
                            // these by alias through the __union subquery.
                            let mut merged_select = union_branch.select.clone();
                            let branch_aliases: std::collections::HashSet<String> = merged_select
                                .items
                                .iter()
                                .filter_map(|i| i.col_alias.as_ref().map(|a| a.0.clone()))
                                .collect();
                            for outer_item in &plan.select.items {
                                if !render_expr_contains_aggregate(&outer_item.expression) {
                                    if let Some(ref alias) = outer_item.col_alias {
                                        if !branch_aliases.contains(&alias.0) {
                                            merged_select.items.push(outer_item.clone());
                                        }
                                    }
                                }
                            }
                            // Also add aggregate items from outer plan so their
                            // argument columns get extracted by build_union_inner_select.
                            // These aggregates won't appear in the inner SELECT (filtered),
                            // but their args (tag.name, comment.id) will be added with
                            // dotted aliases (tag.name AS "tag.name").
                            for outer_item in &plan.select.items {
                                if render_expr_contains_aggregate(&outer_item.expression) {
                                    merged_select.items.push(outer_item.clone());
                                }
                            }
                            let (branch_inner, _) =
                                build_union_inner_select(&merged_select, drop_path_metadata);
                            branch_sql.push_str(&branch_inner);
                        } else if needs_swap {
                            // Swap t.start_id ↔ t.end_id and start_* ↔ end_* in SELECT
                            let swapped = swap_vlp_start_end(inner_sql);
                            branch_sql.push_str(&swapped);
                        } else {
                            branch_sql.push_str(inner_sql);
                        }

                        branch_sql.push_str(&union_branch.from.to_sql());
                        branch_sql.push_str(&union_branch.joins.to_sql());
                        branch_sql.push_str(&union_branch.filters.to_sql());
                        sql.push_str(&branch_sql);
                    }
                } else {
                    // Non-aggregation, all branches in union.input
                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                }
            } else {
                // Activate scope context for this UNION branch,
                // scoping alias_label_map and multi_type_vlp_aliases to this scope.
                activate_scope_context(&plan.from, &plan.joins);

                let mut first_branch_sql = String::new();
                first_branch_sql.push_str(&plan.select.to_sql());
                first_branch_sql.push_str(&plan.from.to_sql());
                first_branch_sql.push_str(&plan.joins.to_sql());
                first_branch_sql.push_str(&plan.filters.to_sql());
                sql.push_str(&first_branch_sql);
            }

            sql.push_str(") AS __union\n");

            // Add GROUP BY — for UNION subquery context, reference column aliases
            // from the inner SELECT rather than original table-qualified names
            sql.push_str(&build_aliased_group_by(&plan.group_by, &plan.select));

            // Add ORDER BY after GROUP BY if present
            // For aggregation: use original ORDER BY expressions since the outer SELECT
            // provides the aliased columns. For non-aggregation UNION: reference __union columns.
            if has_aggregation && !plan.order_by.0.is_empty() {
                sql.push_str(&plan.order_by.to_sql());
            } else if !plan.order_by.0.is_empty() && !order_by_columns.is_empty() {
                sql.push_str("ORDER BY ");
                let order_clauses: Vec<String> = plan
                    .order_by
                    .0
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, item)| {
                        if idx >= order_by_columns.len() {
                            log::debug!(
                                "ORDER BY column index {} exceeds available columns ({}), skipping",
                                idx,
                                order_by_columns.len()
                            );
                            return None;
                        }
                        let col_alias = &order_by_columns[idx].1;
                        let order_str = match item.order {
                            OrderByOrder::Asc => "ASC",
                            OrderByOrder::Desc => "DESC",
                        };
                        Some(format!("__union.`{}` {}", col_alias, order_str))
                    })
                    .collect();
                sql.push_str(&order_clauses.join(", "));
                sql.push('\n');
            } else if order_by_columns.is_empty() && !plan.order_by.0.is_empty() {
                // ORDER BY was removed due to unsupported id() function
                log::info!("  ORDER BY removed (contains unsupported id() in UNION context)");
            } else {
                sql.push_str(&plan.order_by.to_sql());
            }

            // Add LIMIT/OFFSET after ORDER BY if present
            sql.push_str(&limit_offset_clause(plan.skip.0, plan.limit.0));
        } else {
            // No ordering/limiting - bare UNION is fine
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };

                if plan.from.0.is_some() {
                    // Activate scope context for this UNION branch,
                    // scoping alias_label_map and multi_type_vlp_aliases to this scope.
                    activate_scope_context(&plan.from, &plan.joins);

                    let mut first_branch_sql = String::new();
                    first_branch_sql.push_str(&plan.select.to_sql());
                    first_branch_sql.push_str(&plan.from.to_sql());
                    first_branch_sql.push_str(&plan.joins.to_sql());
                    first_branch_sql.push_str(&plan.filters.to_sql());
                    sql.push_str(&first_branch_sql);

                    for union_branch in &union.input {
                        sql.push_str(union_type_str);
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                } else {
                    // Shell base: all branches in union.input
                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                }
            } else {
                // Activate scope context for this no-union plan,
                // scoping alias_label_map and multi_type_vlp_aliases to this scope.
                activate_scope_context(&plan.from, &plan.joins);

                let mut first_branch_sql = String::new();
                first_branch_sql.push_str(&plan.select.to_sql());
                first_branch_sql.push_str(&plan.from.to_sql());
                first_branch_sql.push_str(&plan.joins.to_sql());
                first_branch_sql.push_str(&plan.filters.to_sql());
                sql.push_str(&first_branch_sql);
            }
        }

        return sql;
    }

    // Collect UNWIND (ARRAY JOIN) aliases to avoid `.*` expansion for scalar values
    let unwind_aliases: std::collections::HashSet<String> = plan
        .array_join
        .0
        .iter()
        .map(|aj| aj.alias.clone())
        .collect();

    sql.push_str(&plan.ctes.to_sql());
    sql.push_str(&plan.select.to_sql_with_unwind_aliases(&unwind_aliases));

    // Add FROM clause - UNWIND-only queries (no actual table) need a one-row
    // base relation for the array-expansion to hang off. Dialect-specific:
    //   CH:    `system.one` (single-row virtual table) + `ARRAY JOIN`
    //   Spark: `(SELECT 1)` subquery (aliased) + `LATERAL VIEW explode`
    let from_sql = plan.from.to_sql();
    if from_sql.is_empty() && !plan.array_join.0.is_empty() {
        match crate::server::query_context::get_current_dialect() {
            crate::sql_generator::SqlDialect::Databricks => {
                sql.push_str("FROM (SELECT 1) AS _unwind\n");
            }
            _ => {
                sql.push_str("FROM system.one\n");
            }
        }
    } else {
        sql.push_str(&from_sql);
    }

    sql.push_str(&plan.joins.to_sql());
    sql.push_str(&plan.array_join.to_sql());
    sql.push_str(&plan.filters.to_sql());

    sql.push_str(&plan.group_by.to_sql());

    // Add HAVING clause if present (after GROUP BY, before ORDER BY)
    if let Some(having_expr) = &plan.having_clause {
        sql.push_str("HAVING ");
        sql.push_str(&having_expr.to_sql());
        sql.push('\n');
    }

    // Databricks `SELECT DISTINCT` resolves ORDER BY against the aliased
    // DISTINCT output, not the source relation — so a sort term matching a
    // projection must reference its alias, not `table.col` (ClickHouse is
    // lenient here and resolves against the FROM relation).
    let distinct_spark_order = plan.select.distinct
        && matches!(
            crate::server::query_context::get_current_dialect(),
            crate::sql_generator::SqlDialect::Databricks
        );
    if distinct_spark_order {
        sql.push_str(&render_order_by_with_select_aliases(
            &plan.order_by,
            &plan.select,
        ));
    } else {
        sql.push_str(&plan.order_by.to_sql());
    }
    sql.push_str(&plan.union.to_sql());

    sql.push_str(&limit_offset_clause(plan.skip.0, plan.limit.0));

    // Note: max_recursive_cte_evaluation_depth is set as a client-level option
    // in connection_pool.rs, not as a SQL SETTINGS clause.
    // The clickhouse crate sends queries with readonly=1, which prevents
    // SETTINGS in SQL. Client-level options are sent as HTTP query parameters
    // and work in readonly mode.

    // CLEANUP: Clear ALL task-local render contexts before returning
    clear_all_render_contexts();

    sql
}

impl ToSql for RenderPlan {
    fn to_sql(&self) -> String {
        // Use default depth of 100 when called via trait
        render_plan_to_sql(self.clone(), 100)
    }
}

impl ToSql for SelectItems {
    fn to_sql(&self) -> String {
        // Default behavior: no UNWIND aliases to exclude from `.*` expansion
        self.to_sql_with_unwind_aliases(&std::collections::HashSet::new())
    }
}

impl SelectItems {
    /// Generate SQL for SELECT items, excluding `.*` expansion for UNWIND aliases.
    /// UNWIND aliases are scalars, not tables, so `x.*` is invalid for them.
    pub fn to_sql_with_unwind_aliases(
        &self,
        unwind_aliases: &std::collections::HashSet<String>,
    ) -> String {
        let mut sql: String = String::new();

        if self.items.is_empty() {
            return sql;
        }

        if self.distinct {
            sql.push_str("SELECT DISTINCT \n");
        } else {
            sql.push_str("SELECT \n");
        }

        for (i, item) in self.items.iter().enumerate() {
            sql.push_str("      ");

            // 🔧 BUG #9 FIX: For path variables, when TableAlias matches col_alias,
            // render as `alias.*` to avoid "Already registered p AS p" error
            // This handles: SELECT p AS "p" FROM ... AS p (invalid)
            // Should be: SELECT p.* FROM ... AS p (valid)
            //
            // 🔧 UNWIND FIX: Skip `.*` expansion for UNWIND aliases since they're scalars, not tables
            //
            // 🔧 SCALAR FIX: ColumnAlias never gets `.*` expansion - it's a scalar column reference
            // This handles: WITH n.email as group_key ... RETURN group_key
            // where group_key is a scalar column, not a node/table
            let rendered_expr = if let RenderExpr::ColumnAlias(_) = &item.expression {
                // ColumnAlias is always rendered as-is (scalar reference)
                // No wildcard expansion: group_key stays group_key, not group_key.*
                item.expression.to_sql()
            } else if let RenderExpr::TableAlias(TableAlias(alias_name)) = &item.expression {
                log::debug!(
                    "🔍 Rendering TableAlias '{}', col_alias={:?}",
                    alias_name,
                    item.col_alias
                );
                if let Some(col_alias) = &item.col_alias {
                    if alias_name == &col_alias.0 {
                        // Check if this is an UNWIND alias - don't use `.*` for scalars
                        if unwind_aliases.contains(alias_name) {
                            // UNWIND alias: render as just the alias (scalar value)
                            alias_name.clone()
                        } else {
                            // Path/table alias: use `.*` expansion
                            format!("{}.*", alias_name)
                        }
                    } else {
                        log::debug!(
                            "  Alias mismatch: col_alias={} != expr_alias={}",
                            col_alias.0,
                            alias_name
                        );
                        item.expression.to_sql()
                    }
                } else {
                    item.expression.to_sql()
                }
            } else {
                item.expression.to_sql()
            };

            sql.push_str(&rendered_expr);

            // Only add AS clause if the alias differs from the expression
            // (already handled above for matching TableAlias case)
            if let Some(alias) = &item.col_alias {
                let quoted = crate::sql_generator::function_mapper::current_function_mapper()
                    .quote_alias(&alias.0);
                if let RenderExpr::TableAlias(TableAlias(expr_alias)) = &item.expression {
                    // For UNWIND aliases that match OR for aliases that differ, we need the AS clause
                    if expr_alias != &alias.0 || unwind_aliases.contains(expr_alias) {
                        sql.push_str(" AS ");
                        sql.push_str(&quoted);
                    }
                } else {
                    sql.push_str(" AS ");
                    sql.push_str(&quoted);
                }
            }
            if i + 1 < self.items.len() {
                sql.push_str(", ");
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for FromTableItem {
    fn to_sql(&self) -> String {
        if let Some(view_ref) = &self.0 {
            let mut sql = String::new();
            sql.push_str("FROM ");

            // For all references, use the name directly
            // Note: WHERE clause filtering is handled in WhereClause generation,
            // not as a subquery in FROM clause
            sql.push_str(&view_ref.name);

            // Extract the alias - prefer the explicit alias from ViewTableRef,
            // otherwise try to get it from the source logical plan
            let alias = if let Some(explicit_alias) = &view_ref.alias {
                explicit_alias.clone()
            } else {
                match view_ref.source.as_ref() {
                    LogicalPlan::ViewScan(_) => {
                        // ViewScan fallback - should not reach here if alias is properly set
                        VLP_CTE_FROM_ALIAS.to_string()
                    }
                    _ => VLP_CTE_FROM_ALIAS.to_string(), // Default fallback
                }
            };

            sql.push_str(" AS ");
            sql.push_str(&alias);

            // Add FINAL keyword AFTER alias if needed (ClickHouse syntax: FROM table AS alias FINAL).
            // FINAL is ClickHouse-only — never emit it on other dialects (e.g. Databricks/Spark),
            // where it is invalid SQL, regardless of the schema's use_final.
            if view_ref.use_final
                && crate::server::query_context::get_current_dialect().supports_final_keyword()
            {
                sql.push_str(" FINAL");
            }

            sql.push('\n');
            sql
        } else {
            "".into()
        }

        // let mut sql: String = String::new();
        // if self.0.is_none() {
        //     return sql;
        // }
        // sql.push_str("FROM ");

        // sql.push_str(&self.table_name);
        // if let Some(alias) = &self.table_alias {
        //     if !alias.is_empty() {
        //         sql.push_str(" AS ");
        //         sql.push_str(&alias);
        //     }
        // }
        // sql.push('\n');
        // sql
    }
}

impl ToSql for FilterItems {
    fn to_sql(&self) -> String {
        if let Some(expr) = &self.0 {
            format!("WHERE {}\n", expr.to_sql())
        } else {
            "".into()
        }
    }
}

/// ARRAY JOIN for ClickHouse - maps from Cypher UNWIND clauses
/// Supports multiple UNWIND for cartesian product
///
/// Example: `UNWIND [1,2] AS x UNWIND [10,20] AS y`
/// Generates: `ARRAY JOIN [1,2] AS x ARRAY JOIN [10,20] AS y`
impl ToSql for ArrayJoinItem {
    fn to_sql(&self) -> String {
        if self.0.is_empty() {
            return "".into();
        }

        let databricks = matches!(
            crate::server::query_context::get_current_dialect(),
            crate::sql_generator::SqlDialect::Databricks
        );
        let mut sql = String::new();
        for array_join in &self.0 {
            if databricks {
                // Spark has no ARRAY JOIN; UNWIND maps to LATERAL VIEW explode.
                sql.push_str(&format!(
                    "LATERAL VIEW explode({}) AS {}\n",
                    array_join.expression.to_sql(),
                    array_join.alias
                ));
            } else {
                sql.push_str(&format!(
                    "ARRAY JOIN {} AS {}\n",
                    array_join.expression.to_sql(),
                    array_join.alias
                ));
            }
        }
        sql
    }
}

impl ToSql for GroupByExpressions {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("GROUP BY ");
        for (i, e) in self.0.iter().enumerate() {
            sql.push_str(&e.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

/// Render ORDER BY for a Databricks `SELECT DISTINCT`. Spark resolves ORDER BY
/// terms against the (aliased) DISTINCT output, so a term that matches a SELECT
/// projection is rendered as that projection's alias (backtick-quoted) rather
/// than the underlying `table.col`, which is no longer in scope after DISTINCT.
/// Terms with no matching projection fall back to the raw expression.
fn render_order_by_with_select_aliases(order_by: &OrderByItems, select: &SelectItems) -> String {
    if order_by.0.is_empty() {
        return String::new();
    }
    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
    let mut sql = String::from("ORDER BY ");
    for (i, item) in order_by.0.iter().enumerate() {
        let term = select
            .items
            .iter()
            .find(|s| s.expression == item.expression)
            .and_then(|s| s.col_alias.as_ref())
            .map(|a| mapper.quote_alias(&a.0))
            .unwrap_or_else(|| item.expression.to_sql());
        sql.push_str(&term);
        sql.push(' ');
        sql.push_str(&item.order.to_sql());
        if i + 1 < order_by.0.len() {
            sql.push_str(", ");
        }
    }
    sql.push('\n');
    sql
}

impl ToSql for OrderByItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("ORDER BY ");
        for (i, item) in self.0.iter().enumerate() {
            sql.push_str(&item.expression.to_sql());
            sql.push(' ');
            sql.push_str(&item.order.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for CteItems {
    fn to_sql(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }

        // Deduplicate CTEs by name (keep first occurrence)
        let mut seen_names = std::collections::HashSet::new();
        let deduped: Vec<&Cte> = self
            .0
            .iter()
            .filter(|cte| seen_names.insert(cte.cte_name.clone()))
            .collect();

        if deduped.is_empty() {
            return String::new();
        }

        // Simple rule: ONE `WITH RECURSIVE` at the top if any CTE is recursive,
        // then ALL CTEs flat and comma-separated. No nesting, no wrapping.
        let has_recursive = deduped.iter().any(|c| c.is_recursive);

        let mut sql = String::new();
        if has_recursive {
            sql.push_str("WITH RECURSIVE ");
        } else {
            sql.push_str("WITH ");
        }

        for (i, cte) in deduped.iter().enumerate() {
            sql.push_str(&cte.to_sql());
            if i + 1 < deduped.len() {
                sql.push_str(", \n");
            } else {
                sql.push('\n');
            }
        }

        sql
    }
}

impl ToSql for Cte {
    fn to_sql(&self) -> String {
        // Per-CTE registry: set this CTE's variable registry as task-local
        // so PropertyAccessExp::to_sql() can resolve CTE-scoped variables.
        let saved_registry = if self.variable_registry.is_some() {
            let prev = crate::server::query_context::get_current_variable_registry();
            if let Some(ref reg) = self.variable_registry {
                crate::server::query_context::set_current_variable_registry(reg.clone());
            }
            prev
        } else {
            None
        };

        // Handle both structured and raw SQL content
        let result = match &self.content {
            CteContent::Structured(plan) => {
                // Set scope-specific CTE alias mapping so `IN alias.col` resolves correctly
                let cte_names = crate::server::query_context::get_all_cte_names();
                let scope_mapping = build_cte_alias_mapping_for_scope(plan, &cte_names);
                let saved_aliases =
                    crate::server::query_context::set_cte_alias_scope(scope_mapping);

                // Scope branch context (alias_label_map + multi_type_vlp_aliases) to this
                // CTE body's FROM/JOINs. Prevents VLP aliases and node labels from the
                // outer scope leaking into this CTE body's property resolution.
                let branch_snapshot = snapshot_branch_context();
                activate_scope_context(&plan.from, &plan.joins);
                // For structured content, render only the query body (not nested CTEs)
                // CTEs should already be hoisted to the top level
                let mut cte_body = String::new();

                // Handle UNION plans - the union branches contain their own SELECTs
                if plan.union.0.is_some() {
                    // Check if we have custom SELECT items (WITH projection), modifiers, or GROUP BY
                    let has_custom_select = !plan.select.items.is_empty();
                    let has_order_by_skip_limit = !plan.order_by.0.is_empty()
                        || plan.limit.0.is_some()
                        || plan.skip.0.is_some();
                    let has_group_by = !plan.group_by.0.is_empty();
                    let needs_subquery =
                        has_custom_select || has_order_by_skip_limit || has_group_by;

                    if needs_subquery {
                        // When the plan has its own FROM (bidirectional UNION), push the
                        // SELECT projection into each UNION branch instead of using
                        // SELECT * — avoids unresolvable table-qualified column refs.
                        let has_modifiers =
                            has_group_by || has_order_by_skip_limit || plan.having_clause.is_some();
                        let has_aggregation = plan
                            .select
                            .items
                            .iter()
                            .any(|item| render_expr_contains_aggregate(&item.expression));

                        if has_aggregation && has_custom_select && plan.from.0.is_some() {
                            // Aggregate + UNION: inner branches project raw columns,
                            // outer SELECT applies aggregation over the __union subquery
                            let drop_path_metadata = !has_group_by;
                            let (inner_select_sql, agg_arg_cols) =
                                build_union_inner_select(&plan.select, drop_path_metadata);
                            let outer_select = build_outer_aggregate_select(
                                &plan.select,
                                &agg_arg_cols,
                                drop_path_metadata,
                            );

                            cte_body.push_str(&format!("SELECT {} FROM (\n", outer_select));
                            // Plan-level UNWIND expansion applies to every union branch
                            // (see the analogous non-aggregate block below). (#405)
                            let array_join_sql = plan.array_join.to_sql();

                            // First branch with non-aggregate inner SELECT
                            cte_body.push_str(&inner_select_sql);
                            cte_body.push_str(&plan.from.to_sql());
                            cte_body.push_str(&plan.joins.to_sql());
                            cte_body.push_str(&array_join_sql);
                            cte_body.push_str(&plan.filters.to_sql());

                            if let Some(union) = &plan.union.0 {
                                let union_type_str = match union.union_type {
                                    UnionType::Distinct => "UNION DISTINCT \n",
                                    UnionType::All => "UNION ALL \n",
                                };
                                for branch in &union.input {
                                    cte_body.push_str(union_type_str);
                                    cte_body.push_str(&inner_select_sql);
                                    cte_body.push_str(&branch.from.to_sql());
                                    cte_body.push_str(&branch.joins.to_sql());
                                    if branch.array_join.0.is_empty() {
                                        cte_body.push_str(&array_join_sql);
                                    } else {
                                        cte_body.push_str(&branch.array_join.to_sql());
                                    }
                                    cte_body.push_str(&branch.filters.to_sql());
                                }
                            }

                            cte_body.push_str(") AS __union\n");
                        } else if has_custom_select && plan.from.0.is_some() {
                            let select_sql = plan.select.to_sql();
                            // Plan-level UNWIND expansion (ARRAY JOIN / LATERAL VIEW) applies
                            // to EVERY union branch — e.g. a bidirectional VLP + UNWIND in one
                            // WITH segment yields `(... FROM vlp_a_b ARRAY JOIN n) UNION ALL
                            // (... FROM vlp_b_a ARRAY JOIN n)`. Emit it after each branch's
                            // FROM/JOINs (matching the standard branch's order). (#405)
                            let array_join_sql = plan.array_join.to_sql();

                            if has_modifiers {
                                // Need wrapper for GROUP BY/HAVING/ORDER BY/LIMIT
                                cte_body.push_str("SELECT * FROM (\n");
                            }

                            // First branch: plan's own FROM with projected SELECT
                            cte_body.push_str(&select_sql);
                            cte_body.push_str(&plan.from.to_sql());
                            cte_body.push_str(&plan.joins.to_sql());
                            cte_body.push_str(&array_join_sql);
                            cte_body.push_str(&plan.filters.to_sql());

                            if let Some(union) = &plan.union.0 {
                                let union_type_str = match union.union_type {
                                    UnionType::Distinct => "UNION DISTINCT \n",
                                    UnionType::All => "UNION ALL \n",
                                };
                                for branch in &union.input {
                                    cte_body.push_str(union_type_str);
                                    // Each branch gets the same SELECT projection
                                    cte_body.push_str(&select_sql);
                                    cte_body.push_str(&branch.from.to_sql());
                                    cte_body.push_str(&branch.joins.to_sql());
                                    // Prefer the branch's own array_join if present, else the
                                    // shared plan-level UNWIND expansion.
                                    if branch.array_join.0.is_empty() {
                                        cte_body.push_str(&array_join_sql);
                                    } else {
                                        cte_body.push_str(&branch.array_join.to_sql());
                                    }
                                    cte_body.push_str(&branch.filters.to_sql());
                                }
                            }

                            if has_modifiers {
                                cte_body.push_str(") AS __union\n");
                            }
                        } else {
                            // No custom select or no plan.from: use existing wrapper pattern
                            if has_custom_select {
                                cte_body.push_str(&plan.select.to_sql());
                            } else {
                                cte_body.push_str("SELECT * ");
                            }
                            cte_body.push_str("FROM (\n");

                            if plan.from.0.is_some() {
                                // First branch without custom select — use branch's own select
                                cte_body.push_str(&plan.select.to_sql());
                                cte_body.push_str(&plan.from.to_sql());
                                cte_body.push_str(&plan.joins.to_sql());
                                // Plan-level UNWIND expansion (#405). Defensive: this
                                // no-custom-select shape isn't produced for WITH+UNWIND
                                // (WITH segments always carry a custom select), and the line
                                // is a no-op when array_join is empty. Note the per-branch
                                // `render_union_branch_sql` below does NOT emit array_join, so
                                // this only covers the first (plan-level) branch.
                                cte_body.push_str(&plan.array_join.to_sql());
                                cte_body.push_str(&plan.filters.to_sql());

                                if let Some(union) = &plan.union.0 {
                                    let union_type_str = match union.union_type {
                                        UnionType::Distinct => "UNION DISTINCT \n",
                                        UnionType::All => "UNION ALL \n",
                                    };
                                    for branch in &union.input {
                                        cte_body.push_str(union_type_str);
                                        cte_body.push_str(&render_union_branch_sql(branch));
                                    }
                                }
                            } else {
                                cte_body.push_str(&plan.union.to_sql());
                            }

                            cte_body.push_str(") AS __union\n");

                            // Outer JOINs only when NOT already inside UNION branches
                            if plan.from.0.is_none() {
                                cte_body.push_str(&plan.joins.to_sql());
                            }
                        }

                        // Add GROUP BY — use aliased column references since
                        // we're outside the __union subquery wrapper
                        cte_body.push_str(&build_aliased_group_by(&plan.group_by, &plan.select));

                        // Add HAVING clause if present (after GROUP BY)
                        if let Some(having_expr) = &plan.having_clause {
                            cte_body.push_str("HAVING ");
                            cte_body.push_str(&having_expr.to_sql());
                            cte_body.push('\n');
                        }

                        cte_body.push_str(&plan.order_by.to_sql());

                        // Handle SKIP/LIMIT - either or both may be present
                        let clause = limit_offset_clause(plan.skip.0, plan.limit.0);
                        if !clause.is_empty() {
                            cte_body.push_str(&clause);
                            cte_body.push('\n');
                        }
                    } else {
                        // For Union plans without modifiers, just emit the union branches directly
                        cte_body.push_str(&plan.union.to_sql());
                    }
                } else {
                    // Standard single-query plan
                    // If there are no explicit SELECT items, default to SELECT *
                    if plan.select.items.is_empty() {
                        cte_body.push_str("SELECT *\n");
                    } else {
                        cte_body.push_str(&plan.select.to_sql());
                    }

                    // UNWIND-only CTE bodies (e.g. `UNWIND [1,2,3] AS x WITH x ...`)
                    // have no real table; the array expansion needs a one-row base
                    // relation, mirroring the main-query path. Dialect-specific:
                    // CH `system.one`, Spark `(SELECT 1)`. (issue #401)
                    let cte_from_sql = plan.from.to_sql();
                    if cte_from_sql.is_empty() && !plan.array_join.0.is_empty() {
                        match crate::server::query_context::get_current_dialect() {
                            crate::sql_generator::SqlDialect::Databricks => {
                                cte_body.push_str("FROM (SELECT 1) AS _unwind\n");
                            }
                            _ => {
                                cte_body.push_str("FROM system.one\n");
                            }
                        }
                    } else {
                        cte_body.push_str(&cte_from_sql);
                    }
                    cte_body.push_str(&plan.joins.to_sql());
                    cte_body.push_str(&plan.array_join.to_sql());
                    cte_body.push_str(&plan.filters.to_sql());
                    cte_body.push_str(&plan.group_by.to_sql());

                    // Add HAVING clause if present (after GROUP BY)
                    if let Some(having_expr) = &plan.having_clause {
                        cte_body.push_str("HAVING ");
                        cte_body.push_str(&having_expr.to_sql());
                        cte_body.push('\n');
                    }

                    cte_body.push_str(&plan.order_by.to_sql());

                    // Add LIMIT/SKIP for non-union CTEs as well
                    let clause = limit_offset_clause(plan.skip.0, plan.limit.0);
                    if !clause.is_empty() {
                        cte_body.push_str(&clause);
                        cte_body.push('\n');
                    }
                }

                // Restore branch-scoped context (alias_label_map + multi_type_vlp_aliases).
                restore_branch_context(branch_snapshot);
                // Restore previous CTE alias scope
                crate::server::query_context::set_cte_alias_scope(saved_aliases);

                format!("{} AS ({})", self.cte_name, cte_body)
            }
            CteContent::RawSql(sql) => {
                // Check if raw SQL already includes the CTE name and AS clause
                // (legacy behavior from VariableLengthCteGenerator)
                // or if we need to wrap it (new behavior from MultiTypeVlpJoinGenerator)
                if sql.trim_start().to_lowercase().starts_with("with ")
                    || sql
                        .trim_start()
                        .starts_with(&format!("{} AS", self.cte_name))
                    || sql.contains(" AS (")
                {
                    // Already wrapped - use as-is
                    sql.clone()
                } else {
                    // Raw CTE body - wrap it
                    format!("{} AS (\n{}\n)", self.cte_name, sql)
                }
            }
        };

        // Restore previous registry
        match saved_registry {
            Some(prev) => crate::server::query_context::set_current_variable_registry(prev),
            None => crate::server::query_context::clear_current_variable_registry(),
        }

        result
    }
}

impl ToSql for UnionItems {
    fn to_sql(&self) -> String {
        if let Some(union) = &self.0 {
            let union_sql_strs: Vec<String> = union
                .input
                .iter()
                .map(|union_item| union_item.to_sql())
                .collect();

            let union_type_str = match union.union_type {
                UnionType::Distinct => "UNION DISTINCT \n", // ClickHouse requires explicit DISTINCT
                UnionType::All => "UNION ALL \n",
            };

            union_sql_strs.join(union_type_str)
        } else {
            "".into()
        }
    }
}

impl ToSql for JoinItems {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        for join in &self.0 {
            sql.push_str(&join.to_sql());
        }
        sql
    }
}

impl ToSql for Join {
    fn to_sql(&self) -> String {
        crate::debug_println!("🔍 Join::to_sql");
        crate::debug_print!("  table_alias: {}", self.table_alias);
        crate::debug_print!("  table_name: {}", self.table_name);
        crate::debug_print!("  joining_on.len(): {}", self.joining_on.len());
        crate::debug_print!("  pre_filter: {:?}", self.pre_filter.is_some());
        if !self.joining_on.is_empty() {
            crate::debug_print!("  joining_on conditions:");
            for (_idx, _cond) in self.joining_on.iter().enumerate() {
                crate::debug_print!("    [{}]: {:?}", _idx, _cond);
            }
        } else {
            crate::debug_print!("  ⚠️  WARNING: joining_on is EMPTY!");
        }

        // Ensure table_name has database prefix for base tables.
        // CTE references (with_*_cte_*, vlp_*, pattern_*, rel_*) don't need prefix.
        // Base tables that are missing the prefix get it from the task-local schema.
        let qualified_table_name = ensure_database_prefix(&self.table_name);

        let join_type_str = match self.join_type {
            JoinType::Join => {
                if self.joining_on.is_empty() {
                    "CROSS JOIN"
                } else {
                    "JOIN"
                }
            }
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
        };

        // For LEFT JOIN with pre_filter, use subquery form:
        // LEFT JOIN (SELECT * FROM table WHERE pre_filter) AS alias ON ...
        // This ensures the filter is applied BEFORE the join (correct LEFT JOIN semantics)
        //
        // For INNER JOIN with pre_filter, add filter to ON clause:
        // INNER JOIN table AS alias ON <join_cond> AND <pre_filter>
        // This is semantically equivalent and more efficient than subquery
        let table_expr = if let Some(ref pre_filter) = self.pre_filter {
            if matches!(self.join_type, JoinType::Left) {
                // Use to_sql_without_table_alias to render column names without table prefix
                // since inside the subquery, the table is not yet aliased
                let filter_sql = pre_filter.to_sql_without_table_alias();
                crate::debug_print!(
                    "  Using subquery form for LEFT JOIN with pre_filter: {}",
                    filter_sql
                );
                format!(
                    "(SELECT * FROM {} WHERE {})",
                    qualified_table_name, filter_sql
                )
            } else {
                // For non-LEFT joins, pre_filter will be added to ON clause below
                qualified_table_name.clone()
            }
        } else {
            qualified_table_name.clone()
        };

        let mut sql = format!("{} {} AS {}", join_type_str, table_expr, self.table_alias);

        // Note: FINAL keyword for joins would need to be added here if Join struct
        // is enhanced to track use_final. For now, joins don't support FINAL.

        // Only add ON clause if there are joining conditions
        if !self.joining_on.is_empty() {
            let joining_on_str_vec: Vec<String> =
                self.joining_on.iter().map(|cond| cond.to_sql()).collect();

            let mut joining_on_str = joining_on_str_vec.join(" AND ");

            // For INNER JOINs (not LEFT), add pre_filter to ON clause
            // This applies polymorphic edge filters, schema filters, etc.
            if let Some(ref pre_filter) = self.pre_filter {
                if !matches!(self.join_type, JoinType::Left) {
                    let filter_sql = pre_filter.to_sql();
                    crate::debug_print!(
                        "  Adding pre_filter to INNER JOIN ON clause: {}",
                        filter_sql
                    );
                    joining_on_str = format!("{} AND {}", joining_on_str, filter_sql);
                }
            }

            sql.push_str(&format!(" ON {joining_on_str}"));
        } else if matches!(
            self.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        ) {
            // INNER/LEFT/RIGHT JOIN with empty joining_on is likely a planner bug.
            // Log error but use ON 1=1 as fallback to avoid crashing the server.
            log::error!(
                "Join::to_sql: {:?} with empty joining_on for table_alias={} table_name={} — possible planner bug",
                self.join_type, self.table_alias, self.table_name
            );
            sql.push_str(" ON 1=1");
        }

        sql.push('\n');
        sql
    }
}

impl RenderExpr {
    /// Render this expression (including any subqueries) to a SQL string.
    pub fn to_sql(&self) -> String {
        match self {
            RenderExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => {
                    if *b {
                        "true".into()
                    } else {
                        "false".into()
                    }
                }
                Literal::String(s) => format!("'{}'", s),
                Literal::Null => "NULL".into(),
            },
            RenderExpr::Parameter(name) => format!("${}", name),
            RenderExpr::Raw(raw) => raw.clone(),
            RenderExpr::Star => "*".into(),
            RenderExpr::TableAlias(TableAlias(a)) | RenderExpr::ColumnAlias(ColumnAlias(a)) => {
                a.clone()
            }
            RenderExpr::Column(Column(a)) => {
                // For column references, we need to add the table alias prefix
                // to match our FROM clause alias generation
                let raw_value = a.raw();

                // Special case: If the column is "*", return it directly without table prefix
                // This happens when a WITH clause expands a table alias to all columns
                if raw_value == "*" {
                    return "*".to_string();
                }

                if raw_value.contains('.') {
                    raw_value.to_string() // Already has table prefix
                } else {
                    // Detect VLP CTE columns by prefix or name.
                    // VLP CTE columns are named: start_id, end_id, start_city, end_name, etc.
                    // Plus internal path metadata: hop_count, path_relationships, path_nodes
                    // These should NOT be qualified with a table alias because they come from
                    // the VLP CTE and the rendering pipeline handles FROM alias separately
                    if raw_value.starts_with("start_")
                        || raw_value.starts_with("end_")
                        || matches!(raw_value, "hop_count" | "path_relationships" | "path_nodes")
                    {
                        log::info!(
                            "🔧 Detected VLP CTE column '{}', returning unqualified",
                            raw_value
                        );
                        return raw_value.to_string();
                    }

                    // CTE column names use p{N}_ prefix (e.g., p6_friend_lastName).
                    // These are output aliases after GROUP BY/UNION and should NOT get
                    // a heuristic table prefix.
                    if let Some(rest) = raw_value.strip_prefix('p') {
                        if let Some(pos) = rest.find('_') {
                            if pos > 0 && rest[..pos].chars().all(|c| c.is_ascii_digit()) {
                                return raw_value.to_string();
                            }
                        }
                    }

                    // ⚠️ TECHNICAL DEBT: Heuristic table alias inference (Temporary workaround)
                    //
                    // CONTEXT: This uses pattern matching on column names to infer the correct table alias.
                    // Works well for simple queries but breaks down in complex multi-join scenarios.
                    //
                    // CURRENT STRATEGY: Infer table alias from column name patterns and common naming conventions
                    // This covers ~95% of real-world cases and maintains backward compatibility.
                    //
                    // ISSUES WITH THIS APPROACH:
                    // - Fails for non-standard naming conventions (e.g., "t_name" instead of "user_name")
                    // - Ambiguous in multi-table scenarios (e.g., both users and posts have "id")
                    // - Requires hardcoding new patterns for each new entity type
                    // - Fragile when column names conflict across entity types
                    //
                    // TODO: Long-term solution should:
                    // 1. Pass table context/alias through the rendering pipeline
                    // 2. Track which columns belong to which tables in RenderExpr
                    // 3. Eliminate guessing with explicit table.column mappings in RenderPlan
                    // 4. Add property resolution via schema for Cypher→Database column mapping
                    //
                    // PERFORMANCE NOTE: Consider caching heuristic results to avoid repeated pattern matching
                    //
                    // Current table alias patterns:
                    let alias = if raw_value.contains("user")
                        || raw_value.contains("username")
                        || raw_value.contains("last_login")
                        || raw_value.contains("registration")
                        || raw_value == "name"
                        || raw_value == "age"
                        || raw_value == "active"
                        || raw_value.starts_with("u_")
                    {
                        "u" // User-related columns use 'u' alias
                    } else if raw_value.contains("post")
                        || raw_value.contains("article")
                        || raw_value.contains("published")
                        || raw_value == "title"
                        || raw_value == "views"
                        || raw_value == "status"
                        || raw_value == "author"
                        || raw_value == "category"
                        || raw_value.starts_with("p_")
                    {
                        "p" // Post-related columns use 'p' alias
                    } else if raw_value.contains("customer")
                        || raw_value.contains("rating")
                        || raw_value == "email"
                        || raw_value.starts_with("customer_")
                        || raw_value.starts_with("c_")
                    {
                        // CRITICAL FIX: Use 'c' to match FROM clause, not 'customer'
                        // The FROM clause uses original Cypher variable names (c, not customer)
                        "c" // Customer-related columns use 'c' alias to match FROM Customer AS c
                    } else if raw_value.contains("product")
                        || raw_value.contains("price")
                        || raw_value.contains("inventory")
                        || raw_value.starts_with("prod_")
                    {
                        "product" // Product-related columns
                    } else {
                        // FALLBACK: For truly unknown columns, use 't' (temporary/table)
                        // This maintains compatibility while covering 95%+ of real use cases
                        "t"
                    };

                    format!("{}.{}", alias, raw_value)
                }
            }
            RenderExpr::List(items) => {
                let inner = items
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                crate::sql_generator::function_mapper::current_function_mapper()
                    .array_literal(&inner)
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check for special functions that need custom handling
                let fn_name_lower = fn_call.name.to_lowercase();

                // Special handling for duration() with map argument
                if fn_name_lower == "duration" && fn_call.args.len() == 1 {
                    if let RenderExpr::MapLiteral(entries) = &fn_call.args[0] {
                        if !entries.is_empty() {
                            // Convert duration({days: 5, hours: 2}) into the active
                            // dialect's interval constructors (ClickHouse
                            // `toIntervalDay(5) + toIntervalHour(2)`, Databricks
                            // `make_dt_interval(...)`). Shares the unit mapping with
                            // the `LogicalExpr` path via `interval_expr_for_unit`.
                            let dialect = crate::server::query_context::get_current_dialect();
                            let interval_parts: Vec<String> = entries
                                .iter()
                                .filter_map(|(key, value)| {
                                    let value_sql = value.to_sql();
                                    let key_lower = key.to_lowercase();
                                    let mapped = super::function_translator::interval_expr_for_unit(
                                        &key_lower, &value_sql, dialect,
                                    );
                                    if mapped.is_none() {
                                        log::debug!("Unknown duration unit '{}', using as-is", key);
                                    }
                                    mapped
                                })
                                .collect();

                            // If every unit was unknown, `interval_parts` is empty —
                            // fall through to normal function handling rather than
                            // emitting an invalid `()`.
                            if interval_parts.len() == 1 {
                                return interval_parts[0].clone();
                            } else if !interval_parts.is_empty() {
                                return format!("({})", interval_parts.join(" + "));
                            }
                        }
                    }
                }

                // Special handling for datetime({epochMillis: x}) -> identity pass-through
                if fn_name_lower == "datetime" && fn_call.args.len() == 1 {
                    if let RenderExpr::MapLiteral(entries) = &fn_call.args[0] {
                        if entries.len() == 1 && entries[0].0.to_lowercase() == "epochmillis" {
                            return entries[0].1.to_sql();
                        }
                    }
                }

                // Native-function pass-through, keyed by the active dialect
                // (`ch.` for ClickHouse, `dbx.` for Databricks). This arm returns
                // `String`, not `Result`, so a foreign-backend prefix can't be
                // surfaced as a clean error here — instead we emit the *original*
                // prefixed name (e.g. `ch.uniq(x)`) so the query surfaces a
                // database error on the unknown prefixed function rather than
                // silently dropping the prefix into a valid-looking call. The
                // message-bearing error path is `translate_scalar_function` /
                // the `LogicalExpr` arms.
                match crate::sql_generator::passthrough::strip_passthrough(
                    &fn_call.name,
                    crate::server::query_context::get_current_dialect(),
                ) {
                    Ok(Some(bare)) => {
                        let args = fn_call
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return format!("{}({})", bare, args);
                    }
                    Ok(None) => { /* not a pass-through name — normal mapping below */ }
                    Err(e) => {
                        log::error!("scalar pass-through rejected: {}", e);
                        let args = fn_call
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return format!("{}({})", fn_call.name, args);
                    }
                }

                // Check if we have a Neo4j -> ClickHouse mapping
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        // Convert arguments to SQL
                        let args_sql: Vec<String> =
                            fn_call.args.iter().map(|e| e.to_sql()).collect();

                        // Apply transformation if provided
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };

                        let dialect = crate::server::query_context::get_current_dialect();
                        // Cypher `size()` is dialect-/type-sensitive on Spark: emit
                        // `size` for a collection argument, else the string-safe
                        // `length` default. ClickHouse keeps overloaded `length`.
                        let fn_name = if fn_name_lower == "size" {
                            databricks_size_name(fn_call.args.first(), dialect)
                                .unwrap_or_else(|| mapping.name_for(dialect))
                        } else {
                            mapping.name_for(dialect)
                        };

                        // Return dialect-appropriate function with transformed args
                        format!("{}({})", fn_name, transformed_args.join(", "))
                    }
                    None => {
                        // No mapping found - use original function name (passthrough)
                        let args = fn_call
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", fn_call.name, args)
                    }
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                // Native-function pass-through, keyed by the active dialect
                // (`ch.`/`chagg.` for ClickHouse, `dbx.` for Databricks).
                match crate::sql_generator::passthrough::strip_passthrough(
                    &agg.name,
                    crate::server::query_context::get_current_dialect(),
                ) {
                    Ok(Some(bare)) => {
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        log::debug!(
                            "aggregate pass-through: {}(..) -> {}({})",
                            agg.name,
                            bare,
                            args
                        );
                        return format!("{}({})", bare, args);
                    }
                    Ok(None) => { /* not a pass-through name — fall through to the registry */ }
                    Err(e) => {
                        // This arm returns `String`, not `Result`, so a foreign-backend
                        // prefix (e.g. `ch.uniq` on Databricks) can't be surfaced as a
                        // clean translation error here — emit the *original* prefixed
                        // name so the query surfaces a database error on the unknown
                        // prefixed function rather than silently dropping the prefix.
                        // The message-bearing error path is the `LogicalExpr` arms.
                        log::error!("aggregate pass-through rejected: {}", e);
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return format!("{}({})", agg.name, args);
                    }
                }

                // Check if we have a Cypher -> SQL mapping for aggregate functions.
                // Registry entries default to the CH spelling (most ANSI aggregates
                // are identical across dialects); entries that opt in via
                // `databricks_name: Some(...)` get a Spark-specific name back from
                // `mapping.name_for(dialect)`.
                let fn_name_lower = agg.name.to_lowercase();
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        let args_sql: Vec<String> = agg.args.iter().map(|e| e.to_sql()).collect();
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };
                        format!(
                            "{}({})",
                            mapping.name_for(crate::server::query_context::get_current_dialect()),
                            transformed_args.join(", ")
                        )
                    }
                    None => {
                        // No mapping - use original name (count, sum, min, max, avg, etc.)
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", agg.name, args)
                    }
                }
            }
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias,
                column,
            }) => {
                let col_name = column.raw();
                log::info!(
                    "🔍 RenderExpr::PropertyAccessExp: {}.{}",
                    table_alias.0,
                    col_name
                );

                // 🔧 CRITICAL FIX (Jan 23, 2026): Handle bare VLP columns in WITH clauses
                // When path functions are rewritten in WITH contexts, they use __vlp_bare_col marker
                // to indicate the column should be selected without a table alias
                if table_alias.0 == "__vlp_bare_col" {
                    log::info!(
                        "🔧 Detected VLP bare column: {} (from WITH clause path function)",
                        col_name
                    );
                    return col_name.to_string();
                }

                // Special case: Multi-type VLP properties stored in JSON
                // Check if this table alias is a multi-type VLP endpoint
                if is_multi_type_vlp_alias_from_context(&table_alias.0) {
                    log::info!("🎯 Found '{}' in multi-type VLP aliases!", table_alias.0);
                    // Properties like end_type, end_id, hop_count, path_relationships are direct CTE columns
                    if col_name == VLP_START_ID_COLUMN
                        || col_name == VLP_END_ID_COLUMN
                        || matches!(
                            col_name,
                            "end_type" | "end_properties" | "hop_count" | "path_relationships"
                        )
                    {
                        log::info!(
                            "🎯 Multi-type VLP CTE column: {}.{}",
                            table_alias.0,
                            col_name
                        );
                        return format!("{}.{}", table_alias.0, col_name);
                    } else {
                        // Regular properties need JSON extraction from end_properties
                        log::info!("🎯 Multi-type VLP JSON extraction: {}.{} → JSON_VALUE({}.end_properties, '$.{}')",
                                  table_alias.0, col_name, table_alias.0, col_name);
                        return format!(
                            "JSON_VALUE({}.end_properties, '$.{}')",
                            table_alias.0, col_name
                        );
                    }
                }

                // Resolve via unified VariableRegistry for CTE-scoped variables only.
                // Match-sourced variables are already resolved to DB columns during planning,
                // so we only need registry resolution for CTE-sourced variables where the
                // PropertyAccess.column is a Cypher property name that needs CTE column mapping.
                if let Some(resolved) = crate::server::query_context::resolve_with_current_registry(
                    &table_alias.0,
                    col_name,
                ) {
                    use crate::query_planner::typed_variable::ResolvedProperty;
                    match resolved {
                        ResolvedProperty::CteColumn { sql_alias, column } => {
                            log::info!(
                                "🔧 VariableRegistry resolved: {}.{} → {}.{}",
                                table_alias.0,
                                col_name,
                                sql_alias,
                                column
                            );
                            return format!("{}.{}", sql_alias, column);
                        }
                        ResolvedProperty::DbColumn(_) | ResolvedProperty::Unresolved => {
                            // Match-sourced or unresolved: skip — PropertyAccess already has
                            // the correct DB column from planning. Fall through.
                        }
                    }
                }

                // Check if table_alias refers to a CTE and needs property mapping
                // (fallback to task-local context for backward compatibility)
                if let Some(cte_col) = get_cte_property_from_context(&table_alias.0, col_name) {
                    log::debug!(
                        "🔧 CTE property mapping (legacy): {}.{} → {}",
                        table_alias.0,
                        col_name,
                        cte_col
                    );
                    return format!("{}.{}", table_alias.0, cte_col);
                }

                // Resolve "id" pseudo-property (from id() function transform) to actual
                // schema id column. This handles composite ID schemas where the table
                // doesn't have a column literally named "id".
                // Skip this rewrite if any node schema has "id" as a real property mapping —
                // in that case filter_tagging already resolved p.id → Column("id") correctly.
                if col_name == "id" {
                    use crate::server::query_context::{
                        get_current_schema, get_node_label_for_alias,
                    };
                    if let Some(schema) = get_current_schema() {
                        // If any schema has "id" as an explicit property mapping, "id" is a
                        // real column, not a pseudo-property residue from id() transforms.
                        let has_id_as_property = schema
                            .all_node_schemas()
                            .values()
                            .any(|ns| ns.property_mappings.contains_key("id"));
                        if !has_id_as_property {
                            // No schema treats "id" as a real property — this must be an id()
                            // function residue. Rewrite to the actual node_id column.
                            // Use the variable registry to find the label for this specific
                            // alias, then look up that label's node_id column.
                            let label = get_node_label_for_alias(&table_alias.0);
                            let node_schema =
                                label.as_deref().and_then(|l| schema.node_schema_opt(l));
                            if let Some(ns) = node_schema {
                                let cols = ns.node_id.columns();
                                if cols.len() == 1 {
                                    if let Some(first_col) = cols.first() {
                                        log::info!(
                                            "🔧 Resolved {}.id → {}.{} (schema id column, label={})",
                                            table_alias.0,
                                            table_alias.0,
                                            first_col,
                                            label.as_deref().unwrap_or("?"),
                                        );
                                        return format!("{}.{}", table_alias.0, first_col);
                                    }
                                }
                                // Composite node_id: fall through to render as-is
                                log::warn!(
                                    "⚠️  {}.id could not be resolved (composite node_id, label={})",
                                    table_alias.0,
                                    label.as_deref().unwrap_or("?"),
                                );
                            } else {
                                // Label unknown — fall back to iterating all schemas
                                // (pre-existing behaviour; handles cases where registry is absent)
                                for ns in schema.all_node_schemas().values() {
                                    let cols = ns.node_id.columns();
                                    if cols.len() == 1 {
                                        if let Some(first_col) = cols.first() {
                                            log::warn!(
                                                "🔧 Resolved {}.id → {}.{} (fallback — label unknown)",
                                                table_alias.0,
                                                table_alias.0,
                                                first_col
                                            );
                                            return format!("{}.{}", table_alias.0, first_col);
                                        }
                                    }
                                }
                                log::warn!(
                                    "⚠️  {}.id could not be resolved (composite/unknown ID, no label in registry)",
                                    table_alias.0
                                );
                            }
                        }
                    }
                }

                // Property has been resolved from schema during query planning.
                // Just use the resolved mapping directly.
                column.to_sql(&table_alias.0)
            }
            RenderExpr::OperatorApplicationExp(op) => {
                // ⚠️ TODO: Operator rendering consolidation (Phase 3)
                // This code is duplicated in to_sql.rs (~70 lines of similar operator handling).
                // Both implementations handle Operator enums with identical variants but different types:
                // - to_sql.rs: crate::query_planner::logical_expr::Operator
                // - to_sql_query.rs: crate::render_plan::render_expr::Operator
                // Phase 3 consolidation strategy: Create OperatorRenderer trait (see notes/OPERATOR_RENDERING_ANALYSIS.md)
                // Benefits:
                // - Eliminate duplication without type system complexity
                // - Preserve context-specific behavior (error handling, special cases)
                // - Enable future operator extensions
                // Estimated effort: 4-6 hours, should be 100% backward compatible
                log::debug!(
                    "RenderExpr::to_sql() OperatorApplicationExp: operator={:?}, operands.len()={}",
                    op.operator,
                    op.operands.len()
                );
                for (i, operand) in op.operands.iter().enumerate() {
                    log::debug!("  operand[{}]: {:?}", i, operand);
                }

                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX", // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH", // Special handling below
                        Operator::EndsWith => "ENDS WITH",     // Special handling below
                        Operator::Contains => "CONTAINS",      // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                // Special handling for IS NULL / IS NOT NULL with wildcard property access (e.g., r.*)
                // Convert r.* to appropriate ID column for null checks (LEFT JOIN produces NULL for all columns)
                // Since base tables have no NULLABLE columns, LEFT JOIN makes ALL columns NULL together,
                // so checking ANY ID column is sufficient (even for composite keys).
                if matches!(op.operator, Operator::IsNull | Operator::IsNotNull)
                    && op.operands.len() == 1
                {
                    if let RenderExpr::PropertyAccessExp(prop) = &op.operands[0] {
                        let col_name = prop.column.raw();
                        if col_name == "*" {
                            let table_alias = &prop.table_alias.0;
                            let op_str = if op.operator == Operator::IsNull {
                                "IS NULL"
                            } else {
                                "IS NOT NULL"
                            };

                            // Look up the actual column name from the JOIN metadata (populated during rendering)
                            // This ensures we use the CORRECT column for the SPECIFIC relationship table
                            if let Some((from_id, _to_id)) =
                                get_relationship_columns_from_context(table_alias)
                            {
                                // Use from_id - any ID column works since LEFT JOIN makes all NULL together
                                let id_sql = format!("{}.{}", table_alias, from_id);
                                return format!("{} {}", id_sql, op_str);
                            } else {
                                // Not a relationship — likely a node alias from OPTIONAL MATCH
                                // (e.g., CASE WHEN c IS NULL ... where c is a Comment node).
                                // Resolve to the node's ID column for the null check.
                                //
                                // We check ALL node schemas for consensus on the ID column name.
                                // If all nodes agree, we use that column. If they disagree, we log
                                // an error since we cannot determine the specific node type from
                                // the alias at this stage.
                                let id_col = {
                                    use crate::server::query_context::get_current_schema;
                                    use std::collections::BTreeSet;
                                    let mut unique_id_cols = BTreeSet::new();
                                    if let Some(schema) = get_current_schema() {
                                        for ns in schema.all_node_schemas().values() {
                                            let cols = ns.node_id.columns();
                                            if cols.len() == 1 {
                                                if let Some(first_col) = cols.first() {
                                                    unique_id_cols.insert(first_col.to_string());
                                                }
                                            }
                                        }
                                    }
                                    if unique_id_cols.len() == 1 {
                                        unique_id_cols.into_iter().next().unwrap()
                                    } else if unique_id_cols.is_empty() {
                                        log::error!(
                                            "Node wildcard null check for alias '{}': no node schemas found with single-column ID. Defaulting to 'id'.",
                                            table_alias
                                        );
                                        String::from("id")
                                    } else {
                                        log::error!(
                                            "Node wildcard null check for alias '{}': node schemas disagree on ID column name ({:?}). Cannot determine specific node type at SQL generation stage. Defaulting to 'id'.",
                                            table_alias,
                                            unique_id_cols
                                        );
                                        String::from("id")
                                    }
                                };
                                log::debug!(
                                    "Node wildcard null check: {}.{} {}",
                                    table_alias,
                                    id_col,
                                    op_str
                                );
                                let id_sql = format!("{}.{}", table_alias, id_col);
                                return format!("{} {}", id_sql, op_str);
                            }
                        }
                    }
                }

                // Node identity comparison: Cypher `a <> b` or `a = b` where both sides
                // are bare node variables (TableAlias) should compare by node ID column.
                // ClickHouse doesn't understand bare table aliases as values.
                if matches!(op.operator, Operator::Equal | Operator::NotEqual)
                    && op.operands.len() == 2
                {
                    let both_table_aliases = op
                        .operands
                        .iter()
                        .all(|o| matches!(o, RenderExpr::TableAlias(_)));
                    if both_table_aliases {
                        let op_str = if op.operator == Operator::Equal {
                            "="
                        } else {
                            "<>"
                        };
                        let lhs = op.operands[0].to_sql();
                        let rhs = op.operands[1].to_sql();
                        return format!("{}.id {} {}.id", lhs, op_str, rhs);
                    }
                }

                // Cypher literal equality: implement three-valued logic before rendering.
                // ClickHouse has different semantics for type coercion (e.g. '1'=1 → true)
                // and NULL propagation in collections (e.g. [null]=[1] → 0 not NULL).
                if matches!(op.operator, Operator::Equal | Operator::NotEqual)
                    && op.operands.len() == 2
                {
                    let tri_result = cypher_literal_eq(&op.operands[0], &op.operands[1]);
                    if let Some(tri) = tri_result {
                        let result = if op.operator == Operator::NotEqual {
                            tri.negate()
                        } else {
                            tri
                        };
                        return result.sql_str().to_string();
                    }
                }

                let rendered: Vec<String> = op.operands.iter().map(|e| e.to_sql()).collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // IN/NOT IN with CTE entity column → subquery for set membership.
                if rendered.len() == 2 {
                    if let Some(sql) =
                        try_rewrite_in_cte_subquery(&op.operator, &rendered[0], &op.operands[1])
                    {
                        return sql;
                    }
                }

                // Special handling for IN/NOT IN with array columns
                // Cypher: x IN array_property → ClickHouse: has(array, x)
                if op.operator == Operator::In
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    let contains = crate::sql_generator::function_mapper::current_function_mapper()
                        .array_contains();
                    return format!("{}({}, {})", contains, &rendered[1], &rendered[0]);
                }
                if op.operator == Operator::NotIn
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    let contains = crate::sql_generator::function_mapper::current_function_mapper()
                        .array_contains();
                    return format!("NOT {}({}, {})", contains, &rendered[1], &rendered[0]);
                }

                // IN/NOT IN with List containing non-constant elements → expand to OR/AND
                // ClickHouse: `x IN [col1, col2]` fails when array has column refs
                if (op.operator == Operator::In || op.operator == Operator::NotIn)
                    && rendered.len() == 2
                {
                    if let RenderExpr::List(list_items) = &op.operands[1] {
                        let has_non_constant = list_items.iter().any(|item| {
                            !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
                        });
                        if has_non_constant {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> =
                                list_items.iter().map(|item| item.to_sql()).collect();
                            if op.operator == Operator::In {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} = {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" OR "));
                            } else {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} <> {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" AND "));
                            }
                        } else if let Some(s) = render_constant_in_list(op, &rendered) {
                            return s;
                        }
                    }
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return super::common::contains_predicate(&rendered[0], &rendered[1]);
                }

                // Addition/Subtraction special cases (list concat, string concat,
                // interval arithmetic) — shared with the other two operator paths.
                if let Some(s) = render_list_addition(op) {
                    return s;
                }
                if let Some(s) = render_string_addition(op) {
                    return s;
                }
                if let Some(s) = render_interval_arithmetic(op, &rendered) {
                    return s;
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(), // should not happen
                    1 => {
                        // Handle unary operators: IS NULL/IS NOT NULL are suffix, NOT is prefix
                        match op.operator {
                            Operator::IsNull | Operator::IsNotNull => {
                                format!("{} {}", &rendered[0], sql_op) // suffix: "x IS NULL"
                            }
                            _ => {
                                format!("{} {}", sql_op, &rendered[0]) // prefix: "NOT x"
                            }
                        }
                    }
                    2 => {
                        // For AND/OR, wrap in parentheses to ensure correct precedence
                        // when combined with other expressions
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                            }
                            _ => {
                                if render_expr::needs_right_parens(op.operator, &op.operands[1]) {
                                    format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                                } else {
                                    format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                                }
                            }
                        }
                    }
                    _ => {
                        // n-ary: join with the operator, wrap in parentheses for AND/OR
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({})", rendered.join(&format!(" {} ", sql_op)))
                            }
                            _ => rendered.join(&format!(" {} ", sql_op)),
                        }
                    }
                }
            }
            RenderExpr::Case(case) => {
                // Check if any branch returns a List (Array) — if so, NULL branches
                // must be replaced with [] because ClickHouse can't find a supertype
                // for Nullable(Nothing) and Array(T).
                // Note: this checks top-level List variants only; nested lists inside
                // function calls are not detected. This is acceptable because CASE
                // branches that return arrays use direct List expressions in practice.
                let has_list_branch = case
                    .when_then
                    .iter()
                    .any(|(_, t)| matches!(t, RenderExpr::List(_)))
                    || case
                        .else_expr
                        .as_ref()
                        .is_some_and(|e| matches!(e.as_ref(), RenderExpr::List(_)));

                let render_result = |expr: &RenderExpr| -> String {
                    if has_list_branch && matches!(expr, RenderExpr::Literal(Literal::Null)) {
                        crate::sql_generator::function_mapper::current_function_mapper()
                            .array_literal("")
                    } else {
                        expr.to_sql()
                    }
                };

                // For ClickHouse, use caseWithExpression for simple CASE expressions
                if let Some(case_expr) = &case.expr {
                    // caseWithExpression(expr, val1, res1, val2, res2, ..., default)
                    let mut args = vec![case_expr.to_sql()];

                    for (when_expr, then_expr) in &case.when_then {
                        args.push(when_expr.to_sql());
                        args.push(render_result(then_expr));
                    }

                    let else_expr = case
                        .else_expr
                        .as_ref()
                        .map(|e| render_result(e))
                        .unwrap_or_else(|| {
                            if has_list_branch {
                                crate::sql_generator::function_mapper::current_function_mapper()
                                    .array_literal("")
                            } else {
                                "NULL".to_string()
                            }
                        });
                    args.push(else_expr);

                    format!("caseWithExpression({})", args.join(", "))
                } else {
                    // Searched CASE - use standard CASE syntax
                    let mut sql = String::from("CASE");

                    for (when_expr, then_expr) in &case.when_then {
                        sql.push_str(&format!(
                            " WHEN {} THEN {}",
                            when_expr.to_sql(),
                            render_result(then_expr)
                        ));
                    }

                    if let Some(else_expr) = &case.else_expr {
                        sql.push_str(&format!(" ELSE {}", render_result(else_expr)));
                    }

                    sql.push_str(" END");
                    sql
                }
            }
            RenderExpr::InSubquery(InSubquery { expr, subplan }) => {
                let left = expr.to_sql();
                let body = subplan.to_sql();
                let body = body.split_whitespace().collect::<Vec<&str>>().join(" ");

                format!("{} IN ({})", left, body)
            }
            RenderExpr::ExistsSubquery(exists) => {
                // Use the pre-generated SQL from the ExistsSubquery
                format!("EXISTS ({})", exists.sql)
            }
            RenderExpr::ReduceExpr(reduce) => {
                // Convert to ClickHouse arrayFold((acc, x) -> expr, list, init)
                // Cast numeric init to Int64 to prevent type mismatch issues
                let init_sql = reduce.initial_value.to_sql();
                let list_sql = reduce.list.to_sql();
                let expr_sql = reduce.expression.to_sql();

                // Wrap numeric init values in a 64-bit-int cast to prevent
                // type mismatch when the lambda returns a wider type.
                let init_cast = if matches!(
                    *reduce.initial_value,
                    RenderExpr::Literal(Literal::Integer(_))
                ) {
                    let fmap = crate::sql_generator::function_mapper::current_function_mapper();
                    format!("{}({})", fmap.cast_int64(), init_sql)
                } else {
                    init_sql
                };

                format!(
                    "arrayFold({}, {} -> {}, {}, {})",
                    reduce.variable, reduce.accumulator, expr_sql, list_sql, init_cast
                )
            }
            RenderExpr::MapLiteral(entries) => {
                // Use ClickHouse map() function for map literals
                // map('key1', val1, 'key2', val2, ...)
                //
                // IMPORTANT: ClickHouse requires all map values to be of the same type.
                // Since Cypher maps can have mixed types (e.g., {name:'nodes', data:count(*)}),
                // we cast all values to String to ensure type compatibility.
                if entries.is_empty() {
                    "map()".to_string()
                } else {
                    let to_str = crate::sql_generator::function_mapper::current_function_mapper()
                        .cast_string();
                    let args: Vec<String> = entries
                        .iter()
                        .flat_map(|(k, v)| {
                            let val_sql = v.to_sql();
                            vec![format!("'{}'", k), format!("{}({})", to_str, val_sql)]
                        })
                        .collect();
                    format!("map({})", args.join(", "))
                }
            }
            RenderExpr::PatternCount(pc) => {
                // Use the pre-generated SQL from PatternCount (correlated subquery)
                pc.sql.clone()
            }
            RenderExpr::ArraySubscript { array, index } => {
                // Array subscript in ClickHouse: array[index]
                // Cypher uses 0-based indexing; ClickHouse uses 1-based.
                // For integer literals we add +1 at compile time.
                // For expression indices we emit (expr)+1 without an explicit cast
                // so ClickHouse's own type checker catches bad types (e.g. floats,
                // strings) rather than silently coercing them.
                // Exception: string-literal indices are map-key accesses
                //   (e.g. top['score']) and must NOT be offset.
                let array_sql = array.to_sql();
                let index_sql = match index.as_ref() {
                    RenderExpr::Literal(Literal::Integer(n)) => format!("{}", n + 1),
                    RenderExpr::Literal(Literal::String(_)) => index.to_sql(),
                    _ => format!("({})+1", index.to_sql()),
                };
                format!("{}[{}]", array_sql, index_sql)
            }
            RenderExpr::ArraySlicing { array, from, to } => {
                // Array slicing -> arraySlice(arr, offset, length) (CH) / slice (Spark),
                // both 1-based offset + element count. Cypher list ranges are 0-based
                // and HALF-OPEN: `list[from..to]` yields indices [from, to), i.e.
                // `to - from` elements. So offset = from + 1 and length = to - from.
                let array_sql = array.to_sql();
                let mapper = crate::sql_generator::function_mapper::current_function_mapper();

                match (from, to) {
                    (Some(from_expr), Some(to_expr)) => {
                        // [from..to) -> 1-based offset + half-open length (to - from).
                        // Floor at 0: when from > to the slice is empty, but a negative
                        // length means "drop from the end" on ClickHouse arraySlice
                        // (silent wrong data) and errors on Databricks slice().
                        mapper.array_slice(
                            &array_sql,
                            &format!("{} + 1", from_expr.to_sql()),
                            Some(&format!(
                                "greatest({} - {}, 0)",
                                to_expr.to_sql(),
                                from_expr.to_sql()
                            )),
                        )
                    }
                    (Some(from_expr), None) => {
                        // [from..] - slice to end. CH 2-arg form; Spark computes the length.
                        mapper.array_slice(&array_sql, &format!("{} + 1", from_expr.to_sql()), None)
                    }
                    (None, Some(to_expr)) => {
                        // [..to) - from index 1, take `to` elements (indices [0, to)).
                        mapper.array_slice(&array_sql, "1", Some(&to_expr.to_sql()))
                    }
                    (None, None) => {
                        // [..] - no bounds, return entire array (identity operation)
                        array_sql
                    }
                }
            }
            RenderExpr::CteEntityRef(cte_ref) => {
                // CteEntityRef should be expanded to all its columns in the SELECT list
                // When we reach to_sql(), it means it wasn't expanded properly by select_builder
                // For now, generate SQL that selects all prefixed columns from the CTE
                log::debug!(
                    "CteEntityRef '{}' from CTE '{}' reached to_sql() - should have been expanded",
                    cte_ref.alias,
                    cte_ref.cte_name
                );
                // Fall back to table alias reference (this won't work correctly,
                // but prevents crashes while we complete the select_builder integration)
                format!("{}.{}", cte_ref.alias, cte_ref.alias)
            }
        }
    }

    /// Render this expression to SQL without table alias prefixes.
    /// Used for rendering filters inside subqueries where the table is not yet aliased.
    /// e.g., `LEFT JOIN (SELECT * FROM table WHERE is_active = true) AS b`
    /// The filter should be `is_active = true`, not `b.is_active = true`.
    pub fn to_sql_without_table_alias(&self) -> String {
        match self {
            RenderExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                // Just render the column without the table alias prefix
                column.to_sql_column_only()
            }
            RenderExpr::OperatorApplicationExp(op) => {
                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX", // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH", // Special handling below
                        Operator::EndsWith => "ENDS WITH",     // Special handling below
                        Operator::Contains => "CONTAINS",      // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                // Recursively render operands without table alias
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| e.to_sql_without_table_alias())
                    .collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // IN/NOT IN with CTE entity column → subquery for set membership.
                if rendered.len() == 2 {
                    if let Some(sql) =
                        try_rewrite_in_cte_subquery(&op.operator, &rendered[0], &op.operands[1])
                    {
                        return sql;
                    }
                }

                // Special handling for IN/NOT IN with array columns
                if op.operator == Operator::In
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    let contains = crate::sql_generator::function_mapper::current_function_mapper()
                        .array_contains();
                    return format!("{}({}, {})", contains, &rendered[1], &rendered[0]);
                }
                if op.operator == Operator::NotIn
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    let contains = crate::sql_generator::function_mapper::current_function_mapper()
                        .array_contains();
                    return format!("NOT {}({}, {})", contains, &rendered[1], &rendered[0]);
                }

                // IN/NOT IN with List containing non-constant elements → expand to OR/AND
                if (op.operator == Operator::In || op.operator == Operator::NotIn)
                    && rendered.len() == 2
                {
                    if let RenderExpr::List(list_items) = &op.operands[1] {
                        let has_non_constant = list_items.iter().any(|item| {
                            !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
                        });
                        if has_non_constant {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> =
                                list_items.iter().map(|item| item.to_sql()).collect();
                            if op.operator == Operator::In {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} = {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" OR "));
                            } else {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} <> {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" AND "));
                            }
                        } else if let Some(s) = render_constant_in_list(op, &rendered) {
                            return s;
                        }
                    }
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return super::common::contains_predicate(&rendered[0], &rendered[1]);
                }

                // Addition special cases (list concat, interval arithmetic) —
                // shared with the other operator paths. (No string-concat case
                // here, matching this path's original behavior.)
                if let Some(s) = render_list_addition(op) {
                    return s;
                }
                if let Some(s) = render_interval_arithmetic(op, &rendered) {
                    return s;
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(),
                    1 => match op.operator {
                        Operator::IsNull | Operator::IsNotNull => {
                            format!("{} {}", &rendered[0], sql_op)
                        }
                        _ => {
                            format!("{} {}", sql_op, &rendered[0])
                        }
                    },
                    2 => match op.operator {
                        Operator::And | Operator::Or => {
                            format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                        }
                        _ => {
                            if render_expr::needs_right_parens(op.operator, &op.operands[1]) {
                                format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                            } else {
                                format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                            }
                        }
                    },
                    _ => match op.operator {
                        Operator::And | Operator::Or => {
                            format!("({})", rendered.join(&format!(" {} ", sql_op)))
                        }
                        _ => rendered.join(&format!(" {} ", sql_op)),
                    },
                }
            }
            // For Raw expressions, strip table alias prefixes (e.g., "alias.column" -> "column")
            // This is needed for LEFT JOIN subqueries where the filter is inside SELECT * FROM table
            RenderExpr::Raw(raw_sql) => {
                // Simple approach: look for "word.word" patterns and keep only the part after the dot
                // This handles cases like "alias.column = 'value'" -> "column = 'value'"
                let result = raw_sql.clone();
                // Find and replace all "identifier.identifier" patterns
                let parts: Vec<&str> = result.split_whitespace().collect();
                let mut new_parts = Vec::new();
                for part in parts {
                    if part.contains('.') && !part.starts_with('\'') && !part.starts_with('"') {
                        // Split on dot and take the last part (the column name)
                        // But preserve the structure (e.g., "alias.column" becomes "column")
                        let dot_parts: Vec<&str> = part.split('.').collect();
                        if dot_parts.len() == 2
                            && !dot_parts[0].is_empty()
                            && !dot_parts[1].is_empty()
                        {
                            // Check if first part looks like an identifier (not a number)
                            let first_char = dot_parts[0].chars().next().unwrap_or('0');
                            if first_char.is_alphabetic() || first_char == '_' {
                                new_parts.push(dot_parts[1].to_string());
                                continue;
                            }
                        }
                    }
                    new_parts.push(part.to_string());
                }
                new_parts.join(" ")
            }
            // For other expression types, delegate to regular to_sql
            _ => self.to_sql(),
        }
    }
}

impl ToSql for OperatorApplication {
    fn to_sql(&self) -> String {
        // Map your enum to SQL tokens
        fn op_str(o: Operator) -> &'static str {
            match o {
                Operator::Addition => "+",
                Operator::Subtraction => "-",
                Operator::Multiplication => "*",
                Operator::Division => "/",
                Operator::ModuloDivision => "%",
                Operator::Exponentiation => "^",
                Operator::Equal => "=",
                Operator::NotEqual => "<>",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::RegexMatch => "REGEX", // Special handling below
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::In => "IN",
                Operator::NotIn => "NOT IN",
                Operator::StartsWith => "STARTS WITH", // Special handling below
                Operator::EndsWith => "ENDS WITH",     // Special handling below
                Operator::Contains => "CONTAINS",      // Special handling below
                Operator::Not => "NOT",
                Operator::Distinct => "DISTINCT",
                Operator::IsNull => "IS NULL",
                Operator::IsNotNull => "IS NOT NULL",
            }
        }

        let rendered: Vec<String> = self.operands.iter().map(|e| e.to_sql()).collect();

        // Debug operand information
        log::debug!(
            "OperatorApplication.to_sql(): operator={:?}, operands.len()={}, rendered.len()={}",
            self.operator,
            self.operands.len(),
            rendered.len()
        );
        for (i, (op, r)) in self.operands.iter().zip(rendered.iter()).enumerate() {
            log::debug!("  operand[{}]: {:?} -> '{}'", i, op, r);
        }

        // Special handling for RegexMatch - ClickHouse uses match() function
        if self.operator == Operator::RegexMatch && rendered.len() == 2 {
            return format!("match({}, {})", &rendered[0], &rendered[1]);
        }

        // IN/NOT IN with CTE entity column → subquery for set membership.
        if rendered.len() == 2 {
            if let Some(sql) =
                try_rewrite_in_cte_subquery(&self.operator, &rendered[0], &self.operands[1])
            {
                return sql;
            }
        }

        // Special handling for IN/NOT IN with array columns
        if self.operator == Operator::In
            && rendered.len() == 2
            && matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_))
        {
            let contains =
                crate::sql_generator::function_mapper::current_function_mapper().array_contains();
            return format!("{}({}, {})", contains, &rendered[1], &rendered[0]);
        }
        if self.operator == Operator::NotIn
            && rendered.len() == 2
            && matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_))
        {
            let contains =
                crate::sql_generator::function_mapper::current_function_mapper().array_contains();
            return format!("NOT {}({}, {})", contains, &rendered[1], &rendered[0]);
        }

        // IN/NOT IN with List containing non-constant elements → expand to OR/AND
        if (self.operator == Operator::In || self.operator == Operator::NotIn)
            && rendered.len() == 2
        {
            if let RenderExpr::List(list_items) = &self.operands[1] {
                let has_non_constant = list_items
                    .iter()
                    .any(|item| !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_)));
                if has_non_constant {
                    let lhs = &rendered[0];
                    let item_sqls: Vec<String> =
                        list_items.iter().map(|item| item.to_sql()).collect();
                    if self.operator == Operator::In {
                        let clauses: Vec<String> = item_sqls
                            .iter()
                            .map(|rhs| format!("{} = {}", lhs, rhs))
                            .collect();
                        return format!("({})", clauses.join(" OR "));
                    } else {
                        let clauses: Vec<String> = item_sqls
                            .iter()
                            .map(|rhs| format!("{} <> {}", lhs, rhs))
                            .collect();
                        return format!("({})", clauses.join(" AND "));
                    }
                } else if let Some(s) = render_constant_in_list(self, &rendered) {
                    return s;
                }
            }
        }

        // Special handling for string predicates - ClickHouse uses functions
        if self.operator == Operator::StartsWith && rendered.len() == 2 {
            return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::EndsWith && rendered.len() == 2 {
            return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::Contains && rendered.len() == 2 {
            return super::common::contains_predicate(&rendered[0], &rendered[1]);
        }

        // Addition/Subtraction special cases (list concat, string concat,
        // interval arithmetic) — shared with the RenderExpr operator paths.
        if let Some(s) = render_list_addition(self) {
            return s;
        }
        if let Some(s) = render_string_addition(self) {
            return s;
        }
        if let Some(s) = render_interval_arithmetic(self, &rendered) {
            return s;
        }

        let sql_op = op_str(self.operator);

        match rendered.len() {
            0 => "".into(),                              // should not happen
            1 => format!("{} {}", sql_op, &rendered[0]), // unary
            2 => {
                if render_expr::needs_right_parens(self.operator, &self.operands[1]) {
                    format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                } else {
                    format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                }
            }
            _ => {
                // n-ary: join with the operator
                rendered.join(&format!(" {} ", sql_op))
            }
        }
    }
}

impl ToSql for OrderByOrder {
    fn to_sql(&self) -> String {
        match self {
            OrderByOrder::Asc => "ASC".to_string(),
            OrderByOrder::Desc => "DESC".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{Literal, RenderExpr};

    /// Regression test: MapLiteral values must be wrapped in toString() for ClickHouse
    /// type compatibility. Without this, mixed-type maps like {name:'nodes', data:count(*)}
    /// cause ClickHouse type errors.
    #[test]
    fn test_map_literal_wraps_values_in_to_string() {
        let map_expr = RenderExpr::MapLiteral(vec![
            (
                "name".to_string(),
                RenderExpr::Literal(Literal::String("nodes".to_string())),
            ),
            (
                "count".to_string(),
                RenderExpr::Literal(Literal::Integer(42)),
            ),
        ]);

        let sql = map_expr.to_sql();
        assert_eq!(sql, "map('name', toString('nodes'), 'count', toString(42))");
    }

    #[test]
    fn test_map_literal_empty() {
        let map_expr = RenderExpr::MapLiteral(vec![]);
        assert_eq!(map_expr.to_sql(), "map()");
    }

    // ---- WHERE alias inlining for Spark/Databricks (LDBC Q10) ----

    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::render_plan::render_expr::{PropertyAccess, ScalarFnCall, TableAlias};
    use crate::render_plan::{
        ArrayJoinItem, CteItems, GroupByExpressions, Join, JoinItems, JoinType, LimitItem,
        OrderByItems, SkipItem, Union, UnionType,
    };

    /// A branch that joins one table under `table_alias`, with the given WHERE.
    fn branch_joining(table_alias: &str, filter: RenderExpr) -> RenderPlan {
        let mut b = empty_plan();
        b.joins = JoinItems(vec![Join {
            table_name: "ldbc.Person".to_string(),
            table_alias: table_alias.to_string(),
            joining_on: vec![],
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        }]);
        b.filters = FilterItems(Some(filter));
        b
    }

    fn empty_plan() -> RenderPlan {
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![],
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(vec![]),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: None,
            order_by: OrderByItems(vec![]),
            skip: SkipItem(None),
            limit: LimitItem(None),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        }
    }

    fn friend_birthday() -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("friend".to_string()),
            column: PropertyValue::Column("birthday".to_string()),
        })
    }

    /// `month(birthday)` where `birthday` is a bare alias reference (TableAlias).
    fn month_of_birthday_alias() -> RenderExpr {
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "month".to_string(),
            args: vec![RenderExpr::TableAlias(TableAlias("birthday".to_string()))],
        })
    }

    #[test]
    fn substitute_alias_refs_replaces_bare_alias_with_source() {
        let mut expr = month_of_birthday_alias();
        let mut map = HashMap::new();
        map.insert("birthday".to_string(), friend_birthday());
        substitute_alias_refs_in_expr(&mut expr, &map);
        // The bare alias is replaced by its qualified source (`friend.birthday`);
        // the surrounding function rendering is left to the registry.
        let sql = expr.to_sql();
        assert!(sql.contains("friend.birthday"), "got: {sql}");
    }

    #[test]
    fn substitute_alias_refs_leaves_unmapped_refs_untouched() {
        let mut expr = month_of_birthday_alias();
        let map = HashMap::new(); // empty: nothing to inline
        substitute_alias_refs_in_expr(&mut expr, &map);
        // Bare alias is preserved when there is no source to inline.
        let sql = expr.to_sql();
        assert!(!sql.contains("friend.birthday"), "got: {sql}");
        assert!(sql.contains("birthday"), "got: {sql}");
    }

    fn primary_birthday_select() -> SelectItems {
        SelectItems {
            items: vec![SelectItem {
                expression: friend_birthday(),
                col_alias: Some(ColumnAlias("birthday".to_string())),
            }],
            distinct: false,
        }
    }

    /// Regression for LDBC Q10: an undirected internal UNION whose reverse arm
    /// joins `friend` and carries a colliding bare `birthday` column. The primary
    /// `birthday => friend.birthday` binding is inlined into BOTH the primary
    /// filter and the branch filter (the branch joins `friend`, so the guard
    /// permits it), giving `month(friend.birthday)` in each arm — no ambiguous
    /// bare reference, both arms consistent.
    #[test]
    fn inline_where_alias_refs_inlines_into_branch_that_has_source_table() {
        let branch = branch_joining("friend", month_of_birthday_alias());

        let mut plan = empty_plan();
        plan.select = primary_birthday_select();
        plan.filters = FilterItems(Some(month_of_birthday_alias()));
        plan.union = UnionItems(Some(Union {
            input: vec![branch],
            union_type: UnionType::All,
        }));

        inline_where_alias_refs_recursive(&mut plan);

        let primary_sql = plan.filters.0.unwrap().to_sql();
        assert!(
            primary_sql.contains("friend.birthday"),
            "got: {primary_sql}"
        );
        let branch_sql = plan.union.0.unwrap().input[0]
            .filters
            .0
            .clone()
            .unwrap()
            .to_sql();
        assert!(branch_sql.contains("friend.birthday"), "got: {branch_sql}");
    }

    /// Soundness guard for genuine user UNIONs: a branch whose FROM/JOINs do NOT
    /// contain the primary source's table must NOT receive the inline (it would
    /// emit `WHERE friend.birthday` against a table the branch lacks). The branch
    /// filter is left untouched.
    #[test]
    fn inline_where_alias_refs_skips_branch_missing_source_table() {
        // Branch joins `movie`, not `friend`; primary source is `friend.birthday`.
        let branch = branch_joining("movie", month_of_birthday_alias());

        let mut plan = empty_plan();
        plan.select = primary_birthday_select();
        plan.union = UnionItems(Some(Union {
            input: vec![branch],
            union_type: UnionType::All,
        }));

        inline_where_alias_refs_recursive(&mut plan);

        let branch_sql = plan.union.0.unwrap().input[0]
            .filters
            .0
            .clone()
            .unwrap()
            .to_sql();
        // Guard skipped: no `friend.birthday` leaked into the foreign branch.
        assert!(!branch_sql.contains("friend.birthday"), "got: {branch_sql}");
        assert!(branch_sql.contains("birthday"), "got: {branch_sql}");
    }

    /// `datetime({epochMillis: friend.birthday})` — the actual Q10 source shape.
    fn datetime_of_friend_birthday() -> RenderExpr {
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "datetime".to_string(),
            args: vec![RenderExpr::MapLiteral(vec![(
                "epochMillis".to_string(),
                friend_birthday(),
            )])],
        })
    }

    /// The guard must see through `MapLiteral` (Q10's source buries
    /// `friend.birthday` inside `datetime({epochMillis: ...})`). Under-counting
    /// here would either skip the legit inline or, worse, pass a foreign branch.
    #[test]
    fn source_table_aliases_sees_through_map_literal() {
        assert_eq!(
            source_table_aliases(&datetime_of_friend_birthday()),
            Some(HashSet::from(["friend".to_string()]))
        );
    }

    /// Determinable, table-free sources are a subset of every branch.
    #[test]
    fn source_table_aliases_literal_is_empty_set() {
        let lit = RenderExpr::Literal(Literal::Integer(1));
        assert_eq!(source_table_aliases(&lit), Some(HashSet::new()));
    }

    /// Fail-closed: a source with an undeterminable node (`Raw`, bare unqualified
    /// `Column`) returns `None` so it is never inlined into a foreign branch.
    #[test]
    fn source_table_aliases_fails_closed_on_opaque() {
        assert_eq!(
            source_table_aliases(&RenderExpr::Raw("anything(x)".to_string())),
            None
        );
        let bare = RenderExpr::Column(crate::render_plan::render_expr::Column(
            PropertyValue::Column("birthday".to_string()),
        ));
        assert_eq!(source_table_aliases(&bare), None);
    }

    /// A qualified bare column resolves to its qualifier table.
    #[test]
    fn source_table_aliases_qualified_column_yields_table() {
        let qualified = RenderExpr::Column(crate::render_plan::render_expr::Column(
            PropertyValue::Column("friend.birthday".to_string()),
        ));
        assert_eq!(
            source_table_aliases(&qualified),
            Some(HashSet::from(["friend".to_string()]))
        );
    }

    /// An aggregate buried in a `MapLiteral` must keep the source out of the
    /// alias map — `render_expr_contains_aggregate` misses it, `source_contains_
    /// aggregate` does not.
    #[test]
    fn source_contains_aggregate_sees_through_map_literal() {
        let agg_in_map = RenderExpr::MapLiteral(vec![(
            "n".to_string(),
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: "count".to_string(),
                args: vec![RenderExpr::Star],
            }),
        )]);
        assert!(source_contains_aggregate(&agg_in_map));
        assert!(!source_contains_aggregate(&friend_birthday()));
    }

    /// End-to-end: Q10's real source shape inlines into a `friend`-joining arm.
    #[test]
    fn inline_where_alias_refs_inlines_map_literal_source_into_branch() {
        let branch = branch_joining("friend", month_of_birthday_alias());
        let mut plan = empty_plan();
        plan.select = SelectItems {
            items: vec![SelectItem {
                expression: datetime_of_friend_birthday(),
                col_alias: Some(ColumnAlias("birthday".to_string())),
            }],
            distinct: false,
        };
        plan.union = UnionItems(Some(Union {
            input: vec![branch],
            union_type: UnionType::All,
        }));

        inline_where_alias_refs_recursive(&mut plan);

        let branch_sql = plan.union.0.unwrap().input[0]
            .filters
            .0
            .clone()
            .unwrap()
            .to_sql();
        assert!(branch_sql.contains("friend.birthday"), "got: {branch_sql}");
    }

    /// `databricks_size_name`: outside Databricks → None (registry default
    /// `length`); under Databricks a list-literal arg → Spark `size`.
    #[test]
    fn databricks_size_name_dispatches_by_dialect_and_arg() {
        use crate::sql_generator::SqlDialect;
        let list = RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(1))]);
        assert_eq!(
            databricks_size_name(Some(&list), SqlDialect::ClickHouse),
            None
        );
        assert_eq!(
            databricks_size_name(Some(&list), SqlDialect::Databricks),
            Some("size")
        );
        // A bare string-ish arg (no collection signal) → None → falls back to length.
        let lit = RenderExpr::Literal(Literal::String("abc".to_string()));
        assert_eq!(
            databricks_size_name(Some(&lit), SqlDialect::Databricks),
            None
        );
    }

    /// `collect_array_cte_columns` records SELECT aliases whose source is a
    /// collection aggregate or list literal, across CTEs — so a carried-forward
    /// `collect()` column (registry-typed as scalar) is still detectable.
    #[test]
    fn collect_array_cte_columns_finds_collection_columns() {
        let mut cte_plan = empty_plan();
        cte_plan.select = SelectItems {
            items: vec![
                SelectItem {
                    expression: RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: "collect".to_string(),
                        args: vec![friend_birthday()],
                    }),
                    col_alias: Some(ColumnAlias("posts".to_string())),
                },
                SelectItem {
                    expression: friend_birthday(),
                    col_alias: Some(ColumnAlias("bday".to_string())),
                },
            ],
            distinct: false,
        };
        let mut plan = empty_plan();
        plan.ctes = CteItems(vec![Cte::new(
            "with_posts_cte".to_string(),
            CteContent::Structured(Box::new(cte_plan)),
            false,
        )]);

        let cols = collect_array_cte_columns(&plan);
        assert!(cols.contains("posts"), "got: {cols:?}");
        assert!(!cols.contains("bday"), "got: {cols:?}");
    }

    /// Test: collect(x) + collect(y) → arrayConcat(groupArray(x), groupArray(y))
    #[test]
    fn test_list_concat_two_collects() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "collect".to_string(),
                    args: vec![RenderExpr::Literal(Literal::String("x".to_string()))],
                }),
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "groupArray".to_string(),
                    args: vec![RenderExpr::Literal(Literal::String("y".to_string()))],
                }),
            ],
        });
        let sql = expr.to_sql();
        // "collect" is mapped to "groupArray" by the function registry during to_sql()
        assert_eq!(sql, "arrayConcat(groupArray('x'), groupArray('y'))");
    }

    /// Test: list + scalar → arrayConcat(list, [scalar])
    #[test]
    fn test_list_concat_list_plus_scalar() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "collect".to_string(),
                    args: vec![RenderExpr::Literal(Literal::Integer(1))],
                }),
                RenderExpr::Literal(Literal::Integer(42)),
            ],
        });
        let sql = expr.to_sql();
        assert_eq!(sql, "arrayConcat(groupArray(1), [42])");
    }

    /// Test: scalar + list → arrayConcat([scalar], list)
    #[test]
    fn test_list_concat_scalar_plus_list() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::Literal(Literal::Integer(42)),
                RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(1))]),
            ],
        });
        let sql = expr.to_sql();
        assert_eq!(sql, "arrayConcat([42], [1])");
    }

    /// Under the Databricks dialect, list concatenation emits Spark's
    /// `concat(...)` (array-overloaded), not ClickHouse's `arrayConcat(...)`.
    #[tokio::test]
    async fn test_list_concat_uses_concat_under_databricks() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let make_expr = || {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Addition,
                operands: vec![
                    RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(1))]),
                    RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(2))]),
                ],
            })
        };

        // Bare OperatorApplication::to_sql is a separate render arm (reached via
        // JOIN ON predicates) — cover it too.
        let make_op = || OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(1))]),
                RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(2))]),
            ],
        };

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let (sql, op_sql) =
            with_query_context(ctx, async { (make_expr().to_sql(), make_op().to_sql()) }).await;
        for s in [&sql, &op_sql] {
            assert!(
                s.contains("concat(") && !s.contains("arrayConcat("),
                "expected Spark `concat(`, not `arrayConcat(`; got: {s}"
            );
        }

        // CH baseline (default scope) keeps arrayConcat on both arms.
        assert!(
            make_expr().to_sql().contains("arrayConcat(")
                && make_op().to_sql().contains("arrayConcat("),
            "CH baseline should still emit arrayConcat"
        );
    }

    /// CONTAINS renders `position(haystack, needle) > 0` on ClickHouse but must
    /// REVERSE the args on Databricks, since Spark's `position(substr, str)` takes
    /// the substring first.
    #[tokio::test]
    async fn test_contains_reverses_position_args_on_databricks() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let make_expr = || {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Contains,
                operands: vec![
                    RenderExpr::Literal(Literal::String("hay".to_string())),
                    RenderExpr::Literal(Literal::String("need".to_string())),
                ],
            })
        };

        // ClickHouse (default): haystack first.
        assert_eq!(make_expr().to_sql(), "(position('hay', 'need') > 0)");

        // Databricks: substring (needle) first.
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async { make_expr().to_sql() }).await;
        assert_eq!(sql, "(position('need', 'hay') > 0)");
    }

    /// A `tuple(...)` ScalarFnCall renders as CH `tuple(...)` but Spark `struct(...)`.
    #[tokio::test]
    async fn test_tuple_renders_as_struct_on_databricks() {
        use crate::render_plan::render_expr::ScalarFnCall;
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let make_expr = || {
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "tuple".to_string(),
                args: vec![
                    RenderExpr::Literal(Literal::Integer(1)),
                    RenderExpr::Literal(Literal::Integer(2)),
                ],
            })
        };

        // ClickHouse (default).
        assert_eq!(make_expr().to_sql(), "tuple(1, 2)");

        // Databricks.
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async { make_expr().to_sql() }).await;
        assert_eq!(sql, "struct(1, 2)");
    }

    /// Test: numeric + numeric (no list) → stays as addition, not arrayConcat
    #[test]
    fn test_addition_without_lists_unchanged() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                RenderExpr::Literal(Literal::Integer(1)),
                RenderExpr::Literal(Literal::Integer(2)),
            ],
        });
        let sql = expr.to_sql();
        assert_eq!(sql, "1 + 2");
    }
}
