use crate::{
    render_plan::{
        render_expr::{
            self, AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication,
            PropertyAccess, ReduceExpr, RenderCase, RenderExpr, ScalarFnCall, TableAlias,
        },
        {
            ArrayJoinItem, Cte, CteContent, FilterItems, FromTableItem, GroupByExpressions, Join,
            JoinItems, JoinType, OrderByItem, OrderByItems, OrderByOrder, RenderPlan, SelectItems,
            ToSql, UnionType,
        },
    },
    server::query_context::set_alias_label_map,
};
use std::collections::HashMap;
use std::collections::HashSet;

// Import function translator for Neo4j -> ClickHouse function mappings
use super::function_registry::get_function_mapping;

// ============================================================================
// RENDER CONTEXT ACCESSORS (delegating to unified query_context)
// ============================================================================

/// True when `expr` is a string-valued operand of a Cypher `+`, so the `+`
/// should render as `concat(...)` rather than numeric addition. Detects both a
/// bare string literal (fast path, recursing through nested `+`) AND any operand
/// the render-site classifier proves is String-typed — a string-returning
/// function call such as `toUpper(a)`/`toString(x)`, or a schema-declared string
/// column (#871). The classifier is conservative: unknown → not-string, so a
/// numeric `+` (e.g. `toFloat('1') + toFloat('2')`) is never misrouted to concat.
fn is_string_operand(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(is_string_operand)
        }
        // #969: a bare reduce lambda binder (`s`/`x`) whose task-local type is
        // String — consulted ONLY here (the string-concat detector), never via
        // the shared `infer_render_type` classifier, so #880/#854/#962 keep
        // seeing binders as untyped (preserving the conservative-None invariant).
        RenderExpr::TableAlias(ta) => {
            crate::server::query_context::get_reduce_binder_type(&ta.0)
                == Some(super::type_inference::RenderType::String)
        }
        RenderExpr::Column(col) => {
            crate::server::query_context::get_reduce_binder_type(col.raw())
                == Some(super::type_inference::RenderType::String)
        }
        _ => {
            super::type_inference::infer_render_type(expr)
                == Some(super::type_inference::RenderType::String)
        }
    }
}

/// Check if any operand in the expression is string-valued (see [`is_string_operand`]).
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(is_string_operand)
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
            // bodies are raw (escapes undecoded): `'it\'s'` and `"it's"` are the same value
            use crate::utils::sql_literal::cypher_string_value;
            Some(if cypher_string_value(a) == cypher_string_value(b) {
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

/// An operand `sql` of `outer`, parenthesized when it is a comparison, `IN`,
/// `IS [NOT] NULL` or `NOT` and `outer` is one of those (but `NOT`) or
/// arithmetic: SQL has no precedence that keeps Cypher's grouping there
/// (ClickHouse reads `a = b < c` as `(a = b) < c`, `a < b IN l` as
/// `(a < b) IN l`, `NOT a = b` as `NOT (a = b)`).
fn operand_sql(outer: Operator, operand: &RenderExpr, sql: String) -> String {
    use Operator as O;
    let loose = |o: Operator| {
        matches!(
            o,
            O::Equal
                | O::NotEqual
                | O::LessThan
                | O::GreaterThan
                | O::LessThanEqual
                | O::GreaterThanEqual
                | O::In
                | O::NotIn
                | O::IsNull
                | O::IsNotNull
        )
    };
    let arithmetic = matches!(
        outer,
        O::Addition
            | O::Subtraction
            | O::Multiplication
            | O::Division
            | O::ModuloDivision
            | O::Exponentiation
    );
    match operand {
        RenderExpr::OperatorApplicationExp(inner)
            if (loose(outer) || arithmetic)
                && (loose(inner.operator) || inner.operator == O::Not) =>
        {
            format!("({sql})")
        }
        _ => sql,
    }
}

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

/// True when `e` is a compile-time INTEGER constant: an integer literal, or the
/// unary-minus lowering `0 - <integer constant>` the parser emits for `-7`
/// (`Subtraction(Integer(0), …)`). Used to classify integer division operands
/// (see `render_integer_division`). Deliberately does NOT recurse into general
/// arithmetic — only literals and negation — so it stays a conservative,
/// type-certain check; a column or computed operand is never an integer constant
/// here (its type isn't known at render time, #847).
pub(crate) fn is_integer_constant(e: &RenderExpr) -> bool {
    match e {
        RenderExpr::Literal(Literal::Integer(_)) => true,
        RenderExpr::OperatorApplicationExp(op)
            if op.operator == Operator::Subtraction
                && op.operands.len() == 2
                && matches!(&op.operands[0], RenderExpr::Literal(Literal::Integer(0))) =>
        {
            is_integer_constant(&op.operands[1])
        }
        _ => false,
    }
}

/// Integer division: Cypher `int / int` truncates toward zero (`7/2 = 3`,
/// `-7/2 = -3`), only becoming float division when an operand is a float. CH/
/// Spark `/` is always float division, so `7/2` wrongly yields `3.5`. When BOTH
/// operands are integer CONSTANTS (literals or the `-n` negation form) we emit
/// `intDiv(a, b)` (CH) / `div(a, b)` (Spark) — both truncate toward zero,
/// matching Neo4j. Bounded to integer constants: a column's type isn't known at
/// render time (the schema carries no column types), so `u.a / u.b` can't be
/// classified here and stays `/` (tracked in #847). `rendered` holds the
/// already-rendered operand SQL.
fn render_integer_division(op: &OperatorApplication, rendered: &[String]) -> Option<String> {
    if op.operator != Operator::Division || op.operands.len() != 2 || rendered.len() != 2 {
        return None;
    }
    if !op.operands.iter().all(is_integer_constant) {
        return None;
    }
    let int_div =
        crate::sql_generator::function_mapper::current_function_mapper().integer_division();
    Some(format!("{}({}, {})", int_div, rendered[0], rendered[1]))
}

/// Render the Cypher exponentiation operator `a ^ b`. Neither ClickHouse nor
/// Databricks/Spark has an infix `^` (ClickHouse errors Code 62 on a bare `^`);
/// both accept the ANSI `POWER(base, exp)` call form, routed through the
/// function mapper (Rule #7). The call form is self-delimiting, so no
/// precedence parens are needed around it or its operands.
fn render_exponentiation(op: &OperatorApplication, rendered: &[String]) -> Option<String> {
    if op.operator != Operator::Exponentiation || rendered.len() != 2 {
        return None;
    }
    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
    Some(mapper.power(&rendered[0], &rendered[1]))
}

/// Is `expr` a non-negative integer literal? Cypher list-range bounds that are
/// plain non-negative constants need no from-the-end normalization, so the slice
/// renderer keeps them on the simple `bound (+ 1)` path — which also keeps the
/// existing positive-slice goldens byte-identical.
pub(crate) fn is_nonneg_int_literal(expr: &RenderExpr) -> bool {
    matches!(expr, RenderExpr::Literal(Literal::Integer(n)) if *n >= 0)
}

/// Normalize one Cypher list-slice bound into a non-negative index usable by
/// `arraySlice`/`slice`. Cypher allows negative bounds that count from the end
/// (`list[-2..]`) and clamps out-of-range bounds to the array edge, whereas the
/// ClickHouse/Spark array-slice offset has no from-the-end meaning. A runtime or
/// negative bound `i` is therefore rewritten to `i >= 0 ? i : greatest(len + i,
/// 0)`, where `len` is the dialect's array-length call (Rule #7, via
/// [`FunctionMapper::array_length`]). A statically non-negative integer literal
/// is returned verbatim so positive slices render exactly as before.
/// `bound_sql`/`array_sql` are pre-rendered SQL fragments; `array_sql` is
/// re-evaluated inside the guard (fine for the column/literal arrays slicing
/// targets in practice, matching the existing Spark 2-arg `array_slice` form).
pub(crate) fn normalize_slice_bound(
    bound_sql: &str,
    array_sql: &str,
    is_nonneg_literal: bool,
) -> String {
    if is_nonneg_literal {
        return bound_sql.to_string();
    }
    let len =
        crate::sql_generator::function_mapper::current_function_mapper().array_length(array_sql);
    format!(
        "if(({b}) >= 0, ({b}), greatest({len} + ({b}), 0))",
        b = bound_sql,
        len = len
    )
}

/// True for the Cypher temporal-component functions (`year`/`month`/`day`/…)
/// whose registry `arg_transform` is `wrap_epoch_millis_arg`. Used by the #854
/// Date/DateTime-arg fast-path in the `ScalarFnCall` renderer to decide whether
/// to skip the epoch-millis wrap. Kept in lockstep with the `Some(wrap_epoch_millis_arg)`
/// entries in `function_registry.rs`.
fn is_temporal_component_fn(fn_lower: &str) -> bool {
    matches!(
        fn_lower,
        "year"
            | "month"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "dayofweek"
            | "dayofyear"
            | "quarter"
            | "week"
    )
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
        // #854: when the non-interval operand is a native Date/DateTime (a
        // `date(…)`/`datetime(…)` constructor or a declared Date/DateTime column),
        // there is no epoch-millis round-trip to do — CH/Spark add an interval to
        // a date/datetime directly and return a date/datetime. Emit
        // `{temporal} +/- {interval}` verbatim; wrapping it in
        // `fromUnixTimestamp64Milli` (as the epoch path below does) would reject a
        // Date arg (Code 43). Gated on the structured classifier so epoch-millis
        // BIGINT and unknown operands still take the round-trip path (unchanged).
        let temporal_operand_idx = op.operands.iter().position(|o| {
            matches!(
                super::type_inference::infer_render_type(o),
                Some(super::type_inference::RenderType::Date)
                    | Some(super::type_inference::RenderType::DateTime)
            )
        });
        if let Some(idx) = temporal_operand_idx {
            // The other operand is the interval term.
            if idx < rendered.len() && !is_interval(&rendered[idx]) {
                let sql_op = if op.operator == Operator::Addition {
                    "+"
                } else {
                    "-"
                };
                let (lhs, rhs) = (&rendered[0], &rendered[1]);
                return Some(format!("{} {} {}", lhs, sql_op, rhs));
            }
        }

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
///
/// Does the RHS of an `IN`/`NOT IN` contain an explicit `null` list element?
/// openCypher's `IN` is three-valued: when the probe matches no non-null
/// element AND the list holds an unknown (null), the result is `null`, not
/// `false` (`3 IN [1,2,null]` → null; `3 NOT IN [1,null]` → null). ClickHouse's
/// `x IN (…)` / `x IN [array]` treat a null element as a plain non-match (→ 0),
/// losing that. When this returns true the caller routes the predicate through
/// the OR/AND element-wise expansion instead, where a `x = null` / `x <> null`
/// term propagates the null exactly per three-valued logic (verified on CH).
/// A null-free list is unaffected (keeps the byte-stable `IN` form).
fn in_list_has_null_literal(op: &OperatorApplication) -> bool {
    matches!(op.operator, Operator::In | Operator::NotIn)
        && matches!(&op.operands.get(1),
            Some(RenderExpr::List(items))
                if items.iter().any(|i| matches!(i, RenderExpr::Literal(Literal::Null))))
}

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
/// `collect`/`groupArray` aggregate, or a CTE column known to hold a
/// collection (e.g. `WITH collect(post) AS posts`). Returns `None` outside Databricks (CH unchanged).
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

/// #962: ClickHouse UTF8 upgrade for the OVERLOADED string/array functions
/// `size`→`length` and `reverse`. ClickHouse's `length`/`reverse` are
/// byte-based on strings (silently wrong on multi-byte UTF-8 — `size('héllo')`
/// counts 6 bytes, not 5 codepoints; `reverse('héllo')` byte-reverses into
/// garbage), but `lengthUTF8`/`reverseUTF8` are STRING-ONLY (reject arrays).
/// So — unlike the string-only functions fixed at the registry level in #960 —
/// we can only upgrade when the argument is PROVEN to be a string. An array or
/// an unknown-typed argument keeps the plain `length`/`reverse` (correct for
/// arrays; byte-identical to pre-#962 for unknowns, so no regression and never
/// breaks an array). ClickHouse only — Spark's `length`/`reverse` are already
/// codepoint-based (and `size` there is separately dispatched to Spark `size`
/// for collections via [`databricks_size_name`]).
pub(crate) fn clickhouse_utf8_string_fn(
    fn_name_lower: &str,
    arg: Option<&RenderExpr>,
    dialect: crate::sql_generator::SqlDialect,
) -> Option<&'static str> {
    use crate::sql_generator::SqlDialect;
    if dialect == SqlDialect::Databricks {
        return None;
    }
    let arg = arg?;
    let is_string = super::type_inference::infer_render_type(arg)
        == Some(super::type_inference::RenderType::String);
    if !is_string {
        return None;
    }
    match fn_name_lower {
        "size" => Some("lengthUTF8"),
        "reverse" => Some("reverseUTF8"),
        _ => None,
    }
}

/// True when a `size()` argument is recognizably a collection (vs a string).
fn render_arg_is_collection(arg: &RenderExpr) -> bool {
    fn name_is_collection(name: &str) -> bool {
        // The array-CTE-column set collected from the plan (carried-forward
        // collect()/groupArray columns).
        crate::server::query_context::is_array_cte_column(name)
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
        } else if join.table_name.trim_start().starts_with('(') {
            // #616: the combined-anchor fold (plan_optimizer.rs
            // `fold_optional_edge_node_join_with_predicate`) rewrites the
            // optional node's join `table_name` into a subquery string like
            // `(SELECT … FROM edge AS t1 JOIN social.users_bench AS b ON … WHERE
            // <pred>)`, aliased `b`. That hides `b`'s node table from the
            // exact-match above, so `b`'s label goes unknown and the render-time
            // generic `.id` resolver falls back to an alphabetical scan of all
            // node schemas — picking the WRONG label (`Post.post_id` instead of
            // `User.user_id`). Recover the label from the subquery's inner node
            // JOIN.
            //
            // SOUNDNESS: only scan the `FROM … JOIN … ON` PREFIX (everything
            // before the first ` WHERE `). The WHERE predicate can contain user
            // string literals that spoof the magic substring (e.g.
            // `WHERE b.name = 'posts_bench AS b '`) — matching against those
            // would pick a non-deterministic wrong label (the scan iterates a
            // randomized HashMap). The generated inner JOIN is always
            // `JOIN {qualified} AS {alias} ON …` and always precedes the WHERE,
            // so the prefix is literal-free ground truth. Match the exact
            // `JOIN {qualified} AS {alias} ON` form (not a bare substring).
            let join_prefix = join
                .table_name
                .split(" WHERE ")
                .next()
                .unwrap_or(&join.table_name);
            for (qualified, label) in &table_to_label {
                if join_prefix.contains(&format!("JOIN {} AS {} ON", qualified, join.table_alias)) {
                    map.insert(join.table_alias.clone(), label.clone());
                    break;
                }
            }
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

/// Merge `new_cte` into `existing` by name, mirroring the collision-handling
/// rules `plan_builder.rs`'s per-branch UNION assembly already uses (kept in
/// sync intentionally — see #557):
/// - No existing CTE with this name → push as-is.
/// - Existing is an empty VLP placeholder (`WHERE 0 = 1`) and the new one
///   isn't → replace it (a real branch superseding a pruned/empty one).
/// - The new one is empty and the existing one isn't → keep the existing,
///   drop the new (mirrors the same "don't overwrite a real CTE with an
///   empty placeholder" rule, the other direction).
/// - Same name, identical SQL → true duplicate (e.g. the same VLP CTE
///   legitimately shared by multiple UNION branches); skip.
/// - Same name, DIFFERENT SQL → this is #557's case: a Union of per-candidate-
///   end-label branches (`generate_union_for_untyped_nodes`/`clone_plan_with_labels`)
///   each independently compute the SAME formulaic CTE name
///   (`vlp_multi_type_{start}_{end}` — the formula only depends on the
///   Cypher aliases, not the per-branch end-label restriction), but generate
///   DIFFERENT CTE bodies (one scoped to each candidate end label). Rename
///   the new one with a numeric suffix so both survive — `extract_union`/
///   `extract_union_with_ctx` (`plan_builder.rs`) already computes this exact
///   same renamed reference independently (same algorithm, same input order)
///   for the branch's FROM clause, so the two stay consistent without needing
///   to thread the rename back through here.
///
/// Returns `Some((old_name, new_name))` when a rename occurred, for callers
/// that also need to fix up a FROM/JOIN reference to the renamed CTE.
pub(crate) fn merge_cte_deduping_by_name_content(
    existing: &mut Vec<Cte>,
    new_cte: Cte,
) -> Option<(String, String)> {
    let Some(existing_idx) = existing.iter().position(|e| e.cte_name == new_cte.cte_name) else {
        existing.push(new_cte);
        return None;
    };

    let is_empty_placeholder = |content: &CteContent| matches!(content, CteContent::RawSql(s) if s.contains("WHERE 0 = 1"));
    let existing_is_empty = is_empty_placeholder(&existing[existing_idx].content);
    let new_is_empty = is_empty_placeholder(&new_cte.content);

    if existing_is_empty && !new_is_empty {
        existing[existing_idx] = new_cte;
        return None;
    }
    if new_is_empty {
        return None;
    }

    let same_content = match (&existing[existing_idx].content, &new_cte.content) {
        (CteContent::RawSql(a), CteContent::RawSql(b)) => a == b,
        _ => false,
    };
    if same_content {
        return None;
    }

    let base_name = new_cte.cte_name.clone();
    let mut suffix = 2;
    let mut new_name = format!("{}_{}", base_name, suffix);
    while existing.iter().any(|e| e.cte_name == new_name) {
        suffix += 1;
        new_name = format!("{}_{}", base_name, suffix);
    }
    let mut renamed_cte = new_cte;
    renamed_cte.cte_name = new_name.clone();
    // A VLP (and similar) CTE's RawSql content EMBEDS its own name — both the
    // `<old> AS (` header (the `Cte::to_sql` RawSql arm emits the content
    // verbatim when it already contains ` AS (`) and the recursive self-reference
    // `FROM <old> vp`. Renaming only `cte_name` would leave the emitted SQL still
    // saying `<old> AS (…)` (a duplicate definition → ClickHouse Code 179) and
    // the self-ref pointing at the OTHER arm's CTE. Rewrite the embedded name so
    // the definition and its self-reference match the new name (#618).
    rewrite_cte_self_name_in_raw_sql(&mut renamed_cte.content, &base_name, &new_name);
    existing.push(renamed_cte);
    Some((base_name, new_name))
}
/// Rewrite a CTE's OWN name where it appears embedded inside its `RawSql`
/// content: the `<old> AS (` definition header and the recursive self-reference
/// `FROM <old> <alias>`. Only STRUCTURAL positions are rewritten — a leading
/// `<old> AS (` header and `<old>` immediately after a `FROM`/`JOIN` keyword —
/// and never inside a single-quoted string literal. This deliberately does NOT
/// do a blanket identifier replace: a VLP body can legitimately contain the
/// text of the CTE name inside a string literal (e.g. `WHERE name = 'vlp_a_b'`),
/// and corrupting that would be a silent wrong result. No-op for `Structured`
/// content (which carries its name in `cte_name`, not in text). See #618.
pub(crate) fn rewrite_cte_self_name_in_raw_sql(content: &mut CteContent, old: &str, new: &str) {
    let CteContent::RawSql(sql) = content else {
        return;
    };
    // #642: a VLP CTE's RawSql can bundle a whole FAMILY of derived sub-CTEs —
    // `<old>_inner`, `<old>_to_target`, `<old>_bfs`, `<old>_recon`,
    // `<old>_reachable` (shortestPath/BFS/min-hops wrappers) — all defined inside
    // this ONE blob and cross-referencing each other. Renaming only `<old>`→`<new>`
    // (the #618 single-CTE case) leaves those derived names intact, so a second
    // union arm with the same aliases re-defines e.g. `<old>_inner` → Code 179.
    //
    // Discover the derived names actually present as `<old>_<suffix> AS (` headers
    // in this blob and rename each `<old>_<suffix>`→`<new>_<suffix>` FIRST (longest
    // first, so `<old>_inner` is handled before the bare `<old>` — otherwise the
    // whole-identifier boundary would already have stopped `<old>` from matching
    // inside `<old>_inner`, but doing the derived names first is clearest and
    // order-independent since each rewrite targets a distinct whole identifier).
    let mut derived = discover_derived_cte_names(sql, old);
    // Longest suffix first for determinism (no nesting among siblings, but stable).
    derived.sort_by_key(|b| std::cmp::Reverse(b.len()));
    for derived_old in derived {
        let derived_new = format!("{}{}", new, &derived_old[old.len()..]);
        *sql = rewrite_cte_name_structural(sql, &derived_old, &derived_new);
    }
    *sql = rewrite_cte_name_structural(sql, old, new);
}

/// #642: find the derived sub-CTE names (`<base>_<suffix>`) DEFINED as headers
/// (`<base>_<suffix> AS (`) inside a VLP RawSql blob. Only names that appear as an
/// actual CTE definition header are returned, so we never rename a coincidental
/// identifier.
///
/// TWO-LAYER SAFETY: this discovery gate is deliberately NOT the safety boundary —
/// even if it over-includes a name, the actual rewrite goes through
/// `rewrite_cte_name_structural`, which only touches header / post-`FROM` /
/// post-`JOIN` positions and is string-literal-aware. So an over-detected name at
/// worst re-confirms a rename that is already correct; it can never corrupt a
/// column, alias, or literal. The gate requires `AS (` (a real definition, not an
/// `AS <alias>` reference) to keep discovery tight regardless. String-literal spans are skipped (a literal containing
/// `foo_inner AS (` must not be mistaken for a header).
fn discover_derived_cte_names(sql: &str, base: &str) -> Vec<String> {
    if base.is_empty() {
        return Vec::new();
    }
    let bytes = sql.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let prefix = format!("{}_", base);
    let mut names = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut i = 0;
    let mut in_string = false;
    while i < sql.len() {
        let b = bytes[i];
        if in_string {
            // A backslash escapes the next byte (`\'` is a quote).
            if b == b'\\' && i + 1 < sql.len() {
                i += 2;
                continue;
            }
            if b == b'\'' {
                if i + 1 < sql.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            in_string = true;
            i += 1;
            continue;
        }
        // Match `<base>_<suffix>` as a whole identifier at a header position.
        if sql[i..].starts_with(&prefix) {
            let before_ok = i == 0 || !is_ident(bytes[i - 1]);
            if before_ok {
                // Consume the full identifier `<base>_<ident-chars>`.
                let mut end = i + prefix.len();
                while end < sql.len() && is_ident(bytes[end]) {
                    end += 1;
                }
                let candidate = &sql[i..end];
                // Only accept it if this occurrence is a DEFINITION header:
                // `<name> AS (` — `AS` followed (after optional whitespace) by `(`.
                // A CTE definition is ALWAYS `AS (`; an aliased table reference is
                // `AS <alias>` (no paren) or a bare `<name> <alias>` — neither of
                // which we rename. Requiring the `(` avoids mistaking an aliased
                // reference for a definition (defense-in-depth: no VLP template
                // currently emits `<derived> AS <alias>`, but this keeps discovery
                // strictly limited to real definitions).
                let rest = sql[end..].trim_start();
                let is_header = {
                    let low = rest.to_ascii_lowercase();
                    low.strip_prefix("as")
                        .map(|after| after.trim_start())
                        .is_some_and(|after| after.starts_with('('))
                };
                if is_header && candidate.len() > prefix.len() && seen.insert(candidate.to_string())
                {
                    names.push(candidate.to_string());
                }
                i = end;
                continue;
            }
        }
        let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        i += ch_len;
    }
    names
}
/// Rewrite `old`→`new` only where `old` is a CTE reference: the definition
/// header (`old AS (`, possibly leading the string or after whitespace) or a
/// table source (`FROM old`/`JOIN old`). Matches only at whole-identifier
/// boundaries and NEVER inside a single-quoted string literal. Everything else
/// — string literals, column names, comments — is left byte-for-byte intact.
fn rewrite_cte_name_structural(sql: &str, old: &str, new: &str) -> String {
    if old.is_empty() {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_string = false; // inside a single-quoted literal
    while i < sql.len() {
        let b = bytes[i];
        if in_string {
            // Copy verbatim until the closing quote (handle '' escape).
            if !b.is_ascii() {
                // multi-byte char: copy it whole (a per-byte `as char` would mangle it)
                let ch = sql[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
                continue;
            }
            out.push(b as char);
            // A backslash escapes the next byte (`\'` is a quote).
            if b == b'\\' && i + 1 < sql.len() {
                // sql is indexed by bytes here; copy the escaped char whole
                let ch = sql[i + 1..].chars().next().unwrap();
                out.push(ch);
                i += 1 + ch.len_utf8();
                continue;
            }
            if b == b'\'' {
                if i + 1 < sql.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            in_string = true;
            out.push('\'');
            i += 1;
            continue;
        }
        // Try to match `old` as a whole identifier at position i.
        if sql[i..].starts_with(old) {
            let before_ok = i == 0 || !is_ident(bytes[i - 1]);
            let after_idx = i + old.len();
            let after_ok = after_idx >= sql.len() || !is_ident(bytes[after_idx]);
            if before_ok && after_ok {
                // Structural gate: the identifier must be a CTE reference —
                // either the leading `<old> AS (` header (preceded only by
                // whitespace/start-of-string), or preceded by a FROM/JOIN
                // keyword. Anything else (a bare column, an aliased projection)
                // is left untouched.
                let preceding = out.trim_end();
                let is_header =
                    preceding.is_empty() || preceding.ends_with(',') || preceding.ends_with('(');
                let after_kw = {
                    let low = preceding.to_ascii_lowercase();
                    low.ends_with("from") || low.ends_with("join")
                };
                if is_header || after_kw {
                    out.push_str(new);
                    i = after_idx;
                    continue;
                }
            }
        }
        // Advance one UTF-8 char (CTE SQL is ASCII, but stay safe).
        let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        out.push_str(&sql[i..i + ch_len]);
        i += ch_len;
    }
    out
}

/// Recursively collect all CTE definitions from a RenderPlan tree,
/// removing them from their nested locations (union branches, CTE content, etc.).
///
/// Same-named CTEs are merged through `merge_cte_deduping_by_name_content`
/// rather than pushed verbatim (see #567): a Union of per-candidate-end-label
/// branches (unlabeled multi-type VLP end node) can have independent branches
/// each compute the SAME formulaic CTE name while generating DIFFERENT CTE
/// bodies (each scoped to its own candidate end label). This mirrors the fix
/// #557 already applied to the ctx-aware path's `extract_ctes_with_context`
/// Union arm (`cte_extraction.rs`) — naive keep-first-by-name dedup there
/// silently dropped a real branch's CTE, leaving the outer query referencing
/// an undefined table. This ctx-less path (reachable via `to_render_plan`,
/// e.g. from EXISTS subqueries in `render_expr.rs`) had the same gap.
fn collect_nested_ctes(plan: &mut RenderPlan, collected: &mut Vec<Cte>) {
    // Take CTEs from this plan level
    let ctes = std::mem::take(&mut plan.ctes.0);
    for mut cte in ctes {
        // Recursively flatten CTEs inside Structured CTE content
        if let CteContent::Structured(ref mut inner_plan) = cte.content {
            collect_nested_ctes(inner_plan, collected);
        }
        // A rename means this plan level's own CTE name no longer matches
        // what's in the flat list — fix up this level's FROM reference to
        // match (mirrors the equivalent fixup already done for the ctx-aware
        // path in `to_render_plan_with_ctx`'s Union arm, plan_builder.rs).
        // Unlike that path, nothing upstream of this ctx-less flatten step
        // has already resolved the collision, so the fixup must happen here.
        if let Some((old_name, new_name)) = merge_cte_deduping_by_name_content(collected, cte) {
            if let Some(ref mut from_ref) = plan.from.0 {
                if from_ref.name == old_name {
                    from_ref.name = new_name;
                }
            }
        }
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
/// dependency order — no additional sorting needed. Same-name collisions are
/// already resolved (merged or renamed) during collection, so no separate
/// name-keyed dedup pass is needed here.
fn flatten_all_ctes(plan: &mut RenderPlan) {
    let mut collected = Vec::new();
    collect_nested_ctes(plan, &mut collected);

    if collected.is_empty() {
        return;
    }

    plan.ctes.0 = collected;
}

/// P-4c (`EXPLICIT_SCOPE.md` §4.14): print a plan produced by the bound-plan
/// lowering: CTE flattening, then the SQL text. The lowering emits joins in
/// dependency order and every column reference already names its element's
/// alias and physical column.
///
/// Supports the plan shapes the lowering produces today: SELECTs with FROM /
/// JOINs / ARRAY JOIN / WHERE / GROUP BY / HAVING / ORDER BY / SKIP / LIMIT,
/// the WITH bodies as flat CTEs before the final one, and no UNION.
pub fn render_plan_to_sql_plain(plan: RenderPlan) -> String {
    let (ctes, select) = render_plan_parts_plain(plan);
    let mut sql = String::new();
    let recursive = ctes.iter().any(|c| c.recursive);
    for (i, cte) in ctes.iter().enumerate() {
        sql.push_str(match (i, recursive) {
            (0, true) => "WITH RECURSIVE ",
            (0, false) => "WITH ",
            _ => ", \n",
        });
        sql.push_str(&cte.text);
    }
    if !sql.is_empty() {
        sql.push('\n');
    }
    sql.push_str(&select);
    sql
}

/// One CTE of a lowered plan as [`render_plan_to_sql_plain`] writes it.
pub struct PlainCte {
    pub name: String,
    /// `name AS (…)`; a path relation's text also defines the helper CTEs
    /// it uses.
    pub text: String,
    pub recursive: bool,
}

/// A lowered plan's CTEs, in dependency order, and its final SELECT, as
/// SQL ([`render_plan_to_sql_plain`] joins them).
pub fn render_plan_parts_plain(mut plan: RenderPlan) -> (Vec<PlainCte>, String) {
    assert!(
        plan.union.0.is_none(),
        "render_plan_to_sql_plain: UNION is not lowered yet"
    );
    flatten_all_ctes(&mut plan);
    // The Databricks `size()` render picks Spark `size` for a list-valued
    // column (see `databricks_size_name`); the plan's list columns are read
    // off the plan itself.
    struct ArrayColsGuard(HashSet<String>);
    impl Drop for ArrayColsGuard {
        fn drop(&mut self) {
            crate::server::query_context::set_array_cte_columns(std::mem::take(&mut self.0));
        }
    }
    let _array_cols_guard = ArrayColsGuard(crate::server::query_context::get_array_cte_columns());
    crate::server::query_context::set_array_cte_columns(plain_list_columns(&plan));
    // A lowered plan's CTEs, in dependency order, are WITH bodies (plain
    // SELECTs) and the path relations (`vlp_…`: the recursive searches and
    // the selections over them), whose text (`name AS (…)`, with the helper
    // CTEs a path uses) comes from the path generators.
    let mut parts = Vec::new();
    for cte in std::mem::take(&mut plan.ctes.0) {
        let text = match cte.content {
            CteContent::Structured(body) => {
                assert!(
                    !cte.is_recursive && body.ctes.0.is_empty(),
                    "render_plan_to_sql_plain: a CTE body is plain SELECTs"
                );
                format!("{} AS (\n{})", cte.cte_name, plain_body_sql(*body))
            }
            CteContent::RawSql(text) => {
                assert!(
                    cte.cte_name.starts_with("vlp_") && text.starts_with(&cte.cte_name),
                    "render_plan_to_sql_plain: a raw-SQL CTE is a path relation"
                );
                text
            }
        };
        parts.push(PlainCte {
            name: cte.cte_name,
            text,
            recursive: cte.is_recursive,
        });
    }
    (parts, plain_select_sql(plan))
}

/// One expression of a lowered plan as SQL (its column references are final,
/// as in [`render_plan_to_sql_plain`]): a condition handed to a generator
/// that takes SQL text (the recursive path CTE).
/// Cypher `x IN list` / `x NOT IN list` whose list is an expression (a
/// comprehension, a function's list), not a list of items: as the dialect
/// spells it (`FunctionMapper::in_list_expression`; `None`: as `IN`).
fn in_list_expression_sql(
    operator: &Operator,
    operands: &[RenderExpr],
    rendered: &[String],
) -> Option<String> {
    let negated = match operator {
        Operator::In => false,
        Operator::NotIn => true,
        _ => return None,
    };
    match (operands, rendered) {
        ([_, list], [value, list_sql])
            if !matches!(list, RenderExpr::List(_) | RenderExpr::Literal(_)) =>
        {
            crate::sql_generator::function_mapper::current_function_mapper()
                .in_list_expression(value, list_sql, negated)
        }
        _ => None,
    }
}

pub fn render_expr_to_sql_plain(e: &RenderExpr) -> String {
    e.to_sql()
}

/// Sort keys of a lowered plan as SQL (`a ASC, b DESC`, with the NULL
/// placement of [`OrderByItems`]), for a window's `ORDER BY`.
pub fn order_keys_to_sql_plain(keys: &[OrderByItem]) -> String {
    let sql = OrderByItems(keys.to_vec()).to_sql();
    sql.trim_start_matches("ORDER BY ").trim_end().to_string()
}

/// The list-valued CTE columns of a lowered plan. Its column names are
/// unique in the query (`v{N}`, `p{len}_v{N}_<prop>`), so a name identifies
/// one column. A column is a list when its value is a list literal, a
/// collecting aggregate (also inside the lowering's `coalesce(…, [])` and
/// `CASE` folds), or an earlier list column passed through.
fn plain_list_columns(plan: &RenderPlan) -> HashSet<String> {
    fn is_list(e: &RenderExpr, lists: &HashSet<String>) -> bool {
        match e {
            RenderExpr::List(_) => true,
            RenderExpr::AggregateFnCall(a) => is_collection_aggregate(&a.name),
            RenderExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("coalesce") => {
                f.args.iter().any(|a| is_list(a, lists))
            }
            RenderExpr::Case(c) => c
                .when_then
                .iter()
                .map(|(_, t)| t)
                .chain(c.else_expr.as_deref())
                .any(|t| is_list(t, lists)),
            RenderExpr::PropertyAccessExp(pa) => lists.contains(pa.column.raw()),
            _ => false,
        }
    }
    let mut lists = HashSet::new();
    let bodies = plan.ctes.0.iter().filter_map(|c| match &c.content {
        CteContent::Structured(body) => Some(body.as_ref()),
        CteContent::RawSql(_) => None,
    });
    for body in bodies.chain(std::iter::once(plan)) {
        for item in &body.select.items {
            if let Some(alias) = &item.col_alias {
                if is_list(&item.expression, &lists) {
                    lists.insert(alias.0.clone());
                }
            }
        }
    }
    lists
}

/// A CTE body of a lowered plan: one plain SELECT, or the `UNION ALL` of
/// plain SELECTs (the body's `union` holds them; it selects nothing itself).
fn plain_body_sql(body: RenderPlan) -> String {
    let Some(union) = body.union.0 else {
        return plain_select_sql(body);
    };
    assert!(
        body.select.items.is_empty()
            && body.from.0.is_none()
            && union.union_type == UnionType::All
            && union
                .input
                .iter()
                .all(|arm| arm.union.0.is_none() && arm.ctes.0.is_empty()),
        "render_plan_to_sql_plain: a union body is UNION ALL of plain SELECTs"
    );
    union
        .input
        .into_iter()
        .map(plain_select_sql)
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n")
}

/// One SELECT of a lowered plan: its own alias scope (result typing),
/// duplicate result aliases disambiguated, and no repair pass.
fn plain_select_sql(mut plan: RenderPlan) -> String {
    activate_scope_context(&plan.from, &plan.joins);
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for item in plan.select.items.iter_mut() {
        if let Some(ref mut alias) = item.col_alias {
            let count = seen.entry(alias.0.clone()).or_insert(0);
            *count += 1;
            if *count > 1 {
                alias.0 = format!("{}_{}", alias.0, count);
            }
        }
    }
    let mut sql = String::new();
    sql.push_str(&plan.select.to_sql());
    sql.push_str(&plan.from.to_sql());
    sql.push_str(&plan.joins.to_sql());
    // An UNWIND (`Lowerer::unwind`): ONE clause of its lists, read in step
    // (`FunctionMapper::array_join_clause`), after the joins.
    if !plan.array_join.0.is_empty() {
        let lists: Vec<(String, String, Option<String>)> = plan
            .array_join
            .0
            .iter()
            .map(|a| (a.expression.to_sql(), a.alias.clone(), a.position.clone()))
            .collect();
        sql.push_str(
            &crate::sql_generator::function_mapper::current_function_mapper()
                .array_join_clause(&lists),
        );
    }
    sql.push_str(&plan.filters.to_sql());
    sql.push_str(&plan.group_by.to_sql());
    if let Some(having) = &plan.having_clause {
        sql.push_str("HAVING ");
        sql.push_str(&having.to_sql());
        sql.push('\n');
    }
    // As in `render_plan_to_sql`: Databricks resolves a DISTINCT query's
    // ORDER BY against the SELECT aliases.
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
    sql.push_str(&limit_offset_clause(plan.skip.0, plan.limit.0));
    sql
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

            // #1057: a projection item whose Cypher-semantic type is Boolean
            // must surface on the wire as true/false, not ClickHouse's UInt8
            // 1/0 (comparisons, IN, STARTS WITH, IS NULL, boolean fns, and
            // schema-declared Boolean columns all render as UInt8). Wrap ONLY
            // when the render-site classifier PROVES the top-level item is
            // Boolean (conservative-None: unknown stays unchanged, so a
            // comparison nested inside if()/CASE/a lambda condition — which is
            // not itself the item's type — is untouched). The `.*`-expanded
            // TableAlias forms never classify Boolean, so they are unaffected.
            let rendered_expr = if super::type_inference::infer_render_type(&item.expression)
                == Some(super::type_inference::RenderType::Boolean)
            {
                crate::sql_generator::function_mapper::current_function_mapper()
                    .cast_bool(&rendered_expr)
            } else {
                rendered_expr
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

            // The explicit alias, else the VLP CTE's FROM alias.
            let alias = match &view_ref.alias {
                Some(explicit_alias) => explicit_alias.clone(),
                None => crate::query_planner::join_context::VLP_CTE_FROM_ALIAS.to_string(),
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
        // #1065: Neo4j NULL placement (see impl ToSql for OrderByItems).
        sql.push_str(mapper.order_by_nulls_clause(matches!(item.order, OrderByOrder::Desc)));
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
            // #1065: pin NULL placement to Neo4j semantics (nulls last on ASC,
            // first on DESC) — a bare ASC/DESC inherits each backend's own
            // divergent default. Dialect-minimal via the FunctionMapper.
            sql.push_str(
                crate::sql_generator::function_mapper::current_function_mapper()
                    .order_by_nulls_clause(matches!(item.order, OrderByOrder::Desc)),
            );
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
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

        let mut sql = format!(
            "{} {} AS {}",
            join_type_str, qualified_table_name, self.table_alias
        );
        // A dialect without FINAL (Databricks) reads the table as it is, as
        // for the FROM table.
        if self.use_final
            && crate::server::query_context::get_current_dialect().supports_final_keyword()
        {
            sql.push_str(" FINAL");
        }

        // Only add ON clause if there are joining conditions
        if !self.joining_on.is_empty() {
            // Conditions are AND-joined below. When there is more than one, a
            // condition that is itself a top-level `AND`/`OR` (e.g. a cross-alias
            // `OR` predicate moved into the ON by the #462 post-WITH OPTIONAL fix)
            // must be parenthesized, or `key AND a OR b` would mis-parse as
            // `(key AND a) OR b`. `OperatorApplication::to_sql` does not wrap
            // AND/OR (unlike `RenderExpr::to_sql`), so wrap here at the join site.
            // A lone condition needs no wrapping (nothing is AND-joined onto it),
            // which keeps single composite-key `ON (a AND b)` joins paren-free.
            let multi = self.joining_on.len() > 1;
            let joining_on_str_vec: Vec<String> = self
                .joining_on
                .iter()
                .map(|cond| {
                    let s = cond.to_sql();
                    if multi && matches!(cond.operator, Operator::And | Operator::Or) {
                        format!("({})", s)
                    } else {
                        s
                    }
                })
                .collect();

            let joining_on_str = joining_on_str_vec.join(" AND ");

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
                Literal::String(s) => crate::utils::sql_literal::cypher_string_to_sql_literal(s),
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
                if let Some(millis) =
                    super::type_inference::epoch_millis_datetime(&fn_call.name, &fn_call.args)
                {
                    return millis.to_sql();
                }

                // Native-function pass-through, keyed by the active dialect
                // (`ch.` for ClickHouse, `dbx.` for Databricks). This arm returns
                // `String`, not `Result`, so a foreign-backend prefix can't be
                // surfaced as a clean error here — instead we emit the *original*
                // prefixed name (e.g. `ch.uniq(x)`) so the query surfaces a
                // database error on the unknown prefixed function rather than
                // silently dropping the prefix into a valid-looking call.
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

                // Cypher round() follows Neo4j's two-branch semantics (1-arg /
                // precision-0 = Math.round floor(x+0.5); 2-arg d!=0 = BigDecimal
                // HALF_UP away-from-zero), neither of which matches CH's native
                // HALF_EVEN round. Route through the dialect helper (CH gets the
                // Neo4j-faithful formula; Databricks stays a plain round()). The
                // 3-arg explicit-mode form falls through (helper returns None) to
                // native round, which fails loud (Code 42) — unsupported on CH.
                if fn_name_lower == "round" {
                    let args_sql: Vec<String> = fn_call.args.iter().map(|e| e.to_sql()).collect();
                    if let Some(sql) = super::common::round_half_up_sql(&args_sql) {
                        return sql;
                    }
                }

                // #880: Cypher `toInteger`/`toFloat` return NULL (not an error)
                // for an unparseable STRING. CH `toInt64`/`toFloat64` THROW
                // (Code 6) on a bad string, so when the argument is known to be
                // string-typed we route to the dialect's parse-or-null cast
                // (CH `toInt64OrNull`/`toFloat64OrNull`; Spark keeps the plain
                // null-on-failure cast). The OrNull variants accept ONLY a String
                // arg, so this fires strictly when the classifier proves the arg
                // is a String — a numeric/unknown arg falls through to the plain
                // cast below (byte-identical to today), preserving `toInteger(3.9)`
                // = 3 and never mis-parsing a numeric column.
                if (fn_name_lower == "tointeger" || fn_name_lower == "tofloat")
                    && fn_call.args.len() == 1
                    && super::type_inference::infer_render_type(&fn_call.args[0])
                        == Some(super::type_inference::RenderType::String)
                {
                    let arg_sql = fn_call.args[0].to_sql();
                    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
                    return if fn_name_lower == "tointeger" {
                        mapper.cast_int64_or_null(&arg_sql)
                    } else {
                        mapper.cast_float64_or_null(&arg_sql)
                    };
                }

                // #854: Cypher temporal component access (`.year`/`.month`/… →
                // `year(x)`/`month(x)`/…) on a native Date/DateTime arg must NOT
                // be epoch-wrapped. The registry `arg_transform`
                // (`wrap_epoch_millis_arg`) assumes every temporal arg is an
                // epoch-millis BIGINT and wraps it in `fromUnixTimestamp64Milli`,
                // which CH rejects (Code 43) on a real Date/DateTime. When the
                // classifier proves the arg is Date/DateTime (a `date(…)`/
                // `datetime(…)` constructor, or a declared Date/DateTime column),
                // emit `{toYear|…}({arg})` directly — no wrap. Epoch-millis BIGINT
                // and unknown args fall through to the registry path (wrap kept,
                // unchanged for LDBC-style epoch schemas).
                if is_temporal_component_fn(&fn_name_lower) && fn_call.args.len() == 1 {
                    if let Some(t) = super::type_inference::infer_render_type(&fn_call.args[0]) {
                        if matches!(
                            t,
                            super::type_inference::RenderType::Date
                                | super::type_inference::RenderType::DateTime
                        ) {
                            if let Some(mapping) = get_function_mapping(&fn_name_lower) {
                                let dialect = crate::server::query_context::get_current_dialect();
                                let arg_sql = fn_call.args[0].to_sql();
                                return format!("{}({})", mapping.name_for(dialect), arg_sql);
                            }
                        }
                    }
                }

                // #1055: Cypher `toString(<float>)` must keep a whole-valued
                // float's trailing `.0` (`toString(3.0)` → "3.0", `toString(3)`
                // → "3"). Two roots converge: whole-float LITERALS render as
                // integer SQL (`3.0` → `3`), and CH `toString(toFloat64(3))`
                // itself returns "3". Both are cured by gating on the classifier:
                // when the arg is provably `Float` (a float literal, `toFloat`,
                // a declared Float column), route to the dialect's float-aware
                // stringifier. An Integer/Number/unknown arg falls through to the
                // plain registry `toString` — so `toString(3)`, `toString(abs(x))`
                // (Num), and `toString(size(...))` (Int) are byte-identical to
                // today, and only a proven Float ever gains `.0`.
                if fn_name_lower == "tostring"
                    && fn_call.args.len() == 1
                    && super::type_inference::infer_render_type(&fn_call.args[0])
                        == Some(super::type_inference::RenderType::Float)
                {
                    let arg_sql = fn_call.args[0].to_sql();
                    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
                    return mapper.to_string_float(&arg_sql);
                }

                // ClickHouse `split(str, delim)`: Cypher split uses a literal
                // delimiter, but CH's splitByChar requires a single-byte constant
                // and splitByString returns [str] for an empty delimiter. Cover
                // empty / multi-byte / non-constant delimiters with a CASE that
                // splits into code points when the delimiter is empty. (#1418)
                if fn_name_lower == "split"
                    && fn_call.args.len() == 2
                    && matches!(
                        crate::server::query_context::get_current_dialect(),
                        crate::sql_generator::SqlDialect::ClickHouse
                    )
                {
                    let str_sql = fn_call.args[0].to_sql();
                    let delim_sql = fn_call.args[1].to_sql();
                    return format!(
                        "CASE WHEN {delim} = '' THEN arrayMap(i -> substring({s}, i, 1), range(1, lengthUTF8({s}) + 1)) ELSE splitByString({delim}, {s}) END",
                        delim = delim_sql,
                        s = str_sql,
                    );
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
                        // #962: on ClickHouse, `size`/`reverse` over a PROVEN
                        // string upgrade to the codepoint-based `lengthUTF8`/
                        // `reverseUTF8` (array/unknown args keep the plain name).
                        let fn_name = if fn_name_lower == "size" {
                            clickhouse_utf8_string_fn(&fn_name_lower, fn_call.args.first(), dialect)
                                .or_else(|| databricks_size_name(fn_call.args.first(), dialect))
                                .unwrap_or_else(|| mapping.name_for(dialect))
                        } else if fn_name_lower == "reverse" {
                            clickhouse_utf8_string_fn(&fn_name_lower, fn_call.args.first(), dialect)
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

                // percentileCont/Disc are parametric quantiles — render through the
                // dialect FunctionMapper, honoring the percentile arg (#639).
                {
                    let args_sql: Vec<String> = agg.args.iter().map(|e| e.to_sql()).collect();
                    if let Some(sql) =
                        super::common::try_render_percentile(&fn_name_lower, &args_sql)
                    {
                        return sql;
                    }
                }

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
                // A lowered plan's column references are final: the alias is
                // the element's own and the column is physical (P-4c).
                column.to_sql(&table_alias.0)
            }
            RenderExpr::OperatorApplicationExp(op) => {
                // ⚠️ TODO: Operator rendering consolidation (SQL-IR Phase 2 step 3).
                // This block duplicates the operator handling in to_sql.rs (~70 lines).
                // The former blocker — two distinct `Operator` types — is GONE: the
                // enums were unified (render_expr::Operator now re-exports
                // logical_expr::Operator, PR #818), so both blocks match on the SAME
                // enum. What remains: to_sql.rs walks `LogicalExpr` operands and
                // returns `Result`; this block walks `RenderExpr` operands and returns
                // `String`. Collapsing is now the Path D -> A retirement
                // (SQL_IR_DESIGN.md §3.5 step 3): bake LogicalExpr -> RenderExpr via
                // the existing `TryFrom<LogicalExpr>` at to_sql.rs's callers, then
                // render everything through this RenderExpr path.
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
                        Operator::Xor => "XOR", // Special handling below
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

                // Node identity comparison: Cypher `a = b` / `a <> b` where both
                // sides are bare node variables (TableAlias) compares node
                // identity — i.e. the node's ID column(s). Resolve each alias's
                // node_id from the schema (it may be a renamed column such as
                // `user_id`, or composite) rather than assuming a literal `id`
                // column, which usually does not exist and fails at execution
                // (ClickHouse "cannot be resolved" / Databricks UNRESOLVED_COLUMN).
                // ClickHouse doesn't understand bare table aliases as values.
                //
                // #1112: the same comparison in RETURN/projection position does
                // NOT arrive as two `TableAlias` operands — the planner lowers a
                // bare node variable there to a whole-node `PropertyAccess(alias,
                // "*")`. That form reached the generic emitter and rendered
                // `a.* = b.*`, which is not valid SQL. Accept BOTH spellings of
                // "a bare node reference" so WHERE- and RETURN-position identity
                // resolve identically.
                if matches!(op.operator, Operator::Equal | Operator::NotEqual)
                    && op.operands.len() == 2
                {
                    // A bare node reference: `TableAlias("a")` (predicate
                    // position) or `PropertyAccess("a", "*")` (projection
                    // position). Returns the alias.
                    fn bare_node_alias(e: &RenderExpr) -> Option<&str> {
                        match e {
                            RenderExpr::TableAlias(t) => Some(t.0.as_str()),
                            RenderExpr::PropertyAccessExp(p) if p.column.raw() == "*" => {
                                Some(p.table_alias.0.as_str())
                            }
                            _ => None,
                        }
                    }
                    if let (Some(la), Some(ra)) = (
                        bare_node_alias(&op.operands[0]),
                        bare_node_alias(&op.operands[1]),
                    ) {
                        let (l, r) = (TableAlias(la.to_string()), TableAlias(ra.to_string()));
                        let (l, r) = (&l, &r);
                        use crate::server::query_context::{
                            get_current_schema, get_node_label_for_alias,
                        };
                        // Resolve a bare node alias to its schema node_id identifier.
                        let node_id_of =
                            |alias: &str| -> Option<crate::graph_catalog::config::Identifier> {
                                let schema = get_current_schema()?;
                                let label = get_node_label_for_alias(alias)?;
                                schema
                                    .node_schema_opt(&label)
                                    .map(|ns| ns.node_id.id.clone())
                            };
                        if let (Some(lid), Some(rid)) = (node_id_of(&l.0), node_id_of(&r.0)) {
                            // `to_sql_equality` pairs columns element-wise and is
                            // dialect-aware; negate for `<>` (nodes always have a
                            // non-null id, so NOT(=) matches `<>` semantics).
                            let eq = lid.to_sql_equality(&l.0, &rid, &r.0);
                            return if op.operator == Operator::Equal {
                                eq
                            } else {
                                format!("NOT ({})", eq)
                            };
                        }
                        // Label/schema unresolvable — preserve prior behavior.
                        let op_str = if op.operator == Operator::Equal {
                            "="
                        } else {
                            "<>"
                        };
                        return format!("{}.id {} {}.id", l.0, op_str, r.0);
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
                        // Databricks rejects a bare NULL predicate
                        // (FILTER_NOT_BOOLEAN); spell Cypher NULL as a boolean
                        // NULL on Spark. ClickHouse accepts the bare form. (#1414)
                        if result == CypherTriBool::Null
                            && matches!(
                                crate::server::query_context::get_current_dialect(),
                                crate::sql_generator::SqlDialect::Databricks
                            )
                        {
                            return "CAST(NULL AS BOOLEAN)".to_string();
                        }
                        return result.sql_str().to_string();
                    }
                }

                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| operand_sql(op.operator, e, e.to_sql()))
                    .collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return super::common::regex_match_predicate(&rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Xor && rendered.len() == 2 {
                    return crate::sql_generator::function_mapper::current_function_mapper()
                        .logical_xor(&rendered[0], &rendered[1]);
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

                // IN/NOT IN with a list expression, as the dialect spells it.
                if let Some(sql) = in_list_expression_sql(&op.operator, &op.operands, &rendered) {
                    return sql;
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
                        // A null element forces the element-wise OR/AND expansion
                        // so three-valued IN/NOT IN semantics hold (#855): a
                        // `x = null` / `x <> null` term propagates the unknown,
                        // whereas CH `IN (…)` collapses it to a non-match.
                        if has_non_constant || in_list_has_null_literal(op) {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> = list_items
                                .iter()
                                .map(|item| operand_sql(Operator::Equal, item, item.to_sql()))
                                .collect();
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
                    return super::common::starts_with_predicate(&rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return super::common::ends_with_predicate(&rendered[0], &rendered[1]);
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
                if let Some(s) = render_integer_division(op, &rendered) {
                    return s;
                }
                if let Some(s) = render_exponentiation(op, &rendered) {
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
                                let lhs =
                                    if render_expr::needs_left_parens(op.operator, &op.operands[0])
                                    {
                                        format!("({})", &rendered[0])
                                    } else {
                                        rendered[0].clone()
                                    };
                                if render_expr::needs_right_parens(op.operator, &op.operands[1]) {
                                    format!("{} {} ({})", lhs, sql_op, &rendered[1])
                                } else {
                                    format!("{} {} {}", lhs, sql_op, &rendered[1])
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

                // Simple CASE (`CASE expr WHEN v THEN r ...`): dialect-aware.
                // CH spells it caseWithExpression(...); Spark uses standard
                // CASE-expr syntax. See common::simple_case_sql.
                if let Some(case_expr) = &case.expr {
                    let when_then: Vec<(String, String)> = case
                        .when_then
                        .iter()
                        .map(|(when_expr, then_expr)| {
                            (when_expr.to_sql(), render_result(then_expr))
                        })
                        .collect();

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

                    super::common::simple_case_sql(&case_expr.to_sql(), &when_then, &else_expr)
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
            RenderExpr::ReduceExpr(reduce) => {
                // Convert to ClickHouse arrayFold((acc, x) -> expr, list, init)
                // Cast numeric init to Int64 to prevent type mismatch issues
                let init_sql = reduce.initial_value.to_sql();
                let list_sql = reduce.list.to_sql();
                // #969: install the lambda binder types (acc = type of
                // initial_value, x = element type of list) around the BODY render
                // only, so the string-`+`→`concat` detector can see `s + x` as a
                // string concatenation. initial_value/list are outer-scope and
                // rendered above, unaffected. Save/restore for nested reduces.
                let binder_types = super::common::reduce_binder_type_map(
                    &reduce.accumulator,
                    &reduce.initial_value,
                    &reduce.variable,
                    &reduce.list,
                );
                let prev_binders =
                    crate::server::query_context::push_reduce_binder_types(binder_types);
                let expr_sql = reduce.expression.to_sql();
                crate::server::query_context::restore_reduce_binder_types(prev_binders);

                // Wrap a numeric init literal in the matching 64-bit cast to
                // prevent a Code 53 type mismatch when the lambda returns a wider
                // type than the bare literal's inferred type. An Integer `0`
                // infers as UInt8 and a Float `0.0` as Float64-vs-the-bare-`0`
                // rendering; the cast pins the accumulator type. #955 (Integer),
                // #971 (Float — `reduce(n = 0.0, x IN [1.5] | n + x)` seeded a
                // bare `0` (UInt8) against a Float64 body → Code 53).
                let fmap = crate::sql_generator::function_mapper::current_function_mapper();
                let init_cast = match *reduce.initial_value {
                    RenderExpr::Literal(Literal::Integer(_)) => {
                        format!("{}({})", fmap.cast_int64(), init_sql)
                    }
                    RenderExpr::Literal(Literal::Float(_)) => {
                        format!("{}({})", fmap.cast_float64(), init_sql)
                    }
                    _ => init_sql,
                };

                // #955: a fold over an EMPTY list literal does zero iterations,
                // so the result is exactly the initial accumulator. Emitting the
                // fold anyway (`arrayFold(…, [], init)`) fails on ClickHouse with
                // Code 53 — the bare `[]` literal is `Array(Nothing)`, so the
                // lambda's inferred return type can't unify with the accumulator
                // (and Spark's untyped `array()` is the same hazard). We can't
                // synthesize a correctly-typed empty array here (the element type
                // is unknowable from an empty literal), and it isn't needed:
                // short-circuit to the init expression.
                if matches!(reduce.list.as_ref(), RenderExpr::List(items) if items.is_empty()) {
                    return init_cast;
                }

                super::common::reduce_fold_sql(
                    &reduce.variable,
                    &reduce.accumulator,
                    &expr_sql,
                    &list_sql,
                    &init_cast,
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
                            vec![
                                crate::utils::sql_literal::string_value_to_sql_literal(k),
                                format!("{}({})", to_str, val_sql),
                            ]
                        })
                        .collect();
                    format!("map({})", args.join(", "))
                }
            }
            RenderExpr::Cast { expr, target } => format!("CAST({} AS {})", expr.to_sql(), target),
            RenderExpr::ArraySubscript { array, index } => {
                // Cypher uses 0-based indexing; array element access is 1-based on
                // both CH (`arr[i]`) and Spark (`element_at(arr, i)` — Spark's own
                // `arr[i]` subscript is 0-based, so it can't be used directly). The
                // mapper picks the right 1-based accessor per dialect.
                // For integer literals we add +1 at compile time; for expression
                // indices we emit (expr)+1 without a cast so the engine's type
                // checker catches bad types (floats, strings) rather than coercing.
                // Exception: string-literal indices are MAP-KEY accesses
                //   (e.g. top['score']) — `arr['key']` works on both dialects and
                //   must NOT be offset or routed through element_at.
                let array_sql = array.to_sql();
                match index.as_ref() {
                    RenderExpr::Literal(Literal::String(_)) => {
                        // Map-key access (e.g. top['score']) — `arr['key']` works on
                        // both dialects and must NOT be offset or use element_at.
                        format!("{}[{}]", array_sql, index.to_sql())
                    }
                    _ => {
                        // 1-based index (Cypher 0-based + 1). The mapper's
                        // `array_element_or_null` picks the right 1-based accessor
                        // per dialect: CH `arrayElementOrNull(arr, i)`, Spark
                        // `element_at(arr, i)`. Both return NULL for an
                        // out-of-bounds index, matching openCypher `list[i]`
                        // semantics (Neo4j returns null, not a type default).
                        //
                        // Negative indices (Cypher `-1` = last) map UNCHANGED: both
                        // CH and Spark already treat -1 as "last", so a blind +1
                        // wrongly shifts `-1`->`0`. Cypher `-1` reaches here as the
                        // expression `0 - 1` (the parser lowers unary minus that
                        // way), so a literal-only guard misses it — the runtime
                        // `if` below handles both literal and computed negatives.
                        let idx_1based = match index.as_ref() {
                            // Non-negative integer literal: clean compile-time +1.
                            RenderExpr::Literal(Literal::Integer(n)) if *n >= 0 => {
                                format!("{}", n + 1)
                            }
                            // General/runtime index: +1 only when non-negative. `if` is
                            // spelled the same and evaluates identically on CH and Spark.
                            _ => {
                                let i = index.to_sql();
                                format!("if(({i}) >= 0, ({i})+1, ({i}))")
                            }
                        };
                        crate::sql_generator::function_mapper::current_function_mapper()
                            .array_element_or_null(&array_sql, &idx_1based)
                    }
                }
            }
            RenderExpr::ArraySlicing { array, from, to } => {
                // Array slicing -> arraySlice(arr, offset, length) (CH) / slice (Spark),
                // both 1-based offset + element count. Cypher list ranges are 0-based
                // and HALF-OPEN: `list[from..to]` yields indices [from, to), i.e.
                // `to - from` elements. So offset = from + 1 and length = to - from.
                // Cypher also permits NEGATIVE bounds that count from the end
                // (`list[-2..]`) and clamps out-of-range bounds; each bound is
                // normalized to a non-negative index first (see
                // `normalize_slice_bound`). Statically non-negative integer
                // literals bypass normalization, so positive slices render
                // exactly as before.
                let array_sql = array.to_sql();
                let mapper = crate::sql_generator::function_mapper::current_function_mapper();

                match (from, to) {
                    (Some(from_expr), Some(to_expr)) => {
                        // [from..to) -> 1-based offset + half-open length (to - from).
                        // Floor the length at 0: when from >= to (after
                        // normalization) the slice is empty, but a negative
                        // length means "drop from the end" on ClickHouse arraySlice
                        // (silent wrong data) and errors on Databricks slice().
                        let nf = normalize_slice_bound(
                            &from_expr.to_sql(),
                            &array_sql,
                            is_nonneg_int_literal(from_expr),
                        );
                        let nt = normalize_slice_bound(
                            &to_expr.to_sql(),
                            &array_sql,
                            is_nonneg_int_literal(to_expr),
                        );
                        mapper.array_slice(
                            &array_sql,
                            &format!("{} + 1", nf),
                            Some(&format!("greatest({} - {}, 0)", nt, nf)),
                        )
                    }
                    (Some(from_expr), None) => {
                        // [from..] - slice to end. CH 2-arg form; Spark computes the length.
                        let nf = normalize_slice_bound(
                            &from_expr.to_sql(),
                            &array_sql,
                            is_nonneg_int_literal(from_expr),
                        );
                        mapper.array_slice(&array_sql, &format!("{} + 1", nf), None)
                    }
                    (None, Some(to_expr)) => {
                        // [..to) - from index 1, take `to` elements (indices [0, to)).
                        // With offset 1 the length equals the normalized end index.
                        let nt = normalize_slice_bound(
                            &to_expr.to_sql(),
                            &array_sql,
                            is_nonneg_int_literal(to_expr),
                        );
                        mapper.array_slice(&array_sql, "1", Some(&nt))
                    }
                    (None, None) => {
                        // [..] - no bounds, return entire array (identity operation)
                        array_sql
                    }
                }
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
            // OperatorApplicationExp has a special case that pattern-matches on
            // the operand's ORIGINAL TYPE, not its rendered text:
            //   - array membership: `x IN node.tags` -> `has(tags, x)`, detected
            //     by operands[1] being a plain `PropertyAccessExp`.
            // The check MUST run before any alias-stripping touches
            // operands[1]'s type (stripping to `Raw` would blind the type
            // check, silently degrading `has(tags,x)` to a scalar `x IN tags`
            // — a hard ClickHouse error — or `x IN (SELECT col FROM cte)` to a
            // bare unqualified `x IN col`, which could quietly bind to an
            // unrelated same-named column: #477 adversarial review, both
            // reproduced live). So this arm keeps its own dedicated logic
            // (restored from before the #477 AST-rewrite refactor) rather than
            // going through `strip_table_alias_everywhere`: operands are
            // rendered via recursive `to_sql_without_table_alias()` calls
            // (mutual recursion — correctly strips arbitrarily nested operator
            // applications too), while the special-case checks below inspect
            // `op.operands` (the ORIGINAL, untransformed AST), never the
            // rendered strings.
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
                        Operator::Xor => "XOR", // Special handling below
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

                // #535: Cypher literal equality (three-valued logic) was
                // missing from this path. `cypher_literal_eq` only ever
                // pattern-matches on literal/List/MapLiteral operand
                // STRUCTURE (never renders or touches a table alias — it
                // returns `None` immediately for any non-literal operand,
                // e.g. a `PropertyAccessExp`), so it's always safe to call
                // directly against the original, unstripped operands here.
                if matches!(op.operator, Operator::Equal | Operator::NotEqual)
                    && op.operands.len() == 2
                {
                    if let Some(tri) = cypher_literal_eq(&op.operands[0], &op.operands[1]) {
                        let result = if op.operator == Operator::NotEqual {
                            tri.negate()
                        } else {
                            tri
                        };
                        return result.sql_str().to_string();
                    }
                }

                // Recursively render operands without table alias
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| operand_sql(op.operator, e, e.to_sql_without_table_alias()))
                    .collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return super::common::regex_match_predicate(&rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Xor && rendered.len() == 2 {
                    return crate::sql_generator::function_mapper::current_function_mapper()
                        .logical_xor(&rendered[0], &rendered[1]);
                }

                // Special handling for IN/NOT IN with array columns. Type check
                // on the ORIGINAL operand — see the doc comment above.
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

                // IN/NOT IN with a list expression, as the dialect spells it.
                if let Some(sql) = in_list_expression_sql(&op.operator, &op.operands, &rendered) {
                    return sql;
                }

                // IN/NOT IN with List containing non-constant elements → expand to OR/AND
                if (op.operator == Operator::In || op.operator == Operator::NotIn)
                    && rendered.len() == 2
                {
                    if let RenderExpr::List(list_items) = &op.operands[1] {
                        let has_non_constant = list_items.iter().any(|item| {
                            !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
                        });
                        // A null element forces the element-wise OR/AND expansion
                        // so three-valued IN/NOT IN semantics hold (#855): a
                        // `x = null` / `x <> null` term propagates the unknown,
                        // whereas CH `IN (…)` collapses it to a non-match.
                        if has_non_constant || in_list_has_null_literal(op) {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> = list_items
                                .iter()
                                .map(|item| operand_sql(Operator::Equal, item, item.to_sql()))
                                .collect();
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
                    return super::common::starts_with_predicate(&rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return super::common::ends_with_predicate(&rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return super::common::contains_predicate(&rendered[0], &rendered[1]);
                }

                // Addition special cases (list concat, string concat, interval
                // arithmetic) — shared with the other operator paths.
                if let Some(s) = render_list_addition(op) {
                    return s;
                }
                // #535: string `+` -> `concat(...)` was missing from this path
                // (ClickHouse has no `+` for strings, so falling through to the
                // generic `op_str` join below would emit invalid `a + b` SQL for
                // a string-concat predicate inside a LEFT JOIN pre_filter
                // subquery). `render_string_addition` inspects operand TEXT via
                // `flatten_addition_operands`, which calls the ALIAS-PRESERVING
                // `to_sql()` on each leaf — so it must run against an
                // alias-STRIPPED copy of the operands here, not the original
                // `op`, or a leaf `PropertyAccessExp` operand would leak its
                // table alias back into the subquery (the exact class of bug
                // this whole function exists to prevent).
                if has_string_operand(&op.operands) {
                    let stripped_op = OperatorApplication {
                        operator: op.operator,
                        operands: op
                            .operands
                            .iter()
                            .map(strip_table_alias_everywhere)
                            .collect(),
                    };
                    if let Some(s) = render_string_addition(&stripped_op) {
                        return s;
                    }
                }
                if let Some(s) = render_interval_arithmetic(op, &rendered) {
                    return s;
                }
                if let Some(s) = render_integer_division(op, &rendered) {
                    return s;
                }
                if let Some(s) = render_exponentiation(op, &rendered) {
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
                            let lhs =
                                if render_expr::needs_left_parens(op.operator, &op.operands[0]) {
                                    format!("({})", &rendered[0])
                                } else {
                                    rendered[0].clone()
                                };
                            if render_expr::needs_right_parens(op.operator, &op.operands[1]) {
                                format!("{} {} ({})", lhs, sql_op, &rendered[1])
                            } else {
                                format!("{} {} {}", lhs, sql_op, &rendered[1])
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
            RenderExpr::Raw(raw_sql) => strip_raw_alias_prefixes(raw_sql),
            // #477: composite expression types that are not operator
            // applications carry no operand-TYPE-sensitive special-casing (a
            // ScalarFnCall's duration()/datetime() dispatch keys off whether
            // an ARG is a MapLiteral, which `strip_table_alias_everywhere`
            // preserves — it only ever rewrites `PropertyAccessExp` leaves
            // into `Raw`), so it's safe to do a full recursive AST rewrite
            // (stripping every nested `PropertyAccessExp`) and delegate to the
            // ordinary `to_sql()` for final rendering — e.g.
            // `toFloat(o.total_amount)` inside a LEFT JOIN pre_filter subquery
            // (`SELECT * FROM orders_fk WHERE ...` — no `o` alias in scope
            // there).
            // #535: `ReduceExpr` (`reduce(acc = init, x IN list | expr)`) can
            // embed a `PropertyAccessExp` on the outer alias in any of its
            // three sub-expressions — same reasoning as the #477 group below.
            RenderExpr::ScalarFnCall(_)
            | RenderExpr::AggregateFnCall(_)
            | RenderExpr::List(_)
            | RenderExpr::Case(_)
            | RenderExpr::ArraySubscript { .. }
            | RenderExpr::ArraySlicing { .. }
            | RenderExpr::Cast { .. }
            | RenderExpr::MapLiteral(_)
            | RenderExpr::ReduceExpr(_) => strip_table_alias_everywhere(self).to_sql(),
            // Other expression types: regular to_sql.
            _ => self.to_sql(),
        }
    }
}

/// Recursively rewrite `expr` so every `PropertyAccessExp` becomes a bare
/// column reference (no table alias) and every embedded `alias.column`
/// pattern inside a `Raw` string is stripped too. Used to render predicates
/// destined for a LEFT JOIN pre_filter subquery, where the source table has
/// no alias in scope yet (see #477).
///
/// `OperatorApplicationExp` is deliberately NOT rewritten structurally here —
/// it is instead treated as a boundary: delegate to the dedicated, type-safe
/// `to_sql_without_table_alias()` (which has its own IN-array / IN-CTE-
/// subquery special-casing that depends on inspecting the ORIGINAL operand
/// type) and wrap the resulting string as `Raw`. This keeps that logic in
/// exactly one place and means a nested operator application (e.g. inside a
/// CASE WHEN or as a scalar function argument) is handled correctly too,
/// instead of this function's own recursion silently destroying the operand
/// type information those special cases need (see #477 adversarial review).
fn strip_table_alias_everywhere(expr: &RenderExpr) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
            RenderExpr::Raw(column.to_sql_column_only())
        }
        RenderExpr::Raw(raw_sql) => RenderExpr::Raw(strip_raw_alias_prefixes(raw_sql)),
        RenderExpr::OperatorApplicationExp(_) => RenderExpr::Raw(expr.to_sql_without_table_alias()),
        RenderExpr::ScalarFnCall(f) => RenderExpr::ScalarFnCall(ScalarFnCall {
            name: f.name.clone(),
            args: f.args.iter().map(strip_table_alias_everywhere).collect(),
        }),
        RenderExpr::AggregateFnCall(f) => RenderExpr::AggregateFnCall(AggregateFnCall {
            name: f.name.clone(),
            args: f.args.iter().map(strip_table_alias_everywhere).collect(),
        }),
        RenderExpr::List(items) => {
            RenderExpr::List(items.iter().map(strip_table_alias_everywhere).collect())
        }
        RenderExpr::Case(case) => RenderExpr::Case(RenderCase {
            expr: case
                .expr
                .as_ref()
                .map(|e| Box::new(strip_table_alias_everywhere(e))),
            when_then: case
                .when_then
                .iter()
                .map(|(w, t)| {
                    (
                        strip_table_alias_everywhere(w),
                        strip_table_alias_everywhere(t),
                    )
                })
                .collect(),
            else_expr: case
                .else_expr
                .as_ref()
                .map(|e| Box::new(strip_table_alias_everywhere(e))),
        }),
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(strip_table_alias_everywhere(array)),
            index: Box::new(strip_table_alias_everywhere(index)),
        },
        RenderExpr::Cast { expr, target } => RenderExpr::Cast {
            expr: Box::new(strip_table_alias_everywhere(expr)),
            target: target.clone(),
        },
        RenderExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
            array: Box::new(strip_table_alias_everywhere(array)),
            from: from
                .as_ref()
                .map(|e| Box::new(strip_table_alias_everywhere(e))),
            to: to
                .as_ref()
                .map(|e| Box::new(strip_table_alias_everywhere(e))),
        },
        RenderExpr::MapLiteral(entries) => RenderExpr::MapLiteral(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), strip_table_alias_everywhere(v)))
                .collect(),
        ),
        // #535: `reduce(acc = init, x IN list | expr)` carries three nested
        // sub-expressions (`initial_value`, `list`, `expression`) that can
        // each independently reference the outer alias being stripped (e.g.
        // `reduce(total = 0, x IN o.items | total + x.price)` inside a
        // pre_filter predicate) — `accumulator`/`variable` are lambda-bound
        // names, not table aliases, and are left untouched.
        RenderExpr::ReduceExpr(reduce) => RenderExpr::ReduceExpr(ReduceExpr {
            accumulator: reduce.accumulator.clone(),
            initial_value: Box::new(strip_table_alias_everywhere(&reduce.initial_value)),
            variable: reduce.variable.clone(),
            list: Box::new(strip_table_alias_everywhere(&reduce.list)),
            expression: Box::new(strip_table_alias_everywhere(&reduce.expression)),
        }),
        // Other variants (Literal, Column, Parameter, ...) carry no
        // table-alias-qualified property access at this level.
        _ => expr.clone(),
    }
}

/// Strip `alias.` prefixes from `identifier.identifier` tokens in a raw SQL
/// string (e.g. `"alias.column = 'value'"` -> `"column = 'value'"`). Shared by
/// `strip_table_alias_everywhere` for both top-level and nested `Raw` nodes.
fn strip_raw_alias_prefixes(raw_sql: &str) -> String {
    let parts: Vec<&str> = raw_sql.split_whitespace().collect();
    let mut new_parts = Vec::new();
    for part in parts {
        if part.contains('.') && !part.starts_with('\'') && !part.starts_with('"') {
            let dot_parts: Vec<&str> = part.split('.').collect();
            if dot_parts.len() == 2 && !dot_parts[0].is_empty() && !dot_parts[1].is_empty() {
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
                Operator::Xor => "XOR", // Special handling below
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

        let rendered: Vec<String> = self
            .operands
            .iter()
            .map(|e| operand_sql(self.operator, e, e.to_sql()))
            .collect();

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
            return super::common::regex_match_predicate(&rendered[0], &rendered[1]);
        }
        if self.operator == Operator::Xor && rendered.len() == 2 {
            return crate::sql_generator::function_mapper::current_function_mapper()
                .logical_xor(&rendered[0], &rendered[1]);
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

        // IN/NOT IN with a list expression, as the dialect spells it.
        if let Some(sql) = in_list_expression_sql(&self.operator, &self.operands, &rendered) {
            return sql;
        }

        // IN/NOT IN with List containing non-constant elements → expand to OR/AND
        if (self.operator == Operator::In || self.operator == Operator::NotIn)
            && rendered.len() == 2
        {
            if let RenderExpr::List(list_items) = &self.operands[1] {
                let has_non_constant = list_items
                    .iter()
                    .any(|item| !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_)));
                // A null element forces the element-wise OR/AND expansion so
                // three-valued IN/NOT IN semantics hold (#855).
                if has_non_constant || in_list_has_null_literal(self) {
                    let lhs = &rendered[0];
                    let item_sqls: Vec<String> = list_items
                        .iter()
                        .map(|item| operand_sql(Operator::Equal, item, item.to_sql()))
                        .collect();
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
            return super::common::starts_with_predicate(&rendered[0], &rendered[1]);
        }
        if self.operator == Operator::EndsWith && rendered.len() == 2 {
            return super::common::ends_with_predicate(&rendered[0], &rendered[1]);
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
        if let Some(s) = render_integer_division(self, &rendered) {
            return s;
        }
        if let Some(s) = render_exponentiation(self, &rendered) {
            return s;
        }

        let sql_op = op_str(self.operator);

        match rendered.len() {
            0 => "".into(), // should not happen
            1 => {
                // Unary operators: IS NULL / IS NOT NULL are SUFFIX operators
                // ("x IS NULL"), everything else (NOT) is prefix. Mirrors the
                // RenderExpr::OperatorApplicationExp arm above — without this,
                // a join ON condition carrying an IS NULL conjunct (e.g. a
                // #597 anchor-gate fold) rendered invalid "IS NULL x" SQL.
                match self.operator {
                    Operator::IsNull | Operator::IsNotNull => {
                        format!("{} {}", &rendered[0], sql_op)
                    }
                    _ => format!("{} {}", sql_op, &rendered[0]),
                }
            }
            2 => {
                let lhs = if render_expr::needs_left_parens(self.operator, &self.operands[0]) {
                    format!("({})", &rendered[0])
                } else {
                    rendered[0].clone()
                };
                if render_expr::needs_right_parens(self.operator, &self.operands[1]) {
                    format!("{} {} ({})", lhs, sql_op, &rendered[1])
                } else {
                    format!("{} {} {}", lhs, sql_op, &rendered[1])
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

    /// FINAL is a ClickHouse-only keyword (ReplacingMergeTree dedup): it must be
    /// emitted under ClickHouse and omitted under Databricks/Spark (where it is
    /// invalid SQL) even when the schema requests it via `use_final = true`.
    /// This exercises the LIVE render path — `FromTableItem::to_sql` (the
    /// `render_plan::ToSql` impl, `to_sql_query.rs`) — which gates on
    /// `dialect.supports_final_keyword()`. Ported from the retired dead-code
    /// `to_sql.rs` `ViewScan::to_sql` test (SQL-IR Phase 2 step 3, Path D removal).
    #[tokio::test]
    async fn final_emitted_only_on_clickhouse() {
        use crate::render_plan::{FromTableItem, ViewTableRef};
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        fn from_with_final() -> FromTableItem {
            FromTableItem(Some(ViewTableRef {
                name: "users".to_string(),
                alias: Some("u".to_string()),
                use_final: true,
            }))
        }

        // Default scope -> ClickHouse -> FINAL present.
        let ch_sql = from_with_final().to_sql();
        assert!(
            ch_sql.contains(" FINAL"),
            "ClickHouse should emit FINAL; got: {ch_sql}"
        );

        // Databricks dialect -> no FINAL (invalid Spark SQL).
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let dbx_sql = with_query_context(ctx, async { from_with_final().to_sql() }).await;
        assert!(
            !dbx_sql.contains("FINAL"),
            "Databricks must not emit FINAL; got: {dbx_sql}"
        );
    }

    /// #567: `flatten_all_ctes`/`collect_nested_ctes` — the CTX-LESS render
    /// path's final CTE-flattening step (reached e.g. via EXISTS subqueries,
    /// `render_expr.rs:51`, and other `to_render_plan(` callers that never
    /// touch `to_render_plan_with_ctx`) — has the same CTE-name-collision gap
    /// #557 already fixed on the ctx-AWARE path (`extract_ctes_with_context`'s
    /// Union arm, `cte_extraction.rs`): naively concatenating every Union
    /// branch's CTEs and then deduping by keep-first-name silently DROPS a
    /// real branch's CTE body whenever two branches independently compute the
    /// SAME formulaic CTE name (e.g. a multi-type VLP's per-candidate-end-
    /// label branches) but generate DIFFERENT CTE SQL. This directly
    /// exercises `flatten_all_ctes` (via its private helper
    /// `collect_nested_ctes`) with a synthetic two-branch Union shaped
    /// exactly like that collision, without needing a full end-to-end query
    /// repro (attempted but not found reachable through any live query path
    /// — see PR discussion / commit message for what was tried).
    #[test]
    fn test_567_flatten_all_ctes_renames_colliding_union_branch_cte_and_fixes_up_from() {
        use crate::render_plan::render_expr::ColumnAlias;
        use crate::render_plan::{
            ArrayJoinItem, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions,
            JoinItems, LimitItem, OrderByItems, SelectItem, SelectItems, SkipItem, Union,
            UnionItems, UnionType, ViewTableRef,
        };

        fn cte_ref(name: &str) -> ViewTableRef {
            ViewTableRef {
                name: name.to_string(),
                alias: Some("t".to_string()),
                use_final: false,
            }
        }

        fn branch(cte_name: &str, cte_sql: &str) -> RenderPlan {
            RenderPlan {
                ctes: CteItems(vec![Cte::new(
                    cte_name.to_string(),
                    CteContent::RawSql(cte_sql.to_string()),
                    false,
                )]),
                select: SelectItems {
                    items: vec![SelectItem {
                        expression: RenderExpr::Literal(Literal::Integer(1)),
                        col_alias: Some(ColumnAlias("__dummy".to_string())),
                    }],
                    distinct: false,
                },
                from: FromTableItem(Some(cte_ref(cte_name))),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
            }
        }

        // Branch 0 (becomes the base plan) and branch 1 (a nested union
        // sibling) each independently compute the SAME CTE name
        // ("vlp_multi_type_a_b" — the real-world formulaic name for this
        // exact collision, see #557) but with DIFFERENT CTE bodies, exactly
        // like two candidate-end-label branches of an unlabeled multi-type
        // VLP end node.
        let mut base_plan = branch(
            "vlp_multi_type_a_b",
            "SELECT 'User' AS end_type FROM branch_0",
        );
        let branch_1 = branch(
            "vlp_multi_type_a_b",
            "SELECT 'Post' AS end_type FROM branch_1",
        );
        base_plan.union = UnionItems(Some(Union {
            input: vec![branch_1],
            union_type: UnionType::All,
        }));

        flatten_all_ctes(&mut base_plan);

        // Both CTE bodies must survive — the DIFFERENT-content collision
        // must be resolved by renaming, never by silently dropping one.
        assert_eq!(
            base_plan.ctes.0.len(),
            2,
            "#567: both colliding-but-different CTE bodies must survive \
             flattening, not be silently deduped away: {:?}",
            base_plan.ctes.0
        );
        assert_eq!(base_plan.ctes.0[0].cte_name, "vlp_multi_type_a_b");
        assert_eq!(base_plan.ctes.0[1].cte_name, "vlp_multi_type_a_b_2");

        // The renamed branch's own FROM reference must be updated to match
        // — otherwise the branch's SELECT still reads `FROM vlp_multi_type_a_b`
        // while the WITH clause only defines `vlp_multi_type_a_b_2` for that
        // branch's content (a dangling reference, ClickHouse "Unknown table
        // expression identifier").
        let renamed_branch_from = base_plan
            .union
            .0
            .as_ref()
            .expect("union branch must survive flattening")
            .input
            .first()
            .expect("union branch must survive flattening")
            .from
            .0
            .as_ref()
            .expect("branch FROM must survive")
            .name
            .clone();
        assert_eq!(
            renamed_branch_from, "vlp_multi_type_a_b_2",
            "#567: the renamed branch's own FROM reference must be updated \
             to match its renamed CTE — otherwise it dangles"
        );

        // The base (non-renamed) branch's FROM reference must stay untouched.
        assert_eq!(
            base_plan.from.0.as_ref().unwrap().name,
            "vlp_multi_type_a_b"
        );
    }

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

    // ---- #477 adversarial review: `to_sql_without_table_alias` must preserve
    // the IN-array and IN-CTE-subquery special cases, which pattern-match on
    // the ORIGINAL (unstripped) operand TYPE. A prior version of this function
    // converted every `PropertyAccessExp` to `Raw` before any type-based
    // dispatch could run, silently degrading both rewrites — one to a hard
    // ClickHouse error, the other to a bare unqualified column that could
    // quietly bind to an unrelated same-named column. ----

    use crate::graph_catalog::expression_parser::PropertyValue as PropVal;

    /// `x IN node.arrayProp` must still become `has(arrayProp, x)`, not the
    /// bare-column default `x IN arrayProp` (a hard ClickHouse error: "Function
    /// 'in' is supported only if second argument is constant or table
    /// expression"). Live-verified against ClickHouse in the #477 follow-up
    /// (see `array_property_probe.yaml` / `probe_arr.tags`).
    #[test]
    fn test_to_sql_without_table_alias_preserves_array_membership_in() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::In,
            operands: vec![
                RenderExpr::Literal(Literal::String("a".to_string())),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("o".to_string()),
                    column: PropVal::Column("tags".to_string()),
                }),
            ],
        });
        assert_eq!(expr.to_sql_without_table_alias(), "has(tags, 'a')");
    }

    /// The NOT IN / array-membership counterpart.
    #[test]
    fn test_to_sql_without_table_alias_preserves_array_membership_not_in() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::NotIn,
            operands: vec![
                RenderExpr::Literal(Literal::String("a".to_string())),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("o".to_string()),
                    column: PropVal::Column("tags".to_string()),
                }),
            ],
        });
        assert_eq!(expr.to_sql_without_table_alias(), "NOT has(tags, 'a')");
    }

    /// Array membership nested inside a function argument (exercises the
    /// `strip_table_alias_everywhere` <-> `to_sql_without_table_alias` boundary:
    /// a `ScalarFnCall` arg that is itself an `OperatorApplicationExp` must
    /// delegate back to the dedicated, type-safe logic rather than being
    /// destructively rewritten as part of the function-arg AST walk).
    #[test]
    fn test_to_sql_without_table_alias_preserves_array_membership_nested_in_function() {
        let inner = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::In,
            operands: vec![
                RenderExpr::Literal(Literal::String("a".to_string())),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("o".to_string()),
                    column: PropVal::Column("tags".to_string()),
                }),
            ],
        });
        let expr = RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "not".to_string(),
            args: vec![inner],
        });
        assert_eq!(expr.to_sql_without_table_alias(), "not(has(tags, 'a'))");
    }

    // ---- #855: three-valued IN / NOT IN when the list holds an explicit null ----

    /// `x IN [1, 2, null]` must NOT render the plain `IN` form (CH collapses a
    /// null element to a non-match → `false`, but openCypher wants `null` when
    /// no non-null element matches). It expands element-wise so the `x = NULL`
    /// term propagates the unknown.
    #[test]
    fn test_in_list_with_null_literal_expands_to_or_for_three_valued_logic() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::In,
            operands: vec![
                RenderExpr::Literal(Literal::Integer(3)),
                RenderExpr::List(vec![
                    RenderExpr::Literal(Literal::Integer(1)),
                    RenderExpr::Literal(Literal::Integer(2)),
                    RenderExpr::Literal(Literal::Null),
                ]),
            ],
        });
        assert_eq!(expr.to_sql(), "(3 = 1 OR 3 = 2 OR 3 = NULL)");
    }

    /// `x NOT IN [1, null]` expands to an AND chain including `x <> NULL`, which
    /// yields null (not `true`) when the probe matches no non-null element.
    #[test]
    fn test_not_in_list_with_null_literal_expands_to_and_for_three_valued_logic() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::NotIn,
            operands: vec![
                RenderExpr::Literal(Literal::Integer(3)),
                RenderExpr::List(vec![
                    RenderExpr::Literal(Literal::Integer(1)),
                    RenderExpr::Literal(Literal::Null),
                ]),
            ],
        });
        assert_eq!(expr.to_sql(), "(3 <> 1 AND 3 <> NULL)");
    }

    /// A null-free constant list keeps the byte-stable `IN [array]` form — the
    /// fix must not churn the common case (this is what keeps existing goldens
    /// unchanged).
    #[test]
    fn test_in_list_without_null_keeps_plain_in_form() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::In,
            operands: vec![
                RenderExpr::Literal(Literal::Integer(3)),
                RenderExpr::List(vec![
                    RenderExpr::Literal(Literal::Integer(1)),
                    RenderExpr::Literal(Literal::Integer(2)),
                ]),
            ],
        });
        assert_eq!(expr.to_sql(), "3 IN [1, 2]");
    }

    // ---- WHERE alias inlining for Spark/Databricks (LDBC Q10) ----

    use crate::render_plan::render_expr::{PropertyAccess, ScalarFnCall, TableAlias};

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

    /// #962: `clickhouse_utf8_string_fn` upgrades `size`/`reverse` to the
    /// codepoint-based UTF8 variants ONLY for a proven-string arg on ClickHouse.
    #[test]
    fn clickhouse_utf8_string_fn_dispatches_by_arg_type() {
        use crate::sql_generator::SqlDialect;
        let str_lit = RenderExpr::Literal(Literal::String("héllo".to_string()));
        let arr = RenderExpr::List(vec![RenderExpr::Literal(Literal::Integer(1))]);

        // Proven string on ClickHouse → UTF8 variant.
        assert_eq!(
            clickhouse_utf8_string_fn("size", Some(&str_lit), SqlDialect::ClickHouse),
            Some("lengthUTF8")
        );
        assert_eq!(
            clickhouse_utf8_string_fn("reverse", Some(&str_lit), SqlDialect::ClickHouse),
            Some("reverseUTF8")
        );
        // Array arg → None (keep the plain, array-safe name).
        assert_eq!(
            clickhouse_utf8_string_fn("size", Some(&arr), SqlDialect::ClickHouse),
            None
        );
        assert_eq!(
            clickhouse_utf8_string_fn("reverse", Some(&arr), SqlDialect::ClickHouse),
            None
        );
        // Databricks → None (Spark is already codepoint-based).
        assert_eq!(
            clickhouse_utf8_string_fn("size", Some(&str_lit), SqlDialect::Databricks),
            None
        );
        // A non-size/reverse function name → None even for a string.
        assert_eq!(
            clickhouse_utf8_string_fn("upper", Some(&str_lit), SqlDialect::ClickHouse),
            None
        );
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

    #[test]
    fn cte_rewrite_scanner_honours_backslash_escaped_quote_and_multibyte_1217() {
        // Spark spells a literal quote `\'`; the literal does not end there, so the
        // `vlp_a_b` after it is still INSIDE the string and must stay intact.
        let sql = "FROM vlp_a_b WHERE x = 'it\\'s vlp_a_b é' AND y IN (SELECT 1 FROM vlp_a_b)";
        assert_eq!(
            rewrite_cte_name_structural(sql, "vlp_a_b", "z"),
            "FROM z WHERE x = 'it\\'s vlp_a_b é' AND y IN (SELECT 1 FROM z)"
        );
    }

    #[test]
    fn rewrite_cte_name_structural_targets_refs_not_string_literals_618() {
        // Header + recursive self-ref rewritten; a string literal that happens to
        // equal the CTE name is LEFT INTACT (else a silent wrong result).
        let sql =
            "vlp_a_b AS (\n  WHERE start_node.full_name = 'vlp_a_b'\n  UNION ALL\n  FROM vlp_a_b vp\n)";
        assert_eq!(
            rewrite_cte_name_structural(sql, "vlp_a_b", "vlp_a_b_2"),
            "vlp_a_b_2 AS (\n  WHERE start_node.full_name = 'vlp_a_b'\n  UNION ALL\n  FROM vlp_a_b_2 vp\n)"
        );
        // Must NOT corrupt an occurrence that is a prefix of a longer identifier.
        assert_eq!(
            rewrite_cte_name_structural("FROM vlp_a_b_2 vp", "vlp_a_b", "vlp_a_b_9"),
            "FROM vlp_a_b_2 vp"
        );
        // JOIN reference is rewritten too.
        assert_eq!(
            rewrite_cte_name_structural("JOIN vlp_a_b ON x", "vlp_a_b", "vlp_a_b_2"),
            "JOIN vlp_a_b_2 ON x"
        );
        // A bare column/alias occurrence NOT after FROM/JOIN and not a header is
        // left alone (conservative — never over-rewrite).
        assert_eq!(
            rewrite_cte_name_structural("SELECT vlp_a_b.x FROM t", "vlp_a_b", "z"),
            "SELECT vlp_a_b.x FROM t"
        );
        // Escaped quote inside a string literal doesn't desync the scanner.
        assert_eq!(
            rewrite_cte_name_structural(
                "WHERE n = 'a''vlp_a_b' AND FROM vlp_a_b vp",
                "vlp_a_b",
                "vlp_a_b_2"
            ),
            "WHERE n = 'a''vlp_a_b' AND FROM vlp_a_b_2 vp"
        );
    }

    /// #642: the derived sub-CTE family (`<base>_inner`/`_to_target`/…) bundled in
    /// a VLP RawSql blob must be discovered (only at `AS (` definition headers) and
    /// renamed together with the base, so a second union arm with the same aliases
    /// doesn't re-define e.g. `vlp_a_b_inner` (Code 179).
    #[test]
    fn discover_and_rewrite_derived_cte_family_642() {
        // Discovery: only names defined as `<base>_<suffix> AS (` headers.
        let sql = "vlp_a_b_inner AS (\n  FROM vlp_a_b_inner vp\n),\nvlp_a_b_to_target AS (\n  SELECT * FROM vlp_a_b_inner\n),\nvlp_a_b AS (\n  SELECT * FROM vlp_a_b_to_target\n)";
        let mut found = discover_derived_cte_names(sql, "vlp_a_b");
        found.sort();
        assert_eq!(found, vec!["vlp_a_b_inner", "vlp_a_b_to_target"]);

        // A `<base>_<suffix>` that is only REFERENCED (never a header) is NOT
        // discovered — e.g. a bare `FROM foo_inner` with no `foo_inner AS (`.
        assert_eq!(
            discover_derived_cte_names("SELECT * FROM vlp_a_b_inner", "vlp_a_b"),
            Vec::<String>::new()
        );

        // A string literal that looks like a header must NOT be discovered.
        assert_eq!(
            discover_derived_cte_names("WHERE x = 'vlp_a_b_inner AS ('", "vlp_a_b"),
            Vec::<String>::new()
        );

        // Full family rewrite (base + derived) via rewrite_cte_self_name_in_raw_sql.
        let mut content = CteContent::RawSql(sql.to_string());
        rewrite_cte_self_name_in_raw_sql(&mut content, "vlp_a_b", "vlp_a_b_2");
        let CteContent::RawSql(rewritten) = &content else {
            panic!("expected RawSql");
        };
        // Every header and cross-reference of the family is renamed.
        assert!(rewritten.contains("vlp_a_b_2_inner AS ("));
        assert!(rewritten.contains("FROM vlp_a_b_2_inner vp"));
        assert!(rewritten.contains("vlp_a_b_2_to_target AS ("));
        assert!(rewritten.contains("SELECT * FROM vlp_a_b_2_inner")); // to_target's ref
        assert!(rewritten.contains("vlp_a_b_2 AS ("));
        assert!(rewritten.contains("SELECT * FROM vlp_a_b_2_to_target")); // outer's ref
                                                                          // The un-suffixed base must NOT leak (no bare `vlp_a_b_inner`/`vlp_a_b AS`).
        assert!(!rewritten.contains("vlp_a_b_inner"));
        assert!(!rewritten.contains("vlp_a_b_to_target"));
        assert!(!rewritten.contains("\nvlp_a_b AS ("));
    }
}
