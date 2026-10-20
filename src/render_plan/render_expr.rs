use serde::{Deserialize, Serialize};

use crate::graph_catalog::expression_parser::PropertyValue;

// Re-export schema name accessors from the unified query context
// See server/query_context.rs for the task_local! implementation with .scope() support
pub use crate::server::query_context::{
    clear_current_schema_name, get_current_schema_name, set_current_schema_name,
};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RenderExpr {
    Literal(Literal),

    /// Raw SQL expression as a string
    Raw(String),

    Star,

    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    Column(Column),

    Parameter(String),

    List(Vec<RenderExpr>),

    AggregateFnCall(AggregateFnCall),

    ScalarFnCall(ScalarFnCall),

    PropertyAccessExp(PropertyAccess),

    OperatorApplicationExp(OperatorApplication),

    Case(RenderCase),

    /// Reduce expression: fold list into single value
    ReduceExpr(ReduceExpr),

    /// Map literal: {key1: value1, key2: value2}
    /// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
    MapLiteral(Vec<(String, RenderExpr)>),

    /// Array subscript: `array[index]`
    /// Access element at specified index (1-based in Cypher, 0-based in ClickHouse)
    ArraySubscript {
        array: Box<RenderExpr>,
        index: Box<RenderExpr>,
    },

    /// Array slicing: `array[from..to]`
    /// Extract subarray from index 'from' to 'to' (0-based, inclusive in Cypher)
    ArraySlicing {
        array: Box<RenderExpr>,
        from: Option<Box<RenderExpr>>,
        to: Option<Box<RenderExpr>>,
    },

    /// `CAST(expr AS target)`, the target already spelled for the SQL
    /// flavor (`cast_target`): where values of differently typed columns
    /// meet (docs/design/VALUE_TYPES.md T4a).
    Cast {
        expr: Box<RenderExpr>,
        target: String,
    },
}

/// Decision returned by a [`map_render_expr`] rewrite closure for a node.
pub enum RenderRewrite {
    /// Replace this node with `expr` and stop — do NOT recurse into it.
    Replace(RenderExpr),
    /// Rebuild this node by recursing structurally into its children.
    Recurse,
}

/// Structurally rewrite a [`RenderExpr`] tree top-down.
///
/// For each node `f` decides: [`RenderRewrite::Replace`] (stop) or
/// [`RenderRewrite::Recurse`] into children (via [`map_render_expr_children`],
/// whose match is EXHAUSTIVE with no `_` catch-all — a new variant is a compile
/// error, not a silent identity-clone). Render-side dual of the query-planner
/// `map_expression`; exists to retire the hand-rolled
/// `match … _ => expr.clone()` `RenderExpr` rewriters whose catch-alls silently
/// dropped rewrites inside `List`/`Case`/`ArraySubscript`/`ReduceExpr`/… wrappers.
///
/// DESCENT POLICY: recurses into value-carrying wrappers; leaves are cloned.
pub fn map_render_expr<F>(expr: &RenderExpr, f: &mut F) -> RenderExpr
where
    F: FnMut(&RenderExpr) -> RenderRewrite,
{
    match f(expr) {
        RenderRewrite::Replace(e) => e,
        RenderRewrite::Recurse => map_render_expr_children(expr, f),
    }
}

/// Rebuild `expr` by recursing `f` into each value-wrapper child; clone leaves
/// (see [`map_render_expr`] descent policy). EXHAUSTIVE — no `_` catch-all, so a new [`RenderExpr`]
/// variant fails to compile here until its child structure is spelled out.
pub fn map_render_expr_children<F>(expr: &RenderExpr, f: &mut F) -> RenderExpr
where
    F: FnMut(&RenderExpr) -> RenderRewrite,
{
    match expr {
        RenderExpr::ScalarFnCall(sf) => RenderExpr::ScalarFnCall(ScalarFnCall {
            name: sf.name.clone(),
            args: sf.args.iter().map(|a| map_render_expr(a, f)).collect(),
        }),
        RenderExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(AggregateFnCall {
            name: agg.name.clone(),
            args: agg.args.iter().map(|a| map_render_expr(a, f)).collect(),
        }),
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op.operands.iter().map(|o| map_render_expr(o, f)).collect(),
            })
        }
        RenderExpr::Case(case) => RenderExpr::Case(RenderCase {
            expr: case.expr.as_ref().map(|e| Box::new(map_render_expr(e, f))),
            when_then: case
                .when_then
                .iter()
                .map(|(w, t)| (map_render_expr(w, f), map_render_expr(t, f)))
                .collect(),
            else_expr: case
                .else_expr
                .as_ref()
                .map(|e| Box::new(map_render_expr(e, f))),
        }),
        RenderExpr::ReduceExpr(reduce) => RenderExpr::ReduceExpr(ReduceExpr {
            accumulator: reduce.accumulator.clone(),
            initial_value: Box::new(map_render_expr(&reduce.initial_value, f)),
            variable: reduce.variable.clone(),
            list: Box::new(map_render_expr(&reduce.list, f)),
            expression: Box::new(map_render_expr(&reduce.expression, f)),
        }),
        RenderExpr::List(items) => {
            RenderExpr::List(items.iter().map(|i| map_render_expr(i, f)).collect())
        }
        RenderExpr::MapLiteral(entries) => RenderExpr::MapLiteral(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), map_render_expr(v, f)))
                .collect(),
        ),
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(map_render_expr(array, f)),
            index: Box::new(map_render_expr(index, f)),
        },
        RenderExpr::Cast { expr, target } => RenderExpr::Cast {
            expr: Box::new(map_render_expr(expr, f)),
            target: target.clone(),
        },
        RenderExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
            array: Box::new(map_render_expr(array, f)),
            from: from.as_ref().map(|x| Box::new(map_render_expr(x, f))),
            to: to.as_ref().map(|x| Box::new(map_render_expr(x, f))),
        },

        // Leaves. NO `_` catch-all — every variant is named so a new one
        // forces a compile error here.
        RenderExpr::PropertyAccessExp(_)
        | RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => expr.clone(),
    }
}

/// Decision returned by a [`visit_render_expr_mut`] closure for a node.
pub enum MutVisit {
    /// This node was handled in place — do NOT recurse into its children.
    Stop,
    /// Recurse structurally into this node's value-carrying children (via
    /// [`descend_render_expr_mut`]).
    Recurse,
}

/// In-place (`&mut`) structural sibling of [`map_render_expr`]. For each node
/// `f` decides [`MutVisit::Stop`] (handled — no descent) or [`MutVisit::Recurse`]
/// (descend into value children via [`descend_render_expr_mut`], whose match is
/// EXHAUSTIVE — a new [`RenderExpr`] variant is a compile error, not a silently
/// skipped rewrite).
///
/// This exists to retire the hand-rolled `match … _ => {}` `&mut RenderExpr`
/// walkers (e.g. the `apply_property_mapping_to_expr` denorm remappers) whose
/// catch-alls silently dropped rewrites inside
/// `Case`/`List`/`ArraySubscript`/`ArraySlicing`/`MapLiteral`/`ReduceExpr`
/// wrappers (#906/#929). A caller keeps its leaf-mutation logic in `f` and
/// returns `Recurse` for wrappers; descent is centralized and exhaustive.
///
/// DESCENT POLICY mirrors [`map_render_expr`]: descends into value-carrying
/// wrappers only.
pub fn visit_render_expr_mut<F>(expr: &mut RenderExpr, f: &mut F)
where
    F: FnMut(&mut RenderExpr) -> MutVisit,
{
    match f(expr) {
        MutVisit::Stop => {}
        MutVisit::Recurse => descend_render_expr_mut(expr, f),
    }
}

/// Recurse `f` into each value-wrapper child of `expr` in place. Leaves are not
/// descended (see [`visit_render_expr_mut`] descent policy). EXHAUSTIVE — no `_` catch-all, so
/// a new [`RenderExpr`] variant fails to compile here until its child structure
/// is spelled out.
pub fn descend_render_expr_mut<F>(expr: &mut RenderExpr, f: &mut F)
where
    F: FnMut(&mut RenderExpr) -> MutVisit,
{
    match expr {
        RenderExpr::ScalarFnCall(sf) => {
            for a in &mut sf.args {
                visit_render_expr_mut(a, f);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for a in &mut agg.args {
                visit_render_expr_mut(a, f);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for o in &mut op.operands {
                visit_render_expr_mut(o, f);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(e) = case.expr.as_mut() {
                visit_render_expr_mut(e, f);
            }
            for (w, t) in &mut case.when_then {
                visit_render_expr_mut(w, f);
                visit_render_expr_mut(t, f);
            }
            if let Some(e) = case.else_expr.as_mut() {
                visit_render_expr_mut(e, f);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            visit_render_expr_mut(&mut reduce.initial_value, f);
            visit_render_expr_mut(&mut reduce.list, f);
            visit_render_expr_mut(&mut reduce.expression, f);
        }
        RenderExpr::List(items) => {
            for i in items {
                visit_render_expr_mut(i, f);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            for (_k, v) in entries {
                visit_render_expr_mut(v, f);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            visit_render_expr_mut(array, f);
            visit_render_expr_mut(index, f);
        }
        RenderExpr::Cast { expr, .. } => visit_render_expr_mut(expr, f),
        RenderExpr::ArraySlicing { array, from, to } => {
            visit_render_expr_mut(array, f);
            if let Some(x) = from.as_mut() {
                visit_render_expr_mut(x, f);
            }
            if let Some(x) = to.as_mut() {
                visit_render_expr_mut(x, f);
            }
        }

        // Leaves. NO `_` catch-all — every variant is named so a new one
        // forces a compile error here.
        RenderExpr::PropertyAccessExp(_)
        | RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => {}
    }
}

/// Reduce expression for folding a list into a single value
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReduceExpr {
    /// Name of the accumulator variable
    pub accumulator: String,
    /// Initial value for the accumulator
    pub initial_value: Box<RenderExpr>,
    /// Iteration variable name
    pub variable: String,
    /// List to iterate over
    pub list: Box<RenderExpr>,
    /// Expression evaluated for each element
    pub expression: Box<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RenderCase {
    /// Expression for simple CASE (CASE x WHEN ...), None for searched CASE
    pub expr: Option<Box<RenderExpr>>,
    /// WHEN conditions and THEN expressions
    pub when_then: Vec<(RenderExpr, RenderExpr)>,
    /// Optional ELSE expression
    pub else_expr: Option<Box<RenderExpr>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TableAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Column(pub PropertyValue);

impl Column {
    pub fn raw(&self) -> &str {
        self.0.raw()
    }
}

// `Operator` is unified with the planner's operator enum: the two were
// byte-identical 24-variant copies, and maintaining both forced a spurious
// `TryFrom<LogicalOperator> for Operator` identity conversion plus duplicated
// operator-render logic. `RenderExpr` (the leaf layer) re-exports the
// planner's `Operator` so there is a single source of truth. Anything that
// referred to `render_expr::Operator` still resolves through this re-export.
pub use crate::query_planner::logical_expr::Operator;

impl Operator {
    /// Returns a numeric precedence level for arithmetic operators.
    /// Higher = binds tighter. Returns None for non-arithmetic operators.
    fn arithmetic_precedence(self) -> Option<u8> {
        match self {
            Operator::Addition | Operator::Subtraction => Some(1),
            Operator::Multiplication | Operator::Division | Operator::ModuloDivision => Some(2),
            Operator::Exponentiation => Some(3),
            _ => None,
        }
    }
}

/// Check whether a right operand needs parenthesization to preserve semantics.
///
/// Parentheses are needed when the right operand is an operator expression whose
/// precedence is less than or equal to the outer operator's precedence for
/// non-commutative operators (Subtraction, Division, ModuloDivision), because
/// left-to-right evaluation would change the result.
///
/// Examples:
/// - `a - (b + c)` must NOT flatten to `a - b + c`
/// - `a / (b * c)` must NOT flatten to `a / b * c`
/// - `a * (b + c)` must NOT flatten to `a * b + c`
pub fn needs_right_parens(outer_op: Operator, right_operand: &RenderExpr) -> bool {
    let outer_prec = match outer_op.arithmetic_precedence() {
        Some(p) => p,
        None => return false,
    };

    if let RenderExpr::OperatorApplicationExp(inner) = right_operand {
        let inner_prec = match inner.operator.arithmetic_precedence() {
            Some(p) => p,
            None => return false,
        };
        // For non-commutative ops (-, /, %), parens needed when inner precedence <= outer
        // For commutative ops (+, *), parens needed when inner precedence < outer (strict)
        match outer_op {
            Operator::Subtraction | Operator::Division | Operator::ModuloDivision => {
                inner_prec <= outer_prec
            }
            _ => inner_prec < outer_prec,
        }
    } else {
        false
    }
}

/// Check whether a LEFT operand needs parenthesization to preserve semantics.
///
/// Parens are needed when the left operand is an operator expression whose
/// precedence is strictly LESS than the outer operator's — otherwise flattening
/// changes the grouping. Because arithmetic operators are left-associative,
/// EQUAL precedence on the left needs no parens (`(a - b) - c` == `a - b - c`,
/// `(a / b) * c` == `a / b * c`), unlike the right side.
///
/// Examples:
/// - `(a + b) * c` must NOT flatten to `a + b * c` (inner `+` prec 1 < `*` prec 2)
/// - `(a - b) / c` must NOT flatten to `a - b / c`
/// - `(a - b) - c` MAY flatten to `a - b - c` (equal prec, left-associative)
/// - `(a * b) + c` MAY flatten to `a * b + c` (inner prec higher)
pub fn needs_left_parens(outer_op: Operator, left_operand: &RenderExpr) -> bool {
    let outer_prec = match outer_op.arithmetic_precedence() {
        Some(p) => p,
        None => return false,
    };

    if let RenderExpr::OperatorApplicationExp(inner) = left_operand {
        match inner.operator.arithmetic_precedence() {
            Some(inner_prec) => inner_prec < outer_prec,
            None => false,
        }
    } else {
        false
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OperatorApplication {
    pub operator: Operator,
    pub operands: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: PropertyValue, // Use PropertyValue directly
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_binop(op: Operator, left: RenderExpr, right: RenderExpr) -> RenderExpr {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands: vec![left, right],
        })
    }

    fn lit_int(n: i64) -> RenderExpr {
        RenderExpr::Literal(Literal::Integer(n))
    }

    #[test]
    fn test_needs_right_parens_sub_add() {
        // a - (b + c) needs parens
        let right = make_binop(Operator::Addition, lit_int(2), lit_int(3));
        assert!(needs_right_parens(Operator::Subtraction, &right));
    }

    #[test]
    fn test_needs_right_parens_sub_sub() {
        // a - (b - c) needs parens
        let right = make_binop(Operator::Subtraction, lit_int(2), lit_int(3));
        assert!(needs_right_parens(Operator::Subtraction, &right));
    }

    #[test]
    fn test_needs_right_parens_div_mul() {
        // a / (b * c) needs parens
        let right = make_binop(Operator::Multiplication, lit_int(2), lit_int(3));
        assert!(needs_right_parens(Operator::Division, &right));
    }

    #[test]
    fn test_needs_right_parens_mul_add() {
        // a * (b + c) needs parens (I4 fix: was missing before)
        let right = make_binop(Operator::Addition, lit_int(2), lit_int(3));
        assert!(needs_right_parens(Operator::Multiplication, &right));
    }

    #[test]
    fn test_no_parens_add_add() {
        // a + (b + c) does NOT need parens (commutative, same precedence)
        let right = make_binop(Operator::Addition, lit_int(2), lit_int(3));
        assert!(!needs_right_parens(Operator::Addition, &right));
    }

    #[test]
    fn test_no_parens_mul_mul() {
        // a * (b * c) does NOT need parens (commutative, same precedence)
        let right = make_binop(Operator::Multiplication, lit_int(2), lit_int(3));
        assert!(!needs_right_parens(Operator::Multiplication, &right));
    }

    #[test]
    fn test_no_parens_add_mul() {
        // a + (b * c) does NOT need parens (higher precedence inner)
        let right = make_binop(Operator::Multiplication, lit_int(2), lit_int(3));
        assert!(!needs_right_parens(Operator::Addition, &right));
    }

    #[test]
    fn test_no_parens_non_arithmetic() {
        // a AND (b OR c) — handled separately by AND/OR wrapping, not by needs_right_parens
        let right = make_binop(Operator::Or, lit_int(0), lit_int(1));
        assert!(!needs_right_parens(Operator::And, &right));
    }

    #[test]
    fn test_no_parens_literal() {
        // a - 5 does NOT need parens (right is literal, not operator)
        assert!(!needs_right_parens(Operator::Subtraction, &lit_int(5)));
    }

    #[test]
    fn test_needs_left_parens_add_mul() {
        // (a + b) * c needs parens: inner + (prec 1) < outer * (prec 2)
        let left = make_binop(Operator::Addition, lit_int(1), lit_int(2));
        assert!(needs_left_parens(Operator::Multiplication, &left));
    }

    #[test]
    fn test_needs_left_parens_sub_div() {
        // (a - b) / c needs parens: inner - (prec 1) < outer / (prec 2)
        let left = make_binop(Operator::Subtraction, lit_int(5), lit_int(3));
        assert!(needs_left_parens(Operator::Division, &left));
    }

    #[test]
    fn test_no_left_parens_equal_precedence() {
        // (a - b) - c does NOT need parens: left-associative, equal precedence.
        let left = make_binop(Operator::Subtraction, lit_int(1), lit_int(2));
        assert!(!needs_left_parens(Operator::Subtraction, &left));
        // (a / b) * c likewise (equal precedence, left-assoc).
        let left = make_binop(Operator::Division, lit_int(6), lit_int(2));
        assert!(!needs_left_parens(Operator::Multiplication, &left));
    }

    #[test]
    fn test_no_left_parens_higher_precedence() {
        // (a * b) + c does NOT need parens: inner * (prec 2) > outer + (prec 1).
        let left = make_binop(Operator::Multiplication, lit_int(2), lit_int(3));
        assert!(!needs_left_parens(Operator::Addition, &left));
    }

    #[test]
    fn test_no_left_parens_literal() {
        // 5 - a: left literal, not an operator expression.
        assert!(!needs_left_parens(Operator::Subtraction, &lit_int(5)));
    }

    // -------------------------------------------------------------------------
    // map_render_expr combinator
    // -------------------------------------------------------------------------

    fn rprop(col: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("a".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(col.to_string()),
        })
    }

    /// Suffix every property access column with `_x`, recursing everywhere.
    fn suffix(expr: &RenderExpr) -> RenderExpr {
        map_render_expr(expr, &mut |node| {
            if let RenderExpr::PropertyAccessExp(pa) = node {
                RenderRewrite::Replace(RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: pa.table_alias.clone(),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        format!("{}_x", pa.column.raw()),
                    ),
                }))
            } else {
                RenderRewrite::Recurse
            }
        })
    }

    /// The regression this slice targets: the old hand-rolled `RenderExpr`
    /// rewriters' `_ => expr.clone()` catch-all silently skipped property
    /// accesses nested inside List/Case/ArraySubscript/ArraySlicing/ReduceExpr/
    /// MapLiteral wrappers. map_render_expr must reach ALL of them.
    #[test]
    fn map_render_expr_reaches_previously_skipped_wrappers() {
        assert_eq!(
            suffix(&RenderExpr::List(vec![rprop("c1"), rprop("c2")])),
            RenderExpr::List(vec![rprop("c1_x"), rprop("c2_x")])
        );
        assert_eq!(
            suffix(&RenderExpr::MapLiteral(vec![("k".to_string(), rprop("c"))])),
            RenderExpr::MapLiteral(vec![("k".to_string(), rprop("c_x"))])
        );
        assert_eq!(
            suffix(&RenderExpr::ArraySubscript {
                array: Box::new(rprop("arr")),
                index: Box::new(rprop("i")),
            }),
            RenderExpr::ArraySubscript {
                array: Box::new(rprop("arr_x")),
                index: Box::new(rprop("i_x")),
            }
        );
        assert_eq!(
            suffix(&RenderExpr::ArraySlicing {
                array: Box::new(rprop("arr")),
                from: Some(Box::new(rprop("lo"))),
                to: Some(Box::new(rprop("hi"))),
            }),
            RenderExpr::ArraySlicing {
                array: Box::new(rprop("arr_x")),
                from: Some(Box::new(rprop("lo_x"))),
                to: Some(Box::new(rprop("hi_x"))),
            }
        );
        assert_eq!(
            suffix(&RenderExpr::ReduceExpr(ReduceExpr {
                accumulator: "acc".to_string(),
                initial_value: Box::new(rprop("init")),
                variable: "v".to_string(),
                list: Box::new(rprop("lst")),
                expression: Box::new(rprop("body")),
            })),
            RenderExpr::ReduceExpr(ReduceExpr {
                accumulator: "acc".to_string(),
                initial_value: Box::new(rprop("init_x")),
                variable: "v".to_string(),
                list: Box::new(rprop("lst_x")),
                expression: Box::new(rprop("body_x")),
            })
        );
    }

    /// `Replace` returns the replacement without descending into the node.
    #[test]
    fn map_render_expr_replace_stops() {
        let hit = std::cell::Cell::new(0);
        let out = map_render_expr(&RenderExpr::List(vec![rprop("c1")]), &mut |node| {
            hit.set(hit.get() + 1);
            if matches!(node, RenderExpr::List(_)) {
                RenderRewrite::Replace(RenderExpr::Star)
            } else {
                RenderRewrite::Recurse
            }
        });
        assert_eq!(out, RenderExpr::Star);
        assert_eq!(hit.get(), 1); // only the List node visited, no child descent
    }

    /// A pure Recurse over a leaf-heavy tree is an identity clone.
    #[test]
    fn map_render_expr_identity_on_recurse() {
        let expr = make_binop(Operator::Addition, rprop("a"), lit_int(1));
        assert_eq!(
            map_render_expr(&expr, &mut |_| RenderRewrite::Recurse),
            expr
        );
    }

    // -------------------------------------------------------------------------
    // visit_render_expr_mut — the in-place (&mut) structural dual
    // -------------------------------------------------------------------------

    /// Suffix every property-access column with `_x` IN PLACE, recursing
    /// everywhere via the exhaustive `descend_render_expr_mut`.
    fn suffix_mut(expr: &mut RenderExpr) {
        visit_render_expr_mut(expr, &mut |node| {
            if let RenderExpr::PropertyAccessExp(pa) = node {
                pa.column = crate::graph_catalog::expression_parser::PropertyValue::Column(
                    format!("{}_x", pa.column.raw()),
                );
                MutVisit::Stop
            } else {
                MutVisit::Recurse
            }
        });
    }

    /// The regression this slice targets (#929): the hand-rolled `&mut
    /// RenderExpr` denorm remappers' `_ => {}` catch-all silently skipped
    /// property accesses nested inside
    /// Case/List/ArraySubscript/ArraySlicing/ReduceExpr/MapLiteral wrappers.
    /// `visit_render_expr_mut` must reach ALL of them.
    #[test]
    fn visit_render_expr_mut_reaches_previously_skipped_wrappers() {
        // Case — the specific #929 shape (denorm property buried in CASE).
        let mut case = RenderExpr::Case(RenderCase {
            expr: Some(Box::new(rprop("scrut"))),
            when_then: vec![(rprop("w"), rprop("t"))],
            else_expr: Some(Box::new(rprop("e"))),
        });
        suffix_mut(&mut case);
        assert_eq!(
            case,
            RenderExpr::Case(RenderCase {
                expr: Some(Box::new(rprop("scrut_x"))),
                when_then: vec![(rprop("w_x"), rprop("t_x"))],
                else_expr: Some(Box::new(rprop("e_x"))),
            })
        );

        let mut list = RenderExpr::List(vec![rprop("c1"), rprop("c2")]);
        suffix_mut(&mut list);
        assert_eq!(list, RenderExpr::List(vec![rprop("c1_x"), rprop("c2_x")]));

        let mut sub = RenderExpr::ArraySubscript {
            array: Box::new(rprop("arr")),
            index: Box::new(rprop("i")),
        };
        suffix_mut(&mut sub);
        assert_eq!(
            sub,
            RenderExpr::ArraySubscript {
                array: Box::new(rprop("arr_x")),
                index: Box::new(rprop("i_x")),
            }
        );

        let mut reduce = RenderExpr::ReduceExpr(ReduceExpr {
            accumulator: "acc".to_string(),
            initial_value: Box::new(rprop("init")),
            variable: "v".to_string(),
            list: Box::new(rprop("lst")),
            expression: Box::new(rprop("body")),
        });
        suffix_mut(&mut reduce);
        assert_eq!(
            reduce,
            RenderExpr::ReduceExpr(ReduceExpr {
                accumulator: "acc".to_string(),
                initial_value: Box::new(rprop("init_x")),
                variable: "v".to_string(),
                list: Box::new(rprop("lst_x")),
                expression: Box::new(rprop("body_x")),
            })
        );
    }

    /// Descent policy parity with `map_render_expr`: `Stop` handles a node
    /// without descending into its children.
    #[test]
    fn visit_render_expr_mut_stop_skips_children() {
        let mut expr = RenderExpr::List(vec![rprop("c1")]);
        let mut hits = 0;
        visit_render_expr_mut(&mut expr, &mut |node| {
            hits += 1;
            if matches!(node, RenderExpr::List(_)) {
                MutVisit::Stop
            } else {
                MutVisit::Recurse
            }
        });
        assert_eq!(hits, 1); // only the List node visited, no child descent
                             // Unchanged — the closure stopped without mutating.
        assert_eq!(expr, RenderExpr::List(vec![rprop("c1")]));
    }
}
