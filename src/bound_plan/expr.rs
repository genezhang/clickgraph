//! Binding expressions: AST expression -> [`LogicalExpr`] with every variable
//! renamed to its binding's generated name (`types.rs`).
//!
//! The AST is converted with the existing converter
//! (`logical_expr/ast_conversion.rs`, which also lowers list comprehensions to
//! lambdas), then renamed by an exhaustive walk that resolves each variable
//! against the clause's scope and the expression's own local variables
//! (lambda parameters, `reduce` accumulator and element), innermost first.
//! A name that resolves nowhere is "Variable `x` not defined".
//!
//! Expressions that contain graph patterns (EXISTS, pattern predicates,
//! `size(pattern)`, pattern comprehensions) are correlated subqueries: they
//! are bound in S9 and are `Unsupported` here.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::open_cypher_parser::ast::Expression;
use crate::query_planner::logical_expr::{
    AggregateFnCall, LambdaExpr, LogicalCase, LogicalExpr, OperatorApplication, PropertyAccess,
    ReduceExpr, ScalarFnCall, TableAlias,
};

use super::types::{BindError, VarId};

/// Resolves the names an expression may reference, and allocates bindings for
/// the expression's own local variables.
pub(crate) trait NameEnv {
    /// The binding `name` refers to in the clause's scope, if any.
    fn lookup(&self, name: &str) -> Option<VarId>;
    /// A new local binding (lambda parameter, reduce variable).
    fn new_local(&mut self, name: &str) -> VarId;
}

/// Bind one AST expression.
pub(crate) fn bind_expr(
    ast: &Expression<'_>,
    env: &mut dyn NameEnv,
) -> Result<LogicalExpr, BindError> {
    let converted = LogicalExpr::try_from(ast.clone())
        .map_err(|e| BindError::Unsupported(format!("expression: {e}")))?;
    let mut locals: Vec<(String, VarId)> = Vec::new();
    rename(converted, env, &mut locals)
}

fn resolve(name: &str, env: &dyn NameEnv, locals: &[(String, VarId)]) -> Result<VarId, BindError> {
    locals
        .iter()
        .rev()
        .find(|(n, _)| n == name)
        .map(|(_, v)| *v)
        .or_else(|| env.lookup(name))
        .ok_or_else(|| BindError::UndefinedVariable(name.to_string()))
}

fn rename_all(
    exprs: Vec<LogicalExpr>,
    env: &mut dyn NameEnv,
    locals: &mut Vec<(String, VarId)>,
) -> Result<Vec<LogicalExpr>, BindError> {
    exprs.into_iter().map(|e| rename(e, env, locals)).collect()
}

fn rename_box(
    mut e: Box<LogicalExpr>,
    env: &mut dyn NameEnv,
    locals: &mut Vec<(String, VarId)>,
) -> Result<Box<LogicalExpr>, BindError> {
    // In place, reusing the allocation.
    *e = rename(std::mem::replace(&mut *e, LogicalExpr::Star), env, locals)?;
    Ok(e)
}

/// Exhaustive on purpose: a new `LogicalExpr` variant is a compile error here
/// until it is decided whether it references variables.
fn rename(
    expr: LogicalExpr,
    env: &mut dyn NameEnv,
    locals: &mut Vec<(String, VarId)>,
) -> Result<LogicalExpr, BindError> {
    Ok(match expr {
        LogicalExpr::Literal(_)
        | LogicalExpr::Raw(_)
        | LogicalExpr::Star
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Column(_)
        | LogicalExpr::ColumnAlias(_) => expr,
        LogicalExpr::TableAlias(TableAlias(name)) => {
            LogicalExpr::TableAlias(TableAlias(resolve(&name, env, locals)?.name()))
        }
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias,
            column,
        }) => {
            let column = match column {
                PropertyValue::Column(c) => PropertyValue::Column(c),
                other => {
                    return Err(BindError::Unsupported(format!(
                        "computed property access {other:?}"
                    )))
                }
            };
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(resolve(&table_alias.0, env, locals)?.name()),
                column,
            })
        }
        LogicalExpr::LabelExpression { variable, label } => LogicalExpr::LabelExpression {
            variable: resolve(&variable, env, locals)?.name(),
            label,
        },
        LogicalExpr::Operator(op) => LogicalExpr::Operator(rename_op(op, env, locals)?),
        LogicalExpr::OperatorApplicationExp(op) => {
            LogicalExpr::OperatorApplicationExp(rename_op(op, env, locals)?)
        }
        LogicalExpr::List(items) => LogicalExpr::List(rename_all(items, env, locals)?),
        LogicalExpr::AggregateFnCall(AggregateFnCall { name, args }) => {
            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name,
                args: rename_all(args, env, locals)?,
            })
        }
        LogicalExpr::ScalarFnCall(ScalarFnCall { name, args }) => {
            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name,
                args: rename_all(args, env, locals)?,
            })
        }
        LogicalExpr::Case(LogicalCase {
            expr,
            when_then,
            else_expr,
        }) => LogicalExpr::Case(LogicalCase {
            expr: expr.map(|e| rename_box(e, env, locals)).transpose()?,
            when_then: when_then
                .into_iter()
                .map(|(w, t)| Ok((rename(w, env, locals)?, rename(t, env, locals)?)))
                .collect::<Result<_, BindError>>()?,
            else_expr: else_expr.map(|e| rename_box(e, env, locals)).transpose()?,
        }),
        LogicalExpr::ReduceExpr(ReduceExpr {
            accumulator,
            initial_value,
            variable,
            list,
            expression,
        }) => {
            if accumulator == variable {
                return Err(BindError::AlreadyDeclared(variable));
            }
            let initial_value = rename_box(initial_value, env, locals)?;
            let list = rename_box(list, env, locals)?;
            let acc = env.new_local(&accumulator);
            let var = env.new_local(&variable);
            locals.push((accumulator, acc));
            locals.push((variable, var));
            let expression = rename_box(expression, env, locals);
            locals.truncate(locals.len() - 2);
            LogicalExpr::ReduceExpr(ReduceExpr {
                accumulator: acc.name(),
                initial_value,
                variable: var.name(),
                list,
                expression: expression?,
            })
        }
        LogicalExpr::Lambda(LambdaExpr { params, body }) => {
            let depth = locals.len();
            let mut renamed = Vec::with_capacity(params.len());
            for p in params {
                let v = env.new_local(&p);
                renamed.push(v.name());
                locals.push((p, v));
            }
            let body = rename_box(body, env, locals);
            locals.truncate(depth);
            LogicalExpr::Lambda(LambdaExpr {
                params: renamed,
                body: body?,
            })
        }
        LogicalExpr::MapLiteral(entries) => LogicalExpr::MapLiteral(
            entries
                .into_iter()
                .map(|(k, v)| Ok((k, rename(v, env, locals)?)))
                .collect::<Result<_, BindError>>()?,
        ),
        LogicalExpr::ArraySubscript { array, index } => LogicalExpr::ArraySubscript {
            array: rename_box(array, env, locals)?,
            index: rename_box(index, env, locals)?,
        },
        LogicalExpr::ArraySlicing { array, from, to } => LogicalExpr::ArraySlicing {
            array: rename_box(array, env, locals)?,
            from: from.map(|e| rename_box(e, env, locals)).transpose()?,
            to: to.map(|e| rename_box(e, env, locals)).transpose()?,
        },
        // Graph patterns in expressions are correlated subqueries (S9).
        LogicalExpr::PathPattern(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::PatternCount(_)
        | LogicalExpr::PatternComprehension(_)
        | LogicalExpr::InSubquery(_) => {
            return Err(BindError::Unsupported(
                "a graph pattern inside an expression (EXISTS, pattern predicate, size(pattern), \
                 pattern comprehension)"
                    .to_string(),
            ))
        }
        // Produced only by the legacy analyzer, never by AST conversion.
        LogicalExpr::CteEntityRef(_) => {
            return Err(BindError::Unsupported("CteEntityRef".to_string()))
        }
    })
}

fn rename_op(
    op: OperatorApplication,
    env: &mut dyn NameEnv,
    locals: &mut Vec<(String, VarId)>,
) -> Result<OperatorApplication, BindError> {
    Ok(OperatorApplication {
        operator: op.operator,
        operands: rename_all(op.operands, env, locals)?,
    })
}

/// Does the (bound) expression contain an aggregate function call?
pub(crate) fn contains_aggregate(expr: &LogicalExpr) -> bool {
    let mut found = false;
    visit(expr, &mut |e| {
        if matches!(e, LogicalExpr::AggregateFnCall(_)) {
            found = true;
        }
    });
    found
}

/// The bindings an expression references (by generated name), excluding
/// those inside aggregate calls when `outside_aggregates`.
pub(crate) fn referenced_names(expr: &LogicalExpr, outside_aggregates: bool) -> Vec<String> {
    fn go(e: &LogicalExpr, outside: bool, out: &mut Vec<String>) {
        if outside && matches!(e, LogicalExpr::AggregateFnCall(_)) {
            return;
        }
        match e {
            LogicalExpr::TableAlias(TableAlias(n)) => out.push(n.clone()),
            LogicalExpr::PropertyAccessExp(pa) => out.push(pa.table_alias.0.clone()),
            LogicalExpr::LabelExpression { variable, .. } => out.push(variable.clone()),
            _ => {}
        }
        for child in children(e) {
            go(child, outside, out);
        }
    }
    let mut out = Vec::new();
    go(expr, outside_aggregates, &mut out);
    out
}

/// Replace every subtree equal to one of `targets[i].0` with `targets[i].1`.
pub(crate) fn replace_subtrees(
    expr: LogicalExpr,
    targets: &[(LogicalExpr, LogicalExpr)],
) -> LogicalExpr {
    if let Some((_, with)) = targets.iter().find(|(t, _)| *t == expr) {
        return with.clone();
    }
    map_children(expr, &mut |c| replace_subtrees(c, targets))
}

/// Re-point every reference to a binding in `map` (bare, property access,
/// label test) at the mapped binding.
pub(crate) fn rename_refs(
    expr: LogicalExpr,
    map: &std::collections::HashMap<String, String>,
) -> LogicalExpr {
    match expr {
        LogicalExpr::TableAlias(TableAlias(n)) => {
            LogicalExpr::TableAlias(TableAlias(map.get(&n).cloned().unwrap_or(n)))
        }
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias,
            column,
        }) => LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(map.get(&table_alias.0).cloned().unwrap_or(table_alias.0)),
            column,
        }),
        LogicalExpr::LabelExpression { variable, label } => LogicalExpr::LabelExpression {
            variable: map.get(&variable).cloned().unwrap_or(variable),
            label,
        },
        other => map_children(other, &mut |c| rename_refs(c, map)),
    }
}

fn visit(e: &LogicalExpr, f: &mut dyn FnMut(&LogicalExpr)) {
    f(e);
    for c in children(e) {
        visit(c, f);
    }
}

fn children(e: &LogicalExpr) -> Vec<&LogicalExpr> {
    match e {
        LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
            op.operands.iter().collect()
        }
        LogicalExpr::List(items) => items.iter().collect(),
        LogicalExpr::AggregateFnCall(f) => f.args.iter().collect(),
        LogicalExpr::ScalarFnCall(f) => f.args.iter().collect(),
        LogicalExpr::Case(c) => {
            let mut v: Vec<&LogicalExpr> = Vec::new();
            v.extend(c.expr.as_deref());
            for (w, t) in &c.when_then {
                v.push(w);
                v.push(t);
            }
            v.extend(c.else_expr.as_deref());
            v
        }
        LogicalExpr::ReduceExpr(r) => vec![&*r.initial_value, &*r.list, &*r.expression],
        LogicalExpr::Lambda(l) => vec![&*l.body],
        LogicalExpr::MapLiteral(entries) => entries.iter().map(|(_, v)| v).collect(),
        LogicalExpr::ArraySubscript { array, index } => vec![&**array, &**index],
        LogicalExpr::ArraySlicing { array, from, to } => {
            let mut v = vec![&**array];
            v.extend(from.as_deref());
            v.extend(to.as_deref());
            v
        }
        _ => Vec::new(),
    }
}

fn map_children(e: LogicalExpr, f: &mut dyn FnMut(LogicalExpr) -> LogicalExpr) -> LogicalExpr {
    let fb = |b: Box<LogicalExpr>, f: &mut dyn FnMut(LogicalExpr) -> LogicalExpr| Box::new(f(*b));
    match e {
        LogicalExpr::Operator(op) => LogicalExpr::Operator(OperatorApplication {
            operator: op.operator,
            operands: op.operands.into_iter().map(&mut *f).collect(),
        }),
        LogicalExpr::OperatorApplicationExp(op) => {
            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op.operands.into_iter().map(&mut *f).collect(),
            })
        }
        LogicalExpr::List(items) => LogicalExpr::List(items.into_iter().map(&mut *f).collect()),
        LogicalExpr::AggregateFnCall(c) => LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: c.name,
            args: c.args.into_iter().map(&mut *f).collect(),
        }),
        LogicalExpr::ScalarFnCall(c) => LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: c.name,
            args: c.args.into_iter().map(&mut *f).collect(),
        }),
        LogicalExpr::Case(c) => LogicalExpr::Case(LogicalCase {
            expr: c.expr.map(|b| fb(b, f)),
            when_then: c.when_then.into_iter().map(|(w, t)| (f(w), f(t))).collect(),
            else_expr: c.else_expr.map(|b| fb(b, f)),
        }),
        LogicalExpr::ReduceExpr(r) => LogicalExpr::ReduceExpr(ReduceExpr {
            accumulator: r.accumulator,
            initial_value: fb(r.initial_value, f),
            variable: r.variable,
            list: fb(r.list, f),
            expression: fb(r.expression, f),
        }),
        LogicalExpr::Lambda(l) => LogicalExpr::Lambda(LambdaExpr {
            params: l.params,
            body: fb(l.body, f),
        }),
        LogicalExpr::MapLiteral(entries) => {
            LogicalExpr::MapLiteral(entries.into_iter().map(|(k, v)| (k, f(v))).collect())
        }
        LogicalExpr::ArraySubscript { array, index } => LogicalExpr::ArraySubscript {
            array: fb(array, f),
            index: fb(index, f),
        },
        LogicalExpr::ArraySlicing { array, from, to } => LogicalExpr::ArraySlicing {
            array: fb(array, f),
            from: from.map(|b| fb(b, f)),
            to: to.map(|b| fb(b, f)),
        },
        other => other,
    }
}
