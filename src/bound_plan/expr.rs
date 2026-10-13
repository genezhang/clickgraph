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
//! `exists(pattern)`, `size(pattern)`, pattern comprehensions) are correlated
//! subqueries (S9, §4.12). Before conversion each is taken out of the AST and
//! bound by the binder as a MATCH over the expression's scope
//! ([`NameEnv::subquery`]); the expression keeps a placeholder call
//! `__cg_subquery(<var>)` ([`SUBQUERY_FN`]), whose arguments become the
//! subquery's value binding and its correlation when renamed. A subquery
//! inside a list comprehension, `reduce` or lambda (which could read the
//! local variable) is `Unsupported`.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::open_cypher_parser::ast::{self, Expression, PathPattern};
use crate::query_planner::logical_expr::{
    AggregateFnCall, LambdaExpr, Literal, LogicalCase, LogicalExpr, OperatorApplication,
    PropertyAccess, ReduceExpr, ScalarFnCall, TableAlias,
};

use super::types::{BindError, VarId};

/// The placeholder function a subquery expression's value is read through:
/// `__cg_subquery(<var>, <correlation>...)` (`types::Subquery`).
pub(crate) const SUBQUERY_FN: &str = "__cg_subquery";

/// What a subquery expression, as written, computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawKind {
    /// `EXISTS { … }`, `exists(pattern)`.
    Exists,
    /// A pattern used as a predicate: it may not introduce variables.
    Predicate,
    /// `size(pattern)`, `size([pattern …])`.
    Count,
    /// A pattern comprehension.
    Collect,
    /// The nodes of a pattern predicate's matches in a list comprehension's
    /// element's place (its projection names them), for `x IN` (S11o9);
    /// `predicate`: of a pattern used as a predicate.
    Members { predicate: bool },
}

/// A subquery expression taken out of its expression.
pub(crate) struct RawSubquery<'a> {
    pub kind: RawKind,
    pub pattern: PathPattern<'a>,
    pub where_: Option<Expression<'a>>,
    /// A pattern comprehension's projection (also of one under `size()`,
    /// bound for its errors only).
    pub projection: Option<Expression<'a>>,
    /// [`RawKind::Members`]: the list of the comprehension whose element
    /// the predicate reads, bound in the enclosing scope (its matches are
    /// looked for from its elements), when it reads no local.
    pub elements: Option<Expression<'a>>,
}

/// Resolves the names an expression may reference, and allocates bindings for
/// the expression's own local variables.
pub(crate) trait NameEnv {
    /// The binding `name` refers to in the clause's scope, if any.
    fn lookup(&self, name: &str) -> Option<VarId>;
    /// A new local binding (lambda parameter, reduce variable).
    fn new_local(&mut self, name: &str) -> VarId;
    /// Bind a subquery expression in the clause's scope: the binding of its
    /// value.
    fn subquery(&mut self, sq: RawSubquery<'_>) -> Result<VarId, BindError>;
    /// The correlation of the subquery whose value is `var`.
    fn correlation(&self, var: VarId) -> Option<Vec<VarId>>;
}

/// Bind one AST expression.
pub(crate) fn bind_expr(
    ast: &Expression<'_>,
    env: &mut dyn NameEnv,
) -> Result<LogicalExpr, BindError> {
    let mut ast = ast.clone();
    hoist_subqueries(&mut ast, env, &[])?;
    let converted = LogicalExpr::try_from(ast)
        .map_err(|e| BindError::Unsupported(format!("expression: {e}")))?;
    let mut locals: Vec<(String, VarId)> = Vec::new();
    rename(converted, env, &mut locals)
}

/// Does the AST expression contain a subquery expression?
pub(crate) fn has_subquery(e: &Expression<'_>) -> bool {
    match e {
        Expression::ExistsExpression(_) | Expression::PatternComprehension(_) => true,
        Expression::PathPattern(PathPattern::ConnectedPattern(_)) => true,
        _ => ast_children(e).into_iter().any(has_subquery),
    }
}

/// The subexpressions of an AST expression.
fn ast_children<'e, 'a>(e: &'e Expression<'a>) -> Vec<&'e Expression<'a>> {
    match e {
        Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::PropertyAccessExp(_)
        | Expression::LabelExpression { .. }
        | Expression::PathPattern(_)
        | Expression::ExistsExpression(_)
        | Expression::PatternComprehension(_) => Vec::new(),
        Expression::List(xs) => xs.iter().collect(),
        Expression::FunctionCallExp(f) => f.args.iter().collect(),
        Expression::OperatorApplicationExp(op) => op.operands.iter().collect(),
        Expression::Case(c) => {
            let mut v: Vec<&Expression<'a>> = c.expr.iter().map(|b| &**b).collect();
            for (w, t) in &c.when_then {
                v.push(w);
                v.push(t);
            }
            v.extend(c.else_expr.iter().map(|b| &**b));
            v
        }
        Expression::ReduceExp(r) => vec![&*r.initial_value, &*r.list, &*r.expression],
        Expression::MapLiteral(entries) => entries.iter().map(|(_, v)| v).collect(),
        Expression::Lambda(l) => vec![&*l.body],
        Expression::ListComprehension(lc) => {
            let mut v = vec![&*lc.list_expr];
            v.extend(lc.where_clause.iter().map(|b| &**b));
            v.extend(lc.projection.iter().map(|b| &**b));
            v
        }
        Expression::ArraySubscript { array, index } => vec![&**array, &**index],
        Expression::ArraySlicing { array, from, to } => {
            let mut v = vec![&**array];
            v.extend(from.iter().map(|b| &**b));
            v.extend(to.iter().map(|b| &**b));
            v
        }
    }
}

/// Replace every subquery expression of `e` by its placeholder, binding it
/// through `env`. `locals`: the names a list comprehension, `reduce` or
/// lambda around `e` binds. A subquery reading none of them is bound as
/// any (its value is the same for every element); a pattern predicate
/// whose only one is a node of its pattern, `x` (LDBC IC 10's
/// `[p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]`), is
/// `x IN` the nodes in `x`'s place of the pattern's matches
/// ([`SubqueryKind::Members`], S11o9). Others are not lowered.
fn hoist_subqueries<'a>(
    e: &mut Expression<'a>,
    env: &mut dyn NameEnv,
    locals: &[Local<'a>],
) -> Result<(), BindError> {
    let raw = match e {
        Expression::ExistsExpression(_) | Expression::PatternComprehension(_) => true,
        Expression::PathPattern(PathPattern::ConnectedPattern(_)) => true,
        Expression::FunctionCallExp(f) => {
            if f.name.eq_ignore_ascii_case(SUBQUERY_FN) {
                return Err(BindError::Unsupported(format!(
                    "a function named {SUBQUERY_FN}"
                )));
            }
            let name = f.name.to_ascii_lowercase();
            match f.args.as_slice() {
                [Expression::PathPattern(PathPattern::ConnectedPattern(_))] => {
                    name == "size" || name == "exists"
                }
                [Expression::PatternComprehension(_)] => name == "size",
                _ => false,
            }
        }
        _ => false,
    };
    if raw {
        let mut used = Vec::new();
        ast_names(e, &mut used);
        let mut read: Vec<&str> = locals
            .iter()
            .map(|l| l.name)
            .filter(|l| used.contains(l))
            .collect();
        read.dedup();
        if !read.is_empty() {
            let Some((x, mut sq)) = element_membership(e, &read) else {
                return Err(BindError::Unsupported(
                    "a subquery expression inside a list comprehension, reduce or lambda"
                        .to_string(),
                ));
            };
            // The innermost `x`'s list.
            sq.elements = locals
                .iter()
                .rev()
                .find(|l| l.name == x)
                .and_then(|l| l.list.clone());
            let var = env.subquery(sq)?;
            *e = Expression::OperatorApplicationExp(ast::OperatorApplication {
                operator: ast::Operator::In,
                operands: vec![Expression::Variable(x), placeholder(var)],
            });
            return Ok(());
        }
        let taken = std::mem::replace(e, Expression::Literal(ast::Literal::Null));
        let sq = match taken {
            Expression::ExistsExpression(x) => RawSubquery {
                kind: RawKind::Exists,
                pattern: x.pattern,
                where_: x.where_clause.map(|w| w.conditions),
                projection: None,
                elements: None,
            },
            Expression::PathPattern(pattern) => RawSubquery {
                kind: RawKind::Predicate,
                pattern,
                where_: None,
                projection: None,
                elements: None,
            },
            Expression::PatternComprehension(pc) => RawSubquery {
                kind: RawKind::Collect,
                pattern: *pc.pattern,
                where_: pc.where_clause.map(|w| *w),
                projection: Some(*pc.projection),
                elements: None,
            },
            Expression::FunctionCallExp(mut f) => {
                let exists = f.name.eq_ignore_ascii_case("exists");
                match f.args.pop() {
                    Some(Expression::PathPattern(pattern)) => RawSubquery {
                        kind: if exists {
                            RawKind::Exists
                        } else {
                            RawKind::Count
                        },
                        pattern,
                        where_: None,
                        projection: None,
                        elements: None,
                    },
                    Some(Expression::PatternComprehension(pc)) => RawSubquery {
                        kind: RawKind::Count,
                        pattern: *pc.pattern,
                        where_: pc.where_clause.map(|w| *w),
                        projection: Some(*pc.projection),
                        elements: None,
                    },
                    _ => {
                        return Err(BindError::Unsupported(
                            "internal: a subquery call without its pattern".to_string(),
                        ))
                    }
                }
            }
            _ => {
                return Err(BindError::Unsupported(
                    "internal: not a subquery expression".to_string(),
                ))
            }
        };
        let var = env.subquery(sq)?;
        *e = placeholder(var);
        return Ok(());
    }
    let with = |names: &[&'a str], list: Option<Expression<'a>>| -> Vec<Local<'a>> {
        let mut inner = locals.to_vec();
        inner.extend(names.iter().map(|&name| Local {
            name,
            list: list.clone(),
        }));
        inner
    };
    match e {
        Expression::ReduceExp(r) => {
            hoist_subqueries(&mut r.initial_value, env, locals)?;
            hoist_subqueries(&mut r.list, env, locals)?;
            let inner = with(&[r.accumulator, r.variable], None);
            hoist_subqueries(&mut r.expression, env, &inner)
        }
        Expression::Lambda(l) => {
            let inner = with(&l.params, None);
            hoist_subqueries(&mut l.body, env, &inner)
        }
        Expression::ListComprehension(lc) => {
            // Its list as written, when it reads no local and no subquery.
            let mut names = Vec::new();
            ast_names(&lc.list_expr, &mut names);
            let list = (!has_subquery(&lc.list_expr)
                && !locals.iter().any(|l| names.contains(&l.name)))
            .then(|| (*lc.list_expr).clone());
            hoist_subqueries(&mut lc.list_expr, env, locals)?;
            let inner = with(&[lc.variable], list);
            if let Some(w) = lc.where_clause.as_mut() {
                hoist_subqueries(w, env, &inner)?;
            }
            if let Some(p) = lc.projection.as_mut() {
                hoist_subqueries(p, env, &inner)?;
            }
            Ok(())
        }
        Expression::List(xs) => xs
            .iter_mut()
            .try_for_each(|x| hoist_subqueries(x, env, locals)),
        Expression::FunctionCallExp(f) => f
            .args
            .iter_mut()
            .try_for_each(|x| hoist_subqueries(x, env, locals)),
        Expression::OperatorApplicationExp(op) => op
            .operands
            .iter_mut()
            .try_for_each(|x| hoist_subqueries(x, env, locals)),
        Expression::Case(c) => {
            if let Some(x) = c.expr.as_mut() {
                hoist_subqueries(x, env, locals)?;
            }
            for (w, t) in c.when_then.iter_mut() {
                hoist_subqueries(w, env, locals)?;
                hoist_subqueries(t, env, locals)?;
            }
            if let Some(x) = c.else_expr.as_mut() {
                hoist_subqueries(x, env, locals)?;
            }
            Ok(())
        }
        Expression::MapLiteral(entries) => entries
            .iter_mut()
            .try_for_each(|(_, v)| hoist_subqueries(v, env, locals)),
        Expression::ArraySubscript { array, index } => {
            hoist_subqueries(array, env, locals)?;
            hoist_subqueries(index, env, locals)
        }
        Expression::ArraySlicing { array, from, to } => {
            hoist_subqueries(array, env, locals)?;
            if let Some(x) = from.as_mut() {
                hoist_subqueries(x, env, locals)?;
            }
            if let Some(x) = to.as_mut() {
                hoist_subqueries(x, env, locals)?;
            }
            Ok(())
        }
        Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::PropertyAccessExp(_)
        | Expression::LabelExpression { .. }
        | Expression::PathPattern(_)
        | Expression::ExistsExpression(_)
        | Expression::PatternComprehension(_) => Ok(()),
    }
}

/// A name a list comprehension, `reduce` or lambda binds; a list
/// comprehension's with its list when that reads no such name and no
/// subquery ([`RawSubquery::elements`]).
#[derive(Clone)]
struct Local<'a> {
    name: &'a str,
    list: Option<Expression<'a>>,
}

/// The name a pattern predicate's element takes in its membership rewrite
/// ([`element_membership`]).
pub(crate) const MEMBER: &str = "__cg_member";

/// A pattern predicate (`(x)-[…]->(…)`, `exists(…)`, `EXISTS { … }`) whose
/// only name of `read` (a lambda's) is `x`, a node of its pattern and
/// nowhere else: `x`, and the subquery of the nodes `m` in its place
/// (`(m)-[…]->(…)`, `m`: [`MEMBER`]) whose value `x IN` reads.
fn element_membership<'a>(
    e: &Expression<'a>,
    read: &[&'a str],
) -> Option<(&'a str, RawSubquery<'a>)> {
    let [x] = read else {
        return None;
    };
    let predicate = matches!(e, Expression::PathPattern(_));
    let (pattern, where_) = match e {
        Expression::PathPattern(p @ PathPattern::ConnectedPattern(_)) => (p.clone(), None),
        Expression::ExistsExpression(x) => (
            x.pattern.clone(),
            x.where_clause.as_ref().map(|w| w.conditions.clone()),
        ),
        Expression::FunctionCallExp(f) if f.name.eq_ignore_ascii_case("exists") => {
            match f.args.as_slice() {
                [Expression::PathPattern(p)] => (p.clone(), None),
                _ => return None,
            }
        }
        _ => return None,
    };
    // `x` a node of the pattern, read nowhere else (its WHERE, a property
    // map, a relationship).
    if let Some(w) = &where_ {
        let mut names = Vec::new();
        ast_names(w, &mut names);
        if names.contains(x) {
            return None;
        }
    }
    let PathPattern::ConnectedPattern(steps) = &pattern else {
        return None;
    };
    let mut seen = false;
    for step in steps {
        if step.relationship.name == Some(*x)
            || property_names(step.relationship.properties.as_ref()).contains(x)
        {
            return None;
        }
        for node in [&step.start_node, &step.end_node] {
            let n = node.borrow();
            if property_names(n.properties.as_ref()).contains(x) {
                return None;
            }
            seen |= n.name == Some(*x);
        }
    }
    let mut names = Vec::new();
    ast_names(e, &mut names);
    if !seen || names.contains(&MEMBER) {
        return None;
    }
    // A copy of the pattern, `x` named `m` (its nodes are shared between
    // steps: one `Rc` each, copied once).
    let mut copied: Vec<(
        *const (),
        std::rc::Rc<std::cell::RefCell<ast::NodePattern<'a>>>,
    )> = Vec::new();
    let mut copy = |n: &std::rc::Rc<std::cell::RefCell<ast::NodePattern<'a>>>| {
        let key = std::rc::Rc::as_ptr(n).cast::<()>();
        if let Some((_, c)) = copied.iter().find(|(k, _)| *k == key) {
            return c.clone();
        }
        let mut node = n.borrow().clone();
        if node.name == Some(*x) {
            node.name = Some(MEMBER);
        }
        let c = std::rc::Rc::new(std::cell::RefCell::new(node));
        copied.push((key, c.clone()));
        c
    };
    let renamed = steps
        .iter()
        .map(|s| ast::ConnectedPattern {
            start_node: copy(&s.start_node),
            relationship: s.relationship.clone(),
            end_node: copy(&s.end_node),
        })
        .collect();
    Some((
        x,
        RawSubquery {
            kind: RawKind::Members { predicate },
            pattern: PathPattern::ConnectedPattern(renamed),
            where_,
            projection: Some(Expression::Variable(MEMBER)),
            elements: None,
        },
    ))
}

/// The placeholder call reading subquery `var`'s value.
fn placeholder<'a>(var: VarId) -> Expression<'a> {
    Expression::FunctionCallExp(ast::FunctionCall {
        name: SUBQUERY_FN.to_string(),
        args: vec![Expression::Literal(ast::Literal::Integer(i64::from(var.0)))],
    })
}

/// The names a property map's values read.
fn property_names<'a>(props: Option<&Vec<ast::Property<'a>>>) -> Vec<&'a str> {
    let mut out = Vec::new();
    for p in props.into_iter().flatten() {
        if let ast::Property::PropertyKV(kv) = p {
            ast_names(&kv.value, &mut out);
        }
    }
    out
}

/// Every variable name an AST expression reads, its patterns' included.
fn ast_names<'a>(e: &Expression<'a>, out: &mut Vec<&'a str>) {
    let pattern = |p: &PathPattern<'a>, out: &mut Vec<&'a str>| {
        let node = |n: &ast::NodePattern<'a>, out: &mut Vec<&'a str>| {
            out.extend(n.name);
            out.extend(property_names(n.properties.as_ref()));
        };
        match p {
            PathPattern::Node(n) => node(n, out),
            PathPattern::ConnectedPattern(steps) => {
                for s in steps {
                    node(&s.start_node.borrow(), out);
                    node(&s.end_node.borrow(), out);
                    out.extend(s.relationship.name);
                    out.extend(property_names(s.relationship.properties.as_ref()));
                }
            }
            PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {}
        }
    };
    match e {
        Expression::Variable(n) => out.push(n),
        Expression::PropertyAccessExp(pa) => out.push(pa.base),
        Expression::LabelExpression { variable, .. } => out.push(variable),
        Expression::PathPattern(p) => pattern(p, out),
        Expression::ExistsExpression(x) => {
            pattern(&x.pattern, out);
            if let Some(w) = &x.where_clause {
                ast_names(&w.conditions, out);
            }
        }
        Expression::PatternComprehension(pc) => {
            pattern(&pc.pattern, out);
            if let Some(w) = &pc.where_clause {
                ast_names(w, out);
            }
            ast_names(&pc.projection, out);
        }
        _ => {
            for c in ast_children(e) {
                ast_names(c, out);
            }
        }
    }
}

/// The subquery whose value a bound expression's placeholder call reads.
pub(crate) fn subquery_var(e: &LogicalExpr) -> Option<VarId> {
    match e {
        LogicalExpr::ScalarFnCall(f) if f.name == SUBQUERY_FN => match f.args.first() {
            Some(LogicalExpr::Literal(Literal::Integer(i))) => u32::try_from(*i).ok().map(VarId),
            _ => None,
        },
        _ => None,
    }
}

/// The subqueries a bound expression reads (not those nested in them).
pub(crate) fn subquery_vars(e: &LogicalExpr) -> Vec<VarId> {
    let mut out = Vec::new();
    visit(e, &mut |x| out.extend(subquery_var(x)));
    out
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
                _ => {
                    return Err(BindError::Unsupported(
                        "a computed property access".to_string(),
                    ))
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
                args: rename_all(args.into_iter().map(hoist_distinct).collect(), env, locals)?,
            })
        }
        // A subquery's placeholder: its value binding, then its correlation
        // (what every check of the variables an expression reads sees).
        LogicalExpr::ScalarFnCall(ScalarFnCall { name, args }) if name == SUBQUERY_FN => {
            let var = match args.as_slice() {
                [LogicalExpr::Literal(Literal::Integer(i))] => u32::try_from(*i).ok().map(VarId),
                _ => None,
            };
            let Some((var, correlation)) = var.and_then(|v| Some((v, env.correlation(v)?))) else {
                return Err(BindError::Unsupported(format!(
                    "a function named {SUBQUERY_FN}"
                )));
            };
            let mut args = vec![LogicalExpr::Literal(Literal::Integer(i64::from(var.0)))];
            args.extend(
                correlation
                    .into_iter()
                    .map(|v| LogicalExpr::TableAlias(TableAlias(v.name()))),
            );
            LogicalExpr::ScalarFnCall(ScalarFnCall { name, args })
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
            if let Some(p) = params
                .iter()
                .enumerate()
                .find_map(|(i, p)| params[..i].contains(p).then(|| p.clone()))
            {
                return Err(BindError::AlreadyDeclared(p));
            }
            let depth = locals.len();
            let mut renamed = Vec::with_capacity(params.len());
            for p in params {
                let v = env.new_local(&p);
                renamed.push(v.name());
                locals.push((p, v));
            }
            let body = rename_box(body, env, locals);
            locals.truncate(depth);
            let body = body?;
            // As in Neo4j: an aggregate is not evaluated per list element.
            if contains_aggregate(&body) {
                return Err(BindError::Invalid(
                    "Can't use aggregating expressions inside of expressions executing over lists"
                        .to_string(),
                ));
            }
            LogicalExpr::Lambda(LambdaExpr {
                params: renamed,
                body,
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
        // Graph patterns in expressions are hoisted before conversion
        // (`hoist_subqueries`); what is left is not a subquery expression.
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

/// An aggregate's argument `DISTINCT e`: the parser reads `DISTINCT` as a
/// prefix operator of the first operand (`DISTINCT a > 1` as `(DISTINCT a) >
/// 1`); it applies to the whole argument.
fn hoist_distinct(arg: LogicalExpr) -> LogicalExpr {
    fn take(e: &mut LogicalExpr) -> bool {
        match e {
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                if op.operator == crate::query_planner::logical_expr::Operator::Distinct
                    && op.operands.len() == 1
                {
                    *e = op.operands.remove(0);
                    return true;
                }
                op.operands.first_mut().is_some_and(take)
            }
            _ => false,
        }
    }
    let distinct = |e: LogicalExpr| {
        LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: crate::query_planner::logical_expr::Operator::Distinct,
            operands: vec![e],
        })
    };
    match arg {
        LogicalExpr::Operator(ref op) | LogicalExpr::OperatorApplicationExp(ref op)
            if op.operator == crate::query_planner::logical_expr::Operator::Distinct =>
        {
            arg
        }
        mut other => {
            if take(&mut other) {
                distinct(other)
            } else {
                other
            }
        }
    }
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

/// Does the (bound) expression read the properties or labels of the
/// binding `name` (generated name): `name.p`, `labels(name)`,
/// `properties(name)`, `keys(name)`, `type(name)` — not its identity
/// (`id`, `count`, `IS NULL`, the bare variable)?
/// The tie of a value used as a node to the clause's node there
/// (`binder::PatternBuilder::ties`): `NODE_TIE(node, value)`, the node's
/// identity equal to the node the value holds.
pub(crate) const NODE_TIE: &str = "__cg_node_tie";

pub(crate) fn reads_element(expr: &LogicalExpr, name: &str) -> bool {
    let is_name =
        |e: &LogicalExpr| matches!(e, LogicalExpr::TableAlias(TableAlias(n)) if n == name);
    let mut found = false;
    visit(expr, &mut |e| match e {
        LogicalExpr::PropertyAccessExp(PropertyAccess { table_alias, .. }) => {
            found |= table_alias.0 == name;
        }
        LogicalExpr::ScalarFnCall(f) => {
            let reads = ["labels", "properties", "keys", "type"]
                .iter()
                .any(|r| f.name.eq_ignore_ascii_case(r));
            found |= reads && f.args.iter().any(is_name);
        }
        _ => {}
    });
    found
}

/// Does the (bound) expression call one of the functions `names`
/// (lowercase) with no argument?
pub(crate) fn calls_nullary(expr: &LogicalExpr, names: &[&str]) -> bool {
    let mut found = false;
    visit(expr, &mut |e| {
        if let LogicalExpr::ScalarFnCall(f) = e {
            found |= f.args.is_empty() && names.contains(&f.name.to_ascii_lowercase().as_str());
        }
    });
    found
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

/// Every `binding.property` an expression reads: (generated name, property).
pub(crate) fn property_refs(expr: &LogicalExpr) -> Vec<(String, String)> {
    let mut out = Vec::new();
    visit(expr, &mut |e| {
        if let LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias,
            column: PropertyValue::Column(prop),
        }) = e
        {
            out.push((table_alias.0.clone(), prop.clone()));
        }
    });
    out
}

/// True if the expression calls the aggregate `name` (case-insensitive).
pub(crate) fn calls_aggregate(expr: &LogicalExpr, name: &str) -> bool {
    let mut found = false;
    visit(expr, &mut |e| {
        if let LogicalExpr::AggregateFnCall(f) = e {
            found |= f.name.eq_ignore_ascii_case(name);
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

pub(crate) fn children(e: &LogicalExpr) -> Vec<&LogicalExpr> {
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

/// Bind an edge `constraints:` expression of the schema (a condition every
/// relationship of the definition satisfies, over its two nodes): `from` is
/// the node the relationship leaves as stored, `to` the node it enters. Any
/// other variable is undefined; a subquery or a local variable (a
/// comprehension, `reduce`) is `Unsupported`.
pub(crate) fn bind_edge_constraint(
    text: &str,
    from: VarId,
    to: VarId,
) -> Result<LogicalExpr, BindError> {
    struct Ends {
        from: VarId,
        to: VarId,
        local: bool,
    }
    impl NameEnv for Ends {
        fn lookup(&self, name: &str) -> Option<VarId> {
            match name {
                "from" => Some(self.from),
                "to" => Some(self.to),
                _ => None,
            }
        }
        fn new_local(&mut self, _: &str) -> VarId {
            // Refused below: nothing reads the variable.
            self.local = true;
            self.from
        }
        fn subquery(&mut self, _: RawSubquery<'_>) -> Result<VarId, BindError> {
            Err(BindError::Unsupported(
                "an edge `constraints:` expression with a pattern".to_string(),
            ))
        }
        fn correlation(&self, _: VarId) -> Option<Vec<VarId>> {
            None
        }
    }
    let unsupported =
        |why: String| BindError::Unsupported(format!("edge constraint `{text}`: {why}"));
    let (rest, ast) = crate::open_cypher_parser::expression::parse_expression(text)
        .map_err(|e| unsupported(format!("{e:?}")))?;
    if !rest.trim().is_empty() {
        return Err(unsupported(format!("unparsed `{}`", rest.trim())));
    }
    let mut env = Ends {
        from,
        to,
        local: false,
    };
    let bound = bind_expr(&ast, &mut env)?;
    if env.local {
        return Err(unsupported("a local variable".to_string()));
    }
    Ok(bound)
}
