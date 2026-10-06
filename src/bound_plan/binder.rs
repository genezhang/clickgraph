//! The binder (P-4c S3, `docs/design/EXPLICIT_SCOPE.md` §4.4): walks a
//! clause-list statement once, keeping the current scope, and resolves every
//! variable name against it exactly once.
//!
//! Rules (each checked against Neo4j 5.26; tests in `tests.rs`):
//! * MATCH / OPTIONAL MATCH: a pattern variable already in scope refers to that
//!   binding (an identity tie); otherwise it is new. Anonymous elements get
//!   their own bindings. OPTIONAL MATCH bindings are nullable. The clause's
//!   WHERE sees the scope plus the clause's variables.
//! * UNWIND adds one binding; a name already in scope is an error.
//! * WITH / RETURN: items are bound in the input scope; the output scope is
//!   exactly the items (new bindings). The projection's own ORDER BY and WHERE
//!   see the items' names, and also the input scope unless the projection
//!   aggregates or is DISTINCT; then an input-scope expression is allowed only
//!   where it equals a projected expression, or is a property of a projected
//!   variable (and is rewritten to the item). An ORDER BY expression equal to
//!   a projected expression means that item even where an item's name shadows
//!   the variable it reads (`RETURN a.age AS a ORDER BY a.age`).
//! * An aggregating item may use non-aggregated variables only inside
//!   expressions that are grouping keys, or as properties of a projected
//!   variable.
//! * A free-standing ORDER BY / SKIP / LIMIT sees only the current scope.
//! * UNION arms are bound independently; their column names must agree.
//! * An aggregate is allowed only in a projection item, and in the ORDER BY /
//!   WHERE of an aggregating projection where it equals a projected item.
//! * `*` expands to the variables in scope in name order, minus the names
//!   projected explicitly; the explicit items follow.
//!
//! Not bound yet (`BindError::Unsupported`, the caller falls back): CALL,
//! updating clauses, graph patterns inside expressions, property-map
//! parameters, re-matching a bound variable-length relationship list, the
//! same relationship variable twice in one MATCH, and a value-typed variable
//! (list element, `coalesce(a, b)`) used as a node or relationship.

use std::collections::{BTreeSet, HashMap};

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast::{
    Direction, Expression, NodePattern, OrerByOrder, PathPattern, Property, RelationshipPattern,
    UnionType,
};
use crate::open_cypher_parser::clause_list::{Clause, ClauseQuery, ClauseStatement};
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};

use super::expr::{
    bind_expr, contains_aggregate, referenced_names, rename_refs, replace_subtrees, NameEnv,
};
use super::labels::{infer, Feasibility, NodeSlot, RelSlot};
use super::types::*;

/// Bind a parsed statement against a schema.
pub fn bind_statement(
    stmt: &ClauseStatement<'_>,
    schema: &GraphSchema,
) -> Result<BoundStatement, BindError> {
    let mut b = Binder {
        bindings: Vec::new(),
        feasibility: Feasibility::from_schema(schema),
    };
    let (first_plan, first_cols) = b.bind_query(&stmt.first)?;
    if stmt.unions.is_empty() {
        return Ok(BoundStatement {
            plan: first_plan,
            bindings: b.bindings,
            columns: first_cols,
        });
    }
    let all = matches!(stmt.unions[0].0, UnionType::All);
    if stmt
        .unions
        .iter()
        .any(|(t, _)| matches!(t, UnionType::All) != all)
    {
        return Err(BindError::Invalid(
            "Invalid combination of UNION and UNION ALL".to_string(),
        ));
    }
    let names: Vec<String> = first_cols.iter().map(|(n, _)| n.clone()).collect();
    let mut arms = vec![first_plan];
    let mut arm_columns = vec![first_cols.iter().map(|(_, v)| *v).collect::<Vec<_>>()];
    for (_, q) in &stmt.unions {
        let (plan, cols) = b.bind_query(q)?;
        // Matched by name, in the first arm's order.
        let mut by_name: HashMap<&str, VarId> = HashMap::new();
        for (n, v) in &cols {
            by_name.insert(n.as_str(), *v);
        }
        let aligned: Option<Vec<VarId>> = names
            .iter()
            .map(|n| by_name.get(n.as_str()).copied())
            .collect();
        match aligned {
            Some(vars) if cols.len() == names.len() => arm_columns.push(vars),
            _ => {
                return Err(BindError::Invalid(
                    "All sub queries in an UNION must have the same return column names"
                        .to_string(),
                ))
            }
        }
        arms.push(plan);
    }
    let columns = names
        .into_iter()
        .map(|n| {
            let v = b.new_binding(Some(&n), BindingKind::Value, false, BindingSource::Local);
            (n, v)
        })
        .collect();
    Ok(BoundStatement {
        plan: BoundOp::Union {
            arms,
            arm_columns,
            all,
        },
        bindings: b.bindings,
        columns,
    })
}

struct Binder {
    bindings: Vec<Binding>,
    feasibility: Feasibility,
}

/// Name resolution for one expression: the scope, then the binder's local
/// variables.
struct Env<'b> {
    binder: &'b mut Binder,
    scopes: &'b [&'b Scope],
}

impl NameEnv for Env<'_> {
    fn lookup(&self, name: &str) -> Option<VarId> {
        self.scopes.iter().find_map(|s| s.lookup(name))
    }
    fn new_local(&mut self, name: &str) -> VarId {
        self.binder
            .new_binding(Some(name), BindingKind::Value, false, BindingSource::Local)
    }
}

impl Binder {
    fn new_binding(
        &mut self,
        name: Option<&str>,
        kind: BindingKind,
        nullable: bool,
        source: BindingSource,
    ) -> VarId {
        let id = VarId(self.bindings.len() as u32);
        self.bindings.push(Binding {
            id,
            name: name.map(str::to_string),
            kind,
            nullable,
            source,
        });
        id
    }

    fn binding(&self, v: VarId) -> &Binding {
        &self.bindings[v.0 as usize]
    }

    /// Bind an expression against `scopes` (searched in order).
    fn expr(&mut self, e: &Expression<'_>, scopes: &[&Scope]) -> Result<LogicalExpr, BindError> {
        let mut env = Env {
            binder: self,
            scopes,
        };
        bind_expr(e, &mut env)
    }

    fn bind_query(
        &mut self,
        q: &ClauseQuery<'_>,
    ) -> Result<(BoundOp, Vec<(String, VarId)>), BindError> {
        let mut scope = Scope::default();
        let mut op = BoundOp::Unit;
        for (idx, clause) in q.clauses.iter().enumerate() {
            match clause {
                Clause::Match(m) => {
                    let parts: Vec<(Option<&str>, &PathPattern<'_>)> =
                        m.path_patterns.iter().map(|(v, p)| (*v, p)).collect();
                    let where_ = m.where_clause.as_ref().map(|w| &w.conditions);
                    (op, scope) = self.bind_match(op, scope, &parts, where_, false, idx)?;
                }
                Clause::OptionalMatch(o) => {
                    let parts: Vec<(Option<&str>, &PathPattern<'_>)> =
                        o.path_patterns.iter().map(|p| (None, p)).collect();
                    let where_ = o.where_clause.as_ref().map(|w| &w.conditions);
                    (op, scope) = self.bind_match(op, scope, &parts, where_, true, idx)?;
                }
                Clause::Unwind(u) => {
                    let expr = self.expr(&u.expression, &[&scope])?;
                    no_aggregate(
                        &expr,
                        "Can't use aggregating expressions inside of expressions executing \
                         over lists",
                    )?;
                    if scope.lookup(u.alias).is_some() {
                        return Err(BindError::AlreadyDeclared(u.alias.to_string()));
                    }
                    let var = self.new_binding(
                        Some(u.alias),
                        BindingKind::Value,
                        false,
                        BindingSource::Unwind { clause: idx },
                    );
                    scope.insert(u.alias, var);
                    op = BoundOp::Unwind {
                        input: Box::new(op),
                        expr,
                        var,
                    };
                }
                Clause::With(w) => {
                    let items: Vec<RawItem<'_, '_>> = w
                        .items
                        .iter()
                        .map(|i| RawItem {
                            expr: &i.expression,
                            alias: i.alias,
                            text: None,
                        })
                        .collect();
                    let raw = RawProjection {
                        kind: ProjectionKind::With,
                        distinct: w.distinct,
                        star: w.is_star,
                        items,
                        order_by: w
                            .order_by
                            .as_ref()
                            .map(|o| o.order_by_items.iter().collect())
                            .unwrap_or_default(),
                        skip: w.skip.as_ref().map(|s| s.skip_item),
                        limit: w.limit.as_ref().map(|l| l.limit_item),
                        filter: w.where_clause.as_ref().map(|wc| &wc.conditions),
                    };
                    let (projection, out_scope) = self.bind_projection(&raw, &scope, idx)?;
                    op = BoundOp::Project {
                        input: Box::new(op),
                        projection,
                    };
                    scope = out_scope;
                }
                Clause::Return(r) => {
                    let is_star = |i: &&crate::open_cypher_parser::ast::ReturnItem<'_>| {
                        matches!(i.expression, Expression::Variable("*"))
                    };
                    let star = r.clause.return_items.iter().any(|i| is_star(&i));
                    if r.clause.return_items.iter().skip(1).any(|i| is_star(&i)) {
                        return Err(BindError::Invalid(
                            "Invalid input '*': `*` must be the first RETURN item".to_string(),
                        ));
                    }
                    let items: Vec<RawItem<'_, '_>> = r
                        .clause
                        .return_items
                        .iter()
                        .filter(|i| !matches!(i.expression, Expression::Variable("*")))
                        .map(|i| RawItem {
                            expr: &i.expression,
                            alias: i.alias,
                            text: i.original_text,
                        })
                        .collect();
                    let raw = RawProjection {
                        kind: ProjectionKind::Return,
                        distinct: r.clause.distinct,
                        star,
                        items,
                        order_by: r
                            .order_by
                            .as_ref()
                            .map(|o| o.order_by_items.iter().collect())
                            .unwrap_or_default(),
                        skip: r.skip.as_ref().map(|s| s.skip_item),
                        limit: r.limit.as_ref().map(|l| l.limit_item),
                        filter: None,
                    };
                    let (projection, out_scope) = self.bind_projection(&raw, &scope, idx)?;
                    let columns = projection
                        .items
                        .iter()
                        .map(|i| (i.name.clone(), i.var))
                        .collect();
                    let _ = out_scope;
                    return Ok((
                        BoundOp::Project {
                            input: Box::new(op),
                            projection,
                        },
                        columns,
                    ));
                }
                Clause::OrderBy(o) => {
                    let keys = o
                        .order_by_items
                        .iter()
                        .map(|k| {
                            let expr = self.expr(&k.expression, &[&scope])?;
                            not_star(&expr)?;
                            no_aggregate(
                                &expr,
                                "Cannot use aggregation in ORDER BY if there are no aggregate \
                                 expressions in the preceding RETURN",
                            )?;
                            Ok(SortKey {
                                expr,
                                descending: matches!(k.order, OrerByOrder::Desc),
                            })
                        })
                        .collect::<Result<_, BindError>>()?;
                    op = BoundOp::Sort {
                        input: Box::new(op),
                        keys,
                    };
                }
                Clause::Skip(s) => {
                    op = BoundOp::Skip {
                        input: Box::new(op),
                        count: s.skip_item,
                    }
                }
                Clause::Limit(l) => {
                    op = BoundOp::Limit {
                        input: Box::new(op),
                        count: l.limit_item,
                    }
                }
                Clause::Call(_) => return Err(BindError::Unsupported("CALL".to_string())),
                Clause::Create(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) => {
                    return Err(BindError::Unsupported("updating clause".to_string()))
                }
            }
        }
        // The clause-list parser guarantees a final RETURN / CALL / update.
        Err(BindError::Unsupported("query without RETURN".to_string()))
    }

    // ---------------------------------------------------------------- MATCH

    #[allow(clippy::too_many_arguments)]
    fn bind_match(
        &mut self,
        input: BoundOp,
        scope: Scope,
        parts: &[(Option<&str>, &PathPattern<'_>)],
        where_: Option<&Expression<'_>>,
        optional: bool,
        clause: usize,
    ) -> Result<(BoundOp, Scope), BindError> {
        let mut pb = PatternBuilder {
            input_scope: &scope,
            clause_vars: HashMap::new(),
            anon_nodes: HashMap::new(),
            introduces: Vec::new(),
            optional,
            clause,
        };
        // Phase 1: allocate every pattern variable (so inline property maps
        // and the WHERE can refer to any of them).
        let mut shaped: Vec<ShapedPart<'_, '_>> = Vec::new();
        for (path_var, pattern) in parts {
            shaped.push(pb.shape(self, *path_var, pattern)?);
        }
        let mut clause_scope = scope.clone();
        for v in &pb.introduces {
            if let Some(name) = self.binding(*v).name.clone() {
                if self.binding(*v).source != BindingSource::Local {
                    clause_scope.insert(&name, *v);
                }
            }
        }
        // Phase 2: inline property maps.
        let mut bound_parts = Vec::new();
        for sp in shaped {
            bound_parts.push(sp.bind_props(self, &clause_scope)?);
        }
        // Label inference over the whole clause.
        self.infer_labels(&mut bound_parts, &pb.introduces);
        let predicate = where_.map(|w| self.expr(w, &[&clause_scope])).transpose()?;
        if let Some(p) = &predicate {
            no_aggregate(p, AGGREGATE_MISPLACED)?;
        }
        let pattern = BoundPattern { parts: bound_parts };
        Ok((
            BoundOp::Match {
                input: Box::new(input),
                optional,
                pattern,
                predicate,
                introduces: pb.introduces.clone(),
            },
            clause_scope,
        ))
    }

    fn infer_labels(&mut self, parts: &mut [PatternPart], introduces: &[VarId]) {
        // Slots: one per distinct node variable, one per relationship.
        let mut node_index: HashMap<VarId, usize> = HashMap::new();
        let mut nodes: Vec<NodeSlot> = Vec::new();
        let mut node_vars: Vec<VarId> = Vec::new();
        let mut rels: Vec<RelSlot> = Vec::new();
        let mut rel_vars: Vec<VarId> = Vec::new();
        let f = &self.feasibility;
        for part in parts.iter() {
            for n in &part.nodes {
                let fixed = !introduces.contains(&n.var);
                let idx = *node_index.entry(n.var).or_insert_with(|| {
                    let labels = match &self.bindings[n.var.0 as usize].kind {
                        BindingKind::Node { labels } if fixed => labels.clone(),
                        _ => f.all_labels.clone(),
                    };
                    nodes.push(NodeSlot { labels, fixed });
                    node_vars.push(n.var);
                    nodes.len() - 1
                });
                // Written labels are alternatives; several occurrences of one
                // variable intersect. A label the schema lacks matches nothing.
                // A variable bound before the clause is narrowed here only for
                // this clause's inference; its binding keeps its labels.
                if !n.labels.is_empty() {
                    let written: BTreeSet<String> = n.labels.iter().cloned().collect();
                    let slot = &mut nodes[idx];
                    slot.labels = slot.labels.intersection(&written).cloned().collect();
                }
            }
        }
        for part in parts.iter() {
            for (i, r) in part.rels.iter().enumerate() {
                // A relationship bound before the clause starts from its bound
                // types (its written types, if any, narrow this clause only).
                let start: BTreeSet<String> = match &self.bindings[r.var.0 as usize].kind {
                    BindingKind::Rel { types, .. } if r.bound_before => types.clone(),
                    _ => f.all_types.clone(),
                };
                let types: BTreeSet<String> = if r.types.is_empty() {
                    start
                } else {
                    r.types
                        .iter()
                        .filter(|t| start.contains(*t))
                        .cloned()
                        .collect()
                };
                rels.push(RelSlot {
                    left: node_index[&part.nodes[i].var],
                    right: node_index[&part.nodes[i + 1].var],
                    direction: r.direction,
                    length: r.length,
                    types,
                    fixed: r.bound_before,
                });
                rel_vars.push(r.var);
            }
        }
        infer(f, &mut nodes, &mut rels);
        for (slot, var) in nodes.iter().zip(&node_vars) {
            if introduces.contains(var) {
                self.bindings[var.0 as usize].kind = BindingKind::Node {
                    labels: slot.labels.clone(),
                };
            }
        }
        for (slot, var) in rels.iter().zip(&rel_vars) {
            if introduces.contains(var) {
                if let BindingKind::Rel { types, .. } = &mut self.bindings[var.0 as usize].kind {
                    *types = slot.types.clone();
                }
            }
        }
    }

    // ----------------------------------------------------------- PROJECTION

    fn bind_projection(
        &mut self,
        raw: &RawProjection<'_, '_>,
        input: &Scope,
        clause: usize,
    ) -> Result<(Projection, Scope), BindError> {
        let what = match raw.kind {
            ProjectionKind::With => "WITH",
            ProjectionKind::Return => "RETURN",
        };
        // Explicit items, in the input scope.
        let mut explicit: Vec<ProjItem> = Vec::new();
        for it in &raw.items {
            let name = match (it.alias, it.expr) {
                (Some(a), _) => a.to_string(),
                (None, Expression::Variable(v)) => v.to_string(),
                (None, _) => match (raw.kind, it.text) {
                    (ProjectionKind::Return, Some(t)) => t.to_string(),
                    _ => return Err(BindError::MissingAlias(what)),
                },
            };
            if explicit.iter().any(|i| i.name == name) {
                return Err(BindError::DuplicateColumn(name));
            }
            let expr = self.expr(it.expr, &[input])?;
            let aggregate = contains_aggregate(&expr);
            explicit.push(ProjItem {
                var: VarId(u32::MAX), // assigned below
                name,
                expr,
                aggregate,
            });
        }
        // `*` (Neo4j 5.26): the variables in scope in name order, except the
        // names projected explicitly; the explicit items follow.
        let mut items: Vec<ProjItem> = Vec::new();
        if raw.star {
            let mut entries: Vec<&(String, VarId)> = input.entries().iter().collect();
            if entries.is_empty() {
                return Err(BindError::Invalid(format!(
                    "{what} * is not allowed when there are no variables in scope"
                )));
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, var) in entries {
                if explicit.iter().any(|i| &i.name == name) {
                    continue;
                }
                items.push(ProjItem {
                    var: *var,
                    name: name.clone(),
                    expr: LogicalExpr::TableAlias(TableAlias(var.name())),
                    aggregate: false,
                });
            }
        }
        items.extend(explicit);
        let aggregating = items.iter().any(|i| i.aggregate);
        // Bare variables projected without aggregation (grouping keys when
        // aggregating), by generated name -> the item.
        let bare_items: Vec<(String, usize)> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.aggregate)
            .filter_map(|(idx, i)| match &i.expr {
                LogicalExpr::TableAlias(TableAlias(n)) => Some((n.clone(), idx)),
                _ => None,
            })
            .collect();
        // Implicit grouping: outside aggregate calls, an aggregating item may
        // only use grouping-key expressions, properties of a grouping-key
        // variable, and its own local variables.
        if aggregating {
            let keys: Vec<(LogicalExpr, LogicalExpr)> = items
                .iter()
                .filter(|i| !i.aggregate)
                .map(|i| {
                    (
                        i.expr.clone(),
                        LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::Null),
                    )
                })
                .collect();
            for it in items.iter().filter(|i| i.aggregate) {
                let without_keys = replace_subtrees(it.expr.clone(), &keys);
                if let Some(v) = referenced_names(&without_keys, true)
                    .into_iter()
                    .find(|n| !self.is_local(n) && !bare_items.iter().any(|(k, _)| k == n))
                {
                    return Err(BindError::Invalid(format!(
                        "Aggregation column contains implicit grouping expressions: `{}` is \
                         neither aggregated nor a grouping key",
                        self.display_name(&v)
                    )));
                }
            }
        }
        // Output bindings: a bare variable passes its binding's kind through.
        let mut out = Scope::default();
        for it in items.iter_mut() {
            let (kind, nullable, of) = match &it.expr {
                LogicalExpr::TableAlias(TableAlias(n)) if !it.aggregate => {
                    let v = parse_var(n).expect("bound name");
                    let b = self.binding(v);
                    (b.kind.clone(), b.nullable, Some(v))
                }
                _ => (BindingKind::Value, false, None),
            };
            it.var = self.new_binding(
                Some(&it.name),
                kind,
                nullable,
                BindingSource::Projection { clause, of },
            );
            out.insert(&it.name, it.var);
        }
        // ORDER BY and WHERE: projected names, then (without aggregation or
        // DISTINCT) the input scope. With aggregation or DISTINCT, an input
        // expression is allowed only where it equals a projected one (and a
        // grouping-key variable's properties); it is rewritten to the item.
        let restricted = aggregating || raw.distinct;
        let projected: Vec<(LogicalExpr, LogicalExpr)> = items
            .iter()
            .map(|i| {
                (
                    i.expr.clone(),
                    LogicalExpr::TableAlias(TableAlias(i.var.name())),
                )
            })
            .collect();
        let key_vars: HashMap<String, String> = bare_items
            .iter()
            .map(|(n, idx)| (n.clone(), items[*idx].var.name()))
            .collect();
        let item_vars: BTreeSet<String> = items.iter().map(|i| i.var.name()).collect();
        let out_names: BTreeSet<&str> = items.iter().map(|i| i.name.as_str()).collect();
        let order_by_misplaced = if aggregating {
            "Illegal aggregation expression(s) in order by"
        } else {
            "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the \
             preceding RETURN"
        };
        let bind_modifier = |me: &mut Self,
                             e: &Expression<'_>,
                             is_order_by: bool|
         -> Result<LogicalExpr, BindError> {
            let mut bound = me.expr(e, &[&out, input])?;
            if is_order_by {
                // Neo4j resolves an ORDER BY expression that equals a
                // projected expression to that item before resolving names:
                // `RETURN a.age AS a ORDER BY a.age` sorts by the item. Taken
                // only when nothing left refers to an input variable whose
                // name an item now shadows.
                if let Ok(in_input) = me.expr(e, &[input]) {
                    let rewritten = replace_subtrees(in_input, &projected);
                    let shadowed = referenced_names(&rewritten, false).into_iter().any(|n| {
                        !item_vars.contains(&n)
                            && !me.is_local(&n)
                            && out_names.contains(me.display_name(&n).as_str())
                    });
                    if !shadowed {
                        bound = rewritten;
                    }
                }
            }
            let rewritten = rename_refs(replace_subtrees(bound.clone(), &projected), &key_vars);
            if contains_aggregate(&rewritten) {
                return Err(BindError::Invalid(
                    if is_order_by {
                        order_by_misplaced
                    } else {
                        AGGREGATE_MISPLACED
                    }
                    .to_string(),
                ));
            }
            if !restricted {
                return Ok(bound);
            }
            if let Some(v) = referenced_names(&rewritten, false)
                .into_iter()
                .find(|n| !item_vars.contains(n) && !me.is_local(n))
            {
                return Err(BindError::Invalid(format!(
                    "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to \
                     access variables declared before the WITH/RETURN: {}",
                    me.display_name(&v)
                )));
            }
            Ok(rewritten)
        };
        let order_by = raw
            .order_by
            .iter()
            .map(|k| {
                let expr = bind_modifier(self, &k.expression, true)?;
                not_star(&expr)?;
                Ok(SortKey {
                    expr,
                    descending: matches!(k.order, OrerByOrder::Desc),
                })
            })
            .collect::<Result<Vec<_>, BindError>>()?;
        let filter = raw
            .filter
            .map(|f| bind_modifier(self, f, false))
            .transpose()?;
        Ok((
            Projection {
                kind: raw.kind,
                distinct: raw.distinct,
                items,
                order_by,
                skip: raw.skip,
                limit: raw.limit,
                filter,
            },
            out,
        ))
    }

    /// Is `generated` a comprehension / reduce / lambda variable?
    fn is_local(&self, generated: &str) -> bool {
        parse_var(generated)
            .and_then(|v| self.bindings.get(v.0 as usize))
            .is_some_and(|b| b.source == BindingSource::Local)
    }

    /// The user's name for a generated name (for error messages).
    fn display_name(&self, generated: &str) -> String {
        parse_var(generated)
            .and_then(|v| self.bindings.get(v.0 as usize))
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| generated.to_string())
    }
}

/// Neo4j's message for an aggregate outside a projection item.
const AGGREGATE_MISPLACED: &str = "Aggregations should not be used like this.";

fn no_aggregate(e: &LogicalExpr, message: &str) -> Result<(), BindError> {
    if contains_aggregate(e) {
        return Err(BindError::Invalid(message.to_string()));
    }
    Ok(())
}

/// `ORDER BY *` is not a sort key.
fn not_star(e: &LogicalExpr) -> Result<(), BindError> {
    if matches!(e, LogicalExpr::Star) {
        return Err(BindError::Invalid(
            "Invalid input '*' in ORDER BY".to_string(),
        ));
    }
    Ok(())
}

fn parse_var(generated: &str) -> Option<VarId> {
    generated.strip_prefix('v')?.parse().ok().map(VarId)
}

struct RawItem<'q, 'a> {
    expr: &'q Expression<'a>,
    alias: Option<&'a str>,
    /// RETURN's original text, its default column name.
    text: Option<&'a str>,
}

struct RawProjection<'q, 'a> {
    kind: ProjectionKind,
    distinct: bool,
    star: bool,
    items: Vec<RawItem<'q, 'a>>,
    order_by: Vec<&'q crate::open_cypher_parser::ast::OrderByItem<'a>>,
    skip: Option<i64>,
    limit: Option<i64>,
    filter: Option<&'q Expression<'a>>,
}

// ------------------------------------------------------------------ patterns

struct PatternBuilder<'s> {
    input_scope: &'s Scope,
    /// Named variables introduced by this clause so far.
    clause_vars: HashMap<String, VarId>,
    /// Anonymous nodes, by their (shared) AST node: consecutive steps of a
    /// path share the node between them.
    anon_nodes: HashMap<NodeKey, VarId>,
    introduces: Vec<VarId>,
    optional: bool,
    clause: usize,
}

/// A pattern node as written: a standalone node, or a node shared by two
/// consecutive steps of a path (behind a `RefCell`).
enum NodeAst<'q, 'a> {
    Plain(&'q NodePattern<'a>),
    Shared(std::rc::Rc<std::cell::RefCell<NodePattern<'a>>>),
}

/// A pattern part whose variables are allocated but whose inline property
/// maps are not bound yet (they may refer to any variable of the clause).
struct ShapedPart<'q, 'a> {
    part: PatternPart,
    node_asts: Vec<NodeAst<'q, 'a>>,
    rel_props: Vec<Option<&'q Vec<Property<'a>>>>,
}

impl ShapedPart<'_, '_> {
    fn bind_props(self, b: &mut Binder, scope: &Scope) -> Result<PatternPart, BindError> {
        let mut part = self.part;
        for (n, ast) in part.nodes.iter_mut().zip(&self.node_asts) {
            n.props = match ast {
                NodeAst::Plain(np) => bind_props(b, np.properties.as_ref(), scope)?,
                NodeAst::Shared(rc) => bind_props(b, rc.borrow().properties.as_ref(), scope)?,
            };
        }
        for (r, props) in part.rels.iter_mut().zip(self.rel_props) {
            r.props = bind_props(b, props, scope)?;
        }
        Ok(part)
    }
}

fn bind_props(
    b: &mut Binder,
    props: Option<&Vec<Property<'_>>>,
    scope: &Scope,
) -> Result<Vec<(String, LogicalExpr)>, BindError> {
    let mut out = Vec::new();
    for p in props.into_iter().flatten() {
        match p {
            Property::PropertyKV(kv) => {
                let value = b.expr(&kv.value, &[scope])?;
                no_aggregate(&value, AGGREGATE_MISPLACED)?;
                out.push((kv.key.to_string(), value))
            }
            Property::Param(_) => {
                return Err(BindError::Unsupported(
                    "a parameter as a property map".to_string(),
                ))
            }
        }
    }
    Ok(out)
}

impl<'s> PatternBuilder<'s> {
    fn shape<'q, 'a>(
        &mut self,
        b: &mut Binder,
        path_var: Option<&str>,
        pattern: &'q PathPattern<'a>,
    ) -> Result<ShapedPart<'q, 'a>, BindError> {
        let (pattern, shortest) = match pattern {
            PathPattern::ShortestPath(inner) => (&**inner, Some(ShortestMode::Shortest)),
            PathPattern::AllShortestPaths(inner) => (&**inner, Some(ShortestMode::AllShortest)),
            p => (p, None),
        };
        let mut part = PatternPart {
            path_var: None,
            shortest,
            nodes: Vec::new(),
            rels: Vec::new(),
        };
        let mut node_asts = Vec::new();
        let mut rel_props = Vec::new();
        match pattern {
            PathPattern::Node(np) => {
                part.nodes.push(self.node(b, np, None)?);
                node_asts.push(NodeAst::Plain(np));
            }
            PathPattern::ConnectedPattern(steps) => {
                for (i, step) in steps.iter().enumerate() {
                    if i == 0 {
                        let node = self.node(
                            b,
                            &step.start_node.borrow(),
                            Some(rc_key(&step.start_node)),
                        )?;
                        part.nodes.push(node);
                        node_asts.push(NodeAst::Shared(step.start_node.clone()));
                    }
                    part.rels.push(self.rel(b, &step.relationship)?);
                    rel_props.push(step.relationship.properties.as_ref());
                    let node =
                        self.node(b, &step.end_node.borrow(), Some(rc_key(&step.end_node)))?;
                    part.nodes.push(node);
                    node_asts.push(NodeAst::Shared(step.end_node.clone()));
                }
            }
            PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
                return Err(BindError::Invalid(
                    "nested shortestPath is not allowed".to_string(),
                ))
            }
        }
        if let Some(pv) = path_var {
            part.path_var = Some(self.path_var(b, pv)?);
        }
        Ok(ShapedPart {
            part,
            node_asts,
            rel_props,
        })
    }

    fn path_var(&mut self, b: &mut Binder, name: &str) -> Result<VarId, BindError> {
        if self.input_scope.lookup(name).is_some() || self.clause_vars.contains_key(name) {
            return Err(BindError::AlreadyDeclared(name.to_string()));
        }
        let v = b.new_binding(
            Some(name),
            BindingKind::Path,
            self.optional,
            BindingSource::Pattern {
                clause: self.clause,
            },
        );
        self.clause_vars.insert(name.to_string(), v);
        self.introduces.push(v);
        Ok(v)
    }

    fn node(
        &mut self,
        b: &mut Binder,
        np: &NodePattern<'_>,
        shared: Option<NodeKey>,
    ) -> Result<PatNode, BindError> {
        let labels: Vec<String> = np.labels.iter().flatten().map(|l| l.to_string()).collect();
        let (var, bound_before) = match np.name {
            Some(name) => self.named(b, name, "node", || BindingKind::Node {
                labels: BTreeSet::new(),
            })?,
            None => match shared.and_then(|k| self.anon_nodes.get(&k).copied()) {
                Some(v) => (v, false),
                None => {
                    let v = b.new_binding(
                        None,
                        BindingKind::Node {
                            labels: BTreeSet::new(),
                        },
                        self.optional,
                        BindingSource::Pattern {
                            clause: self.clause,
                        },
                    );
                    if let Some(k) = shared {
                        self.anon_nodes.insert(k, v);
                    }
                    self.introduces.push(v);
                    (v, false)
                }
            },
        };
        Ok(PatNode {
            var,
            labels,
            props: Vec::new(),
            bound_before,
        })
    }

    fn rel(&mut self, b: &mut Binder, rp: &RelationshipPattern<'_>) -> Result<PatRel, BindError> {
        // Any `*` form binds a LIST of relationships, `[r*1]` included.
        let length = rp
            .variable_length
            .as_ref()
            .map(|vl| (vl.min_hops.unwrap_or(1), vl.max_hops));
        let types: Vec<String> = rp.labels.iter().flatten().map(|t| t.to_string()).collect();
        let direction = match rp.direction {
            Direction::Outgoing => RelDirection::Right,
            Direction::Incoming => RelDirection::Left,
            Direction::Either => RelDirection::Either,
        };
        let (var, bound_before) = match rp.name {
            Some(name) => {
                let (v, before) = self.named(b, name, "relationship", || BindingKind::Rel {
                    types: BTreeSet::new(),
                    length,
                })?;
                if before {
                    let bound_length = match b.binding(v).kind {
                        BindingKind::Rel { length, .. } => length,
                        _ => None,
                    };
                    match (bound_length, length) {
                        (None, None) => {}
                        (Some(_), None) => {
                            return Err(BindError::TypeMismatch {
                                name: name.to_string(),
                                bound: "List<Relationship>",
                                used: "Relationship",
                            })
                        }
                        (None, Some(_)) => {
                            return Err(BindError::TypeMismatch {
                                name: name.to_string(),
                                bound: "Relationship",
                                used: "List<Relationship>",
                            })
                        }
                        (Some(_), Some(_)) => {
                            return Err(BindError::Unsupported(
                                "re-matching a bound variable-length relationship list".to_string(),
                            ))
                        }
                    }
                }
                (v, before)
            }
            None => {
                let v = b.new_binding(
                    None,
                    BindingKind::Rel {
                        types: BTreeSet::new(),
                        length,
                    },
                    self.optional,
                    BindingSource::Pattern {
                        clause: self.clause,
                    },
                );
                self.introduces.push(v);
                (v, false)
            }
        };
        Ok(PatRel {
            var,
            types,
            direction,
            length,
            props: Vec::new(),
            bound_before,
        })
    }

    /// Resolve a named pattern variable: this clause's, the input scope's, or
    /// new. Returns (binding, bound before this clause).
    fn named(
        &mut self,
        b: &mut Binder,
        name: &str,
        used_as: &'static str,
        new_kind: impl FnOnce() -> BindingKind,
    ) -> Result<(VarId, bool), BindError> {
        let check = |b: &Binder, v: VarId| -> Result<(), BindError> {
            let bound = match b.binding(v).kind {
                BindingKind::Node { .. } => "node",
                BindingKind::Rel { .. } => "relationship",
                BindingKind::Path => "path",
                BindingKind::Value => {
                    // A list element or `coalesce(a, b)` may be a node or a
                    // relationship at runtime; Neo4j accepts it.
                    return Err(BindError::Unsupported(format!(
                        "a value-typed variable `{name}` used as a {used_as}"
                    )));
                }
            };
            if bound != used_as {
                return Err(BindError::TypeMismatch {
                    name: name.to_string(),
                    bound,
                    used: used_as,
                });
            }
            Ok(())
        };
        if let Some(v) = self.clause_vars.get(name).copied() {
            check(b, v)?;
            if used_as == "relationship" {
                // Relationship uniqueness makes the clause match nothing
                // (OPTIONAL MATCH: one null row); lowered in S4/S5.
                return Err(BindError::Unsupported(
                    "the same relationship variable twice in one MATCH".to_string(),
                ));
            }
            return Ok((v, false));
        }
        if let Some(v) = self.input_scope.lookup(name) {
            check(b, v)?;
            return Ok((v, true));
        }
        let v = b.new_binding(
            Some(name),
            new_kind(),
            self.optional,
            BindingSource::Pattern {
                clause: self.clause,
            },
        );
        self.clause_vars.insert(name.to_string(), v);
        self.introduces.push(v);
        Ok((v, false))
    }
}

/// Identity of a node pattern the parser shares between two steps of a path
/// (`(a)-[]->()-[]->(c)`: the middle node is one `Rc`), used only as a map key.
type NodeKey = *const ();

fn rc_key(rc: &std::rc::Rc<std::cell::RefCell<NodePattern<'_>>>) -> NodeKey {
    std::rc::Rc::as_ptr(rc).cast()
}
