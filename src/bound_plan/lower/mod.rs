//! Lowering (P-4c S4, `docs/design/EXPLICIT_SCOPE.md` §4.6, §4.10, §4.13,
//! §4.14): a bound plan → a [`RenderPlan`] printed by
//! `render_plan_to_sql_plain`.
//!
//! Every element of a pattern is its own scan, aliased by its binding's
//! generated name (`v{N}`), so no two elements can share an alias and a
//! column reference is always `v{N}.<physical column>`:
//! * a node is a scan of its label's table;
//! * a relationship is a scan of its type's edge table, tied to its endpoint
//!   nodes by `edge.from = from-node.id`, `edge.to = to-node.id` in the
//!   stored orientation;
//! * a variable appearing again (in the same clause or an earlier one) is the
//!   same scan: its appearances are ties, never a second scan.
//!
//! Scans are joined in pattern order; each tie is placed in the ON of the
//! later of its two scans, so the joins are already in dependency order and
//! the printer runs no repair pass. Within one MATCH, relationships of the
//! same table must differ (relationship uniqueness).
//!
//! S4a scope — everything else is [`LowerError::Unsupported`] and the query
//! is translated by the legacy pipeline:
//! * MATCH (not OPTIONAL) over standard-layout labels and types
//!   (`NodeSchema::is_standard_own_table`, `RelationshipSchema::
//!   is_standard_edge_table`), each node with one label and each
//!   relationship with one type after label inference, fixed length,
//!   directed;
//! * a final RETURN of values (no whole nodes or relationships: Bolt and
//!   graph output need the result shape first), with aggregation, DISTINCT,
//!   ORDER BY, SKIP, LIMIT.
//!
//! A node or relationship whose label / type set is empty matches nothing
//! (Cypher returns no rows; it is not an error): the query lowers to a
//! relation with no rows, and its properties read as NULL.

mod expr;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{
    ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::{
    ArrayJoinItem, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join, JoinItems,
    JoinType, LimitItem, OrderByItem, OrderByItems, OrderByOrder, RenderPlan, SelectItem,
    SelectItems, SkipItem, UnionItems, ViewTableRef,
};

use super::types::*;

/// Why a bound plan was not lowered.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LowerError {
    /// Not lowered yet; the legacy pipeline translates the query.
    #[error("not lowered yet: {0}")]
    Unsupported(String),
}

fn unsupported<T>(what: impl Into<String>) -> Result<T, LowerError> {
    Err(LowerError::Unsupported(what.into()))
}

/// Request inputs the lowering needs.
#[derive(Debug, Clone, Default)]
pub struct LowerOptions {
    /// Values for parameterized views (`ReadOptions::view_parameter_values`).
    pub view_parameter_values: Option<HashMap<String, String>>,
    /// Neo4j-compat mode: an undeclared property is NULL on every element,
    /// not only on those whose columns were discovered.
    pub neo4j_compat: bool,
}

/// Lower a bound statement to a render plan.
pub fn lower_statement(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
) -> Result<RenderPlan, LowerError> {
    let mut l = Lowerer {
        schema,
        bindings: &stmt.bindings,
        options,
        scans: HashMap::new(),
        emitted: Vec::new(),
        from: None,
        joins: Vec::new(),
        pending: Vec::new(),
        filters: Vec::new(),
        empty: false,
    };
    let BoundOp::Project { input, projection } = &stmt.plan else {
        return unsupported("a statement that does not end in RETURN (UNION: S7)");
    };
    if projection.kind != ProjectionKind::Return {
        return unsupported("WITH (S4b)");
    }
    l.relation(input)?;
    l.finish_relation();
    l.project(projection)
}

/// How a pattern element is read.
#[derive(Debug, Clone)]
enum Scan<'s> {
    Node {
        schema: &'s NodeSchema,
        label: String,
    },
    Rel {
        schema: &'s RelationshipSchema,
        rel_type: String,
    },
    /// An element whose label / type set is empty: it matches nothing.
    Impossible,
}

/// An equality between columns of two scans, placed in the ON of whichever
/// is emitted later (or in WHERE if both already are).
struct Tie {
    a: VarId,
    b: VarId,
    eqs: Vec<(RenderExpr, RenderExpr)>,
}

struct Lowerer<'s> {
    schema: &'s GraphSchema,
    bindings: &'s [Binding],
    options: &'s LowerOptions,
    scans: HashMap<VarId, Scan<'s>>,
    /// Scans in join order.
    emitted: Vec<VarId>,
    from: Option<ViewTableRef>,
    joins: Vec<Join>,
    pending: Vec<Tie>,
    filters: Vec<RenderExpr>,
    /// Some element matches nothing: the relation has no rows.
    empty: bool,
}

pub(crate) fn col(alias: VarId, column: &str) -> RenderExpr {
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(alias.name()),
        column: PropertyValue::Column(column.to_string()),
    })
}

fn eq(a: RenderExpr, b: RenderExpr) -> OperatorApplication {
    OperatorApplication {
        operator: Operator::Equal,
        operands: vec![a, b],
    }
}

impl<'s> Lowerer<'s> {
    fn binding(&self, v: VarId) -> &'s Binding {
        &self.bindings[v.0 as usize]
    }

    // ------------------------------------------------------------- relation

    fn relation(&mut self, op: &BoundOp) -> Result<(), LowerError> {
        match op {
            BoundOp::Unit => Ok(()),
            BoundOp::Match {
                input,
                optional,
                pattern,
                predicate,
                introduces: _,
            } => {
                if *optional {
                    return unsupported("OPTIONAL MATCH (S5)");
                }
                self.relation(input)?;
                self.lower_match(pattern)?;
                if let Some(p) = predicate {
                    let e = self.expr(p, &HashMap::new())?;
                    self.filters.push(e);
                }
                Ok(())
            }
            BoundOp::Project { .. } => unsupported("WITH (S4b)"),
            BoundOp::Unwind { .. } => unsupported("UNWIND (S7)"),
            BoundOp::Sort { .. } | BoundOp::Skip { .. } | BoundOp::Limit { .. } => {
                unsupported("a free-standing ORDER BY / SKIP / LIMIT (S4b)")
            }
            BoundOp::Union { .. } => unsupported("UNION (S7)"),
        }
    }

    fn lower_match(&mut self, pattern: &BoundPattern) -> Result<(), LowerError> {
        // Decide every element's scan first (a relationship's table depends
        // on its endpoints' labels), then join them in path order: node,
        // relationship, node, … so each scan joins on the one before it.
        let mut clause_rels: Vec<VarId> = Vec::new();
        for part in &pattern.parts {
            if part.path_var.is_some() || part.shortest.is_some() {
                return unsupported("a path variable or shortestPath (S6)");
            }
            for n in &part.nodes {
                self.node_scan(n.var)?;
            }
            for (i, r) in part.rels.iter().enumerate() {
                self.rel_scan(r, part.nodes[i].var, part.nodes[i + 1].var)?;
                clause_rels.push(r.var);
            }
        }
        for part in &pattern.parts {
            self.emit(part.nodes[0].var)?;
            for (i, r) in part.rels.iter().enumerate() {
                self.emit(r.var)?;
                self.emit(part.nodes[i + 1].var)?;
            }
        }
        for part in &pattern.parts {
            for n in &part.nodes {
                self.written(n.var, &n.labels);
                self.props(n.var, &n.props)?;
            }
            for r in &part.rels {
                self.written(r.var, &r.types);
                self.props(r.var, &r.props)?;
            }
        }
        self.uniqueness(&clause_rels)
    }

    /// Decide how a node is read (once per variable).
    fn node_scan(&mut self, v: VarId) -> Result<(), LowerError> {
        if self.scans.contains_key(&v) {
            return Ok(());
        }
        let BindingKind::Node { labels } = &self.binding(v).kind else {
            return unsupported("a pattern node that is not a node binding");
        };
        let scan = match labels.len() {
            0 => Scan::Impossible,
            1 => {
                let label = labels.iter().next().expect("one label").clone();
                let Some(ns) = self.schema.node_schema_opt(&label) else {
                    return unsupported(format!("label {label} has no node schema"));
                };
                if !ns.is_standard_own_table() {
                    return unsupported(format!("label {label} is not the standard layout (S8)"));
                }
                Scan::Node { schema: ns, label }
            }
            _ => return unsupported("a node with several possible labels (S7)"),
        };
        self.scans.insert(v, scan);
        Ok(())
    }

    /// Decide how a relationship is read (once per variable) and tie it to
    /// its endpoints in the stored orientation.
    fn rel_scan(&mut self, r: &PatRel, left: VarId, right: VarId) -> Result<(), LowerError> {
        if r.length.is_some() {
            return unsupported("a variable-length relationship (S6)");
        }
        let (from, to) = match r.direction {
            RelDirection::Right => (left, right),
            RelDirection::Left => (right, left),
            RelDirection::Either => return unsupported("an undirected relationship (S7)"),
        };
        if !self.scans.contains_key(&r.var) {
            let BindingKind::Rel { types, .. } = &self.binding(r.var).kind else {
                return unsupported("a pattern relationship that is not a relationship binding");
            };
            let scan = match (types.len(), self.single_label(from), self.single_label(to)) {
                (1, Some(fl), Some(tl)) => {
                    let rel_type = types.iter().next().expect("one type").clone();
                    let Ok(rs) =
                        self.schema
                            .get_rel_schema_with_nodes(&rel_type, Some(&fl), Some(&tl))
                    else {
                        return unsupported(format!("no schema for ({fl})-[:{rel_type}]->({tl})"));
                    };
                    if !rs.is_standard_edge_table() {
                        return unsupported(format!(
                            "type {rel_type} is not the standard layout (S8)"
                        ));
                    }
                    if rs.constraints.is_some() {
                        return unsupported("an edge `constraints:` expression (S8)");
                    }
                    Scan::Rel {
                        schema: rs,
                        rel_type,
                    }
                }
                // No feasible type, or an endpoint that matches nothing.
                (0, ..) | (1, ..) => Scan::Impossible,
                _ => return unsupported("a relationship with several possible types (S7)"),
            };
            self.scans.insert(r.var, scan);
        }
        if let Scan::Rel { schema: rs, .. } = self.scans[&r.var].clone() {
            for (end, cols) in [(from, rs.from_id.columns()), (to, rs.to_id.columns())] {
                let Some(ids) = self.identity_columns(end) else {
                    continue; // an endpoint that matches nothing
                };
                if ids.len() != cols.len() {
                    return unsupported("endpoint id arity differs from the edge's");
                }
                let eqs = cols
                    .iter()
                    .zip(&ids)
                    .map(|(c, id)| (col(r.var, c), col(end, id)))
                    .collect();
                self.tie(r.var, end, eqs);
            }
        }
        Ok(())
    }

    /// Written labels / types are alternatives the element must have. Labels
    /// are static (one per scan), so this is decided here: a variable from an
    /// earlier clause written with a label it does not have (`MATCH (a:User)
    /// MATCH (a:Post)`) makes the clause match nothing.
    fn written(&mut self, v: VarId, written: &[String]) {
        if written.is_empty() {
            return;
        }
        let holds = match &self.scans[&v] {
            Scan::Node { label, .. } => written.contains(label),
            Scan::Rel { rel_type, .. } => written.contains(rel_type),
            Scan::Impossible => false,
        };
        if !holds {
            self.empty = true;
        }
    }

    /// Inline property maps: `v.prop = value`.
    fn props(&mut self, v: VarId, props: &[(String, LogicalExpr)]) -> Result<(), LowerError> {
        for (prop, value) in props {
            let lhs = self.property(v, prop)?;
            let rhs = self.expr(value, &HashMap::new())?;
            self.filters
                .push(RenderExpr::OperatorApplicationExp(eq(lhs, rhs)));
        }
        Ok(())
    }

    /// The node's label when it has exactly one (its scan is a table).
    fn single_label(&self, v: VarId) -> Option<String> {
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Some(label.clone()),
            _ => None,
        }
    }

    /// A node's identity columns (physical), or a relationship's: its
    /// `edge_id`, else its stored (from, to) endpoint columns (#887 policy).
    fn identity_columns(&self, v: VarId) -> Option<Vec<String>> {
        match self.scans.get(&v)? {
            Scan::Node { schema, .. } => Some(schema.id_physical_columns()),
            Scan::Rel { schema, .. } => Some(match &schema.edge_id {
                Some(id) => id.columns().iter().map(|c| c.to_string()).collect(),
                None => schema
                    .from_id
                    .columns()
                    .iter()
                    .chain(schema.to_id.columns().iter())
                    .map(|c| c.to_string())
                    .collect(),
            }),
            Scan::Impossible => None,
        }
    }

    /// Relationships of one MATCH are pairwise distinct. Only two scans of
    /// the same edge table can be the same relationship.
    fn uniqueness(&mut self, rels: &[VarId]) -> Result<(), LowerError> {
        for (i, a) in rels.iter().enumerate() {
            for b in &rels[i + 1..] {
                let (Some(Scan::Rel { schema: sa, .. }), Some(Scan::Rel { schema: sb, .. })) =
                    (self.scans.get(a), self.scans.get(b))
                else {
                    continue;
                };
                if sa.full_table_name() != sb.full_table_name() {
                    continue;
                }
                let cols = self.identity_columns(*a).expect("a relationship scan");
                let differs: Vec<RenderExpr> = cols
                    .iter()
                    .map(|c| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::NotEqual,
                            operands: vec![col(*a, c), col(*b, c)],
                        })
                    })
                    .collect();
                self.filters.push(or_all(differs));
            }
        }
        Ok(())
    }

    /// Add a decided scan to the join tree (once per variable).
    fn emit(&mut self, v: VarId) -> Result<(), LowerError> {
        if self.is_emitted(v) {
            return Ok(());
        }
        let table = match &self.scans[&v] {
            Scan::Node { schema, .. } => (
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Rel { schema, .. } => (
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Impossible => {
                self.empty = true;
                return Ok(());
            }
        };
        let (base, params, use_final, filter) = table;
        let name = ViewTableRef::parameterized_name(
            &base,
            params,
            self.options.view_parameter_values.as_ref(),
        );
        if let Some(f) = filter {
            match f.to_sql(&v.name()) {
                Ok(sql) => self.filters.push(RenderExpr::Raw(format!("({sql})"))),
                Err(e) => return unsupported(format!("schema filter: {e}")),
            }
        }
        if self.from.is_none() {
            self.from = Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name,
                alias: Some(v.name()),
                use_final,
            });
        } else {
            if use_final {
                // `Join` prints no FINAL (S8 with the other table options).
                return unsupported("FINAL on a joined table");
            }
            self.joins.push(Join {
                table_name: name,
                table_alias: v.name(),
                joining_on: Vec::new(),
                join_type: JoinType::Join,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
                is_cartesian: false,
            });
        }
        self.emitted.push(v);
        // Ties that this scan completes go in its ON.
        let mut still = Vec::new();
        for t in std::mem::take(&mut self.pending) {
            if (t.a == v || t.b == v) && self.is_emitted(t.a) && self.is_emitted(t.b) {
                self.place(t);
            } else {
                still.push(t);
            }
        }
        self.pending = still;
        Ok(())
    }

    fn is_emitted(&self, v: VarId) -> bool {
        self.emitted.contains(&v)
    }

    fn tie(&mut self, a: VarId, b: VarId, eqs: Vec<(RenderExpr, RenderExpr)>) {
        let t = Tie { a, b, eqs };
        if self.is_emitted(a) && self.is_emitted(b) {
            self.place(t);
        } else {
            self.pending.push(t);
        }
    }

    /// Put a tie whose scans are both emitted in the ON of the later one. Its
    /// ON can see the earlier scan, and for inner joins ON and WHERE mean the
    /// same; a self-tie (`(a)-[r]->(a)` ties `r` to `a` twice) is no special
    /// case. Only the FROM scan has no ON: a tie between it and itself cannot
    /// arise (a scan is never tied to itself).
    fn place(&mut self, t: Tie) {
        let position = |v: VarId| self.emitted.iter().position(|e| *e == v);
        let later = position(t.a).max(position(t.b)).expect("both emitted");
        let eqs = t.eqs.into_iter().map(|(x, y)| eq(x, y));
        if later == 0 {
            self.filters
                .extend(eqs.map(RenderExpr::OperatorApplicationExp));
            return;
        }
        let alias = self.emitted[later].name();
        let join = self
            .joins
            .iter_mut()
            .find(|j| j.table_alias == alias)
            .expect("every emitted scan after the first is a join");
        join.joining_on.extend(eqs);
    }

    fn finish_relation(&mut self) {
        // A tie to an impossible scan never completes; the relation is empty.
        self.pending.clear();
    }

    // ----------------------------------------------------------- projection

    fn project(&mut self, p: &Projection) -> Result<RenderPlan, LowerError> {
        if p.filter.is_some() {
            return unsupported("a projection WHERE (S4b)");
        }
        let mut items_env: HashMap<VarId, RenderExpr> = HashMap::new();
        let mut select = Vec::new();
        let mut group_by: Vec<RenderExpr> = Vec::new();
        let aggregating = p.items.iter().any(|i| i.aggregate);
        for it in &p.items {
            if let LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) =
                &it.expr
            {
                if let Some(v) = parse_var(n) {
                    if !matches!(self.binding(v).kind, BindingKind::Value) {
                        return unsupported(
                            "returning a whole node, relationship or path (needs the result shape)",
                        );
                    }
                }
            }
            let e = self.expr(&it.expr, &HashMap::new())?;
            if aggregating && !it.aggregate {
                group_by.push(e.clone());
            }
            items_env.insert(it.var, e.clone());
            select.push(SelectItem {
                expression: e,
                col_alias: Some(ColumnAlias(it.name.clone())),
            });
        }
        let order_by = p
            .order_by
            .iter()
            .map(|k| {
                Ok(OrderByItem {
                    expression: self.expr(&k.expr, &items_env)?,
                    order: if k.descending {
                        OrderByOrder::Desc
                    } else {
                        OrderByOrder::Asc
                    },
                })
            })
            .collect::<Result<Vec<_>, LowerError>>()?;
        // A relation that matches nothing keeps its scans (expressions may
        // read them) and filters every row out. With no scan at all (every
        // element impossible, or no MATCH: `RETURN 1 + 1`) there is no FROM.
        let mut filters = std::mem::take(&mut self.filters);
        if self.empty {
            filters = vec![RenderExpr::Literal(Literal::Boolean(false))];
        }
        let (from, joins) = (self.from.take(), std::mem::take(&mut self.joins));
        // A constant key orders nothing, and ClickHouse would read an
        // integer one as a column position (`ORDER BY 1`). A constant
        // grouping key forms one group: drop it, but keep the one-group
        // semantics on an empty input (no row, unlike a global aggregate).
        let order_by: Vec<OrderByItem> = order_by
            .into_iter()
            .filter(|o| !is_constant(&o.expression))
            .collect();
        let only_constant_keys = !group_by.is_empty() && group_by.iter().all(is_constant);
        group_by.retain(|g| !is_constant(g));
        let having_clause = only_constant_keys.then(|| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    RenderExpr::AggregateFnCall(crate::render_plan::render_expr::AggregateFnCall {
                        name: "count".to_string(),
                        args: vec![RenderExpr::Star],
                    }),
                    RenderExpr::Literal(Literal::Integer(0)),
                ],
            })
        });
        Ok(RenderPlan {
            ctes: CteItems(Vec::new()),
            select: SelectItems {
                items: select,
                distinct: p.distinct,
            },
            from: FromTableItem(from),
            joins: JoinItems(joins),
            array_join: ArrayJoinItem(Vec::new()),
            filters: FilterItems(and_all(filters)),
            group_by: GroupByExpressions(group_by),
            having_clause,
            order_by: OrderByItems(order_by),
            skip: SkipItem(p.skip),
            limit: LimitItem(p.limit),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        })
    }
}

use crate::query_planner::logical_expr::LogicalExpr;

pub(crate) fn parse_var(generated: &str) -> Option<VarId> {
    generated.strip_prefix('v')?.parse().ok().map(VarId)
}

/// No column, variable or aggregate: the same value on every row.
fn is_constant(e: &RenderExpr) -> bool {
    match e {
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) => true,
        RenderExpr::List(xs) => xs.iter().all(is_constant),
        RenderExpr::MapLiteral(entries) => entries.iter().all(|(_, v)| is_constant(v)),
        RenderExpr::OperatorApplicationExp(op) => op.operands.iter().all(is_constant),
        _ => false,
    }
}

pub(crate) fn and_all(mut exprs: Vec<RenderExpr>) -> Option<RenderExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: exprs,
        })),
    }
}

pub(crate) fn or_all(mut exprs: Vec<RenderExpr>) -> RenderExpr {
    if exprs.len() == 1 {
        return exprs.pop().expect("one");
    }
    RenderExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Or,
        operands: exprs,
    })
}
