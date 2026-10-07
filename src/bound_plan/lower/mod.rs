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
//! A WITH ends a **segment** (§4.10): the rows so far become a CTE whose
//! columns are exactly the WITH's output scope, and the next segment reads
//! from it. A carried node or relationship exports its identity columns (a
//! relationship also its endpoint columns) and the properties later clauses
//! read (the demand pass, [`demand`]); a value exports its value. A
//! free-standing SKIP / LIMIT ends a segment the same way, exporting the
//! scope unchanged. Rows keep an order only until a MATCH or an aggregation;
//! while they have one it travels as exported sort-key columns, so a later
//! SKIP / LIMIT / RETURN reads the rows in that order.
//!
//! Scope today — everything else is [`LowerError::Unsupported`] and the
//! query is translated by the legacy pipeline:
//! * MATCH (not OPTIONAL) over standard-layout labels and types
//!   (`NodeSchema::is_standard_own_table`, `RelationshipSchema::
//!   is_standard_edge_table`), each node with one label and each
//!   relationship with one type after label inference, fixed length,
//!   directed;
//! * WITH and RETURN with aggregation, DISTINCT, ORDER BY, SKIP, LIMIT and
//!   (WITH) WHERE, evaluated in that order; free-standing ORDER BY, SKIP and
//!   LIMIT;
//! * a final RETURN of values (no whole nodes or relationships: Bolt and
//!   graph output need the result shape first).
//!
//! A node or relationship whose label / type set is empty matches nothing
//! (Cypher returns no rows; it is not an error): the query lowers to a
//! relation with no rows, and its properties read as NULL.

mod expr;
#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{
    ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::{
    ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
    JoinItems, JoinType, LimitItem, OrderByItem, OrderByItems, OrderByOrder, RenderPlan,
    SelectItem, SelectItems, SkipItem, UnionItems, ViewTableRef,
};
use crate::utils::cte_column_naming::cte_column_name;

use super::expr::{calls_aggregate, property_refs};
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
    let BoundOp::Project { input, projection } = &stmt.plan else {
        return unsupported("a statement that does not end in RETURN (UNION: S7)");
    };
    let mut l = Lowerer {
        schema,
        bindings: &stmt.bindings,
        options,
        demand: demand(stmt),
        ctes: Vec::new(),
        scans: HashMap::new(),
        values: HashMap::new(),
        emitted: Vec::new(),
        from: None,
        joins: Vec::new(),
        pending: Vec::new(),
        filters: Vec::new(),
        empty: false,
        order: Vec::new(),
    };
    l.relation(input)?;
    l.finish_relation();
    let mut plan = l.project(projection)?;
    plan.ctes = CteItems(std::mem::take(&mut l.ctes));
    Ok(plan)
}

/// How a pattern element is read.
#[derive(Debug, Clone)]
enum Scan<'s> {
    Node {
        schema: &'s NodeSchema,
        label: String,
        at: At,
    },
    Rel {
        schema: &'s RelationshipSchema,
        rel_type: String,
        at: At,
    },
    /// An element whose label / type set is empty: it matches nothing.
    Impossible,
}

/// Where an element's columns are.
#[derive(Debug, Clone)]
enum At {
    /// Its own table, under this alias (`v{N}`).
    Table(String),
    /// Columns of a CTE (a WITH or a SKIP / LIMIT) under `alias`: the
    /// element's physical identity / endpoint columns by exported name, and
    /// its demanded properties as expressions over the CTE (a property that
    /// is a constant, such as an unmapped one's NULL, stays the constant).
    Exported {
        alias: String,
        physical: HashMap<String, String>,
        props: HashMap<String, RenderExpr>,
    },
}

impl At {
    fn alias(&self) -> &str {
        match self {
            At::Table(a) => a,
            At::Exported { alias, .. } => alias,
        }
    }
}

impl Scan<'_> {
    fn at(&self) -> Option<&At> {
        match self {
            Scan::Node { at, .. } | Scan::Rel { at, .. } => Some(at),
            Scan::Impossible => None,
        }
    }
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
    /// Properties read of each binding, anywhere downstream (§4.10).
    demand: HashMap<VarId, BTreeSet<String>>,
    /// Finished segments, in order.
    ctes: Vec<Cte>,
    // --- the current segment
    scans: HashMap<VarId, Scan<'s>>,
    /// Values exported by the segment's CTE: a column, or a constant.
    values: HashMap<VarId, RenderExpr>,
    /// Relation aliases in join order.
    emitted: Vec<String>,
    from: Option<ViewTableRef>,
    joins: Vec<Join>,
    pending: Vec<Tie>,
    filters: Vec<RenderExpr>,
    /// Some element matches nothing: the relation has no rows.
    empty: bool,
    /// The rows' order, while they have one (after an ORDER BY, until a
    /// MATCH or an aggregation).
    order: Vec<OrderByItem>,
}

/// `alias.column`.
pub(crate) fn col_at(alias: &str, column: &str) -> RenderExpr {
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(alias.to_string()),
        column: PropertyValue::Column(column.to_string()),
    })
}

fn eq(a: RenderExpr, b: RenderExpr) -> OperatorApplication {
    OperatorApplication {
        operator: Operator::Equal,
        operands: vec![a, b],
    }
}

fn select(expression: RenderExpr, alias: &str) -> SelectItem {
    SelectItem {
        expression,
        col_alias: Some(ColumnAlias(alias.to_string())),
    }
}

/// What a projection or a SKIP / LIMIT puts in its SELECT.
#[derive(Default)]
struct Body {
    select: Vec<SelectItem>,
    distinct: bool,
    group_by: Vec<RenderExpr>,
    having: Vec<RenderExpr>,
    order_by: Vec<OrderByItem>,
    skip: Option<i64>,
    limit: Option<i64>,
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
                // Joining other rows to them leaves the rows in no order.
                self.order.clear();
                Ok(())
            }
            BoundOp::Project { input, projection } => {
                if projection.kind != ProjectionKind::With {
                    return unsupported("a RETURN that is not the last clause");
                }
                self.relation(input)?;
                self.finish_relation();
                self.with(projection)
            }
            BoundOp::Sort { input, keys } => {
                self.relation(input)?;
                self.order = self.sort_keys(keys, &HashMap::new())?;
                Ok(())
            }
            BoundOp::Skip { input, count } => {
                self.relation(input)?;
                self.finish_relation();
                self.page(Some(*count), None)
            }
            BoundOp::Limit { input, count } => {
                self.relation(input)?;
                self.finish_relation();
                self.page(None, Some(*count))
            }
            BoundOp::Unwind { .. } => unsupported("UNWIND (S7)"),
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
                Scan::Node {
                    schema: ns,
                    label,
                    at: At::Table(v.name()),
                }
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
                        at: At::Table(r.var.name()),
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
                let Some(ids) = self.identity(end)? else {
                    continue; // an endpoint that matches nothing
                };
                if ids.len() != cols.len() {
                    return unsupported("endpoint id arity differs from the edge's");
                }
                let mut eqs = Vec::new();
                for (c, id) in cols.iter().zip(ids) {
                    eqs.push((self.physical(r.var, c)?, id));
                }
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

    /// The physical identity columns of a node, or of a relationship: its
    /// `edge_id`, else its stored (from, to) endpoint columns (#887 policy).
    fn identity_physical(&self, v: VarId) -> Option<Vec<String>> {
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

    /// An element's identity, one expression per identity column; `None` for
    /// an element that matches nothing.
    fn identity(&self, v: VarId) -> Result<Option<Vec<RenderExpr>>, LowerError> {
        let Some(cols) = self.identity_physical(v) else {
            return Ok(None);
        };
        cols.iter()
            .map(|c| self.physical(v, c))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// A physical column of an element's table, wherever the element is read.
    fn physical(&self, v: VarId, column: &str) -> Result<RenderExpr, LowerError> {
        match self.scans.get(&v).and_then(Scan::at) {
            Some(At::Table(alias)) => Ok(col_at(alias, column)),
            Some(At::Exported {
                alias, physical, ..
            }) => match physical.get(column) {
                Some(exported) => Ok(col_at(alias, exported)),
                None => unsupported(format!("internal: column {column} of {v} is not exported")),
            },
            None => unsupported(format!("internal: {v} has no columns")),
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
                let ia = self.identity(*a)?.expect("a relationship scan");
                let ib = self.identity(*b)?.expect("a relationship scan");
                let differs: Vec<RenderExpr> = ia
                    .into_iter()
                    .zip(ib)
                    .map(|(x, y)| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::NotEqual,
                            operands: vec![x, y],
                        })
                    })
                    .collect();
                self.filters.push(or_all(differs));
            }
        }
        Ok(())
    }

    /// Add a decided scan to the join tree (once per variable). An element
    /// read from the segment's CTE is already there.
    fn emit(&mut self, v: VarId) -> Result<(), LowerError> {
        if self.is_emitted(v) {
            return Ok(());
        }
        let table = match &self.scans[&v] {
            Scan::Node {
                schema,
                at: At::Table(alias),
                ..
            } => (
                alias.clone(),
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Rel {
                schema,
                at: At::Table(alias),
                ..
            } => (
                alias.clone(),
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Node {
                at: At::Exported { .. },
                ..
            }
            | Scan::Rel {
                at: At::Exported { .. },
                ..
            } => return unsupported("internal: a CTE element outside its segment"),
            Scan::Impossible => {
                self.empty = true;
                return Ok(());
            }
        };
        let (alias, base, params, use_final, filter) = table;
        let name = ViewTableRef::parameterized_name(
            &base,
            params,
            self.options.view_parameter_values.as_ref(),
        );
        if let Some(f) = filter {
            match f.to_sql(&alias) {
                Ok(sql) => self.filters.push(RenderExpr::Raw(format!("({sql})"))),
                Err(e) => return unsupported(format!("schema filter: {e}")),
            }
        }
        if self.from.is_none() {
            self.from = Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name,
                alias: Some(alias.clone()),
                use_final,
            });
        } else {
            if use_final {
                // `Join` prints no FINAL (S8 with the other table options).
                return unsupported("FINAL on a joined table");
            }
            self.joins.push(join(name, &alias));
        }
        self.emitted.push(alias);
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

    /// The position of an element's relation in the join tree.
    fn position(&self, v: VarId) -> Option<usize> {
        let alias = self.scans.get(&v)?.at()?.alias();
        self.emitted.iter().position(|e| e == alias)
    }

    fn is_emitted(&self, v: VarId) -> bool {
        self.position(v).is_some()
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
    /// case. A tie within the FROM relation (two elements of the segment's
    /// CTE, `WITH a, r MATCH (a)-[r]->()`) is a WHERE.
    fn place(&mut self, t: Tie) {
        let later = self
            .position(t.a)
            .max(self.position(t.b))
            .expect("both emitted");
        let eqs = t.eqs.into_iter().map(|(x, y)| eq(x, y));
        if later == 0 {
            self.filters
                .extend(eqs.map(RenderExpr::OperatorApplicationExp));
            return;
        }
        let alias = self.emitted[later].clone();
        let join = self
            .joins
            .iter_mut()
            .find(|j| j.table_alias == alias)
            .expect("every emitted relation after the first is a join");
        join.joining_on.extend(eqs);
    }

    fn finish_relation(&mut self) {
        // A tie to an impossible scan never completes; the relation is empty.
        self.pending.clear();
    }

    // ------------------------------------------------------------- segments

    /// `ORDER BY` keys over the current relation (and a projection's items).
    fn sort_keys(
        &self,
        keys: &[SortKey],
        items: &HashMap<VarId, RenderExpr>,
    ) -> Result<Vec<OrderByItem>, LowerError> {
        let keys = keys
            .iter()
            .map(|k| {
                Ok(OrderByItem {
                    expression: self.expr(&k.expr, items)?,
                    order: if k.descending {
                        OrderByOrder::Desc
                    } else {
                        OrderByOrder::Asc
                    },
                })
            })
            .collect::<Result<Vec<_>, LowerError>>()?;
        // A constant key orders nothing, and ClickHouse would read an
        // integer one as a column position (`ORDER BY 1`).
        Ok(keys
            .into_iter()
            .filter(|o| !is_constant(&o.expression))
            .collect())
    }

    /// The SELECT of a WITH or RETURN over the current relation. For a WITH
    /// (`cte` = the alias its CTE will have), node and relationship items are
    /// exported as columns and the returned scope maps each item to where
    /// the next segment reads it.
    fn projection_body(
        &mut self,
        p: &Projection,
        cte: Option<&str>,
    ) -> Result<(Body, Exports<'s>), LowerError> {
        let aggregating = p.aggregates();
        let mut body = Body {
            distinct: p.distinct,
            skip: p.skip,
            limit: p.limit,
            ..Body::default()
        };
        let mut exports = Exports::default();
        let mut items_env: HashMap<VarId, RenderExpr> = HashMap::new();
        for it in &p.items {
            let element = match &it.expr {
                LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) => {
                    parse_var(n).filter(|v| {
                        !matches!(self.binding(*v).kind, BindingKind::Value) && !it.aggregate
                    })
                }
                _ => None,
            };
            if let Some(src) = element {
                let Some(alias) = cte else {
                    return unsupported(
                        "returning a whole node, relationship or path (needs the result shape)",
                    );
                };
                if matches!(self.binding(src).kind, BindingKind::Path) {
                    return unsupported("a path variable (S6)");
                }
                let mut keys = Vec::new();
                let scan = self.export_element(src, it.var, alias, &mut body.select, &mut keys)?;
                if aggregating {
                    body.group_by.extend(keys);
                }
                // In this projection's ORDER BY / WHERE the item is the
                // element itself.
                if let Some(s) = self.scans.get(&src).cloned() {
                    self.scans.insert(it.var, s);
                }
                exports.scans.push((it.var, scan));
                continue;
            }
            let e = self.expr(&it.expr, &HashMap::new())?;
            if aggregating && !it.aggregate {
                body.group_by.push(e.clone());
            }
            items_env.insert(it.var, e.clone());
            match cte {
                // A constant needs no column: the next segment uses it as is.
                Some(_) if is_constant(&e) => {
                    exports.values.push((it.var, e));
                }
                Some(alias) => {
                    let name = it.var.name();
                    body.select.push(select(e, &name));
                    exports.values.push((it.var, col_at(alias, &name)));
                }
                None => body.select.push(select(e, &it.name)),
            }
        }
        body.order_by = self.sort_keys(&p.order_by, &items_env)?;
        // Without its own ORDER BY, a projection keeps the order of its input
        // rows unless it aggregates or is DISTINCT.
        if body.order_by.is_empty() && !self.order.is_empty() {
            if aggregating && p.items.iter().any(|i| calls_aggregate(&i.expr, "collect")) {
                return unsupported("collect() over ordered rows (keeping the order)");
            }
            if p.distinct && (p.skip.is_some() || p.limit.is_some()) {
                return unsupported("DISTINCT then SKIP / LIMIT over ordered rows");
            }
            if !aggregating && !p.distinct {
                body.order_by = self.order.clone();
            }
        }
        if let Some(f) = &p.filter {
            let e = self.expr(f, &items_env)?;
            if p.skip.is_some() || p.limit.is_some() {
                // The WHERE filters the rows the SKIP / LIMIT kept (#1311):
                // computed per row here, applied by the next segment.
                let alias = cte.expect("only a WITH has a WHERE");
                body.select.push(select(e, KEEP));
                exports.keep = Some(col_at(alias, KEEP));
            } else if aggregating {
                body.having.push(e);
            } else {
                self.filters.push(e);
            }
        }
        Ok((body, exports))
    }

    /// Export a node or relationship `src` as `out` from a CTE aliased
    /// `alias`: its physical identity (and endpoint) columns, and the
    /// properties read of `out` downstream. Returns how the next segment
    /// reads it, and (in `keys`) the exported expressions.
    fn export_element(
        &self,
        src: VarId,
        out: VarId,
        alias: &str,
        select_list: &mut Vec<SelectItem>,
        keys: &mut Vec<RenderExpr>,
    ) -> Result<Scan<'s>, LowerError> {
        let Some(scan) = self.scans.get(&src).cloned() else {
            return unsupported(format!("internal: {src} has no scan"));
        };
        let mut physical_cols = self.identity_physical(src).unwrap_or_default();
        if let Scan::Rel { schema, .. } = &scan {
            for c in schema
                .from_id
                .columns()
                .into_iter()
                .chain(schema.to_id.columns())
            {
                if !physical_cols.iter().any(|p| p == c) {
                    physical_cols.push(c.to_string());
                }
            }
        }
        if matches!(scan, Scan::Impossible) {
            return Ok(Scan::Impossible);
        }
        let mut physical = HashMap::new();
        for c in &physical_cols {
            let name = format!("{out}__{c}");
            let e = self.physical(src, c)?;
            select_list.push(select(e.clone(), &name));
            keys.push(e);
            physical.insert(c.clone(), name);
        }
        let mut props = HashMap::new();
        for prop in self.demand.get(&out).into_iter().flatten() {
            let e = self.property(src, prop)?;
            if is_constant(&e) {
                props.insert(prop.clone(), e);
                continue;
            }
            let name = cte_column_name(&out.name(), prop);
            select_list.push(select(e.clone(), &name));
            keys.push(e);
            props.insert(prop.clone(), col_at(alias, &name));
        }
        let at = At::Exported {
            alias: alias.to_string(),
            physical,
            props,
        };
        Ok(match scan {
            Scan::Node { schema, label, .. } => Scan::Node { schema, label, at },
            Scan::Rel {
                schema, rel_type, ..
            } => Scan::Rel {
                schema,
                rel_type,
                at,
            },
            Scan::Impossible => Scan::Impossible,
        })
    }

    /// A WITH: the rows so far become a CTE whose columns are the WITH's
    /// output scope, and a new segment reads from it.
    fn with(&mut self, p: &Projection) -> Result<(), LowerError> {
        let alias = self.next_cte_alias();
        let (body, exports) = self.projection_body(p, Some(&alias))?;
        self.close_segment(body, exports, alias)
    }

    /// A free-standing SKIP / LIMIT: the rows so far, in their order, become
    /// a CTE exporting the scope unchanged.
    fn page(&mut self, skip: Option<i64>, limit: Option<i64>) -> Result<(), LowerError> {
        let alias = self.next_cte_alias();
        let mut body = Body {
            order_by: self.order.clone(),
            skip,
            limit,
            ..Body::default()
        };
        let mut exports = Exports::default();
        // The scope: every named element and value of this segment.
        let mut vars: Vec<VarId> = self
            .scans
            .keys()
            .chain(self.values.keys())
            .copied()
            .filter(|v| self.binding(*v).name.is_some())
            .collect();
        vars.sort();
        vars.dedup();
        for v in vars {
            if let Some(e) = self.values.get(&v).cloned() {
                if is_constant(&e) {
                    exports.values.push((v, e));
                } else {
                    body.select.push(select(e, &v.name()));
                    exports.values.push((v, col_at(&alias, &v.name())));
                }
                continue;
            }
            let scan = self.export_element(v, v, &alias, &mut body.select, &mut Vec::new())?;
            exports.scans.push((v, scan));
        }
        self.close_segment(body, exports, alias)
    }

    fn next_cte_alias(&self) -> String {
        format!("w{}", self.ctes.len() + 1)
    }

    /// Turn the current segment and `body` into a CTE named after `alias`,
    /// and start a segment that reads it.
    fn close_segment(
        &mut self,
        mut body: Body,
        exports: Exports<'s>,
        alias: String,
    ) -> Result<(), LowerError> {
        // The rows' order travels as exported sort-key columns; the CTE body
        // itself needs an ORDER BY only for its SKIP / LIMIT.
        let mut order = Vec::new();
        for (i, k) in body.order_by.iter().enumerate() {
            let name = format!("__o{i}");
            body.select.push(select(k.expression.clone(), &name));
            order.push(OrderByItem {
                expression: col_at(&alias, &name),
                order: k.order.clone(),
            });
        }
        if body.skip.is_none() && body.limit.is_none() {
            body.order_by.clear();
        }
        if body.select.is_empty() {
            // Nothing in scope needs a column; the rows still count.
            body.select
                .push(select(RenderExpr::Literal(Literal::Integer(1)), "__row"));
        }
        let plan = self.render(body);
        let name = format!("with_{alias}");
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        // The new segment: the CTE is its FROM; only the exported scope is
        // visible.
        self.scans = exports.scans.into_iter().collect();
        self.values = exports.values.into_iter().collect();
        self.emitted = vec![alias.clone()];
        self.from = Some(ViewTableRef {
            source: Arc::new(LogicalPlan::Empty),
            name,
            alias: Some(alias),
            use_final: false,
        });
        self.joins = Vec::new();
        self.pending = Vec::new();
        self.filters = exports.keep.into_iter().collect();
        self.empty = false;
        self.order = order;
        Ok(())
    }

    /// The final RETURN.
    fn project(&mut self, p: &Projection) -> Result<RenderPlan, LowerError> {
        if p.filter.is_some() {
            return unsupported("internal: a RETURN with a WHERE");
        }
        let (body, _) = self.projection_body(p, None)?;
        Ok(self.render(body))
    }

    /// The current relation under `body`'s SELECT.
    fn render(&mut self, body: Body) -> RenderPlan {
        // A relation that matches nothing keeps its scans (expressions may
        // read them) and filters every row out. With no scan at all (every
        // element impossible, or no MATCH: `RETURN 1 + 1`) there is no FROM.
        let mut filters = std::mem::take(&mut self.filters);
        if self.empty {
            filters = vec![RenderExpr::Literal(Literal::Boolean(false))];
        }
        let (from, joins) = (self.from.take(), std::mem::take(&mut self.joins));
        // A constant grouping key forms one group: drop it, but keep the
        // one-group semantics on an empty input (no row, unlike a global
        // aggregate).
        let mut group_by = body.group_by;
        let mut having = body.having;
        let only_constant_keys = !group_by.is_empty() && group_by.iter().all(is_constant);
        group_by.retain(|g| !is_constant(g));
        if only_constant_keys {
            having.insert(
                0,
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::GreaterThan,
                    operands: vec![
                        RenderExpr::AggregateFnCall(
                            crate::render_plan::render_expr::AggregateFnCall {
                                name: "count".to_string(),
                                args: vec![RenderExpr::Star],
                            },
                        ),
                        RenderExpr::Literal(Literal::Integer(0)),
                    ],
                }),
            );
        }
        RenderPlan {
            ctes: CteItems(Vec::new()),
            select: SelectItems {
                items: body.select,
                distinct: body.distinct,
            },
            from: FromTableItem(from),
            joins: JoinItems(joins),
            array_join: ArrayJoinItem(Vec::new()),
            filters: FilterItems(and_all(filters)),
            group_by: GroupByExpressions(group_by),
            having_clause: and_all(having),
            order_by: OrderByItems(body.order_by),
            skip: SkipItem(body.skip),
            limit: LimitItem(body.limit),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        }
    }
}

/// The per-row WITH WHERE value a CTE exports when the WHERE follows a SKIP
/// or LIMIT.
const KEEP: &str = "__keep";

/// What a segment's CTE exports: the next segment's scope.
#[derive(Default)]
struct Exports<'s> {
    scans: Vec<(VarId, Scan<'s>)>,
    values: Vec<(VarId, RenderExpr)>,
    /// A filter the next segment applies first (a WITH's WHERE after its
    /// SKIP / LIMIT).
    keep: Option<RenderExpr>,
}

fn join(table_name: String, alias: &str) -> Join {
    Join {
        table_name,
        table_alias: alias.to_string(),
        joining_on: Vec::new(),
        join_type: JoinType::Join,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
        graph_rel: None,
        is_cartesian: false,
    }
}

/// The properties read of each binding anywhere in the statement, carried
/// back through pass-through projection items (`WITH a AS b … b.name` reads
/// `a.name`), so a CTE exports what later clauses use (§4.10).
fn demand(stmt: &BoundStatement) -> HashMap<VarId, BTreeSet<String>> {
    fn exprs<'a>(op: &'a BoundOp, out: &mut Vec<&'a LogicalExpr>) {
        match op {
            BoundOp::Unit => {}
            BoundOp::Match {
                input,
                pattern,
                predicate,
                ..
            } => {
                exprs(input, out);
                for part in &pattern.parts {
                    for n in &part.nodes {
                        out.extend(n.props.iter().map(|(_, e)| e));
                    }
                    for r in &part.rels {
                        out.extend(r.props.iter().map(|(_, e)| e));
                    }
                }
                out.extend(predicate);
            }
            BoundOp::Unwind { input, expr, .. } => {
                exprs(input, out);
                out.push(expr);
            }
            BoundOp::Project { input, projection } => {
                exprs(input, out);
                out.extend(projection.items.iter().map(|i| &i.expr));
                out.extend(projection.order_by.iter().map(|k| &k.expr));
                out.extend(&projection.filter);
            }
            BoundOp::Sort { input, keys } => {
                exprs(input, out);
                out.extend(keys.iter().map(|k| &k.expr));
            }
            BoundOp::Skip { input, .. } | BoundOp::Limit { input, .. } => exprs(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| exprs(a, out)),
        }
    }
    let mut all = Vec::new();
    exprs(&stmt.plan, &mut all);
    let mut demand: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut add = |v: VarId, prop: &str| {
        demand.entry(v).or_default().insert(prop.to_string());
    };
    for e in all {
        for (name, prop) in property_refs(e) {
            if let Some(v) = parse_var(&name) {
                add(v, &prop);
            }
        }
    }
    for part in pattern_parts(&stmt.plan) {
        for n in &part.nodes {
            n.props.iter().for_each(|(p, _)| add(n.var, p));
        }
        for r in &part.rels {
            r.props.iter().for_each(|(p, _)| add(r.var, p));
        }
    }
    // A pass-through item is a later binding than its source.
    for b in stmt.bindings.iter().rev() {
        if let BindingSource::Projection { of: Some(src), .. } = b.source {
            if let Some(props) = demand.get(&b.id).cloned() {
                demand.entry(src).or_default().extend(props);
            }
        }
    }
    demand
}

fn pattern_parts(op: &BoundOp) -> Vec<&PatternPart> {
    match op {
        BoundOp::Unit => Vec::new(),
        BoundOp::Match { input, pattern, .. } => {
            let mut v = pattern_parts(input);
            v.extend(&pattern.parts);
            v
        }
        BoundOp::Unwind { input, .. }
        | BoundOp::Project { input, .. }
        | BoundOp::Sort { input, .. }
        | BoundOp::Skip { input, .. }
        | BoundOp::Limit { input, .. } => pattern_parts(input),
        BoundOp::Union { arms, .. } => arms.iter().flat_map(pattern_parts).collect(),
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
