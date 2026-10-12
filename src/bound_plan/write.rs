//! The embedded write path on the bound plan (S13,
//! `docs/design/EXPLICIT_SCOPE.md`): a statement of SET / REMOVE / DELETE
//! after a read part, planned as the lightweight UPDATE / DELETE statements
//! of [`WriteRenderPlan`].
//!
//! Each operation's node is updated or deleted by its identity: the
//! identities of the node in the read part's records
//! ([`lower::write::lower_identities`]), one statement per label the node
//! can have, reading that label's identities. A SET value is computed from
//! the row it updates ([`lower::write::lower_write_value`]); REMOVE sets the
//! property to NULL; DETACH DELETE first deletes the relationships of every
//! standard edge table at either end of the label.
//!
//! Admission (the read-only rules of the embedded-writes design): a
//! source-backed label (Parquet / S3 / Iceberg / Delta) is read-only; a
//! node is written in its own table, by a single-column identity.

use std::collections::HashMap;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema};
use crate::render_plan::render_expr::{Literal, RenderExpr};
use crate::render_plan::write_render::{
    CheckOp, CheckTable, DeleteOp, InsertOp, RowSource, UpdateOp, WriteRenderPlan,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_plan_to_sql_plain;

use super::lower::write::{lower_identities, lower_records, lower_write_value, IdentityRows};
use super::lower::LowerOptions;
use super::types::{BindingKind, BoundOp, BoundWrite, BoundWriteStatement, VarId};
use super::{bind_write_statement, BindError};

/// Why a write statement was not planned on the bound plan.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WritePlanError {
    /// A statement this planner does not handle yet (CREATE, an update
    /// followed by RETURN, …): the caller's other path.
    #[error("{0}")]
    Unsupported(String),
    /// A statement that cannot be executed (invalid, or a read-only
    /// target).
    #[error("{0}")]
    Rejected(String),
}

/// Whether `cypher` (comments stripped) is a statement of updating clauses
/// the clause-list parser reads: a write, whichever planner plans it.
pub fn is_write_statement(cypher: &str) -> bool {
    use crate::open_cypher_parser::clause_list::Clause;
    crate::open_cypher_parser::clause_list::parse_clause_statement(cypher).is_ok_and(|(_, stmt)| {
        std::iter::once(&stmt.first)
            .chain(stmt.unions.iter().map(|(_, q)| q))
            .flat_map(|q| &q.clauses)
            .any(|c| {
                matches!(
                    c,
                    Clause::Create(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_)
                )
            })
    })
}

/// A planned write statement: its writes, and its RETURN's SQL (a read run
/// after them).
#[derive(Debug, Clone)]
pub struct WritePlan {
    pub plan: WriteRenderPlan,
    pub ret: Option<String>,
    /// The RETURN reads the records before the writes (after a DELETE:
    /// Neo4j's RETURN reads the deleted nodes' records), else after them.
    pub ret_before: bool,
}

/// A RETURN after the writes ([`return_sql`]).
struct ReturnPlan {
    sql: String,
    before: bool,
    /// Nodes the RETURN reads that can be deleted ones: the statement fails
    /// when one is (Neo4j: "has been deleted in this transaction"; a NULL
    /// of an OPTIONAL MATCH is none).
    guarded: Vec<Guard>,
}

/// Plan the write statement `cypher` (comments stripped).
pub fn plan_write(cypher: &str, schema: &GraphSchema) -> Result<WritePlan, WritePlanError> {
    let (rest, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(cypher)
        .map_err(|e| WritePlanError::Unsupported(format!("not parsed: {e:?}")))?;
    if !rest.trim().trim_end_matches(';').trim().is_empty() {
        return Err(WritePlanError::Unsupported("unparsed text".to_string()));
    }
    let bound = bind_write_statement(&stmt, schema).map_err(|e| match e {
        BindError::Unsupported(_) | BindError::NotARead(_) => {
            WritePlanError::Unsupported(e.to_string())
        }
        e => WritePlanError::Rejected(e.to_string()),
    })?;
    let ret = match &bound.ret {
        Some(ret) => Some(return_sql(&bound, ret, schema)?),
        None => None,
    };
    let plan = Planner {
        stmt: &bound,
        schema,
        options: LowerOptions::default(),
        identities: HashMap::new(),
        detached: Vec::new(),
        guarded: ret.as_ref().map(|r| r.guarded.clone()).unwrap_or_default(),
    }
    .plan()?;
    Ok(WritePlan {
        plan,
        ret_before: ret.as_ref().is_some_and(|r| r.before),
        ret: ret.map(|r| r.sql),
    })
}

/// The SQL of a RETURN after the writes (S13c): the read part re-read
/// after them, which gives the records Cypher's RETURN reads (the same
/// records, their properties as the writes left them) when the writes
/// change none of the records the read part produces. Refused otherwise: a
/// DELETE or CREATE (other records), a SKIP / LIMIT in the read part (it
/// may pick others), a SET / REMOVE of a property the read part reads (it
/// may match others).
fn return_sql(
    bound: &BoundWriteStatement,
    ret: &super::types::BoundStatement,
    schema: &GraphSchema,
) -> Result<ReturnPlan, WritePlanError> {
    let refuse = |why: &str| {
        Err(WritePlanError::Rejected(format!(
            "RETURN after {why}: not supported (the statement's records are re-read after \
             its writes)"
        )))
    };
    // After a CREATE the RETURN reads the created nodes: not yet (the
    // legacy planner's: refused).
    if bound
        .writes
        .iter()
        .any(|w| matches!(w, BoundWrite::Create { .. }))
    {
        return Err(WritePlanError::Unsupported(
            "RETURN after a CREATE".to_string(),
        ));
    }
    if bound
        .writes
        .iter()
        .any(|w| matches!(w, BoundWrite::Delete { .. }))
    {
        return delete_return(bound, ret, schema);
    }
    fn pages(op: &BoundOp) -> bool {
        match op {
            BoundOp::Skip { .. } | BoundOp::Limit { .. } => true,
            BoundOp::Project { input, projection } => {
                projection.skip.is_some() || projection.limit.is_some() || pages(input)
            }
            BoundOp::Match { input, .. }
            | BoundOp::Unwind { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Filter { input, .. } => pages(input),
            BoundOp::Union { arms, .. } => arms.iter().any(pages),
            BoundOp::Unit => false,
        }
    }
    if pages(&bound.read.plan) {
        return refuse("an update of a MATCH with SKIP or LIMIT");
    }
    // A non-deterministic call is drawn again by the re-read.
    let mut exprs = Vec::new();
    super::lower::op_exprs(&bound.read.plan, &mut exprs);
    for sq in &bound.read.subqueries {
        super::lower::op_exprs(&sq.plan, &mut exprs);
    }
    const DRAWN: [&str; 8] = [
        "rand",
        "randomuuid",
        "timestamp",
        "datetime",
        "localdatetime",
        "date",
        "time",
        "localtime",
    ];
    if exprs
        .iter()
        .any(|e| crate::bound_plan::expr::calls_nullary(e, &DRAWN))
    {
        return refuse("an update of a MATCH that calls rand() or reads the clock");
    }
    // A column a SET / REMOVE writes, read by any node of the read part that
    // can be of its label (another variable can be the same node; an
    // undeclared property reads its column by name; an expression-mapped
    // one reads columns that cannot be told).
    let mut written: Vec<(String, String, &String)> = Vec::new();
    for w in &bound.writes {
        let (BoundWrite::Set { var, property, .. } | BoundWrite::Remove { var, property, .. }) = w
        else {
            continue;
        };
        if let BindingKind::Node { labels } = &bound.read.binding(*var).kind {
            for label in labels {
                let column = schema
                    .node_schema_opt(label)
                    .and_then(|n| property_column(n, property));
                if let Some(column) = column {
                    written.push((label.clone(), column, property));
                }
            }
        }
    }
    let read = super::lower::write::read_properties(&bound.read, schema);
    for (v, props) in &read {
        let BindingKind::Node { labels } = &bound.read.binding(*v).kind else {
            continue;
        };
        for (label, column, property) in &written {
            if !labels.contains(label) {
                continue;
            }
            let Some(node) = schema.node_schema_opt(label) else {
                continue;
            };
            let conflict = props.iter().any(|p| match node.property_mappings.get(p) {
                Some(PropertyValue::Expression(_)) => true,
                _ => property_column(node, p).as_ref() == Some(column),
            });
            if conflict {
                return refuse(&format!(
                    "a SET or REMOVE of `{property}`, which the MATCH reads"
                ));
            }
        }
    }
    let lowered = super::lower::lower_statement(ret, schema, &LowerOptions::default())
        .map_err(|e| WritePlanError::Unsupported(e.to_string()))?;
    Ok(ReturnPlan {
        sql: render_plan_to_sql_plain(lowered.plan),
        before: false,
        guarded: Vec::new(),
    })
}

/// A RETURN after a DELETE (S13d), as Neo4j's: it reads the records as they
/// were (read before the writes): `count(n)` the deleted nodes, a deleted
/// node returned whole its identity alone (no properties), its properties
/// or labels read an error when it is a node (a check before any write; a
/// NULL of an OPTIONAL MATCH reads NULL).
///
/// A node is read only where the check sees it: returned whole, its
/// properties, labels, `id`, `count`, `IS NULL`. In a list, a map, a
/// `collect`, a path or a value carried through WITH / UNWIND it is not
/// (refused), nor is the graph read by a subquery (after the DELETE). A
/// node of another variable can be a deleted one (the same node, or one
/// another record deletes): read, the statement fails when it is.
fn delete_return(
    bound: &BoundWriteStatement,
    ret: &super::types::BoundStatement,
    schema: &GraphSchema,
) -> Result<ReturnPlan, WritePlanError> {
    use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
    let refuse = |why: &str| {
        Err(WritePlanError::Rejected(format!(
            "RETURN after {why}: not supported"
        )))
    };
    if bound
        .writes
        .iter()
        .any(|w| matches!(w, BoundWrite::Set { .. } | BoundWrite::Remove { .. }))
    {
        return refuse("a DELETE and a SET or REMOVE");
    }
    let read = &bound.read;
    // A node is the deleted one when it passes it through (`WITH a AS b`).
    let root = |mut v: VarId| loop {
        match read.binding(v).source {
            super::types::BindingSource::Projection { of: Some(of), .. } => v = of,
            _ => return v,
        }
    };
    let mut targets = Vec::new();
    let mut detach = false;
    for w in &bound.writes {
        if let BoundWrite::Delete { var, detach: d, .. } = w {
            targets.push(*var);
            detach |= *d;
        }
    }
    let roots: Vec<VarId> = targets.iter().map(|v| root(*v)).collect();
    let deleted: std::collections::BTreeSet<VarId> = read
        .bindings
        .iter()
        .filter(|b| matches!(b.kind, BindingKind::Node { .. }) && roots.contains(&root(b.id)))
        .map(|b| b.id)
        .collect();
    let BoundOp::Project { projection, .. } = &ret.plan else {
        return Err(WritePlanError::Rejected(
            "internal: a RETURN that is not a projection".to_string(),
        ));
    };
    let exprs: Vec<&LogicalExpr> = projection
        .items
        .iter()
        .map(|i| &i.expr)
        .chain(projection.order_by.iter().map(|k| &k.expr))
        .collect();
    if exprs
        .iter()
        .any(|e| !crate::bound_plan::expr::subquery_vars(e).is_empty())
    {
        return refuse("a DELETE, of a pattern (it reads the graph after the DELETE)");
    }
    let names = |v: VarId| {
        exprs.iter().any(|e| {
            crate::bound_plan::expr::referenced_names(e, false)
                .iter()
                .any(|n| crate::bound_plan::lower::parse_var(n) == Some(v))
        })
    };
    // DETACH DELETE deletes the node's relationships too: a relationship
    // the RETURN reads may be one (not told here).
    if detach
        && read
            .bindings
            .iter()
            .any(|b| matches!(b.kind, BindingKind::Rel { .. }) && names(b.id))
    {
        return refuse("a DETACH DELETE, of a relationship");
    }
    let mut defs = HashMap::new();
    definitions(&ret.plan, &mut defs);
    let exposure = Exposure {
        stmt: ret,
        defs,
        memo: Default::default(),
    };
    if projection
        .items
        .iter()
        .any(|i| exposure.exposes(&i.expr, true))
        || projection
            .order_by
            .iter()
            .any(|k| exposure.exposes(&k.expr, false))
    {
        return refuse(
            "a DELETE, of a node, relationship or path in a list, map, path or value \
             (a deleted node in it is not told)",
        );
    }
    // The RETURN's own column of a node (`RETURN a ORDER BY a.x`): the node
    // it returns.
    let input = |v: VarId| {
        projection
            .items
            .iter()
            .find(|i| i.var == v)
            .and_then(|i| match &i.expr {
                LogicalExpr::TableAlias(TableAlias(n)) => crate::bound_plan::lower::parse_var(n),
                _ => None,
            })
            .unwrap_or(v)
    };
    let labels = |v: VarId| match &read.binding(v).kind {
        BindingKind::Node { labels } => labels.clone(),
        _ => Default::default(),
    };
    let mut guarded = Vec::new();
    for b in &read.bindings {
        if !matches!(b.kind, BindingKind::Node { .. }) {
            continue;
        }
        let name = b.id.name();
        let reads = exprs.iter().any(|e| reads_node(e, &name));
        let whole = projection
            .items
            .iter()
            .any(|i| matches!(&i.expr, LogicalExpr::TableAlias(TableAlias(n)) if *n == name));
        if !reads && !whole {
            continue;
        }
        let var = input(b.id);
        if deleted.contains(&b.id) {
            if reads && !guarded.contains(&Guard::Deleted(var)) {
                guarded.push(Guard::Deleted(var));
            }
            // Returned whole: its identity alone, of one label's columns.
            if whole && labels(b.id).len() != 1 {
                return refuse("a DELETE, of a deleted node of several possible labels");
            }
            continue;
        }
        for d in &targets {
            let guard = Guard::Other {
                var,
                deleted: *d,
                reads,
            };
            if !labels(*d).is_disjoint(&labels(var)) && !guarded.contains(&guard) {
                guarded.push(guard);
            }
        }
    }
    let options = LowerOptions {
        deleted: deleted.clone(),
        ..LowerOptions::default()
    };
    // Rejected, not the caller's other path: the legacy planner deletes
    // before it reads.
    let lowered = super::lower::lower_statement(ret, schema, &options)
        .map_err(|e| WritePlanError::Rejected(format!("RETURN after a DELETE: {e}")))?;
    Ok(ReturnPlan {
        sql: render_plan_to_sql_plain(lowered.plan),
        before: true,
        guarded,
    })
}

/// A node a RETURN after a DELETE reads ([`delete_return`]): the statement
/// fails when it is a deleted one.
#[derive(Debug, Clone, PartialEq)]
enum Guard {
    /// A deleted node's properties or labels.
    Deleted(VarId),
    /// A node of another variable (`reads`: its properties or labels, else
    /// returned whole), when it is one `deleted` deletes.
    Other {
        var: VarId,
        deleted: VarId,
        reads: bool,
    },
}

/// Does `e` read the properties or labels of node `name`?
fn reads_node(e: &crate::query_planner::logical_expr::LogicalExpr, name: &str) -> bool {
    crate::bound_plan::expr::reads_element(e, name) || label_tested(e, name)
}

fn label_tested(e: &crate::query_planner::logical_expr::LogicalExpr, name: &str) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;
    match e {
        LogicalExpr::LabelExpression { variable, .. } => variable == name,
        _ => crate::bound_plan::expr::children(e)
            .into_iter()
            .any(|c| label_tested(c, name)),
    }
}

/// The expression each value binding of `op` is projected or unwound
/// from (a UNION's arms: none, not told).
fn definitions<'e>(
    op: &'e BoundOp,
    out: &mut HashMap<VarId, &'e crate::query_planner::logical_expr::LogicalExpr>,
) {
    match op {
        BoundOp::Project { input, projection } => {
            for i in &projection.items {
                out.insert(i.var, &i.expr);
            }
            definitions(input, out);
        }
        BoundOp::Unwind { input, expr, var } => {
            out.insert(*var, expr);
            definitions(input, out);
        }
        BoundOp::Match { input, .. }
        | BoundOp::Sort { input, .. }
        | BoundOp::Skip { input, .. }
        | BoundOp::Limit { input, .. }
        | BoundOp::Filter { input, .. } => definitions(input, out),
        BoundOp::Union { .. } | BoundOp::Unit => {}
    }
}

/// Where a RETURN after a DELETE reads a node, relationship or path that
/// the check of a deleted node does not see ([`delete_return`]).
struct Exposure<'s> {
    stmt: &'s super::types::BoundStatement,
    defs: HashMap<VarId, &'s crate::query_planner::logical_expr::LogicalExpr>,
    memo: std::cell::RefCell<HashMap<VarId, bool>>,
}

impl Exposure<'_> {
    /// Can value `v` be, or hold, a node, relationship or path?
    fn holds(&self, v: VarId) -> bool {
        use super::types::{BindingSource, SubqueryKind};
        if let Some(h) = self.memo.borrow().get(&v) {
            return *h;
        }
        self.memo.borrow_mut().insert(v, true);
        let b = self.stmt.binding(v);
        let holds = match (&b.kind, &b.source) {
            (BindingKind::Value, BindingSource::Projection { of: Some(of), .. }) => self.holds(*of),
            // A comprehension's element, of a list read where it is checked.
            (BindingKind::Value, BindingSource::Local) => false,
            (BindingKind::Value, BindingSource::Subquery) => {
                match self.stmt.subqueries.iter().find(|s| s.var == v) {
                    Some(s) => match &s.kind {
                        SubqueryKind::Collect(e) => self.exposes(e, false),
                        SubqueryKind::Exists | SubqueryKind::Count => false,
                    },
                    None => true,
                }
            }
            (BindingKind::Value, _) => self.defs.get(&v).is_none_or(|e| self.exposes(e, false)),
            _ => true,
        };
        self.memo.borrow_mut().insert(v, holds);
        holds
    }

    /// Does `e` read a node, relationship or path other than as an item of
    /// its own (`top`), its properties, labels, `id`, `count`, `IS NULL`?
    fn exposes(&self, e: &crate::query_planner::logical_expr::LogicalExpr, top: bool) -> bool {
        use crate::bound_plan::lower::parse_var;
        use crate::query_planner::logical_expr::{LogicalExpr, Operator, TableAlias};
        const READS: [&str; 7] = [
            "id",
            "elementid",
            "labels",
            "properties",
            "keys",
            "type",
            "length",
        ];
        let var = |x: &LogicalExpr| match x {
            LogicalExpr::TableAlias(TableAlias(n)) => parse_var(n),
            _ => None,
        };
        // A node, a relationship or a path: read where it is checked.
        let element = |v: VarId| {
            matches!(
                self.stmt.binding(v).kind,
                BindingKind::Node { .. }
                    | BindingKind::Rel { length: None, .. }
                    | BindingKind::Path
            )
        };
        let read = |v: VarId| !element(v) && self.holds(v);
        match e {
            LogicalExpr::TableAlias(TableAlias(n)) => parse_var(n).is_some_and(|v| {
                let whole = top && !matches!(self.stmt.binding(v).kind, BindingKind::Path);
                !(whole && element(v)) && self.holds(v)
            }),
            LogicalExpr::PropertyAccessExp(pa) => parse_var(&pa.table_alias.0).is_some_and(read),
            LogicalExpr::LabelExpression { variable, .. } => parse_var(variable).is_some_and(read),
            LogicalExpr::ScalarFnCall(f)
                if f.args.len() == 1
                    && var(&f.args[0]).is_some()
                    && READS.contains(&f.name.to_ascii_lowercase().as_str()) =>
            {
                var(&f.args[0]).is_some_and(read)
            }
            LogicalExpr::AggregateFnCall(f)
                if f.name.eq_ignore_ascii_case("count")
                    && f.args.len() == 1
                    && var(&f.args[0]).is_some() =>
            {
                false
            }
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op)
                if matches!(op.operator, Operator::IsNull | Operator::IsNotNull)
                    && op.operands.len() == 1
                    && var(&op.operands[0]).is_some() =>
            {
                false
            }
            _ => crate::bound_plan::expr::children(e)
                .into_iter()
                .any(|c| self.exposes(c, false)),
        }
    }
}

/// One SET / REMOVE of a node: its property, and its value (REMOVE: none).
type Assignment<'e> = (
    String,
    Option<&'e crate::query_planner::logical_expr::LogicalExpr>,
);

struct Planner<'a> {
    stmt: &'a BoundWriteStatement,
    schema: &'a GraphSchema,
    options: LowerOptions,
    /// Each node's identity rows, lowered once.
    identities: HashMap<VarId, Option<IdentityRows>>,
    /// The relationship rows the statement's DETACH DELETEs delete: edge
    /// table, its column, the ids (a node's relationships are not "still
    /// there" for another DELETE when this one deletes them).
    detached: Vec<(EdgeEnd, RowSource)>,
    /// Nodes the RETURN reads that can be deleted ones ([`ReturnPlan`]).
    guarded: Vec<Guard>,
}

impl<'a> Planner<'a> {
    fn plan(mut self) -> Result<WriteRenderPlan, WritePlanError> {
        // In clause order: one UPDATE per SET / REMOVE clause, node and
        // label (a later clause reads what an earlier one wrote: its own
        // statement, run after), its assignments in order (a property
        // assigned twice: the last); a DELETE per node and label.
        enum Group<'e> {
            Update {
                clause: usize,
                var: VarId,
                items: Vec<Assignment<'e>>,
            },
            Delete {
                var: VarId,
                detach: bool,
            },
            Create {
                label: &'e str,
                properties: &'e [(String, crate::query_planner::logical_expr::LogicalExpr)],
            },
        }
        let mut groups: Vec<Group<'_>> = Vec::new();
        for w in &self.stmt.writes {
            let (var, property, value, clause) = match w {
                BoundWrite::Set {
                    var,
                    property,
                    value,
                    clause,
                } => (*var, property, Some(value), *clause),
                BoundWrite::Remove {
                    var,
                    property,
                    clause,
                } => (*var, property, None, *clause),
                BoundWrite::Create {
                    label, properties, ..
                } => {
                    groups.push(Group::Create { label, properties });
                    continue;
                }
                BoundWrite::Delete { var, detach, .. } => {
                    let earlier = groups.iter_mut().find_map(|g| match g {
                        Group::Delete { var: v, detach } if v == var => Some(detach),
                        _ => None,
                    });
                    match earlier {
                        Some(d) => *d |= *detach,
                        None => groups.push(Group::Delete {
                            var: *var,
                            detach: *detach,
                        }),
                    }
                    continue;
                }
            };
            let same = groups.iter_mut().find_map(|g| match g {
                Group::Update {
                    clause: c,
                    var: v,
                    items,
                } if *c == clause && *v == var => Some(items),
                _ => None,
            });
            match same {
                Some(items) => items.push((property.clone(), value)),
                None => groups.push(Group::Update {
                    clause,
                    var,
                    items: vec![(property.clone(), value)],
                }),
            }
        }
        for g in &groups {
            if let Group::Delete { var, detach: true } = g {
                for label in self.labels(*var)? {
                    self.writable(&label)?;
                    let Some(source) = self.source(*var, &label)? else {
                        continue;
                    };
                    for end in edge_ends(self.schema, &label, true)? {
                        self.detached.push((end, source.clone()));
                    }
                }
            }
        }
        let mut ops = Vec::new();
        for guard in self.guarded.clone() {
            match guard {
                Guard::Deleted(var) => {
                    for label in self.labels(var)? {
                        if let Some(source) = self.source(var, &label)? {
                            ops.push(WriteRenderPlan::Check(CheckOp {
                                source,
                                within: None,
                                message: format!(
                                    "a `{label}` node the statement deleted is read by its \
                                     RETURN (Neo4j: it has been deleted in this transaction)"
                                ),
                            }));
                        }
                    }
                }
                Guard::Other {
                    var,
                    deleted,
                    reads,
                } => {
                    let of = self.labels(deleted)?;
                    for label in self.labels(var)? {
                        if !of.contains(&label) {
                            continue;
                        }
                        let Some(source) = self.overlap(var, deleted, &label)? else {
                            continue;
                        };
                        let name = self.stmt.read.binding(var).name.clone().unwrap_or_default();
                        let message = match reads {
                            true => format!(
                                "a `{label}` node the statement deleted is read by its RETURN, \
                                 as `{name}` (Neo4j: it has been deleted in this transaction)"
                            ),
                            false => format!(
                                "a `{label}` node the statement deleted is returned as `{name}`: \
                                 not supported"
                            ),
                        };
                        ops.push(WriteRenderPlan::Check(CheckOp {
                            source,
                            within: None,
                            message,
                        }));
                    }
                }
            }
        }
        // The nodes an earlier clause of the statement changed: the read
        // part's records were read before any write (a snapshot), so a
        // CREATE value reading one would see it unchanged.
        let mut changed: Vec<VarId> = Vec::new();
        for g in groups {
            match g {
                Group::Update { var, items, .. } => {
                    self.update(var, &items, &mut ops)?;
                    changed.push(var);
                }
                Group::Delete { var, detach } => {
                    self.delete(var, detach, &mut ops)?;
                    changed.push(var);
                }
                Group::Create { label, properties } => {
                    self.create(label, properties, &changed, &mut ops)?
                }
            }
        }
        // Several statements: the ids of each read before any runs (one
        // statement's MATCH re-run after another's write would find other
        // rows: a node's relationships deleted, it is no longer matched).
        let statements = ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    WriteRenderPlan::Update(_)
                        | WriteRenderPlan::Delete(_)
                        | WriteRenderPlan::Insert(_)
                )
            })
            .count();
        if statements > 1 {
            for op in &mut ops {
                let source = match op {
                    WriteRenderPlan::Update(u) => &mut u.source,
                    WriteRenderPlan::Delete(d) => &mut d.source,
                    WriteRenderPlan::Check(c) => &mut c.source,
                    WriteRenderPlan::Insert(InsertOp {
                        select: Some(select),
                        ..
                    }) => select,
                    _ => continue,
                };
                if let RowSource::Sql(sql) = source {
                    *source = RowSource::Read(std::mem::take(sql));
                }
            }
        }
        Ok(match ops.len() {
            1 => ops.remove(0),
            _ => WriteRenderPlan::Sequence(ops),
        })
    }

    /// The UPDATEs of one SET / REMOVE clause's assignments of node `var`:
    /// one per label. A property a label cannot write fails the statement
    /// (a node of several labels: when a node of that label matches).
    fn update(
        &mut self,
        var: VarId,
        items: &[Assignment<'_>],
        ops: &mut Vec<WriteRenderPlan>,
    ) -> Result<(), WritePlanError> {
        let labels = self.labels(var)?;
        for label in &labels {
            let node = self.writable(label)?;
            let Some(source) = self.source(var, label)? else {
                continue;
            };
            let unwritable = items
                .iter()
                .find(|(p, _)| property_column(node, p).is_none());
            if let Some((property, _)) = unwritable {
                let message = format!(
                    "SET: property `{property}` of `{label}` cannot be mapped to a writable column"
                );
                if labels.len() == 1 {
                    return Err(WritePlanError::Rejected(message));
                }
                ops.push(WriteRenderPlan::Check(CheckOp {
                    source,
                    within: None,
                    message,
                }));
                continue;
            }
            // Cypher sets a value per record, each reading the node as the
            // records before left it: one UPDATE per node equals it only
            // when the node is in one record, or the value does not read
            // the node.
            let reads_node = items.iter().any(|(_, v)| {
                v.is_some_and(|v| {
                    crate::bound_plan::expr::referenced_names(v, false)
                        .iter()
                        .any(|n| crate::bound_plan::lower::parse_var(n) == Some(var))
                })
            });
            if reads_node {
                if let Some(repeated) = self.repeated(var, label)? {
                    ops.push(WriteRenderPlan::Check(CheckOp {
                        source: repeated,
                        within: None,
                        message: format!(
                            "a SET of a `{label}` node from its own properties when the node \
                             is in several of the MATCH's records (Cypher sets it once per \
                             record): not supported"
                        ),
                    }));
                }
            }
            let mut assignments: Vec<(String, RenderExpr)> = Vec::new();
            for (property, value) in items {
                let column = property_column(node, property).unwrap_or_default();
                let value = match value {
                    Some(v) => RenderExpr::Raw(
                        lower_write_value(
                            &self.stmt.read,
                            self.schema,
                            &self.options,
                            var,
                            label,
                            node,
                            v,
                        )
                        // A row's UPDATE computes no other value: refused
                        // (the legacy planner's SQL fails).
                        .map_err(|e| WritePlanError::Rejected(e.to_string()))?,
                    ),
                    None => RenderExpr::Literal(Literal::Null),
                };
                match assignments.iter_mut().find(|(c, _)| *c == column) {
                    Some(a) => a.1 = value,
                    None => assignments.push((column, value)),
                }
            }
            ops.push(WriteRenderPlan::Update(UpdateOp {
                database: node.database.clone(),
                table: node.table_name.clone(),
                assignments,
                id_column: id_column(node, label)?,
                source,
            }));
        }
        Ok(())
    }

    /// The INSERT of a CREATE of a node of `label`: one row per record of
    /// the read part (a read part of no MATCH: one, its `VALUES`), its
    /// properties' values read in the record.
    fn create(
        &mut self,
        label: &str,
        properties: &[(String, crate::query_planner::logical_expr::LogicalExpr)],
        changed: &[VarId],
        ops: &mut Vec<WriteRenderPlan>,
    ) -> Result<(), WritePlanError> {
        let node = self.writable(label)?;
        let reads_changed = properties.iter().any(|(_, v)| {
            crate::bound_plan::expr::referenced_names(v, false)
                .iter()
                .filter_map(|n| crate::bound_plan::lower::parse_var(n))
                .any(|v| changed.contains(&v))
        });
        if reads_changed {
            return Err(WritePlanError::Rejected(
                "a CREATE value that reads a node an earlier SET, REMOVE or DELETE of the \
                 statement changed: not supported"
                    .to_string(),
            ));
        }
        if properties.is_empty() {
            return Err(WritePlanError::Unsupported(
                "CREATE of a node with no properties".to_string(),
            ));
        }
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for (property, value) in properties {
            let column = property_column(node, property).ok_or_else(|| {
                WritePlanError::Rejected(format!(
                    "CREATE: property `{property}` of `{label}` cannot be mapped to a writable \
                     column"
                ))
            })?;
            columns.push(column);
            values.push(value.clone());
        }
        let (plan, sqls) = lower_records(&self.stmt.read, self.schema, &self.options, &values)
            .map_err(|e| match e {
                // Neo4j's TypeError: no property holds it.
                Ok(reason) => WritePlanError::Rejected(format!("CREATE: {reason}")),
                Err(e) => WritePlanError::Unsupported(e.to_string()),
            })?;
        let (rows, select) = match self.stmt.read.plan {
            BoundOp::Unit => (vec![sqls.into_iter().map(RenderExpr::Raw).collect()], None),
            _ => (
                Vec::new(),
                Some(RowSource::Sql(render_plan_to_sql_plain(plan))),
            ),
        };
        ops.push(WriteRenderPlan::Insert(InsertOp {
            database: node.database.clone(),
            table: node.table_name.clone(),
            columns,
            rows,
            select,
        }));
        Ok(())
    }

    /// The DELETEs of node `var`: one per label; DETACH: its relationships
    /// first. Without DETACH, a node with relationships fails the statement
    /// (Neo4j's rule), checked in each edge table at the label.
    fn delete(
        &mut self,
        var: VarId,
        detach: bool,
        ops: &mut Vec<WriteRenderPlan>,
    ) -> Result<(), WritePlanError> {
        for label in self.labels(var)? {
            let node = self.writable(&label)?;
            let Some(source) = self.source(var, &label)? else {
                continue;
            };
            match detach {
                true => ops.extend(detach_deletes(self.schema, &label, &source)?),
                false => {
                    for (table, column) in edge_ends(self.schema, &label, false)? {
                        let unless = self
                            .detached
                            .iter()
                            .filter(|((t, _), _)| *t == table)
                            .map(|((_, c), s)| (c.clone(), s.clone()))
                            .collect();
                        ops.push(WriteRenderPlan::Check(CheckOp {
                            source: source.clone(),
                            within: Some(CheckTable {
                                database: table.0,
                                table: table.1,
                                column,
                                unless,
                            }),
                            message: format!(
                                "Cannot delete a node of `{label}` because it still has \
                                 relationships. To delete this node, you must first delete its \
                                 relationships, or use DETACH DELETE."
                            ),
                        }));
                    }
                }
            }
            ops.push(WriteRenderPlan::Delete(DeleteOp {
                database: node.database.clone(),
                table: node.table_name.clone(),
                id_column: id_column(node, &label)?,
                source,
                relationships: false,
            }));
        }
        Ok(())
    }

    /// The ids of `var` of `label` in more than one of the read part's
    /// records; `None` when the node matches nothing.
    fn repeated(&self, var: VarId, label: &str) -> Result<Option<RowSource>, WritePlanError> {
        let Some(rows) = lower_identities(&self.stmt.read, self.schema, &self.options, var, false)
            .map_err(|e| WritePlanError::Unsupported(e.to_string()))?
        else {
            return Ok(None);
        };
        let ids = &rows.columns[usize::from(rows.labeled)..];
        let [id] = ids else {
            return Err(WritePlanError::Rejected(format!(
                "a write to `{label}`: composite primary keys are not supported"
            )));
        };
        let inner = render_plan_to_sql_plain(rows.plan.clone());
        let label_filter = match rows.labeled {
            true => format!(
                " WHERE {} = '{}'",
                rows.columns[0],
                label.replace('\'', "''")
            ),
            false => String::new(),
        };
        Ok(Some(RowSource::Sql(format!(
            "SELECT {id} FROM ({inner}) AS __cg_write_ids{label_filter} \
             GROUP BY {id} HAVING count() > 1"
        ))))
    }

    /// The labels node `var` can have.
    fn labels(&self, var: VarId) -> Result<Vec<String>, WritePlanError> {
        match &self.stmt.read.binding(var).kind {
            BindingKind::Node { labels } => Ok(labels.iter().cloned().collect()),
            _ => Err(WritePlanError::Rejected(
                "internal: a write target that is not a node".to_string(),
            )),
        }
    }

    /// The schema of `label`, refused when it is not writable.
    fn writable(&self, label: &str) -> Result<&'a NodeSchema, WritePlanError> {
        let Some(node) = self.schema.node_schema_opt(label) else {
            return Err(WritePlanError::Rejected(format!(
                "unknown node label `{label}`"
            )));
        };
        if node.source.is_some() {
            return Err(WritePlanError::Rejected(format!(
                "Cannot write to node label `{label}`: it resolves to a source-backed \
                 (read-only) table. Source-backed schemas (Parquet/S3/Iceberg/Delta) are \
                 read-only by design."
            )));
        }
        if !node.is_standard_own_table() {
            return Err(WritePlanError::Rejected(format!(
                "Cannot write to node label `{label}`: its nodes are not rows of their own table"
            )));
        }
        Ok(node)
    }

    /// The identities of `var` of `label` in the read part's records, as a
    /// subquery of one column; `None` when the node matches nothing. A node
    /// of several labels: the identities of `label`'s (its rows' label
    /// column). A subquery with CTEs is a [`RowSource::Read`]: ClickHouse's
    /// lightweight DELETE (and chdb's UPDATE) resolves its subquery's tables
    /// in the target's database, where a CTE is unknown, so the executor
    /// reads the ids first.
    fn source(&mut self, var: VarId, label: &str) -> Result<Option<RowSource>, WritePlanError> {
        Ok(self.ids(var, label)?.map(|(sql, _, ctes)| match ctes {
            true => RowSource::Read(sql),
            false => RowSource::Sql(sql),
        }))
    }

    /// The identities of `var` of `label` that are also of `deleted` (both
    /// in any record).
    fn overlap(
        &mut self,
        var: VarId,
        deleted: VarId,
        label: &str,
    ) -> Result<Option<RowSource>, WritePlanError> {
        let (Some((these, column, a)), Some((those, _, b))) =
            (self.ids(var, label)?, self.ids(deleted, label)?)
        else {
            return Ok(None);
        };
        let sql =
            format!("SELECT {column} FROM ({these}) AS __cg_these WHERE {column} IN ({those})");
        Ok(Some(match a || b {
            true => RowSource::Read(sql),
            false => RowSource::Sql(sql),
        }))
    }

    /// The SELECT of `var`'s identities of `label` ([`Self::source`]): its
    /// column, and whether it has CTEs.
    fn ids(
        &mut self,
        var: VarId,
        label: &str,
    ) -> Result<Option<(String, String, bool)>, WritePlanError> {
        if !self.identities.contains_key(&var) {
            let rows = lower_identities(&self.stmt.read, self.schema, &self.options, var, true)
                .map_err(|e| WritePlanError::Unsupported(e.to_string()))?;
            self.identities.insert(var, rows);
        }
        let Some(rows) = &self.identities[&var] else {
            return Ok(None);
        };
        let ids = &rows.columns[usize::from(rows.labeled)..];
        let [id] = ids else {
            return Err(WritePlanError::Rejected(format!(
                "a write to `{label}`: composite primary keys are not supported"
            )));
        };
        let inner = render_plan_to_sql_plain(rows.plan.clone());
        let label_filter = match rows.labeled {
            true => format!(
                " WHERE {} = '{}'",
                rows.columns[0],
                label.replace('\'', "''")
            ),
            false => String::new(),
        };
        let sql = format!("SELECT {id} FROM ({inner}) AS __cg_write_ids{label_filter}");
        Ok(Some((sql, id.clone(), !rows.plan.ctes.0.is_empty())))
    }
}

/// The node table's column of `property`: a column mapping, or a column of
/// the table by that name; an expression-mapped property is not writable.
fn property_column(node: &NodeSchema, property: &str) -> Option<String> {
    match node.property_mappings.get(property) {
        Some(PropertyValue::Column(c)) => Some(c.clone()),
        Some(PropertyValue::Expression(_)) => None,
        None => node
            .column_names
            .iter()
            .any(|c| c == property)
            .then(|| property.to_string()),
    }
}

fn id_column(node: &NodeSchema, label: &str) -> Result<String, WritePlanError> {
    node.node_id
        .column_or_error()
        .map(str::to_string)
        .map_err(|e| {
            WritePlanError::Rejected(format!(
                "a write to `{label}`: composite primary keys are not supported ({e})"
            ))
        })
}

/// An edge table, `(database, table)`, and a column of it holding a
/// relationship's end.
type EdgeEnd = ((String, String), String);

/// The edge table columns that hold a node of `label` as a relationship's
/// end: each plain edge table with fixed end labels, `(database, table)`
/// and column, per end at the label. An FK edge's relationship is a node's
/// row (gone with either node). A polymorphic table, or one embedding node
/// properties, cannot tell the label's relationships apart by its id: with
/// `strict` (DETACH DELETE, which must delete them) refused, else skipped.
fn edge_ends(
    schema: &GraphSchema,
    label: &str,
    strict: bool,
) -> Result<Vec<EdgeEnd>, WritePlanError> {
    let mut out = Vec::new();
    let mut rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    rels.sort_by(|a, b| a.0.cmp(b.0));
    let mut seen = std::collections::HashSet::new();
    for (_, rel) in rels {
        // A polymorphic table's ends are any label (`$any`, a label column).
        let fixed = rel.has_fixed_endpoint_labels();
        if rel.is_node_row_reference() || (fixed && rel.from_node != label && rel.to_node != label)
        {
            continue;
        }
        // Without DETACH a row of either end column holding the id is a
        // relationship the node may have (another label's of the same id
        // too: refused, loudly).
        let any_end = !fixed || !rel.is_plain_edge_table();
        if any_end && strict {
            return Err(WritePlanError::Rejected(format!(
                "a DELETE of `{label}`: the relationships of table `{}` cannot be told apart by \
                 the node's id alone (a polymorphic table, or one embedding node properties)",
                rel.table_name
            )));
        }
        let table = (rel.database.clone(), rel.table_name.clone());
        for (end, at_end) in [
            (&rel.from_id, any_end || rel.from_node == label),
            (&rel.to_id, any_end || rel.to_node == label),
        ] {
            if !at_end {
                continue;
            }
            let column = end.as_single().map_err(|_| {
                WritePlanError::Rejected(format!(
                    "a DELETE of `{label}`: a composite end on `{}` is not supported",
                    rel.table_name
                ))
            })?;
            if seen.insert((table.clone(), column.to_string())) {
                out.push((table.clone(), column.to_string()));
            }
        }
    }
    Ok(out)
}

/// DETACH DELETE: the relationships at each end column of `label`
/// ([`edge_ends`]), one DELETE each.
fn detach_deletes(
    schema: &GraphSchema,
    label: &str,
    source: &RowSource,
) -> Result<Vec<WriteRenderPlan>, WritePlanError> {
    Ok(edge_ends(schema, label, true)?
        .into_iter()
        .map(|((database, table), column)| {
            WriteRenderPlan::Delete(DeleteOp {
                database,
                table,
                id_column: column,
                source: source.clone(),
                relationships: true,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse_query_generator::write_to_sql::write_render_to_sql;

    fn social() -> GraphSchema {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap()
    }

    fn planned(q: &str, schema: &GraphSchema) -> Result<WriteRenderPlan, WritePlanError> {
        plan_write(q, schema).map(|p| p.plan)
    }

    fn sql(q: &str) -> Vec<String> {
        write_render_to_sql(&planned(q, &social()).unwrap())
            .into_iter()
            .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect()
    }

    /// A SET value is computed from the row it updates (its properties
    /// qualified by the table's name: an UPDATE has no alias), by the
    /// node's identities in the read part's records.
    #[test]
    fn set_reads_the_row_it_updates() {
        let got = sql("MATCH (u:User) WHERE u.user_id = 1 SET u.age = u.age + 1, u.name = 'x'");
        assert_eq!(got.len(), 1);
        assert!(
            got[0].starts_with(
                "UPDATE `test_integration`.`users_test` SET `age` = age + 1, \
                 `full_name` = 'x' WHERE `user_id` IN (SELECT __cg_id0 FROM (SELECT DISTINCT \
                 v0.user_id AS \"__cg_id0\" FROM test_integration.users_test AS v0 WHERE"
            ),
            "{got:?}"
        );
        // A property assigned twice in a clause: the last.
        let got = sql("MATCH (u:User) WHERE u.user_id = 1 SET u.age = 1, u.age = 2");
        assert!(got[0].contains("SET `age` = 2 WHERE"), "{got:?}");
        // Each clause its statement, in order (REMOVE: NULL), each reading
        // the ids its MATCH gave before any ran.
        let plan = planned(
            "MATCH (u:User) WHERE u.user_id = 1 SET u.age = 1 REMOVE u.age",
            &social(),
        )
        .unwrap();
        let WriteRenderPlan::Sequence(ops) = &plan else {
            panic!("two statements");
        };
        assert_eq!(ops.len(), 2);
        for op in ops {
            let WriteRenderPlan::Update(u) = op else {
                panic!("an UPDATE");
            };
            assert!(matches!(u.source, RowSource::Read(_)), "{u:?}");
        }
        let got: Vec<String> = write_render_to_sql(&plan);
        assert!(got[1].contains("SET `age` = NULL WHERE"), "{got:?}");
    }

    /// DETACH DELETE: each standard edge table's relationships at either
    /// end, then the node (an FK edge is the other node's row: kept). A
    /// node of several labels: one per label, its ids read first (the
    /// labels' union is a CTE, which a lightweight DELETE cannot read).
    #[test]
    fn delete_and_detach() {
        let plan = planned("MATCH (u:User {user_id: 2}) DETACH DELETE u", &social()).unwrap();
        let got: Vec<String> = write_render_to_sql(&plan)
            .iter()
            .map(|s| s.split(" WHERE ").next().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            got,
            [
                "DELETE FROM `test_integration`.`user_follows_test`",
                "DELETE FROM `test_integration`.`user_follows_test`",
                "DELETE FROM `test_integration`.`post_likes_test`",
                "DELETE FROM `test_integration`.`users_test`",
            ]
        );
        let WriteRenderPlan::Sequence(ops) = planned("MATCH (n) DELETE n", &social()).unwrap()
        else {
            panic!("one DELETE per label");
        };
        let deletes: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                WriteRenderPlan::Delete(d) => Some(d),
                _ => None,
            })
            .collect();
        assert_eq!(deletes.len(), 2);
        for d in deletes {
            assert!(matches!(d.source, RowSource::Read(_)), "{d:?}");
        }
        // Without DETACH: a node with relationships fails the statement,
        // checked in each edge table at its label before anything runs.
        let checks: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                WriteRenderPlan::Check(c) => c.within.as_ref().map(|t| (&t.table, &t.column)),
                _ => None,
            })
            .collect();
        assert!(
            checks.contains(&(&"user_follows_test".to_string(), &"followed_id".to_string())),
            "{checks:?}"
        );
        // One label, one statement: its subquery, read by the DELETE.
        let WriteRenderPlan::Sequence(ops) = planned("MATCH (u:User) DELETE u", &social()).unwrap()
        else {
            panic!("its checks and the DELETE");
        };
        let Some(WriteRenderPlan::Delete(d)) = ops.last() else {
            panic!("a DELETE last");
        };
        assert!(matches!(d.source, RowSource::Sql(_)), "{d:?}");
    }

    /// A polymorphic edge table's ends are any label: DETACH DELETE cannot
    /// tell a node's relationships apart by its id (refused); a plain
    /// DELETE checks both its end columns (another label's id too: loud).
    #[test]
    fn polymorphic_edges() {
        let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../../schemas/test/social_polymorphic.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap();
        let err = planned("MATCH (u:User {user_id: 1}) DETACH DELETE u", &schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be told apart"), "{err}");
        let WriteRenderPlan::Sequence(ops) =
            planned("MATCH (u:User {user_id: 1}) DELETE u", &schema).unwrap()
        else {
            panic!("checks and the DELETE");
        };
        let checked: Vec<&str> = ops
            .iter()
            .filter_map(|op| match op {
                WriteRenderPlan::Check(c) => c.within.as_ref().map(|t| t.column.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(checked.len(), 2, "{ops:?}");
    }

    /// CREATE: one node per record of the read part, its values read in
    /// the record; with no MATCH, one, its `VALUES`.
    #[test]
    fn create_per_record() {
        let got = sql("CREATE (u:User {user_id: 99, name: 'n' + 'x'})");
        assert_eq!(got.len(), 1);
        assert!(
            got[0].starts_with(
                "INSERT INTO `test_integration`.`users_test` (`user_id`, `full_name`) VALUES (99, "
            ),
            "{got:?}"
        );
        let got = sql(
            "MATCH (u:User) WHERE u.age > 30 CREATE (p:Post {post_id: u.user_id, title: u.name})",
        );
        assert!(
            got[0].starts_with(
                "INSERT INTO `test_integration`.`posts_test` (`post_id`, `post_title`) SELECT * FROM \
                 (SELECT v0.user_id AS \"__cg_v0\", v0.full_name AS \"__cg_v1\" FROM \
                 test_integration.users_test AS v0 WHERE"
            ),
            "{got:?}"
        );
        // Refused (Neo4j: a TypeError, or a value a snapshot would read
        // stale): a map, a node, a list holding NULL; a value reading a
        // node an earlier clause changed.
        for (q, why) in [
            ("CREATE (u:User {user_id: 7, name: {a: 1}})", "a map"),
            (
                "MATCH (a:User) CREATE (p:Post {post_id: 1, title: a})",
                "a node, relationship or path",
            ),
            (
                "CREATE (u:User {user_id: 8, name: [1, null]})",
                "holding a NULL",
            ),
            (
                "MATCH (a:User) WHERE a.user_id = 1 SET a.age = 99 \
                 CREATE (p:Post {post_id: 5, title: a.name})",
                "an earlier SET",
            ),
        ] {
            let err = planned(q, &social()).unwrap_err();
            assert!(
                matches!(&err, WritePlanError::Rejected(m) if m.contains(why)),
                "{q}: {err:?}"
            );
        }
        // A CREATE'd node read by a later clause: not yet.
        assert!(matches!(
            planned("CREATE (u:User {user_id: 1}) SET u.age = 2", &social()),
            Err(WritePlanError::Unsupported(_))
        ));
    }

    /// S13c: a RETURN after the writes re-reads the read part after them:
    /// the same records when the writes change none of them (Neo4j:
    /// `MATCH (n) WHERE n.x = 1 SET n.age = n.age + 1 RETURN n.age` returns
    /// the new ages). Refused when they may: a DELETE or CREATE, a SKIP /
    /// LIMIT, a SET of a property the read part reads (through a WITH too).
    #[test]
    fn write_then_return() {
        let planned = plan_write(
            "MATCH (u:User) WHERE u.country = 'US' SET u.age = u.age + 1 RETURN u.name, u.age",
            &social(),
        )
        .unwrap();
        let ret = planned.ret.expect("a RETURN");
        // After a CREATE: the legacy planner's.
        assert!(matches!(
            plan_write("CREATE (u:User {user_id: 5}) RETURN 1 AS x", &social()),
            Err(WritePlanError::Unsupported(_))
        ));
        assert!(
            ret.contains("FROM test_integration.users_test AS v0"),
            "{ret}"
        );
        assert!(ret.contains("v0.country = 'US'"), "{ret}");
        for (q, why) in [
            (
                "MATCH (u:User) WHERE u.age > 30 SET u.age = 1 RETURN u.name",
                "which the MATCH reads",
            ),
            (
                "MATCH (u:User {country: 'US'}) WITH u AS w SET w.country = 'x' RETURN w.name",
                "which the MATCH reads",
            ),
            (
                "MATCH (u:User) WITH u LIMIT 2 SET u.name = 'x' RETURN u.name",
                "SKIP or LIMIT",
            ),
            // Another variable can be the same node (review).
            (
                "MATCH (a:User), (b:User) WHERE b.name = 'x' SET a.name = 'y' \
                 RETURN a.user_id",
                "which the MATCH reads",
            ),
            // An undeclared property reads its column by name (review).
            (
                "MATCH (a:User) WHERE a.full_name = 'x' SET a.name = 'y' RETURN a.user_id",
                "which the MATCH reads",
            ),
            (
                "MATCH (a:User) WHERE rand() < 0.5 SET a.age = 2 RETURN count(*) AS c",
                "rand()",
            ),
        ] {
            let err = plan_write(q, &social()).unwrap_err();
            assert!(
                matches!(&err, WritePlanError::Rejected(m) if m.contains(why)),
                "{q}: {err:?}"
            );
        }
    }

    /// S13d: RETURN after DELETE reads the records before the writes; a
    /// deleted node's properties read by it: a check (Neo4j's error when it
    /// is a node); refused: DELETE with SET, a relationship after DETACH.
    #[test]
    fn delete_then_return() {
        let p = plan_write(
            "MATCH (u:User) WHERE u.user_id = 1 DELETE u RETURN count(u) AS c",
            &social(),
        )
        .unwrap();
        assert!(p.ret_before && p.ret.is_some());
        let p = plan_write(
            "MATCH (u:User) WHERE u.user_id = 1 DELETE u RETURN u.name AS n",
            &social(),
        )
        .unwrap();
        let WriteRenderPlan::Sequence(ops) = &p.plan else {
            panic!("a check and the DELETE: {:?}", p.plan);
        };
        assert!(
            ops.iter().any(|op| matches!(op, WriteRenderPlan::Check(c) if c.within.is_none() && c.message.contains("deleted in this transaction"))),
            "{ops:?}"
        );
        // Returned whole: no properties (NULL columns).
        let p = plan_write(
            "MATCH (u:User) WHERE u.user_id = 1 DELETE u RETURN u",
            &social(),
        )
        .unwrap();
        let ret = p.ret.unwrap();
        assert!(ret.contains("NULL AS \"u.name\""), "{ret}");
        for (q, why) in [
            (
                "MATCH (u:User) WHERE u.user_id = 1 SET u.age = 1 DELETE u RETURN 1 AS x",
                "a DELETE and a SET",
            ),
            (
                "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.user_id = 1 DETACH DELETE u \
                 RETURN r",
                "of a relationship",
            ),
        ] {
            let err = plan_write(q, &social()).unwrap_err();
            assert!(
                matches!(&err, WritePlanError::Rejected(m) if m.contains(why)),
                "{q}: {err:?}"
            );
        }
    }

    /// S13d: a node the RETURN reads where a deleted one's check does not
    /// see it (in a list, map, collect, path, a value through WITH /
    /// UNWIND, a pattern read after the DELETE): refused; a node of
    /// another variable read or returned: checked against the deleted.
    #[test]
    fn delete_then_return_reads_nodes_where_checked() {
        let schema = social();
        let one = "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 DETACH DELETE u";
        for tail in [
            "RETURN [u] AS l",
            "RETURN collect(u) AS l",
            "RETURN collect(f) AS l",
            "RETURN {k: u} AS m",
            "RETURN [x IN [u] | x.name] AS l",
            "RETURN u.name + '' AS n ORDER BY u",
        ] {
            let q = format!("{one} {tail}");
            let err = plan_write(&q, &schema).unwrap_err();
            assert!(
                matches!(&err, WritePlanError::Rejected(m) if m.contains("in a list, map, path or value")),
                "{q}: {err:?}"
            );
        }
        for q in [
            "MATCH p = (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 DETACH DELETE u \
             RETURN p",
            "MATCH p = (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 DETACH DELETE u \
             RETURN nodes(p) AS ns",
            "MATCH (u:User) WHERE u.user_id = 1 WITH u, [u] AS l UNWIND l AS x DELETE u \
             RETURN x.name AS n",
            "MATCH (u:User) WHERE u.user_id = 1 WITH u, collect(u) AS l DELETE u \
             RETURN size(l) AS n",
        ] {
            let err = plan_write(q, &schema).unwrap_err();
            assert!(
                matches!(&err, WritePlanError::Rejected(m) if m.contains("in a list, map, path or value")),
                "{q}: {err:?}"
            );
        }
        let q = "MATCH (u:User) WHERE u.user_id = 1 DELETE u RETURN size((u)-->()) AS n";
        let err = plan_write(q, &schema).unwrap_err();
        assert!(
            matches!(&err, WritePlanError::Rejected(m) if m.contains("of a pattern")),
            "{q}: {err:?}"
        );
        // Read where checked: no check for its identity, `count`, IS NULL.
        let p = plan_write(
            "MATCH (u:User) WHERE u.user_id = 1 DELETE u \
             RETURN id(u) AS i, u IS NULL AS z, count(u) AS c",
            &schema,
        )
        .unwrap();
        let WriteRenderPlan::Sequence(ops) = &p.plan else {
            panic!("{:?}", p.plan);
        };
        assert!(
            !ops.iter()
                .any(|op| matches!(op, WriteRenderPlan::Check(c) if c.message.contains("RETURN"))),
            "{ops:?}"
        );
        // Another variable's node: checked against the deleted ones.
        for (tail, message) in [
            ("RETURN f.name AS n", "read by its RETURN, as `f`"),
            ("RETURN f", "returned as `f`"),
        ] {
            let q = format!("{one} {tail}");
            let p = plan_write(&q, &schema).unwrap();
            let WriteRenderPlan::Sequence(ops) = &p.plan else {
                panic!("{q}: {:?}", p.plan);
            };
            let check = ops.iter().find_map(|op| match op {
                WriteRenderPlan::Check(c) if c.message.contains(message) => Some(c),
                _ => None,
            });
            let Some(check) = check else {
                panic!("{q}: {ops:?}");
            };
            let (RowSource::Sql(sql) | RowSource::Read(sql)) = &check.source else {
                panic!("{q}: {check:?}");
            };
            assert!(sql.contains("FROM (SELECT"), "{sql}");
            assert!(sql.contains("__cg_these WHERE"), "{sql}");
        }
    }

    #[test]
    fn refusals() {
        let schema = social();
        for (q, why) in [
            (
                "MATCH (u:User)-[:FOLLOWS]->(f:User) SET u.name = f.name",
                "another variable",
            ),
            (
                "MATCH (u:User)-[r:FOLLOWS]->(f:User) DELETE r",
                "relationship",
            ),
            ("MATCH (u:User) SET u.nope_xyz = 1", "writable column"),
        ] {
            let err = planned(q, &schema).unwrap_err().to_string();
            assert!(err.contains(why), "{q}: {err}");
        }
        // Not planned here yet (the legacy planner's): a relationship
        // CREATE.
        let q = "CREATE (a:User {user_id: 1})-[:FOLLOWS]->(b:User {user_id: 2})";
        assert!(
            matches!(planned(q, &schema), Err(WritePlanError::Unsupported(_))),
            "{q}"
        );
    }
}
