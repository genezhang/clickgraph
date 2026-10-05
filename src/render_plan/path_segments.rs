//! #1202: a path variable over a CTE-backed variable-length path PLUS fixed hops
//! (`MATCH p=(c)-[:R]->(a)-[:R*1..2]->(b)`).
//!
//! The VLP's recursive CTE only knows its OWN hops, so every path function derived from the CTE
//! (`t.hop_count`, `t.path_nodes`, `t.path_relationships`) describes the variable-length part
//! alone: `length(p)` came out short by the number of fixed hops, and `nodes(p)` /
//! `relationships(p)` / a bare `p` silently dropped the hop's nodes and edge.
//!
//! This pass runs once on the OUTERMOST plan:
//! - `length(p)` over exactly one path plus `k` fixed single hops is `hop_count + k`: `k` is
//!   registered in the query context and added by the two path-function rewriters;
//! - every other use of such a path variable — `nodes(p)`, `relationships(p)`, a bare `p`, a
//!   second variable-length segment, a multi-hop segment — cannot be answered and is refused.

use std::collections::{BTreeMap, BTreeSet};

use crate::query_planner::logical_expr::visitors::{walk_expression, ExpressionVisitor};
use crate::query_planner::logical_expr::{LogicalExpr, ScalarFnCall};
use crate::query_planner::logical_plan::{GraphRel, LogicalPlan};
use crate::render_plan::errors::RenderBuildError;

#[derive(Default)]
struct PathShape {
    /// Distinct relationship aliases of CTE-backed variable-length segments.
    vlps: BTreeSet<String>,
    /// Distinct relationship aliases of plain single-hop segments.
    hops: BTreeSet<String>,
    /// A segment that is neither (a fixed-length `*N`, ...): hop count unknown to this pass.
    other_segments: bool,
}

/// How an expression refers to one path variable.
#[derive(Default)]
struct PathUse {
    /// `length(p)` calls.
    length_calls: usize,
    /// Every other path function over `p` (`nodes(p)`, `relationships(p)`, `cost(p)`, ...).
    other_calls: usize,
    /// All references to the bare alias `p` (a `length(p)` call contributes one).
    alias_refs: usize,
}

struct UseScan<'a> {
    path_var: &'a str,
    out: PathUse,
}

impl ExpressionVisitor for UseScan<'_> {
    type Output = ();

    fn visit_scalar_fn(&mut self, f: &ScalarFnCall) {
        let on_path =
            matches!(f.args.as_slice(), [LogicalExpr::TableAlias(a)] if a.0 == self.path_var);
        if on_path {
            if f.name.eq_ignore_ascii_case("length") {
                self.out.length_calls += 1;
            } else {
                self.out.other_calls += 1;
            }
        }
    }

    fn visit_table_alias(&mut self, alias: &str) {
        if alias == self.path_var {
            self.out.alias_refs += 1;
        }
    }

    fn visit_property_access(&mut self, prop: &crate::query_planner::logical_expr::PropertyAccess) {
        // `p.<anything>` is a use the path functions cannot express
        if prop.table_alias.0 == self.path_var {
            self.out.other_calls += 1;
        }
    }
}

fn scan_use(expr: &LogicalExpr, path_var: &str, out: &mut PathUse) {
    let mut scan = UseScan {
        path_var,
        out: PathUse::default(),
    };
    walk_expression(expr, &mut scan);
    out.length_calls += scan.out.length_calls;
    out.other_calls += scan.out.other_calls;
    out.alias_refs += scan.out.alias_refs;
}

/// One path-variable scope: the GraphRels and expressions that can see each other. A `WITH`
/// starts a new scope (its input pattern), and so does every branch of a `UNION` — the same
/// path-variable name in two of them is two different paths.
#[derive(Default)]
struct Scope<'a> {
    rels: Vec<&'a GraphRel>,
    exprs: Vec<&'a LogicalExpr>,
}

fn collect_scopes<'a>(plan: &'a LogicalPlan, scope: &mut Scope<'a>, done: &mut Vec<Scope<'a>>) {
    match plan {
        LogicalPlan::Projection(p) => scope.exprs.extend(p.items.iter().map(|i| &i.expression)),
        LogicalPlan::Filter(f) => scope.exprs.push(&f.predicate),
        LogicalPlan::OrderBy(ob) => scope.exprs.extend(ob.items.iter().map(|i| &i.expression)),
        LogicalPlan::GroupBy(g) => {
            scope.exprs.extend(g.expressions.iter());
            scope.exprs.extend(g.having_clause.iter());
        }
        LogicalPlan::Unwind(u) => scope.exprs.push(&u.expression),
        LogicalPlan::GraphRel(gr) => {
            scope.rels.push(gr);
            scope.exprs.extend(gr.where_predicate.iter());
        }
        LogicalPlan::WithClause(w) => {
            // the WITH's own projection reads its INPUT scope; what is above it is another scope
            let mut inner = Scope::default();
            inner.exprs.extend(w.items.iter().map(|i| &i.expression));
            if let Some(ob) = &w.order_by {
                inner.exprs.extend(ob.iter().map(|i| &i.expression));
            }
            inner.exprs.extend(w.where_clause.iter());
            collect_scopes(&w.input, &mut inner, done);
            done.push(inner);
            return;
        }
        LogicalPlan::Union(_) => {
            for child in plan.children() {
                let mut branch = Scope::default();
                collect_scopes(child, &mut branch, done);
                done.push(branch);
            }
            return;
        }
        _ => {}
    }
    for child in plan.children() {
        collect_scopes(child, scope, done);
    }
}

fn classify(gr: &GraphRel, shape: &mut PathShape) {
    match &gr.variable_length {
        None => {
            shape.hops.insert(gr.alias.clone());
        }
        Some(_) if !crate::render_plan::from_builder::is_fixed_length_vlp(gr) => {
            shape.vlps.insert(gr.alias.clone());
        }
        Some(_) => shape.other_segments = true,
    }
}

/// Run the pass on the outermost plan: register `length(p)`'s fixed-hop offset and refuse the
/// path-variable uses a composite path cannot answer.
pub(crate) fn register_composite_paths(root: &LogicalPlan) -> Result<(), RenderBuildError> {
    crate::server::query_context::clear_path_fixed_hops();

    let mut done: Vec<Scope> = Vec::new();
    let mut top = Scope::default();
    collect_scopes(root, &mut top, &mut done);
    done.push(top);

    // path variable → the scopes that bind it
    let mut bound_in: BTreeMap<String, usize> = BTreeMap::new();
    for scope in &done {
        let vars: BTreeSet<&str> = scope
            .rels
            .iter()
            .filter_map(|g| g.path_variable.as_deref())
            .collect();
        for v in vars {
            *bound_in.entry(v.to_string()).or_default() += 1;
        }
    }

    let mut registered: BTreeMap<String, usize> = BTreeMap::new();
    for scope in &done {
        let mut shapes: BTreeMap<String, PathShape> = BTreeMap::new();
        for gr in &scope.rels {
            if let Some(pv) = gr.path_variable.as_deref() {
                classify(gr, shapes.entry(pv.to_string()).or_default());
            }
        }
        for (path_var, shape) in shapes {
            // A plain path (one VLP, nothing else) or a path with no VLP at all is not this
            // pass's business: those render as before.
            let composite = !shape.vlps.is_empty()
                && (shape.vlps.len() > 1 || !shape.hops.is_empty() || shape.other_segments);
            if !composite {
                continue;
            }
            let mut uses = PathUse::default();
            for e in &scope.exprs {
                scan_use(e, &path_var, &mut uses);
            }
            let bare_refs = uses
                .alias_refs
                .saturating_sub(uses.length_calls + uses.other_calls);
            let supported_length = shape.vlps.len() == 1 && !shape.other_segments;
            // the offset is keyed by NAME: the same name bound in another scope is ambiguous
            let ambiguous = uses.length_calls > 0 && bound_in.get(&path_var).copied() > Some(1);
            if uses.other_calls == 0
                && bare_refs == 0
                && !ambiguous
                && (uses.length_calls == 0 || supported_length)
            {
                if uses.length_calls > 0 {
                    registered.insert(path_var.clone(), shape.hops.len());
                }
                continue;
            }
            return Err(RenderBuildError::UnsupportedFeature(format!(
                "the path variable `{path_var}` spans a variable-length part plus other hops or \
                 paths, and only `length({path_var})` over ONE variable-length part plus fixed \
                 single hops can be answered (#1202, #1210): the recursive CTE describes its own \
                 hops alone, so `nodes({path_var})`, `relationships({path_var})`, `{path_var}` \
                 itself and a second variable-length segment would silently omit the rest of the \
                 path (and the same name bound in another scope is ambiguous)."
            )));
        }
    }
    for (path_var, k) in registered {
        crate::server::query_context::register_path_fixed_hops(&path_var, k);
    }
    Ok(())
}

/// #1155: an UNDIRECTED variable-length path that was split into two monotone UNION arms (the
/// legacy strategy, used where the single doubled-edge walk does not apply — the denormalized
/// layout) and is CHAINED to another hop in the same arm.
///
/// The arms are rendered without the analyzer's join plan, so the chained hop pulls the path's own
/// `GraphRel` into the outer query as a plain single hop; the path's CTE is then unreferenced and
/// dead-CTE elimination drops it. The query silently ran as ONE hop plus the chained hop whatever
/// the bound (`*1..1`, `*1..2` and `*1..3` all returned the same rows), ~3x off an independent
/// oracle. Refused loudly until the single-walk strategy covers that layout.
pub(crate) fn refuse_chained_split_undirected_vlp(
    root: &LogicalPlan,
) -> Result<(), RenderBuildError> {
    let Some(schema) = crate::server::query_context::get_current_schema() else {
        return Ok(());
    };
    let mut offender: Option<String> = None;
    root.any_node(|n| {
        let LogicalPlan::Union(u) = n else {
            return false;
        };
        for arm in &u.inputs {
            let mut rels: Vec<&GraphRel> = Vec::new();
            collect_arm_rels(arm, &mut rels);
            let split_vlp = rels.iter().find(|g| {
                g.variable_length.is_some()
                    && !crate::render_plan::from_builder::is_fixed_length_vlp(g)
                    && g.was_undirected == Some(true)
                    && g.shortest_path_mode.is_none()
                    && !crate::query_planner::analyzer::bidirectional_union::undirected_vlp_single_walk_core(g, &schema)
            });
            if let Some(vlp) = split_vlp {
                if rels.iter().any(|g| g.alias != vlp.alias) {
                    offender = Some(vlp.alias.clone());
                    return true;
                }
            }
        }
        false
    });
    match offender {
        Some(alias) => Err(RenderBuildError::UnsupportedFeature(format!(
            "an undirected variable-length path (`{alias}`) chained to another hop is not \
             supported on this schema layout (#1155): the two-direction split renders the chained \
             hop without the path's recursive CTE, so the path bound would be silently ignored. \
             Write the path directed, or split the query."
        ))),
        None => Ok(()),
    }
}

/// #1233: a REQUIRED undirected hop (the legacy two-arm split) next to a CTE-backed variable-length
/// path is answered correctly only when the hop-vs-path edge guard (`NOT has(path_edges, hop edge)`,
/// `hop_vlp_uniqueness`) is emitted for every hop of the arm — the path's CTE tracks only its own
/// edges, so without the guard the path may walk back over the hop's edge and the count is silently
/// too high (271 where 204 is right). The guard's verified shape excludes WITH scopes, comma patterns,
/// a second path, shortestPath, optional hops/paths and other layouts; those arms are refused loudly
/// instead of answered wrongly.
pub(crate) fn refuse_unguarded_undirected_hop_next_to_path(
    root: &LogicalPlan,
) -> Result<(), RenderBuildError> {
    let mut offender: Option<String> = None;
    root.any_node(|n| {
        let LogicalPlan::Union(u) = n else {
            return false;
        };
        for arm in &u.inputs {
            let mut rels: Vec<&GraphRel> = Vec::new();
            collect_arm_rels(arm, &mut rels);
            let is_path = |g: &&GraphRel| {
                g.variable_length.is_some()
                    && !crate::render_plan::from_builder::is_fixed_length_vlp(g)
            };
            // Cypher's relationship uniqueness spans ONE `MATCH` clause: a hop and a path in
            // different clauses may share an edge, so no guard is owed (nor emitted).
            let path_clauses: Vec<_> = rels
                .iter()
                .filter(|g| is_path(g))
                .map(|g| g.match_clause_index)
                .collect();
            let split_hop = rels.iter().find(|g| {
                !is_path(g)
                    && g.was_undirected == Some(true)
                    && !g.is_optional.unwrap_or(false)
                    && path_clauses.contains(&g.match_clause_index)
            });
            if let Some(hop) = split_hop {
                let hops = rels.iter().filter(|g| !is_path(g)).count();
                if super::hop_vlp_uniqueness::guards(arm).len() != hops {
                    offender = Some(hop.alias.clone());
                    return true;
                }
            }
        }
        false
    });
    match offender {
        Some(alias) => Err(RenderBuildError::UnsupportedFeature(format!(
            "an undirected hop (`{alias}`) next to a variable-length path is not supported in this \
             shape (#1233, #1203): the path's recursive CTE tracks only its own edges, so the hop's \
             edge could be reused by the path and the count would be silently too high. Write the \
             hop with an explicit direction, or match it in a pattern with no WITH / comma part / \
             second path."
        ))),
        None => Ok(()),
    }
}

/// GraphRels of one UNION arm, not descending into a nested UNION.
fn collect_arm_rels<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a GraphRel>) {
    match plan {
        LogicalPlan::Union(_) => {}
        LogicalPlan::GraphRel(gr) => {
            out.push(gr);
            for child in plan.children() {
                collect_arm_rels(child, out);
            }
        }
        _ => {
            for child in plan.children() {
                collect_arm_rels(child, out);
            }
        }
    }
}
