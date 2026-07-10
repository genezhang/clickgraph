//! Variable-Length Path Transitivity Check
//!
//! Validates that variable-length path patterns are semantically meaningful.
//!
//! A VLP pattern `(a)-[r:TYPE*]->(b)` can only recurse if the relationship
//! is transitive, meaning the TO node can also be a FROM node for the same
//! relationship type.
//!
//! Example:
//!   ✓ Valid:   (Person)-[KNOWS*]->(Person)  - Person can KNOW another Person
//!   ✗ Invalid: (IP)-[DNS_REQUESTED*]->(Domain) - Domain cannot DNS_REQUEST anything
//!
//! For non-transitive patterns, this pass converts them to fixed-length (min_hops only):
//!   `(a)-[r:TYPE*]->(b)` → `(a)-[r:TYPE*1]->(b)` (exactly 1 hop)
//!   `(a)-[r:TYPE*2..]->(b)` → Semantic error (impossible, min_hops > 1 but non-transitive)

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        logical_plan::{GraphRel, LogicalPlan, VariableLengthSpec},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

use super::{
    analyzer_pass::{AnalyzerPass, AnalyzerResult},
    errors::AnalyzerError,
};

pub struct VlpTransitivityCheck;

impl VlpTransitivityCheck {
    pub fn new() -> Self {
        Self
    }

    /// Extract the base type name from a potentially composite key
    /// "KNOWS::Person::Person" -> "KNOWS"
    /// "KNOWS" -> "KNOWS"
    fn extract_type_name(key: &str) -> &str {
        // Composite keys have format "TYPE::FROM::TO"
        // Split by "::" and take the first part
        key.split("::").next().unwrap_or(key)
    }

    /// Check if a relationship can be transitive (recursive)
    /// Returns true if the TO node can also be a FROM node for the same relationship type
    fn is_transitive_relationship(
        rel_type: &str,
        schema: &GraphSchema,
    ) -> Result<bool, AnalyzerError> {
        // Extract base type name from potentially composite key
        // "KNOWS::Person::Person" -> "KNOWS"
        let base_type = Self::extract_type_name(rel_type);

        // Get all relationship schemas for this type
        let rel_schemas = schema.rel_schemas_for_type(base_type);

        if rel_schemas.is_empty() {
            return Err(AnalyzerError::RelationshipTypeNotFound(
                rel_type.to_string(),
            ));
        }

        // Polymorphic edges with $any from/to and no label_values are
        // type-agnostic — they can connect any node types, so transitivity
        // depends on the query's actual node types, not the schema.
        // Treat them as transitive to allow VLP recursive CTEs.
        for rel_schema in &rel_schemas {
            let from_is_any =
                rel_schema.from_node == "$any" && rel_schema.from_label_values.is_none();
            let to_is_any = rel_schema.to_node == "$any" && rel_schema.to_label_values.is_none();
            if from_is_any && to_is_any {
                log::info!(
                    "✓ VLP transitivity: '{}' is transitive (polymorphic $any → $any)",
                    rel_type
                );
                return Ok(true);
            }
        }

        // Check if ANY variant of this relationship type allows transitivity
        // A relationship is transitive if:
        // 1. from_node == to_node (self-loop like Person-KNOWS->Person), OR
        // 2. The to_node of one variant can be the from_node of another variant
        // 3. For polymorphic relationships: check if any to_label_value overlaps with from_label_values

        // Collect all (from_node, to_node) pairs for this relationship type
        // For polymorphic relationships, use from_label_values and to_label_values
        let mut from_nodes = std::collections::HashSet::new();
        let mut to_nodes = std::collections::HashSet::new();

        for rel_schema in &rel_schemas {
            // For polymorphic FROM side, use from_label_values if available
            if let Some(ref values) = rel_schema.from_label_values {
                for v in values {
                    from_nodes.insert(v.clone());
                }
            } else if rel_schema.from_node != "$any" {
                from_nodes.insert(rel_schema.from_node.clone());
            }

            // For polymorphic TO side, use to_label_values if available
            if let Some(ref values) = rel_schema.to_label_values {
                for v in values {
                    to_nodes.insert(v.clone());
                }
            } else if rel_schema.to_node != "$any" {
                to_nodes.insert(rel_schema.to_node.clone());
            }

            // Check for self-loop (from == to) - with polymorphic support
            let from_set: std::collections::HashSet<_> = rel_schema
                .from_label_values
                .as_ref()
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_else(|| {
                    let mut s = std::collections::HashSet::new();
                    if rel_schema.from_node != "$any" {
                        s.insert(rel_schema.from_node.clone());
                    }
                    s
                });
            let to_set: std::collections::HashSet<_> = rel_schema
                .to_label_values
                .as_ref()
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_else(|| {
                    let mut s = std::collections::HashSet::new();
                    if rel_schema.to_node != "$any" {
                        s.insert(rel_schema.to_node.clone());
                    }
                    s
                });

            // If any value is in both from and to, it's a self-loop (transitive)
            if from_set.intersection(&to_set).next().is_some() {
                log::info!(
                    "✓ VLP transitivity: '{}' is transitive (self-loop found in from={:?}, to={:?})",
                    rel_type,
                    from_set,
                    to_set
                );
                return Ok(true);
            }
        }

        // Check if any to_node can also be a from_node (allows chaining)
        let can_chain = to_nodes.iter().any(|to| from_nodes.contains(to));

        if can_chain {
            log::info!(
                "✓ VLP transitivity: '{}' is transitive (to_nodes {:?} overlap with from_nodes {:?})",
                rel_type,
                to_nodes,
                from_nodes
            );
        } else {
            log::warn!(
                "⚠ VLP transitivity: '{}' is NON-transitive! from_nodes: {:?}, to_nodes: {:?}. Converting to fixed-length.",
                rel_type,
                from_nodes,
                to_nodes
            );
        }

        Ok(can_chain)
    }

    /// Check if non-transitive VLP should error (min_hops > 1)
    fn validate_non_transitive(
        vlp_spec: &VariableLengthSpec,
        rel_type: &str,
    ) -> Result<(), AnalyzerError> {
        // If min_hops is Some and > 1, this is a semantic error
        // You can't have a path of length 2+ if the relationship is non-transitive!
        if let Some(min) = vlp_spec.min_hops {
            if min > 1 {
                return Err(AnalyzerError::InvalidPlan(
                    format!(
                        "Variable-length path pattern [{}*{}..] is semantically invalid: \
                         relationship '{}' is non-transitive (cannot recurse). \
                         The TO node never appears as a FROM node, so paths longer than 1 hop are impossible.",
                        rel_type,
                        min,
                        rel_type
                    )
                ));
            }
        }
        Ok(())
    }
}

impl AnalyzerPass for VlpTransitivityCheck {
    fn analyze_with_graph_schema(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let result = self.check_transitivity_recursive(plan, plan_ctx, graph_schema)?;

        // #499: comma-separated multi-pattern MATCH (`MATCH p1=..., p2=...`)
        // plans as independent GraphRel subtrees joined by CartesianProduct.
        // When a pattern's VLP is non-transitive it's clamped to a fixed hop
        // above (now reachable in every branch thanks to the CartesianProduct
        // arm) and renders fine via the ordinary fixed-path route (verified
        // live). But when at least one independent branch still needs a
        // genuine recursive VLP CTE, the FROM/CTE-trigger machinery
        // (`find_vlp_graph_rel` in from_builder.rs, `vlp_overrides_from` in
        // join_builder.rs) doesn't know how to combine a VLP CTE with a
        // SIBLING CartesianProduct branch at all — verified live, even with
        // just ONE VLP branch + one already-fixed sibling
        // (`MATCH p1=(a)-[:A*1..2]->(b), p2=(c)-[:B]->(d) RETURN p1, p2`): the
        // VLP branch's entire FROM/JOIN vanishes (not merely a missing JOIN —
        // the WHOLE pattern), while `p1` still renders as
        // `tuple(t.path_nodes, ...)` referencing a never-generated CTE alias
        // `t` (ClickHouse Code 47). Two-or-more VLP-needing branches hit the
        // same wall via a different symptom (one CTE referenced twice).
        // Supporting N independent VLP CTEs coexisting with CartesianProduct
        // siblings (each own name/alias, its own FROM/JOIN + its own
        // RETURN-side gating) is a real rendering feature, not a clamp fix —
        // out of scope here. Fail loudly instead of emitting broken SQL for
        // EITHER shape: any CartesianProduct with at least one VLP-needing
        // branch, not just >1.
        let plan_ref: &Arc<LogicalPlan> = match &result {
            Transformed::Yes(p) | Transformed::No(p) => p,
        };
        let vlp_branch_count = count_cartesian_branches_needing_vlp_cte(plan_ref.as_ref());
        let has_cartesian_product = plan_contains_cartesian_product(plan_ref.as_ref());
        if vlp_branch_count > 1 || (has_cartesian_product && vlp_branch_count >= 1) {
            return Err(AnalyzerError::InvalidPlan(
                "A variable-length path pattern combined with another independent \
                 pattern in one MATCH (comma-separated patterns or multiple path \
                 variables, e.g. `MATCH p1 = (a)-[:A*1..3]->(b), p2 = (c)-[:B]->(d) \
                 ...`) is not yet supported when at least one pattern still needs its \
                 own recursive VLP CTE: only one variable-length recursive CTE can be \
                 generated per query today, and it cannot yet coexist with an \
                 independent sibling pattern. Split the independent patterns into \
                 separate queries. (tracked: #499)"
                    .to_string(),
            ));
        }

        Ok(result)
    }
}

/// Whether a CartesianProduct (comma-separated MATCH / multiple independent
/// patterns) appears anywhere in the plan.
fn plan_contains_cartesian_product(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CartesianProduct(_) => true,
        LogicalPlan::Projection(p) => plan_contains_cartesian_product(&p.input),
        LogicalPlan::Filter(f) => plan_contains_cartesian_product(&f.input),
        LogicalPlan::GraphJoins(gj) => plan_contains_cartesian_product(&gj.input),
        LogicalPlan::GroupBy(gb) => plan_contains_cartesian_product(&gb.input),
        LogicalPlan::OrderBy(ob) => plan_contains_cartesian_product(&ob.input),
        LogicalPlan::Limit(l) => plan_contains_cartesian_product(&l.input),
        LogicalPlan::Skip(s) => plan_contains_cartesian_product(&s.input),
        _ => false,
    }
}

/// Count how many independent branches of a CartesianProduct (comma-separated
/// MATCH / multiple path variables) still need their own recursive VLP CTE
/// after transitivity clamping. See #499 doc comment above for why any
/// nonzero count combined with a CartesianProduct is unsupported today.
fn count_cartesian_branches_needing_vlp_cte(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::CartesianProduct(cp) => {
            count_cartesian_branches_needing_vlp_cte(&cp.left)
                + count_cartesian_branches_needing_vlp_cte(&cp.right)
        }
        LogicalPlan::Projection(p) => count_cartesian_branches_needing_vlp_cte(&p.input),
        LogicalPlan::Filter(f) => count_cartesian_branches_needing_vlp_cte(&f.input),
        LogicalPlan::GraphJoins(gj) => count_cartesian_branches_needing_vlp_cte(&gj.input),
        LogicalPlan::GroupBy(gb) => count_cartesian_branches_needing_vlp_cte(&gb.input),
        LogicalPlan::OrderBy(ob) => count_cartesian_branches_needing_vlp_cte(&ob.input),
        LogicalPlan::Limit(l) => count_cartesian_branches_needing_vlp_cte(&l.input),
        LogicalPlan::Skip(s) => count_cartesian_branches_needing_vlp_cte(&s.input),
        other => usize::from(subtree_has_uncapped_vlp(other)),
    }
}

/// Whether a (non-CartesianProduct) plan subtree still contains a GraphRel
/// requiring a genuine recursive VLP CTE (i.e. survived transitivity clamping).
fn subtree_has_uncapped_vlp(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            gr.variable_length.is_some()
                || subtree_has_uncapped_vlp(&gr.left)
                || subtree_has_uncapped_vlp(&gr.right)
        }
        LogicalPlan::GraphNode(gn) => subtree_has_uncapped_vlp(&gn.input),
        LogicalPlan::Filter(f) => subtree_has_uncapped_vlp(&f.input),
        _ => false,
    }
}

impl VlpTransitivityCheck {
    fn check_transitivity_recursive(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::GraphRel(rel) => {
                log::info!(
                    "🔍 VLP Transitivity Check: Found GraphRel, variable_length={:?}",
                    rel.variable_length
                );
                // Check if this is a variable-length path
                if let Some(ref vlp_spec) = rel.variable_length {
                    log::info!("✓ VLP Transitivity Check: Has VLP spec: {:?}", vlp_spec);
                    // Only check unbounded or multi-hop patterns
                    // Single fixed-hop patterns (e.g., *1) don't need transitivity
                    let needs_transitivity = match (vlp_spec.min_hops, vlp_spec.max_hops) {
                        (Some(min), Some(max)) if min == max && min == 1 => false, // *1 is fine
                        _ => true, // *, *2, *1.., *2..5, etc. all need transitivity check
                    };

                    log::info!(
                        "🔍 VLP Transitivity Check: needs_transitivity={}",
                        needs_transitivity
                    );
                    if needs_transitivity {
                        log::info!("🔍 VLP Transitivity Check: Checking transitivity...");

                        // 🔧 FIX: Shortest path queries don't require explicit relationship types
                        // They can traverse any relationship, and the schema will be resolved dynamically
                        if rel.shortest_path_mode.is_some() {
                            log::info!("🔧 VLP: Shortest path query - skipping relationship type requirement");
                            // For shortest path, we don't enforce relationship type requirement
                            // The query will traverse all available relationships
                            return Ok(Transformed::No(plan));
                        }

                        // Get relationship type(s) - required for non-shortest-path VLP
                        let rel_types = rel.labels.as_ref().ok_or_else(|| {
                            AnalyzerError::InvalidPlan(
                                "Variable-length path missing relationship type".to_string(),
                            )
                        })?;
                        log::info!("🔍 VLP Transitivity Check: rel_types={:?}", rel_types);

                        // For simplicity, check the first relationship type
                        // TODO: Handle multiple types (TYPE1|TYPE2)
                        let rel_type = rel_types.first().ok_or_else(|| {
                            AnalyzerError::InvalidPlan(
                                "Variable-length path has empty relationship type list".to_string(),
                            )
                        })?;

                        // Check if this relationship is transitive. This check
                        // is inherently direction-agnostic (it only looks at
                        // schema from/to label overlap), which is correct: a
                        // relationship is either self-loop/overlapping (safe to
                        // chain regardless of direction) or it isn't.
                        log::info!(
                            "⚠ VLP Transitivity Check: Checking if '{}' is transitive...",
                            rel_type
                        );
                        let is_transitive =
                            Self::is_transitive_relationship(rel_type, graph_schema)?;
                        log::info!(
                            "⚠ VLP transitivity: '{}' is {}!",
                            rel_type,
                            if is_transitive {
                                "TRANSITIVE"
                            } else {
                                "NON-TRANSITIVE"
                            }
                        );

                        if !is_transitive {
                            let is_undirected = rel.direction
                                == crate::query_planner::logical_expr::Direction::Either;
                            let is_zero_hop = vlp_spec.min_hops == Some(0);

                            // #496: two shapes need MORE than a single fixed
                            // hop to be correct, and both require the SQL
                            // renderer to reconstruct a path across the
                            // relationship's two DIFFERENT node tables (a
                            // non-transitive relationship is by construction
                            // heterogeneous — see `is_transitive_relationship`'s
                            // self-loop check above: any from/to label overlap
                            // already returns transitive=true, so every case
                            // reaching here has genuinely different node types
                            // on each end):
                            //
                            //   - undirected (`-[..]-`): reverse-direction
                            //     chaining can make >1-hop paths real (e.g.
                            //     Order-PLACED_BY->Customer<-PLACED_BY-Order),
                            //     so simply clamping to 1 hop drops rows.
                            //   - `*0..N`: the zero-hop row (start node
                            //     standing in as its own path) is real and
                            //     must not be dropped by clamping straight to
                            //     a required 1-hop join.
                            //
                            // Verified empirically (2026-07, live SQL inspection
                            // against fk_edge/standard schemas) that simply
                            // *not* clamping and routing these through the
                            // existing recursive-VLP-CTE machinery does NOT
                            // work: both the zero-hop base case and the
                            // recursive/base-case JOIN generators in
                            // variable_length_cte.rs hard-assume the start and
                            // end node tables are the SAME (a safe assumption
                            // for every case they've been exercised on so far,
                            // since genuinely transitive relationships are
                            // always self-loop/overlapping by definition) —
                            // e.g. `MATCH (o:Order)-[:PLACED_BY*0..2]->(c)
                            // RETURN count(*)` produced a recursive term that
                            // joined `orders_fk` to itself and never touched
                            // `customers_fk`; the undirected 1..2 case produced
                            // a base-case join predicate comparing Order's own
                            // id column to Customer's id column. Both are
                            // syntactically-valid-but-semantically-wrong SQL —
                            // i.e. exactly the silent-wrong-results failure
                            // mode ground rule (1) forbids, just relocated.
                            // Extending that machinery to support heterogeneous
                            // start/end tables across every schema pattern
                            // (standard/fk_edge/denormalized/polymorphic) is a
                            // real feature, not a clamp fix — out of scope
                            // here. Fail LOUDLY instead: strictly better than
                            // both the old silent clamp and the silently-wrong
                            // SQL the naive "don't clamp" fix produces.
                            if is_undirected || is_zero_hop {
                                return Err(AnalyzerError::InvalidPlan(format!(
                                    "Variable-length path pattern [{}*{}..{}] is not yet \
                                     supported: relationship '{}' is non-transitive (its FROM \
                                     and TO node types differ), and this pattern requires {} \
                                     across those two different node tables. This needs the SQL \
                                     renderer to reconstruct paths across heterogeneous node \
                                     tables, which is not yet implemented (tracked: #496). A \
                                     single fixed-hop pattern (e.g. `[{}]` without `*`) is \
                                     supported.",
                                    rel_type,
                                    vlp_spec.min_hops.map(|m| m.to_string()).unwrap_or_default(),
                                    vlp_spec.max_hops.map(|m| m.to_string()).unwrap_or_default(),
                                    rel_type,
                                    if is_undirected {
                                        "alternating-direction chaining"
                                    } else {
                                        "a zero-hop (start-node-as-both-endpoints) row"
                                    },
                                    rel_type,
                                )));
                            }

                            // Remaining case: DIRECTED, effective min_hops == 1
                            // (Cypher defaults an unspecified min to 1). Here
                            // the clamp is semantically exact — chaining past 1
                            // hop is impossible (non-transitive) and the
                            // zero-hop shape doesn't apply (min >= 1) — so
                            // remove variable_length entirely and become a
                            // simple single-hop edge, exactly like before.
                            //
                            // Validate first: min_hops > 1 is a hard semantic
                            // error (a path of length 2+ is impossible for a
                            // non-transitive relationship).
                            Self::validate_non_transitive(vlp_spec, rel_type)?;

                            log::info!(
                                "→ Removing VLP from non-transitive [{}*] - converting to simple single-hop pattern",
                                rel_type
                            );

                            // #488: keep the path-variable registration
                            // consistent with the rewritten plan. `MATCH p =
                            // ...` registered the path with the VLP's length
                            // bounds; if those stay in place the renderer
                            // emits tuple(t.path_nodes, ...) referencing a
                            // recursive VLP CTE that is never generated for
                            // this (now single-hop) pattern — unbound alias
                            // `t`, ClickHouse Code 47. Re-register as a fixed
                            // single-hop path so rendering takes the
                            // fixed-path route.
                            //
                            // #496: this arm is only reached for DIRECTED
                            // patterns with effective min_hops == 1 (undirected
                            // and zero-hop shapes are handled above and never
                            // fall through to here), so the clamp is always
                            // sound at this point — no extra guard needed
                            // beyond "was this path variable actually
                            // registered for this relationship".
                            if let Some(ref pvar) = rel.path_variable {
                                let registered_for_this_rel = plan_ctx
                                    .lookup_variable(pvar)
                                    .and_then(|v| v.as_path())
                                    .is_some_and(|p| {
                                        p.relationship.as_deref() == Some(rel.alias.as_str())
                                            && !p.is_shortest_path
                                    });
                                if registered_for_this_rel {
                                    log::info!(
                                        "→ Re-registering path variable '{}' as fixed single-hop (VLP stripped)",
                                        pvar
                                    );
                                    plan_ctx.define_path(
                                        pvar.clone(),
                                        Some(rel.left_connection.clone()),
                                        Some(rel.right_connection.clone()),
                                        Some(rel.alias.clone()),
                                        None,
                                        false,
                                    );
                                }
                            }

                            // Create new GraphRel WITHOUT variable_length
                            let new_rel = GraphRel {
                                variable_length: None, // Remove VLP - just a normal edge
                                ..rel.clone()
                            };

                            return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))));
                        }
                    }
                }

                // Recursively check child nodes
                let left_result =
                    self.check_transitivity_recursive(rel.left.clone(), plan_ctx, graph_schema)?;
                let center_result =
                    self.check_transitivity_recursive(rel.center.clone(), plan_ctx, graph_schema)?;
                let right_result =
                    self.check_transitivity_recursive(rel.right.clone(), plan_ctx, graph_schema)?;

                if left_result.is_yes() || center_result.is_yes() || right_result.is_yes() {
                    let new_rel = GraphRel {
                        left: left_result.get_plan().clone(),
                        center: center_result.get_plan().clone(),
                        right: right_result.get_plan().clone(),
                        ..rel.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Recursively traverse other plan types
            LogicalPlan::Projection(proj) => {
                let input_result =
                    self.check_transitivity_recursive(proj.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input_result.get_plan().clone(),
                        ..proj.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_result = self.check_transitivity_recursive(
                    filter.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                if input_result.is_yes() {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: input_result.get_plan().clone(),
                        ..filter.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphJoins(joins) => {
                let input_result =
                    self.check_transitivity_recursive(joins.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_joins = crate::query_planner::logical_plan::GraphJoins {
                        input: input_result.get_plan().clone(),
                        ..joins.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(
                        new_joins,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Limit(limit) => {
                let input_result =
                    self.check_transitivity_recursive(limit.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: input_result.get_plan().clone(),
                        ..limit.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // #499: comma-separated multi-pattern MATCH (`MATCH p1=..., p2=...`)
            // plans as a CartesianProduct of independent GraphRel subtrees. This
            // walker previously had no arm for it, so a SECOND (or later)
            // pattern's VLP was never visited by this pass at all — its
            // non-transitive clamp (and, more generally, VLP CTE trigger
            // detection elsewhere) silently never applied to it. Recurse into
            // both sides so every independent pattern gets checked.
            LogicalPlan::CartesianProduct(cp) => {
                let left_result =
                    self.check_transitivity_recursive(cp.left.clone(), plan_ctx, graph_schema)?;
                let right_result =
                    self.check_transitivity_recursive(cp.right.clone(), plan_ctx, graph_schema)?;
                if left_result.is_yes() || right_result.is_yes() {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: left_result.get_plan().clone(),
                        right: right_result.get_plan().clone(),
                        ..cp.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(
                        new_cp,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Other plan types don't contain GraphRel, pass through
            _ => Ok(Transformed::No(plan)),
        }
    }
}
