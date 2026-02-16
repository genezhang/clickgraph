//! # Query Analyzer
//!
//! The analyzer module transforms parsed Cypher AST into an optimized logical
//! plan ready for SQL generation. It runs a configurable pipeline of analysis
//! and optimization passes.
//!
//! ## Pass Pipeline Overview
//!
//! The analyzer executes passes in a specific order, with each pass having
//! dependencies on previous passes:
//!
//! ```text
//! 1. PlanSanitization     - Basic plan cleanup and validation
//! 2. SchemaInference      - Resolve labels to tables, create ViewScans
//! 3. DuplicateScansRemove - Deduplicate repeated alias scans
//! 4. ProjectionTagging    - Expand RETURN *, tag columns
//! 5. TypeInference        - Infer types for untyped variables
//! 6. VariableResolver     - Resolve property accesses to columns
//! 7. FilterTagging        - Push filters down, tag extractable filters
//! 8. GroupByBuilding      - Create GROUP BY from aggregations
//! 9. GraphJoinInference   - Generate JOINs from graph patterns
//! 10. CteColumnResolver   - Resolve CTE column references
//! 11. VlpTransitivityCheck - Validate variable-length path patterns
//! ```
//!
//! ## Key Responsibilities
//!
//! - **Schema Integration**: Maps Cypher labels/types to ClickHouse tables
//! - **Type Resolution**: Infers and validates variable types
//! - **Filter Optimization**: Pushes WHERE conditions to optimal positions
//! - **Join Planning**: Converts graph patterns to efficient JOIN trees
//! - **CTE Management**: Handles WITH clauses and subqueries
//!
//! ## Module Organization
//!
//! - `analyzer_pass.rs`: Pass trait and infrastructure
//! - `graph_join_inference.rs`: Core JOIN generation logic (largest module)
//! - `filter_tagging.rs`: Filter pushdown and extraction
//! - `schema_inference.rs`: Label-to-table resolution
//! - `type_inference.rs`: Variable type inference
//! - `variable_resolver.rs`: Property-to-column resolution

use std::sync::Arc;

use analyzer_pass::AnalyzerResult;

pub mod view_resolver;
#[cfg(test)]
mod view_resolver_tests;

pub mod multi_type_vlp_expansion;
pub mod property_requirements;
pub mod property_requirements_analyzer;
#[cfg(test)]
mod test_multi_type_vlp_auto_inference;
pub mod where_property_extractor;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::AnalyzerPass,
            bidirectional_union::BidirectionalUnion,
            cte_column_resolver::CteColumnResolver,
            cte_reference_populator::CteReferencePopulator,
            cte_schema_resolver::CteSchemaResolver,
            duplicate_scans_removing::DuplicateScansRemoving,
            filter_tagging::FilterTagging,
            graph_join_inference::GraphJoinInference,
            graph_traversal_planning::GraphTRaversalPlanning,
            group_by_building::GroupByBuilding,
            plan_sanitization::PlanSanitization,
            projected_columns_resolver::ProjectedColumnsResolver,
            projection_tagging::ProjectionTagging,
            query_validation::QueryValidation,
            // SchemaInference REMOVED (Feb 16, 2026) - Merged into TypeInference
            type_inference::TypeInference,
            variable_resolver::VariableResolver,
            vlp_transitivity_check::VlpTransitivityCheck,
        },
        logical_plan::LogicalPlan,
        optimizer::{
            cartesian_join_extraction::CartesianJoinExtraction,
            collect_unwind_elimination::CollectUnwindElimination, optimizer_pass::OptimizerPass,
            trivial_with_elimination::TrivialWithElimination,
        },
    },
};

use super::plan_ctx::PlanCtx;

mod analyzer_pass;
mod bidirectional_union;
mod cte_column_resolver;
mod cte_reference_populator;
mod cte_schema_resolver;
mod duplicate_scans_removing;
pub mod errors;
mod filter_tagging;
mod graph_context;
pub mod graph_join;
pub use graph_join as graph_join_inference;
mod graph_traversal_planning;
mod group_by_building;
pub mod match_type_inference;
mod plan_sanitization;
mod projected_columns_resolver;
mod projection_tagging;
mod query_validation;
// mod schema_inference;  // REMOVED (Feb 16, 2026) - Fully merged into TypeInference
mod type_inference;
mod unwind_property_rewriter;
mod unwind_tuple_enricher;
mod variable_resolver;
mod vlp_transitivity_check;

// PatternResolver module and configuration
mod pattern_resolver;
mod pattern_resolver_config;

pub fn initial_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    log::info!("🔍 ANALYZER: Entering initial_analyzing");

    // Step 1: Schema Inference - REMOVED (merged into TypeInference Phase 0+3)
    // SchemaInference functionality has been fully merged into UnifiedTypeInference:
    // - Phase 0: Relationship-based label inference (infer_missing_labels logic)
    // - Phase 3: ViewScan resolution (push_inferred_table_names_to_scan logic)
    // Removed: February 16, 2026
    /*
    let schema_inference = SchemaInference::new();
    let plan = if let Ok(transformed_plan) =
        schema_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };
    */

    // Step 2: Type Inference - infer missing node labels AND edge types from schema
    // Now runs as FIRST pass (after SchemaInference consolidation)
    // 4-phase unified type inference:
    // - Phase 0: Relationship-based label inference
    // - Phase 1: Filter→GraphRel UNION generation
    // - Phase 2: Untyped node UNION generation
    // - Phase 3: ViewScan resolution
    let type_inference = TypeInference::new();
    let plan = if let Ok(transformed_plan) =
        type_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };

    // Step 2.1: Pattern Resolver - enumerate type combinations for remaining untyped nodes
    // Step 2.1: PatternResolver - DEPRECATED (merged into TypeInference)
    //
    // PatternResolver functionality has been fully absorbed into UnifiedTypeInference.
    // TypeInference now handles BOTH:
    // - Filter→GraphRel patterns with WHERE constraints (Phase 1)
    // - Untyped node discovery and UNION generation (Phase 2)
    //
    // Key improvements over old PatternResolver:
    // - Direction validation: check_relationship_exists_with_direction()
    // - Undirected optimization: optimize_undirected_pattern()
    // - Filters invalid branches like (Post)-[AUTHORED]->(User)
    //
    // Removed: February 16, 2026
    // See: src/query_planner/analyzer/type_inference.rs (lines 2100-2450)
    /*
    log::info!("🔍 ANALYZER: Running PatternResolver (handle ambiguous types)");
    use crate::query_planner::analyzer::pattern_resolver::PatternResolver;
    let pattern_resolver = PatternResolver::new();
    let plan = match pattern_resolver.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    ) {
        Ok(transformed_plan) => transformed_plan.get_plan(),
        Err(e) => {
            log::warn!(
                "⚠️  PatternResolver failed: {:?}, continuing with original plan",
                e
            );
            plan
        }
    };
    */

    // Step 2.5: VLP Transitivity Check - validate variable-length path patterns
    // This runs after TypeInference to ensure we have relationship types resolved
    // Checks if VLP patterns are semantically valid (relationship must be transitive)
    // Converts non-transitive patterns (e.g., IP-[DNS*]->Domain) to fixed-length
    log::info!("🔍 Running VLP Transitivity Check...");
    let vlp_transitivity_check = VlpTransitivityCheck::new();
    let plan = vlp_transitivity_check
        .analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?
        .get_plan();

    // Step 3: CTE Schema Resolver - register CTE schemas in plan_ctx for analyzer/planner
    // This runs after SchemaInference to ensure property mappings are available
    // Registers WithClause CTE schemas, making column info available to downstream passes
    // Also marks exported aliases as CTE-sourced so FilterTagging skips schema mapping
    let cte_schema_resolver = CteSchemaResolver::new();
    let plan = if let Ok(transformed_plan) =
        cte_schema_resolver.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };

    // Step 3.5: BidirectionalUnion - Transform undirected patterns (Direction::Either) into UNION ALL
    // CRITICAL: This MUST run BEFORE GraphJoinInference to avoid OR-based JOINs that ClickHouse handles incorrectly
    // GraphJoinInference converts GraphRel to GraphJoins, so we need to do the bidirectional expansion first
    log::info!("🔍 ANALYZER: Running BidirectionalUnion (before GraphJoinInference)");
    let bidirectional_union = BidirectionalUnion;
    let plan = match bidirectional_union.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    ) {
        Ok(transformed_plan) => transformed_plan.get_plan(),
        Err(e) => {
            log::warn!(
                "⚠️  BidirectionalUnion failed: {:?}, continuing with original plan",
                e
            );
            plan
        }
    };

    // Step 4: Graph Join Inference - analyze graph patterns and create PatternSchemaContext
    // MOVED UP from Step 15 to make PatternSchemaContext available for downstream passes
    // This is a pure analysis pass that only needs: GraphSchema, node/edge schemas, pattern structure
    // Enables property resolution passes to use explicit role information (from/to)
    log::info!("🔍 ANALYZER: Running GraphJoinInference (Phase 0: moved to Step 4)");
    let graph_join_inference = GraphJoinInference::new();
    let plan = if let Ok(transformed_plan) =
        graph_join_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        log::warn!("⚠️  GraphJoinInference failed, continuing with original plan");
        plan
    };

    // Step 5: Projected Columns Resolver - pre-compute projected columns for GraphNodes
    // Now can use PatternSchemaContext from PlanCtx for explicit role information
    // Populates GraphNode.projected_columns with correct from/to property resolution
    let projected_columns_resolver = ProjectedColumnsResolver::new();
    let plan = if let Ok(transformed_plan) = projected_columns_resolver.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    ) {
        transformed_plan.get_plan()
    } else {
        plan
    };

    // Step 6: Query Validation - VALIDATE EARLY before any transformations
    // This prevents invalid queries from being processed further
    let query_validation = QueryValidation::new();
    let transformed_plan =
        query_validation.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?;
    let plan = transformed_plan.get_plan();

    // Step 7: Property Mapping - map Cypher properties to database columns (ONCE)
    // NOTE: FilterTagging now PRESERVES cross-table filters (those referencing WITH aliases
    // and having CartesianProduct descendants) instead of extracting them. This allows
    // CartesianJoinExtraction (step 3.5) to pick up the property-mapped predicate.
    let filter_tagging = FilterTagging::new();
    let transformed_plan =
        filter_tagging.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?;
    let plan = transformed_plan.get_plan();

    // Step 3.5: CartesianJoinExtraction - extract cross-pattern filters into join_condition
    // CRITICAL: This runs AFTER FilterTagging to get property-mapped predicates.
    // FilterTagging now preserves cross-table filters in the plan (instead of extracting to plan_ctx).
    // This enables proper JOIN ... ON generation for correlated WITH clauses.
    let cartesian_join_extraction = CartesianJoinExtraction::new();
    let plan = match cartesian_join_extraction.optimize(plan.clone(), plan_ctx) {
        Ok(transformed) => transformed.get_plan(),
        Err(e) => {
            return Err(errors::AnalyzerError::OptimizerError {
                message: e.to_string(),
            });
        }
    };

    // Step 4: Projection Tagging - tag projections into plan_ctx (NO mapping, just tagging)
    let projection_tagging = ProjectionTagging::new();
    let transformed_plan = projection_tagging.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    // Step 5: Group By Building
    let group_by_building = GroupByBuilding::new();
    let transformed_plan = group_by_building.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    if let LogicalPlan::GraphJoins(gj) = plan.as_ref() {
        log::warn!(
            "🔍   GraphJoins! {} joins, input type: {:?}",
            gj.joins.len(),
            std::mem::discriminant(gj.input.as_ref())
        );
        match gj.input.as_ref() {
            LogicalPlan::WithClause(wc) => {
                log::warn!(
                    "🔍     WithClause! input type: {:?}",
                    std::mem::discriminant(wc.input.as_ref())
                );
                if let LogicalPlan::Filter(_f) = wc.input.as_ref() {
                    log::warn!("🔍       ✅✅✅ Filter EXISTS at END of initial_analyzing!");
                }
            }
            LogicalPlan::Projection(proj) => {
                log::debug!("GraphJoins → Projection (not WithClause) at end of initial_analyzing");
                log::warn!(
                    "🔍       Projection.input type: {:?}",
                    std::mem::discriminant(proj.input.as_ref())
                );
                // Check if Projection → WithClause → Filter
                if let LogicalPlan::WithClause(wc) = proj.input.as_ref() {
                    log::warn!(
                        "🔍         WithClause! input type: {:?}",
                        std::mem::discriminant(wc.input.as_ref())
                    );
                    if let LogicalPlan::Filter(f) = wc.input.as_ref() {
                        log::warn!(
                            "🔍           ✅✅✅ Filter EXISTS in WithClause: {:?}",
                            f.predicate
                        );
                    } else {
                        log::error!(
                            "🔥🔥🔥 FILTER LOST! WithClause.input is: {:?}",
                            std::mem::discriminant(wc.input.as_ref())
                        );
                    }
                }
            }
            _ => {}
        }
    }

    Ok(plan)
}

pub fn intermediate_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    // Note: SchemaInference and QueryValidation already ran in initial_analyzing

    match plan.as_ref() {
        LogicalPlan::GraphJoins(gj) => {
            log::warn!(
                "🔍   GraphJoins! {} joins, input type: {:?}",
                gj.joins.len(),
                std::mem::discriminant(gj.input.as_ref())
            );
            match gj.input.as_ref() {
                LogicalPlan::WithClause(wc) => {
                    log::warn!(
                        "🔍     WithClause EXISTS! input type: {:?}",
                        std::mem::discriminant(wc.input.as_ref())
                    );
                    if let LogicalPlan::Filter(f) = wc.input.as_ref() {
                        log::warn!(
                            "🔍       ✅✅✅ Filter exists with predicate: {:?}",
                            f.predicate
                        );
                    } else {
                        log::error!(
                            "🔥 BUG: WithClause.input is NOT Filter! Type: {:?}",
                            std::mem::discriminant(wc.input.as_ref())
                        );
                    }
                }
                LogicalPlan::Projection(proj) => {
                    log::debug!("GraphJoins.input is Projection (WHERE already lost)");
                    log::warn!(
                        "🔍       Projection.input type: {:?}",
                        std::mem::discriminant(proj.input.as_ref())
                    );
                }
                _ => log::warn!(
                    "🔍     GraphJoins.input type: {:?}",
                    std::mem::discriminant(gj.input.as_ref())
                ),
            }
        }
        _ => log::warn!("🔍   NOT GraphJoins"),
    }
    // This pass focuses on graph-specific planning and optimizations

    let graph_traversal_planning = GraphTRaversalPlanning::new();
    let transformed_plan = graph_traversal_planning.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    // NOTE: SchemaInference removed (Feb 16, 2026)
    // ViewScan resolution now handled by TypeInference Phase 3
    // let transformed_plan = SchemaInference::push_inferred_table_names_to_scan(plan, plan_ctx)?;
    // let plan = transformed_plan.get_plan();

    let duplicate_scans_removing = DuplicateScansRemoving::new();
    let transformed_plan = duplicate_scans_removing.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // NOTE: BidirectionalUnion has been moved to initial_analyzing() to run BEFORE GraphJoinInference
    // This ensures undirected patterns are expanded to UNION ALL before GraphRel is converted to GraphJoins

    // CRITICAL: Resolve variables BEFORE join inference
    // This transforms TableAlias("cnt") → PropertyAccessExp("cnt_cte", "cnt")
    // Making the renderer "dumb" - it only needs to emit SQL for resolved expressions
    log::info!("🔍 ANALYZER: About to call VariableResolver.analyze()");
    let variable_resolver = VariableResolver::new();
    let transformed_plan = variable_resolver.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("🔍 ANALYZER: VariableResolver.analyze() completed");

    fn count_cte_refs_here(p: &LogicalPlan) -> usize {
        match p {
            LogicalPlan::WithClause(wc) => wc.cte_references.len() + count_cte_refs_here(&wc.input),
            LogicalPlan::Projection(proj) => count_cte_refs_here(&proj.input),
            LogicalPlan::Limit(l) => count_cte_refs_here(&l.input),
            LogicalPlan::GraphJoins(gj) => count_cte_refs_here(&gj.input), // ADD THIS
            _ => 0,
        }
    }
    eprintln!(
        "🔬 ANALYZER: IMMEDIATELY after VariableResolver: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // CRITICAL: Populate GraphRel.cte_references AFTER VariableResolver
    // This tells the renderer which node connections come from CTEs
    log::info!("🔍 ANALYZER: About to call CteReferencePopulator.analyze()");
    let cte_ref_populator = CteReferencePopulator::new();
    let transformed_plan = cte_ref_populator.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("🔍 ANALYZER: CteReferencePopulator.analyze() completed");
    eprintln!(
        "🔬 ANALYZER: After CteReferencePopulator: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // NOTE: GraphJoinInference now runs earlier in initial_analyzing() (Step 4)
    // PatternSchemaContext is already available in plan_ctx at this point

    // CRITICAL: Resolve CTE column names AFTER join inference
    // GraphJoinInference populates CTE column mappings in plan_ctx
    // This pass resolves PropertyAccess expressions in SELECT/WHERE/HAVING to use CTE column names
    // Example: PropertyAccess("p", "firstName") → PropertyAccess("p", "p_firstName")
    let cte_column_resolver = CteColumnResolver;
    let transformed_plan = cte_column_resolver.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();
    eprintln!(
        "🔬 ANALYZER: After CteColumnResolver: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // Enrich Unwind nodes with tuple structure metadata for property-to-index mapping
    // This enables user.name → user.5 (tuple index) after UNWIND of collect(node)
    // Must run AFTER all analysis passes that might recreate Unwind nodes
    let plan = unwind_tuple_enricher::enrich_unwind_with_tuple_info(plan);
    eprintln!(
        "🔬 ANALYZER: After unwind_tuple_enricher: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // Collect+UNWIND Elimination - remove no-op patterns like WITH collect(x) as xs + UNWIND xs as x
    // This must run BEFORE PropertyRequirementsAnalyzer to eliminate patterns that would complicate analysis
    log::info!("🔍 Running Collect+UNWIND Elimination...");
    let collect_unwind_elimination = CollectUnwindElimination;
    let plan = match collect_unwind_elimination.optimize(plan.clone(), plan_ctx) {
        Ok(transformed) => transformed.get_plan(),
        Err(e) => {
            return Err(errors::AnalyzerError::OptimizerError {
                message: e.to_string(),
            });
        }
    };
    log::info!("✓ Collect+UNWIND Elimination completed");
    eprintln!(
        "🔬 ANALYZER: After CollectUnwindElimination: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // Trivial WITH Elimination - remove pass-through WITH clauses that add no value
    // Run after collect+UNWIND elimination to clean up any resulting trivial WITHs
    log::info!("🔍 Running Trivial WITH Elimination...");
    log::warn!(
        "🔍 BEFORE TrivialWithElimination: plan type: {:?}",
        std::mem::discriminant(plan.as_ref())
    );
    match plan.as_ref() {
        LogicalPlan::GraphJoins(gj) => {
            log::warn!(
                "🔍   It's GraphJoins! {} joins, input type: {:?}",
                gj.joins.len(),
                std::mem::discriminant(gj.input.as_ref())
            );
            match gj.input.as_ref() {
                LogicalPlan::WithClause(wc) => {
                    log::warn!(
                        "🔍     WithClause! input type: {:?}",
                        std::mem::discriminant(wc.input.as_ref())
                    );
                    if let LogicalPlan::Filter(f) = wc.input.as_ref() {
                        log::warn!(
                            "🔍       ✅ Filter exists with predicate: {:?}",
                            f.predicate
                        );
                    } else {
                        log::error!(
                            "🔥🔥🔥 BUG FOUND: WithClause.input is NOT Filter! Type: {:?}",
                            std::mem::discriminant(wc.input.as_ref())
                        );
                    }
                }
                _ => log::warn!(
                    "🔍     GraphJoins.input is NOT WithClause: {:?}",
                    std::mem::discriminant(gj.input.as_ref())
                ),
            }
        }
        _ => log::warn!(
            "🔍   NOT GraphJoins (type: {:?})",
            std::mem::discriminant(plan.as_ref())
        ),
    }

    let trivial_with_elimination = TrivialWithElimination;
    let plan = match trivial_with_elimination.optimize(plan.clone(), plan_ctx) {
        Ok(transformed) => transformed.get_plan(),
        Err(e) => {
            return Err(errors::AnalyzerError::OptimizerError {
                message: e.to_string(),
            });
        }
    };
    log::info!("✓ Trivial WITH Elimination completed");

    log::warn!(
        "🔍 AFTER TrivialWithElimination: plan type: {:?}",
        std::mem::discriminant(plan.as_ref())
    );
    if let LogicalPlan::Projection(proj) = plan.as_ref() {
        log::warn!(
            "🔍   Projection.input type: {:?}",
            std::mem::discriminant(proj.input.as_ref())
        );
        if let LogicalPlan::WithClause(wc) = proj.input.as_ref() {
            log::warn!(
                "🔍     WithClause.input type: {:?}",
                std::mem::discriminant(wc.input.as_ref())
            );
        } else if let LogicalPlan::Filter(f) = proj.input.as_ref() {
            log::warn!(
                "🔍     Filter still exists with predicate: {:?}",
                f.predicate
            );
        }
    }
    eprintln!(
        "🔬 ANALYZER: After TrivialWithElimination: {} cte_references",
        count_cte_refs_here(&plan)
    );

    // Property Requirements Analysis - determine which properties are actually needed
    // This runs at the END of analysis, after all property references are stable
    // Enables property pruning optimization in renderer (85-98% memory reduction)
    log::info!("🔍 Running Property Requirements Analyzer...");
    let property_requirements_analyzer =
        property_requirements_analyzer::PropertyRequirementsAnalyzer;
    let transformed_plan = property_requirements_analyzer.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("✓ Property Requirements Analyzer completed");
    eprintln!(
        "🔬 ANALYZER: After PropertyRequirementsAnalyzer: {} cte_references",
        count_cte_refs_here(&plan)
    );

    Ok(plan)
}

pub fn final_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    _: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    // Debug: Print projection items before sanitization
    if let LogicalPlan::Projection(proj) = plan.as_ref() {
        crate::debug_print!(
            "final_analyzing BEFORE sanitization: {} projection items",
            proj.items.len()
        );
        for (_i, _item) in proj.items.iter().enumerate() {
            crate::debug_print!("  item {}: expr={:?}", _i, _item.expression);
        }
    }

    let plan_sanitization = PlanSanitization::new();
    let transformed_plan = plan_sanitization.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // Debug: Print projection items after sanitization
    if let LogicalPlan::Projection(proj) = plan.as_ref() {
        crate::debug_print!(
            "final_analyzing AFTER sanitization: {} projection items",
            proj.items.len()
        );
        for (_i, _item) in proj.items.iter().enumerate() {
            crate::debug_print!("  item {}: expr={:?}", _i, _item.expression);
        }
    }

    // Rewrite property access expressions to use tuple indices
    // MUST run at the VERY END after all transformations complete
    let plan = unwind_property_rewriter::rewrite_unwind_properties(plan);

    Ok(plan)
}
