use std::sync::Arc;

use analyzer_pass::AnalyzerResult;

pub mod view_resolver;
#[cfg(test)]
mod view_resolver_tests;

pub mod property_requirements;
pub mod property_requirements_analyzer;
pub mod multi_type_vlp_expansion;
#[cfg(test)]
mod test_multi_type_vlp_auto_inference;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::AnalyzerPass, bidirectional_union::BidirectionalUnion,
            cte_column_resolver::CteColumnResolver, cte_schema_resolver::CteSchemaResolver,
            duplicate_scans_removing::DuplicateScansRemoving, filter_tagging::FilterTagging,
            graph_join_inference::GraphJoinInference,
            graph_traversal_planning::GraphTRaversalPlanning, group_by_building::GroupByBuilding,
            type_inference::TypeInference,
            plan_sanitization::PlanSanitization, projection_tagging::ProjectionTagging,
            projected_columns_resolver::ProjectedColumnsResolver,
            query_validation::QueryValidation, schema_inference::SchemaInference,
            variable_resolver::VariableResolver,
            cte_reference_populator::CteReferencePopulator,
            vlp_transitivity_check::VlpTransitivityCheck,
        },
        logical_plan::LogicalPlan,
        optimizer::{
            cartesian_join_extraction::CartesianJoinExtraction, optimizer_pass::OptimizerPass,
            collect_unwind_elimination::CollectUnwindElimination,
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
mod graph_join_inference;
mod graph_traversal_planning;
mod group_by_building;
mod type_inference;
mod plan_sanitization;
mod projection_tagging;
mod projected_columns_resolver;
mod query_validation;
mod schema_inference;
mod variable_resolver;
mod vlp_transitivity_check;
mod unwind_property_rewriter;
mod unwind_tuple_enricher;

pub fn initial_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    log::info!("🔍 ANALYZER: Entering initial_analyzing");
    // Step 1: Schema Inference - infer missing schema information
    let schema_inference = SchemaInference::new();
    let plan = if let Ok(transformed_plan) =
        schema_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };

    // Step 2: Type Inference - infer missing node labels AND edge types from schema
    // This runs early to ensure all downstream passes have complete type information
    // Works across WITH boundaries using existing plan_ctx scope barriers
    // Infers: node labels from edge types, edge types from node labels, defaults from schema
    let type_inference = TypeInference::new();
    let plan = if let Ok(transformed_plan) =
        type_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };

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
    let cte_schema_resolver = CteSchemaResolver::new();
    let plan = if let Ok(transformed_plan) = cte_schema_resolver.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    ) {
        transformed_plan.get_plan()
    } else {
        plan
    };

    // Step 4: Projected Columns Resolver - pre-compute projected columns for GraphNodes
    // This runs after SchemaInference to ensure we have property mappings available
    // Populates GraphNode.projected_columns, eliminating need for renderer to traverse plan
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

    // Step 5: Query Validation - VALIDATE EARLY before any transformations
    // This prevents invalid queries from being processed further
    let query_validation = QueryValidation::new();
    let transformed_plan =
        query_validation.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?;
    let plan = transformed_plan.get_plan();

    // Step 6: Property Mapping - map Cypher properties to database columns (ONCE)
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

    Ok(plan)
}

pub fn intermediate_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    // Note: SchemaInference and QueryValidation already ran in initial_analyzing
    // This pass focuses on graph-specific planning and optimizations

    let graph_traversal_planning = GraphTRaversalPlanning::new();
    let transformed_plan = graph_traversal_planning.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    let transformed_plan = SchemaInference::push_inferred_table_names_to_scan(plan, plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let duplicate_scans_removing = DuplicateScansRemoving::new();
    let transformed_plan = duplicate_scans_removing.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // Transform bidirectional patterns (Direction::Either) into UNION ALL of two directed patterns
    // This MUST run before GraphJoinInference to avoid OR-based JOINs that ClickHouse handles incorrectly
    let bidirectional_union = BidirectionalUnion;
    let transformed_plan = bidirectional_union.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    // CRITICAL: Resolve variables BEFORE join inference
    // This transforms TableAlias("cnt") → PropertyAccessExp("cnt_cte", "cnt")
    // Making the renderer "dumb" - it only needs to emit SQL for resolved expressions
    log::info!("🔍 ANALYZER: About to call VariableResolver.analyze()");
    let variable_resolver = VariableResolver::new();
    let transformed_plan = variable_resolver.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("🔍 ANALYZER: VariableResolver.analyze() completed");
    
    // DEBUG: Check cte_references RIGHT after VariableResolver
    fn count_cte_refs_here(p: &LogicalPlan) -> usize {
        match p {
            LogicalPlan::WithClause(wc) => {
                wc.cte_references.len() + count_cte_refs_here(&wc.input)
            }
            LogicalPlan::Projection(proj) => count_cte_refs_here(&proj.input),
            LogicalPlan::Limit(l) => count_cte_refs_here(&l.input),
            LogicalPlan::GraphJoins(gj) => count_cte_refs_here(&gj.input),  // ADD THIS
            _ => 0,
        }
    }
    eprintln!("🔬 ANALYZER: IMMEDIATELY after VariableResolver: {} cte_references", count_cte_refs_here(&plan));

    // CRITICAL: Populate GraphRel.cte_references AFTER VariableResolver
    // This tells the renderer which node connections come from CTEs
    log::info!("🔍 ANALYZER: About to call CteReferencePopulator.analyze()");
    let cte_ref_populator = CteReferencePopulator::new();
    let transformed_plan = cte_ref_populator.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("🔍 ANALYZER: CteReferencePopulator.analyze() completed");
    eprintln!("🔬 ANALYZER: After CteReferencePopulator: {} cte_references", count_cte_refs_here(&plan));

    let graph_join_inference = GraphJoinInference::new();
    let transformed_plan = graph_join_inference.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();
    eprintln!("🔬 ANALYZER: After GraphJoinInference: {} cte_references", count_cte_refs_here(&plan));

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
    eprintln!("🔬 ANALYZER: After CteColumnResolver: {} cte_references", count_cte_refs_here(&plan));

    // Enrich Unwind nodes with tuple structure metadata for property-to-index mapping
    // This enables user.name → user.5 (tuple index) after UNWIND of collect(node)
    // Must run AFTER all analysis passes that might recreate Unwind nodes
    let plan = unwind_tuple_enricher::enrich_unwind_with_tuple_info(plan);
    eprintln!("🔬 ANALYZER: After unwind_tuple_enricher: {} cte_references", count_cte_refs_here(&plan));

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
    eprintln!("🔬 ANALYZER: After CollectUnwindElimination: {} cte_references", count_cte_refs_here(&plan));

    // Trivial WITH Elimination - remove pass-through WITH clauses that add no value
    // Run after collect+UNWIND elimination to clean up any resulting trivial WITHs
    log::info!("🔍 Running Trivial WITH Elimination...");
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
    eprintln!("🔬 ANALYZER: After TrivialWithElimination: {} cte_references", count_cte_refs_here(&plan));

    // Property Requirements Analysis - determine which properties are actually needed
    // This runs at the END of analysis, after all property references are stable
    // Enables property pruning optimization in renderer (85-98% memory reduction)
    log::info!("🔍 Running Property Requirements Analyzer...");
    let property_requirements_analyzer = property_requirements_analyzer::PropertyRequirementsAnalyzer;
    let transformed_plan = property_requirements_analyzer.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();
    log::info!("✓ Property Requirements Analyzer completed");
    eprintln!("🔬 ANALYZER: After PropertyRequirementsAnalyzer: {} cte_references", count_cte_refs_here(&plan));

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
