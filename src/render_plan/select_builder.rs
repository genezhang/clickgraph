//! Select Builder Module
//!
//! This module handles the extraction and processing of SELECT items from logical plans.
//! It manages property expansion, aggregation handling, wildcard expansion, and
//! denormalized node processing for RETURN clauses.
//!
//! Key responsibilities:
//! - Convert LogicalExpr items to SelectItem structures
//! - Handle property expansion for table aliases (u.name, u.email, etc.)
//! - Process wildcard expansion (u.* → explicit property list)
//! - Apply aggregation wrapping (anyLast() for non-ID columns in GROUP BY)
//! - Handle denormalized node properties from edge tables
//! - Support path variable extraction (nodes(p), relationships(p))
//! - Manage collect() function expansion

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
use crate::query_planner::logical_plan::{LogicalPlan, ProjectionItem};
use crate::query_planner::typed_variable::{TypedVariable, VariableSource};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::properties_builder::PropertiesBuilder;
use crate::render_plan::render_expr::{
    Column, ColumnAlias, Literal, PropertyAccess, RenderExpr, ScalarFnCall,
    TableAlias as RenderTableAlias,
};
use crate::render_plan::SelectItem;
use crate::sql_generator::function_mapper::current_function_mapper;
use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};

/// Build the second argument to a `JSONExtractString` / `get_json_object`
/// call.
///
/// ClickHouse's `JSONExtractString(json, 'field')` takes a bare field name.
/// Databricks/Spark's `get_json_object(json, '$.field')` requires JSONPath.
/// Function name comes from `FunctionMapper::json_extract_string`; the
/// argument shape is a call-site choice, handled here.
///
/// Dialect is read from the task-local `QueryContext`; outside a scope this
/// defaults to ClickHouse (bare name).
fn json_extract_field_arg(col_name: &str) -> String {
    match crate::server::query_context::get_current_dialect() {
        crate::sql_generator::SqlDialect::Databricks => format!("$.{}", col_name),
        _ => col_name.to_string(),
    }
}

/// #492: Translate a denormalized node's DB column to the side of the edge a
/// property lookup actually bound the alias to.
///
/// In a denormalized multi-hop chain the shared middle node is adjacent to two
/// edges; the planner's schema rewrite may resolve its property against one
/// edge's side (e.g. `b.code` → `Dest`, b as t1's to-node) while
/// `get_properties_with_table_alias` binds the alias to the other edge (t2,
/// where b is the from-node whose column is `Origin`). Rendering the first
/// edge's column with the second edge's table alias reads the WRONG endpoint.
/// Reverse-lookup the Cypher property whose from-/to-side DB column equals
/// `col_name` on the node schema, then map it through `properties` (the bound
/// side's `(cypher_name, db_column)` pairs). Mirrors the cross-side fix in the
/// WITH-CTE path (`plan_builder_utils::resolve_denormalized_property_in_expr`).
///
/// `pub(crate)`: shared with the WHERE/filter path
/// (`plan_builder_helpers::apply_property_mapping_to_expr`), which has the
/// identical cross-side hazard.
pub(crate) fn translate_denorm_cross_side_column(
    plan: &LogicalPlan,
    alias: &str,
    col_name: &str,
    properties: &[(String, String)],
) -> Option<String> {
    use crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan;
    use crate::server::query_context::get_current_schema_with_fallback;

    let schema = get_current_schema_with_fallback()?;
    // Label may be unresolved on some plan shapes (e.g. ORDER BY-wrapped
    // branches leave GraphNode.label = None); the catalog API then searches
    // all node schemas and the `properties` filter below keeps it scoped to
    // the alias's actual bound side.
    let label = find_label_for_alias_in_plan(plan, alias);
    for cypher_name in schema.denorm_properties_for_side_column(label.as_deref(), col_name) {
        if let Some((_, correct_col)) = properties.iter().find(|(pn, _)| *pn == cypher_name) {
            if correct_col != col_name {
                log::info!(
                    "🔧 Denormalized cross-side fix: '{}.{}' (from '{}') → '{}'",
                    alias,
                    col_name,
                    cypher_name,
                    correct_col
                );
            }
            return Some(correct_col.clone());
        }
    }
    None
}

/// #492/#491 interaction fix: `get_properties_with_table_alias` picks a
/// node alias's property source PURELY STRUCTURALLY — the first GraphRel in
/// the tree whose left/right connection matches, regardless of which edge
/// the alias will actually be RENDERED against (`table_alias_override`, from
/// the `denormalized_node_edges` registry). Ordinarily these agree (the
/// registry's last-write-wins matches the structurally-outermost edge for
/// required chains). #491 made OPTIONAL patterns keep an EARLIER binding
/// instead of overwriting it — so for `(a)-[t1]->(b) OPTIONAL (b)-[t2]->(c)`,
/// `b` renders against `t1` (registry, #491-correct) while the structural
/// walk still matches `t2` (the outer/optional GraphRel) first. Using `t2`'s
/// properties for cross-side translation while rendering against `t1`
/// combines a value from the WRONG edge with the RIGHT alias (silently wrong:
/// `t1.origin_code`, `a`'s own column, instead of `t1.dest_code`).
///
/// Re-derive the `(cypher_name, db_column)` pairs directly from the edge
/// identified by `table_alias_override` when it is present in the tree as a
/// `GraphRel.alias`, so cypher-name lookups and cross-side translation always
/// operate on the SAME edge the alias renders against. Returns `None` (caller
/// falls back to the structurally-obtained properties) for alias schemes that
/// aren't literal `GraphRel.alias` values in this plan (e.g. `CoupledSameRow`'s
/// `unified_alias` — #481 territory, unaffected by this fix).
pub(crate) fn properties_for_registered_edge(
    plan: &LogicalPlan,
    node_alias: &str,
    edge_alias: &str,
) -> Option<Vec<(String, String)>> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            if rel.alias == edge_alias {
                // Route through the schema catalog (axis-dispatch rule): the
                // edge's own denormalized property maps live on
                // `RelationshipSchema`, not the `ViewScan` — reading the scan
                // field directly would be a raw-flag branch outside its
                // canonical dispatch module.
                let is_from_node = if node_alias == rel.left_connection {
                    true
                } else if node_alias == rel.right_connection {
                    false
                } else {
                    return None;
                };
                let rel_type = rel
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.first())
                    .map(|l| l.split("::").next().unwrap_or(l))?;
                let schema = crate::server::query_context::get_current_schema_with_fallback()?;
                let rel_schema = schema.get_relationships_schema_opt(rel_type)?;
                let props = rel_schema.denorm_side_properties(is_from_node);
                return if props.is_empty() { None } else { Some(props) };
            }
            properties_for_registered_edge(&rel.left, node_alias, edge_alias)
                .or_else(|| properties_for_registered_edge(&rel.right, node_alias, edge_alias))
        }
        LogicalPlan::GraphNode(n) => {
            properties_for_registered_edge(&n.input, node_alias, edge_alias)
        }
        LogicalPlan::Projection(p) => {
            properties_for_registered_edge(&p.input, node_alias, edge_alias)
        }
        LogicalPlan::Filter(f) => properties_for_registered_edge(&f.input, node_alias, edge_alias),
        LogicalPlan::GraphJoins(gj) => {
            properties_for_registered_edge(&gj.input, node_alias, edge_alias)
        }
        LogicalPlan::OrderBy(ob) => {
            properties_for_registered_edge(&ob.input, node_alias, edge_alias)
        }
        LogicalPlan::Skip(s) => properties_for_registered_edge(&s.input, node_alias, edge_alias),
        LogicalPlan::Limit(l) => properties_for_registered_edge(&l.input, node_alias, edge_alias),
        LogicalPlan::GroupBy(gb) => {
            properties_for_registered_edge(&gb.input, node_alias, edge_alias)
        }
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .find_map(|i| properties_for_registered_edge(i, node_alias, edge_alias)),
        LogicalPlan::WithClause(wc) => {
            properties_for_registered_edge(&wc.input, node_alias, edge_alias)
        }
        _ => None,
    }
}

/// SelectBuilder trait for extracting SELECT items from logical plans
pub trait SelectBuilder {
    /// Extract SELECT items from the logical plan
    fn extract_select_items(
        &self,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) -> Result<Vec<SelectItem>, RenderBuildError>;
}

/// Implementation of SelectBuilder for LogicalPlan
impl SelectBuilder for LogicalPlan {
    fn extract_select_items(
        &self,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) -> Result<Vec<SelectItem>, RenderBuildError> {
        log::trace!("🔍🔍🔍 extract_select_items CALLED on plan type");
        crate::debug_println!("DEBUG: extract_select_items called on: {:?}", self);
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::ViewScan(view_scan) => {
                // Build select items from ViewScan's property mappings and projections
                // This is needed for multiple relationship types where ViewScan nodes are created
                // for start/end nodes but don't have explicit projections

                if !view_scan.projections.is_empty() {
                    // Use explicit projections if available
                    view_scan
                        .projections
                        .iter()
                        .map(|proj: &LogicalExpr| {
                            let expr: RenderExpr = proj.clone().try_into()?;
                            Ok(SelectItem {
                                expression: expr,
                                col_alias: None,
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else if !view_scan.property_mapping.is_empty() {
                    // Fall back to property mappings - build select items for each property.
                    //
                    // Iterate in sorted cypher-property (key) order. `property_mapping`
                    // is a HashMap, whose iteration order varies per process. For a
                    // denormalized node materialization (#464) this SELECT is one branch
                    // of a positional `UNION DISTINCT` (the origin-side and dest-side
                    // scans of the same `flights_denorm` table). SQL UNION aligns
                    // branches BY POSITION, but the emitted column alias is the cypher
                    // property name — so both branches MUST emit their columns in the
                    // SAME order or `code` in one branch aligns with `state` in the
                    // other (live: 14 rows with `a.code` holding STATE values).
                    //
                    // When the two branches carry identical cypher-property key sets
                    // (the node's own properties, e.g. {code, city, state}), sorting
                    // each branch by that shared key yields the single canonical
                    // ordering both branches derive from — alignment-by-position is
                    // then structurally correct. NOTE: identical key sets are NOT
                    // validated by the schema loader (validate_denormalized_nodes only
                    // checks non-emptiness); a schema whose from/to property sets
                    // differ still misaligns here (the whole-node RETURN path in
                    // plan_builder.rs handles that case via union-of-keys + NULL
                    // padding; this fallback does not). Mirrors the #458 fix in
                    // cte_extraction.rs (sort denorm property-blob columns by key).
                    let mut entries: Vec<(&String, &PropertyValue)> =
                        view_scan.property_mapping.iter().collect();
                    entries.sort_by(|a, b| a.0.cmp(b.0));
                    entries
                        .into_iter()
                        .map(|(prop_name, prop_value): (&String, &PropertyValue)| {
                            Ok(SelectItem {
                                expression: RenderExpr::Column(Column(prop_value.clone())),
                                col_alias: Some(ColumnAlias(prop_name.clone())),
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else {
                    // No projections or property mappings - this might be a relationship scan
                    // Return empty for now (relationship CTEs are handled differently)
                    vec![]
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!(
                    "🔍 GraphRel.extract_select_items: alias={}, path_variable={:?}",
                    graph_rel.alias,
                    graph_rel.path_variable
                );
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];

                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items(plan_ctx)?);

                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items(plan_ctx)?);

                // SIMPLE FIX: If GraphRel has path_variable, add the path tuple directly
                // This handles UNION branches without needing plan_ctx or Projection wrapping
                if let Some(ref path_var) = graph_rel.path_variable {
                    log::debug!(
                        "🔍 GraphRel has path_variable '{}', adding path tuple to SELECT",
                        path_var
                    );
                    items.push(SelectItem {
                        expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: "tuple".to_string(),
                            args: vec![
                                RenderExpr::Literal(Literal::String("fixed_path".to_string())),
                                RenderExpr::Literal(Literal::String(
                                    graph_rel.left_connection.clone(),
                                )),
                                RenderExpr::Literal(Literal::String(
                                    graph_rel.right_connection.clone(),
                                )),
                                RenderExpr::Literal(Literal::String(graph_rel.alias.clone())),
                            ],
                        }),
                        col_alias: Some(ColumnAlias(path_var.clone())),
                    });

                    // CRITICAL FIX: For path queries, also include node and relationship properties
                    // Neo4j Browser (and Bolt protocol) expects full properties in path objects
                    // This enables convert_path_branches_to_json() to build _start_properties, _end_properties
                    log::debug!(
                        "🔍 Path query: expanding properties for left='{}', right='{}', rel='{}'",
                        graph_rel.left_connection,
                        graph_rel.right_connection,
                        graph_rel.alias
                    );

                    // Add left node properties with prefixed aliases (e.g., "a_0.user_id")
                    self.add_node_properties_for_path(
                        &graph_rel.left,
                        &graph_rel.left_connection,
                        &mut items,
                    )?;

                    // Add right node properties with prefixed aliases (e.g., "o_0.post_id")
                    self.add_node_properties_for_path(
                        &graph_rel.right,
                        &graph_rel.right_connection,
                        &mut items,
                    )?;

                    // Add relationship properties with prefixed aliases (e.g., "t20.follow_date")
                    self.add_relationship_properties_for_path(
                        &graph_rel.center,
                        &graph_rel.alias,
                        &mut items,
                    )?;
                }

                log::debug!(
                    "🔍 GraphRel.extract_select_items: returning {} items",
                    items.len()
                );

                items
            }
            LogicalPlan::Filter(filter) => filter.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Projection(projection) => {
                // Convert ProjectionItem expressions to SelectItems
                // CRITICAL: Expand table aliases (RETURN n → all properties)
                let mut select_items = vec![];

                for item in &projection.items {
                    log::debug!("🔍 TRACING: Processing SELECT item: {:?}", item.expression);
                    match &item.expression {
                        // Case 0: ColumnAlias (regular column reference)
                        LogicalExpr::ColumnAlias(col_alias) => {
                            log::info!(
                                "🔍 ColumnAlias('{}') - treating as regular column",
                                col_alias.0
                            );

                            // Regular column alias - pass through as-is
                            select_items.push(SelectItem {
                                expression: RenderExpr::ColumnAlias(ColumnAlias(
                                    col_alias.0.clone(),
                                )),
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ColumnAlias(ca.0.clone())),
                            });
                        }

                        // Case 1: TableAlias (e.g., RETURN n)
                        LogicalExpr::TableAlias(table_alias) => {
                            log::debug!(
                                "🔍 Processing TableAlias('{}'), has_plan_ctx={}",
                                table_alias.0,
                                plan_ctx.is_some()
                            );

                            // NEW APPROACH: Use TypedVariable for type/source checking
                            if let Some(plan_ctx) = plan_ctx {
                                log::trace!("  🔍 Looking up '{}' in plan_ctx...", table_alias.0);
                                match plan_ctx.lookup_variable(&table_alias.0) {
                                    Some(typed_var) if typed_var.is_entity() => {
                                        log::trace!(
                                            "  ✓ Found ENTITY variable '{}'",
                                            table_alias.0
                                        );
                                        // Entity (Node or Relationship) - expand properties
                                        match &typed_var.source() {
                                            VariableSource::Match => {
                                                // Check if this node is in a multi-type VLP context.
                                                // If so, use JSON columns from the CTE instead of
                                                // expanding individual properties (which differ per type).
                                                if let TypedVariable::Node(_) = typed_var {
                                                    if let Some(gr) = self
                                                        .find_graph_rel_for_alias(&table_alias.0)
                                                    {
                                                        // Detect CTE-based node: multi-type VLP (labels > 1) OR pattern_combinations
                                                        let is_multi_type_vlp = gr
                                                            .labels
                                                            .as_ref()
                                                            .is_some_and(|l| l.len() > 1);
                                                        let is_pattern_combinations =
                                                            gr.pattern_combinations.is_some();
                                                        if is_multi_type_vlp
                                                            || is_pattern_combinations
                                                        {
                                                            let cte_alias =
                                                                if is_pattern_combinations {
                                                                    gr.alias.as_str()
                                                                } else {
                                                                    "t"
                                                                };
                                                            log::info!(
                                                                "🎯 CTE node '{}' detected (multi_vlp={}, pattern_comb={}), using JSON columns from '{}'",
                                                                table_alias.0, is_multi_type_vlp, is_pattern_combinations, cte_alias
                                                            );
                                                            let position = if gr.left_connection
                                                                == table_alias.0
                                                            {
                                                                "start"
                                                            } else {
                                                                "end"
                                                            };
                                                            // Emit JSON properties column
                                                            select_items.push(SelectItem {
                                                                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                    table_alias: RenderTableAlias(cte_alias.to_string()),
                                                                    column: PropertyValue::Column(format!("{}_properties", position)),
                                                                }),
                                                                col_alias: Some(ColumnAlias(format!("{}.properties", table_alias.0))),
                                                            });
                                                            // Emit ID column
                                                            select_items.push(SelectItem {
                                                                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                    table_alias: RenderTableAlias(cte_alias.to_string()),
                                                                    column: PropertyValue::Column(format!("{}_id", position)),
                                                                }),
                                                                col_alias: Some(ColumnAlias(format!("{}.id", table_alias.0))),
                                                            });
                                                            // Emit type column
                                                            select_items.push(SelectItem {
                                                                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                    table_alias: RenderTableAlias(cte_alias.to_string()),
                                                                    column: PropertyValue::Column(format!("{}_type", position)),
                                                                }),
                                                                col_alias: Some(ColumnAlias(format!("{}.__label__", table_alias.0))),
                                                            });
                                                            continue; // skip expand_base_table_entity
                                                        }
                                                    }
                                                }
                                                // Base table: use schema + logical plan table alias
                                                self.expand_base_table_entity(
                                                    &table_alias.0,
                                                    typed_var,
                                                    &mut select_items,
                                                    Some(plan_ctx),
                                                );
                                            }
                                            VariableSource::Cte { cte_name, .. } => {
                                                // CTE: parse CTE name, compute FROM alias, expand
                                                self.expand_cte_entity(
                                                    &table_alias.0,
                                                    typed_var,
                                                    cte_name,
                                                    Some(plan_ctx),
                                                    &mut select_items,
                                                );
                                            }
                                            _ => {
                                                log::debug!("⚠️ Entity variable '{}' has unexpected source, treating as scalar", table_alias.0);
                                                select_items.push(SelectItem {
                                                    expression: RenderExpr::ColumnAlias(
                                                        ColumnAlias(table_alias.0.clone()),
                                                    ),
                                                    col_alias: item
                                                        .col_alias
                                                        .as_ref()
                                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                                });
                                            }
                                        }
                                    }
                                    Some(typed_var) if typed_var.is_scalar() => {
                                        log::trace!(
                                            "  ✓ Found SCALAR variable '{}'",
                                            table_alias.0
                                        );
                                        // Scalar - single item, no expansion
                                        match &typed_var.source() {
                                            VariableSource::Cte { cte_name, .. } => {
                                                self.expand_cte_scalar(
                                                    &table_alias.0,
                                                    cte_name,
                                                    &mut select_items,
                                                );
                                            }
                                            _ => {
                                                // Base table scalar or other
                                                select_items.push(SelectItem {
                                                    expression: RenderExpr::ColumnAlias(
                                                        ColumnAlias(table_alias.0.clone()),
                                                    ),
                                                    col_alias: item
                                                        .col_alias
                                                        .as_ref()
                                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                                });
                                            }
                                        }
                                    }
                                    Some(typed_var) if typed_var.is_path() => {
                                        // Path variable - expand to tuple of path components
                                        // Handles both VLP (variable-length) and fixed single-hop paths
                                        log::debug!(
                                            "🔍 Found PATH variable '{}', calling expand_path_variable",
                                            table_alias.0
                                        );
                                        self.expand_path_variable(
                                            &table_alias.0,
                                            typed_var,
                                            &mut select_items,
                                            Some(plan_ctx),
                                        );
                                    }
                                    _ => {
                                        log::trace!("  ✗ Variable '{}' NOT FOUND or not a recognized type in plan_ctx", table_alias.0);
                                        // Unknown variable - check if it's a path by looking for GraphRel
                                        if let Some(graph_rel) =
                                            self.find_graph_rel_for_path(&table_alias.0)
                                        {
                                            log::info!(
                                                "🔍 Found unregistered path variable '{}' in GraphRel, expanding with actual aliases",
                                                table_alias.0
                                            );
                                            // Create a minimal TypedVariable for path expansion
                                            // The expand_path_variable will use find_graph_rel_for_path again to get aliases
                                            use crate::query_planner::typed_variable::{
                                                PathVariable, TypedVariable, VariableSource,
                                            };
                                            let path_var = TypedVariable::Path(PathVariable {
                                                source: VariableSource::Match,
                                                start_node: Some(graph_rel.left_connection.clone()),
                                                end_node: Some(graph_rel.right_connection.clone()),
                                                relationship: Some(graph_rel.alias.clone()),
                                                length_bounds: graph_rel
                                                    .variable_length
                                                    .as_ref()
                                                    .map(|v| (v.min_hops, v.max_hops)),
                                                is_shortest_path: graph_rel
                                                    .shortest_path_mode
                                                    .is_some(),
                                            });
                                            self.expand_path_variable(
                                                &table_alias.0,
                                                &path_var,
                                                &mut select_items,
                                                Some(plan_ctx),
                                            );
                                        } else {
                                            // Really unknown - fallback to old logic
                                            log::debug!("⚠️ Variable '{}' not found in TypedVariable registry or GraphRel, using fallback logic", table_alias.0);
                                            self.fallback_table_alias_expansion(
                                                table_alias,
                                                item,
                                                &mut select_items,
                                            );
                                        }
                                    }
                                }
                            } else {
                                // No PlanCtx available - check if it's a path by looking for GraphRel
                                if let Some(graph_rel) =
                                    self.find_graph_rel_for_path(&table_alias.0)
                                {
                                    log::info!(
                                        "🔍 Found unregistered path variable '{}' in GraphRel (no plan_ctx), expanding with actual aliases",
                                        table_alias.0
                                    );
                                    // Create a minimal TypedVariable for path expansion
                                    use crate::query_planner::typed_variable::{
                                        PathVariable, TypedVariable, VariableSource,
                                    };
                                    let path_var = TypedVariable::Path(PathVariable {
                                        source: VariableSource::Match,
                                        start_node: Some(graph_rel.left_connection.clone()),
                                        end_node: Some(graph_rel.right_connection.clone()),
                                        relationship: Some(graph_rel.alias.clone()),
                                        length_bounds: graph_rel
                                            .variable_length
                                            .as_ref()
                                            .map(|v| (v.min_hops, v.max_hops)),
                                        is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                                    });
                                    self.expand_path_variable(
                                        &table_alias.0,
                                        &path_var,
                                        &mut select_items,
                                        None, // No plan_ctx available
                                    );
                                } else {
                                    log::warn!(
                                        "⚠️ No PlanCtx available for '{}' and no GraphRel found, using fallback logic",
                                        table_alias.0
                                    );
                                    self.fallback_table_alias_expansion(
                                        table_alias,
                                        item,
                                        &mut select_items,
                                    );
                                }
                            }
                        }

                        // Case 2: PropertyAccessExp with wildcard (e.g., RETURN n.*)
                        LogicalExpr::PropertyAccessExp(prop) if prop.column.raw() == "*" => {
                            log::info!(
                                "🔍 Expanding PropertyAccessExp('{}.*') to properties",
                                prop.table_alias.0
                            );

                            // Multi-type nodes: use JSON columns instead of individual properties
                            // Handles both VLP multi-type (labels > 1) and pattern_combinations paths
                            if let Some(gr) = self.find_graph_rel_for_alias(&prop.table_alias.0) {
                                let is_multi_type_vlp =
                                    gr.labels.as_ref().is_some_and(|l| l.len() > 1);
                                let has_pattern_combinations = gr.pattern_combinations.is_some();

                                if is_multi_type_vlp || has_pattern_combinations {
                                    // For VLP multi-type, CTE alias is "t"
                                    // For pattern_combinations, CTE alias is the relationship alias
                                    let cte_alias = if has_pattern_combinations {
                                        gr.alias.clone()
                                    } else {
                                        "t".to_string()
                                    };
                                    let position = if gr.left_connection == prop.table_alias.0 {
                                        "start"
                                    } else {
                                        "end"
                                    };
                                    log::info!(
                                        "🎯 Multi-type node '{}' detected (pattern_combinations={}, VLP={}), using CTE '{}' {}_properties",
                                        prop.table_alias.0, has_pattern_combinations, is_multi_type_vlp, cte_alias, position
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column(format!(
                                                "{}_properties",
                                                position
                                            )),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.properties",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column(format!(
                                                "{}_id",
                                                position
                                            )),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.id",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias),
                                            column: PropertyValue::Column(format!(
                                                "{}_type",
                                                position
                                            )),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.__label__",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    continue;
                                }
                            }

                            // Multi-type VLP relationship: use CTE relationship columns
                            // Only applies to VLP multi-type path (labels > 1 + variable_length)
                            // NOT to pattern_combinations path which uses regular table JOINs
                            if let Some(gr) = self.find_graph_rel_by_rel_alias(&prop.table_alias.0)
                            {
                                if let Some(ref labels) = gr.labels {
                                    if labels.len() > 1
                                        && gr.variable_length.is_some()
                                        && gr.pattern_combinations.is_none()
                                    {
                                        log::info!(
                                            "🎯 Multi-type VLP relationship '{}' detected ({} types), using CTE columns",
                                            prop.table_alias.0, labels.len()
                                        );
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "path_relationships".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.type",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "rel_properties".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.properties",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "start_id".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.start_id",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "end_id".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.end_id",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        // Include start_type/end_type for polymorphic schemas
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "start_type".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.start_type",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "end_type".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.end_type",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        continue;
                                    }
                                }
                            }

                            // Pattern combinations relationship: use CTE relationship columns
                            if let Some(gr) = self.find_graph_rel_by_rel_alias(&prop.table_alias.0)
                            {
                                if gr.pattern_combinations.is_some() {
                                    let cte_alias = gr.alias.clone();
                                    log::info!(
                                        "🎯 Pattern combinations relationship '{}' detected, using CTE '{}' columns",
                                        prop.table_alias.0, cte_alias
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column(
                                                "path_relationships".to_string(),
                                            ),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.type",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column(
                                                "rel_properties".to_string(),
                                            ),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.properties",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column("start_id".to_string()),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.start_id",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column("end_id".to_string()),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.end_id",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias.clone()),
                                            column: PropertyValue::Column("start_type".to_string()),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.start_type",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(cte_alias),
                                            column: PropertyValue::Column("end_type".to_string()),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.end_type",
                                            prop.table_alias.0
                                        ))),
                                    });
                                    continue;
                                }
                            }

                            // CTE reference wildcard: alias was renamed to CTE name
                            // (e.g., "a" → "with_a_cte_0" by rewrite_logical_expr_cte_refs)
                            // Reverse-map to original alias, then expand using CTE columns.
                            if let Some(original_alias) =
                                self.find_cte_original_alias(&prop.table_alias.0)
                            {
                                let cte_props =
                                    crate::server::query_context::get_all_cte_properties(
                                        &original_alias,
                                    );
                                if !cte_props.is_empty() {
                                    log::info!(
                                        "🔍 CTE wildcard expansion: '{}' → original alias '{}' with {} properties",
                                        prop.table_alias.0, original_alias, cte_props.len()
                                    );
                                    for (prop_name, cte_column) in &cte_props {
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias(
                                                        original_alias.clone(),
                                                    ),
                                                    column: PropertyValue::Column(
                                                        cte_column.clone(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.{}",
                                                original_alias, prop_name
                                            ))),
                                        });
                                    }
                                    continue;
                                }
                            }

                            // Check if this is a denormalized edge alias mapping
                            // First try plan_ctx (populated during planning), then fall back to
                            // logical plan inspection, then QUERY_CONTEXT
                            let mapped_alias = if let Some(ctx) = plan_ctx {
                                if let Some((edge_alias, _is_from, _label, _type)) =
                                    ctx.get_denormalized_alias_info(&prop.table_alias.0)
                                {
                                    edge_alias.clone()
                                } else {
                                    // plan_ctx doesn't have it (e.g., Union branches use cloned ctx)
                                    // Fall back to inspecting the logical plan's GraphRel
                                    self.find_denormalized_edge_alias(&prop.table_alias.0)
                                        .or_else(|| {
                                            crate::render_plan::get_denormalized_alias_mapping(
                                                &prop.table_alias.0,
                                            )
                                        })
                                        .unwrap_or_else(|| prop.table_alias.0.clone())
                                }
                            } else {
                                self.find_denormalized_edge_alias(&prop.table_alias.0)
                                    .or_else(|| {
                                        crate::render_plan::get_denormalized_alias_mapping(
                                            &prop.table_alias.0,
                                        )
                                    })
                                    .unwrap_or_else(|| prop.table_alias.0.clone())
                            };

                            if mapped_alias != prop.table_alias.0 {
                                log::info!(
                                    "🔍 Denormalized alias mapping found for wildcard: '{}' → '{}'",
                                    prop.table_alias.0,
                                    mapped_alias
                                );
                            }

                            // For denormalized nodes, look up properties using original node alias
                            // but render with the edge table alias
                            let property_lookup_alias = if mapped_alias != prop.table_alias.0 {
                                &prop.table_alias.0
                            } else {
                                &mapped_alias
                            };

                            let (properties, table_alias_for_render) =
                                match self.get_properties_with_table_alias(property_lookup_alias) {
                                    Ok((props, _)) => {
                                        let props: Vec<(String, String)> = props;
                                        if props.is_empty() {
                                            (None, prop.table_alias.0.clone())
                                        } else {
                                            (Some(props), mapped_alias)
                                        }
                                    }
                                    Err(_) => (None, prop.table_alias.0.clone()),
                                };

                            if let Some(properties) = properties {
                                // Expand to multiple SelectItems, one per property
                                for (prop_name, col_name) in properties {
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(
                                                table_alias_for_render.clone(),
                                            ),
                                            column: PropertyValue::Column(col_name),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            prop.table_alias.0, prop_name
                                        ))),
                                    });
                                }

                                log::info!(
                                    "✅ Expanded '{}.*' to {} properties",
                                    prop.table_alias.0,
                                    select_items.len()
                                );
                            } else {
                                log::warn!(
                                    "⚠️ No properties found for alias '{}'",
                                    prop.table_alias.0
                                );
                            }
                        }

                        // Case 3: CteEntityRef (e.g., RETURN u when u comes from WITH)
                        // CteEntityRef contains the CTE name and the prefixed columns
                        LogicalExpr::CteEntityRef(cte_ref) => {
                            log::info!(
                                "🔍 Expanding CteEntityRef('{}') from CTE '{}' with {} columns",
                                cte_ref.alias,
                                cte_ref.cte_name,
                                cte_ref.columns.len()
                            );

                            if cte_ref.columns.is_empty() {
                                log::debug!("⚠️ CteEntityRef '{}' has no columns - falling back to TableAlias", cte_ref.alias);
                                select_items.push(SelectItem {
                                    expression: RenderExpr::TableAlias(RenderTableAlias(
                                        cte_ref.alias.clone(),
                                    )),
                                    col_alias: item
                                        .col_alias
                                        .as_ref()
                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                });
                                continue;
                            }

                            // The CTE was aliased as the original variable name (e.g., FROM cte AS u)
                            // So we use the alias as the table reference
                            let table_alias_to_use = cte_ref.alias.clone();

                            // Expand to multiple SelectItems, one per CTE column
                            // CTE columns are already prefixed (u_name, u_email, etc.)
                            for col_name in &cte_ref.columns {
                                // Extract property name from prefixed column
                                // Try new p{N} format first, fall back to old underscore prefix strip
                                let prop_name =
                                    if let Some((_alias, property)) = parse_cte_column(col_name) {
                                        property
                                    } else {
                                        col_name
                                            .strip_prefix(&format!("{}_", cte_ref.alias))
                                            .unwrap_or(col_name)
                                            .to_string()
                                    };

                                select_items.push(SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: RenderTableAlias(table_alias_to_use.clone()),
                                        column: PropertyValue::Column(col_name.clone()),
                                    }),
                                    col_alias: Some(ColumnAlias(format!(
                                        "{}.{}",
                                        cte_ref.alias, prop_name
                                    ))),
                                });
                            }

                            log::info!(
                                "✅ Expanded CteEntityRef '{}' to {} columns",
                                cte_ref.alias,
                                cte_ref.columns.len()
                            );
                        }

                        // Case 4: PropertyAccessExp - special handling for denormalized nodes
                        LogicalExpr::PropertyAccessExp(prop_access) => {
                            let cypher_alias = &prop_access.table_alias.0;
                            let col_name = prop_access.column.raw(); // This is the resolved column name (e.g., "OriginCityName")

                            log::debug!(
                                "🔍🔍🔍 Case 4 PropertyAccessExp: cypher_alias='{}', col_name='{}'",
                                cypher_alias,
                                col_name
                            );

                            // Multi-type relationship backed by a `pattern_union` CTE:
                            // the CTE projects this relationship's properties as direct
                            // columns under their PROPERTY names (e.g. `... AS timestamp`).
                            // Per CLAUDE.md §2 (forward resolution through a CTE barrier),
                            // reference the property-named CTE column directly — do NOT
                            // remap it to the physical schema column via
                            // get_properties_with_table_alias() below (that would emit
                            // e.g. `r.ts`, which does not exist in the CTE → CH Code 47).
                            if crate::render_plan::cte_extraction::is_pattern_union_rel_alias(
                                cypher_alias,
                                self,
                            ) {
                                log::info!(
                                    "🔀 pattern_union relationship property '{}.{}': passing through property-named CTE column (no physical-column remap)",
                                    cypher_alias,
                                    col_name
                                );
                                select_items.push(SelectItem {
                                    expression: item.expression.clone().try_into()?,
                                    col_alias: item
                                        .col_alias
                                        .as_ref()
                                        .map(|ca| ca.clone().try_into())
                                        .transpose()?,
                                });
                                continue;
                            }

                            // 🔧 FIX: Check if this is a multi-type VLP endpoint first
                            // Multi-type VLP endpoints need JSON extraction, not direct column access
                            if let Some(gr) = self.find_graph_rel_for_alias(cypher_alias) {
                                let is_multi_type_vlp =
                                    gr.labels.as_ref().is_some_and(|l| l.len() > 1);
                                let has_pattern_combinations = gr.pattern_combinations.is_some();

                                if is_multi_type_vlp || has_pattern_combinations {
                                    // For VLP multi-type, CTE alias is "t"
                                    // For pattern_combinations, CTE alias is the relationship alias
                                    let cte_alias = if has_pattern_combinations {
                                        gr.alias.clone()
                                    } else {
                                        "t".to_string()
                                    };
                                    let position = if gr.left_connection == *cypher_alias {
                                        "start"
                                    } else {
                                        "end"
                                    };

                                    log::info!(
                                        "🎯 Multi-type VLP property access: '{}.{}' -> extracting from {}_properties JSON in CTE '{}'",
                                        cypher_alias, col_name, position, cte_alias
                                    );

                                    // Extract property from JSON blob.
                                    // CH:        JSONExtractString(json, 'name')
                                    // Databricks: get_json_object(json, '$.name')
                                    // See `json_extract_field_arg` for the argument rewrite.
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                                            name: current_function_mapper()
                                                .json_extract_string()
                                                .to_string(),
                                            args: vec![
                                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                                    table_alias: RenderTableAlias(
                                                        cte_alias.clone(),
                                                    ),
                                                    column: PropertyValue::Column(format!(
                                                        "{}_properties",
                                                        position
                                                    )),
                                                }),
                                                RenderExpr::Literal(Literal::String(
                                                    json_extract_field_arg(col_name),
                                                )),
                                            ],
                                        }),
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone()))
                                            .or_else(|| {
                                                Some(ColumnAlias(format!(
                                                    "{}.{}",
                                                    cypher_alias, col_name
                                                )))
                                            }),
                                    });
                                    continue;
                                }
                            }

                            // ✅ DETERMINISTIC LOGIC: Check if this variable comes from a CTE
                            // VLP endpoint nodes and WITH clause variables are CTE-sourced
                            // For CTE variables, properties should reference the CTE alias directly,
                            // NOT use denormalized property table resolution
                            if let Some(ctx) = plan_ctx {
                                if let Some(typed_var) = ctx.lookup_variable(cypher_alias) {
                                    if matches!(
                                        typed_var.source(),
                                        crate::query_planner::typed_variable::VariableSource::Cte { .. }
                                    ) {
                                        log::debug!(
                                            "Variable '{}' is CTE-sourced - skipping get_properties_with_table_alias",
                                            cypher_alias
                                        );
                                        // Pass through as-is - will use CTE alias from PropertyAccessExp
                                        select_items.push(SelectItem {
                                            expression: item.expression.clone().try_into()?,
                                            col_alias: item
                                                .col_alias
                                                .as_ref()
                                                .map(|ca| ca.clone().try_into())
                                                .transpose()?,
                                        });
                                        continue;
                                    }
                                }
                            }

                            log::debug!("   → trying get_properties_with_table_alias...");

                            // For denormalized nodes in edges, we need to get the actual table alias
                            // AND map the property name to the actual column name
                            if let Ok((properties, table_alias_override)) =
                                self.get_properties_with_table_alias(cypher_alias)
                            {
                                // #492/#491 interaction fix: `properties` may have been
                                // matched structurally against a DIFFERENT edge than
                                // `table_alias_override` (e.g. an OPTIONAL pattern's
                                // registry entry was kept from an earlier required
                                // pattern per #491, but the structural walk still finds
                                // the optional GraphRel first). Re-derive from the
                                // REGISTERED edge when possible so column and alias
                                // always come from the same source.
                                let properties = table_alias_override
                                    .as_deref()
                                    .and_then(|edge_alias| {
                                        properties_for_registered_edge(
                                            self,
                                            cypher_alias,
                                            edge_alias,
                                        )
                                    })
                                    .unwrap_or(properties);

                                // Look up the column name for this property.
                                // Match by Cypher property name first, then by DB column
                                // name (schema mapping may have already rewritten the
                                // expression), then cross-side (#492): in a denormalized
                                // multi-hop chain the schema rewrite may have bound the
                                // shared middle node to a DIFFERENT adjacent edge's side
                                // (e.g. b.code → t1's `Dest`) than the edge this lookup
                                // resolved (t2, whose side for b is `Origin`). Translate
                                // the column through the node schema so column and table
                                // alias come from the SAME edge.
                                let mapped_column = properties
                                    .iter()
                                    .find(|(prop_name, _)| prop_name == col_name)
                                    .map(|(_, col)| col.clone())
                                    .or_else(|| {
                                        // Only for denormalized bindings (the lookup
                                        // returned an edge alias override) — standard
                                        // nodes (incl. expression-valued property
                                        // mappings) keep the pass-through behavior.
                                        if table_alias_override.is_none() {
                                            None
                                        } else if properties.iter().any(|(_, col)| col == col_name)
                                        {
                                            // Already the correct side's DB column
                                            Some(col_name.to_string())
                                        } else {
                                            translate_denorm_cross_side_column(
                                                self,
                                                cypher_alias,
                                                col_name,
                                                &properties,
                                            )
                                        }
                                    });

                                if let Some(actual_column) = mapped_column {
                                    let table_alias_to_use = table_alias_override
                                        .unwrap_or_else(|| cypher_alias.to_string());
                                    log::debug!(
                                        "🔍 Mapped property '{}.{}' to column '{}.{}'",
                                        cypher_alias,
                                        col_name,
                                        table_alias_to_use,
                                        actual_column
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(table_alias_to_use),
                                            column: PropertyValue::Column(actual_column),
                                        }),
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone())),
                                    });
                                    continue;
                                } else if let Some(actual_table_alias) = table_alias_override {
                                    // Has actual_table_alias but property not found in mapping
                                    // Use original column name with the overridden alias
                                    log::debug!(
                                        "🔍 Using actual table alias '{}' for {}.{} (property not in mapping)",
                                        actual_table_alias,
                                        cypher_alias,
                                        col_name
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(actual_table_alias),
                                            column: PropertyValue::Column(col_name.to_string()),
                                        }),
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone())),
                                    });
                                    continue;
                                }
                            }

                            // Default handling: pass through the PropertyAccessExp as-is
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }

                        // Case 5: id()/elementId() function - transform to ID column access
                        // The id() function needs special handling because:
                        // 1. We preserve ScalarFnCall("id") in LogicalPlan for metadata extraction
                        // 2. But for SQL we need the actual ID column value to compute encoded ID
                        // elementId() is only handled here for pattern_union
                        // endpoints (#466 round 4.5) — elsewhere it keeps its
                        // pre-existing behavior (Bolt id_rewriter / result
                        // transformer handle Browser traffic).
                        LogicalExpr::ScalarFnCall(fn_call)
                            if (fn_call.name.eq_ignore_ascii_case("id")
                                || fn_call.name.eq_ignore_ascii_case("elementid"))
                                && fn_call.args.len() == 1 =>
                        {
                            let is_element_id = fn_call.name.eq_ignore_ascii_case("elementid");
                            // The argument is normally a bare TableAlias; some
                            // passes rewrite it to a wildcard PropertyAccess
                            // (`o.*`) — accept both.
                            let arg_alias = match &fn_call.args[0] {
                                LogicalExpr::TableAlias(a) => Some(a.clone()),
                                LogicalExpr::PropertyAccessExp(p) if p.column.raw() == "*" => {
                                    Some(p.table_alias.clone())
                                }
                                _ => None,
                            };
                            if let Some(ref alias) = arg_alias {
                                log::info!(
                                    "🔍 SelectBuilder: {}({}) - transforming to ID column access",
                                    fn_call.name,
                                    alias.0
                                );

                                // #466 round 4: a pattern_union endpoint binds a
                                // DIFFERENT label per branch — a single label's id
                                // column is NULL on the other branches (and the raw
                                // node alias does not even exist in the outer query,
                                // whose FROM is the CTE). Use the CTE's
                                // label-agnostic start_id/end_id instead
                                // (left_connection binds start, right binds end;
                                // real ids, not the toInt64(0) placeholder the
                                // generic function mapping would emit). For
                                // elementId(), rebuild the codebase's composite
                                // `Label:id-` format (generate_node_element_id)
                                // from the CTE's type + id columns.
                                if let Some((rel_alias, is_left)) =
                                    self.pattern_union_endpoint_role(&alias.0)
                                {
                                    let (id_col, type_col) = if is_left {
                                        ("start_id", "start_type")
                                    } else {
                                        ("end_id", "end_type")
                                    };
                                    log::debug!(
                                        "🔍 SelectBuilder: {}({}) -> {}.{} (pattern_union endpoint)",
                                        fn_call.name,
                                        alias.0,
                                        rel_alias,
                                        id_col
                                    );
                                    let expression = if is_element_id {
                                        RenderExpr::Raw(format!(
                                            "concat({rel_alias}.{type_col}, ':', {rel_alias}.{id_col}, '-')"
                                        ))
                                    } else {
                                        RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(rel_alias),
                                            column: PropertyValue::Column(id_col.to_string()),
                                        })
                                    };
                                    select_items.push(SelectItem {
                                        expression,
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone())),
                                    });
                                    continue;
                                }

                                // Non-endpoint elementId(): keep pre-existing
                                // behavior (pass through unchanged; Bolt-layer
                                // handling / plain-path semantics untouched).
                                if is_element_id {
                                    select_items.push(SelectItem {
                                        expression: item.expression.clone().try_into()?,
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ca.clone().try_into())
                                            .transpose()?,
                                    });
                                    continue;
                                }

                                // Get schema from plan_ctx to find the ID column
                                if let Some(ctx) = plan_ctx {
                                    if let Some(typed_var) = ctx.lookup_variable(&alias.0) {
                                        let id_column = match typed_var {
                                            TypedVariable::Node(node_var) => {
                                                if let Some(label) = node_var.labels.first() {
                                                    ctx.schema().node_schema(label).ok().and_then(
                                                        |ns| {
                                                            ns.node_id
                                                                .columns()
                                                                .first()
                                                                .map(|s| s.to_string())
                                                        },
                                                    )
                                                } else {
                                                    None
                                                }
                                            }
                                            TypedVariable::Relationship(rel_var) => {
                                                if let Some(rel_type) = rel_var.rel_types.first() {
                                                    ctx.schema()
                                                        .get_rel_schema(rel_type)
                                                        .ok()
                                                        .and_then(|rs| {
                                                            if let Some(ref edge_id) = rs.edge_id {
                                                                edge_id
                                                                    .columns()
                                                                    .first()
                                                                    .map(|s| s.to_string())
                                                            } else {
                                                                Some(rs.from_id.to_string())
                                                            }
                                                        })
                                                } else {
                                                    None
                                                }
                                            }
                                            _ => None,
                                        };

                                        if let Some(id_col) = id_column {
                                            // For denormalized nodes, resolve alias and column
                                            // through from/to_node_properties (e.g., a.code → r.Origin).
                                            // edge_alias is Some for coupled edge nodes, None for standalone.
                                            let (resolved_alias, resolved_col) = match self
                                                .get_properties_with_table_alias(&alias.0)
                                            {
                                                Ok((props, edge_alias_opt))
                                                    if !props.is_empty() =>
                                                {
                                                    // Find the mapped column for this id property
                                                    let mapped = props
                                                        .iter()
                                                        .find(|(prop_name, _)| *prop_name == id_col)
                                                        .map(|(_, col)| col.clone())
                                                        .unwrap_or_else(|| id_col.clone());
                                                    // For standalone denormalized nodes, edge_alias is None;
                                                    // fall back to the original node alias.
                                                    let resolved_alias = edge_alias_opt
                                                        .unwrap_or_else(|| alias.0.clone());
                                                    (resolved_alias, mapped)
                                                }
                                                _ => (alias.0.clone(), id_col),
                                            };

                                            log::debug!(
                                                "🔍 SelectBuilder: id({}) -> {}.{}",
                                                alias.0,
                                                resolved_alias,
                                                resolved_col
                                            );
                                            select_items.push(SelectItem {
                                                expression: RenderExpr::PropertyAccessExp(
                                                    PropertyAccess {
                                                        table_alias: RenderTableAlias(
                                                            resolved_alias,
                                                        ),
                                                        column: PropertyValue::Column(resolved_col),
                                                    },
                                                ),
                                                col_alias: item
                                                    .col_alias
                                                    .as_ref()
                                                    .map(|ca| ColumnAlias(ca.0.clone())),
                                            });
                                            continue;
                                        }
                                    }
                                }

                                // Fallback: couldn't resolve ID column, pass through as-is
                                log::debug!("🔍 SelectBuilder: id({}) - couldn't resolve ID column, passing through", alias.0);
                            }

                            // Fallback for non-alias argument or failed resolution
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }

                        // Case 6a: Expression containing aggregate call(s) — a bare
                        // aggregate OR one nested in an operator/scalar-fn/CASE
                        // wrapper (e.g. `count(b) + 0`). Resolve denormalized node
                        // references inside it (#493). The planner rewrites
                        // count(b) → count(b.<node_id>) for NULL-correct
                        // OPTIONAL counting, but for a denormalized node `b` has no
                        // physical table: the reference must resolve onto the owning
                        // edge's embedded column (e.g. count(b.code) →
                        // count(t1.dest_code)), exactly as Case 4 does for a bare
                        // property access. Without this the alias leaks unresolved →
                        // ClickHouse UNKNOWN_IDENTIFIER. Standard/own-table nodes are
                        // untouched (no override alias).
                        expr_with_agg
                            if crate::query_planner::logical_expr::visitors::HasAggregateCheck::check(
                                expr_with_agg,
                            ) =>
                        {
                            let mut expr: RenderExpr = item.expression.clone().try_into()?;
                            self.resolve_denorm_refs_in_expr(&mut expr, plan_ctx);
                            select_items.push(SelectItem {
                                expression: expr,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }

                        // Case 6: Other regular expressions (function call, literals, etc.)
                        _ => {
                            log::debug!(
                                "🔍 SelectBuilder Case 6 (Other): Expression type = {:?}",
                                item.expression
                            );
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }
                    }
                }

                select_items
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                log::debug!(
                    "🔍 GraphJoins.extract_select_items: input type={:?}",
                    std::mem::discriminant(graph_joins.input.as_ref())
                );
                graph_joins.input.extract_select_items(plan_ctx)?
            }
            LogicalPlan::GroupBy(group_by) => {
                // GroupBy doesn't define select items, extract from input
                group_by.input.extract_select_items(plan_ctx)?
            }
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
            LogicalPlan::Unwind(u) => u.input.extract_select_items(plan_ctx)?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine select items from both sides
                log::trace!("🔍 CartesianProduct.extract_select_items START");
                let left_items = cp.left.extract_select_items(plan_ctx)?;
                log::debug!(
                    "🔍 CartesianProduct.extract_select_items: left side returned {} items",
                    left_items.len()
                );
                let right_items = cp.right.extract_select_items(plan_ctx)?;
                log::debug!(
                    "🔍 CartesianProduct.extract_select_items: right side returned {} items, combining...",
                    right_items.len()
                );
                let mut items = left_items;
                items.extend(right_items);
                log::debug!(
                    "🔍 CartesianProduct.extract_select_items DONE: total {} items",
                    items.len()
                );
                items
            }
            LogicalPlan::GraphNode(graph_node) => {
                let mut items = graph_node.input.extract_select_items(plan_ctx)?;
                // Qualify bare Column expressions with the node's alias.
                // ViewScan returns unqualified Column("name") which the SQL generator
                // would resolve via heuristic (defaulting to "t"). By qualifying here
                // with the correct Cypher alias (e.g., "friend", "tag"), the SQL
                // generator emits correct table-qualified references.
                if !graph_node.alias.is_empty() {
                    for item in &mut items {
                        if let RenderExpr::Column(Column(ref prop_val)) = item.expression {
                            let col_name = prop_val.raw().to_string();
                            item.expression = RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(graph_node.alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        col_name.clone(),
                                    ),
                            });
                            // Note: we do NOT qualify the col_alias here.
                            // The alias becomes a column name in UNION subqueries,
                            // and the outer SELECT references it. Changing the alias
                            // would break outer references. Duplicate aliases
                            // (complex-12) are a known chdb limitation (#258).
                        }
                    }
                }
                items
            }
            LogicalPlan::WithClause(wc) => {
                log::trace!("🔍 WithClause.extract_select_items: calling extract on input");
                let items = wc.input.extract_select_items(plan_ctx)?;
                log::debug!(
                    "🔍 WithClause.extract_select_items DONE: extracted {} items from input plan",
                    items.len()
                );
                for (idx, item) in items.iter().enumerate() {
                    log::debug!(
                        "🔍   Item[{}]: alias={:?}",
                        idx,
                        item.col_alias.as_ref().map(|a| a.0.clone())
                    );
                }
                items
            }

            // Write variants — recurse into preceding read pipeline. The write
            // clause itself does not contribute SELECT items (Phase 2 renders
            // counters separately).
            LogicalPlan::Create(c) => c.input.extract_select_items(plan_ctx)?,
            LogicalPlan::SetProperties(sp) => sp.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Delete(d) => d.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Remove(r) => r.input.extract_select_items(plan_ctx)?,
        };

        Ok(select_items)
    }
}

// ============================================================================
// Helper Methods for TypedVariable-Based Resolution
// ============================================================================

impl LogicalPlan {
    /// Resolve denormalized-node property references nested inside an
    /// expression (aggregate arguments, and the DISTINCT/scalar wrappers the
    /// planner builds around them) onto the owning edge's table alias and
    /// embedded column — the recursive sibling of Case 4's bare
    /// `PropertyAccessExp` handling (#493).
    ///
    /// Only references that resolve to an OVERRIDE table alias (i.e. the node
    /// is embedded in an edge table / denorm-scan binding) are rewritten;
    /// own-table nodes, CTE-sourced variables, and pattern_union relationship
    /// aliases are left untouched, so standard schemas render byte-identically.
    fn resolve_denorm_refs_in_expr(
        &self,
        expr: &mut RenderExpr,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) {
        match expr {
            RenderExpr::PropertyAccessExp(pa) => {
                let alias = pa.table_alias.0.clone();
                // CTE-sourced variables resolve forward through their CTE
                // columns (CLAUDE.md rule 2) — never remap them here.
                if let Some(ctx) = plan_ctx {
                    if let Some(typed_var) = ctx.lookup_variable(&alias) {
                        if matches!(typed_var.source(), VariableSource::Cte { .. }) {
                            return;
                        }
                    }
                }
                // pattern_union CTEs project properties under their property
                // names — same pass-through as Case 4.
                if crate::render_plan::cte_extraction::is_pattern_union_rel_alias(&alias, self) {
                    return;
                }
                if let Ok((props, Some(actual_alias))) =
                    self.get_properties_with_table_alias(&alias)
                {
                    let col_name = pa.column.raw().to_string();
                    // GATE (review of #493/#475): rebind the table alias ONLY
                    // when the reference actually RESOLVES in this binding's
                    // property set — either by Cypher property name
                    // (count(b.code) → t1.dest_code) or by already-mapped
                    // column value (count(b.dest_code) → t1.dest_code, column
                    // kept). An unconditional rebind re-attributed anchor
                    // references that do NOT live on the binding (e.g. `a.uid`
                    // resolved through the anchor's own conn_log table while
                    // `a` is bound to the dns_log edge scan) onto the
                    // LEFT-JOINed edge alias: valid-but-wrong SQL when the
                    // edge happens to have a same-named column (count(r.uid):
                    // NULL-extended → 0 instead of 1 on OPTIONAL-miss rows),
                    // invalid SQL when it doesn't (r.port). Unresolvable refs
                    // pass through untouched for the anchor-CTE machinery to
                    // handle.
                    if let Some((_, mapped)) = props.iter().find(|(p, _)| *p == col_name) {
                        pa.column = PropertyValue::Column(mapped.clone());
                        pa.table_alias = RenderTableAlias(actual_alias.clone());
                    } else if props.iter().any(|(_, c)| *c == col_name) {
                        // Already-mapped column value — keep the column name.
                        pa.table_alias = RenderTableAlias(actual_alias.clone());
                    } else {
                        log::debug!(
                            "🔍 resolve_denorm_refs_in_expr: {}.{} does not resolve on binding '{}' — leaving untouched",
                            alias,
                            col_name,
                            actual_alias
                        );
                        return;
                    }
                    log::debug!(
                        "🔍 resolve_denorm_refs_in_expr: {}.{} → {}.{}",
                        alias,
                        col_name,
                        actual_alias,
                        pa.column.raw()
                    );
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                for arg in &mut agg.args {
                    self.resolve_denorm_refs_in_expr(arg, plan_ctx);
                }
            }
            RenderExpr::ScalarFnCall(sf) => {
                for arg in &mut sf.args {
                    self.resolve_denorm_refs_in_expr(arg, plan_ctx);
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                for operand in &mut op.operands {
                    self.resolve_denorm_refs_in_expr(operand, plan_ctx);
                }
            }
            RenderExpr::Case(case) => {
                if let Some(ref mut e) = case.expr {
                    self.resolve_denorm_refs_in_expr(e, plan_ctx);
                }
                for (cond, result) in &mut case.when_then {
                    self.resolve_denorm_refs_in_expr(cond, plan_ctx);
                    self.resolve_denorm_refs_in_expr(result, plan_ctx);
                }
                if let Some(ref mut e) = case.else_expr {
                    self.resolve_denorm_refs_in_expr(e, plan_ctx);
                }
            }
            RenderExpr::List(items) => {
                for item in items {
                    self.resolve_denorm_refs_in_expr(item, plan_ctx);
                }
            }
            _ => {}
        }
    }

    /// Check if this plan contains a GraphRel with pattern_combinations for the given alias
    fn has_pattern_combinations_for_alias(&self, alias: &str) -> bool {
        match self {
            LogicalPlan::GraphRel(gr) => {
                if gr.alias == alias && gr.pattern_combinations.is_some() {
                    return true;
                }
                // Check recursively
                gr.left.has_pattern_combinations_for_alias(alias)
                    || gr.center.has_pattern_combinations_for_alias(alias)
                    || gr.right.has_pattern_combinations_for_alias(alias)
            }
            LogicalPlan::GraphNode(gn) => gn.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Projection(p) => p.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Filter(f) => f.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::GroupBy(g) => g.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Limit(l) => l.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Skip(s) => s.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::GraphJoins(gj) => gj.input.has_pattern_combinations_for_alias(alias),
            _ => false,
        }
    }

    /// Expand a base table entity (Node/Relationship from MATCH)
    fn expand_base_table_entity(
        &self,
        alias: &str,
        typed_var: &TypedVariable,
        select_items: &mut Vec<SelectItem>,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) {
        log::info!("✅ Expanding base table entity '{}' to properties", alias);

        // Get labels from TypedVariable
        let labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // 🔧 FIX: Check if this is a VLP relationship (variable_length) that uses a CTE
        // VLP relationships generate a vlp_{start}_{end} CTE with columns:
        // - path_relationships: array of relationship types
        // - rel_properties: array of relationship property JSON objects
        // - end_type, end_id, start_id, end_properties, hop_count
        //
        // IMPORTANT: Only use CTE columns when the current plan branch actually uses a CTE.
        // PlanCtx may list multiple rel_types for 'r' globally, but pattern_combinations
        // splits the query into single-type UNION branches that use regular table JOINs.
        // We verify by checking the GraphRel on the plan: it must have variable_length
        // (VLP CTE) or pattern_combinations (pattern_union CTE).
        if let TypedVariable::Relationship(_) = typed_var {
            let graph_rel = self.find_graph_rel_by_rel_alias(alias);
            let is_vlp = graph_rel
                .as_ref()
                .is_some_and(|gr| gr.variable_length.is_some());
            let has_pattern_combinations = graph_rel
                .as_ref()
                .is_some_and(|gr| gr.pattern_combinations.is_some());
            let uses_cte = is_vlp || has_pattern_combinations;

            // VLP relationships (single-type or multi-type) use CTE columns
            // Pattern combinations also use CTE columns
            if uses_cte {
                // Determine log message based on type count
                let type_desc = if labels.len() > 1 {
                    format!("multi-type ({} types)", labels.len())
                } else {
                    "single-type VLP".to_string()
                };
                log::info!(
                    "🎯 VLP relationship '{}' detected ({}), using CTE columns",
                    alias,
                    type_desc
                );

                // === PATTERNRESOLVER 2.0: Determine CTE alias ===
                // For pattern_combinations, CTE alias is the relationship alias (e.g., "r")
                // For regular multi-type ([:TYPE1|TYPE2]), CTE alias is "t" (VLP_CTE_FROM_ALIAS)
                let has_pattern_combinations = self.has_pattern_combinations_for_alias(alias);
                let cte_alias = if has_pattern_combinations {
                    log::info!(
                        "🔀 PatternResolver 2.0: Using relationship alias '{}' for CTE reference",
                        alias
                    );
                    alias // Pattern combinations use relationship alias
                } else {
                    "t" // VLP uses VLP_CTE_FROM_ALIAS
                };

                // Add all CTE columns needed to reconstruct the relationship
                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column("path_relationships".to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.type", alias))),
                });

                // Only add rel_properties for multi-type VLP or pattern_combinations
                //
                // IMPORTANT: Standard single-type VLP doesn't include rel_properties column.
                // This is a known limitation: edge properties are not tracked in the CTE for
                // single-type VLP patterns like (u)-[r:TYPE*1..2]->(n).
                //
                // The CTE for single-type VLP uses columns: start_id, end_id, hop_count,
                // path_relationships, path_nodes - but NOT rel_properties.
                //
                // For multi-type VLP ([:TYPE1|TYPE2]) or pattern_combinations, the CTE
                // generates rel_properties as a JSON array of relationship property objects.
                //
                // Users should not expect `RETURN r.some_property` to work with single-type VLP.
                // Workaround: Use regular relationship patterns for property access:
                //   MATCH (u)-[r:TYPE]->(n) RETURN r.property  (without *1..2)
                if labels.len() > 1 || has_pattern_combinations {
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("rel_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias(format!("{}.properties", alias))),
                    });
                }

                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column(VLP_START_ID_COLUMN.to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.start_id", alias))),
                });

                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column(VLP_END_ID_COLUMN.to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.end_id", alias))),
                });

                log::info!(
                    "✅ Expanded VLP relationship '{}' to {} CTE columns",
                    alias,
                    select_items.len()
                );
                return;
            }
        }

        // CRITICAL: Check if this alias is a FK-edge (denormalized on another table)
        // For FK-edge patterns like (u)-[r:AUTHORED]->(po), relationship r is stored ON po table
        // We need to select columns from po table but alias them as r.*
        let (actual_table_alias, is_fk_edge) = if let Some(ctx) = plan_ctx {
            if let Some((edge_alias, _is_from, _label, _type)) =
                ctx.get_denormalized_alias_info(alias)
            {
                log::info!(
                    "🔑 FK-edge detected: '{}' is denormalized on '{}'",
                    alias,
                    edge_alias
                );
                (edge_alias.clone(), true)
            } else {
                // Try global denormalized alias mapping (for SingleTableScan)
                let mapped = crate::render_plan::get_denormalized_alias_mapping(alias)
                    .unwrap_or_else(|| alias.to_string());
                (mapped, false)
            }
        } else {
            // No plan_ctx, use global mapping
            let mapped = crate::render_plan::get_denormalized_alias_mapping(alias)
                .unwrap_or_else(|| alias.to_string());
            (mapped, false)
        };

        if actual_table_alias != alias {
            log::info!(
                "🔍 {} alias mapping: '{}' → '{}'",
                if is_fk_edge {
                    "FK-edge"
                } else {
                    "Denormalized"
                },
                alias,
                actual_table_alias
            );
        }

        // For FK-edge (denormalized nodes), first try the original alias to get projected_columns
        // Then fall back to actual_table_alias for relationship tables
        let lookup_alias = if is_fk_edge {
            alias
        } else {
            &actual_table_alias
        };

        // Use plan_ctx-aware method to handle coupled edges
        match self.get_properties_with_plan_ctx(lookup_alias, plan_ctx) {
            Ok((properties, resolved_table_alias)) if !properties.is_empty() => {
                // For coupled edges, resolved_table_alias may differ from actual_table_alias
                let table_alias_to_use =
                    resolved_table_alias.unwrap_or_else(|| actual_table_alias.clone());
                let prop_count = properties.len();
                for (prop_name, col_name) in properties {
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(table_alias_to_use.clone()),
                            column: PropertyValue::Column(col_name),
                        }),
                        col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
                    });
                }
                log::info!(
                    "✅ Expanded base table '{}' (lookup: '{}', table: '{}') to {} properties",
                    alias,
                    lookup_alias,
                    table_alias_to_use,
                    prop_count
                );
            }
            _ => {
                log::debug!("⚠️ No properties found for base table entity '{}'", alias);
            }
        }
    }

    /// Expand a CTE-sourced entity (Node/Relationship from WITH)
    fn expand_cte_entity(
        &self,
        alias: &str,
        typed_var: &TypedVariable,
        cte_name: &str,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
        select_items: &mut Vec<SelectItem>,
    ) {
        log::info!(
            "✅ Expanding CTE entity '{}' from CTE '{}' to properties",
            alias,
            cte_name
        );

        // 🔧 FIX: Check if this is a multi-type CTE (uses JSON columns)
        if cte_name.starts_with("vlp_multi_type_") {
            log::info!(
                "🎯 Multi-type CTE detected: '{}', using JSON columns",
                cte_name
            );

            // Multi-type CTEs use JSON columns instead of individual property columns:
            // - start_properties (JSON) for start node
            // - end_properties (JSON) for end node
            // We need to select the appropriate JSON column

            let from_alias = self.compute_from_alias_from_cte_name(cte_name);

            // Determine which JSON column to use based on alias position in CTE name
            // CTE name format: vlp_multi_type_{start}_{end}
            let json_column = if cte_name.contains(&format!("_{}_", alias))
                || cte_name.ends_with(&format!("_{}", alias))
            {
                // This is the end node
                "end_properties"
            } else {
                // This is the start node
                "start_properties"
            };

            log::info!("🔍 Node '{}' maps to JSON column '{}'", alias, json_column);

            // Select the JSON column and also the ID column
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(from_alias.clone()),
                    column: PropertyValue::Column(json_column.to_string()),
                }),
                col_alias: Some(ColumnAlias(format!("{}.properties", alias))),
            });

            // Also select the ID
            let id_column = if json_column == "end_properties" {
                "end_id"
            } else {
                "start_id"
            };
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(from_alias),
                    column: PropertyValue::Column(id_column.to_string()),
                }),
                col_alias: Some(ColumnAlias(format!("{}.id", alias))),
            });

            log::info!("✅ Expanded multi-type node '{}' to JSON column", alias);
            return;
        }

        // Parse CTE name to get aliases and compute FROM alias
        let from_alias = self.compute_from_alias_from_cte_name(cte_name);
        log::trace!("🔍 CTE '{}' → FROM alias '{}'", cte_name, from_alias);

        // Get labels from TypedVariable
        let labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // Get properties from schema
        let plan_ctx = plan_ctx.unwrap(); // Should always be Some for CTE expansion
        let schema = plan_ctx.schema();
        let mut properties = if let TypedVariable::Node(_) = typed_var {
            schema.get_node_properties(labels)
        } else {
            schema.get_relationship_properties(labels)
        };

        // For denormalized nodes, property_mappings only has node_id (e.g., code).
        // Merge in from_properties/to_properties to get all denormalized properties.
        if let TypedVariable::Node(_) = typed_var {
            if let Some(label) = labels.first() {
                if let Some(node_schema) = schema.node_schema_opt(label) {
                    if node_schema.is_denormalized {
                        let mut denorm_props = Vec::new();
                        if let Some(from_props) = &node_schema.from_properties {
                            for prop_name in from_props.keys() {
                                if !properties.iter().any(|(p, _)| p == prop_name)
                                    && !denorm_props
                                        .iter()
                                        .any(|(p, _): &(String, String)| p == prop_name)
                                {
                                    denorm_props.push((prop_name.clone(), prop_name.clone()));
                                }
                            }
                        }
                        if let Some(to_props) = &node_schema.to_properties {
                            for prop_name in to_props.keys() {
                                if !properties.iter().any(|(p, _)| p == prop_name)
                                    && !denorm_props
                                        .iter()
                                        .any(|(p, _): &(String, String)| p == prop_name)
                                {
                                    denorm_props.push((prop_name.clone(), prop_name.clone()));
                                }
                            }
                        }
                        if !denorm_props.is_empty() {
                            log::info!(
                                "✅ Merged {} denormalized properties for CTE entity '{}'",
                                denorm_props.len(),
                                alias
                            );
                            properties.extend(denorm_props);
                        }
                    }
                }
            }
        }

        // Sort by cypher property name for a deterministic column order — the
        // schema getters iterate HashMaps and the denormalized merge above
        // iterates `from_properties`/`to_properties` (also HashMaps), so the
        // combined list otherwise flaps across processes (#480, same recipe
        // as the #464 fixes below).
        properties.sort_by(|a, b| a.0.cmp(&b.0));

        if properties.is_empty() {
            log::warn!(
                "⚠️ No properties found in schema for CTE entity '{}'",
                alias
            );
            return;
        }

        // Generate CTE column names and SelectItems
        // Use CTE property mappings from query context (populated by cte_manager)
        // to get the actual column names rather than constructing them manually.
        // Use individual node alias as table reference (not combined CTE FROM alias)
        // because JOINs use individual aliases (e.g., `AS a`, not `AS a_allNeighboursCount`)
        let table_ref = alias.to_string();
        let prop_count = properties.len();
        for (prop_name, _db_column) in properties {
            let cte_column =
                crate::server::query_context::get_cte_property_mapping(&from_alias, &prop_name)
                    .unwrap_or_else(|| cte_column_name(alias, &prop_name));
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(table_ref.clone()),
                    column: PropertyValue::Column(cte_column),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }
        log::info!(
            "✅ Expanded CTE entity '{}' to {} properties",
            alias,
            prop_count
        );
    }

    /// Handle a CTE-sourced scalar (from WITH)
    fn expand_cte_scalar(&self, alias: &str, cte_name: &str, select_items: &mut Vec<SelectItem>) {
        log::info!("✅ Handling CTE scalar '{}' from CTE '{}'", alias, cte_name);

        // Compute FROM alias
        let from_alias = self.compute_from_alias_from_cte_name(cte_name);

        // For scalars, use the alias as column name (assumes CTE generates alias column)
        select_items.push(SelectItem {
            expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: RenderTableAlias(from_alias),
                column: PropertyValue::Column(alias.to_string()),
            }),
            col_alias: Some(ColumnAlias(alias.to_string())),
        });
    }

    /// Fallback logic for when TypedVariable is not available
    fn fallback_table_alias_expansion(
        &self,
        table_alias: &TableAlias,
        item: &ProjectionItem,
        select_items: &mut Vec<SelectItem>,
    ) {
        // Base table logic
        // First, get the properties for the original node alias
        let (properties, resolved_table_alias) =
            match self.get_properties_with_table_alias(&table_alias.0) {
                Ok((props, Some(edge_alias))) if !props.is_empty() => {
                    // Got properties and an edge alias from plan tree traversal
                    // But for coupled edges, we need to check task-local context for unified_alias
                    if let Some(unified_alias) =
                        crate::render_plan::get_denormalized_alias_mapping(&table_alias.0)
                    {
                        // Override with the unified alias for coupled edges
                        (Some(props), unified_alias)
                    } else {
                        (Some(props), edge_alias)
                    }
                }
                Ok((props, None)) if !props.is_empty() => {
                    // Properties found but no edge alias override needed
                    (Some(props), table_alias.0.clone())
                }
                _ => (None, table_alias.0.clone()),
            };

        if let Some(properties) = properties {
            for (prop_name, col_name) in properties {
                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(resolved_table_alias.clone()),
                        column: PropertyValue::Column(col_name),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.{}", table_alias.0, prop_name))),
                });
            }
        } else {
            // Scalar fallback
            select_items.push(SelectItem {
                expression: RenderExpr::ColumnAlias(ColumnAlias(table_alias.0.clone())),
                col_alias: item.col_alias.as_ref().map(|ca| ColumnAlias(ca.0.clone())),
            });
        }
    }

    /// Compute FROM alias from CTE name
    fn compute_from_alias_from_cte_name(&self, cte_name: &str) -> String {
        // For WITH CTEs: "with_a_allNeighboursCount_cte_0" → "a_allNeighboursCount"
        if cte_name.starts_with("with_") {
            if let Some(base) = cte_name.strip_prefix("with_") {
                // Strip _cte_N suffix
                if let Some(idx) = base.rfind("_cte_") {
                    return base[..idx].to_string();
                }
            }
        }
        cte_name.to_string()
    }

    /// Find GraphRel with matching path_variable in the plan tree.
    /// This is used to get the actual connection aliases used in UNION branches.
    fn find_graph_rel_for_path(
        &self,
        path_name: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr) if gr.path_variable.as_deref() == Some(path_name) => Some(gr),
            LogicalPlan::GraphRel(gr) => {
                // Check children
                gr.left
                    .find_graph_rel_for_path(path_name)
                    .or_else(|| gr.right.find_graph_rel_for_path(path_name))
            }
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_for_path(path_name),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_for_path(path_name),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_for_path(path_name),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Union(u) => {
                // Check first branch - all branches should have same path structure
                u.inputs
                    .first()
                    .and_then(|branch| branch.find_graph_rel_for_path(path_name))
            }
            _ => None,
        }
    }

    /// Extract the physical source table (`db.table`) backing a plan node.
    /// Walks through GraphNode/Filter wrappers down to the ViewScan.
    fn plan_source_table(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::ViewScan(vs) => Some(vs.source_table.clone()),
            LogicalPlan::GraphNode(gn) => Self::plan_source_table(&gn.input),
            LogicalPlan::Filter(f) => Self::plan_source_table(&f.input),
            _ => None,
        }
    }

    /// For a fixed-hop path whose relationship is denormalized/coupled INTO one of
    /// its endpoint tables (the edge row lives in the same physical row as that
    /// endpoint — e.g. a Zeek `dns_log` row, or an `AUTHORED` edge stored in the
    /// `posts` table), the edge has no separate scan in the FROM clause. Its
    /// columns must therefore be rendered against the endpoint alias that IS bound.
    ///
    /// Returns `Some(alias)` only when that alias differs from `rel_alias` (i.e. the
    /// edge is genuinely coupled to an endpoint). Returns `None` for a traditional
    /// separate edge table (group_membership) and for the case where the embedded
    /// endpoints already map onto the edge alias (ontime: nodes → edge), so the
    /// existing `rel_alias` rendering is preserved unchanged.
    fn coupled_edge_render_alias(
        graph_rel: &crate::query_planner::logical_plan::GraphRel,
        start_alias: &str,
        end_alias: &str,
        rel_alias: &str,
    ) -> Option<String> {
        let edge_table = Self::plan_source_table(&graph_rel.center)?;
        let start_table = Self::plan_source_table(&graph_rel.left);
        let end_table = Self::plan_source_table(&graph_rel.right);

        // The coupled endpoint is the one whose physical table equals the edge table.
        // Prefer the end endpoint when both match (fully-denormalized single table).
        let endpoint_alias = if end_table.as_deref() == Some(edge_table.as_str()) {
            end_alias
        } else if start_table.as_deref() == Some(edge_table.as_str()) {
            start_alias
        } else {
            return None; // traditional separate edge table — not coupled
        };

        // Follow any denormalized-alias remapping so we land on the alias that is
        // actually bound in FROM. For ontime this resolves the embedded endpoint
        // back onto the edge alias (== rel_alias), which we then filter out below.
        let resolved = crate::render_plan::get_denormalized_alias_mapping(endpoint_alias)
            .unwrap_or_else(|| endpoint_alias.to_string());

        (resolved != rel_alias).then_some(resolved)
    }

    /// Find GraphRel where the given alias is a left or right connection.
    /// Used to detect multi-type VLP context for whole-node expansion.
    fn find_graph_rel_for_alias(
        &self,
        alias: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr)
                if gr.left_connection == alias || gr.right_connection == alias =>
            {
                Some(gr)
            }
            LogicalPlan::GraphRel(gr) => gr
                .left
                .find_graph_rel_for_alias(alias)
                .or_else(|| gr.right.find_graph_rel_for_alias(alias)),
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_for_alias(alias),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_for_alias(alias),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_for_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Union(u) => u
                .inputs
                .first()
                .and_then(|branch| branch.find_graph_rel_for_alias(alias)),
            _ => None,
        }
    }

    /// Find the edge alias for a denormalized node.
    /// If the given alias is a node in a GraphRel with a denormalized center ViewScan
    /// (has from_node_properties or to_node_properties), return the relationship alias.
    fn find_denormalized_edge_alias(&self, node_alias: &str) -> Option<String> {
        let gr = self.find_graph_rel_for_alias(node_alias)?;
        if let LogicalPlan::ViewScan(scan) = gr.center.as_ref() {
            if scan.from_node_properties.is_some() || scan.to_node_properties.is_some() {
                return Some(gr.alias.clone());
            }
        }
        None
    }

    /// Reverse-map a CTE name to the original alias.
    /// Searches GraphJoins.cte_references for an entry where value == cte_name,
    /// returning the key (original alias).
    fn find_cte_original_alias(&self, cte_name: &str) -> Option<String> {
        match self {
            LogicalPlan::GraphJoins(gj) => {
                // Collect all matching aliases and pick the smallest for determinism
                let mut matches: Vec<&String> = gj
                    .cte_references
                    .iter()
                    .filter(|(_, name)| name.as_str() == cte_name)
                    .map(|(alias, _)| alias)
                    .collect();
                matches.sort();
                if let Some(alias) = matches.first() {
                    return Some((*alias).clone());
                }
                gj.input.find_cte_original_alias(cte_name)
            }
            LogicalPlan::Union(u) => u
                .inputs
                .iter()
                .find_map(|branch| branch.find_cte_original_alias(cte_name)),
            LogicalPlan::Projection(p) => p.input.find_cte_original_alias(cte_name),
            LogicalPlan::Filter(f) => f.input.find_cte_original_alias(cte_name),
            LogicalPlan::GraphRel(gr) => {
                let mut matches: Vec<&String> = gr
                    .cte_references
                    .iter()
                    .filter(|(_, name)| name.as_str() == cte_name)
                    .map(|(alias, _)| alias)
                    .collect();
                matches.sort();
                if let Some(alias) = matches.first() {
                    return Some((*alias).clone());
                }
                gr.left
                    .find_cte_original_alias(cte_name)
                    .or_else(|| gr.right.find_cte_original_alias(cte_name))
            }
            _ => None,
        }
    }

    /// Find a GraphRel whose own relationship alias matches the given alias.
    /// Unlike `find_graph_rel_for_alias` which matches node connections (left/right),
    /// this matches the GraphRel's own `alias` field (the relationship variable).
    fn find_graph_rel_by_rel_alias(
        &self,
        alias: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr) if gr.alias == alias => Some(gr),
            LogicalPlan::GraphRel(gr) => gr
                .left
                .find_graph_rel_by_rel_alias(alias)
                .or_else(|| gr.right.find_graph_rel_by_rel_alias(alias)),
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Union(u) => u
                .inputs
                .first()
                .and_then(|branch| branch.find_graph_rel_by_rel_alias(alias)),
            _ => None,
        }
    }

    /// Expand a path variable to its constituent components
    ///
    /// For VLP (variable-length paths) queries:
    ///   - Uses VLP CTE columns: path_nodes, path_relationships, hop_count
    ///   - tuple(t.path_nodes, t.path_relationships, t.hop_count) AS "p"
    ///
    /// For fixed single-hop paths:
    ///   - Constructs path from actual node/relationship aliases
    ///   - Adds component property columns based on schema mappings
    fn expand_path_variable(
        &self,
        path_alias: &str,
        typed_var: &TypedVariable,
        select_items: &mut Vec<SelectItem>,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) {
        log::debug!(
            "🔍 expand_path_variable ENTRY: path='{}', has_plan_ctx={}",
            path_alias,
            plan_ctx.is_some()
        );

        // Check if this is a VLP (variable-length path) or fixed-hop path
        let path_var = match typed_var.as_path() {
            Some(pv) => pv,
            None => {
                log::debug!("expand_path_variable called with non-path variable");
                return;
            }
        };

        // VLP paths have length_bounds set (e.g., *1..3, *, *2..)
        // Fixed single-hop paths have length_bounds = None
        let is_vlp = path_var.length_bounds.is_some() || path_var.is_shortest_path;

        if is_vlp {
            // VLP path - use VLP CTE columns
            use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
            let cte_alias = VLP_CTE_FROM_ALIAS;

            log::info!(
                "🔍 Expanding VLP path variable '{}' using CTE columns from '{}'",
                path_alias,
                cte_alias
            );

            // 🔧 FIX: Check if this is a multi-type VLP (doesn't have path_nodes/path_edges)
            // Multi-type CTEs use different columns: start_properties, end_properties, rel_properties
            // Detection: check if the GraphRel has multiple relationship types (implicit *1 multi-type)
            let is_multi_type = self
                .find_graph_rel_for_path(path_alias)
                .map(|gr| {
                    gr.labels.as_ref().is_some_and(|l| l.len() > 1)
                        || gr.pattern_combinations.is_some()
                })
                .unwrap_or(false);

            if is_multi_type {
                log::info!("🎯 Multi-type VLP path detected for '{}'", path_alias);

                // For multi-type paths, we construct the path from individual components
                // tuple(start_properties, end_properties, rel_properties, hop_count)
                select_items.push(SelectItem {
                    expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: "tuple".to_string(),
                        args: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("rel_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("path_relationships".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_id".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_id".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("hop_count".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_type".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_type".to_string()),
                            }),
                        ],
                    }),
                    col_alias: Some(ColumnAlias(path_alias.to_string())),
                });
                return;
            }

            // Standard (single-type) VLP: materialize the path from what the
            // recursive CTE actually projects — path_nodes, path_relationships,
            // hop_count. Note: the CTE deliberately does NOT project a
            // `path_edges` column (cycle detection is node-uniqueness via
            // path_nodes; per-edge arrays were dropped as a memory
            // optimization). Referencing it here produced unbound-identifier
            // SQL (ClickHouse Code 47) for every `RETURN p` over a VLP (#469).
            // tuple(t.path_nodes, t.path_relationships, t.hop_count)
            select_items.push(SelectItem {
                expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("path_nodes".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("path_relationships".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("hop_count".to_string()),
                        }),
                    ],
                }),
                col_alias: Some(ColumnAlias(path_alias.to_string())),
            });
        } else {
            // Fixed single-hop path - expand component properties
            // All node tables are now in FROM clause (after FK-edge duplicate fix),
            // so we can expand properties for start node, end node, and relationship.

            // Try to find the actual GraphRel in the plan tree to get real aliases
            // This is critical for UNION branches which use branch-specific aliases (t1_0, t2_0)
            // instead of the original aliases (a, b) registered in plan_ctx
            let graph_rel_ref = self.find_graph_rel_for_path(path_alias);
            log::info!(
                "🔍 Fixed-hop path '{}': graph_rel_ref found={}, has_pattern_combinations={}",
                path_alias,
                graph_rel_ref.is_some(),
                graph_rel_ref
                    .as_ref()
                    .and_then(|g| g.pattern_combinations.as_ref())
                    .is_some()
            );
            let (start_alias, end_alias, rel_alias) = if let Some(graph_rel) = &graph_rel_ref {
                log::info!(
                    "🔍 Found GraphRel for path '{}' with actual aliases: left={}, right={}, rel={}",
                    path_alias, graph_rel.left_connection, graph_rel.right_connection, graph_rel.alias
                );
                (
                    graph_rel.left_connection.clone(),
                    graph_rel.right_connection.clone(),
                    graph_rel.alias.clone(),
                )
            } else {
                // Fallback to registered aliases from plan_ctx (for non-UNION patterns)
                let start = path_var
                    .start_node
                    .as_deref()
                    .unwrap_or("_start")
                    .to_string();
                let end = path_var.end_node.as_deref().unwrap_or("_end").to_string();
                let rel = path_var
                    .relationship
                    .as_deref()
                    .unwrap_or("_rel")
                    .to_string();
                log::info!(
                    "🔍 Using registered aliases for path '{}': start={}, end={}, rel={}",
                    path_alias,
                    start,
                    end,
                    rel
                );
                (start, end, rel)
            };

            // 🔥 PatternResolver 2.0: Check if using pattern_union CTE
            // PatternResolver 2.0 CTEs have JSON property columns
            // We need to expand these as individual SELECT columns for result transformer
            if let Some(graph_rel) = &graph_rel_ref {
                if graph_rel.pattern_combinations.is_some() {
                    log::info!("🔀 PatternResolver 2.0 path detected: expanding JSON columns for result transformer");

                    // Use CTE alias (pattern_union_{rel_alias})
                    let cte_alias = &rel_alias;

                    // Expand JSON property columns individually (for result transformer to parse)
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("start_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_start_properties".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("end_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_end_properties".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("rel_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                    });

                    // Extract relationship type from array (first element).
                    // Both ClickHouse `arrayElement` and Spark `element_at` are
                    // 1-indexed.
                    select_items.push(SelectItem {
                        expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: current_function_mapper().array_element().to_string(),
                            args: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: RenderTableAlias(cte_alias.clone()),
                                    column: PropertyValue::Column("path_relationships".to_string()),
                                }),
                                RenderExpr::Literal(Literal::Integer(1)),
                            ],
                        }),
                        col_alias: Some(ColumnAlias("__rel_type__".to_string())),
                    });

                    // Add start_id and end_id for element_id construction
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("start_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__start_id__".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("end_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__end_id__".to_string())),
                    });

                    // Use start_type/end_type from the CTE (added per-branch)
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("start_type".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__start_label__".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("end_type".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__end_label__".to_string())),
                    });

                    return; // Done - skip the rest of fixed-path expansion
                }
            }

            // Check if relationship is denormalized
            let is_rel_denormalized = if let Some(graph_rel) = &graph_rel_ref {
                log::info!(
                    "🔍 Checking if relationship '{}' is denormalized, center type: {:?}",
                    rel_alias,
                    std::mem::discriminant(graph_rel.center.as_ref())
                );
                if let crate::query_planner::logical_plan::LogicalPlan::ViewScan(vs) =
                    graph_rel.center.as_ref()
                {
                    log::info!(
                        "🔍 Relationship '{}' center IS ViewScan, table={}, is_denormalized={}",
                        rel_alias,
                        vs.source_table,
                        vs.is_denormalized
                    );
                    vs.is_denormalized
                } else {
                    log::trace!("🔍 Relationship '{}' center is NOT ViewScan", rel_alias);
                    false
                }
            } else {
                log::debug!(
                    "🔍 No GraphRel found for relationship '{}', assuming not denormalized",
                    rel_alias
                );
                false
            };

            log::info!(
                "🔍 Expanding fixed-hop path variable '{}': start={}, end={}, rel={}",
                path_alias,
                start_alias,
                end_alias,
                rel_alias
            );

            // Expand properties for each component if we have plan_ctx
            if let Some(ctx) = plan_ctx {
                log::debug!(
                    "  🔍 Have plan_ctx, looking up path components: start={}, end={}, rel={}",
                    start_alias,
                    end_alias,
                    rel_alias
                );

                // Expand start node properties
                if let Some(typed_var) = ctx.lookup_variable(&start_alias) {
                    let variant_name = if typed_var.is_node() {
                        "Node"
                    } else if typed_var.is_relationship() {
                        "Relationship"
                    } else if typed_var.is_scalar() {
                        "Scalar"
                    } else if typed_var.as_path().is_some() {
                        "Path"
                    } else {
                        "Unknown"
                    };
                    log::debug!(
                        "  ✓ Found start node '{}' in plan_ctx, variant={}, is_entity={}",
                        start_alias,
                        variant_name,
                        typed_var.is_entity()
                    );
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding start node '{}' properties", start_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(
                                    &start_alias,
                                    typed_var,
                                    select_items,
                                    Some(ctx),
                                );
                            }
                            VariableSource::Cte { cte_name, .. } => {
                                self.expand_cte_entity(
                                    &start_alias,
                                    typed_var,
                                    cte_name,
                                    Some(ctx),
                                    select_items,
                                );
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::trace!("  ✗ Start node '{}' not found in plan_ctx", start_alias);
                }

                // Expand end node properties
                if let Some(typed_var) = ctx.lookup_variable(&end_alias) {
                    log::debug!("  ✓ Found end node '{}' in plan_ctx", end_alias);
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding end node '{}' properties", end_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(
                                    &end_alias,
                                    typed_var,
                                    select_items,
                                    Some(ctx),
                                );
                            }
                            VariableSource::Cte { cte_name, .. } => {
                                self.expand_cte_entity(
                                    &end_alias,
                                    typed_var,
                                    cte_name,
                                    Some(ctx),
                                    select_items,
                                );
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::trace!("  ✗ End node '{}' not found in plan_ctx", end_alias);
                }

                // Relationship/edge property expansion.
                //
                // First handle the coupled case: when the edge is denormalized INTO
                // one of its endpoint tables, the edge row shares that endpoint's
                // physical row and has no separate scan in FROM. Render its columns
                // against the endpoint alias that IS bound (e.g. a Zeek `dns_log`
                // row, or an `AUTHORED` edge stored in the `posts` table) so they
                // resolve instead of dangling on an unbound `rel_alias` (t3).
                let coupled_edge_alias = graph_rel_ref.as_ref().and_then(|gr| {
                    Self::coupled_edge_render_alias(gr, &start_alias, &end_alias, &rel_alias)
                });
                if let Some(edge_alias) = coupled_edge_alias {
                    log::info!(
                        "  📦 Coupled edge '{}' → binding properties to endpoint alias '{}'",
                        rel_alias,
                        edge_alias
                    );
                    if let Some(graph_rel) = &graph_rel_ref {
                        if let Some(ref labels) = graph_rel.labels {
                            if let Some(rel_type) = labels.first() {
                                let schema = ctx.schema();
                                let mut rel_props = schema
                                    .get_relationship_properties(std::slice::from_ref(rel_type));
                                // Sort by cypher property name for a deterministic column
                                // order — `get_relationship_properties` iterates a HashMap
                                // (#464). A RETURN p path materializes these edge columns;
                                // unsorted, the byte output flips run-to-run.
                                rel_props.sort_by(|a, b| a.0.cmp(&b.0));
                                for (prop_name, db_column) in rel_props {
                                    // Expression uses the bound endpoint alias; the column
                                    // alias keeps the `rel_alias.` prefix so path assembly
                                    // (convert_path_branches_to_json) still groups it as the
                                    // relationship's properties.
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(edge_alias.clone()),
                                            column: PropertyValue::Column(db_column),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            rel_alias, prop_name
                                        ))),
                                    });
                                }
                            }
                        }
                    }
                }
                // Expand relationship properties (ONLY if not denormalized)
                // Denormalized relationships (e.g., AUTHORED) don't have a separate relationship table
                else if !is_rel_denormalized {
                    if let Some(typed_var) = ctx.lookup_variable(&rel_alias) {
                        log::debug!(
                            "  ✓ Found relationship '{}' in plan_ctx, is_entity={}, source={:?}",
                            rel_alias,
                            typed_var.is_entity(),
                            typed_var.source()
                        );
                        if typed_var.is_entity() {
                            log::info!("  📦 Expanding relationship '{}' properties", rel_alias);
                            match typed_var.source() {
                                VariableSource::Match => {
                                    self.expand_base_table_entity(
                                        &rel_alias,
                                        typed_var,
                                        select_items,
                                        Some(ctx),
                                    );
                                }
                                VariableSource::Cte { cte_name, .. } => {
                                    self.expand_cte_entity(
                                        &rel_alias,
                                        typed_var,
                                        cte_name,
                                        Some(ctx),
                                        select_items,
                                    );
                                }
                                _ => {}
                            }
                        }
                    } else {
                        log::trace!("  ✗ Relationship '{}' not found in plan_ctx", rel_alias);
                    }
                } else {
                    // Denormalized relationship: properties come from end node's table
                    // Get relationship properties from schema and select using end_alias table
                    log::info!("  📦 Expanding denormalized relationship '{}' properties using end node table '{}'", rel_alias, end_alias);
                    if let Some(graph_rel) = &graph_rel_ref {
                        // Get relationship type from GraphRel labels
                        if let Some(ref labels) = graph_rel.labels {
                            if let Some(rel_type) = labels.first() {
                                // Get property mappings from schema via plan_ctx
                                let schema = ctx.schema();
                                let mut rel_props = schema
                                    .get_relationship_properties(std::slice::from_ref(rel_type));
                                // Sort by cypher property name for a deterministic column
                                // order — `get_relationship_properties` iterates a HashMap
                                // (#464). A RETURN p path materializes these edge columns;
                                // unsorted, the byte output flips run-to-run.
                                rel_props.sort_by(|a, b| a.0.cmp(&b.0));
                                log::info!(
                                    "  🔍 Found {} properties for denormalized rel '{}': {:?}",
                                    rel_props.len(),
                                    rel_type,
                                    rel_props
                                );
                                for (prop_name, db_column) in rel_props {
                                    // For denormalized relationships, use the relationship alias
                                    // (which points to the actual table) not the virtual node alias
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(rel_alias.clone()),
                                            column: PropertyValue::Column(db_column),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            rel_alias, prop_name
                                        ))),
                                    });
                                }
                            }
                        }
                    }
                }
            } else {
                log::debug!(
                    "  ✗ NO plan_ctx available for path variable '{}' property expansion!",
                    path_alias
                );
            }

            // Add the path metadata column with component aliases
            // Format: tuple('fixed_path', start_alias, end_alias, rel_alias)
            select_items.push(SelectItem {
                expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: vec![
                        // Path type marker
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            "fixed_path".to_string(),
                        )),
                        // Start node alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            start_alias.to_string(),
                        )),
                        // End node alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            end_alias.to_string(),
                        )),
                        // Relationship alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            rel_alias.to_string(),
                        )),
                    ],
                }),
                col_alias: Some(ColumnAlias(path_alias.to_string())),
            });
        }
    }

    /// Add node properties for path queries with prefixed aliases
    ///
    /// For path queries like `MATCH path = (a)-[r]->(b) RETURN path`,
    /// we need to include node properties with aliases like "a.user_id", "a.full_name"
    /// so that convert_path_branches_to_json() can build _start_properties JSON.
    fn add_node_properties_for_path(
        &self,
        node_plan: &std::sync::Arc<LogicalPlan>,
        alias: &str,
        items: &mut Vec<SelectItem>,
    ) -> Result<(), RenderBuildError> {
        // Get properties from the node plan (ViewScan or denormalized)
        let (properties, actual_table_alias) =
            PropertiesBuilder::get_properties_with_table_alias(node_plan.as_ref(), alias)?;

        log::debug!(
            "🔍 add_node_properties_for_path: node '{}' has {} properties (table: {:?})",
            alias,
            properties.len(),
            actual_table_alias
        );

        // Use actual_table_alias for denormalized properties, fallback to alias
        let table_alias_str = actual_table_alias.unwrap_or_else(|| alias.to_string());

        // Add each property as a SELECT item with prefixed alias
        for (prop_name, col_name) in properties {
            items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(table_alias_str.clone()),
                    column: PropertyValue::Column(col_name),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }

        Ok(())
    }

    /// Add relationship properties for path queries with prefixed aliases
    ///
    /// Similar to node properties, but for the relationship in the path.
    fn add_relationship_properties_for_path(
        &self,
        rel_plan: &std::sync::Arc<LogicalPlan>,
        alias: &str,
        items: &mut Vec<SelectItem>,
    ) -> Result<(), RenderBuildError> {
        // Get properties from the relationship plan (ViewScan or denormalized)
        let (properties, actual_table_alias) =
            PropertiesBuilder::get_properties_with_table_alias(rel_plan.as_ref(), alias)?;

        log::debug!(
            "🔍 add_relationship_properties_for_path: rel '{}' has {} properties (table: {:?})",
            alias,
            properties.len(),
            actual_table_alias
        );

        // Use actual_table_alias for denormalized properties, fallback to alias
        let table_alias_str = actual_table_alias.unwrap_or_else(|| alias.to_string());

        // Add each property as a SELECT item with prefixed alias
        for (prop_name, col_name) in properties {
            items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(table_alias_str.clone()),
                    column: PropertyValue::Column(col_name),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Outside a task-local scope (or with default ClickHouse dialect), the
    /// JSON-extract argument is the bare field name — matching CH's
    /// `JSONExtractString(json, 'name')` shape.
    #[test]
    fn json_extract_field_arg_defaults_to_bare_name() {
        assert_eq!(json_extract_field_arg("OriginCityName"), "OriginCityName");
    }

    /// Inside a Databricks task-local scope, the argument becomes a JSONPath
    /// (`$.field`) — matching `get_json_object(json, '$.name')`.
    #[tokio::test]
    async fn json_extract_field_arg_databricks_uses_jsonpath() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let arg = with_query_context(ctx, async { json_extract_field_arg("OriginCityName") }).await;
        assert_eq!(arg, "$.OriginCityName");
    }
}
