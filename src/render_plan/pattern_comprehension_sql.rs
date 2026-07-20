//! Pattern-comprehension SQL string emitters.
//!
//! This module emits SQL *text* for pattern comprehensions and the
//! `size([(a)-[:R]->(b) WHERE ... | ...])` / pattern-count family: the
//! correlated-subquery and pre-aggregated-CTE renderings, their `arrayCount`
//! list-comprehension lowering, the `count(*)`-placeholder substitution passes,
//! and the small edge/node id-column and CTE-column lookup helpers those
//! emitters need. It is a distinct layer from the rest of `plan_builder_utils`
//! (it builds SQL strings, not `RenderPlan`/`RenderExpr` trees), which is why
//! §5.1 of `docs/design/REFACTORING_SAFETY_PLAN.md` (Phase 2 / P2.2) calls it
//! out as cohesive and separable.
//!
//! Extracted verbatim from `plan_builder_utils.rs` (no logic edits). The old
//! path re-exports the externally-referenced functions via `pub(crate) use`
//! during the transition so existing callers resolve unchanged.

use crate::graph_catalog::GraphSchema;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{
    Literal, PropertyAccess, RenderExpr, ScalarFnCall, TableAlias,
};
use crate::render_plan::{FromTableItem, Join, RenderPlan, SelectItem, UnionItems};
use crate::sql_generator::function_mapper::current_function_mapper;
use crate::utils::cte_column_naming::parse_cte_column;
use std::collections::{HashMap, HashSet};

// Helpers that remain in `plan_builder_utils` (widely used there) but are
// called by this module; imported by name (no globs, per §5.3).
use super::plan_builder_utils::{extract_from_alias_from_cte_name, quote_qualified_col};

/// Build a column map from a WITH CTE render plan's SELECT items.
/// Maps (cypher_alias, cypher_property) → "cte_from_alias.cte_column_name"
/// Used to resolve outer-scope variable references inside correlated subqueries.
pub(crate) fn build_cte_column_map(
    render_plan: &RenderPlan,
    cte_from_alias: &str,
) -> HashMap<(String, String), String> {
    let mut map = HashMap::new();

    // Use the actual FROM alias for column qualification, not the CTE name.
    // Within a CTE body, correlated subqueries reference the FROM table alias
    // (e.g., person_tag.p3_tag_id), not the CTE being defined
    // (e.g., with_person_score_tag_cte_1.p3_tag_id).
    let effective_alias = if let FromTableItem(Some(ref from)) = render_plan.from {
        from.alias.as_deref().unwrap_or(cte_from_alias).to_string()
    } else {
        cte_from_alias.to_string()
    };

    // Determine the source of SELECT items to scan
    let select_items_list: Vec<&Vec<SelectItem>> =
        if let UnionItems(Some(ref union)) = render_plan.union {
            // For UNION, scan all branches
            union.input.iter().map(|b| &b.select.items).collect()
        } else {
            vec![&render_plan.select.items]
        };

    for select_items in &select_items_list {
        for item in *select_items {
            if let Some(ref col_alias) = item.col_alias {
                let cte_col_name = &col_alias.0;
                if let Some((parsed_alias, parsed_property)) = parse_cte_column(cte_col_name) {
                    // Determine the real column to use in correlated subqueries.
                    // If the SELECT expression is a PropertyAccess (e.g., `a.user_id AS p1_a_user_id`),
                    // use the actual column from the expression (user_id), not the alias (p1_a_user_id).
                    // Uses PropertyValue::to_sql() which handles:
                    // - simple columns (with proper quoting),
                    // - expression-based mappings (e.g., toYear(FlightDate)),
                    // - base tables (where p{N} names don't exist as real columns),
                    // - CTE references (where the expression itself will use the CTE column name).
                    let qualified = if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                        pa.column.to_sql(&effective_alias)
                    } else {
                        // Non-property expressions (aggregates, subqueries, etc.):
                        // fall back to the CTE column alias name
                        format!("{}.{}", effective_alias, cte_col_name)
                    };
                    map.insert((parsed_alias, parsed_property), qualified);
                } else {
                    // Non-p{N} columns: treat the alias itself as a bare variable
                    let qualified = quote_qualified_col(&effective_alias, cte_col_name);
                    map.insert((cte_col_name.clone(), "id".to_string()), qualified.clone());
                    map.insert((cte_col_name.clone(), cte_col_name.clone()), qualified);
                }
            }
        }
    }

    // Also scan FROM table to find CTE-backed columns
    // For UNION branches, scan each branch's FROM
    let from_tables: Vec<&RenderPlan> = if let UnionItems(Some(ref union)) = render_plan.union {
        union.input.iter().collect()
    } else {
        vec![render_plan]
    };

    for branch in &from_tables {
        if let FromTableItem(Some(ref from)) = branch.from {
            if let Some(ref from_alias) = from.alias {
                // If FROM is a CTE reference, scan its SELECT items for p{N} columns
                // The from_alias is what's used in the SQL body
                for item in &branch.select.items {
                    if let Some(ref col_alias) = item.col_alias {
                        let cte_col_name = &col_alias.0;
                        if let Some((parsed_alias, parsed_property)) =
                            parse_cte_column(cte_col_name)
                        {
                            // Map the parsed alias/property to the FROM alias qualified reference
                            let qualified = format!("{}.{}", from_alias, cte_col_name);
                            map.entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        }
                    }
                    // Also look for PropertyAccess expressions that reference FROM alias
                    scan_expr_for_aliases(&item.expression, from_alias, &mut map);
                }

                // Scan JOINs for table aliases (e.g., friend from Person table)
                for join in &branch.joins.0 {
                    let join_alias = &join.table_alias;
                    // Map join alias to itself (it's directly available in scope)
                    map.entry((join_alias.clone(), "id".to_string()))
                        .or_insert_with(|| format!("{}.id", join_alias));
                }
            }
        }
    }

    map
}

/// Scan a RenderExpr for PropertyAccess patterns that reference a FROM alias,
/// and add them to the column map for correlated subquery resolution.
fn scan_expr_for_aliases(
    expr: &RenderExpr,
    from_alias: &str,
    map: &mut HashMap<(String, String), String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == from_alias {
                if let crate::graph_catalog::expression_parser::PropertyValue::Column(ref col) =
                    pa.column
                {
                    // This is a reference like `from_alias.some_cte_column`
                    if let Some((parsed_alias, parsed_property)) = parse_cte_column(col) {
                        let qualified = format!("{}.{}", from_alias, col);
                        map.entry((parsed_alias, parsed_property))
                            .or_insert(qualified);
                    }
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                scan_expr_for_aliases(operand, from_alias, map);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                scan_expr_for_aliases(arg, from_alias, map);
            }
        }
        RenderExpr::AggregateFnCall(a) => {
            for arg in &a.args {
                scan_expr_for_aliases(arg, from_alias, map);
            }
        }
        _ => {}
    }
}

/// Generate a correlated subquery SQL string for a single pattern comprehension.
///
/// For a pattern like `(tag)<-[:HAS_INTEREST]-(person)`:
/// ```sql
/// (SELECT COUNT(*) FROM ldbc.Person_hasInterest_Tag
///  WHERE TagId = cte_alias.p3_tag_id AND PersonId = cte_alias.p6_person_id)
/// ```
///
/// For multi-hop like `(tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person)`:
/// ```sql
/// (SELECT COUNT(*) FROM ldbc.Message_hasTag_Tag AS __r0
///  INNER JOIN ldbc.Message_hasCreator_Person AS __r1 ON __r0.MessageId = __r1.MessageId
///  INNER JOIN ldbc.Message AS __m0 ON __m0.id = __r0.MessageId
///  WHERE __r0.TagId = cte_alias.p3_tag_id AND __r1.PersonId = cte_alias.p6_person_id
///    AND $startDate < __m0.creationDate AND __m0.creationDate < $endDate)
/// ```
fn generate_pattern_comprehension_correlated_subquery(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
    from_cte_name: Option<String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::{ConnectedPatternInfo, PatternPosition};

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    // For list comprehension patterns (e.g., size([p IN posts WHERE pattern])),
    // use arrayCount() instead of correlated subquery to avoid ClickHouse
    // "Cannot clone Union plan step" error when outer query has UNION ALL.
    if pc_meta.list_constraint.is_some() {
        return generate_list_comp_array_count(pc_meta, schema, cte_column_map, from_cte_name);
    }

    log::info!(
        "🔧 generate_pc_correlated_subquery: {} hops, {} correlation vars, where={:?}",
        pc_meta.pattern_hops.len(),
        pc_meta.correlation_vars.len(),
        pc_meta.where_clause.is_some()
    );

    // Check if any hop uses Direction::Either (undirected edge).
    // For undirected edges, we need to generate a UNION of both directions.
    let has_either_direction = pc_meta
        .pattern_hops
        .iter()
        .any(|h| h.direction == crate::query_planner::logical_expr::Direction::Either);

    if has_either_direction {
        // Delegate to specialized handler that generates UNION for undirected hops
        return generate_pc_correlated_subquery_with_either(pc_meta, schema, cte_column_map);
    }

    // For each hop, find the matching edge table in schema
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new(); // (db_table, alias, hop_info)

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        // Determine from/to labels based on direction
        let (from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    // Incoming: (end)<-[:REL]-(start) → in schema terms from=start, to=end
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => {
                    // Outgoing: (start)-[:REL]->(end)
                    (hop.start_label.clone(), hop.end_label.clone())
                }
            };

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();

        let rel_type = hop.rel_type.as_deref();

        // Find matching edge table
        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            // Join this hop to the previous one through the shared node
            // The previous hop's to_id connects to this hop's from_id
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;

            // Find the join column: prev hop's "to" side = this hop's "from" side
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);

            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Add WHERE conditions for each correlation variable
    for cv in &pc_meta.correlation_vars {
        // Find which hop and which side (from/to) this variable connects to
        let (hop_idx, is_start) = match &cv.pattern_position {
            PatternPosition::StartOfHop(idx) => (*idx, true),
            PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];

        // Determine which edge column to use for this correlation variable
        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        // Find the CTE column for this correlation variable's ID
        let cte_col =
            find_cte_column_for_correlation_var(&cv.var_name, &cv.label, schema, cte_column_map);

        if let Some(cte_ref) = cte_col {
            where_conditions.push(format!("{}.{} = {}", edge_alias, edge_col, cte_ref));
        } else {
            log::warn!(
                "⚠️ Could not find CTE column for correlation var '{}' (label='{}')",
                cv.var_name,
                cv.label
            );
        }
    }

    // Handle WHERE clause from pattern comprehension
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Build final SQL
    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    Some(format!(
        "(SELECT COUNT(*) FROM {}{}{})",
        from_clause, joins_str, where_str
    ))
}

/// Result of generating a pre-aggregated CTE for a pattern comprehension.
pub(crate) struct PcCteResult {
    /// The CTE body SQL (SELECT ... FROM ... GROUP BY ...)
    pub(crate) cte_sql: String,
    /// Correlation columns: (var_name, var_label, cte_col_alias) — e.g., ("tag", "Tag", "corr_0")
    pub(crate) correlation_columns: Vec<(String, String, String)>,
}

/// Generate a pre-aggregated CTE SQL for a pattern comprehension.
///
/// Instead of a correlated subquery like:
/// ```sql
/// (SELECT COUNT(*) FROM edge WHERE TagId = outer.tag_id AND PersonId = outer.person_id)
/// ```
///
/// Generates a non-correlated CTE:
/// ```sql
/// SELECT TagId AS corr_0, PersonId AS corr_1, COUNT(*) AS result
/// FROM edge_table AS __r0
/// INNER JOIN ... AS __r1 ON ...
/// WHERE date_filters...
/// GROUP BY TagId, PersonId
/// ```
///
/// The caller then adds a LEFT JOIN from the main CTE to this PC CTE.
pub(crate) fn generate_pattern_comprehension_cte(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
) -> Option<PcCteResult> {
    use crate::query_planner::logical_plan::{ConnectedPatternInfo, PatternPosition};

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    // Skip list comprehension patterns — they use arrayCount() instead
    if pc_meta.list_constraint.is_some() {
        return None;
    }

    log::info!(
        "🔧 generate_pattern_comprehension_cte: {} hops, {} correlation vars",
        pc_meta.pattern_hops.len(),
        pc_meta.correlation_vars.len(),
    );

    // Check for Direction::Either — need UNION ALL of both directions
    let has_either_direction = pc_meta
        .pattern_hops
        .iter()
        .any(|h| h.direction == crate::query_planner::logical_expr::Direction::Either);

    if has_either_direction {
        return generate_pc_cte_with_either(pc_meta, schema);
    }

    // For each hop, find the matching edge table in schema
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new();

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        let (from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => (hop.start_label.clone(), hop.end_label.clone()),
            };

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();
        let rel_type = hop.rel_type.as_deref();

        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for CTE hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);
            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Build SELECT columns for correlation variables (these become GROUP BY columns)
    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();
    let mut correlation_columns: Vec<(String, String, String)> = Vec::new();

    for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
        let (hop_idx, is_start) = match &cv.pattern_position {
            PatternPosition::StartOfHop(idx) => (*idx, true),
            PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];

        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        let corr_alias = format!("corr_{}", cv_idx);
        select_cols.push(format!("{}.{} AS {}", edge_alias, edge_col, corr_alias));
        group_by_cols.push(format!("{}.{}", edge_alias, edge_col));
        correlation_columns.push((cv.var_name.clone(), cv.label.clone(), corr_alias));
    }

    // Handle WHERE clause from pattern comprehension
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Build final CTE SQL
    select_cols.push("COUNT(*) AS result".to_string());

    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    let group_by_str = if group_by_cols.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_by_cols.join(", "))
    };

    let cte_sql = format!(
        "SELECT {} FROM {}{}{}{}",
        select_cols.join(", "),
        from_clause,
        joins_str,
        where_str,
        group_by_str
    );

    log::info!("🔧 PC CTE SQL: {}", &cte_sql[..cte_sql.len().min(300)]);

    Some(PcCteResult {
        cte_sql,
        correlation_columns,
    })
}

/// Generate a pre-aggregated CTE for pattern comprehensions with Direction::Either.
/// Creates UNION ALL of both direction variants, wrapped in an outer GROUP BY.
fn generate_pc_cte_with_either(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
) -> Option<PcCteResult> {
    use crate::query_planner::logical_plan::PatternPosition;

    // Build direction variants (same logic as correlated subquery path)
    let mut direction_variants: Vec<Vec<crate::query_planner::logical_expr::Direction>> =
        vec![vec![]];

    for hop in &pc_meta.pattern_hops {
        if hop.direction == crate::query_planner::logical_expr::Direction::Either {
            let mut new_variants = Vec::new();
            for variant in &direction_variants {
                let mut v_out = variant.clone();
                v_out.push(crate::query_planner::logical_expr::Direction::Outgoing);
                new_variants.push(v_out);

                let mut v_in = variant.clone();
                v_in.push(crate::query_planner::logical_expr::Direction::Incoming);
                new_variants.push(v_in);
            }
            direction_variants = new_variants;
        } else {
            for variant in &mut direction_variants {
                variant.push(hop.direction.clone());
            }
        }
    }

    // Determine correlation column aliases (consistent across all variants)
    let mut correlation_columns: Vec<(String, String, String)> = Vec::new();
    for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
        let corr_alias = format!("corr_{}", cv_idx);
        correlation_columns.push((cv.var_name.clone(), cv.label.clone(), corr_alias));
    }

    let mut union_parts: Vec<String> = Vec::new();

    for directions in &direction_variants {
        let mut edge_tables: Vec<(
            String,
            String,
            &crate::query_planner::logical_plan::ConnectedPatternInfo,
            crate::query_planner::logical_expr::Direction,
        )> = Vec::new();

        let mut all_found = true;
        for (hop_idx, (hop, dir)) in pc_meta
            .pattern_hops
            .iter()
            .zip(directions.iter())
            .enumerate()
        {
            let rel_alias = format!("__r{}", hop_idx);

            let (from_label, to_label) = match dir {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.as_deref(), hop.start_label.as_deref())
                }
                _ => (hop.start_label.as_deref(), hop.end_label.as_deref()),
            };

            let rel_type = hop.rel_type.as_deref();
            let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

            if let Some(table) = db_table {
                edge_tables.push((table, rel_alias, hop, dir.clone()));
            } else {
                if *dir == crate::query_planner::logical_expr::Direction::Outgoing {
                    let db_table_rev =
                        find_edge_table_in_schema(schema, rel_type, to_label, from_label);
                    if let Some(table) = db_table_rev {
                        edge_tables.push((
                            table,
                            format!("__r{}", hop_idx),
                            hop,
                            crate::query_planner::logical_expr::Direction::Incoming,
                        ));
                        continue;
                    }
                }
                all_found = false;
                break;
            }
        }

        if !all_found {
            continue;
        }

        // Build FROM + JOINs for this direction variant
        let mut from_clause = String::new();
        let mut join_clauses: Vec<String> = Vec::new();
        let mut where_conditions: Vec<String> = Vec::new();

        for (idx, (db_table, alias, _hop, _dir)) in edge_tables.iter().enumerate() {
            if idx == 0 {
                from_clause = format!("{} AS {}", db_table, alias);
            } else {
                let prev_alias = &edge_tables[idx - 1].1;
                let prev_hop = edge_tables[idx - 1].2;
                let prev_dir = &edge_tables[idx - 1].3;

                let prev_to_col = find_edge_id_column_with_direction(
                    schema,
                    &edge_tables[idx - 1].0,
                    false,
                    prev_hop,
                    prev_dir,
                );
                let curr_from_col =
                    find_edge_id_column_with_direction(schema, db_table, true, _hop, _dir);

                join_clauses.push(format!(
                    "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                    db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
                ));
            }
        }

        // SELECT columns for correlation variables
        let mut select_cols: Vec<String> = Vec::new();
        for (cv_idx, cv) in pc_meta.correlation_vars.iter().enumerate() {
            let (hop_idx, is_start) = match &cv.pattern_position {
                PatternPosition::StartOfHop(idx) => (*idx, true),
                PatternPosition::EndOfHop(idx) => (*idx, false),
            };

            if hop_idx >= edge_tables.len() {
                continue;
            }

            let (_, edge_alias, hop_info, dir) = &edge_tables[hop_idx];
            let edge_col = find_edge_id_column_with_direction(
                schema,
                &edge_tables[hop_idx].0,
                is_start,
                hop_info,
                dir,
            );

            let corr_alias = format!("corr_{}", cv_idx);
            select_cols.push(format!("{}.{} AS {}", edge_alias, edge_col, corr_alias));
        }

        // WHERE clause
        if let Some(ref where_expr) = pc_meta.where_clause {
            if let Some(where_sql) = render_pc_where_clause(
                where_expr,
                &pc_meta.pattern_hops,
                &edge_tables
                    .iter()
                    .map(|(t, a, h, _)| (t.clone(), a.clone(), *h))
                    .collect::<Vec<_>>(),
                schema,
                &mut join_clauses,
            ) {
                where_conditions.push(where_sql);
            }
        }

        let where_str = if where_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_conditions.join(" AND "))
        };

        let joins_str = if join_clauses.is_empty() {
            String::new()
        } else {
            format!(" {}", join_clauses.join(" "))
        };

        union_parts.push(format!(
            "SELECT {} FROM {}{}{}",
            select_cols.join(", "),
            from_clause,
            joins_str,
            where_str,
        ));
    }

    if union_parts.is_empty() {
        return None;
    }

    // Build the corr column aliases for the outer GROUP BY
    let corr_aliases: Vec<String> = correlation_columns
        .iter()
        .map(|(_, _, alias)| alias.clone())
        .collect();

    let cte_sql = if union_parts.len() == 1 {
        // Single variant — wrap in subquery with outer aggregation
        format!(
            "SELECT {}, COUNT(*) AS result FROM ({}) AS __u GROUP BY {}",
            corr_aliases.join(", "),
            union_parts[0],
            corr_aliases.join(", ")
        )
    } else {
        // Multiple variants — wrap UNION ALL in subquery with outer GROUP BY
        let inner_union = union_parts.join(" UNION ALL ");
        format!(
            "SELECT {}, COUNT(*) AS result FROM ({}) AS __u GROUP BY {}",
            corr_aliases.join(", "),
            inner_union,
            corr_aliases.join(", ")
        )
    };

    log::info!(
        "🔧 PC CTE (Either) SQL: {}",
        &cte_sql[..cte_sql.len().min(300)]
    );

    Some(PcCteResult {
        cte_sql,
        correlation_columns,
    })
}

/// Generate an `arrayCount()` expression for list comprehension patterns.
///
/// For `size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])`:
/// ```sql
/// arrayCount(
///   x -> (x, person_id_col) IN (
///     SELECT __r0.PostId, __r1.PersonId
///     FROM Post_hasTag_Tag AS __r0
///     INNER JOIN Person_hasInterest_Tag AS __r1 ON __r0.TagId = __r1.TagId
///   ),
///   posts_col
/// )
/// ```
///
/// This avoids correlated subqueries, which fail with ClickHouse's
/// "Cannot clone Union plan step" error when the outer query has UNION ALL.
fn generate_list_comp_array_count(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
    from_cte_name: Option<String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::ConnectedPatternInfo;

    let lc = pc_meta.list_constraint.as_ref()?;

    if pc_meta.pattern_hops.is_empty() {
        return None;
    }

    log::info!(
        "🔧 generate_list_comp_array_count: {} hops, list_alias='{}', source_label={:?}",
        pc_meta.pattern_hops.len(),
        lc.list_alias,
        lc.source_label
    );

    // Build edge tables chain (same logic as correlated subquery path)
    let mut edge_tables: Vec<(String, String, &ConnectedPatternInfo)> = Vec::new();

    for (hop_idx, hop) in pc_meta.pattern_hops.iter().enumerate() {
        let rel_alias = format!("__r{}", hop_idx);

        let (mut from_label_owned, to_label_owned): (Option<String>, Option<String>) =
            match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.clone(), hop.start_label.clone())
                }
                _ => (hop.start_label.clone(), hop.end_label.clone()),
            };

        // Override first hop's from_label with list source label
        if hop_idx == 0 && from_label_owned.is_none() {
            if let Some(ref src_label) = lc.source_label {
                from_label_owned = Some(src_label.clone());
            }
        }

        let from_label = from_label_owned.as_deref();
        let to_label = to_label_owned.as_deref();
        let rel_type = hop.rel_type.as_deref();

        let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);
        if let Some(table) = db_table {
            edge_tables.push((table, rel_alias, hop));
        } else {
            log::warn!(
                "⚠️ No edge table found for arrayCount hop {}: rel_type={:?}, from={:?}, to={:?}",
                hop_idx,
                rel_type,
                from_label,
                to_label
            );
            return None;
        }
    }

    // Build FROM + JOINs for the edge chain
    let mut from_clause = String::new();
    let mut join_clauses: Vec<String> = Vec::new();
    let mut where_conditions: Vec<String> = Vec::new();

    for (idx, (db_table, alias, hop)) in edge_tables.iter().enumerate() {
        if idx == 0 {
            from_clause = format!("{} AS {}", db_table, alias);
        } else {
            let prev_alias = &edge_tables[idx - 1].1;
            let prev_hop = edge_tables[idx - 1].2;
            let prev_to_col = find_edge_id_column(schema, &edge_tables[idx - 1].0, false, prev_hop);
            let curr_from_col = find_edge_id_column(schema, db_table, true, hop);
            join_clauses.push(format!(
                "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
            ));
        }
    }

    // Find the list element column (first hop's from_id — the iteration variable)
    let list_element_col = {
        let (_, first_alias, first_hop) = &edge_tables[0];
        let col = find_edge_id_column(schema, &edge_tables[0].0, true, first_hop);
        format!("{}.{}", first_alias, col)
    };

    // Find correlation variable columns (from edge tables) — go into the tuple
    let mut corr_edge_cols: Vec<String> = Vec::new();
    let mut corr_outer_cols: Vec<String> = Vec::new();

    for cv in &pc_meta.correlation_vars {
        let (hop_idx, is_start) = match &cv.pattern_position {
            crate::query_planner::logical_plan::PatternPosition::StartOfHop(idx) => (*idx, true),
            crate::query_planner::logical_plan::PatternPosition::EndOfHop(idx) => (*idx, false),
        };

        if hop_idx >= edge_tables.len() {
            continue;
        }

        let (_, edge_alias, hop_info) = &edge_tables[hop_idx];
        let edge_col = if is_start {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, true, hop_info)
        } else {
            find_edge_id_column(schema, &edge_tables[hop_idx].0, false, hop_info)
        };

        corr_edge_cols.push(format!("{}.{}", edge_alias, edge_col));

        // Find outer column reference for this correlation variable
        let cte_col =
            find_cte_column_for_correlation_var(&cv.var_name, &cv.label, schema, cte_column_map);
        if let Some(cte_ref) = cte_col {
            corr_outer_cols.push(cte_ref);
        } else {
            log::warn!(
                "⚠️ No CTE column for correlation var '{}' in arrayCount — falling back",
                cv.var_name
            );
            return None;
        }
    }

    // Find list array column reference
    let list_col = find_cte_column_for_list_alias(&lc.list_alias, cte_column_map);
    let list_col = match list_col {
        Some(c) => c,
        None => {
            log::warn!(
                "⚠️ No CTE column for list alias '{}' in arrayCount",
                lc.list_alias
            );
            return None;
        }
    };

    // Handle additional WHERE clause from pattern
    if let Some(ref where_expr) = pc_meta.where_clause {
        if let Some(where_sql) = render_pc_where_clause(
            where_expr,
            &pc_meta.pattern_hops,
            &edge_tables,
            schema,
            &mut join_clauses,
        ) {
            where_conditions.push(where_sql);
        }
    }

    // Optimization: push correlation variables into WHERE as non-correlated
    // subqueries against the source CTE. This avoids the massive tuple hash
    // set from the full cross-product.
    //
    // Transforms:
    //   arrayCount(x -> (x, P) IN (SELECT PostId, PersonId FROM A JOIN B), arr)
    // into:
    //   arrayCount(x -> x IN (SELECT PostId FROM A JOIN B
    //     WHERE PersonId IN (SELECT DISTINCT p6_person_id FROM source_cte)), arr)
    //
    // The IN subquery is fully non-correlated: both the inner filter and the
    // outer IN reference only CTE names and literal columns.
    let select_cols = [list_element_col.clone()];

    if !corr_outer_cols.is_empty() {
        // Derive the FROM alias from the CTE name for validation.
        // outer_col is like "from_alias.p6_person_id" — we must verify the alias
        // prefix matches the FROM source to avoid binding the column to the wrong CTE.
        let from_alias_prefix = from_cte_name
            .as_deref()
            .map(|n| extract_from_alias_from_cte_name(n).to_string());

        for (edge_col, outer_col) in corr_edge_cols.iter().zip(corr_outer_cols.iter()) {
            if let Some(dot_pos) = outer_col.find('.') {
                let alias_prefix = &outer_col[..dot_pos];
                let col_name = &outer_col[dot_pos + 1..];

                // Only apply optimized path when the alias matches the FROM source
                let alias_matches_from = from_alias_prefix
                    .as_ref()
                    .is_some_and(|from_alias| alias_prefix == from_alias.as_str());

                if alias_matches_from {
                    let cte_name = from_cte_name.as_ref().unwrap();
                    where_conditions.push(format!(
                        "{} IN (SELECT DISTINCT {} FROM {})",
                        edge_col, col_name, cte_name
                    ));
                } else {
                    // Alias doesn't match FROM source — fall back to tuple approach
                    // to avoid binding the column to the wrong CTE
                    log::warn!(
                        "arrayCount optimization skipped: outer column '{}' alias '{}' doesn't match FROM alias {:?}",
                        outer_col, alias_prefix, from_alias_prefix
                    );
                    return generate_list_comp_array_count_tuple_fallback(
                        &list_element_col,
                        &corr_edge_cols,
                        &corr_outer_cols,
                        &from_clause,
                        &join_clauses,
                        &where_conditions,
                        &list_col,
                    );
                }
            } else {
                // No dot — fall back to tuple approach
                return generate_list_comp_array_count_tuple_fallback(
                    &list_element_col,
                    &corr_edge_cols,
                    &corr_outer_cols,
                    &from_clause,
                    &join_clauses,
                    &where_conditions,
                    &list_col,
                );
            }
        }
    }

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let inner_select = format!(
        "SELECT {} FROM {}{}{}",
        select_cols.join(", "),
        from_clause,
        joins_str,
        where_str
    );

    let result = emit_array_count_in_subquery("x", &inner_select, &list_col);

    log::info!(
        "🔧 array-count expression: {}",
        &result[..result.len().min(300)]
    );

    Some(result)
}

/// Emit an `arrayCount(lambda_var -> predicate, array)` call for the active
/// dialect.
///
/// ClickHouse: `arrayCount(x -> pred, arr)` — single function, predicate-first.
/// Databricks/Spark: `size(filter(arr, x -> pred))` — structural rewrite.
/// Calling `FunctionMapper::array_count()` on the Databricks mapper panics;
/// see `sql_generator::function_mapper::databricks` module docs.
///
/// Currently exercised only by this module's own unit tests (no production caller) —
/// scoped `allow(dead_code)` instead of the module-level one removed in Phase 2, since
/// `#[cfg(test)]` callers aren't visible to a non-test build of the lib target.
#[allow(dead_code)]
pub(crate) fn emit_array_count_call(lambda_var: &str, predicate: &str, array: &str) -> String {
    match crate::server::query_context::get_current_dialect() {
        crate::sql_generator::SqlDialect::Databricks => {
            format!("size(filter({array}, {lambda_var} -> {predicate}))")
        }
        _ => format!(
            "{}({lambda_var} -> {predicate}, {array})",
            current_function_mapper().array_count()
        ),
    }
}

/// Count array elements whose membership is tested by an `IN (subquery)`
/// predicate (the list-comprehension-with-pattern idiom, e.g. LDBC Q10's
/// `size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])`).
///
/// ClickHouse keeps the native `arrayCount(x -> <lhs> IN (subq), arr)` — CH
/// allows subqueries inside lambdas. Spark/Databricks forbids ANY subquery
/// inside a higher-order-function lambda (`UNSUPPORTED_SUBQUERY_EXPRESSION_
/// CATEGORY.HIGHER_ORDER_FUNCTION`), so neither `size(filter(arr, x -> x IN
/// (subq)))` nor an `array_contains` over a CTE-bound set works. Instead explode
/// the array in a scalar subquery and move the membership test to a plain WHERE:
/// `(SELECT count(*) FROM (SELECT explode(arr) AS x) WHERE <lhs> IN (subq))`.
/// This is duplicate-preserving — matching `arrayCount` semantics exactly (unlike
/// `array_intersect`, which would dedupe) — and supports a correlated tuple `lhs`.
pub(crate) fn emit_array_count_in_subquery(
    membership_lhs: &str,
    inner_select: &str,
    array: &str,
) -> String {
    match crate::server::query_context::get_current_dialect() {
        crate::sql_generator::SqlDialect::Databricks => format!(
            "(SELECT count(*) FROM (SELECT explode({array}) AS x) WHERE {membership_lhs} IN ({inner_select}))"
        ),
        _ => format!(
            "{}(x -> {membership_lhs} IN ({inner_select}), {array})",
            current_function_mapper().array_count()
        ),
    }
}

/// Fallback: generate arrayCount with the original tuple-based approach.
/// Used when from_cte_name is unavailable for the optimized non-correlated filter.
fn generate_list_comp_array_count_tuple_fallback(
    list_element_col: &str,
    corr_edge_cols: &[String],
    corr_outer_cols: &[String],
    from_clause: &str,
    join_clauses: &[String],
    where_conditions: &[String],
    list_col: &str,
) -> Option<String> {
    let mut select_cols = vec![list_element_col.to_string()];
    select_cols.extend(corr_edge_cols.iter().cloned());

    let joins_str = if join_clauses.is_empty() {
        String::new()
    } else {
        format!(" {}", join_clauses.join(" "))
    };

    let where_str = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    let lambda_tuple = if corr_outer_cols.is_empty() {
        "x".to_string()
    } else {
        let mut parts = vec!["x".to_string()];
        parts.extend(corr_outer_cols.iter().cloned());
        format!("({})", parts.join(", "))
    };

    let inner_select = format!(
        "SELECT {} FROM {}{}{}",
        select_cols.join(", "),
        from_clause,
        joins_str,
        where_str
    );

    let result = emit_array_count_in_subquery(&lambda_tuple, &inner_select, list_col);

    log::info!(
        "🔧 array-count tuple fallback: {}",
        &result[..result.len().min(300)]
    );

    Some(result)
}

/// Generate a correlated subquery for pattern comprehensions that contain
/// `Direction::Either` (undirected) hops. For each undirected hop, we generate
/// two subqueries (forward and reverse direction) and UNION ALL them, then
/// wrap with an outer SELECT COUNT(*).
///
/// For a single-hop pattern like `(person)-[:KNOWS]-(friend)`:
/// ```sql
/// (SELECT COUNT(*) FROM (
///     SELECT 1 FROM ldbc.Person_knows_Person AS __r0
///     WHERE __r0.Person1Id = cte.person_id
///   UNION ALL
///     SELECT 1 FROM ldbc.Person_knows_Person AS __r0
///     WHERE __r0.Person2Id = cte.person_id
/// ) AS __u)
/// ```
fn generate_pc_correlated_subquery_with_either(
    pc_meta: &crate::query_planner::logical_plan::PatternComprehensionMeta,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    use crate::query_planner::logical_plan::PatternPosition;

    // For Direction::Either, we create two copies of the pattern:
    // one treating it as Outgoing, one as Incoming.
    // Both share the same edge table but use different from/to ID columns.
    let mut direction_variants: Vec<Vec<crate::query_planner::logical_expr::Direction>> =
        vec![vec![]];

    for hop in &pc_meta.pattern_hops {
        if hop.direction == crate::query_planner::logical_expr::Direction::Either {
            // Fork: each existing variant gets duplicated with Outgoing and Incoming
            let mut new_variants = Vec::new();
            for variant in &direction_variants {
                let mut v_out = variant.clone();
                v_out.push(crate::query_planner::logical_expr::Direction::Outgoing);
                new_variants.push(v_out);

                let mut v_in = variant.clone();
                v_in.push(crate::query_planner::logical_expr::Direction::Incoming);
                new_variants.push(v_in);
            }
            direction_variants = new_variants;
        } else {
            for variant in &mut direction_variants {
                variant.push(hop.direction.clone());
            }
        }
    }

    log::info!(
        "🔧 Direction::Either expansion: {} direction variants for {} hops",
        direction_variants.len(),
        pc_meta.pattern_hops.len()
    );

    let mut union_parts: Vec<String> = Vec::new();

    for directions in &direction_variants {
        // Build edge_tables for this direction variant
        let mut edge_tables: Vec<(
            String,
            String,
            &crate::query_planner::logical_plan::ConnectedPatternInfo,
            crate::query_planner::logical_expr::Direction,
        )> = Vec::new();

        let mut all_found = true;
        for (hop_idx, (hop, dir)) in pc_meta
            .pattern_hops
            .iter()
            .zip(directions.iter())
            .enumerate()
        {
            let rel_alias = format!("__r{}", hop_idx);

            let (from_label, to_label) = match dir {
                crate::query_planner::logical_expr::Direction::Incoming => {
                    (hop.end_label.as_deref(), hop.start_label.as_deref())
                }
                _ => (hop.start_label.as_deref(), hop.end_label.as_deref()),
            };

            let rel_type = hop.rel_type.as_deref();
            let db_table = find_edge_table_in_schema(schema, rel_type, from_label, to_label);

            if let Some(table) = db_table {
                edge_tables.push((table, rel_alias, hop, dir.clone()));
            } else {
                // Try reversed labels for Either direction that couldn't find forward match
                if *dir == crate::query_planner::logical_expr::Direction::Outgoing {
                    let db_table_rev =
                        find_edge_table_in_schema(schema, rel_type, to_label, from_label);
                    if let Some(table) = db_table_rev {
                        // Found with reversed labels — treat as Incoming for this variant
                        edge_tables.push((
                            table,
                            format!("__r{}", hop_idx),
                            hop,
                            crate::query_planner::logical_expr::Direction::Incoming,
                        ));
                        continue;
                    }
                }
                log::warn!(
                    "⚠️ No edge table for Either hop {}: rel={:?}, from={:?}, to={:?}",
                    hop_idx,
                    rel_type,
                    from_label,
                    to_label
                );
                all_found = false;
                break;
            }
        }

        if !all_found {
            continue;
        }

        // Build FROM + JOINs + WHERE for this direction variant
        let mut from_clause = String::new();
        let mut join_clauses: Vec<String> = Vec::new();
        let mut where_conditions: Vec<String> = Vec::new();

        for (idx, (db_table, alias, _hop, _dir)) in edge_tables.iter().enumerate() {
            if idx == 0 {
                from_clause = format!("{} AS {}", db_table, alias);
            } else {
                let prev_alias = &edge_tables[idx - 1].1;
                let prev_hop = edge_tables[idx - 1].2;
                let prev_dir = &edge_tables[idx - 1].3;

                // Create a temporary ConnectedPatternInfo with the effective direction
                let prev_to_col = find_edge_id_column_with_direction(
                    schema,
                    &edge_tables[idx - 1].0,
                    false,
                    prev_hop,
                    prev_dir,
                );
                let curr_from_col =
                    find_edge_id_column_with_direction(schema, db_table, true, _hop, _dir);

                join_clauses.push(format!(
                    "INNER JOIN {} AS {} ON {}.{} = {}.{}",
                    db_table, alias, prev_alias, prev_to_col, alias, curr_from_col
                ));
            }
        }

        // WHERE conditions for correlation variables
        for cv in &pc_meta.correlation_vars {
            let (hop_idx, is_start) = match &cv.pattern_position {
                PatternPosition::StartOfHop(idx) => (*idx, true),
                PatternPosition::EndOfHop(idx) => (*idx, false),
            };

            if hop_idx >= edge_tables.len() {
                continue;
            }

            let (_, edge_alias, hop_info, dir) = &edge_tables[hop_idx];
            let edge_col = find_edge_id_column_with_direction(
                schema,
                &edge_tables[hop_idx].0,
                is_start,
                hop_info,
                dir,
            );

            let cte_col = find_cte_column_for_correlation_var(
                &cv.var_name,
                &cv.label,
                schema,
                cte_column_map,
            );

            if let Some(cte_ref) = cte_col {
                where_conditions.push(format!("{}.{} = {}", edge_alias, edge_col, cte_ref));
            }
        }

        // WHERE clause from pattern comprehension
        if let Some(ref where_expr) = pc_meta.where_clause {
            if let Some(where_sql) = render_pc_where_clause(
                where_expr,
                &pc_meta.pattern_hops,
                &edge_tables
                    .iter()
                    .map(|(t, a, h, _)| (t.clone(), a.clone(), *h))
                    .collect::<Vec<_>>(),
                schema,
                &mut join_clauses,
            ) {
                where_conditions.push(where_sql);
            }
        }

        let where_str = if where_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_conditions.join(" AND "))
        };

        let joins_str = if join_clauses.is_empty() {
            String::new()
        } else {
            format!(" {}", join_clauses.join(" "))
        };

        union_parts.push(format!(
            "SELECT 1 FROM {}{}{}",
            from_clause, joins_str, where_str
        ));
    }

    if union_parts.is_empty() {
        return None;
    }

    if union_parts.len() == 1 {
        // Single variant (no Either hops were actually encountered, shouldn't happen but safe)
        Some(format!(
            "(SELECT COUNT(*) FROM ({}) AS __u)",
            union_parts[0]
        ))
    } else {
        let union_sql = union_parts.join(" UNION ALL ");
        Some(format!("(SELECT COUNT(*) FROM ({}) AS __u)", union_sql))
    }
}

/// Like `find_edge_id_column` but takes an explicit direction parameter
/// instead of reading from the hop's direction field. Used by the Either
/// direction handler which overrides the hop's original direction.
fn find_edge_id_column_with_direction(
    schema: &GraphSchema,
    db_table: &str,
    is_from: bool,
    _hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
    direction: &crate::query_planner::logical_expr::Direction,
) -> String {
    for rel_schema in schema.get_relationships_schemas().values() {
        let table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        if table == db_table {
            let effective_is_from = match direction {
                crate::query_planner::logical_expr::Direction::Incoming => !is_from,
                _ => is_from,
            };

            return if effective_is_from {
                rel_schema.from_id.first_column().to_string()
            } else {
                rel_schema.to_id.first_column().to_string()
            };
        }
    }

    if is_from {
        "from_id".to_string()
    } else {
        "to_id".to_string()
    }
}

/// Find an edge table in schema matching the given rel_type, from_label, and to_label.
/// Returns fully qualified table name (database.table).
fn find_edge_table_in_schema(
    schema: &GraphSchema,
    rel_type: Option<&str>,
    from_label: Option<&str>,
    to_label: Option<&str>,
) -> Option<String> {
    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());

    let mut best_match: Option<String> = None;

    for (rel_key, rel_schema) in &sorted_rels {
        let key_rel_name = rel_key.split("::").next().unwrap_or(rel_key);

        // Filter by relationship type
        if let Some(rt) = rel_type {
            if !key_rel_name.eq_ignore_ascii_case(rt) {
                continue;
            }
        }

        let db_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);

        // If from_label/to_label specified, match them
        let from_matches = from_label.is_none()
            || from_label.is_some_and(|fl| {
                rel_schema.from_node.eq_ignore_ascii_case(fl) || rel_schema.from_node == "$any"
            });
        let to_matches = to_label.is_none()
            || to_label.is_some_and(|tl| {
                rel_schema.to_node.eq_ignore_ascii_case(tl) || rel_schema.to_node == "$any"
            });

        if from_matches && to_matches {
            // Prefer exact match over $any
            let is_exact = from_label
                .is_some_and(|fl| rel_schema.from_node.eq_ignore_ascii_case(fl))
                && to_label.is_some_and(|tl| rel_schema.to_node.eq_ignore_ascii_case(tl));

            if is_exact {
                return Some(db_table);
            }
            if best_match.is_none() {
                best_match = Some(db_table);
            }
        }
    }

    best_match
}

/// Find the from_id or to_id column name for an edge table.
/// `is_from` = true → from_id column, false → to_id column.
fn find_edge_id_column(
    schema: &GraphSchema,
    db_table: &str,
    is_from: bool,
    hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
) -> String {
    find_edge_id_column_with_direction(schema, db_table, is_from, hop, &hop.direction)
}

/// Find the CTE column reference for a correlation variable.
/// Looks up in cte_column_map by (var_name, "id") pattern.
fn find_cte_column_for_correlation_var(
    var_name: &str,
    label: &str,
    schema: &GraphSchema,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    // Try common ID column patterns
    let id_col = if let Ok(ns) = schema.node_schema(label) {
        ns.node_id.id.first_column().to_string()
    } else {
        "id".to_string()
    };

    // Look up (var_name, id_col) in the CTE column map
    if let Some(cte_ref) = cte_column_map.get(&(var_name.to_string(), id_col.clone())) {
        return Some(cte_ref.clone());
    }

    // Also try just "id"
    if id_col != "id" {
        if let Some(cte_ref) = cte_column_map.get(&(var_name.to_string(), "id".to_string())) {
            return Some(cte_ref.clone());
        }
    }

    log::debug!(
        "🔍 CTE column map lookup failed for var='{}', id_col='{}'. Map keys: {:?}",
        var_name,
        id_col,
        cte_column_map.keys().collect::<Vec<_>>()
    );

    None
}

/// Find the CTE column reference for a list alias (e.g., "posts" from `collect(post) AS posts`).
/// Looks for the alias as a direct key in the CTE column map — list aliases are scalar columns
/// (arrays), not node aliases with property sub-columns.
fn find_cte_column_for_list_alias(
    list_alias: &str,
    cte_column_map: &HashMap<(String, String), String>,
) -> Option<String> {
    // List aliases like "posts" are stored as (alias, "*") or (alias, alias) in CTE column maps.
    // Try several patterns used by the CTE naming system.

    // Pattern 1: (alias, "*") — wildcard property
    if let Some(cte_ref) = cte_column_map.get(&(list_alias.to_string(), "*".to_string())) {
        return Some(cte_ref.clone());
    }

    // Pattern 2: (alias, alias) — self-reference
    if let Some(cte_ref) = cte_column_map.get(&(list_alias.to_string(), list_alias.to_string())) {
        return Some(cte_ref.clone());
    }

    // Pattern 3: Search for any key where the first element matches the alias.
    // Collect all matches to detect ambiguity — only return if exactly one match.
    let matches: Vec<&String> = cte_column_map
        .iter()
        .filter(|((alias, _prop), _)| alias == list_alias)
        .map(|(_, cte_ref)| cte_ref)
        .collect();
    match matches.len() {
        1 => return Some(matches[0].clone()),
        n if n > 1 => {
            log::warn!(
                "Ambiguous CTE column for list alias '{}': {} matches found. Skipping.",
                list_alias,
                n
            );
        }
        _ => {}
    }

    log::debug!(
        "🔍 List alias CTE column lookup failed for '{}'. Map keys: {:?}",
        list_alias,
        cte_column_map.keys().collect::<Vec<_>>()
    );

    None
}

/// Render a WHERE clause from a LogicalExpr for use inside a correlated subquery.
/// Resolves property accesses to schema-mapped column names on edge table aliases.
/// May add additional JOINs for intermediate node tables referenced in the WHERE clause.
fn render_pc_where_clause(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    schema: &GraphSchema,
    join_clauses: &mut Vec<String>,
) -> Option<String> {
    // Build a map of alias → (label, node table alias) for intermediate nodes in the pattern
    // that might be referenced in the WHERE clause
    let mut node_alias_map: HashMap<String, (String, String)> = HashMap::new(); // alias → (label, sql_alias)
    let mut node_joins_added: HashSet<String> = HashSet::new();

    for (hop_idx, hop) in pattern_hops.iter().enumerate() {
        // Check start node
        if let (Some(ref alias), Some(ref label)) = (&hop.start_alias, &hop.start_label) {
            if !node_alias_map.contains_key(alias.as_str()) {
                let sql_alias = format!("__n{}s", hop_idx);
                node_alias_map.insert(alias.clone(), (label.clone(), sql_alias));
            }
        }
        // Check end node
        if let (Some(ref alias), Some(ref label)) = (&hop.end_alias, &hop.end_label) {
            if !node_alias_map.contains_key(alias.as_str()) {
                let sql_alias = format!("__n{}e", hop_idx);
                node_alias_map.insert(alias.clone(), (label.clone(), sql_alias));
            }
        }
    }

    let sql = render_logical_expr_to_sql(
        expr,
        &node_alias_map,
        pattern_hops,
        edge_tables,
        schema,
        join_clauses,
        &mut node_joins_added,
    );

    if sql.is_empty() {
        None
    } else {
        Some(sql)
    }
}

/// Recursively render a LogicalExpr to SQL for use in a correlated subquery WHERE clause.
fn render_logical_expr_to_sql(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    node_alias_map: &HashMap<String, (String, String)>,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    schema: &GraphSchema,
    join_clauses: &mut Vec<String>,
    node_joins_added: &mut HashSet<String>,
) -> String {
    use crate::query_planner::logical_expr::LogicalExpr;

    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            let alias = &pa.table_alias.0;
            let prop_name = match &pa.column {
                crate::graph_catalog::expression_parser::PropertyValue::Column(col) => col.clone(),
                other => format!("{:?}", other),
            };

            // Look up alias in node_alias_map to find the label and SQL alias
            if let Some((label, sql_alias)) = node_alias_map.get(alias) {
                // Add JOIN for this node table if not already added
                if !node_joins_added.contains(alias) {
                    if let Ok(ns) = schema.node_schema(label) {
                        let node_table = format!("{}.{}", ns.database, ns.table_name);
                        let node_id_col = ns.node_id.id.first_column();

                        // Find which edge table connects to this node
                        if let Some(join_condition) = find_node_edge_join_condition(
                            alias,
                            pattern_hops,
                            edge_tables,
                            node_id_col,
                            sql_alias,
                            schema,
                        ) {
                            join_clauses.push(format!(
                                "INNER JOIN {} AS {} ON {}",
                                node_table, sql_alias, join_condition
                            ));
                            node_joins_added.insert(alias.clone());
                        }
                    }
                }

                // Resolve property name through schema
                let db_col = if let Ok(ns) = schema.node_schema(label) {
                    ns.property_mappings
                        .get(&prop_name)
                        .map(|pv| pv.raw().to_string())
                        .unwrap_or(prop_name)
                } else {
                    prop_name
                };

                format!("{}.{}", sql_alias, db_col)
            } else {
                // Not a pattern node - might be an outer reference, use raw
                format!("{}.{}", alias, prop_name)
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            use crate::query_planner::logical_expr::Operator as Op;
            let op_str = match op.operator {
                Op::And => " AND ",
                Op::Or => " OR ",
                Op::LessThan => " < ",
                Op::GreaterThan => " > ",
                Op::Equal => " = ",
                Op::NotEqual => " <> ",
                Op::LessThanEqual => " <= ",
                Op::GreaterThanEqual => " >= ",
                Op::Addition => " + ",
                Op::Subtraction => " - ",
                Op::Multiplication => " * ",
                Op::Division => " / ",
                Op::ModuloDivision => " % ",
                Op::Exponentiation => " ^ ",
                Op::In => " IN ",
                Op::NotIn => " NOT IN ",
                Op::StartsWith | Op::EndsWith | Op::Contains | Op::RegexMatch => {
                    // These are function-like operators handled specially below
                    ""
                }
                Op::Not => "NOT ",
                Op::IsNull => " IS NULL",
                Op::IsNotNull => " IS NOT NULL",
                Op::Distinct => " ?? ",
            };

            // Handle function-like operators
            if matches!(
                op.operator,
                Op::StartsWith | Op::EndsWith | Op::Contains | Op::RegexMatch
            ) && op.operands.len() == 2
            {
                let left = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                let right = render_logical_expr_to_sql(
                    &op.operands[1],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return match op.operator {
                    Op::StartsWith => format!("startsWith({}, {})", left, right),
                    Op::EndsWith => format!("endsWith({}, {})", left, right),
                    Op::Contains => {
                        // Dialect-aware: Spark's position(substr, str) reverses CH's arg order.
                        crate::clickhouse_query_generator::contains_predicate(&left, &right)
                    }
                    Op::RegexMatch => format!("match({}, {})", left, right),
                    _ => unreachable!(),
                };
            }

            // Unary postfix operators (IS NULL, IS NOT NULL)
            if matches!(op.operator, Op::IsNull | Op::IsNotNull) && op.operands.len() == 1 {
                let operand = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return format!("{}{}", operand, op_str);
            }

            // Unary prefix operator (NOT)
            if op.operator == Op::Not && op.operands.len() == 1 {
                let operand = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                return format!("{}{}", op_str, operand);
            }

            if op.operands.len() == 2 {
                let left = render_logical_expr_to_sql(
                    &op.operands[0],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                let right = render_logical_expr_to_sql(
                    &op.operands[1],
                    node_alias_map,
                    pattern_hops,
                    edge_tables,
                    schema,
                    join_clauses,
                    node_joins_added,
                );
                format!("{}{}{}", left, op_str, right)
            } else {
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|o| {
                        render_logical_expr_to_sql(
                            o,
                            node_alias_map,
                            pattern_hops,
                            edge_tables,
                            schema,
                            join_clauses,
                            node_joins_added,
                        )
                    })
                    .collect();
                rendered.join(op_str)
            }
        }
        LogicalExpr::Literal(lit) => match lit {
            crate::query_planner::logical_expr::Literal::Integer(i) => i.to_string(),
            crate::query_planner::logical_expr::Literal::Float(f) => f.to_string(),
            crate::query_planner::logical_expr::Literal::String(s) => {
                format!("'{}'", s.replace('\'', "\\'"))
            }
            crate::query_planner::logical_expr::Literal::Boolean(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            _ => "NULL".to_string(),
        },
        LogicalExpr::Parameter(p) => format!("${}", p),
        _ => {
            log::warn!(
                "⚠️ Unhandled LogicalExpr variant in PC WHERE clause: {:?}",
                expr
            );
            String::new()
        }
    }
}

/// Find the JOIN condition to connect a node table to an edge table in the pattern.
fn find_node_edge_join_condition(
    node_alias: &str,
    pattern_hops: &[crate::query_planner::logical_plan::ConnectedPatternInfo],
    edge_tables: &[(
        String,
        String,
        &crate::query_planner::logical_plan::ConnectedPatternInfo,
    )],
    node_id_col: &str,
    node_sql_alias: &str,
    schema: &GraphSchema,
) -> Option<String> {
    for (hop_idx, hop) in pattern_hops.iter().enumerate() {
        if hop_idx >= edge_tables.len() {
            continue;
        }
        let (ref db_table, ref edge_alias, _) = edge_tables[hop_idx];

        // Check if this node is the start or end of this hop
        let is_start = hop.start_alias.as_deref() == Some(node_alias);
        let is_end = hop.end_alias.as_deref() == Some(node_alias);

        if is_start || is_end {
            let edge_col = find_edge_id_column_for_node(schema, db_table, hop, is_start);
            return Some(format!(
                "{}.{} = {}.{}",
                node_sql_alias, node_id_col, edge_alias, edge_col
            ));
        }
    }
    None
}

/// Find the edge ID column that corresponds to a node position (start/end) in a hop.
fn find_edge_id_column_for_node(
    schema: &GraphSchema,
    db_table: &str,
    hop: &crate::query_planner::logical_plan::ConnectedPatternInfo,
    is_start: bool,
) -> String {
    let sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();

    for (_, rel_schema) in &sorted_rels {
        let table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        if table == db_table {
            // For Incoming direction, start/end are swapped relative to schema from/to
            let effective_is_from = match hop.direction {
                crate::query_planner::logical_expr::Direction::Incoming => !is_start,
                _ => is_start,
            };

            return if effective_is_from {
                rel_schema.from_id.first_column().to_string()
            } else {
                rel_schema.to_id.first_column().to_string()
            };
        }
    }

    if is_start {
        "from_id".to_string()
    } else {
        "to_id".to_string()
    }
}

/// Replace count(*) placeholder expressions in a RenderPlan's SELECT items
/// with correlated subquery SQL strings.
///
/// Walks SELECT items in order, finds AggregateFnCall("count", [Star|Raw("*")])
/// expressions that were placeholder replacements from pattern comprehension rewriting,
/// and replaces them with Raw(subquery_sql).
fn replace_count_star_placeholders_in_select(
    select_items: &mut [SelectItem],
    pc_subqueries: &[String],
) {
    let mut pc_idx = 0;

    for item in select_items.iter_mut() {
        replace_count_star_in_expr(&mut item.expression, pc_subqueries, &mut pc_idx);
    }
}

/// Recursively find and replace count(*) placeholders in a RenderExpr tree.
fn replace_count_star_in_expr(expr: &mut RenderExpr, pc_subqueries: &[String], pc_idx: &mut usize) {
    match expr {
        RenderExpr::AggregateFnCall(agg) => {
            let is_count_star = agg.name.eq_ignore_ascii_case("count") && agg.args.len() == 1 && {
                let arg = &agg.args[0];
                matches!(arg, RenderExpr::Star)
                    || matches!(arg, RenderExpr::Raw(s) if s == "*")
                    || matches!(arg, RenderExpr::Literal(Literal::String(s)) if s == "*")
            };

            if is_count_star && *pc_idx < pc_subqueries.len() {
                log::info!(
                    "🔧 Replacing count(*) placeholder #{} with correlated subquery",
                    pc_idx
                );
                *expr = RenderExpr::Raw(pc_subqueries[*pc_idx].clone());
                *pc_idx += 1;
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                replace_count_star_in_expr(operand, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &mut f.args {
                replace_count_star_in_expr(arg, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref mut e) = case.expr {
                replace_count_star_in_expr(e, pc_subqueries, pc_idx);
            }
            for (ref mut when, ref mut then) in &mut case.when_then {
                replace_count_star_in_expr(when, pc_subqueries, pc_idx);
                replace_count_star_in_expr(then, pc_subqueries, pc_idx);
            }
            if let Some(ref mut e) = case.else_expr {
                replace_count_star_in_expr(e, pc_subqueries, pc_idx);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                replace_count_star_in_expr(item, pc_subqueries, pc_idx);
            }
        }
        _ => {}
    }
}

/// Build raw SQL for a pattern comprehension CTE.
///
/// Given a node label and direction, finds all matching edge tables in the schema
/// and generates a UNION ALL query that counts/aggregates connections, grouped by node_id.
///
/// Returns SQL like:
/// ```sql
/// SELECT node_id, COUNT(*) AS result FROM (
///   SELECT follower_id AS node_id FROM brahmand.user_follows_bench  -- outgoing
///   UNION ALL
///   SELECT followed_id AS node_id FROM brahmand.user_follows_bench  -- incoming
///   UNION ALL ...
/// ) GROUP BY node_id
/// ```
pub(crate) fn build_pattern_comprehension_sql(
    correlation_label: &str,
    direction: &crate::open_cypher_parser::ast::Direction,
    rel_types: &Option<Vec<String>>,
    agg_type: &crate::query_planner::logical_plan::AggregationType,
    schema: &GraphSchema,
    target_label: Option<&str>,
    target_property: Option<&str>,
) -> Option<String> {
    use crate::open_cypher_parser::ast::Direction;
    use crate::query_planner::logical_plan::AggregationType;

    // Resolve target node table/column for property-based aggregation (e.g., collect(f.name))
    let target_join_info = target_label.and_then(|tl| {
        target_property.and_then(|tp| {
            schema.node_schema(tl).ok().map(|ns| {
                let target_table = format!("{}.{}", ns.database, ns.table_name);
                let target_id = ns.node_id.id.to_pipe_joined_sql("__tgt");
                let db_column = ns
                    .property_mappings
                    .get(tp)
                    .map(|pv| pv.raw().to_string())
                    .unwrap_or_else(|| tp.to_string());
                (target_table, target_id, db_column, tl.to_string())
            })
        })
    });

    let mut branches: Vec<String> = Vec::new();

    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());
    for (rel_key, rel_schema) in sorted_rels {
        // Extract base relationship type from key (keys may be "TYPE::From::To")
        let rel_name = rel_key.split("::").next().unwrap_or(rel_key);
        // If specific rel types are requested, filter
        if let Some(types) = rel_types {
            if !types.iter().any(|t| t.eq_ignore_ascii_case(rel_name)) {
                continue;
            }
        }

        let db_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);

        // Build optional type_column filter for polymorphic edges
        let mut where_clauses = Vec::new();
        if let Some(ref type_col) = rel_schema.type_column {
            where_clauses.push(format!("{}.{} = '{}'", db_table, type_col, rel_name));
        }

        // Handle $any (polymorphic) from_node/to_node matching
        let from_matches = rel_schema.from_node.eq_ignore_ascii_case(correlation_label)
            || rel_schema.from_node == "$any";
        let to_matches = rel_schema.to_node.eq_ignore_ascii_case(correlation_label)
            || rel_schema.to_node == "$any";

        // Check outgoing: correlation_label is the from_node
        if (matches!(direction, Direction::Outgoing | Direction::Either)) && from_matches {
            let mut branch_where = where_clauses.clone();
            if rel_schema.from_node == "$any" {
                if let Some(ref from_label_col) = rel_schema.from_label_column {
                    branch_where.push(format!(
                        "{}.{} = '{}'",
                        db_table, from_label_col, correlation_label
                    ));
                }
            }
            let where_str = if branch_where.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", branch_where.join(" AND "))
            };
            // For property aggregation, JOIN the target node table
            if let Some((ref tgt_table, ref _tgt_id, ref tgt_col, ref _tgt_label)) =
                target_join_info
            {
                // Build JOIN condition: edge.to_id = target_node.node_id
                let join_cond = {
                    let edge_cols = rel_schema.to_id.columns();
                    let tgt_ns = schema.node_schema(&rel_schema.to_node).ok();
                    let tgt_cols = tgt_ns.map(|ns| ns.node_id.id.columns()).unwrap_or_default();
                    edge_cols
                        .iter()
                        .zip(tgt_cols.iter())
                        .map(|(e, t)| format!("{} = __tgt.{}", e, t))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                };
                branches.push(format!(
                    "SELECT {} AS node_id, __tgt.{} AS target_prop FROM {} INNER JOIN {} AS __tgt ON {}{}",
                    rel_schema.from_id.to_pipe_joined_sql(""),
                    tgt_col,
                    db_table,
                    tgt_table,
                    join_cond,
                    where_str
                ));
            } else {
                branches.push(format!(
                    "SELECT {} AS node_id FROM {}{}",
                    rel_schema.from_id.to_pipe_joined_sql(""),
                    db_table,
                    where_str
                ));
            }
        }

        // Check incoming: correlation_label is the to_node
        if (matches!(direction, Direction::Incoming | Direction::Either)) && to_matches {
            let mut branch_where = where_clauses.clone();
            if rel_schema.to_node == "$any" {
                if let Some(ref to_label_col) = rel_schema.to_label_column {
                    branch_where.push(format!(
                        "{}.{} = '{}'",
                        db_table, to_label_col, correlation_label
                    ));
                }
            }
            let where_str = if branch_where.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", branch_where.join(" AND "))
            };
            // For property aggregation, JOIN the target (from) node table
            if let Some((ref tgt_table, ref _tgt_id, ref tgt_col, ref _tgt_label)) =
                target_join_info
            {
                let join_cond = {
                    let edge_cols = rel_schema.from_id.columns();
                    let tgt_ns = schema.node_schema(&rel_schema.from_node).ok();
                    let tgt_cols = tgt_ns.map(|ns| ns.node_id.id.columns()).unwrap_or_default();
                    edge_cols
                        .iter()
                        .zip(tgt_cols.iter())
                        .map(|(e, t)| format!("{} = __tgt.{}", e, t))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                };
                branches.push(format!(
                    "SELECT {} AS node_id, __tgt.{} AS target_prop FROM {} INNER JOIN {} AS __tgt ON {}{}",
                    rel_schema.to_id.to_pipe_joined_sql(""),
                    tgt_col,
                    db_table,
                    tgt_table,
                    join_cond,
                    where_str
                ));
            } else {
                branches.push(format!(
                    "SELECT {} AS node_id FROM {}{}",
                    rel_schema.to_id.to_pipe_joined_sql(""),
                    db_table,
                    where_str
                ));
            }
        }
    }

    if branches.is_empty() {
        return None;
    }

    // All branches output a single uniform column (node_id), so UNION ALL is safe.
    // Aggregate outside: COUNT(*) counts all rows per node_id across all edge tables.
    let union_sql = branches.join(" UNION ALL ");
    let agg_fn = match agg_type {
        AggregationType::Count => "COUNT(*)".to_string(),
        AggregationType::GroupArray => {
            // Dialect-aware list aggregate: CH `groupArray`, Spark `collect_list`.
            let collect =
                crate::sql_generator::function_mapper::current_function_mapper().collect_list();
            if target_join_info.is_some() {
                format!("{collect}(target_prop)")
            } else {
                format!("{collect}(1)")
            }
        }
        AggregationType::Sum => "SUM(1)".to_string(),
        AggregationType::Avg => "AVG(1)".to_string(),
        AggregationType::Min => "MIN(1)".to_string(),
        AggregationType::Max => "MAX(1)".to_string(),
    };

    Some(format!(
        "SELECT node_id, {} AS result FROM ({}) GROUP BY node_id",
        agg_fn, union_sql
    ))
}

/// Build a RenderExpr for a node's ID, handling composite keys.
/// For single IDs: `alias.col` (PropertyAccess)
/// For composite IDs: `concat(toString(alias.col1), '|', toString(alias.col2))`
pub(crate) fn build_node_id_expr_for_join(
    from_alias: &str,
    label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    if let Ok(ns) = schema.node_schema(label) {
        return build_id_render_expr(&ns.node_id.id, from_alias);
    }

    // Fallback: use find_node_id_column_from_schema
    let id_col = find_node_id_column_from_schema("", label, schema);
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(from_alias.to_string()),
        column: PropertyValue::Column(id_col),
    })
}

/// Convert an Identifier to a RenderExpr with the given alias.
/// Single: `alias.col`, Composite: `concat(toString(alias.c1), '|', toString(alias.c2))`
pub(super) fn build_id_render_expr(
    id: &crate::graph_catalog::config::Identifier,
    alias: &str,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    if id.is_composite() {
        let parts: Vec<RenderExpr> = id
            .columns()
            .iter()
            .enumerate()
            .flat_map(|(i, col)| {
                let mut items = Vec::new();
                if i > 0 {
                    items.push(RenderExpr::Literal(Literal::String("|".to_string())));
                }
                items.push(RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: current_function_mapper().cast_string().to_string(),
                    args: vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(col.to_string()),
                    })],
                }));
                items
            })
            .collect();
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "concat".to_string(),
            args: parts,
        })
    } else {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(id.first_column().to_string()),
        })
    }
}

/// Find the ID column for a node label in the schema.
/// E.g., for label "User" in the social benchmark, returns "user_id".
pub(crate) fn find_node_id_column_from_schema(
    _alias: &str,
    label: &str,
    schema: &GraphSchema,
) -> String {
    // Look through node schemas to find the ID column
    if let Ok(node_schema) = schema.node_schema(label) {
        return node_schema.node_id.id.first_column().to_string();
    }

    // Fallback: look through relationship schemas for from_node/to_node matching
    let mut sorted_rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    sorted_rels.sort_by_key(|(k, _)| k.as_str());
    for (_, rel_schema) in sorted_rels {
        if rel_schema.from_node.eq_ignore_ascii_case(label) {
            return rel_schema.from_id.first_column().to_string();
        }
        if rel_schema.to_node.eq_ignore_ascii_case(label) {
            return rel_schema.to_id.first_column().to_string();
        }
    }

    // Last resort: generic "id"
    log::debug!(
        "⚠️  Could not find ID column for label '{}', defaulting to 'id'",
        label
    );
    "id".to_string()
}

/// Find the CTE column reference for a correlation variable in the WITH CTE body.
/// Used to build LEFT JOIN ON conditions between the WITH CTE and a PC CTE.
///
/// The correlation variable (e.g., "tag" with label "Tag") needs to resolve to
/// a qualified column reference (e.g., "person_tag.p3_tag_id") in the WITH CTE body.
pub(crate) fn find_pc_cte_join_column(
    var_name: &str,
    label: &str,
    schema: &GraphSchema,
    with_cte_render: &RenderPlan,
    cte_name: &str,
) -> Option<String> {
    // Build the CTE column map from the WITH CTE body
    let cte_col_map = build_cte_column_map(with_cte_render, cte_name);

    // Also include direct JOINs as available references
    let mut col_map = cte_col_map;
    for join in &with_cte_render.joins.0 {
        let join_alias = &join.table_alias;
        col_map
            .entry((join_alias.clone(), "id".to_string()))
            .or_insert_with(|| format!("{}.id", join_alias));
    }

    // Scan FROM's ViewScan property_mapping
    if let FromTableItem(Some(ref from)) = with_cte_render.from {
        if let Some(ref from_alias) = from.alias {
            if let LogicalPlan::ViewScan(ref scan) = from.source.as_ref() {
                for col_value in scan.property_mapping.values() {
                    if let crate::graph_catalog::expression_parser::PropertyValue::Column(
                        ref col_name,
                    ) = col_value
                    {
                        if let Some((parsed_alias, parsed_property)) = parse_cte_column(col_name) {
                            let qualified = format!("{}.{}", from_alias, col_name);
                            col_map
                                .entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        } else {
                            let qualified = quote_qualified_col(from_alias, col_name);
                            col_map
                                .entry((col_name.clone(), "id".to_string()))
                                .or_insert(qualified.clone());
                            col_map
                                .entry((col_name.clone(), col_name.clone()))
                                .or_insert(qualified);
                        }
                    }
                }
            }
        }
    }

    // Augment with correlation variable CTE column names.
    // If the CTE has a bare column matching the variable name (UNWIND scalar —
    // the alias IS the ID value), prefer `from_alias."person"` over generating
    // `from_alias.p6_person_id` which wouldn't exist.
    // Uses dual-condition guard (same as Phase D) for robustness:
    //   1. CTE SELECT has a column with bare alias match (not pN_ prefixed)
    //   2. FROM ViewScan has a bare property column matching var_name
    if let FromTableItem(Some(ref from)) = with_cte_render.from {
        if let Some(ref from_alias) = from.alias {
            let key = (var_name.to_string(), "id".to_string());
            col_map.entry(key).or_insert_with(|| {
                // Condition 1: CTE SELECT has a bare alias matching var_name
                let cte_has_bare_alias = with_cte_render
                    .select
                    .items
                    .iter()
                    .any(|item| item.col_alias.as_ref().is_some_and(|ca| ca.0 == var_name));

                // Condition 2: FROM ViewScan has a bare column matching var_name
                let has_bare_column = if let LogicalPlan::ViewScan(ref scan) = from.source.as_ref()
                {
                    scan.property_mapping.values().any(|pv| {
                        if let crate::graph_catalog::expression_parser::PropertyValue::Column(
                            ref col,
                        ) = pv
                        {
                            col == var_name
                        } else {
                            false
                        }
                    })
                } else {
                    false
                };
                if cte_has_bare_alias && has_bare_column {
                    // UNWIND scalar: the column name IS the alias
                    quote_qualified_col(from_alias, var_name)
                } else {
                    let cte_col = crate::utils::cte_column_naming::cte_column_name(var_name, "id");
                    format!("{}.{}", from_alias, cte_col)
                }
            });
        }
    }

    find_cte_column_for_correlation_var(var_name, label, schema, &col_map)
}

/// Add a JOIN to a render plan, handling UNION branches.
/// If the plan has UNION branches, the join is cloned into each branch.
pub(crate) fn add_join_to_plan_or_union_branches(plan: &mut RenderPlan, join: Join) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        // Add to each UNION branch AND the outer plan.
        // The outer plan's FROM+JOINs form the first UNION ALL branch
        // in CTE body rendering, so it needs the join too.
        for (bi, branch) in union.input.iter_mut().enumerate() {
            log::debug!(
                "🔧 add_join_to_plan_or_union_branches: branch {} has {} existing joins, adding '{}'",
                bi,
                branch.joins.0.len(),
                join.table_alias
            );
            branch.joins.0.push(join.clone());
        }
        plan.joins.0.push(join);
    } else {
        log::debug!(
            "🔧 add_join_to_plan_or_union_branches: non-union plan has {} existing joins, adding '{}'",
            plan.joins.0.len(),
            join.table_alias
        );
        plan.joins.0.push(join);
    }
}

/// Replace count(*) placeholders in SELECT items, handling UNION branches.
pub(crate) fn replace_count_star_placeholders_in_select_or_union(
    plan: &mut RenderPlan,
    pc_replacements: &[String],
) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in union.input.iter_mut() {
            replace_count_star_placeholders_in_select(&mut branch.select.items, pc_replacements);
        }
    }
    // Always replace in the main plan's SELECT too
    replace_count_star_placeholders_in_select(&mut plan.select.items, pc_replacements);
}

/// Generate and replace arrayCount subqueries for list-constraint pattern comprehensions.
/// This handles only PCs with list_constraint (e.g., `size([p IN posts WHERE pattern])`).
/// Non-list PCs are handled by the CTE approach and should not be passed here.
pub(crate) fn generate_and_replace_arraycount_pc_subqueries(
    plan: &mut RenderPlan,
    pattern_comprehensions: &[crate::query_planner::logical_plan::PatternComprehensionMeta],
    schema: &GraphSchema,
    cte_name: &str,
) {
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in union.input.iter_mut() {
            generate_and_replace_arraycount_pc_subqueries(
                branch,
                pattern_comprehensions,
                schema,
                cte_name,
            );
        }
        return;
    }

    if plan.select.items.is_empty() {
        return;
    }

    let cte_col_map = build_cte_column_map(plan, cte_name);
    let mut branch_col_map = cte_col_map;

    // Include JOINs as available references
    for join in &plan.joins.0 {
        let join_alias = &join.table_alias;
        branch_col_map
            .entry((join_alias.clone(), "id".to_string()))
            .or_insert_with(|| format!("{}.id", join_alias));
    }

    // Scan FROM's ViewScan property_mapping
    if let FromTableItem(Some(ref from)) = plan.from {
        if let Some(ref from_alias) = from.alias {
            if let LogicalPlan::ViewScan(ref scan) = from.source.as_ref() {
                for col_value in scan.property_mapping.values() {
                    if let crate::graph_catalog::expression_parser::PropertyValue::Column(
                        ref col_name,
                    ) = col_value
                    {
                        if let Some((parsed_alias, parsed_property)) = parse_cte_column(col_name) {
                            let qualified = format!("{}.{}", from_alias, col_name);
                            branch_col_map
                                .entry((parsed_alias, parsed_property))
                                .or_insert(qualified);
                        } else {
                            let qualified = quote_qualified_col(from_alias, col_name);
                            branch_col_map
                                .entry((col_name.clone(), "id".to_string()))
                                .or_insert(qualified.clone());
                            branch_col_map
                                .entry((col_name.clone(), col_name.clone()))
                                .or_insert(qualified);
                        }
                    }
                }
            }
        }
    }

    // Augment with correlation variables and list aliases
    if let FromTableItem(Some(ref from)) = plan.from {
        if let Some(ref from_alias) = from.alias {
            for pc in pattern_comprehensions.iter() {
                for cv in &pc.correlation_vars {
                    let key = (cv.var_name.clone(), "id".to_string());
                    branch_col_map.entry(key).or_insert_with(|| {
                        let cte_col =
                            crate::utils::cte_column_naming::cte_column_name(&cv.var_name, "id");
                        format!("{}.{}", from_alias, cte_col)
                    });
                }
                if let Some(ref lc) = pc.list_constraint {
                    let key1 = (lc.list_alias.clone(), "id".to_string());
                    if !branch_col_map.contains_key(&key1) {
                        let qualified = quote_qualified_col(from_alias, &lc.list_alias);
                        branch_col_map.insert(key1, qualified.clone());
                        branch_col_map
                            .insert((lc.list_alias.clone(), lc.list_alias.clone()), qualified);
                    }
                }
            }
        }
    }

    // Extract FROM CTE name for arrayCount optimization
    let from_cte_name = if let FromTableItem(Some(ref from)) = plan.from {
        Some(from.name.clone())
    } else {
        None
    };

    // Only generate subqueries for list_constraint PCs (arrayCount path)
    let mut pc_subqueries: Vec<String> = Vec::new();
    for pc_meta in pattern_comprehensions {
        if pc_meta.pattern_hops.is_empty() {
            continue;
        }
        if pc_meta.list_constraint.is_some() {
            if let Some(subquery_sql) = generate_pattern_comprehension_correlated_subquery(
                pc_meta,
                schema,
                &branch_col_map,
                from_cte_name.clone(),
            ) {
                pc_subqueries.push(subquery_sql);
            } else {
                pc_subqueries.push("0".to_string());
            }
        }
        // Non-list PCs: already handled as COALESCE — use placeholder markers
        // that won't match count(*) patterns
    }

    // Only replace __arraycount_placeholder__ entries with arrayCount subqueries
    let mut ac_idx = 0;
    for item in plan.select.items.iter_mut() {
        replace_arraycount_placeholders_in_expr(&mut item.expression, &pc_subqueries, &mut ac_idx);
    }
}

/// Replace __arraycount_placeholder__ markers with actual arrayCount subquery SQL.
fn replace_arraycount_placeholders_in_expr(
    expr: &mut RenderExpr,
    pc_subqueries: &[String],
    ac_idx: &mut usize,
) {
    match expr {
        RenderExpr::Raw(s) if s == "__arraycount_placeholder__" => {
            if *ac_idx < pc_subqueries.len() {
                *expr = RenderExpr::Raw(pc_subqueries[*ac_idx].clone());
                *ac_idx += 1;
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                replace_arraycount_placeholders_in_expr(operand, pc_subqueries, ac_idx);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in f.args.iter_mut() {
                replace_arraycount_placeholders_in_expr(arg, pc_subqueries, ac_idx);
            }
        }
        _ => {}
    }
}

/// Rewrite table aliases in a LogicalExpr using an alias mapping.
/// Used for post-WITH WHERE clauses: `person.user_id` → `u.user_id`
/// when the WHERE uses a renamed alias but the input uses the original.
pub(crate) fn rewrite_logical_expr_aliases(
    expr: &LogicalExpr,
    alias_map: &std::collections::HashMap<String, String>,
) -> LogicalExpr {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            if let Some(source_alias) = alias_map.get(&pa.table_alias.0) {
                LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                    table_alias: crate::query_planner::logical_expr::TableAlias(
                        source_alias.clone(),
                    ),
                    column: pa.column.clone(),
                })
            } else {
                expr.clone()
            }
        }
        LogicalExpr::TableAlias(ta) => {
            if let Some(source_alias) = alias_map.get(&ta.0) {
                LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(
                    source_alias.clone(),
                ))
            } else {
                expr.clone()
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => LogicalExpr::OperatorApplicationExp(
            crate::query_planner::logical_expr::OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| rewrite_logical_expr_aliases(o, alias_map))
                    .collect(),
            },
        ),
        LogicalExpr::ScalarFnCall(func) => {
            LogicalExpr::ScalarFnCall(crate::query_planner::logical_expr::ScalarFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| rewrite_logical_expr_aliases(a, alias_map))
                    .collect(),
            })
        }
        LogicalExpr::AggregateFnCall(func) => {
            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| rewrite_logical_expr_aliases(a, alias_map))
                    .collect(),
            })
        }
        _ => expr.clone(),
    }
}
