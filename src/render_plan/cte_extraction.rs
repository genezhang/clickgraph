use crate::clickhouse_query_generator::variable_length_cte::{
    VariableLengthCteGenerator,
};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::LogicalPlan;

use super::cte_generation::map_property_to_column_with_schema;
use super::errors::RenderBuildError;
use super::filter_pipeline::categorize_filters;
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{
    Literal, Operator, PropertyAccess,
    RenderExpr,
};
use super::{
    Cte, Join,
    JoinType,
};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(|o| contains_string_literal(o))
        }
        _ => false,
    }
}

/// Check if any operand is a string literal (for string concatenation detection)
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(|op| contains_string_literal(op))
}

/// Flatten nested + operations into a list of operands for concat()
fn flatten_addition_operands(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().flat_map(|o| flatten_addition_operands(o, alias_mapping)).collect()
        }
        _ => vec![render_expr_to_sql_string(expr, alias_mapping)],
    }
}

/// Helper function to extract the node alias from a GraphNode
fn extract_node_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::Filter(filter) => extract_node_alias(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_alias(&proj.input),
        _ => None,
    }
}

/// Extract schema filter from a node's ViewScan (for CTE generation)
/// Returns the raw filter SQL with table alias replaced to match CTE convention
fn extract_schema_filter_from_node(plan: &LogicalPlan, cte_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            extract_schema_filter_from_node(&node.input, cte_alias)
        }
        LogicalPlan::ViewScan(view_scan) => {
            if let Some(ref schema_filter) = view_scan.schema_filter {
                // Convert schema filter to SQL with the CTE alias
                schema_filter.to_sql(cte_alias).ok()
            } else {
                None
            }
        }
        LogicalPlan::Filter(filter) => {
            extract_schema_filter_from_node(&filter.input, cte_alias)
        }
        _ => None,
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan(scan) => scan.table_name.clone(),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_table_name(&node.input),
        LogicalPlan::GraphRel(rel) => extract_table_name(&rel.center),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        _ => None,
    }
}

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
fn render_expr_to_sql_string(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> String {
    match expr {
        RenderExpr::Column(col) => col.0.raw().to_string(),
        RenderExpr::TableAlias(alias) => alias.0.clone(),
        RenderExpr::ColumnAlias(alias) => alias.0.clone(),
        RenderExpr::Literal(lit) => match lit {
            super::render_expr::Literal::String(s) => format!("'{}'", s.replace("'", "''")),
            super::render_expr::Literal::Integer(i) => i.to_string(),
            super::render_expr::Literal::Float(f) => f.to_string(),
            super::render_expr::Literal::Boolean(b) => b.to_string(),
            super::render_expr::Literal::Null => "NULL".to_string(),
        },
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            let table_alias = alias_mapping
                .iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
            format!("{}.{}", table_alias, prop.column.0.raw())
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let operands: Vec<String> = op
                .operands
                .iter()
                .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                .collect();
            match op.operator {
                Operator::Equal => format!("{} = {}", operands[0], operands[1]),
                Operator::NotEqual => format!("{} != {}", operands[0], operands[1]),
                Operator::LessThan => format!("{} < {}", operands[0], operands[1]),
                Operator::GreaterThan => format!("{} > {}", operands[0], operands[1]),
                Operator::LessThanEqual => format!("{} <= {}", operands[0], operands[1]),
                Operator::GreaterThanEqual => format!("{} >= {}", operands[0], operands[1]),
                Operator::And => format!("({})", operands.join(" AND ")),
                Operator::Or => format!("({})", operands.join(" OR ")),
                Operator::Not => format!("NOT ({})", operands[0]),
                Operator::Addition => {
                    // Use concat() for string concatenation
                    // Flatten nested + operations for cases like: a + ' - ' + b
                    if has_string_operand(&op.operands) {
                        let flattened: Vec<String> = op.operands.iter()
                            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
                            .collect();
                        format!("concat({})", flattened.join(", "))
                    } else {
                        format!("{} + {}", operands[0], operands[1])
                    }
                }
                Operator::Subtraction => format!("{} - {}", operands[0], operands[1]),
                Operator::Multiplication => format!("{} * {}", operands[0], operands[1]),
                Operator::Division => format!("{} / {}", operands[0], operands[1]),
                Operator::ModuloDivision => format!("{} % {}", operands[0], operands[1]),
                Operator::Exponentiation => format!("POWER({}, {})", operands[0], operands[1]),
                Operator::In => {
                    // Check if right operand is a property access (array column)
                    // Cypher: x IN array_property → ClickHouse: has(array, x)
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} IN {}", operands[0], operands[1])
                    }
                }
                Operator::NotIn => {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("NOT has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} NOT IN {}", operands[0], operands[1])
                    }
                }
                Operator::StartsWith => format!("startsWith({}, {})", operands[0], operands[1]),
                Operator::EndsWith => format!("endsWith({}, {})", operands[0], operands[1]),
                Operator::Contains => format!("(position({}, {}) > 0)", operands[0], operands[1]),
                Operator::IsNull => format!("{} IS NULL", operands[0]),
                Operator::IsNotNull => format!("{} IS NOT NULL", operands[0]),
                Operator::Distinct => format!("{} IS DISTINCT FROM {}", operands[0], operands[1]),
                Operator::RegexMatch => format!("match({}, {})", operands[0], operands[1]),
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", func.name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        RenderExpr::List(list) => {
            let items: Vec<String> = list
                .iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            format!("({})", items.join(", "))
        }
        RenderExpr::InSubquery(subq) => {
            format!(
                "{} IN ({})",
                render_expr_to_sql_string(&subq.expr, alias_mapping),
                "/* subquery */"
            )
        }
        RenderExpr::Case(case) => {
            let when_clauses: Vec<String> = case
                .when_then
                .iter()
                .map(|(condition, result)| {
                    format!(
                        "WHEN {} THEN {}",
                        render_expr_to_sql_string(condition, alias_mapping),
                        render_expr_to_sql_string(result, alias_mapping)
                    )
                })
                .collect();
            let else_clause = case
                .else_expr
                .as_ref()
                .map(|expr| format!(" ELSE {}", render_expr_to_sql_string(expr, alias_mapping)))
                .unwrap_or_default();
            format!(
                "CASE {} {} END",
                case.expr
                    .as_ref()
                    .map(|e| render_expr_to_sql_string(e, alias_mapping))
                    .unwrap_or_default(),
                when_clauses.join(" ") + &else_clause
            )
        }
        RenderExpr::ExistsSubquery(exists) => {
            // Use the pre-generated SQL from ExistsSubquery
            format!("EXISTS ({})", exists.sql)
        }
        RenderExpr::Star => "*".to_string(),
        RenderExpr::Parameter(param) => format!("${}", param),
    }
}

/// Relationship column information
#[derive(Debug, Clone)]
pub struct RelationshipColumns {
    pub from_id: String,
    pub to_id: String,
}

/// Convert a label to its corresponding table name using provided schema
pub fn label_to_table_name_with_schema(
    label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    if let Ok(node_schema) = schema.get_node_schema(label) {
        // Use fully qualified table name: database.table_name
        return format!("{}.{}", node_schema.database, node_schema.table_name);
    }

    // Fallback to label as table name (not ideal but better than wrong hardcoded values)
    label.to_lowercase()
}

/// Convert a label to its corresponding table name
/// DEPRECATED: Use label_to_table_name_with_schema instead
pub fn label_to_table_name(label: &str) -> String {
    // Get the table name from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return label_to_table_name_with_schema(label, schema);
            }
        }
    }

    // Fallback to label as table name (not ideal but better than wrong hardcoded values)
    label.to_lowercase()
}

/// Convert a relationship type to its corresponding table name using provided schema
pub fn rel_type_to_table_name_with_schema(
    rel_type: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
        // Use fully qualified table name: database.table_name
        return format!("{}.{}", rel_schema.database, rel_schema.table_name);
    }

    // Fallback to rel_type as table name
    rel_type.to_lowercase()
}

/// Convert a relationship type to its corresponding table name
/// DEPRECATED: Use rel_type_to_table_name_with_schema instead
pub fn rel_type_to_table_name(rel_type: &str) -> String {
    // Get the table name from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return rel_type_to_table_name_with_schema(rel_type, schema);
            }
        }
    }

    // Fallback to relationship type as table name (not ideal but better than wrong hardcoded values)
    rel_type.to_string()
}

/// Convert multiple relationship types to table names
pub fn rel_types_to_table_names(rel_types: &[String]) -> Vec<String> {
    rel_types
        .iter()
        .map(|rt| rel_type_to_table_name(rt))
        .collect()
}

/// Extract relationship columns from a table name using provided schema
pub fn extract_relationship_columns_from_table_with_schema(
    table_name: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RelationshipColumns {
    // Extract just the table name without database prefix for matching
    let table_only = table_name.split('.').last().unwrap_or(table_name);

    // Find relationship schema by table name
    for rel_schema in schema.get_relationships_schemas().values() {
        // Match both with full name (db.table) or just table name
        if rel_schema.table_name == table_name
            || rel_schema.table_name == table_only
            || table_name.ends_with(&format!(".{}", rel_schema.table_name))
        {
            return RelationshipColumns {
                from_id: rel_schema.from_id.clone(),
                to_id: rel_schema.to_id.clone(),
            };
        }
    }

    // Fallback to hardcoded defaults
    RelationshipColumns {
        from_id: "from_id".to_string(),
        to_id: "to_id".to_string(),
    }
}

/// Extract relationship columns from a table name
/// DEPRECATED: Use extract_relationship_columns_from_table_with_schema instead
pub fn extract_relationship_columns_from_table(table_name: &str) -> RelationshipColumns {
    // Get columns from schema - this should be the single source of truth
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return extract_relationship_columns_from_table_with_schema(table_name, schema);
            }
        }
    }

    // No schema available or table not found - use generic defaults
    // This ensures the system works in schema-less mode but doesn't bypass user configuration
    RelationshipColumns {
        from_id: "from_id".to_string(),
        to_id: "to_id".to_string(),
    }
}

/// Extract relationship columns from a LogicalPlan
pub fn extract_relationship_columns(plan: &LogicalPlan) -> Option<RelationshipColumns> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .table_name
            .as_ref()
            .map(|table| extract_relationship_columns_from_table(table)),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if ViewScan already has relationship columns configured
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                Some(RelationshipColumns {
                    from_id: from_col.clone(),
                    to_id: to_col.clone(),
                })
            } else {
                // Fallback to table-based lookup
                Some(extract_relationship_columns_from_table(
                    &view_scan.source_table,
                ))
            }
        }
        LogicalPlan::Cte(cte) => extract_relationship_columns(&cte.input),
        LogicalPlan::GraphRel(rel) => extract_relationship_columns(&rel.center),
        LogicalPlan::Filter(filter) => extract_relationship_columns(&filter.input),
        LogicalPlan::Projection(proj) => extract_relationship_columns(&proj.input),
        _ => None,
    }
}

/// Extract ID column from a LogicalPlan
fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .table_name
            .as_ref()
            .map(|table| table_to_id_column(table)),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        _ => None,
    }
}

/// Get ID column for a table using provided schema
pub fn table_to_id_column_with_schema(
    table: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    // Find node schema by table name
    // Handle both fully qualified (database.table) and simple (table) names
    for node_schema in schema.get_nodes_schemas().values() {
        let fully_qualified = format!("{}.{}", node_schema.database, node_schema.table_name);
        if node_schema.table_name == table || fully_qualified == table {
            return node_schema.node_id.column.clone();
        }
    }

    // Fallback to "id" if not found
    "id".to_string()
}

/// Get ID column for a table
/// DEPRECATED: Use table_to_id_column_with_schema instead
pub fn table_to_id_column(table: &str) -> String {
    // Get the ID column from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return table_to_id_column_with_schema(table, schema);
            }
        }
    }

    // Fallback to "id" if schema not available or table not found
    "id".to_string()
}

/// Get ID column for a label
fn table_to_id_column_for_label(label: &str) -> String {
    table_to_id_column(&label_to_table_name(label))
}

/// Get relationship columns from schema
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    let table = rel_type_to_table_name(rel_type);
    let cols = extract_relationship_columns_from_table(&table);
    Some((cols.from_id, cols.to_id))
}

/// Get relationship columns by table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    let cols = extract_relationship_columns_from_table(table_name);
    Some((cols.from_id, cols.to_id))
}

/// Get node info from schema
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    let table = label_to_table_name(node_label);
    let id_col = table_to_id_column(&table);
    Some((table, id_col))
}

/// Apply property mapping to an expression
fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Get the node label for this table alias
            if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                // Map the property to the correct column
                let mapped_column = map_property_to_column_with_schema(&prop.column.0.raw(), &node_label)
                    .unwrap_or_else(|_| prop.column.0.raw().to_string());
                prop.column = super::render_expr::Column(PropertyValue::Column(mapped_column));
            }
        }
        RenderExpr::Column(col) => {
            // Check if this column name is actually an alias
            if let Some(node_label) = get_node_label_for_alias(&col.0.raw(), plan) {
                // Convert Column(alias) to PropertyAccess(alias, "id")
                let id_column = table_to_id_column(&label_to_table_name(&node_label));
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: super::render_expr::TableAlias(col.0.raw().to_string()),
                    column: super::render_expr::Column(PropertyValue::Column(id_column)),
                });
            }
        }
        RenderExpr::TableAlias(alias) => {
            // For denormalized nodes, convert TableAlias to PropertyAccess with the ID column
            // This is especially important for GROUP BY expressions
            if let Some((rel_alias, id_column)) = get_denormalized_node_id_reference(&alias.0, plan) {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: super::render_expr::TableAlias(rel_alias),
                    column: super::render_expr::Column(PropertyValue::Column(id_column)),
                });
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                apply_property_mapping_to_expr(operand, plan);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        RenderExpr::List(list) => {
            for item in list {
                apply_property_mapping_to_expr(item, plan);
            }
        }
        RenderExpr::InSubquery(subq) => {
            apply_property_mapping_to_expr(&mut subq.expr, plan);
        }
        // Other expression types don't contain nested expressions
        _ => {}
    }
}

/// Get the node label for a given Cypher alias by searching the plan
fn get_node_label_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::GraphNode(node) => get_node_label_for_alias(alias, &node.input),
        LogicalPlan::GraphRel(rel) => get_node_label_for_alias(alias, &rel.left)
            .or_else(|| get_node_label_for_alias(alias, &rel.center))
            .or_else(|| get_node_label_for_alias(alias, &rel.right)),
        LogicalPlan::Filter(filter) => get_node_label_for_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_node_label_for_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_node_label_for_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => get_node_label_for_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => get_node_label_for_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_node_label_for_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => get_node_label_for_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => get_node_label_for_alias(alias, &cte.input),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(label) = get_node_label_for_alias(alias, input) {
                    return Some(label);
                }
            }
            None
        }
        _ => None,
    }
}

/// For denormalized schemas: get the relationship alias and ID column for a node alias
/// Returns (rel_alias, id_column) if the node is denormalized, None otherwise
fn get_denormalized_node_id_reference(alias: &str, plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this node alias matches left or right connection
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // Check if node is the "from" node (left_connection)
                if alias == rel.left_connection {
                    if let Some(from_id) = &scan.from_id {
                        return Some((rel.alias.clone(), from_id.clone()));
                    }
                }
                // Check if node is the "to" node (right_connection)
                if alias == rel.right_connection {
                    if let Some(to_id) = &scan.to_id {
                        return Some((rel.alias.clone(), to_id.clone()));
                    }
                }
            }
            
            // Recursively check left and right branches
            // Check right branch first (more recent relationships take precedence for multi-hop)
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.right) {
                return Some(result);
            }
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.left) {
                return Some(result);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Check if this is a denormalized node
            if node.is_denormalized && node.alias == alias {
                // For standalone denormalized nodes, check their input ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if let Some(from_id) = &scan.from_id {
                        // Use a placeholder alias since standalone nodes don't have a rel alias
                        return Some((alias.to_string(), from_id.clone()));
                    }
                }
            }
            get_denormalized_node_id_reference(alias, &node.input)
        }
        LogicalPlan::Filter(filter) => get_denormalized_node_id_reference(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_denormalized_node_id_reference(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_denormalized_node_id_reference(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => get_denormalized_node_id_reference(alias, &order_by.input),
        LogicalPlan::Skip(skip) => get_denormalized_node_id_reference(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_denormalized_node_id_reference(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => get_denormalized_node_id_reference(alias, &group_by.input),
        LogicalPlan::Cte(cte) => get_denormalized_node_id_reference(alias, &cte.input),
        LogicalPlan::CartesianProduct(cp) => {
            get_denormalized_node_id_reference(alias, &cp.left)
                .or_else(|| get_denormalized_node_id_reference(alias, &cp.right))
        }
        _ => None,
    }
}

/// Extract CTEs with context - the main CTE extraction function
pub fn extract_ctes_with_context(
    plan: &LogicalPlan,
    last_node_alias: &str,
    context: &mut super::cte_generation::CteGenerationContext,
) -> RenderPlanBuilderResult<Vec<Cte>> {
    match plan {
        LogicalPlan::Empty => Ok(vec![]),
        LogicalPlan::Scan(_) => Ok(vec![]),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a relationship ViewScan (has from_id/to_id)
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                // This is a relationship ViewScan - create a CTE that selects the relationship columns
                let cte_name = format!(
                    "rel_{}",
                    view_scan.source_table.replace([' ', '-', '_'], "")
                );
                let sql = format!(
                    "SELECT {}, {} FROM {}",
                    from_col, to_col, view_scan.source_table
                );
                let formatted_sql = format!("{} AS (\n{}\n)", cte_name, sql);

                Ok(vec![Cte {
                    cte_name,
                    content: super::CteContent::RawSql(formatted_sql),
                    is_recursive: false,
                }])
            } else {
                // This is a node ViewScan - no CTE needed
                Ok(vec![])
            }
        }
        LogicalPlan::GraphNode(graph_node) => {
            // Skip CTE creation for denormalized nodes - their properties are on the relationship table
            if graph_node.is_denormalized {
                log::debug!(
                    "Skipping CTE for denormalized node '{}' (properties stored on relationship table)",
                    graph_node.alias
                );
                return Ok(vec![]);
            }
            extract_ctes_with_context(&graph_node.input, last_node_alias, context)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Handle variable-length paths with context
            if let Some(spec) = &graph_rel.variable_length {
                // Extract actual table names and column information
                let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                    .unwrap_or_else(|| "User".to_string());
                let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                    .unwrap_or_else(|| "User".to_string());
                // 🎯 FIX: Extract table name from ViewScan (authoritative) before label lookup
                // ViewScan contains the correct fully-qualified table name from schema resolution
                let start_table = extract_table_name(&graph_rel.left)
                    .unwrap_or_else(|| label_to_table_name(&start_label));
                let end_table = extract_table_name(&graph_rel.right)
                    .unwrap_or_else(|| label_to_table_name(&end_label));
                
                // Get rel_table from ViewScan's source_table (authoritative) or fall back to label lookup
                let rel_table = match graph_rel.center.as_ref() {
                    LogicalPlan::ViewScan(vs) => {
                        // ViewScan has the authoritative table name
                        vs.source_table.clone()
                    }
                    _ => {
                        // Fallback to label-based lookup
                        if let Some(labels) = &graph_rel.labels {
                            if let Some(first_label) = labels.first() {
                                rel_type_to_table_name(first_label)
                            } else {
                                rel_type_to_table_name(&graph_rel.alias)
                            }
                        } else {
                            rel_type_to_table_name(&graph_rel.alias)
                        }
                    }
                };

                // Extract ID columns
                let start_id_col = extract_id_column(&graph_rel.left)
                    .unwrap_or_else(|| table_to_id_column(&start_table));
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));

                // Extract relationship columns
                let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                    RelationshipColumns {
                        from_id: "from_node_id".to_string(),
                        to_id: "to_node_id".to_string(),
                    },
                );
                let from_col = rel_cols.from_id;
                let to_col = rel_cols.to_id;

                // Define aliases for traversal
                // Note: GraphRel.left_connection and right_connection are ALREADY swapped based on direction
                // in match_clause.rs (lines 1088-1092), so we always use them directly:
                // - left_connection = traversal start node alias
                // - right_connection = traversal end node alias
                let start_alias = graph_rel.left_connection.clone();
                let end_alias = graph_rel.right_connection.clone();

                // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                let (start_filters_sql, end_filters_sql, categorized_filters_opt) =
                    if let Some(where_predicate) = &graph_rel.where_predicate {
                        // Convert LogicalExpr to RenderExpr
                        let mut render_expr = RenderExpr::try_from(where_predicate.clone())
                            .map_err(|e| {
                                RenderBuildError::UnsupportedFeature(format!(
                                    "Failed to convert LogicalExpr to RenderExpr: {}",
                                    e
                                ))
                            })?;

                        // Apply property mapping to the filter expression before categorization
                        apply_property_mapping_to_expr(
                            &mut render_expr,
                            &LogicalPlan::GraphRel(graph_rel.clone()),
                        );

                        // Categorize filters
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            "", // rel_alias not used yet
                        );

                        // Create alias mapping
                        let alias_mapping = [
                            (start_alias.clone(), "start_node".to_string()),
                            (end_alias.clone(), "end_node".to_string()),
                        ];

                        let start_sql = categorized
                            .start_node_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &alias_mapping));
                        let end_sql = categorized
                            .end_node_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &alias_mapping));

                        // For variable-length queries (not shortest path), store end filters in context for outer query
                        if graph_rel.shortest_path_mode.is_none() {
                            if let Some(end_filter_expr) = &categorized.end_node_filters {
                                // 🆕 IMMUTABLE PATTERN: Update context immutably
                                *context = context
                                    .clone()
                                    .with_end_filters_for_outer_query(end_filter_expr.clone());
                            }
                        }

                        (start_sql, end_sql, Some(categorized))
                    } else {
                        (None, None, None)
                    };

                // Extract properties from filter expressions for shortest path queries
                // Even in SQL_ONLY mode, we need properties that appear in filters
                let filter_properties = if graph_rel.shortest_path_mode.is_some() {
                    use crate::render_plan::cte_generation::extract_properties_from_filter;

                    let mut props = Vec::new();

                    if let Some(categorized) = categorized_filters_opt {
                        // Extract from start filters
                        if let Some(ref filter_expr) = categorized.start_node_filters {
                            let start_props = extract_properties_from_filter(
                                filter_expr,
                                &start_alias,
                                &start_label,
                            );
                            props.extend(start_props);
                        }

                        // Extract from end filters
                        if let Some(ref filter_expr) = categorized.end_node_filters {
                            let end_props =
                                extract_properties_from_filter(filter_expr, &end_alias, &end_label);
                            props.extend(end_props);
                        }
                    }

                    props
                } else {
                    vec![]
                };

                // Generate CTE with filters
                // For shortest path queries, always use recursive CTE (even for exact hops)
                // because we need proper filtering and shortest path selection logic
                
                // 🎯 DECISION POINT: CTE or inline JOINs?
                let use_chained_join =
                    spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

                if use_chained_join {
                    // 🚀 OPTIMIZATION: Fixed-length, non-shortest-path → NO CTE!
                    // Return empty CTE list - will be handled as inline JOINs
                    let exact_hops = spec.exact_hop_count().unwrap();
                    println!(
                        "CTE BRANCH: Fixed-length pattern (*{}) detected - skipping CTE, using inline JOINs",
                        exact_hops
                    );
                    
                    // Extract CTEs from child plans (if any)
                    let child_ctes =
                        extract_ctes_with_context(&graph_rel.right, last_node_alias, context)?;
                    
                    return Ok(child_ctes);
                } else {
                    // ✅ Truly variable-length or shortest path → RECURSIVE CTE!
                    println!(
                        "CTE BRANCH: Variable-length pattern detected - using recursive CTE"
                    );
                    
                    // Check if nodes are denormalized (properties embedded in edge table)
                    let start_is_denormalized = match graph_rel.left.as_ref() {
                        LogicalPlan::GraphNode(node) => node.is_denormalized,
                        _ => false,
                    };
                    let end_is_denormalized = match graph_rel.right.as_ref() {
                        LogicalPlan::GraphNode(node) => node.is_denormalized,
                        _ => false,
                    };
                    let both_denormalized = start_is_denormalized && end_is_denormalized;
                    let is_mixed = start_is_denormalized != end_is_denormalized;
                    
                    // 🎯 Extract schema filters from start and end nodes
                    // Schema filters are defined in YAML and should be applied to CTE base/recursive cases
                    let start_schema_filter = extract_schema_filter_from_node(&graph_rel.left, "start_node");
                    let end_schema_filter = extract_schema_filter_from_node(&graph_rel.right, "end_node");
                    
                    // Combine user filters with schema filters using AND
                    let combined_start_filters = match (&start_filters_sql, &start_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };
                    
                    let combined_end_filters = match (&end_filters_sql, &end_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };
                    
                    if start_schema_filter.is_some() || end_schema_filter.is_some() {
                        log::info!("CTE: Applying schema filters - start: {:?}, end: {:?}", 
                                  start_schema_filter, end_schema_filter);
                    }
                    
                    // Get edge_id from relationship schema if available
                    // Use the first relationship label to look up the schema
                    let (edge_id, type_column, from_label_column, to_label_column, is_fk_edge) = if let Some(schema) = context.schema() {
                        if let Some(labels) = &graph_rel.labels {
                            if let Some(first_label) = labels.first() {
                                // Try to get relationship schema by label (not table name)
                                if let Ok(rel_schema) = schema.get_rel_schema(first_label) {
                                    (
                                        rel_schema.edge_id.clone(),
                                        rel_schema.type_column.clone(),
                                        rel_schema.from_label_column.clone(),
                                        rel_schema.to_label_column.clone(),
                                        rel_schema.is_fk_edge,
                                    )
                                } else {
                                    (None, None, None, None, false)
                                }
                            } else {
                                (None, None, None, None, false)
                            }
                        } else {
                            (None, None, None, None, false)
                        }
                    } else {
                        (None, None, None, None, false)
                    };
                    
                    if is_fk_edge {
                        log::debug!("CTE: Detected FK-edge pattern for relationship type");
                    }
                    
                    // Choose generator based on denormalized status
                    let mut generator = if both_denormalized {
                        log::debug!("CTE: Using denormalized generator for variable-length path (both nodes virtual)");
                        VariableLengthCteGenerator::new_denormalized(
                            spec.clone(),
                            &rel_table,   // The only table - edge table
                            &from_col,    // From column
                            &to_col,      // To column
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters.clone(),
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters.clone(),
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id,
                        )
                    } else if is_mixed {
                        log::debug!("CTE: Using mixed generator for variable-length path (start_denorm={}, end_denorm={})", 
                                  start_is_denormalized, end_is_denormalized);
                        VariableLengthCteGenerator::new_mixed(
                            spec.clone(),
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            &end_table,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            filter_properties.clone(),
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters.clone(),
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters.clone(),
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id,
                            start_is_denormalized,
                            end_is_denormalized,
                        )
                    } else {
                        VariableLengthCteGenerator::new_with_fk_edge(
                            spec.clone(),
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            &end_table,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            filter_properties, // Use filter properties
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters, // Start filters (user + schema)
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters,
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id, // Pass edge_id from schema
                            type_column.clone(), // Polymorphic edge type discriminator
                            from_label_column, // Polymorphic edge from label column
                            to_label_column.clone(), // Polymorphic edge to label column
                            Some(start_label.clone()), // Expected from node label
                            Some(end_label.clone()), // Expected to node label
                            is_fk_edge, // FK-edge pattern flag
                        )
                    };
                    
                    // For heterogeneous polymorphic paths (start_label != end_label with to_label_column),
                    // set intermediate node info to enable proper recursive traversal.
                    // The intermediate type is the same as start type (Group→Group recursion).
                    if to_label_column.is_some() && start_label != end_label {
                        log::info!("CTE: Setting intermediate node for heterogeneous polymorphic path");
                        log::info!("  - start_label: {}, end_label: {}", start_label, end_label);
                        log::info!("  - intermediate: table={}, id_col={}, label={}", 
                                  start_table, start_id_col, start_label);
                        generator.set_intermediate_node(&start_table, &start_id_col, &start_label);
                    }
                    
                    let var_len_cte = generator.generate_cte();
                    
                    // Also extract CTEs from child plans
                    let mut child_ctes =
                        extract_ctes_with_context(&graph_rel.right, last_node_alias, context)?;
                    child_ctes.push(var_len_cte);

                    return Ok(child_ctes);
                }
            }

            // Handle multiple relationship types for regular single-hop relationships
            let mut relationship_ctes = vec![];

            if let Some(labels) = &graph_rel.labels {
                eprintln!(
                    "DEBUG cte_extraction: GraphRel labels: {:?} (len={})",
                    labels,
                    labels.len()
                );
                
                // Deduplicate labels to handle cases like [:FOLLOWS|FOLLOWS]
                let unique_labels: Vec<String> = {
                    let mut seen = std::collections::HashSet::new();
                    labels.iter()
                        .filter(|l| seen.insert(l.clone()))
                        .cloned()
                        .collect()
                };
                
                if unique_labels.len() > 1 {
                    // Multiple distinct relationship types: create a UNION CTE
                    let rel_tables = rel_types_to_table_names(&unique_labels);
                    eprintln!(
                        "DEBUG cte_extraction: Resolved tables for labels {:?}: {:?}",
                        unique_labels, rel_tables
                    );

                    // Check if this is a polymorphic edge (all types map to same table with type_column)
                    let is_polymorphic = if let Some(schema) = context.schema() {
                        // Check if the first relationship type has a type_column (indicates polymorphic)
                        if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                            rel_schema.type_column.is_some()
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    let union_queries: Vec<String> = if is_polymorphic {
                        // Polymorphic edge: all types share the same table, need type filters
                        // Get schema info from context
                        if let Some(schema) = context.schema() {
                            if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                                let table_name = format!("{}.{}", rel_schema.database, rel_schema.table_name);
                                let from_col = &rel_schema.from_id;
                                let to_col = &rel_schema.to_id;
                                let type_col = rel_schema.type_column.as_ref().expect("polymorphic edge must have type_column");
                                
                                // For polymorphic edges, use a single query with IN clause
                                // This is more efficient than UNION of identical table scans
                                // Include type_column for relationship property access
                                let type_values: Vec<String> = unique_labels.iter().map(|l| format!("'{}'", l)).collect();
                                let type_in_clause = type_values.join(", ");
                                
                                vec![format!(
                                    "SELECT {from_col} as from_node_id, {to_col} as to_node_id, {type_col} as interaction_type FROM {table_name} WHERE {type_col} IN ({type_in_clause})"
                                )]
                            } else {
                                // Fallback if schema lookup fails
                                rel_tables.iter().map(|table| {
                                    let (from_col, to_col) = get_relationship_columns_by_table(table)
                                        .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string()));
                                    format!("SELECT {} as from_node_id, {} as to_node_id FROM {}", from_col, to_col, table)
                                }).collect()
                            }
                        } else {
                            // No schema in context, fallback
                            rel_tables.iter().map(|table| {
                                let (from_col, to_col) = get_relationship_columns_by_table(table)
                                    .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string()));
                                format!("SELECT {} as from_node_id, {} as to_node_id FROM {}", from_col, to_col, table)
                            }).collect()
                        }
                    } else {
                        // Regular multiple relationship types: UNION of different tables
                        rel_tables
                            .iter()
                            .map(|table| {
                                // Get the correct column names for this table
                                let (from_col, to_col) = get_relationship_columns_by_table(table)
                                    .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string())); // fallback
                                format!(
                                    "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                    from_col, to_col, table
                                )
                            })
                            .collect()
                    };

                    let union_sql = union_queries.join(" UNION ALL ");
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );

                    // Format as proper CTE: cte_name AS (union_sql)
                    let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);

                    eprintln!("DEBUG cte_extraction: Generated UNION CTE: {}", cte_name);

                    relationship_ctes.push(Cte {
                        cte_name: cte_name.clone(),
                        content: super::CteContent::RawSql(formatted_union_sql),
                        is_recursive: false,
                    });
                } else {
                    eprintln!("DEBUG cte_extraction: Single relationship type, no UNION needed");
                }
            } else {
                eprintln!("DEBUG cte_extraction: No labels on GraphRel!");
            }

            // IMPORTANT: Recurse into left and right branches to collect CTEs from nested GraphRels
            // This is needed for multi-hop polymorphic patterns like (u)-[r1]->(m)-[r2]->(t)
            // where both r1 and r2 are wildcard edges needing their own CTEs
            let mut left_ctes = extract_ctes_with_context(&graph_rel.left, last_node_alias, context)?;
            let mut right_ctes = extract_ctes_with_context(&graph_rel.right, last_node_alias, context)?;
            
            // Combine all CTEs: left branch + right branch + current relationship
            left_ctes.append(&mut right_ctes);
            left_ctes.append(&mut relationship_ctes);
            
            Ok(left_ctes)
        }
        LogicalPlan::Filter(filter) => {
            // Store the filter in context so GraphRel nodes can access it
            log::trace!(
                "Filter node detected, storing filter predicate in context: {:?}",
                filter.predicate
            );

            // 🆕 IMMUTABLE PATTERN: Create new context with filter instead of mutating
            let filter_expr: RenderExpr = filter.predicate.clone().try_into()?;
            log::trace!("Converted to RenderExpr: {:?}", filter_expr);
            let new_context = context.clone().with_filter(filter_expr);

            // Extract CTEs with the new context
            let ctes = extract_ctes_with_context(
                &filter.input,
                last_node_alias,
                &mut new_context.clone(),
            )?;

            // Merge end filters from the new context back to the original context
            *context = context.clone().merge_end_filters(&new_context);

            Ok(ctes)
        }
        LogicalPlan::Projection(projection) => {
            log::trace!(
                "Projection node detected, recursing into input type: {}",
                match &*projection.input {
                    LogicalPlan::Empty => "Empty",
                    LogicalPlan::Scan(_) => "Scan",
                    LogicalPlan::ViewScan(_) => "ViewScan",
                    LogicalPlan::GraphNode(_) => "GraphNode",
                    LogicalPlan::GraphRel(_) => "GraphRel",
                    LogicalPlan::Filter(_) => "Filter",
                    LogicalPlan::Projection(_) => "Projection",
                    LogicalPlan::GraphJoins(_) => "GraphJoins",
                    LogicalPlan::GroupBy(_) => "GroupBy",
                    LogicalPlan::OrderBy(_) => "OrderBy",
                    LogicalPlan::Skip(_) => "Skip",
                    LogicalPlan::Limit(_) => "Limit",
                    LogicalPlan::Cte(_) => "Cte",
                    LogicalPlan::Union(_) => "Union",
                    LogicalPlan::PageRank(_) => "PageRank",
                    LogicalPlan::Unwind(_) => "Unwind",
                    LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                }
            );
            extract_ctes_with_context(&projection.input, last_node_alias, context)
        }
        LogicalPlan::GraphJoins(graph_joins) => {
            extract_ctes_with_context(&graph_joins.input, last_node_alias, context)
        }
        LogicalPlan::GroupBy(group_by) => {
            extract_ctes_with_context(&group_by.input, last_node_alias, context)
        }
        LogicalPlan::OrderBy(order_by) => {
            extract_ctes_with_context(&order_by.input, last_node_alias, context)
        }
        LogicalPlan::Skip(skip) => extract_ctes_with_context(&skip.input, last_node_alias, context),
        LogicalPlan::Limit(limit) => {
            extract_ctes_with_context(&limit.input, last_node_alias, context)
        }
        LogicalPlan::Cte(logical_cte) => {
            // Use schema from context if available, otherwise create empty schema for tests
            let schema = context.schema().cloned().unwrap_or_else(|| {
                use crate::graph_catalog::graph_schema::GraphSchema;
                GraphSchema::build(
                    1,
                    "test".to_string(),
                    std::collections::HashMap::new(),
                    std::collections::HashMap::new(),
                )
            });
            Ok(vec![Cte {
                cte_name: logical_cte.name.clone(),
                content: super::CteContent::Structured(logical_cte.input.to_render_plan(&schema)?),
                is_recursive: false,
            }])
        }
        LogicalPlan::Union(union) => {
            let mut ctes = vec![];
            for input_plan in union.inputs.iter() {
                ctes.append(&mut extract_ctes_with_context(
                    input_plan,
                    last_node_alias,
                    context,
                )?);
            }
            Ok(ctes)
        }
        LogicalPlan::PageRank(_) => Ok(vec![]),
        LogicalPlan::Unwind(u) => extract_ctes_with_context(&u.input, last_node_alias, context),
        LogicalPlan::CartesianProduct(cp) => {
            let mut ctes = extract_ctes_with_context(&cp.left, last_node_alias, context)?;
            ctes.append(&mut extract_ctes_with_context(&cp.right, last_node_alias, context)?);
            Ok(ctes)
        }
    }
}

/// Check if a variable-length relationship is optional (for OPTIONAL MATCH semantics)
/// Returns true if the VLP should use LEFT JOIN instead of INNER JOIN
pub fn is_variable_length_optional(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            rel.is_optional.unwrap_or(false)
        }
        LogicalPlan::GraphNode(node) => is_variable_length_optional(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_optional(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_optional(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_optional(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_optional(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_optional(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_optional(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_optional(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_optional(&cte.input),
        _ => false,
    }
}

/// Check if the plan contains a variable-length relationship and return node aliases
/// Returns (left_alias, right_alias) if found
pub fn has_variable_length_rel(plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            Some((rel.left_connection.clone(), rel.right_connection.clone()))
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            has_variable_length_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => has_variable_length_rel(&node.input),
        LogicalPlan::Filter(filter) => has_variable_length_rel(&filter.input),
        LogicalPlan::Projection(proj) => has_variable_length_rel(&proj.input),
        LogicalPlan::GraphJoins(joins) => has_variable_length_rel(&joins.input),
        LogicalPlan::GroupBy(gb) => has_variable_length_rel(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_rel(&ob.input),
        LogicalPlan::Skip(skip) => has_variable_length_rel(&skip.input),
        LogicalPlan::Limit(limit) => has_variable_length_rel(&limit.input),
        LogicalPlan::Cte(cte) => has_variable_length_rel(&cte.input),
        _ => None,
    }
}

/// Check if a variable-length pattern uses denormalized edges
/// Returns true if EITHER node is virtual (embedded in edge table)
/// For checking if BOTH are denormalized, use get_variable_length_denorm_info
pub fn is_variable_length_denormalized(plan: &LogicalPlan) -> bool {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }
    
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Check if either left or right node is denormalized
            check_node_denormalized(&rel.left) || check_node_denormalized(&rel.right)
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        LogicalPlan::GraphRel(rel) => {
            is_variable_length_denormalized(&rel.left)
        }
        LogicalPlan::GraphNode(node) => is_variable_length_denormalized(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_denormalized(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_denormalized(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_denormalized(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_denormalized(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_denormalized(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_denormalized(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_denormalized(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_denormalized(&cte.input),
        _ => false,
    }
}

/// Detailed denormalization info for a variable-length pattern
#[derive(Debug, Clone)]
pub struct VariableLengthDenormInfo {
    pub start_is_denormalized: bool,
    pub end_is_denormalized: bool,
    // Node table information extracted from the plan (fully qualified)
    pub start_table: Option<String>,
    pub start_id_col: Option<String>,
    pub end_table: Option<String>,
    pub end_id_col: Option<String>,
}

impl VariableLengthDenormInfo {
    pub fn is_fully_denormalized(&self) -> bool {
        self.start_is_denormalized && self.end_is_denormalized
    }
    
    pub fn is_mixed(&self) -> bool {
        self.start_is_denormalized != self.end_is_denormalized
    }
    
    pub fn is_any_denormalized(&self) -> bool {
        self.start_is_denormalized || self.end_is_denormalized
    }
}

/// Get detailed denormalization info for a variable-length pattern
pub fn get_variable_length_denorm_info(plan: &LogicalPlan) -> Option<VariableLengthDenormInfo> {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }
    
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Extract table names and id columns from the nodes' ViewScans
            let start_table = extract_table_name(&rel.left);
            let end_table = extract_table_name(&rel.right);
            let start_id_col = extract_id_column(&rel.left);
            let end_id_col = extract_id_column(&rel.right);
            
            Some(VariableLengthDenormInfo {
                start_is_denormalized: check_node_denormalized(&rel.left),
                end_is_denormalized: check_node_denormalized(&rel.right),
                start_table,
                start_id_col,
                end_table,
                end_id_col,
            })
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            // Recurse into left branch to find nested VLP
            get_variable_length_denorm_info(&rel.left)
        }
        LogicalPlan::GraphNode(node) => get_variable_length_denorm_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_denorm_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_denorm_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_denorm_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_denorm_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_denorm_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_denorm_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_denorm_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_denorm_info(&cte.input),
        _ => None,
    }
}

/// Info about the relationship in a variable-length pattern
/// Used for SELECT rewriting to map f.Origin → t.start_id, f.Dest → t.end_id
#[derive(Debug, Clone)]
pub struct VariableLengthRelInfo {
    pub rel_alias: String,    // e.g., "f"
    pub from_col: String,     // e.g., "Origin"  
    pub to_col: String,       // e.g., "Dest"
}

/// Extract relationship info (alias, from_col, to_col) from a variable-length path
pub fn get_variable_length_rel_info(plan: &LogicalPlan) -> Option<VariableLengthRelInfo> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Get the from/to columns from the ViewScan in the center
            let cols = extract_relationship_columns(&rel.center)?;
            Some(VariableLengthRelInfo {
                rel_alias: rel.alias.clone(),
                from_col: cols.from_id,
                to_col: cols.to_id,
            })
        }
        LogicalPlan::GraphNode(node) => get_variable_length_rel_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_rel_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_rel_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_rel_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_rel_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_rel_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_rel_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_rel_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_rel_info(&cte.input),
        _ => None,
    }
}

/// Extract path variable from the plan (variable-length paths only, for CTE generation)
pub fn get_path_variable(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => rel.path_variable.clone(),
        LogicalPlan::GraphNode(node) => get_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_path_variable(&u.input),
        _ => None,
    }
}

/// Extract path variable from fixed multi-hop patterns (no variable_length)
/// Returns (path_variable_name, hop_count) if found
pub fn get_fixed_path_variable(plan: &LogicalPlan) -> Option<(String, u32)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Only handle fixed patterns (no variable_length)
            if rel.variable_length.is_some() {
                return None;
            }
            
            if let Some(ref path_var) = rel.path_variable {
                // Count hops by traversing the GraphRel chain
                let hop_count = count_hops_in_graph_rel(plan);
                return Some((path_var.clone(), hop_count));
            }
            
            // Check nested GraphRels
            if let LogicalPlan::GraphRel(_) = rel.left.as_ref() {
                return get_fixed_path_variable(&rel.left);
            }
            None
        }
        LogicalPlan::GraphNode(node) => get_fixed_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_fixed_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_fixed_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_fixed_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_fixed_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_fixed_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_fixed_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_fixed_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_fixed_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_fixed_path_variable(&u.input),
        _ => None,
    }
}

/// Count the number of hops (relationships) in a GraphRel chain
fn count_hops_in_graph_rel(plan: &LogicalPlan) -> u32 {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Count this relationship + any nested ones
            1 + count_hops_in_graph_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => count_hops_in_graph_rel(&node.input),
        _ => 0,
    }
}

/// Complete information about a fixed path pattern
/// For `p = (a)-[r1]->(b)-[r2]->(c)`:
/// - path_var_name: "p"
/// - node_aliases: ["a", "b", "c"]
/// - rel_aliases: ["r1", "r2"]
/// - hop_count: 2
/// - node_id_columns: mapping from node alias to (rel_alias, id_column)
///   e.g., {"a" -> ("r1", "Origin"), "b" -> ("r1", "Dest"), "c" -> ("r2", "Dest")}
#[derive(Debug, Clone)]
pub struct FixedPathInfo {
    pub path_var_name: String,
    pub node_aliases: Vec<String>,
    pub rel_aliases: Vec<String>,
    pub hop_count: u32,
    /// Maps node alias to (relationship_alias, id_column) for denormalized schemas
    /// e.g., "a" -> ("r", "Origin"), "b" -> ("r", "Dest")
    pub node_id_columns: std::collections::HashMap<String, (String, String)>,
}

/// Extract complete path information from fixed multi-hop patterns
/// Returns FixedPathInfo with all node and relationship aliases
pub fn get_fixed_path_info(plan: &LogicalPlan) -> Option<FixedPathInfo> {
    // First find the path variable and hop count
    let (path_var_name, hop_count) = get_fixed_path_variable(plan)?;
    
    // Then extract all aliases and node ID mappings
    let (node_aliases, rel_aliases, node_id_columns) = collect_path_aliases_with_ids(plan);
    
    Some(FixedPathInfo {
        path_var_name,
        node_aliases,
        rel_aliases,
        hop_count,
        node_id_columns,
    })
}

/// Collect node and relationship aliases plus ID column mappings
fn collect_path_aliases_with_ids(plan: &LogicalPlan) -> (Vec<String>, Vec<String>, std::collections::HashMap<String, (String, String)>) {
    let mut node_aliases = Vec::new();
    let mut rel_aliases = Vec::new();
    let mut node_id_columns = std::collections::HashMap::new();
    
    collect_path_aliases_with_ids_recursive(plan, &mut node_aliases, &mut rel_aliases, &mut node_id_columns);
    
    (node_aliases, rel_aliases, node_id_columns)
}

/// Recursive helper to collect aliases and ID column mappings
fn collect_path_aliases_with_ids_recursive(
    plan: &LogicalPlan,
    node_aliases: &mut Vec<String>,
    rel_aliases: &mut Vec<String>,
    node_id_columns: &mut std::collections::HashMap<String, (String, String)>,
) {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Process left side first (may be another GraphRel or the start node)
            collect_path_aliases_with_ids_recursive(&rel.left, node_aliases, rel_aliases, node_id_columns);
            
            // Get the from_id and to_id columns from the ViewScan
            if let LogicalPlan::ViewScan(view_scan) = rel.center.as_ref() {
                let from_id = view_scan.from_id.clone().unwrap_or_else(|| "id".to_string());
                let to_id = view_scan.to_id.clone().unwrap_or_else(|| "id".to_string());
                
                // Map left node to this relationship's from_id (if not already mapped)
                if !node_id_columns.contains_key(&rel.left_connection) {
                    node_id_columns.insert(
                        rel.left_connection.clone(),
                        (rel.alias.clone(), from_id.clone()),
                    );
                }
                
                // Map right node to this relationship's to_id
                node_id_columns.insert(
                    rel.right_connection.clone(),
                    (rel.alias.clone(), to_id),
                );
            }
            
            // Add this relationship
            rel_aliases.push(rel.alias.clone());
            
            // Add the right node
            if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                if !node_aliases.contains(&right_node.alias) {
                    node_aliases.push(right_node.alias.clone());
                }
            }
        }
        LogicalPlan::GraphNode(node) => {
            // Start node - add it if not already present
            if !node_aliases.contains(&node.alias) {
                node_aliases.push(node.alias.clone());
            }
            // Recurse into input
            collect_path_aliases_with_ids_recursive(&node.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::Filter(filter) => {
            collect_path_aliases_with_ids_recursive(&filter.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::Projection(proj) => {
            collect_path_aliases_with_ids_recursive(&proj.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::GraphJoins(joins) => {
            collect_path_aliases_with_ids_recursive(&joins.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::GroupBy(gb) => {
            collect_path_aliases_with_ids_recursive(&gb.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::OrderBy(ob) => {
            collect_path_aliases_with_ids_recursive(&ob.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::Skip(skip) => {
            collect_path_aliases_with_ids_recursive(&skip.input, node_aliases, rel_aliases, node_id_columns);
        }
        LogicalPlan::Limit(limit) => {
            collect_path_aliases_with_ids_recursive(&limit.input, node_aliases, rel_aliases, node_id_columns);
        }
        _ => {}
    }
}

// ============================================================================
// VLP (Variable-Length Path) Schema Types and Consolidated Info
// ============================================================================

/// Schema type classification for VLP query generation
/// 
/// Different schema types require different SQL generation strategies:
/// - Normal: Separate node and edge tables, standard JOIN patterns
/// - Polymorphic: Single edge table with type_column, nodes still separate
/// - Denormalized: Nodes embedded in edge table (no separate node tables)
/// - FkEdge: FK column on node table represents edge (no separate edge table)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VlpSchemaType {
    /// Standard schema: separate tables for nodes and edges
    /// Example: users table + follows table
    Normal,
    
    /// Polymorphic edge: single edge table with type_column to distinguish edge types
    /// Example: interactions table with interaction_type column
    /// Nodes still have separate tables
    Polymorphic,
    
    /// Denormalized: node properties embedded in edge table
    /// Example: flights table with Origin/Dest as node IDs and OriginCity/DestCity as properties
    /// No separate node tables exist
    Denormalized,
    
    /// FK-Edge: edge is represented by a FK column on the node table
    /// Example: fs_objects table with parent_id FK column
    /// Edge table == Node table (self-referencing)
    FkEdge,
}

/// Consolidated VLP context containing all information needed for SQL generation
/// 
/// This struct gathers all the scattered VLP-related info into one place,
/// making it easier to reason about and pass through the code.
#[derive(Debug, Clone)]
pub struct VlpContext {
    /// Schema type determines SQL generation strategy
    pub schema_type: VlpSchemaType,
    
    /// True if exact hop count (e.g., *2, *3), false if range/unbounded
    pub is_fixed_length: bool,
    
    /// Exact hop count if fixed-length, None otherwise
    pub exact_hops: Option<u32>,
    
    /// Min/max hops for range patterns
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
    
    /// Start node information
    pub start_alias: String,
    pub start_table: String,
    pub start_id_col: String,
    
    /// End node information  
    pub end_alias: String,
    pub end_table: String,
    pub end_id_col: String,
    
    /// Relationship information
    pub rel_alias: String,
    pub rel_table: String,
    pub rel_from_col: String,
    pub rel_to_col: String,
    
    /// For polymorphic edges: type column and value
    pub type_column: Option<String>,
    pub type_value: Option<String>,
    
    /// For denormalized edges: property mappings (logical_name -> ClickHouse column/expression)
    pub from_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,
    pub to_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,
    
    /// For FK-edge patterns: true if edge is represented by FK on node table
    pub is_fk_edge: bool,
}

impl VlpContext {
    /// Check if this VLP needs a recursive CTE (true for range/unbounded patterns)
    pub fn needs_cte(&self) -> bool {
        !self.is_fixed_length
    }
    
    /// Check if nodes have separate tables (not denormalized)
    pub fn has_separate_node_tables(&self) -> bool {
        self.schema_type != VlpSchemaType::Denormalized && self.schema_type != VlpSchemaType::FkEdge
    }
    
    /// Check if this is an FK-edge pattern
    pub fn is_fk_edge(&self) -> bool {
        self.schema_type == VlpSchemaType::FkEdge || self.is_fk_edge
    }
}

/// Detect VLP schema type from a GraphRel
pub fn detect_vlp_schema_type(graph_rel: &crate::query_planner::logical_plan::GraphRel) -> VlpSchemaType {
    // Check if nodes are denormalized
    let left_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.left);
    let right_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.right);
    
    if left_is_denorm && right_is_denorm {
        return VlpSchemaType::Denormalized;
    }
    
    // Check for FK-edge pattern: edge table == node table (self-referencing FK)
    // This is detected by checking if rel_table == start_table == end_table
    let rel_table = extract_table_name(&graph_rel.center);
    let start_table = extract_node_table(&graph_rel.left);
    let end_table = extract_node_table(&graph_rel.right);
    
    if let (Some(rt), Some(st), Some(et)) = (rel_table, start_table, end_table) {
        if rt == st && rt == et {
            return VlpSchemaType::FkEdge;
        }
    }
    
    // Check for polymorphic edge (has type_column)
    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        if scan.type_column.is_some() {
            return VlpSchemaType::Polymorphic;
        }
    }
    
    VlpSchemaType::Normal
}

/// Extract table name from a node plan
fn extract_node_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                Some(scan.source_table.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper to check if a GraphNode is denormalized
fn is_node_denormalized_from_graph_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => node.is_denormalized,
        _ => false,
    }
}

/// Build a complete VlpContext from a GraphRel
/// 
/// This gathers all VLP-related information into a single struct
pub fn build_vlp_context(graph_rel: &crate::query_planner::logical_plan::GraphRel) -> Option<VlpContext> {
    let spec = graph_rel.variable_length.as_ref()?;
    
    let schema_type = detect_vlp_schema_type(graph_rel);
    let is_fixed_length = spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();
    let exact_hops = spec.exact_hop_count();
    
    // Extract start node info
    let (start_alias, start_table, start_id_col) = extract_node_info(&graph_rel.left, schema_type, &graph_rel.center)?;
    
    // Extract end node info
    let (end_alias, end_table, end_id_col) = extract_node_info(&graph_rel.right, schema_type, &graph_rel.center)?;
    
    // Extract relationship info
    let rel_alias = graph_rel.alias.clone();
    let rel_table = extract_table_name(&graph_rel.center)?;
    let rel_cols = extract_relationship_columns(&graph_rel.center)?;
    
    // Extract polymorphic type info
    let (type_column, type_value) = if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        (scan.type_column.clone(), graph_rel.labels.as_ref().and_then(|l| l.first().cloned()))
    } else {
        (None, None)
    };
    
    // Extract denormalized property mappings
    let (from_node_properties, to_node_properties) = if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        (scan.from_node_properties.clone(), scan.to_node_properties.clone())
    } else {
        (None, None)
    };
    
    // Detect FK-edge pattern
    let is_fk_edge = schema_type == VlpSchemaType::FkEdge;
    
    Some(VlpContext {
        schema_type,
        is_fixed_length,
        exact_hops,
        min_hops: spec.min_hops,
        max_hops: spec.max_hops,
        start_alias,
        start_table,
        start_id_col,
        end_alias,
        end_table,
        end_id_col,
        rel_alias,
        rel_table,
        rel_from_col: rel_cols.from_id,
        rel_to_col: rel_cols.to_id,
        type_column,
        type_value,
        from_node_properties,
        to_node_properties,
        is_fk_edge,
    })
}

/// Extract node info (alias, table, id_col) handling different schema types
fn extract_node_info(
    node_plan: &LogicalPlan,
    schema_type: VlpSchemaType,
    rel_center: &LogicalPlan,
) -> Option<(String, String, String)> {
    match node_plan {
        LogicalPlan::GraphNode(node) => {
            let alias = node.alias.clone();
            
            match schema_type {
                VlpSchemaType::Denormalized => {
                    // For denormalized, table comes from relationship
                    let table = extract_table_name(rel_center)?;
                    // ID column is from relationship's from_id or to_id
                    let rel_cols = extract_relationship_columns(rel_center)?;
                    // Determine if this is start or end node by checking if it's the left or right
                    // For now, use from_id - caller should determine correct column
                    Some((alias, table, rel_cols.from_id))
                }
                _ => {
                    // Normal/Polymorphic: get from node's ViewScan
                    if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                        let table = scan.source_table.clone();
                        let id_col = scan.id_column.clone();
                        Some((alias, table, id_col))
                    } else if let Some(label) = &node.label {
                        // Fallback: derive from label
                        let table = label_to_table_name(label);
                        let id_col = table_to_id_column(&table);
                        Some((alias, table, id_col))
                    } else {
                        None
                    }
                }
            }
        }
        _ => None,
    }
}

/// Extract variable length spec from the plan
pub fn get_variable_length_spec(plan: &LogicalPlan) -> Option<crate::query_planner::logical_plan::VariableLengthSpec> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this GraphRel has variable_length
            if rel.variable_length.is_some() {
                return rel.variable_length.clone();
            }
            // Recursively check nested GraphRels (for chained patterns like (a)-[*]->(b)-[:R]->(c))
            get_variable_length_spec(&rel.left)
                .or_else(|| get_variable_length_spec(&rel.right))
        }
        LogicalPlan::GraphNode(node) => get_variable_length_spec(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_spec(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_spec(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_spec(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_spec(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_spec(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_spec(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_spec(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_spec(&cte.input),
        LogicalPlan::Unwind(u) => get_variable_length_spec(&u.input),
        _ => None,
    }
}

/// Extract shortest path mode from the plan
pub fn get_shortest_path_mode(plan: &LogicalPlan) -> Option<crate::query_planner::logical_plan::ShortestPathMode> {
    match plan {
        LogicalPlan::GraphRel(rel) => rel.shortest_path_mode.clone(),
        LogicalPlan::GraphNode(node) => get_shortest_path_mode(&node.input),
        LogicalPlan::Filter(filter) => get_shortest_path_mode(&filter.input),
        LogicalPlan::Projection(proj) => get_shortest_path_mode(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_shortest_path_mode(&joins.input),
        LogicalPlan::GroupBy(gb) => get_shortest_path_mode(&gb.input),
        LogicalPlan::OrderBy(ob) => get_shortest_path_mode(&ob.input),
        LogicalPlan::Skip(skip) => get_shortest_path_mode(&skip.input),
        LogicalPlan::Limit(limit) => get_shortest_path_mode(&limit.input),
        LogicalPlan::Cte(cte) => get_shortest_path_mode(&cte.input),
        LogicalPlan::Unwind(u) => get_shortest_path_mode(&u.input),
        _ => None,
    }
}

/// Extract node label from ViewScan in the plan
pub fn extract_node_label_from_viewscan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Try to get the label from the schema using the table name
            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                if let Ok(schemas) = schemas_lock.try_read() {
                    if let Some(schema) = schemas.get("default") {
                        if let Some((label, _)) =
                            get_node_schema_by_table(schema, &view_scan.source_table)
                        {
                            return Some(label.to_string());
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::Scan(scan) => {
            // For Scan nodes, try to get from table name
            scan.table_name.as_ref().and_then(|table| {
                if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                    if let Ok(schemas) = schemas_lock.try_read() {
                        if let Some(schema) = schemas.get("default") {
                            if let Some((label, _)) = get_node_schema_by_table(schema, table) {
                                return Some(label.to_string());
                            }
                        }
                    }
                }
                None
            })
        }
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        _ => None,
    }
}

/// Get node schema information by table name
pub fn get_node_schema_by_table<'a>(
    schema: &'a GraphSchema,
    table_name: &str,
) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.get_nodes_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}

/// Expand fixed-length path patterns into inline JOINs
/// 
/// This function generates JOIN clauses for exact hop-count patterns (*2, *3, etc.)
/// without using CTEs. It directly chains relationship and node JOINs.
///
/// # Arguments
/// * `exact_hops` - Number of hops (e.g., 2 for *2)
/// * `start_table` - Table name for start node
/// * `start_id_col` - ID column for start node
/// * `rel_table` - Table name for relationship
/// * `from_col` - From-node ID column in relationship table
/// * `to_col` - To-node ID column in relationship table  
/// * `end_table` - Table name for end node
/// * `end_id_col` - ID column for end node
/// * `start_alias` - Cypher alias for start node
/// * `end_alias` - Cypher alias for end node
///
/// # Returns
/// Vector of JOIN items to be added to the FROM clause
pub fn expand_fixed_length_joins(
    exact_hops: u32,
    start_table: &str,
    start_id_col: &str,
    rel_table: &str,
    from_col: &str,
    to_col: &str,
    end_table: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Vec<Join> {
    use super::render_expr::{Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias};
    
    let mut joins = Vec::new();
    
    println!(
        "expand_fixed_length_joins: Generating {} hops from {} to {}",
        exact_hops, start_alias, end_alias
    );
    
    for hop in 1..=exact_hops {
        let rel_alias = format!("r{}", hop);
        
        // Determine previous node/relationship alias
        let prev_alias = if hop == 1 {
            start_alias.to_string()
        } else {
            // Bridge directly through previous relationship's to_id
            format!("r{}", hop - 1)
        };
        
        let prev_id_col = if hop == 1 {
            start_id_col.to_string()
        } else {
            to_col.to_string() // Bridge through previous relationship's to_id
        };
        
        // Add relationship JOIN
        joins.push(Join {
            table_name: rel_table.to_string(),
            table_alias: rel_alias.clone(),
            joining_on: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(prev_alias),
                        column: Column(PropertyValue::Column(prev_id_col)),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(rel_alias.clone()),
                        column: Column(PropertyValue::Column(from_col.to_string())),
                    }),
                ],
            }],
            join_type: JoinType::Inner,
            pre_filter: None,
        });
        
        // TODO: Add intermediate node JOIN only if properties referenced
        // For now, always bridge directly through relationship IDs (optimization!)
    }
    
    // Always add final node JOIN (the endpoint)
    let last_rel = format!("r{}", exact_hops);
    joins.push(Join {
        table_name: end_table.to_string(),
        table_alias: end_alias.to_string(),
        joining_on: vec![OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(last_rel),
                    column: Column(PropertyValue::Column(to_col.to_string())),
                }),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(end_alias.to_string()),
                    column: Column(PropertyValue::Column(end_id_col.to_string())),
                }),
            ],
        }],
        join_type: JoinType::Inner,
        pre_filter: None,
    });
    
    println!(
        "expand_fixed_length_joins: Generated {} JOINs (no intermediate nodes)",
        joins.len()
    );
    
    joins
}

/// Schema-aware fixed-length VLP JOIN generation using VlpContext
/// 
/// This is the consolidated version that handles all schema types correctly:
/// - Normal: FROM start_node, JOINs through r1...rN, final JOIN to end_node
/// - Polymorphic: Same as Normal (nodes have separate tables)
/// - Denormalized: FROM r1 (first edge), JOINs through r2...rN only (no node JOINs)
///
/// # Returns
/// (from_table, from_alias, joins) - The FROM table info and JOIN clauses
pub fn expand_fixed_length_joins_with_context(ctx: &VlpContext) -> (String, String, Vec<Join>) {
    use super::render_expr::{Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias};
    
    let exact_hops = ctx.exact_hops.unwrap_or(1);
    let mut joins = Vec::new();
    
    println!(
        "expand_fixed_length_joins_with_context: schema_type={:?}, {} hops from {} to {}",
        ctx.schema_type, exact_hops, ctx.start_alias, ctx.end_alias
    );
    
    match ctx.schema_type {
        VlpSchemaType::Denormalized => {
            // DENORMALIZED: No separate node tables
            // FROM: edge_table AS r1 (the first hop becomes FROM)
            // JOINs: r2 ON r1.to_id = r2.from_id, ..., rN ON r(N-1).to_id = rN.from_id
            // No final node JOIN needed - end node properties come from rN.to_node_properties
            
            // First hop is the FROM table, not a JOIN
            let from_table = ctx.rel_table.clone();
            let from_alias = "r1".to_string();
            
            // Generate JOINs for hops 2..N
            for hop in 2..=exact_hops {
                let rel_alias = format!("r{}", hop);
                let prev_alias = format!("r{}", hop - 1);
                
                joins.push(Join {
                    table_name: ctx.rel_table.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: Column(PropertyValue::Column(ctx.rel_to_col.clone())),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: Column(PropertyValue::Column(ctx.rel_from_col.clone())),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                });
            }
            
            println!(
                "expand_fixed_length_joins_with_context [DENORMALIZED]: FROM {} AS {}, {} JOINs",
                from_table, from_alias, joins.len()
            );
            
            (from_table, from_alias, joins)
        }
        
        VlpSchemaType::Normal | VlpSchemaType::Polymorphic => {
            // NORMAL/POLYMORPHIC: Separate node tables exist
            // FROM: start_node_table AS start_alias
            // JOINs: r1 ON start.id = r1.from_id, r2 ON r1.to_id = r2.from_id, ..., end ON rN.to_id = end.id
            
            let from_table = ctx.start_table.clone();
            let from_alias = ctx.start_alias.clone();
            
            for hop in 1..=exact_hops {
                let rel_alias = format!("r{}", hop);
                
                let (prev_alias, prev_id_col) = if hop == 1 {
                    (ctx.start_alias.clone(), ctx.start_id_col.clone())
                } else {
                    (format!("r{}", hop - 1), ctx.rel_to_col.clone())
                };
                
                // Add relationship JOIN
                joins.push(Join {
                    table_name: ctx.rel_table.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: Column(PropertyValue::Column(prev_id_col)),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: Column(PropertyValue::Column(ctx.rel_from_col.clone())),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                });
            }
            
            // Add final node JOIN
            let last_rel = format!("r{}", exact_hops);
            joins.push(Join {
                table_name: ctx.end_table.clone(),
                table_alias: ctx.end_alias.clone(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(last_rel),
                            column: Column(PropertyValue::Column(ctx.rel_to_col.clone())),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(ctx.end_alias.clone()),
                            column: Column(PropertyValue::Column(ctx.end_id_col.clone())),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
                pre_filter: None,
            });
            
            println!(
                "expand_fixed_length_joins_with_context [NORMAL/POLYMORPHIC]: FROM {} AS {}, {} JOINs",
                from_table, from_alias, joins.len()
            );
            
            (from_table, from_alias, joins)
        }
        
        VlpSchemaType::FkEdge => {
            // FK-EDGE: Edge is FK column on node table, no separate edge table
            // FROM: start_node_table AS start_alias
            // JOINs: m1 ON start.fk_col = m1.id_col, m2 ON m1.fk_col = m2.id_col, ..., end ON mN-1.fk_col = end.id_col
            //
            // Example for *2 with parent_id FK:
            // FROM fs_objects AS child
            // JOIN fs_objects AS m1 ON child.parent_id = m1.object_id  -- hop 1
            // JOIN fs_objects AS parent ON m1.parent_id = parent.object_id  -- hop 2
            
            let from_table = ctx.start_table.clone();
            let from_alias = ctx.start_alias.clone();
            
            for hop in 1..=exact_hops {
                let is_last_hop = hop == exact_hops;
                let current_alias = if is_last_hop {
                    ctx.end_alias.clone()
                } else {
                    format!("m{}", hop)
                };
                
                let prev_alias = if hop == 1 {
                    ctx.start_alias.clone()
                } else {
                    format!("m{}", hop - 1)
                };
                
                // FK-edge: prev_node.fk_col = current_node.id_col
                // Example: child.parent_id = m1.object_id
                joins.push(Join {
                    table_name: ctx.start_table.clone(), // Same table as start (self-referencing)
                    table_alias: current_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: Column(PropertyValue::Column(ctx.rel_from_col.clone())), // FK column (parent_id)
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(current_alias),
                                column: Column(PropertyValue::Column(ctx.rel_to_col.clone())), // ID column (object_id)
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                });
            }
            
            println!(
                "expand_fixed_length_joins_with_context [FK-EDGE]: FROM {} AS {}, {} JOINs",
                from_table, from_alias, joins.len()
            );
            
            (from_table, from_alias, joins)
        }
    }
}

/// Generate cycle prevention filters for fixed-length paths
/// 
/// Prevents nodes from being revisited in a path by ensuring:
/// 1. Start node != End node
/// 2. All intermediate relationship endpoints are unique
/// 
/// For *2: `a.user_id != c.user_id AND r1.followed_id != r2.follower_id`
/// For *3: `a.user_id != d.user_id AND r1.followed_id != r2.follower_id AND r2.followed_id != r3.follower_id`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_col` - ID column name for start node
/// * `to_col` - "to" ID column name for relationships
/// * `from_col` - "from" ID column name for relationships
/// * `end_id_col` - ID column name for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters(
    exact_hops: u32,
    start_id_col: &str,
    to_col: &str,
    from_col: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    // Delegate to composite version with single-column IDs
    generate_cycle_prevention_filters_composite(
        exact_hops,
        &[start_id_col],
        &[to_col],
        &[from_col],
        &[end_id_col],
        start_alias,
        end_alias,
    )
}

/// Generate cycle prevention filters for fixed-length paths with composite IDs
/// 
/// Supports both simple and composite primary keys. For composite keys, generates
/// NOT (col1=col1 AND col2=col2 AND ...) conditions.
///
/// # Examples
///
/// Simple ID: `a.user_id != c.user_id`
///
/// Composite ID: `NOT (a.flight_date = c.flight_date AND a.flight_num = c.flight_num)`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_cols` - ID column names for start node
/// * `to_cols` - "to" ID column names for relationships
/// * `from_cols` - "from" ID column names for relationships  
/// * `end_id_cols` - ID column names for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters_composite(
    exact_hops: u32,
    start_id_cols: &[&str],
    to_cols: &[&str],
    from_cols: &[&str],
    end_id_cols: &[&str],
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    use super::render_expr::{Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias};
    
    if exact_hops == 0 {
        return None;
    }
    
    let mut filters = Vec::new();
    
    // Helper to generate composite equality check: NOT (col1=col1 AND col2=col2 AND ...)
    let generate_composite_not_equal = |left_alias: &str, left_cols: &[&str], 
                                       right_alias: &str, right_cols: &[&str]| -> RenderExpr {
        if left_cols.len() == 1 {
            // Simple ID: a.user_id != c.user_id
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::NotEqual,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(left_alias.to_string()),
                        column: Column(PropertyValue::Column(left_cols[0].to_string())),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(right_alias.to_string()),
                        column: Column(PropertyValue::Column(right_cols[0].to_string())),
                    }),
                ],
            })
        } else {
            // Composite ID: NOT (a.col1 = c.col1 AND a.col2 = c.col2 AND ...)
            let equality_checks: Vec<RenderExpr> = left_cols.iter().zip(right_cols.iter())
                .map(|(left_col, right_col)| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(PropertyValue::Column(left_col.to_string())),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(PropertyValue::Column(right_col.to_string())),
                            }),
                        ],
                    })
                })
                .collect();
            
            // Combine equality checks with AND
            let combined_equality = if equality_checks.len() == 1 {
                equality_checks.into_iter().next().unwrap()
            } else {
                equality_checks.into_iter().reduce(|acc, expr| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![acc, expr],
                    })
                }).unwrap()
            };
            
            // Wrap in NOT
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![combined_equality],
            })
        }
    };
    
    // 1. Start node != End node (prevents returning to the starting point)
    filters.push(generate_composite_not_equal(
        start_alias, start_id_cols,
        end_alias, end_id_cols,
    ));
    
    // NOTE: We previously had cycle prevention for intermediate nodes, but it was WRONG.
    // The condition `r1.to_id != r2.from_id` blocks VALID paths because that's exactly 
    // how paths connect (r1.to_id = r2.from_id is the JOIN condition).
    //
    // For proper cycle prevention (no node visited twice), we would need to track all
    // intermediate nodes and ensure they're all different from each other. This is
    // complex for inline JOINs (easy in recursive CTEs with path arrays).
    //
    // For now, we only prevent returning to the start node, which is the most common
    // cycle prevention requirement. Full cycle detection can be added later if needed.
    
    // Combine all filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        // Combine with AND
        Some(filters.into_iter().reduce(|acc, filter| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![acc, filter],
            })
        }).unwrap())
    }
}
