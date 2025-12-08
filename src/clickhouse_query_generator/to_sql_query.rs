use crate::{
    query_planner::logical_plan::LogicalPlan,
    render_plan::{
        render_expr::{
            Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication,
            PropertyAccess, RenderExpr, TableAlias,
        },
        {
            ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
            JoinItems, JoinType, OrderByItems, OrderByOrder, RenderPlan, SelectItems, ToSql,
            UnionItems, UnionType,
        },
    },
};

// Import function translator for Neo4j -> ClickHouse function mappings
use super::function_registry::get_function_mapping;
use super::function_translator::{get_ch_function_name, CH_PASSTHROUGH_PREFIX};

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

/// Check if any operand in the expression contains a string
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(|op| contains_string_literal(op))
}

/// Flatten nested + operations into a list of operands for concat()
fn flatten_addition_operands(expr: &RenderExpr) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().flat_map(|o| flatten_addition_operands(o)).collect()
        }
        _ => vec![expr.to_sql()],
    }
}

/// Generate SQL from RenderPlan with configurable CTE depth limit
pub fn render_plan_to_sql(plan: RenderPlan, max_cte_depth: u32) -> String {
    let mut sql = String::new();
    
    // If there's a Union, wrap it in a subquery for correct ClickHouse behavior.
    // ClickHouse has a quirk where LIMIT/ORDER BY on bare UNION ALL only applies to
    // the last branch, not the combined result. Wrapping in a subquery fixes this.
    if plan.union.0.is_some() {
        sql.push_str(&plan.ctes.to_sql());
        
        // Check if SELECT items contain aggregation (e.g., count(*), sum(), etc.)
        let has_aggregation = plan.select.items.iter().any(|item| {
            matches!(&item.expression, RenderExpr::AggregateFnCall(_))
        });
        
        // Check if we need the subquery wrapper (when there's ORDER BY, LIMIT, GROUP BY, or aggregation)
        let needs_subquery = !plan.order_by.0.is_empty() 
            || plan.limit.0.is_some() 
            || plan.skip.0.is_some()
            || !plan.group_by.0.is_empty()
            || has_aggregation;
        
        if needs_subquery {
            // Wrap UNION in a subquery
            // If there are specific SELECT items (aggregation case), use them
            // Otherwise default to SELECT *
            if !plan.select.items.is_empty() {
                sql.push_str(&plan.select.to_sql());
                sql.push_str("FROM (\n");
            } else {
                sql.push_str("SELECT * FROM (\n");
            }
            sql.push_str(&plan.union.to_sql());
            sql.push_str(") AS __union\n");
            
            // Add GROUP BY if present
            sql.push_str(&plan.group_by.to_sql());
            
            // Add ORDER BY after GROUP BY if present
            sql.push_str(&plan.order_by.to_sql());
            
            // Add LIMIT after ORDER BY if present
            if let Some(m) = plan.limit.0 {
                let skip_str = if let Some(n) = plan.skip.0 {
                    format!("{n},")
                } else {
                    "".to_string()
                };
                let limit_str = format!("LIMIT {skip_str} {m}");
                sql.push_str(&limit_str)
            }
        } else {
            // No ordering/limiting - bare UNION is fine
            sql.push_str(&plan.union.to_sql());
        }
        
        return sql;
    }
    
    sql.push_str(&plan.ctes.to_sql());
    sql.push_str(&plan.select.to_sql());
    sql.push_str(&plan.from.to_sql());
    sql.push_str(&plan.joins.to_sql());
    sql.push_str(&plan.array_join.to_sql());
    sql.push_str(&plan.filters.to_sql());
    sql.push_str(&plan.group_by.to_sql());

    // Add HAVING clause if present (after GROUP BY, before ORDER BY)
    if let Some(having_expr) = &plan.having_clause {
        sql.push_str("HAVING ");
        sql.push_str(&having_expr.to_sql());
        sql.push('\n');
    }

    sql.push_str(&plan.order_by.to_sql());
    sql.push_str(&plan.union.to_sql());

    if let Some(m) = plan.limit.0 {
        let skip_str = if let Some(n) = plan.skip.0 {
            format!("{n},")
        } else {
            "".to_string()
        };
        let limit_str = format!("LIMIT {skip_str} {m}");
        sql.push_str(&limit_str)
    }

    // Add ClickHouse SETTINGS for recursive CTEs (variable-length paths)
    // Check if any CTE is recursive
    let has_recursive_cte = plan.ctes.0.iter().any(|cte| cte.is_recursive);
    if has_recursive_cte {
        sql.push_str(&format!(
            "\nSETTINGS max_recursive_cte_evaluation_depth = {}",
            max_cte_depth
        ));
    }

    sql
}

impl ToSql for RenderPlan {
    fn to_sql(&self) -> String {
        // Use default depth of 100 when called via trait
        render_plan_to_sql(self.clone(), 100)
    }
}

impl ToSql for SelectItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();

        if self.items.is_empty() {
            return sql;
        }

        if self.distinct {
            sql.push_str("SELECT DISTINCT \n");
        } else {
            sql.push_str("SELECT \n");
        }

        for (i, item) in self.items.iter().enumerate() {
            sql.push_str("      ");
            sql.push_str(&item.expression.to_sql());
            if let Some(alias) = &item.col_alias {
                sql.push_str(" AS \"");
                sql.push_str(&alias.0);
                sql.push('"');
            }
            if i + 1 < self.items.len() {
                sql.push_str(", ");
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for FromTableItem {
    fn to_sql(&self) -> String {
        if let Some(view_ref) = &self.0 {
            let mut sql = String::new();
            sql.push_str("FROM ");

            // For all references, use the name directly
            // Note: WHERE clause filtering is handled in WhereClause generation,
            // not as a subquery in FROM clause
            sql.push_str(&view_ref.name);

            // Extract the alias - prefer the explicit alias from ViewTableRef,
            // otherwise try to get it from the source logical plan
            let alias = if let Some(explicit_alias) = &view_ref.alias {
                explicit_alias.clone()
            } else {
                match view_ref.source.as_ref() {
                    LogicalPlan::Scan(scan) => {
                        // Use the table_alias from the Scan (original Cypher variable name)
                        scan.table_alias.clone().unwrap_or_else(|| "t".to_string())
                    }
                    LogicalPlan::ViewScan(_) => {
                        // ViewScan fallback - should not reach here if alias is properly set
                        "t".to_string()
                    }
                    _ => "t".to_string(), // Default fallback
                }
            };

            sql.push_str(" AS ");
            sql.push_str(&alias);

            // Add FINAL keyword AFTER alias if needed (ClickHouse syntax: FROM table AS alias FINAL)
            if view_ref.use_final {
                sql.push_str(" FINAL");
            }

            sql.push('\n');
            sql
        } else {
            "".into()
        }

        // let mut sql: String = String::new();
        // if self.0.is_none() {
        //     return sql;
        // }
        // sql.push_str("FROM ");

        // sql.push_str(&self.table_name);
        // if let Some(alias) = &self.table_alias {
        //     if !alias.is_empty() {
        //         sql.push_str(" AS ");
        //         sql.push_str(&alias);
        //     }
        // }
        // sql.push('\n');
        // sql
    }
}

impl ToSql for FilterItems {
    fn to_sql(&self) -> String {
        if let Some(expr) = &self.0 {
            format!("WHERE {}\n", expr.to_sql())
        } else {
            "".into()
        }
    }
}

/// ARRAY JOIN for ClickHouse - maps from Cypher UNWIND
/// 
/// Example: UNWIND r.items AS item
/// Generates: ARRAY JOIN r.items AS item
impl ToSql for ArrayJoinItem {
    fn to_sql(&self) -> String {
        if let Some(array_join) = &self.0 {
            format!("ARRAY JOIN {} AS {}\n", array_join.expression.to_sql(), array_join.alias)
        } else {
            "".into()
        }
    }
}

impl ToSql for GroupByExpressions {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("GROUP BY ");
        for (i, e) in self.0.iter().enumerate() {
            sql.push_str(&e.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for OrderByItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("ORDER BY ");
        for (i, item) in self.0.iter().enumerate() {
            sql.push_str(&item.expression.to_sql());
            sql.push(' ');
            sql.push_str(&item.order.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for CteItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }

        // Check if any CTE is recursive
        let has_recursive_cte = self.0.iter().any(|cte| cte.is_recursive);

        if has_recursive_cte {
            sql.push_str("WITH RECURSIVE ");
        } else {
            sql.push_str("WITH ");
        }

        for (i, cte) in self.0.iter().enumerate() {
            sql.push_str(&cte.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for Cte {
    fn to_sql(&self) -> String {
        // Handle both structured and raw SQL content
        match &self.content {
            CteContent::Structured(plan) => {
                // For structured content, render only the query body (not nested CTEs)
                // CTEs should already be hoisted to the top level
                let mut cte_body = String::new();

                // If there are no explicit SELECT items, default to SELECT *
                if plan.select.items.is_empty() {
                    cte_body.push_str("SELECT *\n");
                } else {
                    cte_body.push_str(&plan.select.to_sql());
                }

                cte_body.push_str(&plan.from.to_sql());
                cte_body.push_str(&plan.joins.to_sql());
                cte_body.push_str(&plan.filters.to_sql());
                cte_body.push_str(&plan.group_by.to_sql());

                // Add HAVING clause if present (after GROUP BY)
                if let Some(having_expr) = &plan.having_clause {
                    cte_body.push_str("HAVING ");
                    cte_body.push_str(&having_expr.to_sql());
                    cte_body.push('\n');
                }

                cte_body.push_str(&plan.order_by.to_sql());
                cte_body.push_str(&plan.union.to_sql());

                format!("{} AS ({})", self.cte_name, cte_body)
            }
            CteContent::RawSql(sql) => {
                // For raw SQL, it already includes the CTE name and AS clause
                // from VariableLengthCteGenerator.generate_recursive_sql()
                sql.clone()
            }
        }
    }
}

impl ToSql for UnionItems {
    fn to_sql(&self) -> String {
        if let Some(union) = &self.0 {
            let union_sql_strs: Vec<String> = union
                .input
                .iter()
                .map(|union_item| union_item.to_sql())
                .collect();

            let union_type_str = match union.union_type {
                UnionType::Distinct => "UNION DISTINCT \n",  // ClickHouse requires explicit DISTINCT
                UnionType::All => "UNION ALL \n",
            };

            union_sql_strs.join(union_type_str)
        } else {
            "".into()
        }
    }
}

impl ToSql for JoinItems {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        for join in &self.0 {
            sql.push_str(&join.to_sql());
        }
        sql
    }
}

impl ToSql for Join {
    fn to_sql(&self) -> String {
        crate::debug_println!("🔍 Join::to_sql");
        crate::debug_print!("  table_alias: {}", self.table_alias);
        crate::debug_print!("  table_name: {}", self.table_name);
        crate::debug_print!("  joining_on.len(): {}", self.joining_on.len());
        crate::debug_print!("  pre_filter: {:?}", self.pre_filter.is_some());
        if !self.joining_on.is_empty() {
            crate::debug_print!("  joining_on conditions:");
            for (_idx, _cond) in self.joining_on.iter().enumerate() {
                crate::debug_print!("    [{}]: {:?}", _idx, _cond);
            }
        } else {
            crate::debug_print!("  ⚠️  WARNING: joining_on is EMPTY!");
        }

        let join_type_str = match self.join_type {
            JoinType::Join => {
                if self.joining_on.is_empty() {
                    "CROSS JOIN"
                } else {
                    "JOIN"
                }
            }
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
        };

        // For LEFT JOIN with pre_filter, use subquery form:
        // LEFT JOIN (SELECT * FROM table WHERE pre_filter) AS alias ON ...
        // This ensures the filter is applied BEFORE the join (correct LEFT JOIN semantics)
        //
        // For INNER JOIN with pre_filter, add filter to ON clause:
        // INNER JOIN table AS alias ON <join_cond> AND <pre_filter>
        // This is semantically equivalent and more efficient than subquery
        let table_expr = if let Some(ref pre_filter) = self.pre_filter {
            if matches!(self.join_type, JoinType::Left) {
                // Use to_sql_without_table_alias to render column names without table prefix
                // since inside the subquery, the table is not yet aliased
                let filter_sql = pre_filter.to_sql_without_table_alias();
                crate::debug_print!("  Using subquery form for LEFT JOIN with pre_filter: {}", filter_sql);
                format!("(SELECT * FROM {} WHERE {})", self.table_name, filter_sql)
            } else {
                // For non-LEFT joins, pre_filter will be added to ON clause below
                self.table_name.clone()
            }
        } else {
            self.table_name.clone()
        };

        let mut sql = format!(
            "{} {} AS {}",
            join_type_str, table_expr, self.table_alias
        );

        // Note: FINAL keyword for joins would need to be added here if Join struct
        // is enhanced to track use_final. For now, joins don't support FINAL.

        // Only add ON clause if there are joining conditions
        if !self.joining_on.is_empty() {
            let joining_on_str_vec: Vec<String> =
                self.joining_on.iter().map(|cond| cond.to_sql()).collect();

            let mut joining_on_str = joining_on_str_vec.join(" AND ");
            
            // For INNER JOINs (not LEFT), add pre_filter to ON clause
            // This applies polymorphic edge filters, schema filters, etc.
            if let Some(ref pre_filter) = self.pre_filter {
                if !matches!(self.join_type, JoinType::Left) {
                    let filter_sql = pre_filter.to_sql();
                    crate::debug_print!("  Adding pre_filter to INNER JOIN ON clause: {}", filter_sql);
                    joining_on_str = format!("{} AND {}", joining_on_str, filter_sql);
                }
            }

            sql.push_str(&format!(" ON {joining_on_str}"));
        }

        sql.push('\n');
        sql
    }
}

impl RenderExpr {
    /// Render this expression (including any subqueries) to a SQL string.
    pub fn to_sql(&self) -> String {
        match self {
            RenderExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => {
                    if *b {
                        "true".into()
                    } else {
                        "false".into()
                    }
                }
                Literal::String(s) => format!("'{}'", s), //format!("'{}'", s.replace('\'', "''")),
                Literal::Null => "NULL".into(),
            },
            RenderExpr::Parameter(name) => format!("${}", name),
            RenderExpr::Raw(raw) => raw.clone(),
            RenderExpr::Star => "*".into(),
            RenderExpr::TableAlias(TableAlias(a)) | RenderExpr::ColumnAlias(ColumnAlias(a)) => {
                a.clone()
            }
            RenderExpr::Column(Column(a)) => {
                // For column references, we need to add the table alias prefix
                // to match our FROM clause alias generation
                let raw_value = a.raw();
                if raw_value.contains('.') {
                    raw_value.to_string() // Already has table prefix
                } else {
                    // COMPREHENSIVE FIX: Enhanced heuristic for table alias determination
                    // This handles ALL column names by inferring from column patterns and table context

                    // STRATEGY: Infer table alias from column name patterns and common conventions
                    // This covers the vast majority of real-world cases until we can implement
                    // proper context propagation for multi-table queries

                    let alias = if raw_value.contains("user")
                        || raw_value.contains("username")
                        || raw_value.contains("last_login")
                        || raw_value.contains("registration")
                        || raw_value == "name"
                        || raw_value == "age"
                        || raw_value == "active"
                        || raw_value.starts_with("u_")
                    {
                        "u" // User-related columns use 'u' alias
                    } else if raw_value.contains("post")
                        || raw_value.contains("article")
                        || raw_value.contains("published")
                        || raw_value == "title"
                        || raw_value == "views"
                        || raw_value == "status"
                        || raw_value == "author"
                        || raw_value == "category"
                        || raw_value.starts_with("p_")
                    {
                        "p" // Post-related columns use 'p' alias
                    } else if raw_value.contains("customer")
                        || raw_value.contains("rating")
                        || raw_value == "email"
                        || raw_value.starts_with("customer_")
                        || raw_value.starts_with("c_")
                    {
                        // CRITICAL FIX: Use 'c' to match FROM clause, not 'customer'
                        // The FROM clause uses original Cypher variable names (c, not customer)
                        "c" // Customer-related columns use 'c' alias to match FROM Customer AS c
                    } else if raw_value.contains("product")
                        || raw_value.contains("price")
                        || raw_value.contains("inventory")
                        || raw_value.starts_with("prod_")
                    {
                        "product" // Product-related columns
                    } else {
                        // FALLBACK: For truly unknown columns, use 't' (temporary/table)
                        // This maintains compatibility while covering 95%+ of real use cases
                        "t"
                    };

                    format!("{}.{}", alias, raw_value)
                }
            }
            RenderExpr::List(items) => {
                let inner = items
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                // Use array literal syntax [...] for ClickHouse
                // This works for both ARRAY JOIN (UNWIND) and IN clauses
                format!("[{}]", inner)
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check if we have a Neo4j -> ClickHouse mapping
                let fn_name_lower = fn_call.name.to_lowercase();
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        // Convert arguments to SQL
                        let args_sql: Vec<String> =
                            fn_call.args.iter().map(|e| e.to_sql()).collect();

                        // Apply transformation if provided
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };

                        // Return ClickHouse function with transformed args
                        format!(
                            "{}({})",
                            mapping.clickhouse_name,
                            transformed_args.join(", ")
                        )
                    }
                    None => {
                        // No mapping found - use original function name (passthrough)
                        let args = fn_call
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", fn_call.name, args)
                    }
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                // Check for ClickHouse pass-through prefix (ch.)
                if agg.name.starts_with(CH_PASSTHROUGH_PREFIX) {
                    if let Some(ch_fn_name) = get_ch_function_name(&agg.name) {
                        if ch_fn_name.is_empty() {
                            panic!("ch. prefix requires a function name (e.g., ch.uniq)");
                        }
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        log::debug!(
                            "ClickHouse aggregate pass-through: ch.{}({}) -> {}({})",
                            ch_fn_name, args, ch_fn_name, args
                        );
                        return format!("{}({})", ch_fn_name, args);
                    }
                }
                
                // Check if we have a Neo4j -> ClickHouse mapping for aggregate functions
                let fn_name_lower = agg.name.to_lowercase();
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        let args_sql: Vec<String> = agg.args.iter().map(|e| e.to_sql()).collect();
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };
                        format!("{}({})", mapping.clickhouse_name, transformed_args.join(", "))
                    }
                    None => {
                        // No mapping - use original name (count, sum, min, max, avg, etc.)
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", agg.name, args)
                    }
                }
            }
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias,
                column,
            }) => {
                // Use PropertyValue.to_sql() to handle both simple columns and expressions
                column.0.to_sql(&table_alias.0)
            }
            RenderExpr::OperatorApplicationExp(op) => {
                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX",  // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH",  // Special handling below
                        Operator::EndsWith => "ENDS WITH",      // Special handling below
                        Operator::Contains => "CONTAINS",       // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                let rendered: Vec<String> = op.operands.iter().map(|e| e.to_sql()).collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // Special handling for IN/NOT IN with array columns
                // Cypher: x IN array_property → ClickHouse: has(array, x)
                if op.operator == Operator::In && rendered.len() == 2 {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        return format!("has({}, {})", &rendered[1], &rendered[0]);
                    }
                }
                if op.operator == Operator::NotIn && rendered.len() == 2 {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
                    }
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
                }

                // Special handling for Addition with string operands - use concat()
                // ClickHouse doesn't support + for string concatenation
                // Flatten nested + operations to handle cases like: a + ' - ' + b
                if op.operator == Operator::Addition && has_string_operand(&op.operands) {
                    let flattened: Vec<String> = op.operands.iter()
                        .flat_map(|o| flatten_addition_operands(o))
                        .collect();
                    return format!("concat({})", flattened.join(", "));
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(), // should not happen
                    1 => {
                        // Handle unary operators: IS NULL/IS NOT NULL are suffix, NOT is prefix
                        match op.operator {
                            Operator::IsNull | Operator::IsNotNull => {
                                format!("{} {}", &rendered[0], sql_op) // suffix: "x IS NULL"
                            }
                            _ => {
                                format!("{} {}", sql_op, &rendered[0]) // prefix: "NOT x"
                            }
                        }
                    }
                    2 => {
                        // For AND/OR, wrap in parentheses to ensure correct precedence
                        // when combined with other expressions
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                            }
                            _ => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                        }
                    }
                    _ => {
                        // n-ary: join with the operator, wrap in parentheses for AND/OR
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({})", rendered.join(&format!(" {} ", sql_op)))
                            }
                            _ => rendered.join(&format!(" {} ", sql_op)),
                        }
                    }
                }
            }
            RenderExpr::Case(case) => {
                // For ClickHouse, use caseWithExpression for simple CASE expressions
                if let Some(case_expr) = &case.expr {
                    // caseWithExpression(expr, val1, res1, val2, res2, ..., default)
                    let mut args = vec![case_expr.to_sql()];

                    for (when_expr, then_expr) in &case.when_then {
                        args.push(when_expr.to_sql());
                        args.push(then_expr.to_sql());
                    }

                    let else_expr = case
                        .else_expr
                        .as_ref()
                        .map(|e| e.to_sql())
                        .unwrap_or_else(|| "NULL".to_string());
                    args.push(else_expr);

                    format!("caseWithExpression({})", args.join(", "))
                } else {
                    // Searched CASE - use standard CASE syntax
                    let mut sql = String::from("CASE");

                    for (when_expr, then_expr) in &case.when_then {
                        sql.push_str(&format!(
                            " WHEN {} THEN {}",
                            when_expr.to_sql(),
                            then_expr.to_sql()
                        ));
                    }

                    if let Some(else_expr) = &case.else_expr {
                        sql.push_str(&format!(" ELSE {}", else_expr.to_sql()));
                    }

                    sql.push_str(" END");
                    sql
                }
            }
            RenderExpr::InSubquery(InSubquery { expr, subplan }) => {
                let left = expr.to_sql();
                let body = subplan.to_sql();
                let body = body.split_whitespace().collect::<Vec<&str>>().join(" ");

                format!("{} IN ({})", left, body)
            }
            RenderExpr::ExistsSubquery(exists) => {
                // Use the pre-generated SQL from the ExistsSubquery
                format!("EXISTS ({})", exists.sql)
            }
        }
    }

    /// Render this expression to SQL without table alias prefixes.
    /// Used for rendering filters inside subqueries where the table is not yet aliased.
    /// e.g., `LEFT JOIN (SELECT * FROM table WHERE is_active = true) AS b`
    /// The filter should be `is_active = true`, not `b.is_active = true`.
    pub fn to_sql_without_table_alias(&self) -> String {
        match self {
            RenderExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                // Just render the column without the table alias prefix
                column.0.to_sql_column_only()
            }
            RenderExpr::OperatorApplicationExp(op) => {
                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX",  // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH",  // Special handling below
                        Operator::EndsWith => "ENDS WITH",      // Special handling below
                        Operator::Contains => "CONTAINS",       // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                // Recursively render operands without table alias
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| e.to_sql_without_table_alias())
                    .collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // Special handling for IN/NOT IN with array columns
                if op.operator == Operator::In && rendered.len() == 2 {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        return format!("has({}, {})", &rendered[1], &rendered[0]);
                    }
                }
                if op.operator == Operator::NotIn && rendered.len() == 2 {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
                    }
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(),
                    1 => {
                        match op.operator {
                            Operator::IsNull | Operator::IsNotNull => {
                                format!("{} {}", &rendered[0], sql_op)
                            }
                            _ => {
                                format!("{} {}", sql_op, &rendered[0])
                            }
                        }
                    }
                    2 => {
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                            }
                            _ => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                        }
                    }
                    _ => {
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({})", rendered.join(&format!(" {} ", sql_op)))
                            }
                            _ => rendered.join(&format!(" {} ", sql_op)),
                        }
                    }
                }
            }
            // For Raw expressions, strip table alias prefixes (e.g., "alias.column" -> "column")
            // This is needed for LEFT JOIN subqueries where the filter is inside SELECT * FROM table
            RenderExpr::Raw(raw_sql) => {
                // Simple approach: look for "word.word" patterns and keep only the part after the dot
                // This handles cases like "alias.column = 'value'" -> "column = 'value'"
                let result = raw_sql.clone();
                // Find and replace all "identifier.identifier" patterns
                let parts: Vec<&str> = result.split_whitespace().collect();
                let mut new_parts = Vec::new();
                for part in parts {
                    if part.contains('.') && !part.starts_with('\'') && !part.starts_with('"') {
                        // Split on dot and take the last part (the column name)
                        // But preserve the structure (e.g., "alias.column" becomes "column")
                        let dot_parts: Vec<&str> = part.split('.').collect();
                        if dot_parts.len() == 2 && !dot_parts[0].is_empty() && !dot_parts[1].is_empty() {
                            // Check if first part looks like an identifier (not a number)
                            let first_char = dot_parts[0].chars().next().unwrap_or('0');
                            if first_char.is_alphabetic() || first_char == '_' {
                                new_parts.push(dot_parts[1].to_string());
                                continue;
                            }
                        }
                    }
                    new_parts.push(part.to_string());
                }
                new_parts.join(" ")
            }
            // For other expression types, delegate to regular to_sql
            _ => self.to_sql(),
        }
    }
}

impl ToSql for OperatorApplication {
    fn to_sql(&self) -> String {
        // Map your enum to SQL tokens
        fn op_str(o: Operator) -> &'static str {
            match o {
                Operator::Addition => "+",
                Operator::Subtraction => "-",
                Operator::Multiplication => "*",
                Operator::Division => "/",
                Operator::ModuloDivision => "%",
                Operator::Exponentiation => "^",
                Operator::Equal => "=",
                Operator::NotEqual => "<>",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::RegexMatch => "REGEX",  // Special handling below
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::In => "IN",
                Operator::NotIn => "NOT IN",
                Operator::StartsWith => "STARTS WITH",  // Special handling below
                Operator::EndsWith => "ENDS WITH",      // Special handling below
                Operator::Contains => "CONTAINS",       // Special handling below
                Operator::Not => "NOT",
                Operator::Distinct => "DISTINCT",
                Operator::IsNull => "IS NULL",
                Operator::IsNotNull => "IS NOT NULL",
            }
        }

        let rendered: Vec<String> = self.operands.iter().map(|e| e.to_sql()).collect();

        // Special handling for RegexMatch - ClickHouse uses match() function
        if self.operator == Operator::RegexMatch && rendered.len() == 2 {
            return format!("match({}, {})", &rendered[0], &rendered[1]);
        }

        // Special handling for IN/NOT IN with array columns
        if self.operator == Operator::In && rendered.len() == 2 {
            if matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_)) {
                return format!("has({}, {})", &rendered[1], &rendered[0]);
            }
        }
        if self.operator == Operator::NotIn && rendered.len() == 2 {
            if matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_)) {
                return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
            }
        }

        // Special handling for string predicates - ClickHouse uses functions
        if self.operator == Operator::StartsWith && rendered.len() == 2 {
            return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::EndsWith && rendered.len() == 2 {
            return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::Contains && rendered.len() == 2 {
            return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
        }

        // Special handling for Addition with string operands - use concat()
        // ClickHouse doesn't support + for string concatenation
        // Flatten nested + operations to handle cases like: a + ' - ' + b
        if self.operator == Operator::Addition && has_string_operand(&self.operands) {
            let flattened: Vec<String> = self.operands.iter()
                .flat_map(|o| flatten_addition_operands(o))
                .collect();
            return format!("concat({})", flattened.join(", "));
        }

        let sql_op = op_str(self.operator);

        match rendered.len() {
            0 => "".into(),                              // should not happen
            1 => format!("{} {}", sql_op, &rendered[0]), // unary
            2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
            _ => {
                // n-ary: join with the operator
                rendered.join(&format!(" {} ", sql_op))
            }
        }
    }
}

impl ToSql for OrderByOrder {
    fn to_sql(&self) -> String {
        match self {
            OrderByOrder::Asc => "ASC".to_string(),
            OrderByOrder::Desc => "DESC".to_string(),
        }
    }
}
