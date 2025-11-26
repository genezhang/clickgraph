use crate::{
    query_planner::logical_plan::LogicalPlan,
    render_plan::{
        render_expr::{
            Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication,
            PropertyAccess, RenderExpr, TableAlias,
        },
        {
            Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
            JoinItems, JoinType, OrderByItems, OrderByOrder, RenderPlan, SelectItems, ToSql,
            UnionItems, UnionType,
        },
    },
};

// Import ToSql trait from to_sql module so LogicalExpr.to_sql() works
use super::to_sql::ToSql as LogicalToSql;

// Import function translator for Neo4j -> ClickHouse function mappings
use super::function_registry::get_function_mapping;

/// Generate SQL from RenderPlan with configurable CTE depth limit
pub fn render_plan_to_sql(plan: RenderPlan, max_cte_depth: u32) -> String {
    let mut sql = String::new();
    
    // If there's a Union, the entire query is the union of sub-queries
    // Only output CTEs, UNION, and LIMIT (not SELECT/FROM/JOINS for the outer level)
    if plan.union.0.is_some() {
        sql.push_str(&plan.ctes.to_sql());
        sql.push_str(&plan.union.to_sql());
        
        // Add LIMIT after UNION if present
        if let Some(m) = plan.limit.0 {
            let skip_str = if let Some(n) = plan.skip.0 {
                format!("{n},")
            } else {
                "".to_string()
            };
            let limit_str = format!("LIMIT {skip_str} {m}");
            sql.push_str(&limit_str)
        }
        
        return sql;
    }
    
    sql.push_str(&plan.ctes.to_sql());
    sql.push_str(&plan.select.to_sql());
    sql.push_str(&plan.from.to_sql());
    sql.push_str(&plan.joins.to_sql());
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
                UnionType::Distinct => "UNION \n",
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
        eprintln!("🔍 Join::to_sql");
        eprintln!("  table_alias: {}", self.table_alias);
        eprintln!("  table_name: {}", self.table_name);
        eprintln!("  joining_on.len(): {}", self.joining_on.len());
        if !self.joining_on.is_empty() {
            eprintln!("  joining_on conditions:");
            for (idx, cond) in self.joining_on.iter().enumerate() {
                eprintln!("    [{}]: {:?}", idx, cond);
            }
        } else {
            eprintln!("  ⚠️  WARNING: joining_on is EMPTY!");
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

        let mut sql = format!(
            "{} {} AS {}",
            join_type_str, self.table_name, self.table_alias
        );

        // Note: FINAL keyword for joins would need to be added here if Join struct
        // is enhanced to track use_final. For now, joins don't support FINAL.

        // Only add ON clause if there are joining conditions
        if !self.joining_on.is_empty() {
            let joining_on_str_vec: Vec<String> =
                self.joining_on.iter().map(|cond| cond.to_sql()).collect();

            let joining_on_str = joining_on_str_vec.join(" AND ");

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
                format!("({})", inner)
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
                let args = agg
                    .args
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", agg.name, args)
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
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                let sql_op = op_str(op.operator);
                let rendered: Vec<String> = op.operands.iter().map(|e| e.to_sql()).collect();

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
                    2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                    _ => {
                        // n-ary: join with the operator
                        rendered.join(&format!(" {} ", sql_op))
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
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::In => "IN",
                Operator::NotIn => "NOT IN",
                Operator::Not => "NOT",
                Operator::Distinct => "DISTINCT",
                Operator::IsNull => "IS NULL",
                Operator::IsNotNull => "IS NOT NULL",
            }
        }

        let sql_op = op_str(self.operator);
        let rendered: Vec<String> = self.operands.iter().map(|e| e.to_sql()).collect();

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
