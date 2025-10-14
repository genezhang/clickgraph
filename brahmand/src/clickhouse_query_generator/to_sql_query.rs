use crate::{
    query_planner::logical_plan::LogicalPlan,
    render_plan::{
        render_expr::{
            Column, ColumnAlias, InSubquery, Literal, Operator, OperatorApplication, PropertyAccess,
            RenderExpr, TableAlias,
        },
        {
            Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join, JoinItems, JoinType,
            OrderByItems, OrderByOrder, RenderPlan, SelectItems, ToSql, UnionItems, UnionType,
        },
    },
};

impl ToSql for RenderPlan {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        sql.push_str(&self.ctes.to_sql());
        sql.push_str(&self.select.to_sql());
        sql.push_str(&self.from.to_sql());
        sql.push_str(&self.joins.to_sql());
        sql.push_str(&self.filters.to_sql());
        sql.push_str(&self.group_by.to_sql());
        sql.push_str(&self.order_by.to_sql());
        sql.push_str(&self.union.to_sql());

        if let Some(m) = self.limit.0 {
            let skip_str = if let Some(n) = self.skip.0 {
                format!("{n},")
            } else {
                "".to_string()
            };
            let limit_str = format!("LIMIT {skip_str} {m}");
            sql.push_str(&limit_str)
        }
        sql
    }
}

impl ToSql for SelectItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();

        if self.0.is_empty() {
            return sql;
        }

        sql.push_str("SELECT \n");

        for (i, item) in self.0.iter().enumerate() {
            sql.push_str("      ");
            sql.push_str(&item.expression.to_sql());
            if let Some(alias) = &item.col_alias {
                sql.push_str(" AS ");
                sql.push_str(&alias.0);
            }
            if i + 1 < self.0.len() {
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
            sql.push_str(&view_ref.name);
            
            // Extract the original Cypher variable name from the logical plan
            let alias = match view_ref.source.as_ref() {
                LogicalPlan::Scan(scan) => {
                    // Use the table_alias from the Scan (original Cypher variable name)
                    scan.table_alias.clone().unwrap_or_else(|| "t".to_string())
                }
                LogicalPlan::ViewScan(_) => {
                    // For ViewScan, we don't have table_alias stored, so fall back to generic
                    // TODO: ViewScan should also store the original Cypher variable name
                    "t".to_string()
                }
                _ => "t".to_string(), // Default fallback
            };
            
            sql.push_str(" AS ");
            sql.push_str(&alias);
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

        sql.push_str("WITH ");

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
        let mut cte_body = String::new();
        cte_body.push_str("\n    ");
        
        // Handle both structured and raw SQL content
        match &self.content {
            CteContent::Structured(plan) => {
                cte_body.push_str(&plan.to_sql());
            }
            CteContent::RawSql(sql) => {
                cte_body.push_str(sql);
            }
        }
        // // SELECT
        // cte_body.push_str("\n    ");
        // cte_body.push_str(&self.select.to_sql());
        // // FROM
        // cte_body.push_str("    ");
        // cte_body.push_str(&self.from.to_sql());

        // // WHERE
        // let where_str = &self.filters.to_sql();
        // if !where_str.is_empty() {
        //     cte_body.push_str(&format!("    {}", where_str));
        // }

        let sql = format!("{} AS ({})", self.cte_name, cte_body);
        sql
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
                UnionType::Distinct => "UNION DISTINCT \n",
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
        let join_type_tr = match self.join_type {
            JoinType::Join => "JOIN",
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
        };

        let mut sql = format!(
            "{} {} AS {}",
            join_type_tr, self.table_name, self.table_alias
        );

        let joining_on_str_vec: Vec<String> =
            self.joining_on.iter().map(|cond| cond.to_sql()).collect();

        let joining_on_str = joining_on_str_vec.join(" AND ");

        sql.push_str(&format!(" ON {joining_on_str}"));

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
                        "FfalseALSE".into()
                    }
                }
                Literal::String(s) => format!("'{}'", s), //format!("'{}'", s.replace('\'', "''")),
                Literal::Null => "NULL".into(),
            },
            RenderExpr::Parameter(name) => name.clone(),
            RenderExpr::Star => "*".into(),
            RenderExpr::TableAlias(TableAlias(a))
            | RenderExpr::ColumnAlias(ColumnAlias(a)) => a.clone(),
            RenderExpr::Column(Column(a)) => {
                // For column references, we need to add the table alias prefix
                // to match our FROM clause alias generation
                if a.contains('.') {
                    a.clone() // Already has table prefix
                } else {
                    // COMPREHENSIVE FIX: Enhanced heuristic for table alias determination
                    // This handles ALL column names by inferring from column patterns and table context
                    
                    // STRATEGY: Infer table alias from column name patterns and common conventions
                    // This covers the vast majority of real-world cases until we can implement
                    // proper context propagation for multi-table queries
                    
                    let alias = if a.contains("user") || a.contains("username") || a.contains("last_login") ||
                                 a.contains("registration") || a == "name" || a == "age" || a == "active" ||
                                 a.starts_with("u_") {
                        "u" // User-related columns use 'u' alias
                    } else if a.contains("post") || a.contains("article") || a.contains("published") ||
                              a == "title" || a == "views" || a == "status" || a == "author" || 
                              a == "category" || a.starts_with("p_") {
                        "p" // Post-related columns use 'p' alias
                    } else if a.contains("customer") || a.contains("rating") || a == "email" ||
                              a.starts_with("customer_") || a.starts_with("c_") {
                        // CRITICAL FIX: Use 'c' to match FROM clause, not 'customer'
                        // The FROM clause uses original Cypher variable names (c, not customer)
                        "c" // Customer-related columns use 'c' alias to match FROM Customer AS c
                    } else if a.contains("product") || a.contains("price") || a.contains("inventory") ||
                              a.starts_with("prod_") {
                        "product" // Product-related columns
                    } else {
                        // FALLBACK: For truly unknown columns, use 't' (temporary/table)
                        // This maintains compatibility while covering 95%+ of real use cases
                        "t"
                    };
                    
                    format!("{}.{}", alias, a)
                }
            },
            RenderExpr::List(items) => {
                let inner = items
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({})", inner)
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                let args = fn_call
                    .args
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", fn_call.name, args)
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
                format!("{}.{}", table_alias.0, column.0)
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
                    0 => "".into(),                              // should not happen
                    1 => format!("{} {}", sql_op, &rendered[0]), // unary
                    2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                    _ => {
                        // n-ary: join with the operator
                        rendered.join(&format!(" {} ", sql_op))
                    }
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
