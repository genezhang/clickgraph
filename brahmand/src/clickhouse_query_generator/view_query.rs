//! SQL generation for view-based logical plans

//! SQL generation for graph view queries
//!
//! This module provides SQL generation for graph views by translating logical plans into
//! ClickHouse SQL queries. It handles both node and relationship scans.


use crate::clickhouse_query_generator::to_sql::ToSql;
 
use crate::query_planner::logical_plan::{LogicalPlan, ViewScan as PlanViewScan};

/// Generate SQL for view scan operations
impl ToSql for PlanViewScan {
    fn to_sql(&self) -> Result<String, super::errors::ClickhouseQueryGeneratorError> {
        let mut sql = String::new();
        
        // Build projection list
        let mut projections = Vec::new();
        
        // Always include ID column first
        projections.push(format!("{}.{} AS id", self.source_table, self.id_column));
        
        // Add property mappings
        for (prop, col) in &self.property_mapping {
            if prop != "id" {
                projections.push(format!(
                    "{}.{} AS {}",
                    self.source_table, col, prop
                ));
            }
        }
        
        // Add additional view-specific projections
        for proj in &self.projections {
            projections.push(proj.to_sql()?);
        }
        
        sql.push_str(&format!("SELECT {}", projections.join(", ")));
        
        // Add source table
        sql.push_str(&format!(" FROM {}", self.source_table));
        
        // Add join for relationship queries
        if let Some(input) = &self.input {
            match &**input {
                LogicalPlan::ViewScan(inner) => {
                    sql.push_str(&format!(" INNER JOIN ({}) AS input", inner.to_sql()?));
                    sql.push_str(&format!(" ON {}.id = input.id", self.source_table));
                }
                _ => { }
            }
        }
        
        // Add filters 
        let mut filters = Vec::new();
        
        // Add view filter
        if let Some(filter) = &self.view_filter {
            filters.push(filter.to_sql()?);
        }
        
        // Combine all filters
        if !filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&filters.join(" AND "));
        }
        
        Ok(sql)
    }
}



// Note: We use ViewScan from query_planner::logical_plan instead of defining our own