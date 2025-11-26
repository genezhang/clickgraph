use crate::{
    open_cypher_parser::ast::ReturnClause,
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem, ProjectionKind, Union},
};
use std::sync::Arc;

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    println!("========================================");
    println!("⚠️ RETURN CLAUSE DISTINCT = {}", return_clause.distinct);
    println!("========================================");
    let projection_items: Vec<ProjectionItem> = return_clause
        .return_items
        .iter()
        .map(|item| item.clone().into())
        .collect();
    
    // If input is a Union, push Projection into each branch
    // This keeps Union at the top level for proper SQL generation
    if let LogicalPlan::Union(union) = plan.as_ref() {
        println!("DEBUG: Input is Union, pushing Projection into {} branches", union.inputs.len());
        let projected_branches: Vec<Arc<LogicalPlan>> = union.inputs.iter().map(|branch| {
            Arc::new(LogicalPlan::Projection(Projection {
                input: branch.clone(),
                items: projection_items.clone(),
                kind: ProjectionKind::Return,
                distinct: return_clause.distinct,
            }))
        }).collect();
        
        return Arc::new(LogicalPlan::Union(Union {
            inputs: projected_branches,
            union_type: union.union_type.clone(),
        }));
    }
    
    let result = Arc::new(LogicalPlan::Projection(Projection {
        input: plan,
        items: projection_items,
        kind: ProjectionKind::Return,
        distinct: return_clause.distinct,
    }));
    println!("DEBUG evaluate_return_clause: Created Projection with distinct={}", 
        if let LogicalPlan::Projection(p) = result.as_ref() { p.distinct } else { false });
    result
}
