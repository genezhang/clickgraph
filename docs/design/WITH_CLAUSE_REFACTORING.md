# WITH Clause Refactoring Design

**Date**: December 10, 2025  
**Status**: Proposed  
**Author**: ClickGraph Team

## Problem Statement

The current implementation treats WITH as a variant of Projection (`Projection(kind=With)`), which fails to capture the full semantics of WITH as defined in OpenCypher:

1. **Missing boundary semantics**: WITH creates a scope boundary that should isolate query transformations (like bidirectional expansion)
2. **Missing syntactic elements**: ORDER BY, SKIP, LIMIT, WHERE are part of WITH syntax but currently represented as separate wrapper nodes
3. **Bridging function lost**: WITH connects query segments, but current structure doesn't make this explicit

This leads to bugs where `BidirectionalUnion` expansion crosses WITH boundaries incorrectly.

## OpenCypher Grammar Reference

```bnf
<with statement> ::= 
  WITH <return statement body> [ <order by and page clause> ] [ <where clause> ]

<return statement body> ::= 
  [ DISTINCT ] <return item list>

<order by and page clause> ::= 
    <order by clause> [ <offset clause> ] [ <limit clause> ]
  | <offset clause> [ <limit clause> ]
  | <limit clause>
```

Valid WITH syntax:
```cypher
WITH DISTINCT a, b.name AS name ORDER BY name SKIP 10 LIMIT 100 WHERE a.active = true
```

## Current Architecture (Problematic)

```rust
pub enum ProjectionKind {
    With,
    Return,
}

pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    pub kind: ProjectionKind,  // Just a flag
    pub distinct: bool,
}
```

WITH with ORDER BY/LIMIT/WHERE becomes a deeply nested structure:
```
Limit
  └── Skip
      └── OrderBy
          └── Filter (WHERE)
              └── Projection(kind=With)
                    └── GraphRel(...)
```

**Problems**:
1. Analyzers don't recognize this as a boundary
2. BidirectionalUnion traverses through all these nodes
3. No explicit representation of what WITH exports

## Proposed Architecture

### New LogicalPlan Variant

```rust
pub enum LogicalPlan {
    // ... existing variants ...
    
    /// WITH clause - creates a scope boundary between query segments
    WithClause(WithClause),
    
    // Projection is now ONLY for RETURN
    Projection(Projection),
}
```

### WithClause Structure

```rust
/// WITH clause as defined in OpenCypher.
/// Creates a materialization boundary between query segments.
/// 
/// Syntax: WITH [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT m] [WHERE ...]
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct WithClause {
    /// The query segment BEFORE this WITH (input to be projected)
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    
    /// The projection items (what WITH exports)
    pub items: Vec<ProjectionItem>,
    
    /// DISTINCT modifier
    pub distinct: bool,
    
    /// ORDER BY clause (part of WITH, not separate)
    pub order_by: Option<Vec<OrderByItem>>,
    
    /// SKIP clause (part of WITH, not separate)
    pub skip: Option<u64>,
    
    /// LIMIT clause (part of WITH, not separate)
    pub limit: Option<u64>,
    
    /// WHERE clause after WITH (filters the intermediate result)
    pub where_clause: Option<LogicalExpr>,
    
    /// Exported aliases - what's visible to downstream clauses
    /// Derived from items, but explicit for clarity
    pub exported_aliases: Vec<String>,
}
```

### Simplified Projection (RETURN only)

```rust
/// Projection for RETURN clause only.
/// No longer used for WITH - use WithClause instead.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Projection {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    pub distinct: bool,
    // Remove: pub kind: ProjectionKind  (no longer needed)
}
```

## Plan Structure Example

Query:
```cypher
MATCH (p:Person {id: 14})-[:KNOWS]-(friend:Person) 
WITH DISTINCT friend 
MATCH (friend)<-[:HAS_CREATOR]-(post:Post) 
WITH friend, post 
RETURN friend.firstName, post.id 
LIMIT 3
```

**New structure**:
```
Limit(3)
  └── Projection(RETURN: friend.firstName, post.id)
        └── WithClause(items: [friend, post], exported: [friend, post])
              └── GraphRel(HAS_CREATOR, direction: Incoming)
                    └── WithClause(items: [friend], distinct: true, exported: [friend])
                          └── GraphRel(KNOWS, direction: Either)  // bidirectional
                                └── GraphNode(p:Person)
```

Each `WithClause` is a clear boundary. Analyzers process segments independently.

## Analyzer Behavior Changes

### BidirectionalUnion

```rust
fn transform_bidirectional(plan: &Arc<LogicalPlan>, ...) -> ... {
    match plan.as_ref() {
        // NEW: WithClause is a boundary - transform input independently
        LogicalPlan::WithClause(with_clause) => {
            // Transform only the input (segment before WITH)
            let transformed_input = transform_bidirectional(&with_clause.input, ...)?;
            
            // Wrap in new WithClause - do NOT propagate Union beyond this point
            Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(WithClause {
                input: transformed_input,
                ..with_clause.clone()
            }))))
        }
        
        // GraphRel, Projection, etc. - existing logic
        // ...
    }
}
```

The key insight: **Union expansion happens WITHIN the input, and the result is wrapped in WithClause**. The downstream query sees WithClause, not Union.

### Other Analyzers

Similar pattern for all analyzers that shouldn't cross WITH boundaries:
- `ViewOptimizer`: Optimize within each segment
- `GroupByBuilding`: Aggregations are scoped to their segment
- `GraphJoinInference`: Join inference respects boundaries

## SQL Generation

WithClause naturally maps to CTE:

```rust
fn render_with_clause(with_clause: &WithClause, ...) -> RenderPlan {
    // Generate CTE name from exported aliases
    let cte_name = format!("with_{}", with_clause.exported_aliases.join("_"));
    
    // Render the input segment as CTE body
    let cte_body = render_plan(&with_clause.input, ...);
    
    // Apply ORDER BY, SKIP, LIMIT, WHERE within CTE
    let cte_sql = format!(
        "SELECT {} FROM ({}) {}{}{}{}",
        render_items(&with_clause.items),
        cte_body,
        render_order_by(&with_clause.order_by),
        render_skip(&with_clause.skip),
        render_limit(&with_clause.limit),
        render_where(&with_clause.where_clause),
    );
    
    // Return CTE definition + reference for downstream
    RenderPlan::Cte { name: cte_name, body: cte_sql }
}
```

## Migration Path

### Phase 1: Add WithClause type (non-breaking)
1. Add `WithClause` struct to `logical_plan/mod.rs`
2. Add `LogicalPlan::WithClause` variant
3. Keep existing Projection(kind=With) working

### Phase 2: Update Parser
1. Modify `evaluate_with_clause` to create `WithClause` instead of `Projection(kind=With)`
2. Consolidate ORDER BY, SKIP, LIMIT, WHERE into WithClause during parsing

### Phase 3: Update Analyzers
1. Add `LogicalPlan::WithClause` handling to all analyzers
2. Implement boundary semantics in BidirectionalUnion
3. Update other analyzers as needed

### Phase 4: Update Renderer
1. Add WithClause → CTE rendering
2. Remove special-case handling for Projection(kind=With)
3. Clean up `build_chained_with_match_cte_plan` complexity

### Phase 5: Cleanup
1. Remove `ProjectionKind` enum (Projection is only for RETURN)
2. Remove dead code paths
3. Update tests

## Testing Strategy

### Unit Tests
- WithClause structure creation
- Exported alias extraction
- Boundary detection

### Integration Tests
- Simple WITH: `MATCH ... WITH ... RETURN`
- Chained WITH: `MATCH ... WITH ... MATCH ... WITH ... RETURN`
- WITH with ORDER BY/LIMIT: `MATCH ... WITH ... ORDER BY ... LIMIT ...`
- WITH with WHERE: `MATCH ... WITH ... WHERE ...`
- Bidirectional + WITH: `MATCH ()-[]-() WITH ... MATCH ()-[]->() RETURN`

### Regression Tests
- All existing WITH tests must pass
- Verify no Union leakage across boundaries

## Benefits

1. **Correctness**: WITH boundaries are explicit and enforced
2. **Simplicity**: Analyzers have clear boundary semantics
3. **Maintainability**: Structure matches OpenCypher grammar
4. **Performance**: CTEs are generated more directly
5. **Extensibility**: Easy to add WITH-specific features

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing queries | Phased migration, keep old path working initially |
| Large code change | Well-scoped to specific files, clear phases |
| Performance regression | Benchmark before/after, CTE generation may be more efficient |

## Files to Modify

### Core Changes
- `src/query_planner/logical_plan/mod.rs` - Add WithClause
- `src/query_planner/logical_plan/with_clause.rs` - Update evaluate_with_clause
- `src/query_planner/analyzer/bidirectional_union.rs` - Add WithClause handling
- `src/render_plan/plan_builder.rs` - WithClause rendering

### Secondary Changes
- All analyzer files that traverse LogicalPlan
- `src/open_cypher_parser/` - May need AST updates
- Test files

## Estimated Effort

- Phase 1: 2-3 hours
- Phase 2: 3-4 hours  
- Phase 3: 4-6 hours
- Phase 4: 4-6 hours
- Phase 5: 2-3 hours
- Testing: 4-6 hours

**Total**: 2-3 days

## Approval

- [ ] Design reviewed
- [ ] Implementation plan approved
- [ ] Ready for Phase 1
