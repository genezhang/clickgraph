# Unified CTE System Design Document

**Date**: January 14, 2026
**Status**: Design Complete - Ready for Implementation
**Priority**: Critical for GA Readiness (CTE Complexity Blocker)

## Executive Summary

The current CTE system spans 8,261 lines across 3 files with complex conditional logic for different schema variations. This design proposes a **CTE Manager** that uses the `PatternSchemaContext` abstraction to provide a unified interface for CTE generation, eliminating scattered conditionals and enabling clean separation of concerns.

**Key Benefits**:
- **-60% lines of code**: Eliminate scattered conditionals and duplicated logic
- **+100% testability**: Each strategy tested independently
- **Exhaustive pattern matching**: Compile-time guarantees for schema variation coverage
- **Unified interface**: Single `CteManager` handling all schema variations

**Timeline**: 11 weeks total implementation time
**Risk Level**: Medium (incremental rollout with feature flags)

---

## 1. Problem Statement

### Current CTE Architecture Complexity

The CTE system has evolved into a **highly complex, multi-file architecture** with severe separation of concerns violations:

| File | Lines | Purpose | Complexity Level |
|------|-------|---------|------------------|
| `cte_extraction.rs` | 4,256 | CTE extraction & property analysis | HIGH |
| `variable_length_cte.rs` | 3,244 | Recursive CTE SQL generation | VERY HIGH |
| `cte_generation.rs` | 761 | CTE context & metadata management | MEDIUM |
| **TOTAL** | **8,261** | **Complete CTE lifecycle** | **EXTREME** |

### Root Cause: Cross-Cutting CTE Concerns

The CTE system suffers from **severe separation of concerns violations**:

```rust
// CTE logic scattered across multiple domains:
pub fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
    // 1. CTE extraction logic (lines 14062-14200)
    // 2. Property requirement analysis (interspersed)
    // 3. JOIN vs CTE decision logic (complex branching)
    // 4. VLP-specific CTE generation (recursive complexity)
    // 5. Multi-type relationship handling (UNION ALL logic)
    // 6. Schema-aware property mapping (cross-cutting)
}
```

### Schema Variation Complexity

ClickGraph supports multiple schema variations for JOIN optimization, but CTE generation handles them via scattered conditional logic:

```rust
// 36+ files contain schema variation checks
if view_scan.is_denormalized && edge_context.is_some() {
    // Special case for denormalized nodes
    if let Some(EdgePosition::From) = edge_position {
        &view_scan.from_node_properties
    } else {
        &view_scan.to_node_properties
    }
} else {
    &view_scan.property_mapping
}
```

**Impact on GA Readiness**:
- **Performance**: CTE generation adds significant overhead for simple queries
- **Maintainability**: 8,261 lines of CTE logic scattered across files
- **Testing**: Complex interactions make comprehensive testing difficult
- **Debugging**: CTE-related bugs are hard to isolate and fix

---

## 2. Solution Overview

### Design Principles

1. **Strategy Pattern**: Different CTE generation strategies for each schema variation
2. **Unified Interface**: Single `CteManager` that handles all cases via exhaustive pattern matching
3. **Schema-Aware**: Leverages `PatternSchemaContext` for all schema decisions
4. **Immutable Context**: Thread-safe, testable CTE generation context
5. **Clear Separation**: Extraction → Planning → Generation pipeline

### Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CteManager    │    │  CteGenerator    │    │  CteExtractor   │
│                 │    │                  │    │                 │
│ • analyze()     │    │ • Strategy       │    │ • extract()     │
│ • generate()    │    │   Pattern        │    │ • plan()        │
│ • validate()    │    │ • Schema-Aware   │    │ • optimize()    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────────────┐
                    │ PatternSchemaContext │
                    │                     │
                    │ • JoinStrategy      │
                    │ • NodeAccessStrategy│
                    │ • EdgeAccessStrategy│
                    └────────────────────┘
```

### Key Innovation: Strategy Pattern with Schema Context

Instead of scattered conditionals, use **exhaustive pattern matching** on `JoinStrategy`:

```rust
// BEFORE: Scattered conditionals in 36+ files
if is_denormalized { /* complex logic */ }
else if is_fk_edge { /* different logic */ }
else { /* standard logic */ }

// AFTER: Single analysis point with strategy pattern
let strategy = cte_manager.analyze_pattern(&pattern_ctx, &vlp_spec)?;
match pattern_ctx.join_strategy {
    JoinStrategy::Traditional { .. } => TraditionalCteStrategy::new(&pattern_ctx),
    JoinStrategy::SingleTableScan { .. } => DenormalizedCteStrategy::new(&pattern_ctx),
    JoinStrategy::FkEdgeJoin { .. } => FkEdgeCteStrategy::new(&pattern_ctx),
    // ... exhaustive matching ensures no missed cases
}
```

---

## 3. Core Components

### 3.1 CteManager (Main Entry Point)

```rust
pub struct CteManager {
    schema: GraphSchema,
    context: CteGenerationContext,
}

impl CteManager {
    /// Analyze a variable-length pattern and determine CTE strategy
    pub fn analyze_pattern(
        &self,
        pattern_ctx: &PatternSchemaContext,
        vlp_spec: &VariableLengthSpec
    ) -> Result<CteStrategy, CteError> {
        // Use exhaustive pattern matching on JoinStrategy
        match pattern_ctx.join_strategy {
            JoinStrategy::Traditional { .. } =>
                Ok(CteStrategy::Traditional(TraditionalCteStrategy::new(pattern_ctx))),
            JoinStrategy::SingleTableScan { .. } =>
                Ok(CteStrategy::Denormalized(DenormalizedCteStrategy::new(pattern_ctx))),
            JoinStrategy::FkEdgeJoin { .. } =>
                Ok(CteStrategy::FkEdge(FkEdgeCteStrategy::new(pattern_ctx))),
            JoinStrategy::MixedAccess { joined_node, .. } =>
                Ok(CteStrategy::MixedAccess(MixedAccessCteStrategy::new(pattern_ctx, joined_node))),
            JoinStrategy::EdgeToEdge { .. } =>
                Ok(CteStrategy::EdgeToEdge(EdgeToEdgeCteStrategy::new(pattern_ctx))),
            JoinStrategy::CoupledSameRow { .. } =>
                Ok(CteStrategy::Coupled(CoupledCteStrategy::new(pattern_ctx))),
        }
    }

    /// Generate CTE SQL using the determined strategy
    pub fn generate_cte(
        &self,
        strategy: &CteStrategy,
        properties: &[NodeProperty],
        filters: &CategorizedFilters
    ) -> Result<CteContent, CteError> {
        strategy.generate_sql(&self.context, properties, filters)
    }

    /// Validate CTE strategy against schema constraints
    pub fn validate_strategy(
        &self,
        strategy: &CteStrategy,
        pattern_ctx: &PatternSchemaContext
    ) -> Result<(), CteError> {
        strategy.validate(pattern_ctx)
    }
}
```

### 3.2 CteStrategy (Strategy Pattern)

```rust
/// Strategy for generating CTEs based on schema variation
pub enum CteStrategy {
    Traditional(TraditionalCteStrategy),
    Denormalized(DenormalizedCteStrategy),
    FkEdge(FkEdgeCteStrategy),
    MixedAccess(MixedAccessCteStrategy),
    EdgeToEdge(EdgeToEdgeCteStrategy),
    Coupled(CoupledCteStrategy),
}

impl CteStrategy {
    /// Generate SQL for this CTE strategy
    pub fn generate_sql(
        &self,
        context: &CteGenerationContext,
        properties: &[NodeProperty],
        filters: &CategorizedFilters
    ) -> Result<CteContent, CteError> {
        match self {
            CteStrategy::Traditional(s) => s.generate_sql(context, properties, filters),
            CteStrategy::Denormalized(s) => s.generate_sql(context, properties, filters),
            CteStrategy::FkEdge(s) => s.generate_sql(context, properties, filters),
            CteStrategy::MixedAccess(s) => s.generate_sql(context, properties, filters),
            CteStrategy::EdgeToEdge(s) => s.generate_sql(context, properties, filters),
            CteStrategy::Coupled(s) => s.generate_sql(context, properties, filters),
        }
    }

    /// Validate this strategy against schema constraints
    pub fn validate(&self, pattern_ctx: &PatternSchemaContext) -> Result<(), CteError> {
        match self {
            CteStrategy::Traditional(s) => s.validate(pattern_ctx),
            // ... delegate validation to specific strategy
        }
    }
}
```

### 3.3 Strategy Implementations

Each strategy encapsulates the logic for its specific schema variation:

#### 3.3.1 TraditionalCteStrategy

**For separate node/edge tables requiring JOINs**

```rust
pub struct TraditionalCteStrategy {
    pattern_ctx: PatternSchemaContext,
    start_table: String,
    end_table: String,
    edge_table: String,
    start_id_col: String,
    end_id_col: String,
    edge_from_col: String,
    edge_to_col: String,
}

impl TraditionalCteStrategy {
    pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
        // Extract table/column info from NodeAccessStrategy::OwnTable and EdgeAccessStrategy::SeparateTable
        let start_table = match &pattern_ctx.left_node {
            NodeAccessStrategy::OwnTable { table, id_column, .. } => table.clone(),
            _ => return Err(CteError::InvalidStrategy("Traditional strategy requires OwnTable nodes".into())),
        };
        // ... extract other table/column mappings
        Ok(Self { pattern_ctx: pattern_ctx.clone(), start_table, /* ... */ })
    }

    pub fn generate_sql(&self, context: &CteGenerationContext, properties: &[NodeProperty], filters: &CategorizedFilters) -> Result<CteContent, CteError> {
        // Generate traditional recursive CTE with proper JOINs
        // Use pattern_ctx for all schema decisions
        let cte_sql = format!(r#"
            WITH RECURSIVE {cte_name} AS (
                SELECT {start_cols}, {end_cols}, {edge_cols}, 1 as depth, ARRAY[{start_id}, {end_id}] as path
                FROM {start_table} {start_alias}
                JOIN {edge_table} {edge_alias} ON {edge_alias}.{edge_from_col} = {start_alias}.{start_id_col}
                JOIN {end_table} {end_alias} ON {end_alias}.{end_id_col} = {edge_alias}.{edge_to_col}
                WHERE {start_conditions}

                UNION ALL

                SELECT {next_start_cols}, {next_end_cols}, {next_edge_cols}, prev.depth + 1, prev.path || {next_end_id}
                FROM {cte_name} prev
                JOIN {edge_table} {next_edge_alias} ON {next_edge_alias}.{edge_from_col} = prev.{end_id_col}
                JOIN {end_table} {next_end_alias} ON {next_end_alias}.{end_id_col} = {next_edge_alias}.{edge_to_col}
                WHERE prev.depth < ? AND {next_end_id} NOT IN prev.path {cycle_conditions}
            )
            SELECT * FROM {cte_name} WHERE depth >= ? AND depth <= ?
        "#, /* ... bindings */);

        Ok(CteContent { sql: cte_sql, parameters: vec![], /* ... */ })
    }
}
```

**Generated SQL Example:**
```sql
WITH RECURSIVE path_cte AS (
    SELECT u1.user_id, u1.name, u2.user_id, u2.name, r.follow_date, 1 as depth, ARRAY[u1.user_id, u2.user_id] as path
    FROM users u1
    JOIN follows r ON r.follower_id = u1.user_id
    JOIN users u2 ON u2.user_id = r.followed_id
    WHERE u1.user_id = ?

    UNION ALL

    SELECT u3.user_id, u3.name, u4.user_id, u4.name, r2.follow_date, prev.depth + 1, prev.path || u4.user_id
    FROM path_cte prev
    JOIN follows r2 ON r2.follower_id = prev.u2_user_id
    JOIN users u4 ON u4.user_id = r2.followed_id
    WHERE prev.depth < ? AND u4.user_id NOT IN prev.path
)
SELECT * FROM path_cte WHERE depth >= ? AND depth <= ?
```

#### 3.3.2 DenormalizedCteStrategy

**For single table patterns with embedded node properties**

```rust
pub struct DenormalizedCteStrategy {
    pattern_ctx: PatternSchemaContext,
    table: String,
    from_col: String,
    to_col: String,
}

impl DenormalizedCteStrategy {
    pub fn generate_sql(&self, context: &CteGenerationContext, properties: &[NodeProperty], filters: &CategorizedFilters) -> Result<CteContent, CteError> {
        // Generate simple recursive CTE without JOINs
        // Properties come from single table via NodeAccessStrategy::EmbeddedInEdge
        let cte_sql = format!(r#"
            WITH RECURSIVE {cte_name} AS (
                SELECT {from_props}, {to_props}, {edge_props}, 1 as depth, ARRAY[{from_id}, {to_id}] as path
                FROM {table} {alias}
                WHERE {from_conditions}

                UNION ALL

                SELECT next.{from_props}, next.{to_props}, next.{edge_props}, prev.depth + 1, prev.path || next.{to_id}
                FROM {cte_name} prev
                JOIN {table} next ON next.{from_col} = prev.{to_col}
                WHERE prev.depth < ? AND next.{to_id} NOT IN prev.path {cycle_conditions}
            )
            SELECT * FROM {cte_name} WHERE depth >= ? AND depth <= ?
        "#, /* ... bindings */);

        Ok(CteContent { sql: cte_sql, parameters: vec![], /* ... */ })
    }
}
```

**Generated SQL Example (OnTime Flights):**
```sql
WITH RECURSIVE path_cte AS (
    SELECT f1.Origin as from_city, f1.Dest as to_city, f1.FlightDate, 1 as depth, ARRAY[f1.Origin, f1.Dest] as path
    FROM flights f1
    WHERE f1.Origin = 'JFK'

    UNION ALL

    SELECT f2.Origin, f2.Dest, f2.FlightDate, prev.depth + 1, prev.path || f2.Dest
    FROM path_cte prev
    JOIN flights f2 ON f2.Origin = prev.to_city
    WHERE prev.depth < 5 AND f2.Dest NOT IN prev.path
)
SELECT * FROM path_cte WHERE depth >= 1 AND depth <= 3
```

#### 3.3.3 FkEdgeCteStrategy

**For relationships defined by foreign keys on node tables**

```rust
pub struct FkEdgeCteStrategy {
    pattern_ctx: PatternSchemaContext,
    node_table: String,
    fk_column: String,
    is_self_referencing: bool,
}

impl FkEdgeCteStrategy {
    pub fn generate_sql(&self, context: &CteGenerationContext, properties: &[NodeProperty], filters: &CategorizedFilters) -> Result<CteContent, CteError> {
        if self.is_self_referencing {
            // Self-referencing hierarchy (e.g., parent_id on objects table)
            let cte_sql = format!(r#"
                WITH RECURSIVE {cte_name} AS (
                    SELECT child.*, parent.*, 1 as depth, ARRAY[child.id, parent.id] as path
                    FROM {table} child
                    JOIN {table} parent ON parent.id = child.{fk_col}
                    WHERE child.id = ?

                    UNION ALL

                    SELECT grandchild.*, child_parent.*, prev.depth + 1, prev.path || grandchild.id
                    FROM {cte_name} prev
                    JOIN {table} grandchild ON grandchild.{fk_col} = prev.child_id
                    JOIN {table} child_parent ON child_parent.id = grandchild.{fk_col}
                    WHERE prev.depth < ? AND grandchild.id NOT IN prev.path
                )
                SELECT * FROM {cte_name} WHERE depth >= ? AND depth <= ?
            "#, /* ... */);
        } else {
            // FK to different table (e.g., orders.user_id → users.id)
            // ... different SQL generation
        }
        Ok(CteContent { sql: cte_sql, parameters: vec![], /* ... */ })
    }
}
```

### 3.4 Enhanced CteGenerationContext

Building on the existing immutable builder pattern:

```rust
#[derive(Debug, Clone)]
pub struct CteGenerationContext {
    // Existing fields...
    variable_length_properties: HashMap<String, Vec<NodeProperty>>,
    filter_expr: Option<RenderExpr>,
    start_cypher_alias: Option<String>,
    end_cypher_alias: Option<String>,
    schema: Option<GraphSchema>,
    fixed_length_joins: HashMap<String, (String, String, Vec<Join>)>,

    // New fields for unified system
    pattern_contexts: HashMap<String, PatternSchemaContext>, // Keyed by "left_alias-right_alias"
    cte_strategies: HashMap<String, CteStrategy>, // Cached strategies
}

impl CteGenerationContext {
    // Enhanced with pattern context storage
    pub fn with_pattern_context(mut self, left_alias: &str, right_alias: &str, ctx: PatternSchemaContext) -> Self {
        let key = format!("{}-{}", left_alias, right_alias);
        self.pattern_contexts.insert(key, ctx);
        self
    }

    pub fn get_pattern_context(&self, left_alias: &str, right_alias: &str) -> Option<&PatternSchemaContext> {
        let key = format!("{}-{}", left_alias, right_alias);
        self.pattern_contexts.get(&key)
    }

    // Cached strategy storage
    pub fn with_cte_strategy(mut self, left_alias: &str, right_alias: &str, strategy: CteStrategy) -> Self {
        let key = format!("{}-{}", left_alias, right_alias);
        self.cte_strategies.insert(key, strategy);
        self
    }

    pub fn get_cte_strategy(&self, left_alias: &str, right_alias: &str) -> Option<&CteStrategy> {
        let key = format!("{}-{}", left_alias, right_alias);
        self.cte_strategies.get(&key)
    }
}
```

---

## 4. CTE Generation Pipeline

```
1. Pattern Analysis → PatternSchemaContext
2. Strategy Selection → CteStrategy enum
3. Property Extraction → NodeProperty vec
4. Filter Categorization → CategorizedFilters
5. SQL Generation → CteContent
```

### 4.1 Pattern Analysis (GraphJoinInference - Already Implemented)

- Creates `PatternSchemaContext` with unified schema decisions
- **Prerequisite**: Phase 0 from schema consolidation (move GraphJoinInference early in analyzer pipeline)

### 4.2 Strategy Selection (CteManager::analyze_pattern)

```rust
pub fn analyze_pattern(
    &self,
    pattern_ctx: &PatternSchemaContext,
    vlp_spec: &VariableLengthSpec
) -> Result<CteStrategy, CteError> {
    match pattern_ctx.join_strategy {
        JoinStrategy::Traditional { .. } => {
            Ok(CteStrategy::Traditional(TraditionalCteStrategy::new(pattern_ctx)?))
        }
        JoinStrategy::SingleTableScan { .. } => {
            Ok(CteStrategy::Denormalized(DenormalizedCteStrategy::new(pattern_ctx)?))
        }
        JoinStrategy::FkEdgeJoin { join_side, is_self_referencing, .. } => {
            Ok(CteStrategy::FkEdge(FkEdgeCteStrategy::new(pattern_ctx, join_side, is_self_referencing)?))
        }
        JoinStrategy::MixedAccess { joined_node, .. } => {
            Ok(CteStrategy::MixedAccess(MixedAccessCteStrategy::new(pattern_ctx, joined_node)?))
        }
        JoinStrategy::EdgeToEdge { .. } => {
            Ok(CteStrategy::EdgeToEdge(EdgeToEdgeCteStrategy::new(pattern_ctx)?))
        }
        JoinStrategy::CoupledSameRow { .. } => {
            Ok(CteStrategy::Coupled(CoupledCteStrategy::new(pattern_ctx)?))
        }
    }
}
```

### 4.3 Property Extraction (Enhanced CteExtractor)

Replace scattered conditionals with unified property resolution:

```rust
// BEFORE: Scattered in 36+ files
let mapping = if view_scan.is_denormalized && edge_context.is_some() {
    if let Some(EdgePosition::From) = edge_position {
        &view_scan.from_node_properties
    } else {
        &view_scan.to_node_properties
    }
} else {
    &view_scan.property_mapping
};

// AFTER: Unified via PatternSchemaContext
let mapping = pattern_ctx.left_node.get_property_column(prop_name)
    .or_else(|| pattern_ctx.right_node.get_property_column(prop_name))
    .or_else(|| pattern_ctx.edge.get_property_column(prop_name));
```

### 4.4 Filter Categorization (Existing - Enhanced)

The existing `CategorizedFilters` separates node/edge/relationship filters. Enhance to use `PatternSchemaContext`:

```rust
pub struct CategorizedFilters {
    pub start_node_filters: Vec<RenderExpr>,
    pub end_node_filters: Vec<RenderExpr>,
    pub relationship_filters: Vec<RenderExpr>,
    pub path_filters: Vec<RenderExpr>, // New: filters on path variables
}

impl CategorizedFilters {
    /// Generate SQL WHERE clauses using pattern context
    pub fn to_sql(&self, pattern_ctx: &PatternSchemaContext, alias_mapping: &HashMap<String, String>) -> Result<String, CteError> {
        let mut conditions = Vec::new();

        // Start node filters
        for filter in &self.start_node_filters {
            let sql = filter.to_sql_with_context(pattern_ctx, NodePosition::Left, alias_mapping)?;
            conditions.push(format!("({})", sql));
        }

        // End node filters
        for filter in &self.end_node_filters {
            let sql = filter.to_sql_with_context(pattern_ctx, NodePosition::Right, alias_mapping)?;
            conditions.push(format!("({})", sql));
        }

        // Relationship filters
        for filter in &self.relationship_filters {
            let sql = filter.to_sql_with_context(pattern_ctx, alias_mapping)?;
            conditions.push(format!("({})", sql));
        }

        Ok(conditions.join(" AND "))
    }
}
```

### 4.5 SQL Generation (Strategy-specific)

Each strategy generates appropriate recursive CTE SQL based on its schema pattern.

---

## 5. Schema Variation Coverage

### 5.1 Traditional Separate Tables
- **Pattern**: `users` ← `follows` → `users`
- **Strategy**: `TraditionalCteStrategy`
- **SQL**: Standard recursive CTE with 2 JOINs per level

### 5.2 Denormalized Single Table
- **Pattern**: `flights` (Origin → Dest in same table)
- **Strategy**: `DenormalizedCteStrategy`
- **SQL**: Simple recursive CTE with 1 JOIN per level

### 5.3 FK-Edge Pattern
- **Pattern**: `objects.parent_id` → `objects.id`
- **Strategy**: `FkEdgeCteStrategy`
- **SQL**: Self-JOIN recursive CTE

### 5.4 Mixed Access Pattern
- **Pattern**: One node embedded, one requires JOIN
- **Strategy**: `MixedAccessCteStrategy`
- **SQL**: Partial JOIN recursive CTE

### 5.5 Edge-to-Edge Pattern
- **Pattern**: Multi-hop denormalized (flight connections)
- **Strategy**: `EdgeToEdgeCteStrategy`
- **SQL**: Edge-to-edge JOIN recursive CTE

### 5.6 Coupled Pattern
- **Pattern**: Multiple relationship types in same row
- **Strategy**: `CoupledCteStrategy`
- **SQL**: Unified row access recursive CTE

---

## 6. Integration Points

### 6.1 With Existing Codebase

#### CteGenerationContext
- Enhanced with immutable builder pattern (already started)
- Add pattern context and strategy caching

#### CteExtractor
- Refactored to use `PatternSchemaContext` instead of scattered checks
- Unified property resolution via `NodeAccessStrategy::get_property_column()`

#### VariableLengthCteGenerator
- Split into strategy-specific generators
- Legacy constructor kept for backward compatibility during migration

### 6.2 With Schema Consolidation

- **Depends on Phase 0**: Move `GraphJoinInference` early in analyzer pipeline
- **Uses PatternSchemaContext** as single source of truth
- **Eliminates 36+ files** with `is_denormalized` checks

### 6.3 With Multi-Schema Support

- `CteManager` takes `GraphSchema` parameter
- Strategies use schema-aware property resolution
- Thread-safe via immutable context

### 6.4 WITH Clause Support

Extend for non-VLP CTEs (aggregations, subqueries):

```rust
pub enum CteType {
    VariableLengthPath { spec: VariableLengthSpec },
    Aggregation { group_by: Vec<String>, aggregates: Vec<AggregateExpr> },
    Subquery { alias: String },
}

impl CteManager {
    pub fn analyze_cte(&self, cte_type: CteType, pattern_ctx: &PatternSchemaContext) -> Result<CteStrategy, CteError> {
        match cte_type {
            CteType::VariableLengthPath { spec } => self.analyze_vlp(pattern_ctx, &spec),
            CteType::Aggregation { .. } => Ok(CteStrategy::Aggregation(AggregationCteStrategy::new(pattern_ctx))),
            CteType::Subquery { .. } => Ok(CteStrategy::Subquery(SubqueryCteStrategy::new(pattern_ctx))),
        }
    }
}
```

---

## 7. Implementation Roadmap

### Phase 1: Core Infrastructure (2 weeks)
1. **Create CteManager struct** with basic analyze/generate methods
2. **Implement CteStrategy enum** with placeholder strategies
3. **Create base strategy trait** with generate_sql method
4. **Add CteError type** for unified error handling
5. **Create CteContent struct** for SQL output

### Phase 2: Traditional Strategy (2 weeks)
1. **Implement TraditionalCteStrategy** for separate node/edge tables
2. **Migrate existing traditional CTE logic** from variable_length_cte.rs
3. **Update CteExtractor** to use PatternSchemaContext for property resolution
4. **Add comprehensive tests** for traditional patterns
5. **Update integration tests** to use new interface

### Phase 3: Denormalized Strategy (2 weeks)
1. **Implement DenormalizedCteStrategy** for single-table patterns
2. **Migrate denormalized logic** from scattered conditional checks
3. **Handle embedded node properties** via NodeAccessStrategy
4. **Test with benchmark denormalized schemas** (OnTime flights)
5. **Validate performance** vs old implementation

### Phase 4: Advanced Strategies (3 weeks)
1. **Implement FkEdgeCteStrategy** for FK-based relationships
2. **Implement PolymorphicCteStrategy** for type-discriminated edges
3. **Implement CoupledCteStrategy** for multi-relationship patterns
4. **Implement MixedAccessCteStrategy** for partial JOIN patterns
5. **Add shortest path optimizations** to relevant strategies
6. **Comprehensive testing** across all schema variations

### Phase 5: Migration & Cleanup (2 weeks)
1. **Replace scattered conditionals** with strategy pattern calls
2. **Remove deprecated CTE functions** from old files
3. **Update all callers** to use new CteManager interface
4. **Performance testing** and optimization
5. **Documentation updates**

### Phase 6: WITH Clause Extension (1 week)
1. **Add non-VLP CTE strategies** (Aggregation, Subquery)
2. **Integrate with existing WITH clause parsing**
3. **Test complex queries** with multiple CTE types

**Total Timeline**: 11 weeks

---

## 8. Testing Strategy

### 8.1 Unit Testing
- **Each strategy**: Independent unit tests with mock PatternSchemaContext
- **Property resolution**: Test all NodeAccessStrategy/EdgeAccessStrategy combinations
- **SQL generation**: Validate generated SQL syntax and parameter binding

### 8.2 Integration Testing
- **Schema variations**: Test all supported patterns with benchmark data
- **Query complexity**: VLP with filters, properties, shortest paths
- **Multi-schema**: Test with different GraphSchema configurations

### 8.3 Regression Testing
- **Existing tests**: All 760+ tests must pass
- **Performance benchmarks**: No >5% regression on CTE queries
- **Memory usage**: Monitor for increased allocations

### 8.4 Edge Case Testing
- **Empty results**: Queries with no matching paths
- **Cycle detection**: Self-referencing and circular patterns
- **Depth limits**: Very deep vs shallow path queries
- **Property conflicts**: Same property name on node vs edge

---

## 9. Risk Mitigation

### 9.1 Incremental Rollout
- **Feature flags** to toggle between old/new implementations
- **Gradual migration** of query patterns
- **Rollback capability** if issues discovered

### 9.2 Performance Validation
- **Benchmark suite** comparing old vs new implementations
- **Memory profiling** for large CTE generations
- **Query performance regression** testing

### 9.3 Error Handling
- **CteError enum** with specific error types for each strategy
- **Validation methods** to catch configuration errors early
- **Graceful degradation** for unsupported patterns

### 9.4 Backward Compatibility
- **Legacy constructors** kept during migration period
- **Deprecation warnings** for old APIs
- **Migration guide** for external users

---

## 10. Success Metrics

### Code Quality
- **-60% lines of code**: Eliminate scattered conditionals and duplicated logic
- **0 scattered conditionals**: All `is_denormalized`, `from_node_properties` checks eliminated
- **Exhaustive pattern matching**: Compile-time guarantees for schema variation coverage

### Functionality
- **All schema variations supported**: 6 strategy implementations covering all patterns
- **All existing functionality preserved**: 760+ tests passing
- **New WITH clause support**: Aggregation and subquery CTEs

### Performance
- **Same or better performance**: Strategies optimize for their specific patterns
- **Reduced overhead**: No runtime conditionals in hot paths
- **Better caching**: Schema analysis done once per pattern

### Maintainability
- **Clear separation of concerns**: Extraction, planning, generation cleanly separated
- **Easy extension**: New schema variations = new strategy implementation
- **Comprehensive testing**: Each strategy tested independently

---

## 11. Dependencies

### Prerequisites
- **Schema Consolidation Phase 0**: Move GraphJoinInference early in analyzer pipeline
- **PatternSchemaContext**: Unified schema abstraction available
- **Immutable CteGenerationContext**: Builder pattern implemented

### Team Requirements
- **Rust expertise**: Deep understanding of pattern matching and traits
- **Graph query knowledge**: Understanding of Cypher semantics and CTE generation
- **Schema variation expertise**: Knowledge of all supported patterns
- **Testing discipline**: Comprehensive testing of complex query patterns

---

## 12. Conclusion

This design provides a solid foundation for unifying the CTE system while maintaining all existing functionality and enabling future schema variations to be added cleanly. The strategy pattern ensures each schema variation is handled optimally while the unified interface simplifies the overall architecture.

**Key Benefits**:
- **Maintainability**: Clear separation of concerns and single source of truth
- **Extensibility**: Easy to add new schema variations
- **Testability**: Each strategy can be tested independently
- **Performance**: Optimized SQL generation for each pattern
- **Correctness**: Exhaustive pattern matching prevents missed cases

**Next Steps**:
1. Review and approve this design document
2. Begin Phase 1 implementation (core infrastructure)
3. Set up comprehensive testing framework
4. Plan migration from existing codebase

---

## Appendix A: Current CTE Architecture Analysis

### File Breakdown
- `cte_extraction.rs` (4,256 lines): Main CTE extraction logic with complex branching for schema variations
- `variable_length_cte.rs` (3,244 lines): SQL generation with 40+ parameters for different patterns
- `cte_generation.rs` (761 lines): Context management and property analysis

### Complexity Issues
1. **Scattered Conditionals**: 36+ files with `is_denormalized` checks
2. **Large Parameter Lists**: `VariableLengthCteGenerator::new_with_polymorphic()` has 40+ parameters
3. **Cross-cutting Concerns**: Property resolution, JOIN generation, and CTE logic intermixed
4. **Hard to Test**: Complex interactions between components
5. **Hard to Extend**: Adding new schema variations requires touching multiple files

### Migration Approach
1. **Keep old code** during transition with feature flags
2. **Gradual replacement** of functionality
3. **Comprehensive testing** at each phase
4. **Performance monitoring** throughout

---

## Appendix B: Strategy Pattern Details

### Strategy Selection Logic
```rust
match pattern_ctx.join_strategy {
    JoinStrategy::Traditional { .. } => {
        // Requires: NodeAccessStrategy::OwnTable for both nodes
        // Requires: EdgeAccessStrategy::SeparateTable
        // Generates: Standard recursive CTE with node-edge-node JOINs
        TraditionalCteStrategy::new(pattern_ctx)
    }
    JoinStrategy::SingleTableScan { .. } => {
        // Requires: NodeAccessStrategy::EmbeddedInEdge for both nodes
        // Generates: Simple recursive CTE with single table JOINs
        DenormalizedCteStrategy::new(pattern_ctx)
    }
    // ... other strategies
}
```

### Strategy Responsibilities
Each strategy encapsulates:
- **Schema validation**: Ensure pattern context matches strategy requirements
- **Table/column mapping**: Extract relevant tables and columns from PatternSchemaContext
- **SQL generation**: Create optimized recursive CTE for the specific pattern
- **Property handling**: Map Cypher properties to SQL columns using NodeAccessStrategy/EdgeAccessStrategy
- **Filter application**: Apply categorized filters appropriately for the schema

### Error Handling
```rust
#[derive(Debug, thiserror::Error)]
pub enum CteError {
    #[error("Invalid strategy for pattern: {0}")]
    InvalidStrategy(String),
    #[error("Missing required table mapping: {0}")]
    MissingTableMapping(String),
    #[error("Unsupported property access: {0}")]
    UnsupportedPropertyAccess(String),
    #[error("SQL generation failed: {0}")]
    SqlGenerationError(String),
    #[error("Schema validation failed: {0}")]
    SchemaValidationError(String),
}
```

---

## Appendix C: Performance Considerations

### Optimization Opportunities
1. **Strategy caching**: Cache analyzed strategies in CteGenerationContext
2. **SQL template reuse**: Pre-compile common SQL patterns
3. **Lazy property resolution**: Only resolve properties actually used in query
4. **Memory-efficient CTEs**: Optimize column selection for large result sets

### Performance Benchmarks
- **Baseline**: Current implementation performance
- **Target**: No >5% regression
- **Metrics**: Query execution time, memory usage, SQL generation time

### Memory Usage
- **PatternSchemaContext**: Computed once per pattern, reused
- **CteStrategy**: Lightweight enum, minimal memory overhead
- **Generated SQL**: Similar size to current implementation

---

## Appendix D: Future Extensions

### Additional CTE Types
- **Shortest Path CTEs**: Specialized strategies for shortest path algorithms
- **Graph Algorithm CTEs**: PageRank, centrality measures
- **Temporal CTEs**: Time-windowed path queries

### Advanced Schema Variations
- **Multi-table nodes**: Node properties split across multiple tables
- **Computed properties**: Properties derived from expressions
- **Dynamic schemas**: Runtime schema discovery and adaptation

### Query Optimizations
- **CTE reuse**: Share CTEs across multiple query parts
- **Incremental evaluation**: Reuse previous CTE results
- **Parallel execution**: Multi-threaded CTE generation for complex queries</content>
<parameter name="filePath">/home/gz/clickgraph/docs/development/cte_unification_design.md