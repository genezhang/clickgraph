# CTE Manager Design Document

**Date**: January 14, 2026  
**Status**: Design Proposal - Awaiting Review  
**Related**: `docs/development/code-quality-analysis.md`, `notes/schema_consolidation_analysis.md`

---

## Executive Summary

This document proposes a comprehensive redesign of ClickGraph's CTE (Common Table Expression) generation system through introduction of a **CTE Manager** abstraction. The current CTE architecture spans **8,261 lines across 3 files** with severe cross-cutting concerns, making it the most critical code quality issue blocking GA readiness.

**Key Proposal**: Create `CteManager` as a unified facade for CTE lifecycle management, eliminating scattered boolean flags (`is_denormalized`, `is_fk_edge`) in favor of pattern-based routing via `PatternSchemaContext`.

**Timeline Estimate**: 6-8 weeks with proper risk mitigation
**Success Probability**: 70% (higher than previous attempts due to phased approach)

---

## Part 1: Current Architecture Analysis

### 1.1 File Structure & Responsibilities

| File | Lines | Primary Responsibility | Complexity |
|------|-------|------------------------|------------|
| **cte_extraction.rs** | 4,342 | CTE extraction from logical plan, property analysis | HIGH |
| **variable_length_cte.rs** | 3,245 | Recursive CTE SQL generation | VERY HIGH |
| **cte_generation.rs** | 761 | CTE context & metadata management | MEDIUM |
| **TOTAL** | **8,348** | Complete CTE lifecycle | **EXTREME** |

### 1.2 Current CTE Lifecycle Flow

```
Query → Analyzer → Plan Builder → CTE Extraction → CTE Generation → SQL
         ↓                            ↓                  ↓
   PatternSchemaContext      extract_ctes_with_context   VariableLengthCteGenerator
   (created & stored)        (recreates context)         (3 constructors w/ flags)
```

**Problem**: Information flows forward but is **not accessible** in later phases, forcing recreation and flag-based logic.

### 1.3 VariableLengthCteGenerator Constructor Explosion

Current implementation has **4 constructor functions** for different schema patterns:

```rust
impl<'a> VariableLengthCteGenerator<'a> {
    // Constructor 1: Standard pattern (3-way JOIN)
    pub fn new(...) -> Self { /* 15 parameters */ }
    
    // Constructor 2: Fully denormalized (single table scan)
    pub fn new_denormalized(...) -> Self { 
        /* 15 parameters + hardcoded is_denormalized=true flags */
    }
    
    // Constructor 3: Mixed denormalized (hybrid)
    pub fn new_mixed(...) -> Self { 
        /* 20 parameters + start_is_denormalized, end_is_denormalized flags */
    }
    
    // Constructor 4: FK-edge pattern (self-referencing)
    pub fn new_with_fk_edge(...) -> Self { 
        /* 25 parameters + is_fk_edge flag + polymorphic fields */
    }
}
```

**Problems**:
1. **Parameter explosion**: Up to 25 parameters per constructor
2. **Boolean flag duplication**: Same flags (`is_denormalized`, `is_fk_edge`) passed repeatedly
3. **No centralized routing**: Caller must know which constructor to use
4. **Hard to maintain**: Adding new schema pattern requires new constructor
5. **Testing complexity**: Must test all constructor combinations

### 1.4 Scattered Decision Logic

CTE generation decisions are scattered across multiple locations:

**Location 1: `cte_extraction.rs` (~line 2120)**
```rust
// Decision: Which generator constructor to call?
let both_denormalized = matches!(pattern_ctx.join_strategy, JoinStrategy::SingleTableScan { .. });
let is_mixed = matches!(pattern_ctx.join_strategy, JoinStrategy::MixedAccess { .. });
let is_fk_edge = matches!(pattern_ctx.join_strategy, JoinStrategy::FkEdgeJoin { .. });

// Then: Extract individual flags from context (compatibility layer)
let start_is_denormalized = pattern_ctx.left_node.is_embedded();
let end_is_denormalized = pattern_ctx.right_node.is_embedded();

// Finally: Call appropriate constructor with flags
if both_denormalized {
    VariableLengthCteGenerator::new_denormalized(...)
} else if is_mixed {
    VariableLengthCteGenerator::new_mixed(..., start_is_denormalized, end_is_denormalized)
} else {
    VariableLengthCteGenerator::new_with_fk_edge(..., is_fk_edge, ...)
}
```

**Location 2: `variable_length_cte.rs` (internal SQL generation)**
```rust
// Decision: How to generate base case SQL?
if self.is_denormalized {
    // Single table scan logic
} else if self.is_fk_edge {
    // Self-join logic
} else if self.start_is_denormalized || self.end_is_denormalized {
    // Mixed logic
} else {
    // Traditional 3-way JOIN logic
}
```

**Result**: Same decision logic **duplicated across 2 layers** with different mechanisms (pattern matching vs boolean checks).

### 1.5 The PatternSchemaContext Gap

From Phase 0 of schema consolidation, we have `PatternSchemaContext` available in the analyzer:

```rust
pub struct PatternSchemaContext {
    pub left_node: NodeAccessStrategy,    // How to access left node
    pub right_node: NodeAccessStrategy,   // How to access right node
    pub edge: EdgeAccessStrategy,         // How to access edge
    pub join_strategy: JoinStrategy,      // How to generate JOINs
    // ... other fields
}
```

**Gap**: This context is **created during analysis** but **not available during rendering**. CTE extraction must **recreate** it, leading to:
- Code duplication
- Potential inconsistencies
- Lost optimization opportunities

### 1.6 Multi-Type VLP Complexity

Multi-type variable-length paths (e.g., `(u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)`) add another dimension:

**Current approach**:
```rust
// Separate code path in cte_extraction.rs
if should_use_join_expansion(&graph_rel, &rel_types, schema) {
    // Use MultiTypeVlpJoinGenerator (different generator!)
    let generator = MultiTypeVlpJoinGenerator::new(...);
    // Generate UNION ALL instead of recursive CTE
} else {
    // Use VariableLengthCteGenerator
}
```

**Problem**: Two completely different generators with no common interface.

---

## Part 2: Root Cause Analysis

### 2.1 Primary Problems

**Problem 1: Information Loss Across Phases**
- Analyzer creates `PatternSchemaContext`
- Render phase doesn't have access
- Must recreate or use boolean flags

**Problem 2: Constructor Explosion**
- 4 constructors for schema patterns
- 15-25 parameters each
- Boolean flags scattered throughout

**Problem 3: No Unified Entry Point**
- Callers must know internal schema details
- Decision logic duplicated
- Hard to test systematically

**Problem 4: Tight Coupling**
- CTE extraction directly calls generators
- Generators hardcode SQL generation logic
- No abstraction layer for variations

### 2.2 Why Previous Refactoring Attempts Failed

From code-quality-analysis.md:
> "Previous Attempts: Multiple refactoring efforts have increased complexity"

**Analysis of failures**:

1. **Big-bang refactoring**: Tried to change too much at once, no rollback
2. **No phased approach**: Didn't validate each step before proceeding
3. **Added layers without removing old**: Created compatibility wrappers that stayed permanent
4. **No clear success criteria**: Unclear when refactoring was "done"
5. **Insufficient testing**: Regressions discovered after merge

**Recent example** (January 14, 2026):
- Added `recreate_pattern_schema_context()` helper
- Added `EdgeAccessStrategy` methods
- But still passes boolean flags to generators
- Result: Another compatibility layer, not a simplification

### 2.3 Core Design Tension

There's a fundamental tension in the architecture:

```
┌─────────────────────┐
│  Analyzer Phase     │  ← Creates PatternSchemaContext (knows schema patterns)
├─────────────────────┤
│  Logical Plan       │  ← Stores plan tree (loses context)
├─────────────────────┤
│  Render Phase       │  ← Needs schema decisions (recreates or uses flags)
└─────────────────────┘
```

**Current "solution"**: Pass boolean flags through logical plan or recreate context in render phase.

**Better solution**: Pass `PatternSchemaContext` reference through OR use CTE Manager that accesses stored context.

---

## Part 3: Design Alternatives

### Alternative 1: CTE Manager with Context Storage ⭐ RECOMMENDED

**Concept**: Create `CteManager` that stores `PatternSchemaContext` references and provides unified CTE generation API.

```rust
pub struct CteManager<'a> {
    schema: &'a GraphSchema,
    // Map: rel_alias → PatternSchemaContext (from analyzer)
    pattern_contexts: HashMap<String, Arc<PatternSchemaContext>>,
}

impl<'a> CteManager<'a> {
    /// Main entry point for VLP CTE generation
    pub fn generate_vlp_cte(
        &self,
        graph_rel: &GraphRel,
        spec: &VariableLengthSpec,
        context: &CteGenerationContext,
    ) -> Result<Vec<Cte>, RenderBuildError> {
        // 1. Get or recreate PatternSchemaContext for this relationship
        let pattern_ctx = self.get_or_create_pattern_context(graph_rel)?;
        
        // 2. Route to appropriate generator based on JoinStrategy
        match pattern_ctx.join_strategy {
            JoinStrategy::SingleTableScan { .. } => {
                self.generate_denormalized_vlp(graph_rel, spec, pattern_ctx, context)
            }
            JoinStrategy::MixedAccess { .. } => {
                self.generate_mixed_vlp(graph_rel, spec, pattern_ctx, context)
            }
            JoinStrategy::Traditional { .. } => {
                self.generate_traditional_vlp(graph_rel, spec, pattern_ctx, context)
            }
            JoinStrategy::FkEdgeJoin { .. } => {
                self.generate_fk_edge_vlp(graph_rel, spec, pattern_ctx, context)
            }
            _ => Err(RenderBuildError::UnsupportedFeature(
                format!("Unsupported JOIN strategy for VLP: {:?}", pattern_ctx.join_strategy)
            ))
        }
    }
    
    /// Get pattern context from storage or recreate it
    fn get_or_create_pattern_context(
        &self,
        graph_rel: &GraphRel,
    ) -> Result<Arc<PatternSchemaContext>, RenderBuildError> {
        if let Some(ctx) = self.pattern_contexts.get(&graph_rel.alias) {
            Ok(Arc::clone(ctx))
        } else {
            // Fallback: recreate (uses existing helper from Phase 2)
            let ctx = recreate_pattern_schema_context(graph_rel, self.schema)?;
            Ok(Arc::new(ctx))
        }
    }
}
```

**Pros**:
- ✅ Single entry point for all VLP CTE generation
- ✅ Eliminates boolean flags - uses pattern matching instead
- ✅ Can access analyzer-created contexts (if plumbed through)
- ✅ Easy to test - mock `CteManager` in tests
- ✅ Extensible - new patterns add new match arm
- ✅ Clear migration path - can coexist with old code

**Cons**:
- ⚠️ Requires plumbing `PatternSchemaContext` from analyzer to render (or storing in plan)
- ⚠️ Performance overhead of Arc/HashMap lookup (likely negligible)
- ⚠️ Need to decide: store in plan vs pass through vs recreate

**Complexity**: Medium - requires careful coordination between phases

---

### Alternative 2: Refactor Generator Constructors (Minimal Change)

**Concept**: Keep current architecture but consolidate constructors.

```rust
pub enum VlpSchemaPattern {
    Traditional { start_table: String, end_table: String, ... },
    Denormalized { edge_table: String, ... },
    Mixed { /* ... */ },
    FkEdge { /* ... */ },
}

impl<'a> VariableLengthCteGenerator<'a> {
    /// Single constructor taking schema pattern enum
    pub fn new_from_pattern(
        schema: &'a GraphSchema,
        spec: VariableLengthSpec,
        pattern: VlpSchemaPattern,
        filters: VlpFilters,
        properties: Vec<NodeProperty>,
    ) -> Self {
        match pattern {
            VlpSchemaPattern::Traditional { .. } => { /* construct */ },
            VlpSchemaPattern::Denormalized { .. } => { /* construct */ },
            // ...
        }
    }
}
```

**Pros**:
- ✅ Simpler than CTE Manager - fewer moving parts
- ✅ Reduces from 4 constructors to 1
- ✅ Still eliminates boolean flags
- ✅ Easier migration - change caller sites incrementally

**Cons**:
- ❌ Doesn't solve routing problem - caller still decides pattern
- ❌ Doesn't provide centralized testing point
- ❌ Doesn't address multi-type VLP divergence
- ❌ Still tight coupling between extraction and generation

**Complexity**: Low - mostly mechanical refactoring

---

### Alternative 3: Pattern-Based Generator Registry (Extensibility Focus)

**Concept**: Registry pattern for generator selection.

```rust
pub trait VlpGenerator {
    fn supports(&self, pattern_ctx: &PatternSchemaContext) -> bool;
    fn generate(&self, ...) -> Result<Cte, Error>;
}

pub struct CteManager<'a> {
    schema: &'a GraphSchema,
    generators: Vec<Box<dyn VlpGenerator>>,
}

impl<'a> CteManager<'a> {
    pub fn generate_vlp_cte(&self, pattern_ctx: &PatternSchemaContext, ...) -> Result<Cte, Error> {
        // Find first generator that supports this pattern
        let generator = self.generators.iter()
            .find(|g| g.supports(pattern_ctx))
            .ok_or(Error::NoSuitableGenerator)?;
        
        generator.generate(...)
    }
}
```

**Pros**:
- ✅ Highly extensible - register new generators easily
- ✅ Testable - test generators in isolation
- ✅ Pluggable - swap implementations without changing manager

**Cons**:
- ❌ Over-engineered for current needs (4 patterns, not 20)
- ❌ Runtime dispatch overhead
- ❌ Harder to understand - indirection through trait
- ❌ More code to maintain (trait + implementations)

**Complexity**: High - introduces significant architectural change

---

### Alternative 4: Keep Current Architecture (Status Quo)

**Concept**: Accept current complexity as acceptable trade-off for functionality.

**Pros**:
- ✅ No migration risk
- ✅ Battle-tested code
- ✅ Familiar to current developers

**Cons**:
- ❌ Complexity continues to grow with new features
- ❌ Hard to test comprehensively
- ❌ New developers struggle to understand
- ❌ Blocks GA due to maintainability concerns

**Complexity**: N/A - no change

---

## Part 4: Recommended Design (Alternative 1)

### 4.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Query Processing                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Analyzer Phase                             │
│  - Creates PatternSchemaContext for each GraphRel            │
│  - Stores in PlanCtx (already exists from Phase 0)           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Render Phase                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              CteManager (NEW)                         │  │
│  │  - Accesses PatternSchemaContext from PlanCtx        │  │
│  │  - Routes to appropriate generator                   │  │
│  │  - Eliminates boolean flag passing                   │  │
│  └───────────────────────────────────────────────────────┘  │
│                              │                                │
│              ┌───────────────┴───────────────┐              │
│              ▼                               ▼              │
│  ┌─────────────────────┐      ┌──────────────────────────┐ │
│  │ VLP Generator       │      │ Multi-Type VLP Generator │ │
│  │ (refactored)        │      │ (future: unified)        │ │
│  └─────────────────────┘      └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Detailed Interface Design

#### CteManager Core Structure

```rust
/// CTE Manager - Unified facade for CTE lifecycle management
pub struct CteManager<'a> {
    /// Graph schema for this query
    schema: &'a GraphSchema,
    
    /// Pattern contexts from analyzer (optional - for optimization)
    pattern_contexts: Option<&'a HashMap<String, Arc<PatternSchemaContext>>>,
    
    /// CTE generation context for property requirements
    cte_context: &'a CteGenerationContext,
}

impl<'a> CteManager<'a> {
    /// Create new CTE manager with schema
    pub fn new(schema: &'a GraphSchema, cte_context: &'a CteGenerationContext) -> Self {
        Self {
            schema,
            pattern_contexts: None,
            cte_context,
        }
    }
    
    /// Create with analyzer-provided pattern contexts (optimization)
    pub fn with_pattern_contexts(
        schema: &'a GraphSchema,
        cte_context: &'a CteGenerationContext,
        contexts: &'a HashMap<String, Arc<PatternSchemaContext>>,
    ) -> Self {
        Self {
            schema,
            pattern_contexts: Some(contexts),
            cte_context,
        }
    }
    
    /// Main entry point: Generate VLP CTE for a graph relationship
    pub fn generate_vlp_cte(
        &self,
        graph_rel: &GraphRel,
        spec: &VariableLengthSpec,
    ) -> Result<Vec<Cte>, RenderBuildError> {
        // 1. Get pattern context (from storage or recreate)
        let pattern_ctx = self.get_pattern_context(graph_rel)?;
        
        // 2. Check for multi-type VLP (special case for now)
        if self.is_multi_type_vlp(graph_rel, &pattern_ctx) {
            return self.generate_multi_type_vlp(graph_rel, spec, &pattern_ctx);
        }
        
        // 3. Route based on JOIN strategy
        match &pattern_ctx.join_strategy {
            JoinStrategy::SingleTableScan { table } => {
                self.generate_single_table_vlp(
                    graph_rel,
                    spec,
                    &pattern_ctx,
                    table,
                )
            }
            JoinStrategy::MixedAccess { joined_node, join_col } => {
                self.generate_mixed_vlp(
                    graph_rel,
                    spec,
                    &pattern_ctx,
                    *joined_node,
                    join_col,
                )
            }
            JoinStrategy::Traditional { left_join_col, right_join_col } => {
                self.generate_traditional_vlp(
                    graph_rel,
                    spec,
                    &pattern_ctx,
                    left_join_col,
                    right_join_col,
                )
            }
            JoinStrategy::FkEdgeJoin { node_table, fk_col, target_col } => {
                self.generate_fk_edge_vlp(
                    graph_rel,
                    spec,
                    &pattern_ctx,
                    node_table,
                    fk_col,
                    target_col,
                )
            }
            JoinStrategy::EdgeToEdge { .. } | 
            JoinStrategy::CoupledSameRow { .. } => {
                Err(RenderBuildError::UnsupportedFeature(
                    format!("VLP not supported for JOIN strategy: {:?}", pattern_ctx.join_strategy)
                ))
            }
        }
    }
    
    // Internal helper methods...
    fn get_pattern_context(
        &self,
        graph_rel: &GraphRel,
    ) -> Result<Arc<PatternSchemaContext>, RenderBuildError> {
        // Try to get from stored contexts first (fast path)
        if let Some(contexts) = self.pattern_contexts {
            if let Some(ctx) = contexts.get(&graph_rel.alias) {
                return Ok(Arc::clone(ctx));
            }
        }
        
        // Fallback: recreate (uses helper from Phase 2)
        let ctx = recreate_pattern_schema_context(graph_rel, self.schema)?;
        Ok(Arc::new(ctx))
    }
    
    fn is_multi_type_vlp(
        &self,
        graph_rel: &GraphRel,
        pattern_ctx: &PatternSchemaContext,
    ) -> bool {
        // Multi-type VLP detection logic (existing code)
        should_use_join_expansion(graph_rel, &pattern_ctx.rel_types, self.schema)
    }
}
```

#### Generator Method Signatures

```rust
impl<'a> CteManager<'a> {
    /// Generate CTE for fully denormalized pattern (single table scan)
    fn generate_single_table_vlp(
        &self,
        graph_rel: &GraphRel,
        spec: &VariableLengthSpec,
        pattern_ctx: &PatternSchemaContext,
        table: &str,
    ) -> Result<Vec<Cte>, RenderBuildError> {
        // Extract required info from pattern_ctx
        let from_col = pattern_ctx.edge.from_id_column();
        let to_col = pattern_ctx.edge.to_id_column();
        let properties = self.extract_properties(graph_rel, pattern_ctx);
        let filters = self.extract_filters(graph_rel, pattern_ctx);
        
        // Create generator with ONLY necessary parameters
        let mut generator = VariableLengthCteGenerator::new_denormalized(
            self.schema,
            spec.clone(),
            table,
            from_col,
            to_col,
            &graph_rel.left_connection,
            &graph_rel.right_connection,
            &graph_rel.alias,
            properties,
            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
            filters.start_node_filters,
            filters.end_node_filters,
            filters.relationship_filters,
            graph_rel.path_variable.clone(),
            Some(pattern_ctx.rel_types.clone()),
            pattern_ctx.edge.edge_id().cloned(),
        );
        
        // Generate SQL
        let cte_name = format!("vlp_{}_{}", graph_rel.left_connection, graph_rel.right_connection);
        let sql = generator.generate_cte_sql(&cte_name)?;
        
        Ok(vec![Cte::new(cte_name, CteContent::RawSql(sql), true)])
    }
    
    /// Generate CTE for traditional pattern (3-way JOIN)
    fn generate_traditional_vlp(
        &self,
        graph_rel: &GraphRel,
        spec: &VariableLengthSpec,
        pattern_ctx: &PatternSchemaContext,
        left_join_col: &str,
        right_join_col: &str,
    ) -> Result<Vec<Cte>, RenderBuildError> {
        // Similar structure...
    }
    
    // ... other generator methods
}
```

### 4.3 Migration Strategy

#### Phase 0: Preparation (1 week)
**Goal**: Set up infrastructure without touching existing code

1. **Create `cte_manager.rs` stub**
   ```rust
   // src/render_plan/cte_manager.rs
   pub struct CteManager<'a> { /* ... */ }
   impl<'a> CteManager<'a> {
       pub fn new(...) -> Self { /* stub */ }
   }
   ```

2. **Add feature flag**
   ```toml
   # Cargo.toml
   [features]
   cte_manager = []
   ```

3. **Write comprehensive tests**
   - Unit tests for each generator method
   - Integration tests for each schema pattern
   - Performance benchmarks

**Success Criteria**:
- ✅ CTE Manager compiles
- ✅ All tests defined (can be ignored initially)
- ✅ No changes to existing code

---

#### Phase 1: Implement Core CTE Manager (2 weeks)
**Goal**: Implement CTE Manager with fallback to existing code

1. **Implement routing logic**
   ```rust
   pub fn generate_vlp_cte(&self, ...) -> Result<Vec<Cte>, Error> {
       #[cfg(feature = "cte_manager")]
       {
           // New implementation
       }
       #[cfg(not(feature = "cte_manager"))]
       {
           // Call existing code
           self.fallback_to_old_code(...)
       }
   }
   ```

2. **Implement each generator method**
   - Start with `generate_single_table_vlp` (simplest)
   - Test thoroughly before moving to next
   - Compare SQL output with existing code

3. **Unit test each method**
   - Mock inputs
   - Verify SQL correctness
   - Check performance

**Success Criteria**:
- ✅ CTE Manager generates identical SQL to existing code
- ✅ All unit tests pass
- ✅ Feature flag OFF by default

---

#### Phase 2: Parallel Execution & Validation (2 weeks)
**Goal**: Run both old and new code, compare outputs

1. **Add comparison mode**
   ```rust
   #[cfg(all(feature = "cte_manager", debug_assertions))]
   {
       let new_ctes = self.generate_vlp_cte_new(...)?;
       let old_ctes = self.generate_vlp_cte_old(...)?;
       assert_sql_equivalent(&new_ctes, &old_ctes);
       new_ctes
   }
   ```

2. **Run on test suite**
   - All integration tests with comparison mode
   - Fix any discrepancies
   - Document differences (if any are intentional improvements)

3. **Performance benchmarking**
   - Benchmark existing code
   - Benchmark new code
   - Ensure <5% regression

**Success Criteria**:
- ✅ 100% SQL equivalence on test suite
- ✅ No performance regression >5%
- ✅ All integration tests pass with CTE Manager

---

#### Phase 3: Gradual Cutover (1 week)
**Goal**: Switch to CTE Manager by default

1. **Enable feature by default**
   ```toml
   [features]
   default = ["cte_manager"]
   ```

2. **Deploy to staging**
   - Monitor for errors
   - Run production-like workload
   - Keep old code path for emergency rollback

3. **Monitor metrics**
   - Query latency
   - Error rates
   - Memory usage

**Success Criteria**:
- ✅ No increase in error rates
- ✅ No significant performance degradation
- ✅ Rollback plan tested and ready

---

#### Phase 4: Cleanup (1 week)
**Goal**: Remove old code

1. **Remove old generator constructors**
   - Keep only minimal constructor used by CTE Manager
   - Remove `new_denormalized`, `new_mixed`, `new_with_fk_edge`

2. **Remove boolean flags**
   - Remove `is_denormalized`, `is_fk_edge` fields from struct
   - Remove all flag-checking logic

3. **Update documentation**
   - Update architecture docs
   - Update inline comments
   - Create migration guide for contributors

**Success Criteria**:
- ✅ Old code removed
- ✅ All tests still pass
- ✅ Documentation updated

---

### 4.4 Rollback Strategy

**Immediate Rollback** (any phase):
```rust
// Disable feature flag
#[cfg(not(feature = "cte_manager"))]

// Reverts to existing code immediately
```

**Partial Rollback** (Phase 3+):
```rust
// Add runtime flag
if env::var("USE_OLD_CTE_LOGIC").is_ok() {
    return self.generate_vlp_cte_old(...);
}
```

**Emergency Hotfix**:
- Git revert to last known good commit
- Deploy old binary
- Investigate in development

---

## Part 5: Alternative Comparison

| Criterion | Alt 1: CTE Manager | Alt 2: Refactor Constructors | Alt 3: Registry Pattern | Alt 4: Status Quo |
|-----------|-------------------|------------------------------|------------------------|-------------------|
| **Complexity Reduction** | ⭐⭐⭐⭐⭐ High | ⭐⭐⭐ Medium | ⭐⭐ Low | ❌ None |
| **Maintainability** | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Good | ⭐⭐⭐⭐ Very Good | ❌ Poor |
| **Testability** | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Good | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐ Difficult |
| **Migration Risk** | ⭐⭐⭐ Medium | ⭐⭐⭐⭐ Low | ⭐⭐ High | ⭐⭐⭐⭐⭐ None |
| **Implementation Time** | 6-8 weeks | 3-4 weeks | 8-10 weeks | 0 |
| **Extensibility** | ⭐⭐⭐⭐ Very Good | ⭐⭐⭐ Good | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐ Limited |
| **Performance** | ⭐⭐⭐⭐ Same | ⭐⭐⭐⭐⭐ Same | ⭐⭐⭐ Slight overhead | ⭐⭐⭐⭐⭐ Current |
| **Eliminates Flags** | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |
| **Unified Entry Point** | ✅ Yes | ⚠️ Partial | ✅ Yes | ❌ No |
| **Uses Existing Infra** | ✅ Phase 2 work | ⚠️ Partial | ❌ No | ✅ N/A |

**Recommendation**: **Alternative 1 (CTE Manager)** provides the best balance of:
- Significant complexity reduction
- Manageable risk with phased approach
- Leverages existing Phase 2 infrastructure
- Clear migration and rollback strategy

---

## Part 6: Success Criteria & Metrics

### 6.1 Code Quality Metrics

**Before Refactoring**:
- CTE-related code: 8,348 lines across 3 files
- Constructor count: 4 constructors, 15-25 parameters each
- Boolean flag checks: 50+ locations
- Cyclomatic complexity: HIGH (nested conditionals)

**After Refactoring Target**:
- CTE-related code: <6,000 lines (28% reduction)
- Constructor count: 1 unified constructor
- Boolean flag checks: 0 (eliminated)
- Cyclomatic complexity: MEDIUM (pattern matching)

### 6.2 Functional Metrics

- ✅ 100% SQL output equivalence with existing code
- ✅ 0 failing tests (maintain 760/760 passing)
- ✅ <5% performance regression on CTE queries
- ✅ 0 production incidents related to CTE changes

### 6.3 Testing Metrics

- ✅ Unit test coverage: >80% for CTE Manager
- ✅ Integration test coverage: All schema patterns tested
- ✅ Performance benchmarks: Established baseline + comparisons
- ✅ Regression suite: 100 representative queries

---

## Part 7: Risk Analysis

### 7.1 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| SQL output differences | Medium | High | Parallel execution, comparison mode |
| Performance regression | Low | Medium | Comprehensive benchmarking |
| Rollback complexity | Low | High | Feature flags, version control |
| Integration test failures | Medium | Medium | Incremental testing, early validation |
| Memory usage increase | Low | Low | Profile before/after |

### 7.2 Project Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Timeline overrun | Medium | Medium | Buffer time, phased approach |
| Scope creep | Medium | High | Strict phase boundaries |
| Team availability | Low | High | Documentation, knowledge transfer |
| Competing priorities | Medium | Medium | Clear milestones, stakeholder alignment |

### 7.3 Lessons from Past Failures

From code-quality-analysis.md, previous attempts failed due to:

1. **Big-bang approach** → **Solution**: Phased migration with validation
2. **No rollback** → **Solution**: Feature flags at every phase
3. **Added layers without removing** → **Solution**: Phase 4 cleanup mandatory
4. **Unclear success criteria** → **Solution**: Quantitative metrics defined
5. **Insufficient testing** → **Solution**: Parallel execution, comparison mode

---

## Part 8: Open Questions & Trade-offs

### 8.1 Design Decisions Needed

**Q1**: Should `PatternSchemaContext` be stored in `LogicalPlan` or passed separately?
- **Option A**: Store in plan (larger plan size, but available everywhere)
- **Option B**: Pass separately (requires threading through functions)
- **Option C**: Recreate in CTE Manager (current Phase 2 approach)

**Recommendation**: Option C initially (use Phase 2 infrastructure), migrate to Option A in future if performance issues.

---

**Q2**: How to handle multi-type VLP generators?
- **Option A**: Integrate into CTE Manager immediately (larger scope)
- **Option B**: Keep separate for now, unify later (incremental)

**Recommendation**: Option B - focus on single-type VLP first, unify multi-type in Phase 5.

---

**Q3**: What to do with existing `VariableLengthCteGenerator` struct?
- **Option A**: Make it private, only accessed via CTE Manager
- **Option B**: Refactor its interface to accept `PatternSchemaContext`
- **Option C**: Keep as-is, let CTE Manager adapt

**Recommendation**: Option B - refactor interface, then make private in Phase 4.

---

### 8.2 Trade-offs

**Complexity vs. Flexibility**
- CTE Manager adds abstraction layer (complexity)
- But provides flexibility for future patterns
- **Decision**: Worth it - current complexity is worse

**Performance vs. Clarity**
- Pattern matching has overhead vs direct flags
- But exhaustive matching catches bugs
- **Decision**: Clarity wins - performance impact negligible

**Migration Risk vs. Long-term Benefit**
- Refactoring always carries risk
- But current code is unmaintainable
- **Decision**: Phased approach mitigates risk enough to proceed

---

## Part 9: Conclusion

### 9.1 Recommendation

**Proceed with Alternative 1 (CTE Manager) using the phased migration strategy.**

**Rationale**:
1. Addresses root cause (scattered decision logic, boolean flags)
2. Leverages existing Phase 2 infrastructure
3. Provides clear rollback at each phase
4. Quantifiable success metrics
5. Manageable 6-8 week timeline

### 9.2 Next Steps

**Immediate** (Week 1):
1. Review this design document with stakeholders
2. Get approval for timeline and approach
3. Set up feature flags and test infrastructure
4. Create `cte_manager.rs` stub

**Phase 1** (Weeks 2-3):
1. Implement CTE Manager routing logic
2. Implement first generator method (single table)
3. Write comprehensive unit tests
4. Validate SQL equivalence

**Phase 2** (Weeks 4-5):
1. Implement remaining generator methods
2. Enable parallel execution mode
3. Run full integration test suite
4. Performance benchmarking

**Phase 3** (Week 6):
1. Enable CTE Manager by default
2. Deploy to staging
3. Monitor metrics
4. Final validation

**Phase 4** (Week 7):
1. Remove old code
2. Remove boolean flags
3. Update documentation
4. Declare Phase 2 complete!

### 9.3 Dependencies

**Prerequisites**:
- ✅ Phase 0 complete (GraphJoinInference moved early in analyzer)
- ✅ Phase 2A complete (`recreate_pattern_schema_context` helper exists)
- ✅ PatternSchemaContext with necessary methods
- ⚠️ Need stakeholder approval for 6-8 week timeline

**Blockers**:
- None identified - can start immediately after approval

---

## Appendix A: Code Size Analysis

### Current CTE Code Distribution

```
src/render_plan/cte_extraction.rs:        4,342 lines
src/clickhouse_query_generator/variable_length_cte.rs: 3,245 lines
src/render_plan/cte_generation.rs:          761 lines
-------------------------------------------------------
Total:                                    8,348 lines
```

### Projected Code Distribution (After Refactoring)

```
src/render_plan/cte_manager.rs:            800 lines (NEW)
src/render_plan/cte_extraction.rs:       3,000 lines (reduced 31%)
src/clickhouse_query_generator/variable_length_cte.rs: 2,000 lines (reduced 38%)
src/render_plan/cte_generation.rs:          800 lines (minimal change)
-------------------------------------------------------
Total:                                    6,600 lines (21% reduction)
```

**Savings**: ~1,700 lines eliminated (boolean flag duplication, scattered logic)

---

## Appendix B: Related Documents

- `docs/development/code-quality-analysis.md` - Overall code quality assessment
- `notes/schema_consolidation_analysis.md` - Phase 0-2 schema refactoring
- `DEVELOPMENT_PROCESS.md` - Standard development workflow
- `STATUS.md` - Current feature status

---

**Document Status**: ✅ Ready for Review  
**Author**: Claude (AI Assistant)  
**Review Required**: Project Lead, Core Contributors  
**Next Review Date**: January 21, 2026 (after initial feedback)
