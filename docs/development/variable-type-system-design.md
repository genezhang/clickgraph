# Variable Type System Design Document

**Status**: ✅ IMPLEMENTED (Phase 1-2 Complete)  
**Author**: Architecture Review  
**Date**: January 19, 2026  
**Updated**: January 20, 2026  
**Related Issues**: Bug #5 (CTE column resolution), WITH clause entity expansion

---

## Implementation Status Summary

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | TypedVariable data structures | ✅ Complete |
| Phase 1.2 | Populate during MATCH processing | ✅ Complete |
| Phase 1.3 | Handle WITH exports | ✅ Complete |
| Phase 2.1 | VariableResolver integration | ✅ Complete |
| Phase 2.2 | CteEntityRef in Renderer | ✅ Complete |
| Phase 3 | Cleanup (ScopeContext removal) | 🔄 Deferred |

**Key Changes Implemented:**
1. `TypedVariable` enum and `VariableRegistry` in `typed_variable.rs`
2. `PlanCtx` now has a `VariableRegistry` field
3. `CteSchemaResolver` populates variables during WITH processing
4. `VariableResolver` uses `plan_ctx.lookup_variable()` as **PRIMARY** source
5. Bug #5 is **FIXED** - all test queries work correctly

---

## Executive Summary

ClickGraph's current variable handling has an architectural flaw: **two parallel scope systems that don't communicate**. This causes bugs when variables cross WITH clause boundaries, particularly when returning full nodes/relationships (e.g., `RETURN a`) rather than scalar values.

This document proposes unifying these systems into a single, authoritative variable tracking mechanism where **variables carry their semantic type from definition to rendering**.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Current Architecture](#2-current-architecture)
3. [Proposed Architecture](#3-proposed-architecture)
4. [Data Structures](#4-data-structures)
5. [Implementation Plan](#5-implementation-plan)
6. [Migration Strategy](#6-migration-strategy)
7. [Testing Strategy](#7-testing-strategy)
8. [Risk Assessment](#8-risk-assessment)

---

## 1. Problem Statement

### 1.1 The Bug

```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) 
WITH a, count(b) as cnt 
RETURN a, cnt
```

**Expected SQL (outer SELECT):**
```sql
SELECT 
    a_cnt.a_city AS "a_city",
    a_cnt.a_name AS "a_name",
    -- ... 7 columns for node 'a'
    a_cnt.cnt AS "cnt"
FROM with_a_cnt_cte_1 AS a_cnt
```

**Actual SQL (buggy):**
```sql
SELECT 
    a_cnt.a AS "a",    -- ❌ Column "a" doesn't exist!
    a_cnt.cnt AS "cnt"
FROM with_a_cnt_cte_1 AS a_cnt
```

### 1.2 Root Cause

When `RETURN a` is processed:
1. VariableResolver looks up `a` in its `ScopeContext`
2. It finds `VarSource::CteColumn { cte_name: "a_cnt", column_name: "a" }`
3. It transforms `TableAlias("a")` → `PropertyAccessExp("a_cnt", "a")`
4. But `a` is NOT a single column - it's a **node with 7 columns**!

The type information (node vs scalar) exists in `PlanCtx.cte_entity_types`, but `ScopeContext` doesn't access it.

### 1.3 Why This Keeps Happening

The codebase has **evolved two parallel systems** for variable tracking:

| System | Purpose | Location | Used By |
|--------|---------|----------|---------|
| `PlanCtx` | Query planning context | `plan_ctx/mod.rs` | Most analyzer passes |
| `ScopeContext` | Variable resolution | `variable_resolver.rs` | VariableResolver only |

These systems have **overlapping responsibilities but don't share data**, leading to information loss at scope boundaries.

---

## 2. Current Architecture

### 2.1 PlanCtx (Primary Planning Context)

```rust
pub struct PlanCtx {
    // Variable → full context (labels, properties, CTE reference)
    alias_table_ctx_map: HashMap<String, TableCtx>,
    
    // CTE entity type tracking (added to fix WITH bugs)
    cte_entity_types: HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>>,
    
    // CTE column mappings
    cte_columns: HashMap<String, HashMap<String, String>>,
    
    // Scope chaining for WITH
    parent_scope: Option<Box<PlanCtx>>,
    is_with_scope: bool,
    
    // ... many other fields
}
```

**Strengths:**
- Rich metadata per variable (TableCtx)
- Proper scope chaining
- Entity type preservation for CTEs

**Weaknesses:**
- Not used by VariableResolver during resolution
- TableCtx doesn't distinguish node/rel/scalar explicitly

### 2.2 ScopeContext (Variable Resolution)

```rust
pub struct ScopeContext {
    visible_vars: HashMap<String, VarSource>,
    parent: Option<Box<ScopeContext>>,
    current_cte_name: Option<String>,
}

pub enum VarSource {
    CteColumn { cte_name: String, column_name: String },
    SchemaEntity { alias: String, entity_type: EntityType },
    CteEntity { cte_name: String, alias: String, entity_type: EntityType },
    Parameter { name: String },
}
```

**Strengths:**
- Clean enum for variable sources
- EntityType distinction (Node/Relationship)
- Proper scope chaining

**Weaknesses:**
- Built from scratch during VariableResolver traversal
- No access to PlanCtx during resolution
- CteEntity exists but isn't populated correctly

### 2.3 The Disconnect

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    INFORMATION FLOW BROKEN                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   MATCH (a:User) ...                                                    │
│         │                                                               │
│         ▼                                                               │
│   ┌─────────────────────────────────────────┐                          │
│   │ PlanCtx.insert_table_ctx("a", TableCtx) │ ← Entity info stored     │
│   │ PlanCtx.cte_entity_types["cte"]["a"]    │ ← Also stored here       │
│   └─────────────────────────────────────────┘                          │
│                                                                         │
│   WITH a, count(b) as cnt                                               │
│         │                                                               │
│         ▼                                                               │
│   ┌─────────────────────────────────────────┐                          │
│   │ VariableResolver creates ScopeContext   │                          │
│   │ ScopeContext.add_variable("a", ???)     │ ← DOESN'T QUERY PlanCtx! │
│   │                                         │   Guesses from parent    │
│   │                                         │   scope.lookup("a")      │
│   └─────────────────────────────────────────┘                          │
│                                                                         │
│   RETURN a, cnt                                                         │
│         │                                                               │
│         ▼                                                               │
│   ┌─────────────────────────────────────────┐                          │
│   │ resolve_expression(TableAlias("a"))     │                          │
│   │ lookup("a") → CteColumn (WRONG!)        │ ← Should be CteEntity    │
│   │ → PropertyAccessExp("cte", "a")         │ ← Wrong transformation   │
│   └─────────────────────────────────────────┘                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Proposed Architecture

### 3.1 Design Principles

1. **Single Source of Truth**: One unified scope system, not two
2. **Variables Carry Type**: Semantic type attached at definition, not guessed later
3. **Explicit Over Implicit**: No guessing based on context
4. **Fail Fast**: Undefined variables caught during typing, not rendering

### 3.2 End-to-End Flow Diagram

The following sequence diagram shows how a query with WITH clause flows through the new type system:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  Query: MATCH (a:User)-[r:FOLLOWS]->(b) WITH a, count(b) as cnt RETURN a, cnt       │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: MATCH Processing (match_clause.rs)                                         │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  plan_ctx.define_node("a", Some(["User"]))                                     │ │
│  │    → TypedVariable::Node { name: "a", labels: ["User"], source: Match,         │ │
│  │                            columns: {user_id, name→full_name, city, ...} }     │ │
│  │                                                                                 │ │
│  │  plan_ctx.define_relationship("r", Some(["FOLLOWS"]))                          │ │
│  │    → TypedVariable::Relationship { name: "r", rel_types: ["FOLLOWS"], ... }    │ │
│  │                                                                                 │ │
│  │  plan_ctx.define_node("b", None)  // Label inferred from relationship          │ │
│  │    → TypedVariable::Node { name: "b", labels: None, source: Match, ... }       │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│  Scope after MATCH: { a: Node, r: Relationship, b: Node }                           │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 2: WITH Processing (with_clause.rs)                                           │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  Analyze WITH items: [a, count(b) as cnt]                                      │ │
│  │    - "a" → lookup("a") → Node → ExportType::Node                               │ │
│  │    - "count(b) as cnt" → aggregate → ExportType::Scalar { column: "cnt" }      │ │
│  │                                                                                 │ │
│  │  Generate CTE name: "with_a_cnt_cte_1"                                         │ │
│  │                                                                                 │ │
│  │  plan_ctx.export_to_with_scope(                                                │ │
│  │      [("a", Node), ("cnt", Scalar)],                                           │ │
│  │      "with_a_cnt_cte_1"                                                        │ │
│  │  )                                                                              │ │
│  │    → Creates NEW scope with:                                                   │ │
│  │      a: Node { source: Cte("with_a_cnt_cte_1"), columns: {a_user_id, a_name...}}│ │
│  │      cnt: Scalar { source: Cte("with_a_cnt_cte_1"), column: "cnt" }            │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│  Scope after WITH: { a: Node(CTE), cnt: Scalar(CTE) }  ← Barrier! Old vars gone     │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: Variable Resolution (variable_resolver.rs)                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  RETURN a, cnt                                                                  │ │
│  │                                                                                 │ │
│  │  resolve_expression(TableAlias("a")):                                          │ │
│  │    plan_ctx.lookup("a") → TypedVariable::Node { source: Cte {...} }            │ │
│  │    → CteEntityRef { cte: "with_a_cnt_cte_1", alias: "a", columns: [...] }      │ │
│  │                                                                                 │ │
│  │  resolve_expression(TableAlias("cnt")):                                        │ │
│  │    plan_ctx.lookup("cnt") → TypedVariable::Scalar { source: Cte {...} }        │ │
│  │    → PropertyAccessExp { table: "with_a_cnt_cte_1", column: "cnt" }            │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│  Resolved expressions carry full type information                                    │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: SQL Rendering (to_sql_query.rs)                                            │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  CteEntityRef("a") → EXPANDS to all columns:                                   │ │
│  │    a_cnt.a_user_id AS "a_user_id",                                             │ │
│  │    a_cnt.a_name AS "a_name",                                                   │ │
│  │    a_cnt.a_city AS "a_city", ...                                               │ │
│  │                                                                                 │ │
│  │  PropertyAccessExp("cnt") → single column:                                     │ │
│  │    a_cnt.cnt AS "cnt"                                                          │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│  Final SQL:                                                                          │
│  WITH with_a_cnt_cte_1 AS (SELECT a.user_id AS "a_user_id", ... count(*) AS "cnt")  │
│  SELECT a_cnt.a_user_id AS "a_user_id", a_cnt.a_name AS "a_name", ...               │
│         a_cnt.cnt AS "cnt"                                                          │
│  FROM with_a_cnt_cte_1 AS a_cnt                                                     │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Unified Scope System

Merge `ScopeContext` functionality into `PlanCtx`:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    UNIFIED SCOPE SYSTEM                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   PlanCtx (Enhanced)                                                    │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                                                                 │  │
│   │  // Core variable tracking (unified)                            │  │
│   │  variables: HashMap<String, TypedVariable>                      │  │
│   │                                                                 │  │
│   │  // Scope chain (already exists, enhanced)                      │  │
│   │  parent_scope: Option<Arc<PlanCtx>>                             │  │
│   │  scope_type: ScopeType  // Root, Match, With, Subquery          │  │
│   │                                                                 │  │
│   │  // Schema reference (already exists)                           │  │
│   │  schema: Arc<GraphSchema>                                       │  │
│   │                                                                 │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   Variable Lookup:                                                      │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  fn lookup(&self, name: &str) -> Option<&TypedVariable> {       │  │
│   │      // 1. Check current scope                                  │  │
│   │      if let Some(var) = self.variables.get(name) {              │  │
│   │          return Some(var);                                      │  │
│   │      }                                                          │  │
│   │      // 2. Check parent (respecting scope barriers)             │  │
│   │      if self.scope_type != ScopeType::With {                    │  │
│   │          if let Some(parent) = &self.parent_scope {             │  │
│   │              return parent.lookup(name);                        │  │
│   │          }                                                      │  │
│   │      }                                                          │  │
│   │      None                                                       │  │
│   │  }                                                              │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.4 TypedVariable: The Core Abstraction

```rust
/// A variable with its semantic type, set at definition time
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypedVariable {
    /// Node entity from MATCH - expands to multiple columns
    Node(NodeVariable),
    
    /// Relationship entity from MATCH - expands to relationship columns
    Relationship(RelVariable),
    
    /// Scalar value (aggregation result, literal, expression)
    Scalar(ScalarVariable),
    
    /// Path variable from path patterns
    Path(PathVariable),
    
    /// Collection from collect() - array type
    Collection(CollectionVariable),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeVariable {
    /// Variable name in Cypher (e.g., "a", "person")
    pub name: String,
    
    /// Node label(s) if known (e.g., ["User"], ["Person", "Employee"])
    pub labels: Option<Vec<String>>,
    
    /// Where this variable was defined
    pub source: VariableSource,
    
    /// Column expansion info (populated after schema binding)
    pub columns: Option<EntityColumns>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelVariable {
    pub name: String,
    pub rel_types: Option<Vec<String>>,
    pub source: VariableSource,
    pub columns: Option<EntityColumns>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalarVariable {
    pub name: String,
    pub source: VariableSource,
    /// The single column this maps to (for CTE sources)
    pub column: Option<String>,
}

/// Path variable from `MATCH p = (a)-[*]->(b)` or `MATCH p = shortestPath(...)`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathVariable {
    pub name: String,
    pub source: VariableSource,
    /// Start node variable name
    pub start_node: String,
    /// End node variable name  
    pub end_node: String,
    /// Relationship variable name (if named)
    pub relationship: Option<String>,
    /// Whether this is a shortestPath/allShortestPaths
    pub is_shortest_path: bool,
    /// Variable-length bounds (min, max) if applicable
    pub length_bounds: Option<(Option<u32>, Option<u32>)>,
}

/// Collection variable from `collect()` aggregation or list comprehension
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionVariable {
    pub name: String,
    pub source: VariableSource,
    /// Type of elements in the collection
    pub element_type: CollectionElementType,
    /// The CTE column containing the array (for CTE sources)
    pub column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CollectionElementType {
    /// Collection of nodes: `collect(n)`
    Nodes { labels: Option<Vec<String>> },
    /// Collection of relationships: `collect(r)`
    Relationships { rel_types: Option<Vec<String>> },
    /// Collection of scalars: `collect(n.name)`
    Scalars,
    /// Collection of paths: `collect(p)`
    Paths,
    /// Unknown element type (inferred later)
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VariableSource {
    /// Defined in a MATCH pattern
    Match,
    
    /// Exported from a WITH clause (lives in a CTE)
    Cte { cte_name: String },
    
    /// Query parameter ($param)
    Parameter,
    
    /// UNWIND element
    Unwind { source_array: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityColumns {
    /// ID column for this entity
    pub id_column: String,
    
    /// Property columns: (cypher_name, db_column, sql_alias)
    pub properties: Vec<PropertyColumn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyColumn {
    pub cypher_name: String,   // e.g., "name"
    pub db_column: String,     // e.g., "full_name"
    pub sql_alias: String,     // e.g., "a_name"
}
```

---

## 4. Data Structures

### 4.1 Enhanced PlanCtx

```rust
pub struct PlanCtx {
    // ========== NEW: Unified Variable Tracking ==========
    /// Variables defined in this scope
    /// Key: variable name (e.g., "a", "cnt")
    /// Value: typed variable with full metadata
    variables: HashMap<String, TypedVariable>,
    
    /// Type of this scope (determines lookup behavior)
    scope_type: ScopeType,
    
    // ========== EXISTING (kept for compatibility) ==========
    /// Legacy alias tracking (deprecate gradually)
    alias_table_ctx_map: HashMap<String, TableCtx>,
    
    /// Parent scope for chaining
    parent_scope: Option<Arc<PlanCtx>>,
    
    /// Graph schema
    schema: Arc<GraphSchema>,
    
    // ... other existing fields
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScopeType {
    /// Root scope (query entry point)
    Root,
    /// MATCH clause scope
    Match,
    /// WITH clause scope (barrier)
    With { cte_name: String },
    /// Subquery scope
    Subquery,
    /// UNION branch scope
    Union,
}

#### 4.1.1 Variable Scoping Rules

Variable scope is primarily determined by WITH clause boundaries and dependencies within scopes:

**WITH Clause Barriers**:
- Variables from outer scopes ARE accessible in WITH projection items (e.g., `WITH a.name AS name`)
- AFTER the WITH, only the explicitly exported aliases are visible (barrier effect)
- WITH creates a new scope containing only the exported variables

**Subquery Scoping** (within WITH scopes):
- Subqueries (IN, EXISTS) have their own isolated scope
- Variables from the outer query are accessible within subqueries
- Subquery variables do not leak to outer scope

**UNION Branch Scoping** (within WITH scopes):
- Each UNION branch has its own scope
- Variables must be defined consistently across all branches
- UNION result scope contains only variables present in all branches

**Scope Chain Lookup** (conceptual logic):
```rust
// This is the core lookup algorithm - full implementation in Section 4.2
pub fn lookup(&self, name: &str) -> Option<&TypedVariable> {
    // 1. Check current scope
    if let Some(var) = self.variables.get(name) {
        return Some(var);
    }
    
    // 2. WITH is a barrier - don't search parent
    if matches!(self.scope_type, ScopeType::With { .. }) {
        return None;
    }
    
    // 3. For Subquery/Union, search parent (WITH boundaries still apply)
    if let Some(ref parent) = self.parent_scope {
        return parent.lookup(name);
    }
    
    None
}
```

### 4.2 Variable Definition API

```rust
impl PlanCtx {
    /// Define a node variable from MATCH pattern
    pub fn define_node(
        &mut self,
        name: &str,
        labels: Option<Vec<String>>,
    ) -> Result<(), PlanCtxError> {
        if self.variables.contains_key(name) {
            return Err(PlanCtxError::DuplicateVariable { name: name.to_string() });
        }
        
        let columns = self.resolve_node_columns(labels.as_ref())?;
        
        self.variables.insert(name.to_string(), TypedVariable::Node(NodeVariable {
            name: name.to_string(),
            labels,
            source: VariableSource::Match,
            columns: Some(columns),
        }));
        
        Ok(())
    }
    
    /// Define a relationship variable from MATCH pattern
    pub fn define_relationship(
        &mut self,
        name: &str,
        rel_types: Option<Vec<String>>,
    ) -> Result<(), PlanCtxError> {
        // Similar to define_node...
    }
    
    /// Define a scalar variable (from aggregation, expression)
    pub fn define_scalar(
        &mut self,
        name: &str,
        source: VariableSource,
        column: Option<String>,
    ) -> Result<(), PlanCtxError> {
        self.variables.insert(name.to_string(), TypedVariable::Scalar(ScalarVariable {
            name: name.to_string(),
            source,
            column,
        }));
        Ok(())
    }
    
    /// Define a path variable from MATCH p = ... patterns
    pub fn define_path(
        &mut self,
        name: &str,
        start_node: &str,
        end_node: &str,
        relationship: Option<String>,
        is_shortest_path: bool,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Result<(), PlanCtxError> {
        if self.variables.contains_key(name) {
            return Err(PlanCtxError::DuplicateVariable { name: name.to_string() });
        }
        
        self.variables.insert(name.to_string(), TypedVariable::Path(PathVariable {
            name: name.to_string(),
            source: VariableSource::Match,
            start_node: start_node.to_string(),
            end_node: end_node.to_string(),
            relationship,
            is_shortest_path,
            length_bounds,
        }));
        Ok(())
    }
    
    /// Define a collection variable from collect() or list comprehension
    pub fn define_collection(
        &mut self,
        name: &str,
        source: VariableSource,
        element_type: CollectionElementType,
        column: Option<String>,
    ) -> Result<(), PlanCtxError> {
        self.variables.insert(name.to_string(), TypedVariable::Collection(CollectionVariable {
            name: name.to_string(),
            source,
            element_type,
            column,
        }));
        Ok(())
    }
    
    /// Export variables to a new WITH scope
    pub fn export_to_with_scope(
        &self,
        exports: &[(String, ExportType)],
        cte_name: &str,
    ) -> PlanCtx {
        let mut new_ctx = PlanCtx::new_with_scope(
            self.schema.clone(),
            ScopeType::With { cte_name: cte_name.to_string() },
            Some(Arc::new(self.clone())),
        );
        
        for (alias, export_type) in exports {
            match export_type {
                ExportType::Node => {
                    // Copy node variable with updated source
                    if let Some(TypedVariable::Node(node)) = self.lookup(alias) {
                        let mut exported = node.clone();
                        exported.source = VariableSource::Cte { cte_name: cte_name.to_string() };
                        // Update column aliases for CTE context
                        if let Some(ref mut cols) = exported.columns {
                            for prop in &mut cols.properties {
                                prop.sql_alias = format!("{}_{}", alias, prop.cypher_name);
                            }
                        }
                        new_ctx.variables.insert(alias.clone(), TypedVariable::Node(exported));
                    }
                }
                ExportType::Scalar { column } => {
                    new_ctx.define_scalar(
                        alias,
                        VariableSource::Cte { cte_name: cte_name.to_string() },
                        Some(column.clone()),
                    ).ok();
                }
                // ... other cases
            }
        }
        
        new_ctx
    }
    
    /// Look up a variable (searches scope chain)
    pub fn lookup(&self, name: &str) -> Option<&TypedVariable> {
        // Current scope
        if let Some(var) = self.variables.get(name) {
            return Some(var);
        }
        
        // WITH scope is a barrier - don't search parent
        if matches!(self.scope_type, ScopeType::With { .. }) {
            return None;
        }
        
        // Search parent scope
        if let Some(ref parent) = self.parent_scope {
            return parent.lookup(name);
        }
        
        None
    }
}

#[derive(Debug, Clone)]
pub enum ExportType {
    Node,
    Relationship,
    Scalar { column: String },
    Path,
    Collection,
}
```

### 4.3 Error Handling and Validation

```rust
#[derive(Debug, thiserror::Error)]
pub enum PlanCtxError {
    #[error("Variable '{name}' is already defined in this scope")]
    DuplicateVariable { name: String },
    
    #[error("Variable '{name}' is not defined in current scope")]
    UndefinedVariable { name: String, available: Vec<String> },
    
    #[error("Schema binding failed for {entity_type} '{name}': {reason}")]
    SchemaBindingFailed { 
        entity_type: String, 
        name: String, 
        reason: String 
    },
    
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    
    #[error("Invalid variable export: {variable} cannot be exported as {export_type}")]
    InvalidExport { 
        variable: String, 
        export_type: String 
    },
}

impl PlanCtx {
    /// Get all visible variable names for error reporting
    pub fn visible_variable_names(&self) -> Vec<String> {
        let mut names = self.variables.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }
}
```

**Error Propagation Strategy**:
- Schema binding errors caught at variable definition time
- Undefined variables detected during resolution, not rendering
- Type mismatches validated at WITH export boundaries
- Fallback to legacy behavior during migration period

### 4.4 Simplified VariableResolver

With unified scope, VariableResolver becomes much simpler:

```rust
pub struct VariableResolver;

impl VariableResolver {
    pub fn resolve_expression(
        &self,
        expr: &LogicalExpr,
        ctx: &PlanCtx,
    ) -> Result<LogicalExpr, AnalyzerError> {
        match expr {
            LogicalExpr::TableAlias(alias) => {
                match ctx.lookup(&alias.0) {
                    Some(TypedVariable::Node(node)) => {
                        match &node.source {
                            VariableSource::Match => {
                                // Keep as TableAlias - renderer expands
                                Ok(expr.clone())
                            }
                            VariableSource::Cte { cte_name } => {
                                // Transform to CTE entity reference
                                Ok(LogicalExpr::CteEntityRef(CteEntityRef {
                                    cte_name: cte_name.clone(),
                                    alias: node.name.clone(),
                                    columns: node.columns.clone(),
                                }))
                            }
                            _ => Ok(expr.clone()),
                        }
                    }
                    Some(TypedVariable::Scalar(scalar)) => {
                        match &scalar.source {
                            VariableSource::Cte { cte_name } => {
                                // Transform to property access
                                Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_name.clone()),
                                    column: PropertyValue::Column(
                                        scalar.column.clone().unwrap_or(scalar.name.clone())
                                    ),
                                }))
                            }
                            _ => Ok(expr.clone()),
                        }
                    }
                    Some(TypedVariable::Relationship(rel)) => {
                        // Similar to Node handling
                        // ...
                    }
                    None => {
                        Err(AnalyzerError::UndefinedVariable {
                            name: alias.0.clone(),
                            available: ctx.visible_variable_names(),
                        })
                    }
                    _ => Ok(expr.clone()),
                }
            }
            // ... other expression types
        }
    }
}
```

---

## 5. Implementation Plan

### Phase 1: Foundation (Week 1) ✅ COMPLETE

**Goal**: Add TypedVariable and integrate with PlanCtx without breaking existing code.

#### 1.1 Add New Types (2 days) ✅

- [x] Create `src/query_planner/typed_variable.rs`
  - TypedVariable enum (Node, Relationship, Scalar, Path, Collection)
  - NodeVariable, RelVariable, ScalarVariable, PathVariable, CollectionVariable structs
  - VariableSource enum (Match, Cte, Parameter, Unwind)
  - VariableRegistry struct with define/lookup methods

- [x] Add to `plan_ctx/mod.rs`:
  - `variables: VariableRegistry` field
  - `define_node()`, `define_relationship()`, `define_scalar()` methods
  - `lookup_variable()` method using VariableRegistry

#### 1.2 Populate During MATCH Processing (2 days) ✅

- [x] Update `plan_ctx.add_alias()`:
  - Automatically calls `variables.define_node()` or `define_relationship()` based on TableCtx
  - Handles path variables

#### 1.3 Handle WITH Exports (1 day) ✅

- [x] Update `cte_schema_resolver.rs`:
  - `register_cte_entity_types()` now populates VariableRegistry
  - Entities exported through WITH get `VariableSource::Cte`
  - Scalars (aggregates) registered as ScalarVariable

### Phase 2: Variable Resolution (Week 2) ✅ COMPLETE

**Goal**: VariableResolver uses TypedVariable for resolution.

#### 2.1 Update VariableResolver (3 days) ✅

- [x] Added `lookup_entity_from_plan_ctx()` helper function
- [x] Made TypedVariable the **PRIMARY** lookup source (not fallback)
- [x] Kept ScopeContext as **FALLBACK** for edge cases during transition
- [x] Updated both WITH clause and Projection scope handling

#### 2.2 Handle CteEntityRef in Renderer (2 days) ✅

- [x] `CteEntityRef` added to `RenderExpr`
- [x] Entity expansion working in SQL generation
- [x] SELECT items correctly expand node/relationship aliases

#### 2.3 Handle Path and Collection Variables (1 day) ✅

- [x] `define_path()` and `define_collection()` methods in PlanCtx
- [x] PathVariable and CollectionVariable structs with constructors
- [x] `from_match()`, `from_cte()`, `shortest_path()` factory methods

### Phase 3: Cleanup (Week 3) 🔄 DEFERRED

**Goal**: Remove duplicate code, ensure consistency.

#### 3.1 Consolidate TableCtx (2 days) - DEFERRED

- [ ] Make `TableCtx` use `TypedVariable` internally
- [ ] Or deprecate `TableCtx` in favor of `TypedVariable`
- [ ] Update all `get_table_ctx()` callers

#### 3.2 Remove Legacy Scope Code (2 days) - DEFERRED

- [ ] Remove `cte_entity_types` from PlanCtx (now in TypedVariable)
- [ ] Remove `ScopeContext` struct
- [ ] Remove `VarSource` enum (replaced by TypedVariable)

**Rationale for Deferral**: 
- Bug #5 is fixed
- ScopeContext fallback provides safety net during transition
- Low risk to leave parallel systems temporarily
- Can be addressed in future refactoring sprint
- [ ] Update VariableResolver to handle Path/Collection TypedVariable variants

### Phase 3: Cleanup (Week 3)

**Goal**: Remove duplicate code, ensure consistency.

#### 3.1 Consolidate TableCtx (2 days)

- [ ] Make `TableCtx` use `TypedVariable` internally
- [ ] Or deprecate `TableCtx` in favor of `TypedVariable`
- [ ] Update all `get_table_ctx()` callers

#### 3.2 Remove Legacy Scope Code (2 days)

- [ ] Remove `cte_entity_types` from PlanCtx (now in TypedVariable)
- [ ] Remove `ScopeContext` struct
- [ ] Remove `VarSource` enum (replaced by TypedVariable)

#### 3.3 Documentation & Testing (1 day)

- [ ] Update architecture docs
- [ ] Add unit tests for TypedVariable
- [ ] Add integration tests for WITH + RETURN patterns

---

## 6. Migration Strategy

### 6.1 Parallel Running Period

During Phase 1-2, both systems run in parallel:

```rust
impl PlanCtx {
    pub fn lookup(&self, name: &str) -> Option<&TypedVariable> {
        // New system
        if let Some(var) = self.variables.get(name) {
            return Some(var);
        }
        
        // Fallback to legacy during migration
        if let Some(table_ctx) = self.alias_table_ctx_map.get(name) {
            log::warn!("⚠️ Variable '{}' found in legacy system, not TypedVariable", name);
            // Convert on-the-fly for compatibility
            return Some(self.convert_table_ctx_to_typed(name, table_ctx));
        }
        
        // Parent scope lookup...
    }
}
```

### 6.2 Feature Flags

```rust
// In config or environment
pub const USE_TYPED_VARIABLES: bool = true;

// In VariableResolver
if USE_TYPED_VARIABLES {
    self.resolve_with_typed_variables(plan, ctx)
} else {
    self.resolve_legacy(plan, scope)
}
```

### 6.3 Gradual Rollout

1. **Week 1**: TypedVariable populated, not used for resolution
2. **Week 2**: TypedVariable used for resolution with legacy fallback
3. **Week 3**: Legacy code removed, TypedVariable only

---

## 7. Testing Strategy

### 7.1 Unit Tests for TypedVariable

```rust
#[test]
fn test_node_variable_lookup() {
    let mut ctx = PlanCtx::new(schema);
    ctx.define_node("a", Some(vec!["User".to_string()])).unwrap();
    
    let var = ctx.lookup("a").unwrap();
    assert!(matches!(var, TypedVariable::Node(_)));
    
    if let TypedVariable::Node(node) = var {
        assert_eq!(node.name, "a");
        assert_eq!(node.labels, Some(vec!["User".to_string()]));
        assert!(matches!(node.source, VariableSource::Match));
    }
}

#[test]
fn test_with_scope_barrier() {
    let mut parent = PlanCtx::new(schema);
    parent.define_node("a", Some(vec!["User".to_string()])).unwrap();
    
    let child = parent.export_to_with_scope(
        &[("a".to_string(), ExportType::Node)],
        "with_a_cte_1",
    );
    
    // Child should have 'a' with CTE source
    let var = child.lookup("a").unwrap();
    if let TypedVariable::Node(node) = var {
        assert!(matches!(node.source, VariableSource::Cte { .. }));
    }
    
    // Variables not exported should not be visible
    parent.define_node("b", Some(vec!["User".to_string()])).unwrap();
    assert!(child.lookup("b").is_none());  // WITH is a barrier!
}
```

### 7.2 Integration Tests

```rust
#[test]
fn test_return_node_through_with() {
    let query = "MATCH (a:User)-[r:FOLLOWS]->(b:User) WITH a, count(b) as cnt RETURN a, cnt";
    let result = execute_sql_only(query);
    
    // Should have 7 columns for 'a' plus 'cnt'
    assert!(result.sql.contains("a_city"));
    assert!(result.sql.contains("a_name"));
    assert!(result.sql.contains("a_user_id"));
    assert!(!result.sql.contains("a_cnt.a AS"));  // NOT this!
}

#[test]
fn test_return_scalar_through_with() {
    let query = "MATCH (a:User) WITH count(a) as total RETURN total";
    let result = execute_sql_only(query);
    
    // Should have single column 'total'
    assert!(result.sql.contains("total"));
    assert!(!result.sql.contains("total_city"));  // NOT expanded!
}
```

### 7.3 Regression Test Suite ✅ **IMPLEMENTED** - January 20, 2026

Created comprehensive test file covering all WITH patterns:

**Location**: `tests/integration/test_with_variable_types.py`

**Test Coverage (18 tests)**:
- `test_node_through_with()` - 4 tests
- `test_relationship_through_with()` - 1 test  
- `test_scalar_through_with()` - 2 tests
- `test_mixed_exports()` - 2 tests
- `test_chained_with_clauses()` - 2 tests (1 is known limitation)
- `test_with_where_clause()` - 2 tests
- `test_collect_through_with()` - 1 test
- `test_path_through_with()` - 1 test
- Edge cases - 3 tests

**Known Limitation** (documented in KNOWN_ISSUES.md):
- Variable alias renaming (`WITH u AS person`) doesn't propagate type information
- Workaround: Use same alias name (`WITH u RETURN u.name`)

**Status**: All 18 tests passing ✅

---

## 8. Risk Assessment

### 8.1 High Risk Areas

| Area | Risk | Mitigation |
|------|------|------------|
| Breaking existing queries | High | Parallel running, extensive tests |
| Performance regression | Medium | Benchmark before/after, lazy column resolution |
| Incomplete migration | Medium | Feature flags, gradual rollout |

#### 8.1.1 Performance Impact Analysis

**Memory Overhead**:
- TypedVariable: ~200-300 bytes per variable (vs legacy TableCtx: ~150-200 bytes)
- Expected increase: 20-50% memory usage for variable storage
- Mitigation: Column resolution is lazy (only when needed for rendering)

**Lookup Performance**:
- HashMap lookup: O(1) average case (same as legacy)
- Scope chain traversal: Maximum 3-5 levels deep
- Expected overhead: <1% for typical queries

**Benchmark Strategy**:
```rust
// Before/after benchmarks
#[bench]
fn bench_variable_lookup(b: &mut Bencher) {
    // Measure lookup time for complex WITH queries
}

#[bench] 
fn bench_query_compilation(b: &mut Bencher) {
    // Measure end-to-end query planning time
}
```

**Optimization Opportunities**:
- Cache resolved column information per variable
- Avoid redundant schema lookups during resolution
- Profile and optimize hot paths in VariableResolver

### 8.2 Rollback Plan

If issues are found after deployment:

1. Set `USE_TYPED_VARIABLES = false`
2. Revert to legacy ScopeContext code
3. Investigate and fix before re-enabling

#### 8.2.1 Integration Compatibility Checklist

**Core Features**:
- [ ] OPTIONAL MATCH variable scoping
- [ ] Shortest path variable handling (`MATCH p = shortestPath(...)`)
- [ ] Variable-length path variables
- [ ] COLLECT() aggregation results
- [ ] Multi-schema variable isolation
- [ ] UNION operations with consistent variable definitions
- [ ] Subquery variable scoping (IN, EXISTS)

**Advanced Patterns**:
- [ ] Pattern comprehensions: `[(a)-[]->(b) | b.name]`
- [ ] Complex WITH chains: `WITH a WITH b WITH c`
- [ ] Mixed entity types in single WITH: `WITH node, rel, scalar`
- [ ] Path functions: `length(p)`, `nodes(p)`, `relationships(p)`

**Edge Cases**:
- [ ] Variables not used in RETURN
- [ ] Duplicate variable names across scopes
- [ ] Circular variable dependencies
- [ ] Schema validation failures during binding

### 8.3 Success Metrics

- [ ] All existing tests pass
- [ ] Bug #5 (CTE column resolution) fixed
- [ ] No performance regression (< 5% overhead)
- [ ] Codebase has single source of truth for variables
- [ ] New WITH-related bugs can be fixed in one place

#### 8.3.1 Debugging and Observability

**Debug Logging**:
```rust
impl PlanCtx {
    pub fn debug_variable_scope(&self) -> String {
        format!(
            "Scope[{}]: {} variables - {}",
            self.scope_type,
            self.variables.len(),
            self.variables.keys().collect::<Vec<_>>().join(", ")
        )
    }
}
```

**Error Context Enhancement**:
- Include scope chain in undefined variable errors
- Show available variables in current and parent scopes
- Log variable type transformations during resolution

**Development Tools**:
- `--debug-variables` flag to dump scope information
- Variable resolution tracing in server logs
- Visual scope chain diagrams for complex queries

**Migration Debugging**:
- Warn when falling back to legacy system
- Log differences between old/new resolution paths
- Performance comparison logging during parallel running

---

## Appendix A: File Changes Summary

| File | Changes |
|------|---------|
| `src/query_planner/typed_variable.rs` | NEW: TypedVariable, NodeVariable, etc. |
| `src/query_planner/plan_ctx/mod.rs` | ADD: variables, scope_type, lookup() |
| `src/query_planner/logical_plan/match_clause.rs` | ADD: calls to define_node/rel |
| `src/query_planner/logical_plan/with_clause.rs` | ADD: export_to_with_scope() |
| `src/query_planner/analyzer/variable_resolver.rs` | REWRITE: use plan_ctx.lookup() |
| `src/query_planner/logical_expr.rs` | ADD: CteEntityRef variant |
| `src/render_plan/select_builder.rs` | ADD: CteEntityRef handling |
| `src/clickhouse_query_generator/to_sql_query.rs` | ADD: CteEntityRef to SQL |

---

## Appendix B: Current Code References

### Where Variables Are Defined

```
match_clause.rs:
  - Line 1498: insert_table_ctx() for nodes
  - Line 1695: insert_table_ctx() for nodes  
  - Line 1782: insert_table_ctx() for relationships

plan_builder.rs:
  - Line 279: insert_table_ctx() during plan building

variable_resolver.rs:
  - Line 609: add_variable() for GraphNode (SchemaEntity)
  - Line 648: add_variable() for GraphRel (SchemaEntity)
  - Line 278-310: add_variable() for WITH exports
```

### Where Variables Are Looked Up

```
variable_resolver.rs:
  - Line 904-968: resolve_expression() - TableAlias lookup
  - Line 386-435: Projection scope building from WITH

plan_ctx/mod.rs:
  - Line 365-383: get_table_ctx() with scope chain

render_plan/select_builder.rs:
  - Line ~300: expand_table_alias_to_select_items()
```

---

## Appendix C: Example Transformation

### Before (Current)

```
LogicalPlan::Projection {
    items: [
        ProjectionItem {
            expr: TableAlias("a"),  // ← Just a name, no type info!
            alias: Some("a")
        },
        ProjectionItem {
            expr: TableAlias("cnt"),
            alias: Some("cnt")
        }
    ]
}
```

### After (Proposed)

```
LogicalPlan::Projection {
    items: [
        ProjectionItem {
            expr: CteEntityRef {      // ← Full type info!
                cte_name: "with_a_cnt_cte_1",
                alias: "a",
                columns: EntityColumns {
                    id_column: "user_id",
                    properties: [
                        PropertyColumn { cypher: "name", db: "full_name", alias: "a_name" },
                        PropertyColumn { cypher: "city", db: "city", alias: "a_city" },
                        // ...
                    ]
                }
            },
            alias: Some("a")
        },
        ProjectionItem {
            expr: PropertyAccessExp {  // ← Scalar, single column
                table_alias: "with_a_cnt_cte_1",
                column: "cnt"
            },
            alias: Some("cnt")
        }
    ]
}
```

---

*Document Version: 1.1*  
*Last Updated: January 19, 2026*  
*Revision Notes: Added PathVariable/CollectionVariable structs, define_path()/define_collection() methods, end-to-end flow diagram, fixed scoping rules*
