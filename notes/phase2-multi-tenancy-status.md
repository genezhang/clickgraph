# Phase 2 Multi-Tenancy Status Report

**Date**: November 15, 2025  
**Current Progress**: Infrastructure Complete, Core Feature Not Yet Implemented

---

## ✅ What's Complete (Tasks 1-5)

### Week 1: Context Infrastructure ✅

**1. Request Models Updated** ✅ (Commit: 0626404)
```rust
// src/server/models.rs
pub struct QueryRequest {
    pub query: String,
    pub parameters: Option<HashMap<String, Value>>,
    pub tenant_id: Option<String>,           // ✅ Added
    pub view_parameters: Option<HashMap<String, Value>>,  // ✅ Added
    pub role: Option<String>,                 // ✅ Added (Task 5)
}
```

**2. HTTP Handler Integration** ✅ (Commit: 73e253d)
- Extracts `tenant_id` and `view_parameters` from HTTP POST body
- Passes to query execution pipeline
- Location: `src/server/handlers.rs`

**3. Bolt Protocol Integration** ✅ (Commit: 73e253d)
- Extracts `tenant_id` from RUN message extra parameters
- Location: `src/server/bolt_protocol/messages.rs`, `handler.rs`

**4. Query Planner Threading** ✅ (Commit: 73e253d)
```rust
// src/query_planner/plan_ctx/mod.rs
pub struct PlanCtx {
    pub schema: Arc<GraphSchema>,
    pub tenant_id: Option<String>,  // ✅ Added
    // ... other fields
}
```

**5. SET ROLE RBAC** ✅ (Commits: 5d0f712, faa7bf4, 1c11932)
- Single-tenant ClickHouse native RBAC
- `SET ROLE` command before queries
- Tested and documented

---

## ❌ What's NOT Implemented (Tasks 6-18)

### Core Feature: Parameterized View SQL Generation ❌

**The Critical Missing Piece**:

**Schema Config Does NOT Support `view_parameters`**:
```rust
// Current: src/graph_catalog/config.rs
pub struct NodeDefinition {
    pub label: String,
    pub database: String,
    pub table: String,
    pub node_id: String,
    pub properties: HashMap<String, String>,
    // ❌ MISSING: pub view_parameters: Option<Vec<String>>,
}
```

**SQL Generator Does NOT Create Parameterized Calls**:
```rust
// What we need but DON'T HAVE:
// FROM users_secure(tenant_id = 'acme', region = 'US')

// What we currently generate:
// FROM brahmand.users_secure
```

**No ViewResolver Logic for Parameters**:
- `src/query_planner/analyzer/view_resolver.rs` - No parameter handling
- `src/clickhouse_query_generator/` - No parameterized view syntax

---

## 📋 Remaining Work Breakdown

### **Task 6: Unit Tests for Parameter Context** (1 day) ❌
**Status**: Not started  
**Files to test**:
- Request parsing (tenant_id, view_parameters extraction)
- PlanCtx threading
- Parameter validation

**Test cases needed**:
```rust
#[test]
fn test_tenant_id_extraction_http() { ... }

#[test]
fn test_view_parameters_extraction_http() { ... }

#[test]
fn test_plan_ctx_tenant_id_threading() { ... }
```

---

### **Week 2: Core Parameterized View Implementation** (4-5 days) ❌

#### **Task 7: Extend Schema YAML** (1 day) ❌
**Status**: Not started  
**Goal**: Add `view_parameters` field to node/relationship schemas

**File**: `src/graph_catalog/config.rs`

**Changes needed**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    pub label: String,
    pub database: String,
    pub table: String,
    pub node_id: String,
    pub properties: HashMap<String, String>,
    
    // ✅ ADD THIS:
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,  // ← NEW FIELD
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipDefinition {
    pub type_name: String,
    pub database: String,
    pub table: String,
    pub from_id: String,
    pub to_id: String,
    pub from_node: Option<String>,
    pub to_node: Option<String>,
    pub properties: HashMap<String, String>,
    
    // ✅ ADD THIS:
    #[serde(default)]
    pub view_parameters: Option<Vec<String>>,  // ← NEW FIELD
}
```

**Schema conversion to GraphSchema**:
```rust
// src/graph_catalog/config.rs - Update from_config() methods
impl NodeSchema {
    pub fn from_config(def: &NodeDefinition) -> Self {
        NodeSchema {
            // ... existing fields
            view_parameters: def.view_parameters.clone(),  // ← ADD
        }
    }
}
```

**Estimated**: 2 hours

---

#### **Task 8: Implement Parameterized View SQL Generation** (2 days) ❌
**Status**: Not started  
**Goal**: Generate `FROM table(param1=val1, param2=val2)` syntax

**Key Files**:
1. `src/graph_catalog/graph_schema.rs` - Add view_parameters field
2. `src/clickhouse_query_generator/view_scan.rs` - Parameterized SQL
3. `src/query_planner/logical_plan/view_scan.rs` - Store parameters in plan

**Implementation Steps**:

**Step 1: Add to GraphSchema** (30 min)
```rust
// src/graph_catalog/graph_schema.rs
pub struct NodeSchema {
    pub label: String,
    pub table_name: String,
    pub database: String,
    pub node_id: NodeIdSchema,
    pub property_mappings: HashMap<String, String>,
    
    // ✅ ADD THIS:
    pub view_parameters: Option<Vec<String>>,  // ← NEW FIELD
}

pub struct RelationshipSchema {
    pub type_name: String,
    pub table_name: String,
    pub database: String,
    pub from_id: String,
    pub to_id: String,
    pub from_node_label: String,
    pub to_node_label: String,
    pub property_mappings: HashMap<String, String>,
    
    // ✅ ADD THIS:
    pub view_parameters: Option<Vec<String>>,  // ← NEW FIELD
}
```

**Step 2: Store Parameters in LogicalPlan** (1 hour)
```rust
// src/query_planner/logical_plan/view_scan.rs
pub struct ViewScan {
    pub source_table: String,
    pub view_filter: Option<LogicalExpr>,
    pub property_mapping: HashMap<String, String>,
    pub node_id: String,
    pub output_schema: Vec<String>,
    pub projections: Vec<LogicalExpr>,
    pub from_id: Option<String>,
    pub to_id: Option<String>,
    pub input: Option<Arc<LogicalPlan>>,
    
    // ✅ ADD THESE:
    pub view_parameters: Option<Vec<String>>,           // ← Parameters from schema
    pub parameter_values: Option<HashMap<String, Value>>, // ← Values from request
}
```

**Step 3: Pass Parameters During Planning** (2 hours)
```rust
// src/query_planner/logical_plan/match_clause.rs
fn try_generate_view_scan(
    _alias: &str,
    label: &str,
    plan_ctx: &PlanCtx,  // ← Has tenant_id and view_parameters
) -> Option<Arc<LogicalPlan>> {
    let schema = plan_ctx.schema();
    let node_schema = schema.get_node_schema(label)?;
    
    // ✅ ADD: Collect parameter values
    let parameter_values = if let Some(view_params) = &node_schema.view_parameters {
        let mut values = HashMap::new();
        
        for param_name in view_params {
            if param_name == "tenant_id" {
                if let Some(tid) = &plan_ctx.tenant_id {
                    values.insert(param_name.clone(), Value::String(tid.clone()));
                }
            } else if let Some(view_params_map) = &plan_ctx.view_parameters {
                if let Some(val) = view_params_map.get(param_name) {
                    values.insert(param_name.clone(), val.clone());
                }
            }
        }
        Some(values)
    } else {
        None
    };
    
    let view_scan = ViewScan {
        // ... existing fields
        view_parameters: node_schema.view_parameters.clone(),  // ← NEW
        parameter_values,                                       // ← NEW
    };
    
    Some(Arc::new(LogicalPlan::ViewScan(view_scan)))
}
```

**Step 4: Generate Parameterized SQL** (4 hours)
```rust
// src/clickhouse_query_generator/view_scan.rs (NEW FILE OR EXTEND EXISTING)

pub fn build_parameterized_table_ref(
    database: &str,
    table: &str,
    view_parameters: &Option<Vec<String>>,
    parameter_values: &Option<HashMap<String, Value>>,
) -> String {
    let base_table = format!("{}.{}", database, table);
    
    // If no parameters, return plain table reference
    if view_parameters.is_none() || parameter_values.is_none() {
        return base_table;
    }
    
    let params = view_parameters.as_ref().unwrap();
    let values = parameter_values.as_ref().unwrap();
    
    // Build parameter list: tenant_id = 'acme', region = 'US'
    let param_strings: Vec<String> = params
        .iter()
        .filter_map(|param_name| {
            values.get(param_name).map(|val| {
                format!("{} = {}", param_name, value_to_sql(val))
            })
        })
        .collect();
    
    if param_strings.is_empty() {
        base_table
    } else {
        // Generate: database.table(param1=val1, param2=val2)
        format!("{}({})", base_table, param_strings.join(", "))
    }
}

fn value_to_sql(value: &Value) -> String {
    match value {
        Value::String(s) => format!("'{}'", s.replace("'", "''")),  // Escape quotes
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" },
        _ => "NULL".to_string(),
    }
}
```

**Integration in SQL Generator**:
```rust
// Wherever we build FROM clauses (table references)
let table_ref = build_parameterized_table_ref(
    &node_schema.database,
    &node_schema.table_name,
    &view_scan.view_parameters,
    &view_scan.parameter_values,
);

// Generates:
// brahmand.users_secure                          (no parameters)
// brahmand.users_secure(tenant_id = 'acme')      (single parameter)
// brahmand.users_secure(tenant_id = 'acme', region = 'US')  (multiple)
```

**Estimated**: 8 hours

---

#### **Task 9: Add view_parameters to HTTP/Bolt APIs** (1 day) ❌
**Status**: Already done for HTTP! ✅  
**Remaining**: Document and test

**HTTP API** (Already works):
```json
POST /query
{
  "query": "MATCH (u:User) RETURN u",
  "tenant_id": "acme",
  "view_parameters": {
    "region": "US",
    "start_date": "2025-01-01"
  }
}
```

**Bolt Protocol** (Need to add):
```python
# RUN message with view_parameters
session.run(
    "MATCH (u:User) RETURN u",
    db="brahmand",
    tenant_id="acme",
    view_parameters={"region": "US"}
)
```

**Files to modify**:
- `src/server/bolt_protocol/messages.rs` - Extract view_parameters
- `src/server/bolt_protocol/handler.rs` - Thread through execution

**Estimated**: 2 hours

---

#### **Task 10: Test Parameterized Views with ClickHouse** (1 day) ❌
**Status**: Not started  
**Goal**: End-to-end test with real ClickHouse parameterized views

**Test Setup**:
```sql
-- Create test table
CREATE TABLE brahmand.users_multi_tenant (
    user_id UInt32,
    tenant_id String,
    name String,
    region String
) ENGINE = Memory;

INSERT INTO brahmand.users_multi_tenant VALUES
    (1, 'acme', 'Alice', 'US'),
    (2, 'acme', 'Bob', 'EU'),
    (3, 'widgets', 'Charlie', 'US');

-- Create parameterized view
CREATE VIEW brahmand.users_secure AS
SELECT * FROM brahmand.users_multi_tenant
WHERE tenant_id = {tenant_id:String}
  AND region = {region:String};
```

**Test Script** (`scripts/test/test_parameterized_views.py`):
```python
# Test 1: Single parameter (tenant_id)
response = requests.post("http://localhost:8080/query", json={
    "query": "MATCH (u:User) RETURN u.user_id, u.name",
    "schema_name": "multi_tenant",
    "tenant_id": "acme"
})
# Expected: Alice, Bob (both from 'acme' tenant)

# Test 2: Multiple parameters (tenant_id + region)
response = requests.post("http://localhost:8080/query", json={
    "query": "MATCH (u:User) RETURN u.user_id, u.name",
    "schema_name": "multi_tenant",
    "tenant_id": "acme",
    "view_parameters": {"region": "US"}
})
# Expected: Alice only (tenant='acme' AND region='US')
```

**Schema**:
```yaml
graph_schema:
  graph_name: multi_tenant
  version: "1.0"
  
  nodes:
    - label: User
      database: brahmand
      table: users_secure
      node_id: user_id
      view_parameters: [tenant_id, region]  # ← KEY FIELD
      property_mappings:
        user_id: user_id
        name: name
        region: region
  
  relationships: []
```

**Estimated**: 4 hours

---

#### **Task 11: Test SET ROLE with Database-Managed Users** (Already Done!) ✅
**Status**: Complete (Commit: 1c11932)  
**Summary**: Tested and documented in `notes/set-role-testing-summary.md`

---

#### **Task 12: Error Handling for Parameter Validation** (1 day) ❌
**Status**: Not started  
**Goal**: Clear error messages for parameter issues

**Error Cases**:
1. Required parameter missing (tenant_id not provided)
2. Unknown parameter (view doesn't expect that parameter)
3. Invalid parameter type (expected String, got Number)
4. Parameter value too long (SQL injection risk)

**Implementation**:
```rust
// src/query_planner/logical_plan/match_clause.rs
fn validate_parameters(
    view_params: &Vec<String>,
    provided_values: &HashMap<String, Value>,
    tenant_id: &Option<String>,
) -> Result<(), String> {
    for param_name in view_params {
        if param_name == "tenant_id" {
            if tenant_id.is_none() {
                return Err(format!(
                    "Required parameter 'tenant_id' not provided. \
                     View requires: {:?}",
                    view_params
                ));
            }
        } else if !provided_values.contains_key(param_name) {
            return Err(format!(
                "Required parameter '{}' not provided. \
                 View requires: {:?}",
                param_name, view_params
            ));
        }
    }
    Ok(())
}
```

**Estimated**: 2 hours

---

### **Week 3: Testing** (3 days) ❌

#### **Task 13: Integration Test Suite** (2 days) ❌
- Multi-tenant data isolation tests
- Parameter combinations (tenant_id + region + date_range)
- Error cases (missing parameters, invalid values)
- **Estimated**: 8 hours

#### **Task 14: Benchmark Performance** (1 day) ❌
- Overhead of parameterized views
- Compare: plain tables vs parameterized views vs non-parameterized views
- **Estimated**: 4 hours

#### **Task 15: SET ROLE Error Handling** (Already Done!) ✅
- Tested in Task 5

---

### **Week 4: Documentation** (2 days) ❌

#### **Task 16: Update phase2-minimal-rbac.md** (Already Done!) ✅
- Fixed Pattern 2 examples (Commit: faa7bf4)
- Documented database-managed user requirement

#### **Task 17: Create Examples and Migration Guide** (1 day) ❌
- Example schemas with parameterized views
- Multi-tenant setup guide
- Parameter configuration examples
- **Estimated**: 4 hours

#### **Task 18: Update CHANGELOG and STATUS** (1 hour) ❌
- Document Phase 2 completion
- Update feature matrix
- **Estimated**: 1 hour

---

## 📊 Summary

### Completed: 5/18 Tasks (28%)
- ✅ Tasks 1-4: Infrastructure (tenant_id, view_parameters in request models)
- ✅ Task 5: SET ROLE RBAC
- ✅ Task 11: SET ROLE testing (part of Task 5)
- ✅ Task 15: SET ROLE error handling (part of Task 5)
- ✅ Task 16: Documentation updates (part of Task 5)

### Remaining: 13 Tasks (72%)

**Critical Path** (Must complete for multi-tenancy to work):
1. **Task 7**: Schema YAML extension (2 hours) ⚡ **CRITICAL**
2. **Task 8**: Parameterized view SQL generation (8 hours) ⚡ **CRITICAL**
3. **Task 10**: End-to-end testing (4 hours) ⚡ **CRITICAL**

**Total critical path**: ~14 hours (2 days)

**Nice-to-Have** (Can be done after core feature works):
- Task 6: Unit tests
- Task 9: Bolt protocol view_parameters
- Task 12: Enhanced error handling
- Task 13-14: Comprehensive testing
- Task 17-18: Documentation

---

## 🎯 Next Action Items

### Immediate (Next Session):

**Option 1: Complete Multi-Tenancy Feature** (~2 days)
1. Implement Task 7 (schema extension)
2. Implement Task 8 (SQL generation)
3. Test with Task 10 (end-to-end test)
4. Result: Multi-tenancy fully working

**Option 2: Move to Other Priorities**
- We have a solid foundation (infrastructure complete)
- Can come back to finish multi-tenancy later
- Focus on other Phase 2 or Phase 3 features

### Recommended: Option 1
Multi-tenancy is **80% infrastructure done**, only needs the **core SQL generation** to be fully functional. Just 2 days of focused work would complete it.

---

## 🔍 Key Insight

**We built all the plumbing but haven't turned on the water yet!**

✅ **What works**:
- Request models accept tenant_id and view_parameters
- Bolt protocol extracts tenant_id
- PlanCtx threads tenant_id through query planner
- SET ROLE RBAC works perfectly

❌ **What doesn't work**:
- Schema doesn't store view_parameters field
- SQL generator doesn't create `table(param=value)` syntax
- **Result**: Parameters are accepted but ignored

**Fix**: Just Task 7 + Task 8 (10 hours) to make it fully functional!
