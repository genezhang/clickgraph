# ReplacingMergeTree & FINAL Support Design

**Feature**: Phase 2 Task #6  
**Effort**: 1-2 weeks  
**Status**: Design Phase  
**Date**: November 17, 2025

## Overview

Add support for ClickHouse table engines that require/benefit from the `FINAL` keyword to deduplicate or merge data at query time.

## Problem Statement

ClickHouse MergeTree family engines handle mutable data through background merges. Until merges complete, queries may return duplicate/unmerged rows. The `FINAL` keyword forces deduplication at query time.

**Engines supporting FINAL** (verified from ClickHouse docs):
1. **ReplacingMergeTree** - Deduplicates rows, keeps latest version by sorting key
2. **CollapsingMergeTree** - Collapses rows using sign column (-1/+1)
3. **VersionedCollapsingMergeTree** - Like Collapsing but with version column for ordering
4. **CoalescingMergeTree** - Newer variant of CollapsingMergeTree (needs verification)
5. **AggregatingMergeTree** - Finalizes pre-aggregated state (materializes aggregates)
6. **SummingMergeTree** - Sums numeric columns for same sorting key

**Note**: All these engines support FINAL, but performance trade-off varies. Auto-detection will verify actual engine capabilities during implementation.

**Performance Trade-off**: `FINAL` adds query overhead (2-10x slower), so users should control when to use it.

## Design Philosophy

**Aligned with ClickGraph principles**:
1. **Auto-detection**: Let ClickHouse metadata tell us what's needed (no manual config by default)
2. **User control**: Allow override via `use_final` flag (ClickHouse gives choice, we do too)
3. **Integration with auto-schema**: Detection happens during schema discovery

## Solution Design

### 1. Auto-Detection (Primary Method)

Query ClickHouse `system.tables` to detect engine type:

```sql
SELECT engine, engine_full
FROM system.tables
WHERE database = 'mydb' AND name = 'users'
```

**Example results**:
- `ReplacingMergeTree` → Use FINAL
- `CollapsingMergeTree(sign)` → Use FINAL
- `MergeTree` → Don't use FINAL

### 2. Manual Override (Optional)

**Schema Configuration**:
```yaml
nodes:
  - label: User
    table: users
    use_final: true   # ← Override: Force FINAL even if not detected
    
  - label: Order
    table: orders
    use_final: false  # ← Override: Disable FINAL even if ReplacingMergeTree
    
  - label: Product
    table: products
    # ← No use_final: Auto-detect from ClickHouse
```

**Behavior**:
- `use_final: true` → Always use FINAL (overrides auto-detection)
- `use_final: false` → Never use FINAL (overrides auto-detection)
- `use_final` not set → Auto-detect from engine type

### 3. Integration with Auto-Schema Feature

When auto-schema runs (`DESCRIBE TABLE` or discovery), also fetch engine type:

```rust
pub struct AutoSchemaDiscovery {
    // Discovers table structure AND engine type
    pub fn discover_table(database: &str, table: &str) -> TableMetadata {
        TableMetadata {
            columns: vec![...],
            engine: detect_engine(database, table),  // ← Auto-detect
            primary_key: detect_primary_key(...),
        }
    }
}
```

## Implementation Details

### Phase 1: Engine Detection with Verification (Days 1-3)

**File**: `src/graph_catalog/engine_detection.rs` (new)

**Key Addition**: Dynamic FINAL support verification for unknown engines

```rust
use clickhouse::Client;

#[derive(Debug, Clone, PartialEq)]
pub enum TableEngine {
    MergeTree,
    ReplacingMergeTree { version_column: Option<String> },
    CollapsingMergeTree { sign_column: String },
    VersionedCollapsingMergeTree { sign_column: String, version_column: String },
    CoalescingMergeTree,  // Newer variant - needs verification
    AggregatingMergeTree,
    SummingMergeTree { sum_columns: Vec<String> },
    Other(String),
}

impl TableEngine {
    /// Returns true if this engine supports FINAL keyword
    /// 
    /// Note: This is conservative - we'll verify actual engine capabilities
    /// by testing FINAL query syntax during auto-detection
    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            TableEngine::ReplacingMergeTree { .. }
                | TableEngine::CollapsingMergeTree { .. }
                | TableEngine::VersionedCollapsingMergeTree { .. }
                | TableEngine::CoalescingMergeTree
                | TableEngine::AggregatingMergeTree
                | TableEngine::SummingMergeTree { .. }
        )
    }
    
    /// Conservative check: Returns true only for engines that ALWAYS benefit from FINAL
    /// (deduplication/collapsing engines)
    pub fn requires_final_for_correctness(&self) -> bool {
        matches!(
            self,
            TableEngine::ReplacingMergeTree { .. }
                | TableEngine::CollapsingMergeTree { .. }
                | TableEngine::VersionedCollapsingMergeTree { .. }
                | TableEngine::CoalescingMergeTree
        )
    }
}

pub async fn detect_table_engine(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<TableEngine> {
    let query = format!(
        "SELECT engine, engine_full FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, table
    );
    
    let row: (String, String) = client
        .query(&query)
        .fetch_one()
        .await?;
    
    let engine = parse_engine(&row.0, &row.1)?;
    
    // For unknown engines or new variants, verify FINAL support
    if matches!(engine, TableEngine::Other(_) | TableEngine::CoalescingMergeTree) {
        let supports_final = verify_final_support(client, database, table).await?;
        tracing::info!(
            "Engine {:?} FINAL support: {}",
            engine,
            supports_final
        );
    }
    
    Ok(engine)
}

/// Verify if a table supports FINAL by attempting a query
async fn verify_final_support(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<bool> {
    let test_query = format!(
        "SELECT * FROM {}.{} FINAL LIMIT 0",
        database, table
    );
    
    match client.query(&test_query).execute().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let err_msg = e.to_string();
            // Check if error is about FINAL not supported
            if err_msg.contains("FINAL") || err_msg.contains("not support") {
                Ok(false)
            } else {
                // Other error - propagate it
                Err(e.into())
            }
        }
    }
}

fn parse_engine(engine: &str, engine_full: &str) -> Result<TableEngine> {
    match engine {
        "ReplacingMergeTree" => {
            // Parse version column from engine_full
            // "ReplacingMergeTree(version)" → Some("version")
            Ok(TableEngine::ReplacingMergeTree {
                version_column: extract_version_column(engine_full),
            })
        }
        "CollapsingMergeTree" => {
            // Parse sign column from engine_full
            // "CollapsingMergeTree(sign)" → "sign"
            Ok(TableEngine::CollapsingMergeTree {
                sign_column: extract_sign_column(engine_full)?,
            })
        }
        "VersionedCollapsingMergeTree" => {
            Ok(TableEngine::VersionedCollapsingMergeTree {
                sign_column: extract_sign_column(engine_full)?,
                version_column: extract_version_column(engine_full)
                    .ok_or_else(|| anyhow!("Missing version column"))?,
            })
        }
        "CoalescingMergeTree" => {
            // Newer engine - verify it exists in actual ClickHouse
            Ok(TableEngine::CoalescingMergeTree)
        }
        "AggregatingMergeTree" => Ok(TableEngine::AggregatingMergeTree),
        "SummingMergeTree" => {
            Ok(TableEngine::SummingMergeTree {
                sum_columns: extract_sum_columns(engine_full),
            })
        }
        "MergeTree" => Ok(TableEngine::MergeTree),
        other => {
            // For unknown engines, we'll test FINAL support dynamically
            Ok(TableEngine::Other(other.to_string()))
        }
    }
}
```

### Phase 2: Schema Configuration (Days 3-4)

**File**: `src/graph_catalog/config.rs`

```rust
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeDefinition {
    pub label: String,
    pub table: String,
    pub node_id: String,
    pub properties: HashMap<String, String>,
    pub view_parameters: Option<Vec<String>>,
    pub use_final: Option<bool>,  // ← New field
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RelationshipDefinition {
    pub type_name: String,
    pub table: String,
    pub from_node_id: String,
    pub to_node_id: String,
    pub properties: Option<HashMap<String, String>>,
    pub view_parameters: Option<Vec<String>>,
    pub use_final: Option<bool>,  // ← New field
}
```

**File**: `src/graph_catalog/graph_schema.rs`

```rust
pub struct NodeSchema {
    pub label: String,
    pub table: String,
    pub node_id: String,
    pub properties: HashMap<String, String>,
    pub view_parameters: Option<Vec<String>>,
    pub engine: TableEngine,      // ← New field (detected)
    pub use_final: Option<bool>,  // ← New field (override)
}

impl NodeSchema {
    /// Determine if FINAL should be used for this node
    pub fn should_use_final(&self) -> bool {
        // 1. Check explicit override (user choice takes precedence)
        if let Some(use_final) = self.use_final {
            return use_final;
        }
        
        // 2. Auto-detect: Use FINAL for engines that need it for correctness
        // (Conservative: only deduplication/collapsing engines)
        self.engine.requires_final_for_correctness()
    }
    
    /// Check if this engine supports FINAL (regardless of whether we use it by default)
    pub fn can_use_final(&self) -> bool {
        self.engine.supports_final()
    }
}
```

### Phase 3: SQL Generation (Days 5-6)

**File**: `src/clickhouse_query_generator/from_clause.rs` (new or modify existing)

```rust
pub fn build_from_clause(
    table: &str,
    alias: &str,
    use_final: bool,
) -> String {
    if use_final {
        format!("{} FINAL AS {}", table, alias)
    } else {
        format!("{} AS {}", table, alias)
    }
}
```

**File**: `src/clickhouse_query_generator/view_scan.rs` (modify)

```rust
pub fn build_view_scan(
    scan: &ViewScan,
    schema: &NodeSchema,  // ← Pass schema to check use_final
) -> String {
    let table_ref = build_table_reference(
        &scan.table_name,
        &scan.view_parameter_names,
        &scan.view_parameter_values,
    );
    
    let use_final = schema.should_use_final();
    
    if use_final {
        format!("(SELECT * FROM {} FINAL) AS {}", table_ref, scan.alias)
    } else {
        format!("{} AS {}", table_ref, scan.alias)
    }
}
```

### Phase 4: Schema Loading (Days 7-8)

**File**: `src/graph_catalog/config.rs` (modify `GraphSchemaConfig::build_schema`)

```rust
impl GraphSchemaConfig {
    pub async fn build_schema(
        &self,
        client: &Client,  // ← Add client parameter for engine detection
    ) -> Result<GraphSchema> {
        let mut nodes = HashMap::new();
        
        for node_def in &self.nodes {
            // Detect engine type from ClickHouse
            let engine = detect_table_engine(
                client,
                &self.database,
                &node_def.table,
            ).await?;
            
            nodes.insert(
                node_def.label.clone(),
                NodeSchema {
                    label: node_def.label.clone(),
                    table: node_def.table.clone(),
                    node_id: node_def.id_column.clone(),
                    properties: node_def.properties.clone(),
                    view_parameters: node_def.view_parameters.clone(),
                    engine,              // ← Store detected engine
                    use_final: node_def.use_final,  // ← Store override
                },
            );
        }
        
        // Similar for relationships...
        
        Ok(GraphSchema { database: self.database.clone(), nodes, relationships })
    }
}
```

### Phase 5: Auto-Schema Discovery Integration (Days 9-10)

**File**: `src/graph_catalog/auto_schema.rs` (new or modify existing)

```rust
pub async fn discover_table_metadata(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<TableMetadata> {
    // Get columns
    let columns = describe_table(client, database, table).await?;
    
    // Get engine type (for FINAL detection)
    let engine = detect_table_engine(client, database, table).await?;
    
    // Get primary key
    let primary_key = detect_primary_key(client, database, table).await?;
    
    Ok(TableMetadata {
        table_name: table.to_string(),
        columns,
        engine,
        primary_key,
    })
}
```

## Testing Strategy

### Unit Tests

**File**: `src/graph_catalog/engine_detection_tests.rs`

```rust
#[tokio::test]
async fn test_detect_replacing_merge_tree() {
    let engine = parse_engine("ReplacingMergeTree", "ReplacingMergeTree(version)").unwrap();
    assert!(matches!(engine, TableEngine::ReplacingMergeTree { .. }));
    assert!(engine.supports_final());
    assert!(engine.requires_final_for_correctness());
}

#[tokio::test]
async fn test_detect_collapsing_merge_tree() {
    let engine = parse_engine("CollapsingMergeTree", "CollapsingMergeTree(sign)").unwrap();
    assert!(matches!(engine, TableEngine::CollapsingMergeTree { .. }));
    assert!(engine.supports_final());
    assert!(engine.requires_final_for_correctness());
}

#[tokio::test]
async fn test_detect_coalescing_merge_tree() {
    let engine = parse_engine("CoalescingMergeTree", "CoalescingMergeTree").unwrap();
    assert!(matches!(engine, TableEngine::CoalescingMergeTree));
    assert!(engine.supports_final());
    // Will be verified dynamically in practice
}

#[tokio::test]
async fn test_detect_aggregating_merge_tree() {
    let engine = parse_engine("AggregatingMergeTree", "AggregatingMergeTree").unwrap();
    assert!(matches!(engine, TableEngine::AggregatingMergeTree));
    assert!(engine.supports_final());
    assert!(!engine.requires_final_for_correctness());  // Optional optimization
}

#[tokio::test]
async fn test_detect_summing_merge_tree() {
    let engine = parse_engine("SummingMergeTree", "SummingMergeTree(amount, quantity)").unwrap();
    assert!(matches!(engine, TableEngine::SummingMergeTree { .. }));
    assert!(engine.supports_final());
    assert!(!engine.requires_final_for_correctness());  // Optional optimization
}

#[test]
fn test_use_final_override_disable() {
    let schema = NodeSchema {
        engine: TableEngine::ReplacingMergeTree { version_column: None },
        use_final: Some(false),  // Override: disable FINAL
        // ...
    };
    assert!(!schema.should_use_final());
    assert!(schema.can_use_final());  // But it's supported
}

#[test]
fn test_use_final_override_enable() {
    let schema = NodeSchema {
        engine: TableEngine::MergeTree,  // Doesn't need FINAL
        use_final: Some(true),  // But user wants it anyway
        // ...
    };
    assert!(schema.should_use_final());
}

#[tokio::test]
async fn test_verify_final_support() {
    // Mock test: Check that verify_final_support correctly identifies support
    let client = create_test_client().await;
    
    // Test with ReplacingMergeTree (should support)
    let supports = verify_final_support(&client, "test_db", "replacing_table").await.unwrap();
    assert!(supports);
    
    // Test with regular MergeTree (may or may not support - depends on ClickHouse version)
    let supports = verify_final_support(&client, "test_db", "regular_table").await.unwrap();
    // Result may vary - just ensure no panic
}
```

### Integration Tests

**File**: `tests/integration/test_replacing_merge_tree.py`

```python
"""Test ReplacingMergeTree and FINAL support."""

# Setup: Create ReplacingMergeTree table
CREATE TABLE users_replacing (
    user_id UInt64,
    name String,
    email String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY user_id;

# Insert duplicate data (different versions)
INSERT INTO users_replacing VALUES
    (1, 'Alice v1', 'alice1@example.com', 1),
    (1, 'Alice v2', 'alice2@example.com', 2),  -- Latest version
    (2, 'Bob v1', 'bob@example.com', 1);

def test_auto_detect_replacing_merge_tree():
    """Engine detection should work automatically."""
    # Schema without use_final (auto-detect)
    schema = """
    nodes:
      - label: User
        table: users_replacing
        node_id: user_id
    """
    
    # Query should use FINAL automatically
    result = query("MATCH (u:User) RETURN u.name, u.email")
    
    # Should return deduplicated data (only latest versions)
    assert len(result) == 2
    assert result[0]["u.name"] == "Alice v2"  # Latest version

def test_use_final_override_false():
    """Override should disable FINAL even for ReplacingMergeTree."""
    schema = """
    nodes:
      - label: User
        table: users_replacing
        use_final: false  # ← Explicit override
    """
    
    result = query("MATCH (u:User) RETURN u.name")
    
    # Without FINAL, may return duplicates
    assert len(result) >= 2  # May be 3 if merge hasn't happened

def test_use_final_override_true():
    """Override should force FINAL even for regular MergeTree."""
    schema = """
    nodes:
      - label: User
        table: users_mergetree  # Regular MergeTree
        use_final: true  # ← Force FINAL
    """
    
    # Query should include FINAL even though not required
    result = query("MATCH (u:User) RETURN u.name", sql_only=True)
    assert "FINAL" in result["sql"]
```

## Documentation

**File**: `docs/replacing-merge-tree.md` (new)

Topics:
- What is ReplacingMergeTree and why it matters
- How ClickGraph auto-detects engines
- When to use `use_final: true/false` override
- Performance considerations (FINAL overhead)
- Example schemas
- Migration guide

## Performance Considerations

**FINAL overhead**:
- Can be 2-10x slower depending on data volume and merge state
- Only use when you need guaranteed deduplication
- Consider disabling for large analytical queries where approximate results are acceptable

**When to use `use_final: false`**:
- Analytics queries where duplicates are acceptable
- Time-series data where latest values emerge naturally
- Large scans where performance > accuracy

**When to use `use_final: true`**:
- Transaction lookups (user profiles, orders)
- Financial calculations
- Compliance/audit queries

## Migration Guide

**Adding to existing schema**:

```yaml
# Before (no FINAL support)
nodes:
  - label: User
    table: users_replacing
    
# After (auto-detected, no changes needed!)
nodes:
  - label: User
    table: users_replacing
    # ClickGraph automatically detects ReplacingMergeTree

# With override (optional)
nodes:
  - label: User
    table: users_replacing
    use_final: false  # Disable for performance
```

## Edge Cases

1. **Parameterized views + FINAL**: 
   ```sql
   SELECT * FROM users_by_tenant(tenant_id = $tenant_id) FINAL
   ```
   Should work - apply FINAL after view materialization

2. **Multiple relationship types + FINAL**:
   ```sql
   (SELECT * FROM follows FINAL)
   UNION ALL
   (SELECT * FROM friends FINAL)
   ```
   Apply FINAL to each branch

3. **View-based tables**: If schema references a view (not base table), engine detection fails gracefully (returns `Other`, no FINAL)

## Implementation Checklist

### Phase 1: Engine Detection & Verification (Days 1-3)
- [ ] Implement `TableEngine` enum with all 6+ engine types
- [ ] Implement `parse_engine()` for known engines
- [ ] Implement `verify_final_support()` for dynamic verification
- [ ] Add `supports_final()` and `requires_final_for_correctness()` methods
- [ ] Unit tests for engine parsing
- [ ] Unit tests for verification logic
- [ ] **Double-check**: Test with actual CoalescingMergeTree table

### Phase 2: Schema Configuration (Days 4-5)
- [ ] Add `use_final: Option<bool>` to NodeDefinition
- [ ] Add `use_final: Option<bool>` to RelationshipDefinition
- [ ] Update YAML parsing to handle new field
- [ ] Implement `should_use_final()` logic (override > auto-detect)
- [ ] Unit tests for override behavior

### Phase 3: SQL Generation (Days 6-7)
- [ ] Update FROM clause generation to add FINAL
- [ ] Update ViewScan generation to add FINAL
- [ ] Handle subqueries and CTEs with FINAL
- [ ] Unit tests for SQL generation

### Phase 4: Schema Loading & Detection (Days 8-9)
- [ ] Modify `GraphSchemaConfig::build_schema()` to detect engines
- [ ] Add ClickHouse client parameter to schema loading
- [ ] Cache detected engine types
- [ ] Integration tests with real tables

### Phase 5: Documentation & Testing (Days 10-12)
- [ ] Write `docs/replacing-merge-tree.md`
- [ ] Integration tests with all 6 engine types
- [ ] Performance benchmarks (FINAL overhead)
- [ ] Update multi-tenancy docs (mutable data pattern)
- [ ] Example schemas

## Timeline

**Week 1** (Days 1-7):
- Engine detection with dynamic verification
- Schema configuration fields
- SQL generation with FINAL
- Unit tests
- **Key**: Verify CoalescingMergeTree actually exists in ClickHouse

**Week 2** (Days 8-12):
- Schema loading with engine detection
- Integration tests with all engine types (ReplacingMergeTree, CollapsingMergeTree, CoalescingMergeTree, etc.)
- Documentation
- Performance testing

## Success Criteria

- ✅ Auto-detects all MergeTree variants that support FINAL:
  - ReplacingMergeTree
  - CollapsingMergeTree
  - VersionedCollapsingMergeTree
  - CoalescingMergeTree (verify it exists)
  - AggregatingMergeTree
  - SummingMergeTree
- ✅ Dynamic verification for unknown/new engine types
- ✅ Generates SQL with `FINAL` keyword when needed
- ✅ Respects `use_final` override in schema
- ✅ Conservative auto-detection (only use FINAL for correctness, not optimization)
- ✅ Works with parameterized views
- ✅ Integration tests pass with real ClickHouse tables (all engine types)
- ✅ Documentation complete with engine compatibility matrix
- ✅ Performance overhead measured and documented (per engine type)

## Next Steps

1. Review and approve this design
2. Create implementation branch
3. Start with engine detection (Day 1-2)
4. Iterative development with daily commits

---

**Questions/Feedback**: [Add your comments here]
