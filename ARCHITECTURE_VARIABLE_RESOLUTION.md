# Variable and Property Resolution Architecture

## Current Problem

Query: `MATCH (u:User) WITH u AS person RETURN person.name`

### Broken Flow

1. **Logical Planning Phase**
   - `u` created as GraphNode(alias="u", label="User")
   - WITH creates: `person` = exported alias for `u`
   - Projection item: `PropertyAccessExp(table_alias="person", column="name")`
   
2. **Analysis Phase** 
   - FilterTagging resolves `person.name` using schema: `name -> full_name`
   - Produces: `PropertyAccessExp(table_alias="person", column="full_name")`
   - **BUG**: Uses User schema even though `person` is a CTE

3. **Planning -> Render Transition**
   - PlanCtx knows `person` is a CTE export
   - But RenderPlan phase has lost this context
   - Tries to access `person.full_name` in SQL
   - CTE only exports `u_name`, not `full_name`
   - **ERROR**: Column not found

### Root Causes

1. **Loss of Variable Source Information**
   - Table alias `person` loses track of being CTE-sourced after planning phase
   - No way to know: "person" → CTE → exports "u_name" for property "name"

2. **Schema Mapping Applied Too Early**
   - During analysis, Cypher property is mapped to database column using schema
   - But CTE-sourced variables don't follow the base schema
   - Should defer mapping until we know the CTE export columns

3. **Multi-Layer Resolution Not Supported**
   - Need to resolve: `person.name` → (person maps to u via WITH) → (u.name in User schema) → (maps to full_name in DB) → (WITH renames to u_name in CTE)
   - Current code tries direct schema lookup on person's label

## Needed Architecture

### 1. Variable Source Tracking (Global)

```rust
#[derive(Debug, Clone)]
pub enum VariableSource {
    BaseTable {
        table_name: String,  // "brahmand.users_bench"
        label: Option<String>, // "User"
    },
    CteExport {
        cte_name: String,     // "with_person_cte_1"
        source_alias: String, // "u" - what it came from
        export_alias: String, // "person" - exported as this name
    },
    Union {
        inputs: Vec<VariableSource>,
    },
}
```

### 2. Property Resolution Pipeline

For `person.name`:

```
person (variable)
  ↓ [VariableSource lookup]
  → CteExport { source_alias: "u", export_alias: "person" }
  ↓ [Resolve u's properties in User schema]
  → User.name maps to "full_name" in database
  ↓ [Check WITH export mapping]
  → CTE SELECT... full_name AS u_name
  ↓ [Result]
  → person.u_name (the actual SQL column)
```

### 3. Key Components to Add

**A. VariableRegistry Extension**
- Store VariableSource for each alias
- Track through logical → render phases

**B. PropertyResolver**
```rust
pub fn resolve_property(
    variable: &str,
    property: &str,
    variable_source: &VariableSource,
    schema: &GraphSchema,
) -> Result<String> {
    // Returns the actual SQL column name
}
```

**C. Selection Item Builder**
- Before schema lookup, check variable source
- If CTE-sourced, resolve through CTE exports
- If base table, use current schema lookup

## Flow Changes Required

### Planning Phase (query_planner/)
- **UNCHANGED**: Create logical plans as normal
- **ADD**: Register VariableSource in PlanCtx for each new alias

### Render Phase (render_plan/)
- **ADD**: Pass VariableSource alongside TypedVariable
- **MODIFY**: extract_select_items uses VariableSource
- **MODIFY**: select_builder checks source before schema

### Key Insertion Points

1. **PlanCtx** - add variable_sources: HashMap<String, VariableSource>
2. **WithClause handling** - register CTE exports with sources
3. **GraphNode creation** - register base table sources
4. **select_builder** - use VariableSource for property resolution

## Example: WITH u AS person RETURN person.name

### Planning Phase
```
LogicalPlan::WithClause {
  input: GraphNode("u"),
  items: [TableAlias("u")],
  exported_aliases: ["person"]
}

PlanCtx registers:
  u -> VariableSource::BaseTable("brahmand.users_bench", "User")
  person -> VariableSource::CteExport {
    cte_name: "with_person_cte_1",
    source_alias: "u",
    export_alias: "person"
  }
```

### Render Phase
```
When extracting properties for Projection item "person.name":

1. Look up variable_source["person"]
   → CteExport { source_alias: "u", ... }
2. Resolve "name" through source:
   → Look up variable_source["u"]
   → BaseTable with User schema
   → User.name → "full_name" in database
3. Find WITH export column:
   → CTE SELECT ... full_name AS u_name ...
   → Exported column is "u_name"
4. Use "u_name" in SQL:
   → SELECT person.u_name AS ...
```

## Implementation Strategy

1. Add VariableSource enum and Registry
2. Modify PlanCtx to track sources
3. Update WithClause planning to register sources
4. Modify select_builder to consult sources
5. Add comprehensive property resolver
6. Test multi-layer resolution
