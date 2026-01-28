# Property Pruning Architecture Diagrams

## Current Architecture (Before Optimization)

```mermaid
graph TB
    A[Cypher Query] --> B[Parser]
    B --> C[Logical Plan]
    C --> D[Analyzer Pipeline]
    D --> E[Type Inference]
    E --> F[Other Passes]
    F --> G[Renderer]
    
    G --> H{Expand TableAlias}
    H --> I[Fetch ALL properties<br/>from schema]
    I --> J[expand_collect_to_group_array<br/>ALL 100+ columns]
    J --> K[SQL: groupArray tuple<br/>col1, col2, ..., col100]
    
    K --> L[ClickHouse Execution]
    L --> M[Materialize 16MB array]
    M --> N[UNWIND to access<br/>2 properties]
    
    style M fill:#ffcccc
    style J fill:#ffcccc
    style I fill:#ffcccc
```

**Problems** (red boxes):
- ❌ Fetches ALL properties from schema
- ❌ Creates tuple with ALL columns
- ❌ Materializes large arrays unnecessarily

---

## Optimized Architecture (After Implementation)

```mermaid
graph TB
    A[Cypher Query] --> B[Parser]
    B --> C[Logical Plan]
    C --> D[Analyzer Pipeline]
    D --> E[Type Inference]
    E --> F[PropertyRequirements<br/>Analyzer 🆕]
    F --> G{Analyze Plan Tree}
    
    G --> H[Scan RETURN clause<br/>friend.firstName]
    G --> I[Scan WHERE clause<br/>friend.age > 25]
    G --> J[Scan ORDER BY<br/>friend.lastName]
    
    H --> K[PropertyRequirements<br/>Map: friend → firstName, lastName, id]
    I --> K
    J --> K
    
    K --> L[Store in PlanCtx]
    L --> M[Other Passes]
    M --> N[Renderer]
    
    N --> O{Expand TableAlias}
    O --> P[Query PlanCtx<br/>requirements]
    P --> Q[Filter: Only required<br/>properties 3 cols]
    Q --> R[expand_collect_to_group_array<br/>FILTERED]
    R --> S[SQL: groupArray tuple<br/>firstName, lastName, id]
    
    S --> T[ClickHouse Execution]
    T --> U[Materialize 240KB array<br/>98% reduction]
    U --> V[UNWIND to access<br/>2 properties ✓]
    
    style F fill:#ccffcc
    style K fill:#ccffcc
    style Q fill:#ccffcc
    style U fill:#ccffcc
```

**Improvements** (green boxes):
- ✅ New analyzer pass tracks requirements
- ✅ Stores requirements in PlanCtx
- ✅ Filters properties before expansion
- ✅ Materializes only what's needed

---

## Data Flow: Property Requirements

```mermaid
sequenceDiagram
    participant Q as Query
    participant P as Parser
    participant A as PropertyRequirements<br/>Analyzer
    participant C as PlanCtx
    participant R as Renderer
    participant S as SQL Generator
    
    Q->>P: WITH collect(f) as friends<br/>UNWIND friends as friend<br/>RETURN friend.firstName
    P->>A: LogicalPlan tree
    
    Note over A: Bottom-up analysis
    A->>A: Scan RETURN clause
    A->>A: Found: friend.firstName
    A->>A: Lookup ID: friend.id
    
    A->>C: Store requirements:<br/>friend → {firstName, id}
    
    Note over C: Requirements available<br/>for renderer
    
    C->>R: Get requirements for "f"
    R->>R: Filter properties:<br/>ALL 50 → REQUIRED 2
    R->>S: Generate SQL with<br/>2 columns only
    S->>Q: Optimized SQL
```

---

## Property Requirements Flow

```mermaid
graph LR
    A[RETURN friend.firstName] --> B{PropertyRequirements<br/>Analyzer}
    C[WHERE friend.age > 25] --> B
    D[ORDER BY friend.lastName] --> B
    
    B --> E[Merge Requirements]
    E --> F[PropertyRequirements<br/>Map]
    
    F --> G[friend:<br/>firstName<br/>lastName<br/>age<br/>id Always!]
    
    G --> H[expand_collect_to_group_array]
    H --> I[Filter Properties]
    
    I --> J[4 columns<br/>instead of 50]
    
    style G fill:#ccffff
    style J fill:#ccffcc
```

---

## Analyzer Pass Integration

```mermaid
graph TB
    A[Logical Plan] --> B[Analyzer Pipeline]
    
    B --> C[Type Inference]
    C --> D[PropertyRequirements<br/>Analyzer 🆕]
    D --> E[CteColumnResolver]
    E --> F[GraphJoinInference]
    F --> G[Other Passes...]
    
    G --> H[Optimizers]
    H --> I[Renderer]
    
    D -.Store requirements.-> J[PlanCtx]
    J -.Query requirements.-> I
    
    style D fill:#ccffcc
    style J fill:#ccffff
```

**Key Points**:
- ✅ Runs AFTER type inference (needs type info)
- ✅ Runs BEFORE rendering (results used during SQL gen)
- ✅ Non-destructive pass (only analysis, no transformation)
- ✅ Results stored in PlanCtx for renderer access

---

## Edge Case Handling

```mermaid
graph TB
    A[Property Access Patterns] --> B{Analyzer Detection}
    
    B --> C[Simple:<br/>friend.firstName]
    B --> D[Nested:<br/>friend.address.city]
    B --> E[Wildcard:<br/>RETURN friend.*]
    B --> F[Function:<br/>toUpper friend.firstName]
    
    C --> G[Require: firstName]
    D --> H[Require: address]
    E --> I[Require: ALL]
    F --> J[Require: firstName<br/>from function arg]
    
    G --> K[PropertyRequirements]
    H --> K
    I --> K
    J --> K
    
    style E fill:#ffffcc
    style I fill:#ffffcc
```

**Edge Cases**:
- ✅ Nested properties → Require parent property
- ✅ Wildcards → Mark as "require all"
- ✅ Functions → Extract from arguments
- ✅ Always include ID for correctness

---

## Performance Impact Visualization

```mermaid
graph LR
    A[Query: collect f<br/>RETURN f.name] --> B{Table Size}
    
    B --> C[50 columns<br/>LDBC Person]
    B --> D[200 columns<br/>E-commerce]
    
    C --> E[Before:<br/>400 KB<br/>100 ms]
    C --> F[After:<br/>16 KB<br/>12 ms]
    
    D --> G[Before:<br/>16 MB<br/>800 ms]
    D --> H[After:<br/>240 KB<br/>50 ms]
    
    F --> I[8x faster<br/>96% less memory]
    H --> J[16x faster<br/>98.5% less memory]
    
    style E fill:#ffcccc
    style G fill:#ffcccc
    style F fill:#ccffcc
    style H fill:#ccffcc
```

---

## Implementation Phases

```mermaid
gantt
    title Property Pruning Implementation Timeline
    dateFormat  YYYY-MM-DD
    
    section Phase 1: Foundation
    PropertyRequirements struct    :p1a, 2025-01-06, 3d
    PlanCtx integration           :p1b, after p1a, 2d
    Unit tests                    :p1c, after p1b, 1d
    
    section Phase 2: Analysis
    Analyzer skeleton             :p2a, 2025-01-13, 2d
    Bottom-up traversal           :p2b, after p2a, 2d
    Multi-scope WITH handling     :p2c, after p2b, 2d
    Pipeline integration          :p2d, after p2c, 1d
    
    section Phase 3: Expansion
    Update collect expansion      :p3a, 2025-01-20, 2d
    Update CTE expansion          :p3b, after p3a, 2d
    Integration testing           :p3c, after p3b, 2d
    
    section Phase 4: Polish
    Edge cases                    :p4a, 2025-01-27, 2d
    Comprehensive tests           :p4b, after p4a, 2d
    Documentation                 :p4c, after p4b, 1d
```

---

## Three Property Resolvers Working Together

```mermaid
graph TB
    A[Cypher Query:<br/>user.name] --> B[1. property_resolver<br/>Translator Phase]
    
    B --> C[Schema Mapping:<br/>name → full_name]
    C --> D[LogicalExpr:<br/>user.full_name]
    
    D --> E[2. projected_columns_resolver<br/>Early Analyzer]
    E --> F[Cache Available:<br/>GraphNode.projected_columns<br/>= full_name, age, email]
    
    F --> G[3. property_requirements_analyzer<br/>Late Analyzer 🆕]
    G --> H{Bottom-Up Analysis}
    
    H --> I[Scan RETURN:<br/>needs full_name]
    H --> J[Scan WHERE:<br/>needs age]
    
    I --> K[PropertyRequirements:<br/>user → full_name, age, id]
    J --> K
    
    K --> L[Store in PlanCtx]
    
    L --> M[Renderer]
    M --> N[Query projected_columns:<br/>available = 50 props]
    M --> O[Query PropertyRequirements:<br/>needed = 3 props]
    
    N --> P[Filter: 50 → 3]
    O --> P
    
    P --> Q[SQL: SELECT<br/>full_name, age, id]
    
    style B fill:#ffeecc
    style E fill:#ccffee
    style G fill:#ccffcc
    style K fill:#ccffcc
    style P fill:#ccffcc
```

**Three Resolvers, Three Jobs**:
1. **property_resolver**: Schema mapping (Cypher names → DB columns)
2. **projected_columns_resolver**: Cache what's available
3. **property_requirements_analyzer**: Determine what's needed

---

## Multi-Scope Bottom-Up Analysis

```mermaid
graph BT
    A[MATCH a-→b] --> B[WITH collect b]
    B --> C[UNWIND friends]
    C --> D[MATCH friend-→p]
    D --> E[RETURN friend.name]
    
    E -->|Step 1: Need friend.name| F{Requirements<br/>friend: name, id}
    F -->|Step 2: Propagate through MATCH| C
    C -->|Step 3: UNWIND needs name| B
    B -->|Step 4: collect must include name| G{Requirements<br/>b: name, id}
    G -->|Step 5: Propagate to MATCH| A
    
    style E fill:#ffcccc
    style F fill:#ffeecc
    style C fill:#ffffcc
    style B fill:#ccffcc
    style G fill:#ccffcc
    style A fill:#ccffff
    
    classDef bottomUp fill:#e1f5ff,stroke:#333,stroke-width:2px
```

**Key**: Analysis flows **BOTTOM → TOP** (RETURN → MATCH) to correctly propagate requirements through scope boundaries!

---

## Backward Compatibility

```mermaid
graph TB
    A[Query Execution] --> B{PropertyRequirements<br/>Available?}
    
    B -->|Yes| C[Use Optimized Path]
    B -->|No| D[Use Legacy Path<br/>collect ALL]
    
    C --> E[Filter Properties]
    D --> F[Fetch ALL Properties]
    
    E --> G[Optimized SQL<br/>3 columns]
    F --> H[Legacy SQL<br/>100 columns]
    
    G --> I[Fast Execution]
    H --> J[Slower but Safe]
    
    style C fill:#ccffcc
    style E fill:#ccffcc
    style D fill:#ffffcc
    style F fill:#ffffcc
```

**Graceful Degradation**:
- ✅ If analyzer doesn't run → Falls back to current behavior
- ✅ If requirements empty → Collects all properties (safe)
- ✅ No breaking changes to existing queries
- ✅ Optional feature flag for gradual rollout

---

These diagrams illustrate:
1. **Before/After** comparison showing the optimization
2. **Data flow** of property requirements through the system
3. **Integration** of new analyzer pass in pipeline
4. **Edge case** handling strategies
5. **Performance impact** visualization
6. **Implementation timeline** as Gantt chart
7. **Backward compatibility** safety measures
