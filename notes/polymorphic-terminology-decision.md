# Polymorphic Relationships: Terminology Decision

**Date**: November 19, 2025  
**Status**: Design Phase - Terminology Finalized

## Decision: Use "Label" Terminology

**Chosen Terminology**:
- `from_label_column` / `from_label_value` (for source node label discrimination)
- `to_label_column` / `to_label_value` (for target node label discrimination)

**Rejected Alternative**:
- ~~`from_type_column` / `from_type_value`~~ (confusing with relationship types)

## Rationale

### Industry Standards Alignment

**Neo4j Property Graph Model** (Most popular graph database):
- Nodes: Have **labels** (zero to many)
- Relationships: Have **types** (exactly one)
- Example: `(p:Person)-[:LIVES_IN]->(c:City)` where `:Person` is a node label, `:LIVES_IN` is a relationship type

**GQL (ISO/IEC 39075:2024)** (International Standard):
- Property graph data model uses **label** terminology for classifying nodes
- Edges/relationships have **types**
- Official ISO standard for graph query languages

**ClickGraph Codebase**:
- Already uses `label` field in `NodeSchema` and `RelationshipSchema`
- Cypher queries reference node labels: `MATCH (u:User)` - `:User` is the label

### Consistency with Property Graph Model

The property graph model distinguishes:
- **Node Labels**: Classify what kind of entity a node represents (User, Post, etc.)
- **Relationship Types**: Classify what kind of connection exists (FOLLOWS, LIKED, etc.)

Our polymorphic relationships feature adds:
- **`type_column`/`type_value`**: Discriminate relationship types (aligns with "relationship type")
- **`from_label_column`/`from_label_value`**: Discriminate source node labels (aligns with "node label")
- **`to_label_column`/`to_label_value`**: Discriminate target node labels (aligns with "node label")

### Examples from Industry

**Neo4j Cypher**:
```cypher
MATCH (u:User)-[:FOLLOWS]->(p:Post)
-- Node labels: User, Post
-- Relationship type: FOLLOWS
```

**GQL Standard** (from Wikipedia):
```gql
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN p.first_name, c.name
-- Labels classify nodes: Person, City
-- Type classifies relationship: LIVES_IN
```

**ClickGraph Schema** (our polymorphic config):
```yaml
relationships:
  - type: FOLLOWS                      # Relationship type
    table: relationships
    type_column: relation_type         # Column storing relationship type
    type_value: "FOLLOWS"              # Value identifying this type
    from_label_column: head_type       # Column storing source node label ✅
    from_label_value: "User"           # Value identifying source label ✅
    to_label_column: tail_type         # Column storing target node label ✅
    to_label_value: "User"             # Value identifying target label ✅
```

## Benefits of "Label" Terminology

1. **Standards Compliance**: Aligns with Neo4j and ISO GQL terminology
2. **Consistency**: Matches existing codebase (`label` field in structs)
3. **Clarity**: Distinguishes node classification (labels) from relationship classification (types)
4. **User Familiarity**: Graph database users already understand node labels vs relationship types
5. **Future-Proof**: If we add polymorphic nodes later, terminology is already consistent

## Implementation Impact

**Files to Use Label Terminology**:
- `src/graph_catalog/config.rs`: `RelationshipDefinition` struct
- `src/graph_catalog/graph_schema.rs`: `RelationshipSchema` struct
- `src/query_planner/analyzer/view_resolver.rs`: ViewScan creation
- YAML schema files: User-facing configuration
- Documentation: All examples and guides

**Field Names**:
```rust
pub struct RelationshipDefinition {
    // Relationship type discrimination
    pub type_column: Option<String>,
    pub type_value: Option<String>,
    
    // Source node label discrimination
    pub from_label_column: Option<String>,  // ✅ "label" not "type"
    pub from_label_value: Option<String>,
    
    // Target node label discrimination
    pub to_label_column: Option<String>,    // ✅ "label" not "type"
    pub to_label_value: Option<String>,
}
```

## References

- **Neo4j Docs**: https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/
  - "Nodes can have zero or more labels to define (classify) what kind of nodes they are"
  - "Relationships must have a type (one type) to define (classify) what type of relationship they are"

- **GQL Standard**: ISO/IEC 39075:2024
  - Property graph model: nodes have labels, edges have types
  - Wikipedia: https://en.wikipedia.org/wiki/Graph_Query_Language

- **PuppyGraph**: Uses `label` field for edge/relationship names (single label per edge)

## Related Documents

- `notes/polymorphic-relationships-design.md` - Full design document (updated to use label terminology)
- `STATUS.md` - Project status (to be updated when feature is implemented)
- `CHANGELOG.md` - Release notes (to be updated when feature ships)

---

**Decision Approved**: November 19, 2025  
**Next Step**: Await user approval of complete design before implementation
