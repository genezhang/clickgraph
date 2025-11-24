"""
Schema Organization Comparison: Where to put denormalized node properties?

Two approaches for organizing the same information
"""

print("="*80)
print("SCHEMA ORGANIZATION: TWO APPROACHES")
print("="*80)
print()

print("Context: OnTime dataset with denormalized node properties in edge table")
print()
print("Same information, different organization:")
print("  - Airport nodes with properties (city, state)")
print("  - Flight edges connecting airports")
print("  - Properties stored in edge table (OriginCityName, DestCityName, etc.)")
print()

print("="*80)
print("APPROACH 1: PROPERTIES IN EDGE DEFINITION")
print("="*80)
print()

print("Rationale: Properties physically live in edge table → define them there")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: null  # Virtual node (no dedicated table)
    node_id: code

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    
    # Node properties defined HERE (in edge)
    from_node_properties:
      city: OriginCityName
      state: OriginState
      state_name: OriginStateName
      
    to_node_properties:
      city: DestCityName
      state: DestState
      state_name: DestStateName
    
    edge_id: [FlightDate, FlightNum, Origin, Dest]
    properties:
      - name: carrier
        column: Carrier
      - name: distance
        column: Distance
""")
print()

print("Pros:")
print("  ✅ Source of truth: properties defined where they physically exist")
print("  ✅ Clear ownership: this edge table provides these node properties")
print("  ✅ Easy to see: all table columns mapped in one place")
print("  ✅ Multiple edges can provide same node properties (flexibility)")
print("  ✅ Follows 'locality principle': edge definition is complete")
print()

print("Cons:")
print("  ⚠️ Node definition incomplete: can't see all node properties")
print("  ⚠️ Scattered: need to check all edges to know node schema")
print("  ⚠️ Duplication: if multiple edges have same node, repeat mappings")
print()

print("="*80)
print("APPROACH 2: PROPERTIES IN NODE DEFINITION")
print("="*80)
print()

print("Rationale: Properties belong to node → define them in node schema")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: null  # Virtual node (no dedicated table)
    node_id: code
    
    # Node properties defined HERE (in node)
    # Source: derive from edge tables
    derived_properties:
      - source_edge: FLIGHT
        source_table: ontime
        when_role: from_node  # When Airport is the origin
        mappings:
          city: OriginCityName
          state: OriginState
          state_name: OriginStateName
          
      - source_edge: FLIGHT
        source_table: ontime
        when_role: to_node  # When Airport is the destination
        mappings:
          city: DestCityName
          state: DestState
          state_name: DestStateName

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum, Origin, Dest]
    properties:
      - name: carrier
        column: Carrier
      - name: distance
        column: Distance
""")
print()

print("Pros:")
print("  ✅ Complete node schema: all node info in one place")
print("  ✅ Node-centric: matches conceptual model (Airport has city/state)")
print("  ✅ Single source of truth for node schema")
print("  ✅ Easy to see all Airport properties at a glance")
print()

print("Cons:")
print("  ⚠️ More verbose: need to specify source_edge, when_role")
print("  ⚠️ Reverse reference: node references edge (unusual)")
print("  ⚠️ Edge definition incomplete: can't see what columns are used")
print("  ⚠️ Harder to validate: need to check if columns exist in edge table")
print()

print("="*80)
print("APPROACH 3: HYBRID - REFERENCE FROM NODE TO EDGE")
print("="*80)
print()

print("Simplified version of Approach 2 with cleaner syntax")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: null  # Virtual node
    node_id: code
    
    # Declare properties and their source
    properties:
      - name: city
        # Get from edge tables where this node appears
        from_edges:
          - edge: FLIGHT
            from_column: OriginCityName  # When Airport is origin
            to_column: DestCityName      # When Airport is destination
            
      - name: state
        from_edges:
          - edge: FLIGHT
            from_column: OriginState
            to_column: DestState
            
      - name: state_name
        from_edges:
          - edge: FLIGHT
            from_column: OriginStateName
            to_column: DestStateName

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum]
    properties:
      - name: carrier
        column: Carrier
""")
print()

print("Pros:")
print("  ✅ Node-centric view (all node properties in node definition)")
print("  ✅ Property-oriented (declare each property once)")
print("  ✅ Clear semantics (from_column vs to_column)")
print()

print("Cons:")
print("  ⚠️ Verbose for many properties")
print("  ⚠️ Node references edge (reverse dependency)")
print()

print("="*80)
print("ANALYSIS: WHICH IS MORE NATURAL?")
print("="*80)
print()

print("Key Questions:")
print()

print("1. Where does the data physically live?")
print("   → In the EDGE table (ontime)")
print("   → Suggests: Define in edge (Approach 1)")
print()

print("2. What is the conceptual ownership?")
print("   → Properties belong to NODE (Airport has city/state)")
print("   → Suggests: Define in node (Approach 2/3)")
print()

print("3. Who is the primary consumer?")
print("   → User queries: MATCH (a:Airport) RETURN a.city")
print("   → User thinks about NODE properties")
print("   → Suggests: Define in node (Approach 2/3)")
print()

print("4. What about schema dependencies?")
print("   → Edges already reference nodes (from_node, to_node)")
print("   → Adding node → edge dependency creates circular reference")
print("   → Suggests: Keep dependencies one-way, define in edge (Approach 1)")
print()

print("5. What if multiple edges provide same node properties?")
print("   Example: FLIGHT and RAIL_TRIP both have airport endpoints")
print()
print("   Approach 1 (in edge):")
print("""
   edges:
     - name: FLIGHT
       from_node_properties:
         city: OriginCityName
     - name: RAIL_TRIP
       from_node_properties:
         city: OriginCity  # Different column name!
   """)
print("   → Each edge declares its own mappings (flexible)")
print()
print("   Approach 2 (in node):")
print("""
   nodes:
     - name: Airport
       derived_properties:
         - source_edge: FLIGHT
           mappings: {city: OriginCityName}
         - source_edge: RAIL_TRIP
           mappings: {city: OriginCity}
   """)
print("   → Need to list all edges in node (tighter coupling)")
print()

print("6. What about validation?")
print()
print("   Approach 1: Easy to validate")
print("   - Edge table known → check if columns exist")
print()
print("   Approach 2: Harder to validate")
print("   - Need to find edge definition first")
print("   - Need to check edge table for columns")
print()

print("="*80)
print("RECOMMENDATION: APPROACH 1 (Properties in Edge Definition)")
print("="*80)
print()

print("Why Approach 1 is more natural:")
print()

print("1. ✅ Physical location: Data lives in edge table")
print("   → Natural to define where it lives")
print()

print("2. ✅ Dependency direction: Edge → Node (one-way)")
print("   → Don't create circular dependency")
print()

print("3. ✅ Validation: Easy to check columns exist")
print("   → Edge knows its table, can validate immediately")
print()

print("4. ✅ Flexibility: Each edge can have different mappings")
print("   → Different edge tables may use different column names")
print()

print("5. ✅ Extensibility: Add new edges without changing node")
print("   → Node definition stays stable")
print()

print("6. ✅ Follows SQL/PGQ model:")
print("   → Edges are primary in property graph mapping")
print("   → Nodes can be derived from edges")
print()

print("7. ✅ Practical: When you define edge table mapping,")
print("      you're looking at the table schema")
print("   → Natural to map all columns (edge + node properties) together")
print()

print("Trade-off: Node schema is distributed")
print("   - To see all Airport properties, check all edges")
print("   - BUT: This matches reality! Properties ARE distributed")
print("   - Solution: Tooling can aggregate for display")
print()

print("="*80)
print("RECOMMENDED SCHEMA STRUCTURE")
print("="*80)
print()

print("Final schema (Approach 1 - refined):")
print("""
nodes:
  - name: Airport
    table: null  # or omit - means virtual/derived node
    node_id: code
    # No property definitions here for virtual nodes

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport  # Already exists ✅
    to_node: Airport    # Already exists ✅
    
    # NEW: Denormalized node properties
    from_node_properties:
      city: OriginCityName
      state: OriginState
      state_name: OriginStateName
      
    to_node_properties:
      city: DestCityName
      state: DestState
      state_name: DestStateName
    
    edge_id: [FlightDate, FlightNum, Origin, Dest]
    
    # Edge properties (as usual)
    properties:
      - name: carrier
        column: Carrier
      - name: distance
        column: Distance
""")
print()

print("When node has dedicated table (comparison):")
print("""
nodes:
  - name: Airport
    table: airports  # Has dedicated table
    node_id: code
    properties:
      - name: city
        column: city_name
      - name: state
        column: state_code

edges:
  - name: FLIGHT
    table: flights
    from_id: origin
    to_id: destination
    from_node: Airport
    to_node: Airport
    # No from_node_properties/to_node_properties
    # → System knows to JOIN with airports table
    
    edge_id: [flight_id]
    properties:
      - name: carrier
        column: airline_code
""")
print()

print("Query Planning Logic:")
print()
print("For: MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.city, b.city")
print()
print("1. Check if Airport has table:")
print("   - If YES: JOIN with airports table for properties")
print("   - If NO: Check edge for from_node_properties/to_node_properties")
print()
print("2. If edge has node properties:")
print("   - Use direct column access: f.OriginCityName, f.DestCityName")
print("   - No JOIN needed! ✅")
print()
print("3. If edge doesn't have node properties:")
print("   - Node is truly virtual (ID only)")
print("   - Only a.code available (no other properties)")
print()

print("="*80)
print("COMPARISON SUMMARY")
print("="*80)
print()

print("┌────────────────────────┬───────────────┬───────────────┐")
print("│ Criterion              │ Approach 1    │ Approach 2    │")
print("│                        │ (In Edge)     │ (In Node)     │")
print("├────────────────────────┼───────────────┼───────────────┤")
print("│ Physical location      │ ✅ Matches    │ ⚠️ Indirect   │")
print("│ Dependency direction   │ ✅ One-way    │ ⚠️ Circular   │")
print("│ Validation             │ ✅ Easy       │ ⚠️ Complex    │")
print("│ Extensibility          │ ✅ Good       │ ⚠️ Coupling   │")
print("│ Node schema clarity    │ ⚠️ Scattered  │ ✅ Complete   │")
print("│ Edge definition        │ ✅ Complete   │ ⚠️ Incomplete │")
print("│ SQL/PGQ alignment      │ ✅ Yes        │ ⚠️ Different  │")
print("│ Practical workflow     │ ✅ Natural    │ ⚠️ Awkward    │")
print("└────────────────────────┴───────────────┴───────────────┘")
print()

print("Winner: APPROACH 1 (Properties in Edge Definition)")
print()
print("Key insight: In denormalized data, the edge table IS the source")
print("of truth for both edge and node properties. Define them together!")
print()
