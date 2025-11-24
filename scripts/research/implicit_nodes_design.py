"""
Case Study: Implicit Nodes from Edge Table

Real-world example: Flight data with a single table

Table: flights
- origin (airport code like 'SFO', 'JFK')
- destination (airport code like 'LAX', 'ORD')
- flight_number
- departure_time
- arrival_time
- airline
- ...

Question: How to model this as a graph with Airport nodes and Flight edges?
The airport data is IMPLICIT - just codes in the origin/destination columns!
"""

print("="*80)
print("IMPLICIT NODES FROM EDGE TABLE - DESIGN OPTIONS")
print("="*80)
print()

print("Real-World Example: Flight Data")
print("-" * 70)
print()
print("Table: flights")
print("  origin        | destination | flight_number | airline | ...")
print("  SFO          | LAX         | UA123         | United  | ...")
print("  JFK          | ORD         | AA456         | American| ...")
print("  LAX          | SFO         | DL789         | Delta   | ...")
print()
print("Graph Model:")
print("  Nodes: Airports (SFO, LAX, JFK, ORD, ...)")
print("  Edges: Flights (origin -> destination)")
print()
print("Challenge: Airport data doesn't exist in a separate table!")
print()

print("="*80)
print("OPTION 1: VIRTUAL NODES (Recommended)")
print("="*80)
print()

print("Concept: Nodes are 'virtual' - derived from edge table columns")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    # No table! Virtual node
    virtual: true
    # OR use a special marker
    table: null  # Indicates virtual node
    node_id: code  # The identifier is just the airport code
    
edges:
  - name: FLIGHT
    table: flights
    from_id: origin       # References Airport.code
    to_id: destination    # References Airport.code
    edge_id: [origin, destination, flight_number, departure_time]
    properties:
      - name: airline
        column: airline
      - name: departure_time
        column: departure_time
""")
print()

print("How it works:")
print("  1. When user queries: MATCH (a:Airport)")
print("     → ClickGraph generates: SELECT DISTINCT origin AS code FROM flights")
print("        UNION SELECT DISTINCT destination AS code FROM flights")
print()
print("  2. When user queries: MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)")
print("     → Normal join on flights table")
print()
print("Benefits:")
print("  ✅ No data duplication")
print("  ✅ Always in sync (derived from actual edges)")
print("  ✅ Simple schema definition")
print()
print("Drawbacks:")
print("  ⚠️ Can't have airport properties (name, location, etc.)")
print("  ⚠️ Queries on Airport alone could be slow (DISTINCT + UNION)")
print()

print("="*80)
print("OPTION 2: SELF-REFERENCING TABLE")
print("="*80)
print()

print("Concept: Same table serves as both node table and edge table")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: flights  # Same table!
    node_id: origin  # Use origin column as node identity
    # But this is weird - what about destination?
    
  - name: Airport2  # Duplicate definition?
    table: flights
    node_id: destination
    
# This approach is confusing and doesn't work well
""")
print()
print("Issues:")
print("  ❌ Doesn't naturally capture the graph structure")
print("  ❌ Airport nodes would have flight properties (wrong!)")
print("  ❌ Hard to query just airports")
print()

print("="*80)
print("OPTION 3: EDGE TABLE AS PRIMARY, INFER NODES")
print("="*80)
print()

print("Concept: Edge table is primary, nodes are automatically inferred")
print()
print("Schema YAML:")
print("""
edges:
  - name: FLIGHT
    table: flights
    from_id: origin
    to_id: destination
    edge_id: [origin, destination, flight_number, departure_time]
    
    # NEW: Declare what the endpoints represent
    from_node_type: Airport
    to_node_type: Airport
    
# Nodes are automatically inferred!
# System knows: Airport nodes exist, with IDs from origin/destination columns
""")
print()

print("How it works:")
print("  1. System sees from_node_type/to_node_type → creates virtual Airport nodes")
print("  2. MATCH (a:Airport) → SELECT DISTINCT origin FROM flights UNION ...")
print("  3. MATCH (a)-[f:FLIGHT]->(b) → Normal join")
print()
print("Benefits:")
print("  ✅ Clean schema (edges declare their endpoints)")
print("  ✅ Nodes automatically inferred")
print("  ✅ Clear semantics")
print()

print("="*80)
print("OPTION 4: HYBRID - OPTIONAL NODE TABLE")
print("="*80)
print()

print("Concept: Nodes can have an optional table for properties")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    # Optional table with airport details
    table: airports  # If exists: JOIN for properties
                     # If null/missing: Virtual node (no properties)
    node_id: code
    # If table exists, these work:
    properties:
      - name: name
        column: airport_name
      - name: city
        column: city
        
edges:
  - name: FLIGHT
    table: flights
    from_id: origin       # Always required
    to_id: destination    # Always required
    from_node_type: Airport  # Links to node definition
    to_node_type: Airport
    edge_id: [origin, destination, flight_number, departure_time]
""")
print()

print("How it works:")
print("  Case A: airports table exists")
print("    MATCH (a:Airport) → SELECT * FROM airports")
print("    MATCH (a)-[f]->(b) → JOIN flights with airports on origin/destination")
print()
print("  Case B: airports table missing")
print("    MATCH (a:Airport) → SELECT DISTINCT origin FROM flights UNION ...")
print("    MATCH (a)-[f]->(b) → SELECT * FROM flights (no node table join)")
print()
print("Benefits:")
print("  ✅ Flexible: works with or without node table")
print("  ✅ Can add node table later for properties")
print("  ✅ Clean migration path")
print()

print("="*80)
print("RECOMMENDATION: OPTION 4 (HYBRID)")
print("="*80)
print()

print("Why Hybrid is best:")
print("  1. ✅ Handles PuppyGraph scenario (no node table)")
print("  2. ✅ Handles traditional graph DBs (separate node table)")
print("  3. ✅ Provides migration path")
print("  4. ✅ Schema is explicit about relationships")
print()

print("Implementation:")
print()
print("Schema Fields:")
print("""
nodes:
  - name: Airport
    table: airports  # Can be null/omitted for virtual nodes
    node_id: code
    # If table is null, node is VIRTUAL (derived from edges)
    
edges:
  - name: FLIGHT
    table: flights  # Always required
    from_id: origin
    to_id: destination
    from_node_type: Airport  # NEW: Links to node type
    to_node_type: Airport    # NEW: Links to node type
    edge_id: [flight_id]
""")
print()

print("Query Planning:")
print()
print("1. MATCH (a:Airport) RETURN a")
print("   → Check if Airport has table")
print("   → If YES: SELECT * FROM airports")
print("   → If NO: SELECT DISTINCT code FROM (")
print("            SELECT origin AS code FROM flights")
print("            UNION")
print("            SELECT destination AS code FROM flights)")
print()

print("2. MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)")
print("   → If Airport has table:")
print("     SELECT * FROM airports a")
print("     JOIN flights f ON f.origin = a.code")
print("     JOIN airports b ON f.destination = b.code")
print()
print("   → If Airport has NO table:")
print("     SELECT * FROM flights f")
print("     -- Virtual nodes: origin and destination ARE the node IDs")
print()

print("3. MATCH (a:Airport {code: 'SFO'})-[f:FLIGHT]->(b)")
print("   → If Airport has table:")
print("     SELECT * FROM airports a")
print("     JOIN flights f ON f.origin = a.code")
print("     JOIN airports b ON f.destination = b.code")
print("     WHERE a.code = 'SFO'")
print()
print("   → If Airport has NO table:")
print("     SELECT * FROM flights f")
print("     WHERE f.origin = 'SFO'")
print("     -- a.code is just f.origin (virtual)")
print()

print("="*80)
print("SCHEMA EXAMPLE: FLIGHT DATA")
print("="*80)
print()

print("Minimal (PuppyGraph style):")
print("""
nodes:
  - name: Airport
    # No table - virtual node
    node_id: code
    
edges:
  - name: FLIGHT
    table: flights
    from_id: origin
    to_id: destination
    from_node_type: Airport
    to_node_type: Airport
    edge_id: [origin, destination, flight_number, departure_time]
    properties:
      - name: airline
        column: airline
      - name: distance
        column: distance_miles
""")
print()

print("With Airport Properties (if table exists):")
print("""
nodes:
  - name: Airport
    table: airports  # Now has a table!
    node_id: code
    properties:
      - name: name
        column: airport_name
      - name: city
        column: city
      - name: country
        column: country
        
edges:
  - name: FLIGHT
    table: flights
    from_id: origin
    to_id: destination
    from_node_type: Airport
    to_node_type: Airport
    edge_id: [origin, destination, flight_number, departure_time]
    properties:
      - name: airline
        column: airline
""")
print()

print("="*80)
print("IMPLEMENTATION CONSIDERATIONS")
print("="*80)
print()

print("1. Schema Validation:")
print("   - If node has no table, must be referenced by at least one edge")
print("   - Virtual nodes can't have properties (no table to get them from)")
print("   - from_node_type/to_node_type must reference defined nodes")
print()

print("2. Query Optimization:")
print("   - Virtual nodes: cache DISTINCT results")
print("   - Avoid unnecessary UNION if only one direction needed")
print("   - Push filters down to edge table when possible")
print()

print("3. Property Access:")
print("   - Virtual nodes: only ID is available (a.code)")
print("   - With table: all properties available (a.name, a.city)")
print()

print("4. Edge Definition:")
print("   - from_node_type/to_node_type should be optional")
print("   - If omitted, infer from schema (look for node with matching from_id)")
print()

print("Benefits of This Approach:")
print("  ✅ Handles PuppyGraph benchmark case perfectly")
print("  ✅ Works with traditional graph schemas")
print("  ✅ No data duplication required")
print("  ✅ Follows SQL/PGQ model (edges define structure)")
print("  ✅ Clean migration: start without node table, add later")
print()

print("Real-World Use Cases:")
print("  - Flight networks (airports implicit in flights)")
print("  - Social networks (users implicit in follows/messages)")
print("  - Transaction graphs (accounts implicit in transfers)")
print("  - Web graphs (pages implicit in links)")
print("  - Citation networks (papers implicit in citations)")
print()
