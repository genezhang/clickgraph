"""
OnTime Flight Dataset - Denormalized Edge Table Design

Real-world dataset: https://clickhouse.com/docs/getting-started/example-datasets/ontime

Challenge: Single denormalized table with BOTH edge data AND node properties!
"""

print("="*80)
print("ONTIME DATASET - DENORMALIZED EDGE TABLE")
print("="*80)
print()

print("Table Structure (simplified):")
print("-" * 70)
print("""
ontime_table:
  -- Edge identity (flight)
  FlightDate            Date
  FlightNum             String
  
  -- Origin Airport (node fields!)
  Origin                String      -- Airport code (e.g., 'SFO')
  OriginCityName        String      -- 'San Francisco, CA'
  OriginState           String      -- 'CA'
  OriginStateName       String      -- 'California'
  
  -- Destination Airport (node fields!)
  Dest                  String      -- Airport code (e.g., 'LAX')
  DestCityName          String      -- 'Los Angeles, CA'
  DestState             String      -- 'CA'
  DestStateName         String      -- 'California'
  
  -- Flight edge properties
  DepTime               UInt16
  ArrTime               UInt16
  AirTime               UInt16
  Distance              UInt16
  Cancelled             UInt8
  Diverted              UInt8
  
  -- Airline info
  Carrier               String
  TailNum               String
  ...
""")
print()

print("Graph Model:")
print("  Nodes: Airport (code, city, state, state_name)")
print("  Edges: Flight (date, flight_num, airline, times, distance)")
print()

print("Key Insight: Airport properties are DUPLICATED for each flight!")
print("  - Origin properties: OriginCityName, OriginState, OriginStateName")
print("  - Dest properties: DestCityName, DestState, DestStateName")
print()

print("="*80)
print("DESIGN CHALLENGE")
print("="*80)
print()

print("Problem: How to expose Airport node properties from edge table?")
print()
print("Naive approach (WRONG):")
print("""
nodes:
  - name: Airport
    table: null  # Virtual
    node_id: code
    # Can't specify properties - no node table!
""")
print()
print("❌ This doesn't work - can't get city, state from virtual node")
print()

print("="*80)
print("SOLUTION 1: PROPERTY PREFIXES (Recommended)")
print("="*80)
print()

print("Concept: Map node properties from edge table columns with prefixes")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: null  # Virtual node (no dedicated table)
    node_id: code
    
    # NEW: Property mappings from edge tables!
    derived_properties:
      - source_table: ontime
        source_role: origin  # or 'destination'
        mappings:
          city: OriginCityName      # When Airport is origin
          state: OriginState
          state_name: OriginStateName
          
      - source_table: ontime
        source_role: destination
        mappings:
          city: DestCityName        # When Airport is destination
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
      - name: dep_time
        column: DepTime
""")
print()

print("How it works:")
print()
print("1. MATCH (a:Airport {code: 'SFO'}) RETURN a.city, a.state")
print()
print("   → Need to get city/state for airport 'SFO'")
print("   → SFO could appear as Origin OR Dest")
print()
print("   Generated SQL:")
print("""
   SELECT 
     code,
     any(city) AS city,
     any(state) AS state
   FROM (
     SELECT Origin AS code, OriginCityName AS city, OriginState AS state
     FROM ontime
     WHERE Origin = 'SFO'
     
     UNION ALL
     
     SELECT Dest AS code, DestCityName AS city, DestState AS state
     FROM ontime
     WHERE Dest = 'SFO'
   )
   GROUP BY code
   """)
print()
print("   Note: any() because same airport might appear multiple times")
print("         Properties should be consistent (same city for same code)")
print()

print("2. MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.city, b.city, f.distance")
print()
print("   Generated SQL:")
print("""
   SELECT 
     f.Origin AS a_code,
     f.OriginCityName AS a_city,        -- ✅ Direct access!
     f.Dest AS b_code,
     f.DestCityName AS b_city,          -- ✅ Direct access!
     f.Distance AS f_distance
   FROM ontime f
   """)
print()
print("   ✅ No JOIN needed! Properties already in edge table!")
print()

print("="*80)
print("SOLUTION 2: COLUMN PATTERN MATCHING")
print("="*80)
print()

print("Concept: Auto-detect node properties by column name patterns")
print()
print("Schema YAML:")
print("""
edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    
    # NEW: Declare node property patterns
    from_node_properties:
      # Pattern: Origin{PropertyName} → Airport.{property_name}
      city: OriginCityName
      state: OriginState
      state_name: OriginStateName
      
    to_node_properties:
      # Pattern: Dest{PropertyName} → Airport.{property_name}
      city: DestCityName
      state: DestState
      state_name: DestStateName
      
    edge_id: [FlightDate, FlightNum, Origin, Dest]
    properties:
      - name: carrier
        column: Carrier
""")
print()

print("Benefits:")
print("  ✅ Simpler schema (no separate node definition needed)")
print("  ✅ Properties co-located with edge definition")
print("  ✅ Clear which columns map to which node")
print()

print("Query Generation:")
print()
print("MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)")
print("RETURN a.code, a.city, b.code, b.city, f.carrier")
print()
print("SQL:")
print("""
SELECT 
  f.Origin AS a_code,
  f.OriginCityName AS a_city,      -- from from_node_properties
  f.Dest AS b_code,
  f.DestCityName AS b_city,        -- from to_node_properties
  f.Carrier AS f_carrier           -- from edge properties
FROM ontime f
""")
print()

print("="*80)
print("SOLUTION 3: HYBRID - OPTIONAL NODE TABLE WITH FALLBACK")
print("="*80)
print()

print("Concept: Try node table first, fallback to edge table properties")
print()
print("Schema YAML:")
print("""
nodes:
  - name: Airport
    table: airports  # Optional - if exists, use it
                     # If doesn't exist, derive from edges
    node_id: code
    properties:
      - name: city
        column: city_name
      - name: state
        column: state_code
        
    # Fallback: if table doesn't exist, get properties from edges
    fallback_sources:
      - edge: FLIGHT
        from_properties:
          city: OriginCityName
          state: OriginState
        to_properties:
          city: DestCityName
          state: DestState

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum]
""")
print()

print("Query Planning:")
print()
print("IF airports table exists:")
print("  → JOIN with airports table for properties")
print()
print("IF airports table missing:")
print("  → Use fallback_sources from edge table")
print()

print("="*80)
print("RECOMMENDATION: SOLUTION 2 (Column Pattern Matching)")
print("="*80)
print()

print("Why this is best for denormalized data:")
print()
print("1. ✅ Clear and explicit - schema shows exactly what maps where")
print("2. ✅ Efficient - direct column access, no UNION needed")
print("3. ✅ Simple to implement - just field mappings")
print("4. ✅ Matches SQL/PGQ model (edges can have node properties)")
print("5. ✅ Works perfectly for OnTime dataset")
print()

print("Complete OnTime Schema Example:")
print("""
edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    
    # Origin Airport properties (denormalized)
    from_node_properties:
      city: OriginCityName
      state: OriginState
      state_name: OriginStateName
      
    # Destination Airport properties (denormalized)
    to_node_properties:
      city: DestCityName
      state: DestState  
      state_name: DestStateName
    
    # Flight edge properties
    edge_id: [FlightDate, FlightNum, Origin, Dest]
    properties:
      - name: carrier
        column: Carrier
      - name: tail_num
        column: TailNum
      - name: dep_time
        column: DepTime
      - name: arr_time
        column: ArrTime
      - name: air_time
        column: AirTime
      - name: distance
        column: Distance
      - name: cancelled
        column: Cancelled
      - name: diverted
        column: Diverted
""")
print()

print("Query Examples:")
print()
print("1. Find airports in California:")
print("   MATCH (a:Airport) WHERE a.state = 'CA' RETURN a.code, a.city")
print()
print("   SQL (via edges):")
print("""
   SELECT DISTINCT code, city
   FROM (
     SELECT Origin AS code, OriginCityName AS city 
     FROM ontime WHERE OriginState = 'CA'
     UNION ALL
     SELECT Dest AS code, DestCityName AS city
     FROM ontime WHERE DestState = 'CA'
   )
   """)
print()

print("2. Flights between California airports:")
print("   MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)")
print("   WHERE a.state = 'CA' AND b.state = 'CA'")
print("   RETURN a.code, b.code, f.carrier, f.distance")
print()
print("   SQL:")
print("""
   SELECT 
     Origin AS a_code,
     Dest AS b_code,
     Carrier AS carrier,
     Distance AS distance
   FROM ontime
   WHERE OriginState = 'CA' 
     AND DestState = 'CA'
   """)
print("   ✅ Simple! No complex joins!")
print()

print("="*80)
print("IMPLEMENTATION CONSIDERATIONS")
print("="*80)
print()

print("1. Node Query Generation:")
print("   - MATCH (a:Airport) needs to UNION origin and dest columns")
print("   - Apply property filters to both sides")
print("   - Use DISTINCT or GROUP BY to deduplicate")
print()

print("2. Edge Traversal:")
print("   - Direct column access for node properties")
print("   - No JOIN needed if properties in edge table")
print("   - Much faster than JOIN with separate node table!")
print()

print("3. Property Consistency:")
print("   - Assume denormalized properties are consistent")
print("   - Same airport code should have same city/state")
print("   - Use any() or argMax() if needed")
print()

print("4. Mixed Mode:")
print("   - Some edges denormalized, some use separate node tables")
print("   - Query planner checks per-edge basis")
print("   - Fallback to UNION if no denormalized properties")
print()

print("5. Schema Validation:")
print("   - Warn if from_node_properties/to_node_properties missing")
print("   - Suggest user add mappings for better performance")
print()

print("Benefits for PuppyGraph Benchmark:")
print("  ✅ Matches their exact use case")
print("  ✅ No data duplication/preprocessing needed")
print("  ✅ Direct use of OnTime dataset")
print("  ✅ Optimal query performance (no extra JOINs)")
print("  ✅ More complete graph model than minimal version")
print()
