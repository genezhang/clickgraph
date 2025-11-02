// Test queries for path functions (Phase 2.6)

// Test 1: length(p) function
MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN length(p) AS path_length

// Test 2: nodes(p) function
MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN nodes(p) AS path_nodes

// Test 3: Multiple path functions
MATCH p = (a:Person)-[:FOLLOWS*1..3]-(b:Person)
WHERE a.name = 'Alice'
RETURN p, length(p) AS hops, nodes(p) AS node_ids

// Test 4: Path function in WHERE clause
MATCH p = (a:Person)-[:FOLLOWS*]-(b:Person)
WHERE length(p) <= 3
RETURN p, length(p)

// Test 5: Path function in ORDER BY
MATCH p = (a:Person)-[:FOLLOWS*1..5]-(b:Person)
WHERE a.name = 'Alice'
RETURN p, length(p) AS distance
ORDER BY length(p)

// Test 6: relationships(p) function (returns empty array for now)
MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice'
RETURN relationships(p) AS rels

// Test 7: Combined - all three functions
MATCH p = (a:Person)-[:FOLLOWS*1..4]-(b:Person)
WHERE a.name = 'Alice'
RETURN length(p), nodes(p), relationships(p)
