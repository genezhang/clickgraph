// Test 1: Shortest path with WHERE clause on both start and end nodes
MATCH shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN a.name, b.name;

// Test 2: Shortest path with WHERE clause on start node only
MATCH shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice'
RETURN a.name, b.name;

// Test 3: Shortest path with WHERE clause on end node only
MATCH shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE b.name = 'Bob'
RETURN a.name, b.name;

// Test 4: Variable-length path with WHERE clause
MATCH (a:Person)-[:FOLLOWS*1..3]-(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Charlie'
RETURN a.name, b.name;
