// Test path variable return - Phase 2.5

// Query 1: Basic path variable return
MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p

// Query 2: Path variable with properties
MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p, a.name AS start_name, b.name AS end_name

// Query 3: Just the path nodes and length (for comparison)
MATCH path = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path

// Query 4: Multiple return items including path
MATCH p = (a:Person)-[:FOLLOWS*1..3]-(b:Person)
WHERE a.name = 'Alice'
RETURN p, a, b
