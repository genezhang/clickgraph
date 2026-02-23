// Q13. Single shortest path (adapted)
// Original uses shortestPath() which is a special Cypher function.
// Adapted to use VLP *1..20 with path variable and length(path).
// Returns empty result (not -1) when no path exists.
/*
:params { person1Id: 8796093022390, person2Id: 8796093022357 }
*/
MATCH path = (person1:Person {id: $person1Id})-[:KNOWS*1..20]-(person2:Person {id: $person2Id})
RETURN length(path) AS shortestPathLength
ORDER BY shortestPathLength ASC
LIMIT 1
