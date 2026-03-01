// Q14. Trusted connection paths (adapted for ClickGraph)
// Original uses GDS gds.shortestPath.dijkstra. This adapted version computes
// edge weights via MATCH+WITH, then uses shortestPath with cost() for weighted path.
// nodes(path) already returns IDs in VLP path_nodes, so no list comprehension needed.
// All edges are directed to produce clean single-branch SQL. The weight CTE
// detection automatically creates a bidirectional version for VLP traversal.
/*
:params { person1Id: 14, person2Id: 27 }
*/
MATCH (pA:Person)-[:KNOWS]->(pB:Person),
      (pA)<-[:HAS_CREATOR]-(m1:Message)-[:REPLY_OF]->(m2:Message)-[:HAS_CREATOR]->(pB)
WITH pA.id AS source, pB.id AS target,
     CASE WHEN round(40.0 - sqrt(toFloat64(count(*)))) > 1
          THEN round(40.0 - sqrt(toFloat64(count(*)))) ELSE 1 END AS weight
MATCH path = shortestPath((person1:Person {id: $person1Id})-[:KNOWS*]-(person2:Person {id: $person2Id}))
RETURN nodes(path) AS personIdsInPath, cost(path) AS pathWeight
LIMIT 1
