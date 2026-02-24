// Q14. International dialog (adapted)
// Original uses multi-pattern MATCH which causes join ordering issues.
// Adapted to use a single chain with directed KNOWS to avoid UNION ALL.
// Each OPTIONAL MATCH uses fresh endpoint nodes (p2a..p2d) with WHERE filter to person2.id
// to avoid invalid JOIN conditions when both endpoints are CTE-backed.
// Final WITH uses unique alias (finalScore) to avoid bare variable resolution ambiguity.
/*
:params { country1: 'Chile', country2: 'Argentina' }
*/
MATCH (country1:Country {name: $country1})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person)-[:KNOWS]->(person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country2:Country {name: $country2})
WITH person1, person2, city1, 0 AS score
// case 1: person1 wrote Comment that replies to Post written by person2
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(m1:Post)-[:HAS_CREATOR]->(p2a:Person)
WHERE p2a.id = person2.id
WITH DISTINCT person1, person2, city1, score + (CASE WHEN c IS NULL THEN 0 ELSE 4 END) AS score
// case 2: person2 wrote Comment that replies to Post written by person1
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m2:Post)<-[:REPLY_OF]-(c2:Comment)-[:HAS_CREATOR]->(p2b:Person)
WHERE p2b.id = person2.id
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m2 IS NULL THEN 0 ELSE 1 END) AS score
// case 3: person1 likes Post written by person2
OPTIONAL MATCH (person1)-[:LIKES]->(m3:Post)-[:HAS_CREATOR]->(p2c:Person)
WHERE p2c.id = person2.id
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m3 IS NULL THEN 0 ELSE 10 END) AS score
// case 4: person2 likes Post written by person1
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m4:Post)<-[:LIKES]-(p2d:Person)
WHERE p2d.id = person2.id
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m4 IS NULL THEN 0 ELSE 1 END) AS finalScore
RETURN person1.id AS person1Id, person2.id AS person2Id, city1.name AS cityName, finalScore AS score
ORDER BY score DESC, person1Id ASC, person2Id ASC
LIMIT 100
