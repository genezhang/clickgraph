-- LDBC Query: BI-11
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.791667
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a)
-- WHERE a.id < b.id AND b.id < c.id
-- RETURN count(*) AS triangleCount

-- Generated ClickHouse SQL:
SELECT 
      count(*) AS "triangleCount"
FROM ldbc.Person AS a
INNER JOIN ldbc.Person_knows_Person AS t32 ON t32.Person1Id = a.id
INNER JOIN ldbc.Person AS b ON b.id = t32.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t33 ON t33.Person1Id = b.id
INNER JOIN ldbc.Person AS c ON c.id = t33.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t34 ON t34.Person1Id = c.id
WHERE (a.id < b.id AND b.id < c.id)

