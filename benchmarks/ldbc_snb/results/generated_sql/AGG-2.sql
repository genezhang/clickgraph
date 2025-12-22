-- LDBC Query: AGG-2
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.801107
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
-- RETURN 'KNOWS' AS relType, count(*) AS cnt
-- UNION ALL
-- MATCH (p:Person)-[l:LIKES]->(post:Post)
-- RETURN 'LIKES' AS relType, count(*) AS cnt

-- Generated ClickHouse SQL:
SELECT 
      "relType" AS "relType", 
      count(*) AS "cnt"
FROM (
SELECT 
      'KNOWS' AS "relType"
FROM ldbc.Person AS p1
INNER JOIN ldbc.Person_knows_Person AS k ON k.Person1Id = p1.id
INNER JOIN ldbc.Person AS p2 ON p2.id = k.Person2Id
UNION ALL 
SELECT 
      'LIKES' AS "relType"
FROM ldbc.Person AS p
INNER JOIN ldbc.Person_likes_Post AS l ON l.PersonId = p.id
INNER JOIN ldbc.Post AS post ON post.id = l.PostId
) AS __union
GROUP BY "relType"

