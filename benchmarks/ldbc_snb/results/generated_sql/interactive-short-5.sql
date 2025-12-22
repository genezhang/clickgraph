-- LDBC Query: interactive-short-5
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.826479
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post {id: $messageId})-[:HAS_CREATOR]->(p:Person)
-- RETURN
--     p.id AS personId,
--     p.firstName AS firstName,
--     p.lastName AS lastName

-- Generated ClickHouse SQL:
SELECT 
      p.id AS "personId", 
      p.firstName AS "firstName", 
      p.lastName AS "lastName"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t85 ON t85.PostId = post.id
INNER JOIN ldbc.Person AS p ON p.id = t85.PersonId
WHERE post.id = 618475290625

