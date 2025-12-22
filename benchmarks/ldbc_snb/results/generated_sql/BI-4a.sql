-- LDBC Query: BI-4a
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.778033
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
-- RETURN 
--     forum.id AS forumId,
--     forum.title AS forumTitle,
--     count(person) AS memberCount
-- ORDER BY memberCount DESC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      forum.id AS "forumId", 
      forum.title AS "forumTitle", 
      count(person.id) AS "memberCount"
FROM ldbc.Forum AS forum
INNER JOIN ldbc.Forum_hasMember_Person AS t9 ON t9.ForumId = forum.id
INNER JOIN ldbc.Person AS person ON person.id = t9.PersonId
GROUP BY forum.id, forum.title
ORDER BY memberCount DESC
LIMIT  100
