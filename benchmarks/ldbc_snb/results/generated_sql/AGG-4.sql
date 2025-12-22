-- LDBC Query: AGG-4
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.803586
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (forum:Forum)-[:CONTAINER_OF]->(post:Post)
-- RETURN 
--     forum.title AS forumTitle,
--     count(post) AS postCount
-- ORDER BY postCount DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      forum.title AS "forumTitle", 
      count(post.id) AS "postCount"
FROM ldbc.Forum AS forum
INNER JOIN ldbc.Forum_containerOf_Post AS t59 ON t59.ForumId = forum.id
INNER JOIN ldbc.Post AS post ON post.id = t59.PostId
GROUP BY forum.title
ORDER BY postCount DESC
LIMIT  20
