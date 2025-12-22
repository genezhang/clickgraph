-- LDBC Query: BI-2a
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.774429
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)
-- RETURN 
--     tag.name AS tagName,
--     count(post) AS postCount
-- ORDER BY postCount DESC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      tag.name AS "tagName", 
      count(post.id) AS "postCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasTag_Tag AS t1 ON t1.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t1.TagId
GROUP BY tag.name
ORDER BY postCount DESC
LIMIT  100
