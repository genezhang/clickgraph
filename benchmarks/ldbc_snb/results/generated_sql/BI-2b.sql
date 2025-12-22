-- LDBC Query: BI-2b
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.775587
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (comment:Comment)-[:HAS_TAG]->(tag:Tag)
-- RETURN 
--     tag.name AS tagName,
--     count(comment) AS commentCount
-- ORDER BY commentCount DESC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      tag.name AS "tagName", 
      count(comment.id) AS "commentCount"
FROM ldbc.Comment AS comment
INNER JOIN ldbc.Comment_hasTag_Tag AS t2 ON t2.CommentId = comment.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t2.TagId
GROUP BY tag.name
ORDER BY commentCount DESC
LIMIT  100
