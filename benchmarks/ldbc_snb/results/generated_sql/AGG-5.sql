-- LDBC Query: AGG-5
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.804807
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
-- MATCH (post:Post)-[:HAS_TAG]->(tag)
-- RETURN 
--     tc.name AS tagClassName,
--     count(DISTINCT tag) AS tagCount,
--     count(post) AS postCount
-- ORDER BY postCount DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      tc.name AS "tagClassName", 
      count(DISTINCT tag.id) AS "tagCount", 
      count(post.id) AS "postCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasTag_Tag AS t61 ON t61.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t61.TagId
INNER JOIN ldbc.Tag_hasType_TagClass AS t60 ON t60.TagId = tag.id
INNER JOIN ldbc.Tag_hasType_TagClass AS t60 ON t61.TagId = t60.TagId
INNER JOIN ldbc.TagClass AS tc ON tc.id = t60.TagClassId
GROUP BY tc.name
ORDER BY postCount DESC
LIMIT  20
