-- LDBC Query: BI-7
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.784283
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (tag:Tag {name: 'Enrique_Iglesias'})<-[:HAS_TAG]-(post:Post)
-- MATCH (post)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
-- WHERE relatedTag.id <> tag.id
-- RETURN 
--     relatedTag.name AS relatedTagName,
--     count(DISTINCT comment) AS commentCount
-- ORDER BY commentCount DESC, relatedTagName
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      relatedTag.name AS "relatedTagName", 
      count(DISTINCT comment.id) AS "commentCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Comment_replyOf_Post AS t87 ON t87.PostId = post.id
INNER JOIN ldbc.Comment_hasTag_Tag AS t88 ON t87.CommentId = t88.CommentId
INNER JOIN ldbc.Tag AS relatedTag ON relatedTag.id = t88.TagId
INNER JOIN ldbc.Post_hasTag_Tag AS t86 ON t86.PostId = post.id
INNER JOIN ldbc.Comment AS comment ON comment.id = t87.CommentId
INNER JOIN ldbc.Tag AS tag ON tag.id = t86.TagId
INNER JOIN ldbc.Comment_replyOf_Post AS t87 ON t86.PostId = t87.PostId
INNER JOIN ldbc.Comment_hasTag_Tag AS t88 ON t88.CommentId = comment.id
WHERE (relatedTag.id <> tag.id AND tag.name = 'Enrique_Iglesias')
GROUP BY relatedTag.name
ORDER BY commentCount DESC, relatedTagName ASC
LIMIT  100
