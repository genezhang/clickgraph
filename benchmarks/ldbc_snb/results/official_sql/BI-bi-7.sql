-- LDBC Official Query: BI-bi-7
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.101095
-- Database: ldbc

-- Original Cypher Query:
-- MATCH
--   (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message),
--   (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
-- WHERE NOT (comment)-[:HAS_TAG]->(tag)
-- RETURN
--   relatedTag.name,
--   count(DISTINCT comment) AS count
-- ORDER BY
--   count DESC,
--   relatedTag.name ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      relatedTag.name AS "relatedTag.name", 
      count(DISTINCT comment.id) AS "count"
FROM ldbc.Message AS message
INNER JOIN ldbc.Comment_replyOf_Message AS t180 ON t180.MessageId = message.id
INNER JOIN ldbc.Message_hasTag_Tag AS t179 ON t179.MessageId = message.id
INNER JOIN ldbc.Comment_hasTag_Tag AS t181 ON t180.CommentId = t181.CommentId
INNER JOIN ldbc.Comment AS comment ON comment.id = t180.CommentId
INNER JOIN ldbc.Comment_hasTag_Tag AS t181 ON t181.CommentId = comment.id
INNER JOIN ldbc.Tag AS relatedTag ON relatedTag.id = t181.TagId
INNER JOIN ldbc.Tag AS tag ON tag.id = t179.TagId
INNER JOIN ldbc.Comment_replyOf_Message AS t180 ON t179.MessageId = t180.MessageId
WHERE tag.name = $tag
GROUP BY relatedTag.name
ORDER BY count DESC, relatedTag.name ASC
LIMIT  100
