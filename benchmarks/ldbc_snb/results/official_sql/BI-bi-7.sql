-- LDBC Official Query: BI-bi-7
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.180674
-- Database: ldbc_snb

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
FROM ldbc.Message_hasTag_Tag AS t55
INNER JOIN ldbc.Tag AS relatedTag ON relatedTag.id = t57.TagId
INNER JOIN ldbc.Comment_replyOf_Message AS t56 ON t56.MessageId = t57.CommentId
INNER JOIN ldbc.Comment_replyOf_Message AS t56 ON t56.MessageId = message.id
INNER JOIN ldbc.Comment AS comment ON comment.id = t56.CommentId
INNER JOIN ldbc.Comment_hasTag_Tag AS t57 ON t57.CommentId = comment.id
WHERE (NOT EXISTS (SELECT 1 FROM ldbc.Comment_hasTag_Tag WHERE Comment_hasTag_Tag.CommentId = comment.id AND Comment_hasTag_Tag.TagId = tag.id) AND tag.name = $tag)
GROUP BY relatedTag.name
ORDER BY count DESC, relatedTag.name ASC
LIMIT  100
