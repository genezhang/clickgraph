-- LDBC Official Query: IC-complex-12
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.196097
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
-- WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
-- WITH collect(tag.id) as tags
-- MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
-- WHERE tag.id in tags
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     collect(DISTINCT tag.name) AS tagNames,
--     count(DISTINCT comment) AS replyCount
-- ORDER BY
--     replyCount DESC,
--     toInteger(personId) ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      groupArray(DISTINCT tag.name) AS "tagNames", 
      count(DISTINCT comment.id) AS "replyCount"
FROM ldbc.Comment AS comment
INNER JOIN ldbc.Comment_hasCreator_Person AS t71 ON t71.CommentId = comment.id
INNER JOIN ldbc.Person AS friend ON friend.id = t71.PersonId
INNER JOIN ldbc.Comment_replyOf_Post AS t72 ON t72.CommentId = comment.id
INNER JOIN ldbc.Post AS t69 ON t69.id = t72.PostId
INNER JOIN ldbc.Comment_hasCreator_Person AS t71 ON t71.MessageId = t72.CommentId
INNER JOIN ldbc.Person_knows_Person AS t70 ON t70.Person2Id = t71.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t73 ON t73.PostId = t69.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t73.TagId
WHERE tag.id IN tags
GROUP BY friend.id, friend.firstName, friend.lastName
ORDER BY replyCount DESC, toInt64(personId) ASC
LIMIT  20
