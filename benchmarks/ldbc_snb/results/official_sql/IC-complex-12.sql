-- LDBC Official Query: IC-complex-12
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.133940
-- Database: ldbc

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
INNER JOIN ldbc.Comment_replyOf_Post AS t199 ON t199.CommentId = comment.id
INNER JOIN ldbc.Comment_hasCreator_Person AS t198 ON t198.CommentId = comment.id
INNER JOIN ldbc.Person AS friend ON friend.id = t198.PersonId
INNER JOIN ldbc.Post AS t196 ON t196.id = t199.PostId
INNER JOIN ldbc.Post_hasTag_Tag AS t200 ON t200.PostId = t196.id
INNER JOIN ldbc.Comment_replyOf_Post AS t199 ON t198.CommentId = t199.CommentId
INNER JOIN ldbc.Post_hasTag_Tag AS t200 ON t199.PostId = t200.PostId
INNER JOIN ldbc.Tag AS tag ON tag.id = t200.TagId
WHERE tag.id IN tags
GROUP BY friend.id, friend.firstName, friend.lastName
ORDER BY replyCount DESC, toInt64(personId) ASC
LIMIT  20
