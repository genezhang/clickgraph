-- LDBC Official Query: IC-complex-8
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.168983
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (start:Person {id: $personId})<-[:HAS_CREATOR]-(:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
-- RETURN
--     person.id AS personId,
--     person.firstName AS personFirstName,
--     person.lastName AS personLastName,
--     comment.creationDate AS commentCreationDate,
--     comment.id AS commentId,
--     comment.content AS commentContent
-- ORDER BY
--     commentCreationDate DESC,
--     commentId ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      person.id AS "personId", 
      person.firstName AS "personFirstName", 
      person.lastName AS "personLastName", 
      comment.creationDate AS "commentCreationDate", 
      comment.id AS "commentId", 
      comment.content AS "commentContent"
FROM ldbc.Message AS t213
INNER JOIN ldbc.Message_hasCreator_Person AS t214 ON t214.MessageId = t213.id
INNER JOIN ldbc.Comment_replyOf_Message AS t215 ON t215.MessageId = t213.id
INNER JOIN ldbc.Comment_replyOf_Message AS t215 ON t214.MessageId = t215.MessageId
INNER JOIN ldbc.Comment AS comment ON comment.id = t215.CommentId
INNER JOIN ldbc.Comment_hasCreator_Person AS t216 ON t215.CommentId = t216.CommentId
INNER JOIN ldbc.Comment_hasCreator_Person AS t216 ON t216.CommentId = comment.id
INNER JOIN ldbc.Person AS start ON start.id = t214.PersonId
INNER JOIN ldbc.Person AS person ON person.id = t216.PersonId
WHERE start.id = $personId
ORDER BY commentCreationDate DESC, commentId ASC
LIMIT  20
