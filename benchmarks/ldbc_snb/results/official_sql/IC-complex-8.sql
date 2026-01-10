-- LDBC Official Query: IC-complex-8
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.213046
-- Database: ldbc_snb

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
FROM ldbc.Message_hasCreator_Person AS t83
INNER JOIN ldbc.Comment_replyOf_Message AS t84 ON t84.MessageId = t82.id
INNER JOIN ldbc.Person AS person ON person.id = t85.PersonId
INNER JOIN ldbc.Comment_hasCreator_Person AS t85 ON t85.CommentId = t84.MessageId
INNER JOIN ldbc.Comment_hasCreator_Person AS t85 ON t85.CommentId = comment.id
INNER JOIN ldbc.Comment AS comment ON comment.id = t84.CommentId
WHERE start.id = $personId
ORDER BY commentCreationDate DESC, commentId ASC
LIMIT  20
