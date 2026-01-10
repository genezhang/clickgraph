-- LDBC Official Query: IC-complex-2
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.200876
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
--     WHERE message.creationDate < $maxDate
--     RETURN
--         friend.id AS personId,
--         friend.firstName AS personFirstName,
--         friend.lastName AS personLastName,
--         message.id AS postOrCommentId,
--         coalesce(message.content,message.imageFile) AS postOrCommentContent,
--         message.creationDate AS postOrCommentCreationDate
--     ORDER BY
--         postOrCommentCreationDate DESC,
--         toInteger(postOrCommentId) ASC
--     LIMIT 20

-- Generated ClickHouse SQL:
SELECT * FROM (
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      message.id AS "postOrCommentId", 
      coalesce(message.content, message.imageFile) AS "postOrCommentContent", 
      message.creationDate AS "postOrCommentCreationDate"
FROM ldbc.Person AS friend
INNER JOIN ldbc.Message_hasCreator_Person AS t77 ON t77.PersonId = friend.id
INNER JOIN ldbc.Message AS message ON message.id = t77.MessageId
INNER JOIN ldbc.Person_knows_Person AS t76 ON t76.Person2Id = t77.PersonId
WHERE (message.creationDate < $maxDate AND t75.id = $personId)
UNION ALL 
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      message.id AS "postOrCommentId", 
      coalesce(message.content, message.imageFile) AS "postOrCommentContent", 
      message.creationDate AS "postOrCommentCreationDate"
FROM ldbc.Person AS friend
INNER JOIN ldbc.Message_hasCreator_Person AS t77 ON t77.PersonId = friend.id
INNER JOIN ldbc.Person_knows_Person AS t76 ON t76.Person2Id = t77.PersonId
INNER JOIN ldbc.Message AS message ON message.id = t77.MessageId
WHERE (message.creationDate < $maxDate AND t75.id = $personId)
) AS __union
ORDER BY "postOrCommentCreationDate" DESC
LIMIT  20
