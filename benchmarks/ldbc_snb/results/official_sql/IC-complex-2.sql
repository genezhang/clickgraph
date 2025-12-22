-- LDBC Official Query: IC-complex-2
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.145928
-- Database: ldbc

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
FROM ldbc.Person AS t203
INNER JOIN ldbc.Person_knows_Person AS t204 ON t204.Person1Id = t203.id
INNER JOIN ldbc.Message_hasCreator_Person AS t205 ON t204.Person2Id = t205.PersonId
INNER JOIN ldbc.Message AS message ON message.id = t205.MessageId
INNER JOIN ldbc.Person AS friend ON friend.id = t204.Person2Id
INNER JOIN ldbc.Message_hasCreator_Person AS t205 ON t205.PersonId = friend.id
WHERE (message.creationDate < $maxDate AND t203.id = $personId)
UNION ALL 
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      message.id AS "postOrCommentId", 
      coalesce(message.content, message.imageFile) AS "postOrCommentContent", 
      message.creationDate AS "postOrCommentCreationDate"
FROM ldbc.Person AS friend
INNER JOIN ldbc.Person_knows_Person AS t204 ON t204.Person1Id = friend.id
INNER JOIN ldbc.Message_hasCreator_Person AS t205 ON t204.Person1Id = t205.PersonId
INNER JOIN ldbc.Message AS message ON message.id = t205.MessageId
INNER JOIN ldbc.Person AS t203 ON t203.id = t204.Person2Id
INNER JOIN ldbc.Message_hasCreator_Person AS t205 ON t205.PersonId = friend.id
WHERE (message.creationDate < $maxDate AND t203.id = $personId)
) AS __union
ORDER BY "postOrCommentCreationDate" DESC
LIMIT  20
