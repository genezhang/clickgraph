-- LDBC Official Query: IC-short-7
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.230440
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (m:Message {id: $messageId })<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
--     OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
--     RETURN c.id AS commentId,
--         c.content AS commentContent,
--         c.creationDate AS commentCreationDate,
--         p.id AS replyAuthorId,
--         p.firstName AS replyAuthorFirstName,
--         p.lastName AS replyAuthorLastName,
--         CASE
--             WHEN r IS NULL THEN false
--             ELSE true
--         END AS replyAuthorKnowsOriginalMessageAuthor
--     ORDER BY commentCreationDate DESC, replyAuthorId

-- Generated ClickHouse SQL:
SELECT * FROM (
SELECT 
      c.id AS "commentId", 
      c.content AS "commentContent", 
      c.creationDate AS "commentCreationDate", 
      p.id AS "replyAuthorId", 
      p.firstName AS "replyAuthorFirstName", 
      p.lastName AS "replyAuthorLastName", 
      CASE WHEN r.Person2Id IS NULL THEN false ELSE true END AS "replyAuthorKnowsOriginalMessageAuthor"
FROM ldbc.Comment AS c
INNER JOIN ldbc.Comment_hasCreator_Person AS t98 ON t98.CommentId = c.id
INNER JOIN ldbc.Comment_replyOf_Message AS t97 ON t97.MessageId = t98.CommentId
LEFT JOIN ldbc.Person AS p ON p.id = t98.PersonId
INNER JOIN ldbc.Person_knows_Person AS r ON r.Person2Id = t98.PersonId
INNER JOIN ldbc.Message_hasCreator_Person AS t99 ON t99.PersonId = r.Person2Id
INNER JOIN ldbc.Message AS m ON m.id = t99.MessageId
LEFT JOIN ldbc.Person AS a ON a.id = t99.PersonId
WHERE m.id = $messageId
UNION ALL 
SELECT 
      c.id AS "commentId", 
      c.content AS "commentContent", 
      c.creationDate AS "commentCreationDate", 
      p.id AS "replyAuthorId", 
      p.firstName AS "replyAuthorFirstName", 
      p.lastName AS "replyAuthorLastName", 
      CASE WHEN r.Person2Id IS NULL THEN false ELSE true END AS "replyAuthorKnowsOriginalMessageAuthor"
FROM ldbc.Comment_hasCreator_Person AS t98
INNER JOIN ldbc.Comment AS c ON c.id = t98.CommentId
INNER JOIN ldbc.Person_knows_Person AS r ON r.Person2Id = t98.PersonId
INNER JOIN ldbc.Comment_replyOf_Message AS t97 ON t97.MessageId = t98.CommentId
LEFT JOIN ldbc.Person AS p ON p.id = t98.PersonId
INNER JOIN ldbc.Message_hasCreator_Person AS t99 ON t99.PersonId = r.Person2Id
INNER JOIN ldbc.Message AS m ON m.id = t99.MessageId
LEFT JOIN ldbc.Person AS a ON a.id = t99.PersonId
WHERE m.id = $messageId
) AS __union
ORDER BY "commentCreationDate" DESC, "replyAuthorId" ASC

