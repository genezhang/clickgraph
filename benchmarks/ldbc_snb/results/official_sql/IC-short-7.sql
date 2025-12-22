-- LDBC Official Query: IC-short-7
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.197453
-- Database: ldbc

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
      CASE WHEN r.Person1Id IS NULL THEN false ELSE true END AS "replyAuthorKnowsOriginalMessageAuthor"
FROM ldbc.Comment AS c
INNER JOIN ldbc.Comment_hasCreator_Person AS t229 ON t229.CommentId = c.id
LEFT JOIN ldbc.Person AS p ON p.id = t229.PersonId
INNER JOIN ldbc.Comment_replyOf_Message AS t228 ON t228.CommentId = c.id
INNER JOIN ldbc.Person_knows_Person AS r ON t229.PersonId = r.Person2Id
INNER JOIN ldbc.Comment_hasCreator_Person AS t229 ON t228.CommentId = t229.CommentId
INNER JOIN ldbc.Message_hasCreator_Person AS t230 ON t228.MessageId = t230.MessageId
LEFT JOIN ldbc.Person AS a ON a.id = t230.PersonId
LEFT JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = a.id
INNER JOIN ldbc.Person_knows_Person AS r ON t230.PersonId = r.Person1Id
INNER JOIN ldbc.Message AS m ON m.id = t228.MessageId
LEFT JOIN ldbc.Message_hasCreator_Person AS t230 ON t230.MessageId = m.id
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
FROM ldbc.Comment AS c
INNER JOIN ldbc.Comment_hasCreator_Person AS t229 ON t229.CommentId = c.id
INNER JOIN ldbc.Person_knows_Person AS r ON t229.PersonId = r.Person1Id
INNER JOIN ldbc.Comment_replyOf_Message AS t228 ON t228.CommentId = c.id
LEFT JOIN ldbc.Person AS p ON p.id = t229.PersonId
INNER JOIN ldbc.Message AS m ON m.id = t228.MessageId
INNER JOIN ldbc.Message_hasCreator_Person AS t230 ON t228.MessageId = t230.MessageId
INNER JOIN ldbc.Comment_hasCreator_Person AS t229 ON t228.CommentId = t229.CommentId
LEFT JOIN ldbc.Person AS a ON a.id = t230.PersonId
LEFT JOIN ldbc.Message_hasCreator_Person AS t230 ON t230.MessageId = m.id
LEFT JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = p.id
INNER JOIN ldbc.Person_knows_Person AS r ON t230.PersonId = r.Person2Id
WHERE m.id = $messageId
) AS __union
ORDER BY "commentCreationDate" DESC, "replyAuthorId" ASC

