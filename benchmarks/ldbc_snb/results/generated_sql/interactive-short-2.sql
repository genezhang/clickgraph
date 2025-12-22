-- LDBC Query: interactive-short-2
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.824037
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p:Person {id: $personId})<-[:HAS_CREATOR]-(post:Post)
-- RETURN
--     post.id AS messageId,
--     post.content AS messageContent,
--     post.creationDate AS messageCreationDate
-- ORDER BY post.creationDate DESC, post.id ASC
-- LIMIT 10

-- Generated ClickHouse SQL:
SELECT 
      post.id AS "messageId", 
      post.content AS "messageContent", 
      post.creationDate AS "messageCreationDate"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t84 ON t84.PostId = post.id
INNER JOIN ldbc.Person AS p ON p.id = t84.PersonId
WHERE p.id = 933
ORDER BY post.creationDate DESC, post.id ASC
LIMIT  10
