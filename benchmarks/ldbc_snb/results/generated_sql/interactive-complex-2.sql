-- LDBC Query: interactive-complex-2
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.818847
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p:Person {id: $personId})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
-- WHERE post.creationDate <= $maxDate
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     post.id AS postId,
--     post.content AS postContent,
--     post.creationDate AS postCreationDate
-- ORDER BY postCreationDate DESC, post.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT * FROM (
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      post.id AS "postId", 
      post.content AS "postContent", 
      post.creationDate AS "postCreationDate"
FROM ldbc.Person AS p
INNER JOIN ldbc.Person_knows_Person AS t76 ON t76.Person1Id = p.id
INNER JOIN ldbc.Person AS friend ON friend.id = t76.Person2Id
INNER JOIN ldbc.Post_hasCreator_Person AS t77 ON t77.PersonId = friend.id
INNER JOIN ldbc.Post_hasCreator_Person AS t77 ON t76.Person2Id = t77.PersonId
INNER JOIN ldbc.Post AS post ON post.id = t77.PostId
WHERE (post.creationDate <= '2012-12-31' AND p.id = 933)
UNION ALL 
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      post.id AS "postId", 
      post.content AS "postContent", 
      post.creationDate AS "postCreationDate"
FROM ldbc.Person AS friend
INNER JOIN ldbc.Person_knows_Person AS t76 ON t76.Person1Id = friend.id
INNER JOIN ldbc.Post_hasCreator_Person AS t77 ON t76.Person1Id = t77.PersonId
INNER JOIN ldbc.Post_hasCreator_Person AS t77 ON t77.PersonId = friend.id
INNER JOIN ldbc.Person AS p ON p.id = t76.Person2Id
INNER JOIN ldbc.Post AS post ON post.id = t77.PostId
WHERE (post.creationDate <= '2012-12-31' AND p.id = 933)
) AS __union
ORDER BY "postCreationDate" DESC, "postId" ASC
LIMIT  20
