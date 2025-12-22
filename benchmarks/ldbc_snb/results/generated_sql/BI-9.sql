-- LDBC Query: BI-9
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.787969
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
-- OPTIONAL MATCH (post)<-[:REPLY_OF]-(reply:Comment)
-- RETURN 
--     person.id AS personId,
--     person.firstName AS firstName,
--     person.lastName AS lastName,
--     count(DISTINCT post) AS threadCount,
--     count(DISTINCT reply) AS replyCount
-- ORDER BY replyCount DESC, personId
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      person.id AS "personId", 
      person.firstName AS "firstName", 
      person.lastName AS "lastName", 
      count(DISTINCT post.id) AS "threadCount", 
      count(DISTINCT reply.id) AS "replyCount"
FROM ldbc.Post AS post
LEFT JOIN ldbc.Comment_replyOf_Post AS t90 ON t90.PostId = post.id
LEFT JOIN ldbc.Comment AS reply ON reply.id = t90.CommentId
INNER JOIN ldbc.Post_hasCreator_Person AS t89 ON t89.PostId = post.id
INNER JOIN ldbc.Comment_replyOf_Post AS t90 ON t89.PostId = t90.PostId
INNER JOIN ldbc.Person AS person ON person.id = t89.PersonId
GROUP BY person.id, person.firstName, person.lastName
ORDER BY replyCount DESC, personId ASC
LIMIT  100
