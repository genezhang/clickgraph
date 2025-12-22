-- LDBC Query: BI-17
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.797578
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum1:Forum)
-- MATCH (person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
-- WHERE forum1.id <> forum2.id
-- RETURN 
--     person.id AS personId,
--     forum1.title AS sourceForumTitle,
--     forum2.title AS targetForumTitle,
--     count(comment) AS crossPostCount
-- ORDER BY crossPostCount DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      person.id AS "personId", 
      forum1.title AS "sourceForumTitle", 
      forum2.title AS "targetForumTitle", 
      count(comment.id) AS "crossPostCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Forum_containerOf_Post AS t92 ON t92.PostId = post.id
INNER JOIN ldbc.Post_hasCreator_Person AS t91 ON t91.PostId = post.id
INNER JOIN ldbc.Forum AS forum1 ON forum1.id = t92.ForumId
INNER JOIN ldbc.Person AS person ON person.id = t91.PersonId
INNER JOIN ldbc.Comment_hasCreator_Person AS t93 ON t91.PersonId = t93.PersonId
INNER JOIN ldbc.Comment_replyOf_Post AS t94 ON t93.CommentId = t94.CommentId
INNER JOIN ldbc.Comment_hasCreator_Person AS t93 ON t93.PersonId = person.id
INNER JOIN ldbc.Post AS post2 ON post2.id = t94.PostId
INNER JOIN ldbc.Forum_containerOf_Post AS t92 ON t91.PostId = t92.PostId
INNER JOIN ldbc.Forum_containerOf_Post AS t95 ON t95.PostId = post2.id
INNER JOIN ldbc.Forum_containerOf_Post AS t95 ON t94.PostId = t95.PostId
INNER JOIN ldbc.Comment AS comment ON comment.id = t93.CommentId
INNER JOIN ldbc.Comment_replyOf_Post AS t94 ON t94.CommentId = comment.id
INNER JOIN ldbc.Forum AS forum2 ON forum2.id = t95.ForumId
WHERE forum1.id <> forum2.id
GROUP BY person.id, forum1.title, forum2.title
ORDER BY crossPostCount DESC
LIMIT  20
