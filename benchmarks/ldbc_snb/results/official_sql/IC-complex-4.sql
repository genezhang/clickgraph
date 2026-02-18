-- LDBC Official Query: IC-complex-4
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.729034
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (person:Person {id: $personId })-[:KNOWS]-(friend:Person),
--       (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
-- WITH DISTINCT tag, post
-- WITH tag,
--      CASE
--        WHEN $endDate > post.creationDate >= $startDate THEN 1
--        ELSE 0
--      END AS valid,
--      CASE
--        WHEN $startDate > post.creationDate THEN 1
--        ELSE 0
--      END AS inValid
-- WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
-- WHERE postCount>0 AND inValidPostCount=0
-- RETURN tag.name AS tagName, postCount
-- ORDER BY postCount DESC, tagName ASC
-- LIMIT 10

-- Generated ClickHouse SQL:
WITH with_post_tag_cte_1 AS (SELECT DISTINCT 
      tag.id AS "p3_tag_id", 
      tag.name AS "p3_tag_name", 
      post.creationDate AS "p4_post_creationDate", 
      post.id AS "p4_post_id"
FROM (
SELECT 
      t.language AS "language", 
      t.creationDate AS "creationDate", 
      t.length AS "length", 
      t.browserUsed AS "browserUsed", 
      t.id AS "id", 
      t.content AS "content", 
      t.locationIP AS "locationIP", 
      t.imageFile AS "imageFile", 
      t.gender AS "gender", 
      t.id AS "id", 
      t.creationDate AS "creationDate", 
      t.birthday AS "birthday", 
      t.firstName AS "firstName", 
      t.locationIP AS "locationIP", 
      t.browserUsed AS "browserUsed", 
      t.lastName AS "lastName", 
      t.gender AS "gender", 
      t.id AS "id", 
      t.creationDate AS "creationDate", 
      t.birthday AS "birthday", 
      t.firstName AS "firstName", 
      t.locationIP AS "locationIP", 
      t.browserUsed AS "browserUsed", 
      t.lastName AS "lastName", 
      t.id AS "id", 
      t.url AS "url", 
      u.name AS "name"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t104 ON t104.PostId = post.id
INNER JOIN ldbc.Person AS friend ON friend.id = t104.PersonId
INNER JOIN ldbc.Person_knows_Person AS t103 ON t103.Person1Id = friend.id
INNER JOIN ldbc.Person AS person ON person.id = t103.Person2Id
INNER JOIN ldbc.Post_hasTag_Tag AS t105 ON t105.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t105.TagId
WHERE person.id = $personId
) AS __union
INNER JOIN ldbc.Person_knows_Person AS t103 ON t103.Person2Id = friend.id
INNER JOIN ldbc.Person AS person ON person.id = t103.Person1Id
INNER JOIN ldbc.Post_hasCreator_Person AS t104 ON t104.PostId = post.id
INNER JOIN ldbc.Person AS friend ON friend.id = t104.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t105 ON t105.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t105.TagId
), 
with_inValid_tag_valid_cte_1 AS (SELECT 
      post_tag.p3_tag_id AS "p3_tag_id", 
      post_tag.p3_tag_name AS "p3_tag_name", 
      CASE WHEN $endDate > post_tag.p4_post_creationDate >= $startDate THEN 1 ELSE 0 END AS "valid", 
      CASE WHEN $startDate > post_tag.p4_post_creationDate THEN 1 ELSE 0 END AS "inValid"
FROM with_post_tag_cte_1 AS post_tag
), 
with_inValidPostCount_postCount_tag_cte_1 AS (SELECT 
      inValid_tag_valid.p3_tag_id AS "p3_tag_id", 
      anyLast(inValid_tag_valid.p3_tag_name) AS "p3_tag_name", 
      sum(post_tag.valid) AS "postCount", 
      sum(post_tag.inValid) AS "inValidPostCount"
FROM with_inValid_tag_valid_cte_1 AS inValid_tag_valid
WHERE (post_tag.postCount > 0 AND post_tag.inValidPostCount = 0)
)
SELECT 
      inValidPostCount.p3_tag_name AS "tagName", 
      inValidPostCount.postCount AS "postCount"
FROM with_inValidPostCount_postCount_tag_cte_1 AS inValidPostCount
INNER JOIN ldbc.Person_knows_Person AS t103 ON t103.Person2Id = t104.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t105 ON t105.PostId = t104.CommentId
ORDER BY postCount DESC, tagName ASC
LIMIT  10
