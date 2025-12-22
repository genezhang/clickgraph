-- LDBC Query: BI-8b
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.786802
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)
-- MATCH (post)-[:HAS_CREATOR]->(person:Person)
-- RETURN 
--     tag.name AS tagName,
--     person.id AS personId,
--     person.firstName AS firstName,
--     count(post) AS postCount
-- ORDER BY postCount DESC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      tag.name AS "tagName", 
      person.id AS "personId", 
      person.firstName AS "firstName", 
      count(post.id) AS "postCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t24 ON t24.PostId = post.id
INNER JOIN ldbc.Post_hasTag_Tag AS t23 ON t23.PostId = post.id
INNER JOIN ldbc.Person AS person ON person.id = t24.PersonId
INNER JOIN ldbc.Tag AS tag ON tag.id = t23.TagId
INNER JOIN ldbc.Post_hasCreator_Person AS t24 ON t23.PostId = t24.PostId
GROUP BY tag.name, person.id, person.firstName
ORDER BY postCount DESC
LIMIT  100
