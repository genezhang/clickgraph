-- LDBC Query: BI-4b
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.779190
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
-- RETURN 
--     person.id AS personId,
--     person.firstName AS firstName,
--     person.lastName AS lastName,
--     count(post) AS postCount
-- ORDER BY postCount DESC, personId
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      person.id AS "personId", 
      person.firstName AS "firstName", 
      person.lastName AS "lastName", 
      count(post.id) AS "postCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t10 ON t10.PostId = post.id
INNER JOIN ldbc.Person AS person ON person.id = t10.PersonId
GROUP BY person.id, person.firstName, person.lastName
ORDER BY postCount DESC, personId ASC
LIMIT  100
