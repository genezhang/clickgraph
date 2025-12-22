-- LDBC Query: BI-16
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.796417
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)<-[:HAS_CREATOR]-(post1:Post)-[:HAS_TAG]->(tag1:Tag)
-- MATCH (person)<-[:HAS_CREATOR]-(post2:Post)-[:HAS_TAG]->(tag2:Tag)
-- WHERE tag1.name = 'Meryl_Streep' AND tag2.name = 'Hank_Williams'
--   AND post1.id <> post2.id
-- RETURN DISTINCT
--     person.id AS personId,
--     person.firstName AS firstName,
--     person.lastName AS lastName,
--     count(DISTINCT post1) AS tag1Posts,
--     count(DISTINCT post2) AS tag2Posts
-- ORDER BY tag1Posts + tag2Posts DESC, personId
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT DISTINCT 
      person.id AS "personId", 
      person.firstName AS "firstName", 
      person.lastName AS "lastName", 
      count(DISTINCT post1.id) AS "tag1Posts", 
      count(DISTINCT post2.id) AS "tag2Posts"
FROM ldbc.Post AS post1
INNER JOIN ldbc.Post_hasCreator_Person AS t44 ON t44.PostId = post1.id
INNER JOIN ldbc.Post_hasTag_Tag AS t45 ON t44.PostId = t45.PostId
INNER JOIN ldbc.Post_hasTag_Tag AS t45 ON t45.PostId = post1.id
INNER JOIN ldbc.Tag AS tag1 ON tag1.id = t45.TagId
INNER JOIN ldbc.Person AS person ON person.id = t44.PersonId
INNER JOIN ldbc.Post_hasCreator_Person AS t46 ON t46.PersonId = person.id
INNER JOIN ldbc.Post AS post2 ON post2.id = t46.PostId
INNER JOIN ldbc.Post_hasTag_Tag AS t47 ON t47.PostId = post2.id
INNER JOIN ldbc.Tag AS tag2 ON tag2.id = t47.TagId
INNER JOIN ldbc.Post_hasTag_Tag AS t47 ON t46.PostId = t47.PostId
WHERE ((tag2.name = 'Hank_Williams' AND post1.id <> post2.id) AND tag1.name = 'Meryl_Streep')
GROUP BY person.id, person.firstName, person.lastName
ORDER BY tag1Posts + tag2Posts DESC, personId ASC
LIMIT  20
