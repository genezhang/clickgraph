-- LDBC Query: BI-6
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.783069
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post)-[:HAS_TAG]->(tag:Tag {name: 'Che_Guevara'})
-- MATCH (post)-[:HAS_CREATOR]->(person:Person)
-- OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
-- RETURN 
--     person.id AS personId,
--     person.firstName AS firstName,
--     count(DISTINCT liker) AS likerCount
-- ORDER BY likerCount DESC, personId
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      person.id AS "personId", 
      person.firstName AS "firstName", 
      count(DISTINCT liker.id) AS "likerCount"
FROM ldbc.Post AS post
LEFT JOIN ldbc.Person_likes_Post AS t18 ON t18.PostId = post.id
LEFT JOIN ldbc.Person AS liker ON liker.id = t18.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t16 ON t16.PostId = post.id
INNER JOIN ldbc.Person_likes_Post AS t18 ON t16.PostId = t18.PostId
INNER JOIN ldbc.Post_hasCreator_Person AS t17 ON t16.PostId = t17.PostId
INNER JOIN ldbc.Person AS person ON person.id = t17.PersonId
INNER JOIN ldbc.Post_hasCreator_Person AS t17 ON t17.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t16.TagId
WHERE tag.name = 'Che_Guevara'
GROUP BY person.id, person.firstName
ORDER BY likerCount DESC, personId ASC
LIMIT  100
