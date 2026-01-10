-- LDBC Official Query: BI-bi-6
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.178515
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
-- OPTIONAL MATCH (message1)<-[:LIKES]-(person2:Person)
-- OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message2:Message)<-[like:LIKES]-(person3:Person)
-- RETURN
--   person1.id,
--   count(DISTINCT like) AS authorityScore
-- ORDER BY
--   authorityScore DESC,
--   person1.id ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT 
      person1.id AS "person1.id", 
      count(DISTINCT tuple(like.PersonId, like.MessageId)) AS "authorityScore"
FROM ldbc.Message_hasTag_Tag AS t51
INNER JOIN ldbc.Message_hasCreator_Person AS t52 ON t52.PersonId = person1.id
LEFT JOIN ldbc.Message AS message2 ON message2.id = t54.MessageId
INNER JOIN ldbc.Message AS message1 ON message1.id = t52.MessageId
LEFT JOIN ldbc.Person_likes_Message AS t53 ON t53.MessageId = message1.id
LEFT JOIN ldbc.Person_likes_Message AS like ON like.MessageId = message2.id
LEFT JOIN ldbc.Person AS person3 ON person3.id = like.PersonId
LEFT JOIN Message_hasCreator_Person AS t54 ON t54.PersonId = person2.id
LEFT JOIN ldbc.Person AS person2 ON person2.id = t53.PersonId
WHERE tag.name = $tag
GROUP BY person1.id
ORDER BY authorityScore DESC, person1.id ASC
LIMIT  100
