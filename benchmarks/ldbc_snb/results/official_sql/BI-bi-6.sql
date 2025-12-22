-- LDBC Official Query: BI-bi-6
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.097192
-- Database: ldbc

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
      count(*) AS "authorityScore"
FROM ldbc.Message AS message1
LEFT JOIN ldbc.Person_likes_Message AS t177 ON t177.MessageId = message1.id
INNER JOIN ldbc.Message_hasTag_Tag AS t175 ON t175.MessageId = message1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t175.TagId
INNER JOIN ldbc.Message_hasCreator_Person AS t176 ON t176.MessageId = message1.id
INNER JOIN ldbc.Message_hasCreator_Person AS t176 ON t175.MessageId = t176.MessageId
LEFT JOIN ldbc.Person AS person2 ON person2.id = t177.PersonId
INNER JOIN ldbc.Message_hasCreator_Person AS t178 ON t177.PersonId = t178.PersonId
LEFT JOIN ldbc.Message AS message2 ON message2.id = t178.MessageId
LEFT JOIN ldbc.Person_likes_Message AS like ON like.MessageId = message2.id
INNER JOIN ldbc.Person AS person1 ON person1.id = t176.PersonId
INNER JOIN ldbc.Person_likes_Message AS like ON t178.MessageId = like.MessageId
LEFT JOIN Message_hasCreator_Person AS t178 ON t178.PersonId = person2.id
INNER JOIN ldbc.Person_likes_Message AS t177 ON t175.MessageId = t177.MessageId
LEFT JOIN ldbc.Person AS person3 ON person3.id = like.PersonId
WHERE tag.name = $tag
GROUP BY person1.id
ORDER BY authorityScore DESC, person1.id ASC
LIMIT  100
