-- LDBC Official Query: IC-short-5
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.187202
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (m:Message {id:  $messageId })-[:HAS_CREATOR]->(p:Person)
-- RETURN
--     p.id AS personId,
--     p.firstName AS firstName,
--     p.lastName AS lastName

-- Generated ClickHouse SQL:
SELECT 
      p.id AS "personId", 
      p.firstName AS "firstName", 
      p.lastName AS "lastName"
FROM ldbc.Message AS m
INNER JOIN ldbc.Message_hasCreator_Person AS t224 ON t224.MessageId = m.id
INNER JOIN ldbc.Person AS p ON p.id = t224.PersonId
WHERE m.id = $messageId

