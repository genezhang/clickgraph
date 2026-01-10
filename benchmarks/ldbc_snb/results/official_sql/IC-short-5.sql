-- LDBC Official Query: IC-short-5
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.225584
-- Database: ldbc_snb

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
INNER JOIN ldbc.Message_hasCreator_Person AS t93 ON t93.MessageId = m.id
INNER JOIN ldbc.Person AS p ON p.id = t93.PersonId
WHERE m.id = $messageId

