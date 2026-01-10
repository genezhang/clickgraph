-- LDBC Official Query: IC-short-3
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.222244
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS firstName,
--     friend.lastName AS lastName,
--     r.creationDate AS friendshipCreationDate
-- ORDER BY
--     friendshipCreationDate DESC,
--     toInteger(personId) ASC

-- Generated ClickHouse SQL:
SELECT * FROM (
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "firstName", 
      friend.lastName AS "lastName", 
      r.creationDate AS "friendshipCreationDate"
FROM ldbc.Person_knows_Person AS r
WHERE n.id = $personId
UNION ALL 
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "firstName", 
      friend.lastName AS "lastName", 
      r.creationDate AS "friendshipCreationDate"
FROM ldbc.Person_knows_Person AS r
WHERE n.id = $personId
) AS __union
ORDER BY "friendshipCreationDate" DESC

