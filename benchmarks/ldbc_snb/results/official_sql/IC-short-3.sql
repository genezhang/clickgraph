-- LDBC Official Query: IC-short-3
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.182166
-- Database: ldbc

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
FROM ldbc.Person AS n
INNER JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = n.id
INNER JOIN ldbc.Person AS friend ON friend.id = r.Person2Id
WHERE n.id = $personId
UNION ALL 
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "firstName", 
      friend.lastName AS "lastName", 
      r.creationDate AS "friendshipCreationDate"
FROM ldbc.Person AS friend
INNER JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = friend.id
INNER JOIN ldbc.Person AS n ON n.id = r.Person2Id
WHERE n.id = $personId
) AS __union
ORDER BY "friendshipCreationDate" DESC

