-- LDBC Query: BI-10a
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.789195
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person {id: 14})-[:KNOWS]->(friend:Person)
-- RETURN 
--     friend.id AS friendId,
--     friend.firstName AS firstName,
--     friend.lastName AS lastName
-- ORDER BY friendId

-- Generated ClickHouse SQL:
SELECT 
      friend.id AS "friendId", 
      friend.firstName AS "firstName", 
      friend.lastName AS "lastName"
FROM ldbc.Person AS person
INNER JOIN ldbc.Person_knows_Person AS t27 ON t27.Person1Id = person.id
INNER JOIN ldbc.Person AS friend ON friend.id = t27.Person2Id
WHERE person.id = 14
ORDER BY friendId ASC

