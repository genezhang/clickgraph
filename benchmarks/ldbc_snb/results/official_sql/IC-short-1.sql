-- LDBC Official Query: IC-short-1
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.174476
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (n:Person {id: $personId })-[:IS_LOCATED_IN]->(p:City)
-- RETURN
--     n.firstName AS firstName,
--     n.lastName AS lastName,
--     n.birthday AS birthday,
--     n.locationIP AS locationIP,
--     n.browserUsed AS browserUsed,
--     p.id AS cityId,
--     n.gender AS gender,
--     n.creationDate AS creationDate

-- Generated ClickHouse SQL:
SELECT 
      n.firstName AS "firstName", 
      n.lastName AS "lastName", 
      n.birthday AS "birthday", 
      n.locationIP AS "locationIP", 
      n.browserUsed AS "browserUsed", 
      p.id AS "cityId", 
      n.gender AS "gender", 
      n.creationDate AS "creationDate"
FROM ldbc.Person AS n
INNER JOIN ldbc.Person_isLocatedIn_Place AS t219 ON t219.PersonId = n.id
INNER JOIN ldbc.Place AS p ON p.id = t219.CityId
WHERE (n.id = $personId AND p.type = 'City')

