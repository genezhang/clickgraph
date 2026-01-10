-- LDBC Official Query: IC-short-1
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.217738
-- Database: ldbc_snb

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
INNER JOIN ldbc.Person_isLocatedIn_Place AS t88 ON t88.PersonId = n.id
INNER JOIN ldbc.Place AS p ON p.id = t88.CityId
WHERE (n.id = $personId AND (p.type = 'City'))

