-- LDBC Query: interactive-short-1
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.822878
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(city:City)
-- RETURN
--     n.firstName AS firstName,
--     n.lastName AS lastName,
--     n.birthday AS birthday,
--     n.locationIP AS locationIP,
--     n.browserUsed AS browserUsed,
--     city.id AS cityId,
--     n.gender AS gender,
--     n.creationDate AS creationDate

-- Generated ClickHouse SQL:
SELECT 
      n.firstName AS "firstName", 
      n.lastName AS "lastName", 
      n.birthday AS "birthday", 
      n.locationIP AS "locationIP", 
      n.browserUsed AS "browserUsed", 
      city.id AS "cityId", 
      n.gender AS "gender", 
      n.creationDate AS "creationDate"
FROM ldbc.Person AS n
INNER JOIN ldbc.Person_isLocatedIn_Place AS t83 ON t83.PersonId = n.id
INNER JOIN ldbc.Place AS city ON city.id = t83.CityId
WHERE (n.id = 933 AND city.type = 'City')

