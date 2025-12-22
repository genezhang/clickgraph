-- LDBC Query: AGG-3
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.802345
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)
-- RETURN 
--     country.name AS country,
--     count(p) AS personCount
-- ORDER BY personCount DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      country.name AS "country", 
      count(p.id) AS "personCount"
FROM ldbc.Person AS p
INNER JOIN ldbc.Person_isLocatedIn_Place AS t57 ON t57.PersonId = p.id
INNER JOIN ldbc.Place_isPartOf_Place AS t58 ON t57.CityId = t58.Place1Id
INNER JOIN ldbc.Place AS country ON country.id = t58.Place2Id
INNER JOIN ldbc.Place AS city ON city.id = t57.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t58 ON t58.Place1Id = city.id
WHERE (city.type = 'City' AND country.type = 'Country')
GROUP BY country.name
ORDER BY personCount DESC
LIMIT  20
