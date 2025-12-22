-- LDBC Official Query: IC-complex-3
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.149380
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (countryX:Country {name: $countryXName }),
--       (countryY:Country {name: $countryYName }),
--       (person:Person {id: $personId })
-- WITH person, countryX, countryY
-- LIMIT 1
-- MATCH (city:City)-[:IS_PART_OF]->(country:Country)
-- WHERE country IN [countryX, countryY]
-- WITH person, countryX, countryY, collect(city) AS cities
-- MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
-- WHERE NOT person=friend AND NOT city IN cities
-- WITH DISTINCT friend, countryX, countryY
-- MATCH (friend)<-[:HAS_CREATOR]-(message),
--       (message)-[:IS_LOCATED_IN]->(country)
-- WHERE $endDate > message.creationDate >= $startDate AND
--       country IN [countryX, countryY]
-- WITH friend,
--      CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
--      CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
-- WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
-- WHERE xCount>0 AND yCount>0
-- RETURN friend.id AS friendId,
--        friend.firstName AS friendFirstName,
--        friend.lastName AS friendLastName,
--        xCount,
--        yCount,
--        xCount + yCount AS xyCount
-- ORDER BY xyCount DESC, friendId ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH with_countryX_countryY_person_cte AS (SELECT 
      person.birthday AS "person_birthday", 
      person.browserUsed AS "person_browserUsed", 
      person.creationDate AS "person_creationDate", 
      person.firstName AS "person_firstName", 
      person.gender AS "person_gender", 
      person.id AS "person_id", 
      person.lastName AS "person_lastName", 
      person.locationIP AS "person_locationIP", 
      countryX.id AS "countryX_id", 
      countryX.name AS "countryX_name", 
      countryX.url AS "countryX_url", 
      countryY.id AS "countryY_id", 
      countryY.name AS "countryY_name", 
      countryY.url AS "countryY_url"
FROM ldbc.Place AS countryX
INNER JOIN ldbc.Place AS countryY
INNER JOIN ldbc.Person AS person
WHERE (((countryX.name = $countryXName AND (countryX.type = 'Country')) AND (countryY.name = $countryYName AND (countryY.type = 'Country'))) AND person.id = $personId)
)
SELECT 
      countryX.id AS "countryX.id", 
      countryX.url AS "countryX.url", 
      countryX.name AS "countryX.name", 
      countryY.id AS "countryY.id", 
      countryY.url AS "countryY.url", 
      countryY.name AS "countryY.name", 
      person.gender AS "person.gender", 
      person.locationIP AS "person.locationIP", 
      person.birthday AS "person.birthday", 
      person.lastName AS "person.lastName", 
      person.id AS "person.id", 
      person.browserUsed AS "person.browserUsed", 
      person.firstName AS "person.firstName", 
      person.creationDate AS "person.creationDate", 
      city.url AS "city.url", 
      city.id AS "city.id", 
      city.name AS "city.name", 
      country.id AS "country.id", 
      country.url AS "country.url", 
      country.name AS "country.name"
FROM ldbc.Place AS countryX
INNER JOIN ldbc.Place_isPartOf_Place AS t206 ON t206.Place1Id = city.id
INNER JOIN ldbc.Place AS country ON country.id = t206.Place2Id
WHERE ((((countryX.name = $countryXName AND (countryX.type = 'Country')) AND (countryY.name = $countryYName AND (countryY.type = 'Country'))) AND person.id = $personId) AND (city.type = 'City' AND country.type = 'Country'))

