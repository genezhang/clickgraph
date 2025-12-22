-- LDBC Official Query: BI-bi-11
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.038215
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country {name: $country}),
--       (a)-[k1:KNOWS]-(b:Person)
-- WHERE a.id < b.id
--   AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
-- WITH DISTINCT country, a, b
-- MATCH (b)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
-- WITH DISTINCT country, a, b
-- MATCH (b)-[k2:KNOWS]-(c:Person),
--       (c)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
-- WHERE b.id < c.id
--   AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
-- WITH DISTINCT a, b, c
-- MATCH (c)-[k3:KNOWS]-(a)
-- WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
-- WITH DISTINCT a, b, c
-- RETURN count(*) AS count

-- Generated ClickHouse SQL:
WITH with_a_b_country_cte_1 AS (SELECT DISTINCT 
      country.id AS "country_id", 
      country.name AS "country_name", 
      country.url AS "country_url", 
      a.birthday AS "a_birthday", 
      a.browserUsed AS "a_browserUsed", 
      a.creationDate AS "a_creationDate", 
      a.firstName AS "a_firstName", 
      a.gender AS "a_gender", 
      a.id AS "a_id", 
      a.lastName AS "a_lastName", 
      a.locationIP AS "a_locationIP", 
      b.birthday AS "b_birthday", 
      b.browserUsed AS "b_browserUsed", 
      b.creationDate AS "b_creationDate", 
      b.firstName AS "b_firstName", 
      b.gender AS "b_gender", 
      b.id AS "b_id", 
      b.lastName AS "b_lastName", 
      b.locationIP AS "b_locationIP"
FROM ldbc.Person AS a
INNER JOIN ldbc.Person_isLocatedIn_Place AS t113 ON t113.PersonId = a.id
INNER JOIN ldbc.Place AS t112 ON t112.id = t113.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t114 ON t114.Place1Id = t112.id
INNER JOIN ldbc.Place AS country ON country.id = t114.Place2Id
INNER JOIN ldbc.Person_knows_Person AS k1 ON k1.Person1Id = a.id OR k1.Person2Id = a.id
INNER JOIN ldbc.Person AS b ON (b.id = k1.Person2Id AND k1.Person1Id = a.id) OR (b.id = k1.Person1Id AND k1.Person2Id = a.id)
WHERE (t112.type = 'City' AND country.type = 'Country')
UNION ALL 
SELECT DISTINCT 
      country.id AS "country_id", 
      country.name AS "country_name", 
      country.url AS "country_url", 
      a.birthday AS "a_birthday", 
      a.browserUsed AS "a_browserUsed", 
      a.creationDate AS "a_creationDate", 
      a.firstName AS "a_firstName", 
      a.gender AS "a_gender", 
      a.id AS "a_id", 
      a.lastName AS "a_lastName", 
      a.locationIP AS "a_locationIP", 
      b.birthday AS "b_birthday", 
      b.browserUsed AS "b_browserUsed", 
      b.creationDate AS "b_creationDate", 
      b.firstName AS "b_firstName", 
      b.gender AS "b_gender", 
      b.id AS "b_id", 
      b.lastName AS "b_lastName", 
      b.locationIP AS "b_locationIP"
FROM ldbc.Person AS a
INNER JOIN ldbc.Person_isLocatedIn_Place AS t113 ON t113.PersonId = a.id
INNER JOIN ldbc.Place AS t112 ON t112.id = t113.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t114 ON t114.Place1Id = t112.id
INNER JOIN ldbc.Place AS country ON country.id = t114.Place2Id
INNER JOIN ldbc.Person_knows_Person AS k1 ON k1.Person1Id = a.id OR k1.Person2Id = a.id
INNER JOIN ldbc.Person AS b ON (b.id = k1.Person2Id AND k1.Person1Id = a.id) OR (b.id = k1.Person1Id AND k1.Person2Id = a.id)
WHERE (t112.type = 'City' AND country.type = 'Country')
), 
with_a_b_country_cte_2 AS (SELECT DISTINCT 
      with_a_b_country_cte_1.country_id AS "country_id", 
      with_a_b_country_cte_1.country_name AS "country_name", 
      with_a_b_country_cte_1.country_url AS "country_url", 
      with_a_b_country_cte_1.a_birthday AS "a_birthday", 
      with_a_b_country_cte_1.a_browserUsed AS "a_browserUsed", 
      with_a_b_country_cte_1.a_creationDate AS "a_creationDate", 
      with_a_b_country_cte_1.a_firstName AS "a_firstName", 
      with_a_b_country_cte_1.a_gender AS "a_gender", 
      with_a_b_country_cte_1.a_id AS "a_id", 
      with_a_b_country_cte_1.a_lastName AS "a_lastName", 
      with_a_b_country_cte_1.a_locationIP AS "a_locationIP", 
      with_a_b_country_cte_1.b_birthday AS "b_birthday", 
      with_a_b_country_cte_1.b_browserUsed AS "b_browserUsed", 
      with_a_b_country_cte_1.b_creationDate AS "b_creationDate", 
      with_a_b_country_cte_1.b_firstName AS "b_firstName", 
      with_a_b_country_cte_1.b_gender AS "b_gender", 
      with_a_b_country_cte_1.b_id AS "b_id", 
      with_a_b_country_cte_1.b_lastName AS "b_lastName", 
      with_a_b_country_cte_1.b_locationIP AS "b_locationIP"
FROM with_a_b_country_cte_1 AS a_b_country
INNER JOIN ldbc.Person_isLocatedIn_Place AS t116 ON t116.PersonId = a_b_country.b_id
INNER JOIN ldbc.Place AS t115 ON t115.id = t116.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t117 ON t117.Place1Id = t115.id
INNER JOIN ldbc.Place AS country ON a_b_country.country_id = t117.Place2Id
WHERE (t115.type = 'City' AND country.type = 'Country')
UNION ALL 
SELECT DISTINCT 
      with_a_b_country_cte_1.country_id AS "country_id", 
      with_a_b_country_cte_1.country_name AS "country_name", 
      with_a_b_country_cte_1.country_url AS "country_url", 
      with_a_b_country_cte_1.a_birthday AS "a_birthday", 
      with_a_b_country_cte_1.a_browserUsed AS "a_browserUsed", 
      with_a_b_country_cte_1.a_creationDate AS "a_creationDate", 
      with_a_b_country_cte_1.a_firstName AS "a_firstName", 
      with_a_b_country_cte_1.a_gender AS "a_gender", 
      with_a_b_country_cte_1.a_id AS "a_id", 
      with_a_b_country_cte_1.a_lastName AS "a_lastName", 
      with_a_b_country_cte_1.a_locationIP AS "a_locationIP", 
      with_a_b_country_cte_1.b_birthday AS "b_birthday", 
      with_a_b_country_cte_1.b_browserUsed AS "b_browserUsed", 
      with_a_b_country_cte_1.b_creationDate AS "b_creationDate", 
      with_a_b_country_cte_1.b_firstName AS "b_firstName", 
      with_a_b_country_cte_1.b_gender AS "b_gender", 
      with_a_b_country_cte_1.b_id AS "b_id", 
      with_a_b_country_cte_1.b_lastName AS "b_lastName", 
      with_a_b_country_cte_1.b_locationIP AS "b_locationIP"
FROM with_a_b_country_cte_1 AS a_b_country
INNER JOIN ldbc.Person_isLocatedIn_Place AS t116 ON t116.PersonId = a_b_country.b_id
INNER JOIN ldbc.Place AS t115 ON t115.id = t116.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t117 ON t117.Place1Id = t115.id
INNER JOIN ldbc.Place AS country ON a_b_country.country_id = t117.Place2Id
WHERE (t115.type = 'City' AND country.type = 'Country')
)
SELECT 
      a_b_country.b_birthday AS "a_b_country.b_birthday", 
      a_b_country.b_id AS "a_b_country.b_id", 
      a_b_country.a_creationDate AS "a_b_country.a_creationDate", 
      a_b_country.b_creationDate AS "a_b_country.b_creationDate", 
      a_b_country.b_gender AS "a_b_country.b_gender", 
      a_b_country.a_birthday AS "a_b_country.a_birthday", 
      a_b_country.b_locationIP AS "a_b_country.b_locationIP", 
      a_b_country.a_firstName AS "a_b_country.a_firstName", 
      a_b_country.country_id AS "a_b_country.country_id", 
      a_b_country.country_name AS "a_b_country.country_name", 
      a_b_country.b_firstName AS "a_b_country.b_firstName", 
      a_b_country.a_browserUsed AS "a_b_country.a_browserUsed", 
      a_b_country.a_gender AS "a_b_country.a_gender", 
      a_b_country.a_lastName AS "a_b_country.a_lastName", 
      a_b_country.b_browserUsed AS "a_b_country.b_browserUsed", 
      a_b_country.b_lastName AS "a_b_country.b_lastName", 
      a_b_country.a_id AS "a_b_country.a_id", 
      a_b_country.a_locationIP AS "a_b_country.a_locationIP", 
      a_b_country.country_url AS "a_b_country.country_url", 
      c.gender AS "c.gender", 
      c.locationIP AS "c.locationIP", 
      c.birthday AS "c.birthday", 
      c.lastName AS "c.lastName", 
      c.id AS "c.id", 
      c.browserUsed AS "c.browserUsed", 
      c.firstName AS "c.firstName", 
      c.creationDate AS "c.creationDate", 
      t118.url AS "t118.url", 
      t118.id AS "t118.id", 
      t118.name AS "t118.name", 
      country.id AS "country.id", 
      country.url AS "country.url", 
      country.name AS "country.name"
FROM with_a_b_country_cte_2 AS a_b_country
INNER JOIN ldbc.Person_knows_Person AS k2 ON k2.Person1Id = b.id
INNER JOIN ldbc.Person AS c ON c.id = k2.Person2Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t119 ON t119.PersonId = c.id
INNER JOIN ldbc.Place AS t118 ON t118.id = t119.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t120 ON t120.Place1Id = t118.id
INNER JOIN ldbc.Place AS country ON country.id = t120.Place2Id
WHERE (((b.id < c.id AND ($startDate <= k2.creationDate AND k2.creationDate <= $endDate)) AND t118.type = 'City') AND country.type = 'Country')
UNION ALL 
SELECT 
      c.gender AS "c.gender", 
      c.locationIP AS "c.locationIP", 
      c.birthday AS "c.birthday", 
      c.lastName AS "c.lastName", 
      c.id AS "c.id", 
      c.browserUsed AS "c.browserUsed", 
      c.firstName AS "c.firstName", 
      c.creationDate AS "c.creationDate", 
      a_b_country.b_id AS "a_b_country.b_id", 
      a_b_country.a_firstName AS "a_b_country.a_firstName", 
      a_b_country.country_id AS "a_b_country.country_id", 
      a_b_country.a_creationDate AS "a_b_country.a_creationDate", 
      a_b_country.b_firstName AS "a_b_country.b_firstName", 
      a_b_country.b_lastName AS "a_b_country.b_lastName", 
      a_b_country.a_birthday AS "a_b_country.a_birthday", 
      a_b_country.b_locationIP AS "a_b_country.b_locationIP", 
      a_b_country.country_url AS "a_b_country.country_url", 
      a_b_country.a_locationIP AS "a_b_country.a_locationIP", 
      a_b_country.a_browserUsed AS "a_b_country.a_browserUsed", 
      a_b_country.b_birthday AS "a_b_country.b_birthday", 
      a_b_country.b_gender AS "a_b_country.b_gender", 
      a_b_country.a_id AS "a_b_country.a_id", 
      a_b_country.b_browserUsed AS "a_b_country.b_browserUsed", 
      a_b_country.b_creationDate AS "a_b_country.b_creationDate", 
      a_b_country.a_lastName AS "a_b_country.a_lastName", 
      a_b_country.country_name AS "a_b_country.country_name", 
      a_b_country.a_gender AS "a_b_country.a_gender", 
      t118.url AS "t118.url", 
      t118.id AS "t118.id", 
      t118.name AS "t118.name", 
      country.id AS "country.id", 
      country.url AS "country.url", 
      country.name AS "country.name"
FROM ldbc.Person AS c
INNER JOIN ldbc.Person_knows_Person AS k2 ON k2.Person1Id = c.id
INNER JOIN with_a_b_country_cte_2 AS b ON b.id = k2.Person2Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t119 ON t119.PersonId = c.id
INNER JOIN ldbc.Place AS t118 ON t118.id = t119.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t120 ON t120.Place1Id = t118.id
INNER JOIN ldbc.Place AS country ON country.id = t120.Place2Id
WHERE (((b.id < c.id AND ($startDate <= k2.creationDate AND k2.creationDate <= $endDate)) AND t118.type = 'City') AND country.type = 'Country')

