-- LDBC Query: BI-14
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.795152
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (country1:Country {name: 'Chile'})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person)
-- MATCH (country2:Country {name: 'Argentina'})<-[:IS_PART_OF]-(city2:City)<-[:IS_LOCATED_IN]-(person2:Person)
-- MATCH (person1)-[:KNOWS]-(person2)
-- RETURN 
--     person1.id AS person1Id,
--     person2.id AS person2Id,
--     city1.name AS city1Name,
--     city2.name AS city2Name
-- ORDER BY person1Id, person2Id
-- LIMIT 100

-- Generated ClickHouse SQL:
SELECT * FROM (
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id", 
      city1.name AS "city1Name", 
      city2.name AS "city2Name"
FROM ldbc.Place AS country2
INNER JOIN ldbc.Place_isPartOf_Place AS t41 ON t41.Place2Id = country2.id
INNER JOIN ldbc.Place AS city2 ON city2.id = t41.Place1Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t42 ON t41.Place1Id = t42.CityId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t42 ON t42.CityId = city2.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t42.PersonId
INNER JOIN ldbc.Person_knows_Person AS t43 ON t42.PersonId = t43.Person2Id
INNER JOIN ldbc.Place_isPartOf_Place AS t39 ON t39.Place1Id = city1.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t40 ON t40.CityId = city1.id
INNER JOIN ldbc.Place AS country1 ON country1.id = t39.Place2Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t40 ON t39.Place1Id = t40.CityId
INNER JOIN ldbc.Person_knows_Person AS t43 ON t40.PersonId = t43.Person1Id
INNER JOIN ldbc.Person AS person1 ON person1.id = t40.PersonId
INNER JOIN ldbc.Person_knows_Person AS t43 ON t43.Person1Id = person1.id
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id", 
      city1.name AS "city1Name", 
      city2.name AS "city2Name"
FROM ldbc.Place AS country2
INNER JOIN ldbc.Place_isPartOf_Place AS t41 ON t41.Place2Id = country2.id
INNER JOIN ldbc.Place AS city2 ON city2.id = t41.Place1Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t42 ON t42.CityId = city2.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t42.PersonId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t42 ON t41.Place1Id = t42.CityId
INNER JOIN ldbc.Person_knows_Person AS t43 ON t42.PersonId = t43.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t43 ON t43.Person1Id = person2.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t40 ON t40.CityId = city1.id
INNER JOIN ldbc.Place_isPartOf_Place AS t39 ON t39.Place1Id = city1.id
INNER JOIN ldbc.Place AS country1 ON country1.id = t39.Place2Id
INNER JOIN ldbc.Person AS person1 ON person1.id = t40.PersonId
INNER JOIN ldbc.Person_knows_Person AS t43 ON t40.PersonId = t43.Person2Id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t40 ON t39.Place1Id = t40.CityId
) AS __union
ORDER BY "person1Id" ASC, "person2Id" ASC
LIMIT  100
