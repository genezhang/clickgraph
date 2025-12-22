-- LDBC Query: BI-13
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.794005
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (country:Country {name: 'France'})<-[:IS_PART_OF]-(city:City)<-[:IS_LOCATED_IN]-(person:Person)
-- OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
-- WITH person, count(post) AS messageCount
-- WHERE messageCount < 5
-- RETURN 
--     person.id AS personId,
--     person.firstName AS firstName,
--     person.lastName AS lastName,
--     messageCount
-- ORDER BY messageCount, personId
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH with_messageCount_person_cte_1 AS (SELECT 
      anyLast(person.birthday) AS "person_birthday", 
      anyLast(person.browserUsed) AS "person_browserUsed", 
      anyLast(person.creationDate) AS "person_creationDate", 
      anyLast(person.firstName) AS "person_firstName", 
      anyLast(person.gender) AS "person_gender", 
      person.id AS "person_id", 
      anyLast(person.lastName) AS "person_lastName", 
      anyLast(person.locationIP) AS "person_locationIP", 
      count(*) AS "messageCount"
FROM ldbc.Post AS post
LEFT JOIN ldbc.Post_hasCreator_Person AS t38 ON t38.PostId = post.id
LEFT JOIN ldbc.Person_isLocatedIn_Place AS person ON person.PersonId = t38.PersonId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t37 ON t37.PersonId = person.id
INNER JOIN ldbc.Place_isPartOf_Place AS city ON city.Place1Id = t37.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t36 ON t36.Place1Id = city.id
INNER JOIN ldbc.Place AS country ON country.id = t36.Place2Id
WHERE ((country.name = 'France' AND city.type = 'City') AND country.type = 'Country')
GROUP BY person.id
HAVING messageCount < 5
)
SELECT 
      messageCount_person.person_id AS "personId", 
      messageCount_person.person_firstName AS "firstName", 
      messageCount_person.person_lastName AS "lastName", 
      messageCount_person.messageCount AS "messageCount"
FROM with_messageCount_person_cte_1 AS messageCount_person
ORDER BY messageCount ASC, personId ASC
LIMIT  100
