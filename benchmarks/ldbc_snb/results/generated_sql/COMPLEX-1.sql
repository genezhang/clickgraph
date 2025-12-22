-- LDBC Query: COMPLEX-1
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.806056
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (country:Country)<-[:IS_PART_OF]-(city:City)<-[:IS_LOCATED_IN]-(person:Person)
-- OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
-- OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
-- WITH country, person, count(DISTINCT post) AS posts, count(DISTINCT liker) AS likes
-- RETURN 
--     country.name AS countryName,
--     count(person) AS persons,
--     sum(posts) AS totalPosts,
--     sum(likes) AS totalLikes
-- ORDER BY totalPosts DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH with_country_likes_person_posts_cte_1 AS (SELECT 
      country.id AS "country_id", 
      anyLast(country.name) AS "country_name", 
      anyLast(country.url) AS "country_url", 
      anyLast(person.birthday) AS "person_birthday", 
      anyLast(person.browserUsed) AS "person_browserUsed", 
      anyLast(person.creationDate) AS "person_creationDate", 
      anyLast(person.firstName) AS "person_firstName", 
      anyLast(person.gender) AS "person_gender", 
      person.id AS "person_id", 
      anyLast(person.lastName) AS "person_lastName", 
      anyLast(person.locationIP) AS "person_locationIP", 
      count(DISTINCT post) AS "posts", 
      count(DISTINCT liker) AS "likes"
FROM ldbc.Person AS liker
LEFT JOIN ldbc.Person_likes_Post AS t65 ON t65.PersonId = liker.id
LEFT JOIN ldbc.Post_hasCreator_Person AS post ON post.PostId = t65.PostId
LEFT JOIN ldbc.Post_hasCreator_Person AS t64 ON t64.PostId = post.id
LEFT JOIN ldbc.Person_isLocatedIn_Place AS person ON person.PersonId = t64.PersonId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t63 ON t63.PersonId = person.id
INNER JOIN ldbc.Place_isPartOf_Place AS city ON city.Place1Id = t63.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t62 ON t62.Place1Id = city.id
INNER JOIN ldbc.Place AS country ON country.id = t62.Place2Id
WHERE (city.type = 'City' AND country.type = 'Country')
GROUP BY country.id, person.id
)
SELECT 
      country_likes_person_posts.country_name AS "countryName", 
      count(country_likes_person_posts.person_id) AS "persons", 
      sum(country_likes_person_posts.posts) AS "totalPosts", 
      sum(country_likes_person_posts.likes) AS "totalLikes"
FROM with_country_likes_person_posts_cte_1 AS country_likes_person_posts
GROUP BY country_likes_person_posts.country_name
ORDER BY totalPosts DESC
LIMIT  20
