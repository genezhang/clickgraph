-- LDBC Official Query: IC-complex-10
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.693527
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
--        (friend)-[:IS_LOCATED_IN]->(city:City)
-- WHERE NOT friend=person AND
--       NOT (friend)-[:KNOWS]-(person)
-- WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
-- WHERE  (birthday.month=$month AND birthday.day>=21) OR
--         (birthday.month=($month%12)+1 AND birthday.day<22)
-- WITH DISTINCT friend, city, person
-- OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
-- WITH friend, city, collect(post) AS posts, person
-- WITH friend,
--      city,
--      size(posts) AS postCount,
--      size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
-- RETURN friend.id AS personId,
--        friend.firstName AS personFirstName,
--        friend.lastName AS personLastName,
--        commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
--        friend.gender AS personGender,
--        city.name AS personCityName
-- ORDER BY commonInterestScore DESC, personId ASC
-- LIMIT 10

-- Generated ClickHouse SQL:
WITH vlp_multi_type_friend_city AS (
SELECT '' AS end_type, '' AS end_id, '' AS start_id, '' AS start_type, 0 AS hop_count WHERE 0 = 1
), 
with_birthday_city_friend_person_cte_0 AS (SELECT 
      person.birthday AS "p6_person_birthday", 
      person.browserUsed AS "p6_person_browserUsed", 
      person.creationDate AS "p6_person_creationDate", 
      person.firstName AS "p6_person_firstName", 
      person.gender AS "p6_person_gender", 
      person.id AS "p6_person_id", 
      person.lastName AS "p6_person_lastName", 
      person.locationIP AS "p6_person_locationIP", 
      city.id AS "p4_city_id", 
      city.name AS "p4_city_name", 
      city.url AS "p4_city_url", 
      friend.birthday AS "p6_friend_birthday", 
      friend.browserUsed AS "p6_friend_browserUsed", 
      friend.creationDate AS "p6_friend_creationDate", 
      friend.firstName AS "p6_friend_firstName", 
      friend.gender AS "p6_friend_gender", 
      friend.id AS "p6_friend_id", 
      friend.lastName AS "p6_friend_lastName", 
      friend.locationIP AS "p6_friend_locationIP", 
      parseDateTime64BestEffort(map('epochMillis', toString(friend.birthday)), 3) AS "birthday"
FROM vlp_multi_type_friend_city AS t
WHERE ((NOT friend.id = person.id AND NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = friend.id AND Person_knows_Person.Person2Id = person.id) OR (Person_knows_Person.Person1Id = person.id AND Person_knows_Person.Person2Id = friend.id))) AND ((birthday.month = $month AND birthday.day >= 21) OR (birthday.month = $month % 12 + 1 AND birthday.day < 22)))
), 
with_city_friend_person_cte_1 AS (SELECT DISTINCT 
      birthday_city_friend_person.p6_friend_birthday AS "p6_friend_birthday", 
      birthday_city_friend_person.p6_friend_browserUsed AS "p6_friend_browserUsed", 
      birthday_city_friend_person.p6_friend_creationDate AS "p6_friend_creationDate", 
      birthday_city_friend_person.p6_friend_firstName AS "p6_friend_firstName", 
      birthday_city_friend_person.p6_friend_gender AS "p6_friend_gender", 
      birthday_city_friend_person.p6_friend_id AS "p6_friend_id", 
      birthday_city_friend_person.p6_friend_lastName AS "p6_friend_lastName", 
      birthday_city_friend_person.p6_friend_locationIP AS "p6_friend_locationIP", 
      birthday_city_friend_person.p4_city_id AS "p4_city_id", 
      birthday_city_friend_person.p4_city_name AS "p4_city_name", 
      birthday_city_friend_person.p4_city_url AS "p4_city_url", 
      birthday_city_friend_person.p6_person_birthday AS "p6_person_birthday", 
      birthday_city_friend_person.p6_person_browserUsed AS "p6_person_browserUsed", 
      birthday_city_friend_person.p6_person_creationDate AS "p6_person_creationDate", 
      birthday_city_friend_person.p6_person_firstName AS "p6_person_firstName", 
      birthday_city_friend_person.p6_person_gender AS "p6_person_gender", 
      birthday_city_friend_person.p6_person_id AS "p6_person_id", 
      birthday_city_friend_person.p6_person_lastName AS "p6_person_lastName", 
      birthday_city_friend_person.p6_person_locationIP AS "p6_person_locationIP"
FROM with_birthday_city_friend_person_cte_0 AS birthday_city_friend_person
), 
with_city_friend_person_posts_cte_1 AS (SELECT 
      anyLast(city_friend_person.p6_friend_birthday) AS "p6_friend_birthday", 
      anyLast(city_friend_person.p6_friend_browserUsed) AS "p6_friend_browserUsed", 
      anyLast(city_friend_person.p6_friend_creationDate) AS "p6_friend_creationDate", 
      anyLast(city_friend_person.p6_friend_firstName) AS "p6_friend_firstName", 
      anyLast(city_friend_person.p6_friend_gender) AS "p6_friend_gender", 
      city_friend_person.p6_friend_id AS "p6_friend_id", 
      anyLast(city_friend_person.p6_friend_lastName) AS "p6_friend_lastName", 
      anyLast(city_friend_person.p6_friend_locationIP) AS "p6_friend_locationIP", 
      city_friend_person.p4_city_id AS "p4_city_id", 
      anyLast(city_friend_person.p4_city_name) AS "p4_city_name", 
      anyLast(city_friend_person.p4_city_url) AS "p4_city_url", 
      groupArray(tuple(post.browserUsed, post.content, post.creationDate, post.id, post.imageFile, post.language, post.length, post.locationIP)) AS "posts", 
      anyLast(city_friend_person.p6_person_birthday) AS "p6_person_birthday", 
      anyLast(city_friend_person.p6_person_browserUsed) AS "p6_person_browserUsed", 
      anyLast(city_friend_person.p6_person_creationDate) AS "p6_person_creationDate", 
      anyLast(city_friend_person.p6_person_firstName) AS "p6_person_firstName", 
      anyLast(city_friend_person.p6_person_gender) AS "p6_person_gender", 
      city_friend_person.p6_person_id AS "p6_person_id", 
      anyLast(city_friend_person.p6_person_lastName) AS "p6_person_lastName", 
      anyLast(city_friend_person.p6_person_locationIP) AS "p6_person_locationIP"
FROM ldbc.Post AS post
LEFT JOIN ldbc.Post_hasCreator_Person AS t82 ON t82.PostId = post.id
LEFT JOIN with_city_friend_person_cte_1 AS city_friend_person ON city_friend_person.p6_friend_id = t82.PersonId
), 
with_city_friend_postCount_size_cte_1 AS (SELECT 
      city_friend_person_posts.p6_friend_birthday AS "p6_friend_birthday", 
      city_friend_person_posts.p6_friend_browserUsed AS "p6_friend_browserUsed", 
      city_friend_person_posts.p6_friend_creationDate AS "p6_friend_creationDate", 
      city_friend_person_posts.p6_friend_firstName AS "p6_friend_firstName", 
      city_friend_person_posts.p6_friend_gender AS "p6_friend_gender", 
      city_friend_person_posts.p6_friend_id AS "p6_friend_id", 
      city_friend_person_posts.p6_friend_lastName AS "p6_friend_lastName", 
      city_friend_person_posts.p6_friend_locationIP AS "p6_friend_locationIP", 
      city_friend_person_posts.p4_city_id AS "p4_city_id", 
      city_friend_person_posts.p4_city_name AS "p4_city_name", 
      city_friend_person_posts.p4_city_url AS "p4_city_url", 
      length(city_friend_person.posts) AS "postCount"
FROM with_city_friend_person_posts_cte_1 AS city_friend_person_posts
)
SELECT 
      city_friend_postCount_size.p6_friend_gender AS "gender", 
      city_friend_postCount_size.p4_city_name AS "name", 
      city_friend_postCount_size.p6_friend_browserUsed AS "browserUsed", 
      city_friend_postCount_size.p6_friend_lastName AS "lastName", 
      city_friend_postCount_size.postCount AS "postCount", 
      city_friend_postCount_size.p6_friend_creationDate AS "creationDate", 
      city_friend_postCount_size.p6_friend_firstName AS "firstName", 
      city_friend_postCount_size.p6_friend_birthday AS "birthday", 
      city_friend_postCount_size.p4_city_id AS "id", 
      city_friend_postCount_size.p6_friend_locationIP AS "locationIP", 
      city_friend_postCount_size.p4_city_url AS "url"
FROM with_city_friend_postCount_size_cte_1 AS city_friend_postCount_size

