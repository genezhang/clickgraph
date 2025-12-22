-- LDBC Official Query: BI-bi-4
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.087597
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
-- WHERE forum.creationDate > $date
-- WITH country, forum, count(person) AS numberOfMembers
-- ORDER BY numberOfMembers DESC, forum.id ASC, country.id
-- WITH DISTINCT forum AS topForum
-- LIMIT 100
-- 
-- WITH collect(topForum) AS topForums
-- 
-- CALL {
--   WITH topForums
--   UNWIND topForums AS topForum1
--   MATCH (topForum1)-[:CONTAINER_OF]->(post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_CREATOR]->(person:Person)<-[:HAS_MEMBER]-(topForum2:Forum)
--   WITH person, message, topForum2
--   WHERE topForum2 IN topForums
--   RETURN person, count(DISTINCT message) AS messageCount
-- UNION ALL
--   WITH topForums
--   UNWIND topForums AS topForum1
--   MATCH (person:Person)<-[:HAS_MEMBER]-(topForum1:Forum)
--   RETURN person, 0 AS messageCount
-- }
-- RETURN
--   person.id AS personId,
--   person.firstName AS personFirstName,
--   person.lastName AS personLastName,
--   person.creationDate AS personCreationDate,
--   sum(messageCount) AS messageCount
-- ORDER BY
--   messageCount DESC,
--   person.id ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH with_country_forum_numberOfMembers_cte_1 AS (SELECT 
      country.id AS "country_id", 
      anyLast(country.name) AS "country_name", 
      anyLast(country.url) AS "country_url", 
      anyLast(forum.creationDate) AS "forum_creationDate", 
      forum.id AS "forum_id", 
      anyLast(forum.title) AS "forum_title", 
      count(*) AS "numberOfMembers"
FROM ldbc.Forum AS forum
INNER JOIN ldbc.Forum_hasMember_Person AS t170 ON t170.ForumId = forum.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS person ON person.PersonId = t170.PersonId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t169 ON t169.PersonId = person.id
INNER JOIN ldbc.Place_isPartOf_Place AS t167 ON t167.Place1Id = t169.CityId
INNER JOIN ldbc.Place_isPartOf_Place AS t168 ON t168.Place1Id = t167.id
INNER JOIN ldbc.Place AS country ON country.id = t168.Place2Id
WHERE ((forum.creationDate > $date AND t167.type = 'City') AND country.type = 'Country')
GROUP BY country.id, forum.id
ORDER BY numberOfMembers DESC, forum.id ASC, country.id ASC
), 
with_topForum_cte_1 AS (SELECT DISTINCT 
      with_country_forum_numberOfMembers_cte_1.forum_creationDate AS "forum_creationDate", 
      with_country_forum_numberOfMembers_cte_1.forum_id AS "forum_id", 
      with_country_forum_numberOfMembers_cte_1.forum_title AS "forum_title"
FROM with_country_forum_numberOfMembers_cte_1 AS country_forum_numberOfMembers
LIMIT 100
), 
with_topForums_cte_1 AS (SELECT 
      groupArray(tuple(topForum.forum_creationDate, topForum.forum_id, topForum.forum_title)) AS "topForums"
FROM with_topForum_cte_1 AS topForum
)
SELECT 
      topForums.topForums AS "topForums.topForums"
FROM with_topForums_cte_1 AS topForums

