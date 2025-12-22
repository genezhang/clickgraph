-- LDBC Query: BI-3
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.776829
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (country:Country {name: 'China'})<-[:IS_PART_OF]-(city:City)<-[:IS_LOCATED_IN]-(person:Person)
-- MATCH (person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->(post:Post)
-- MATCH (post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)
-- WHERE tagClass.name = 'MusicalArtist'
-- RETURN
--     forum.id AS forumId,
--     forum.title AS forumTitle,
--     person.id AS moderatorId,
--     count(DISTINCT post) AS messageCount
-- ORDER BY messageCount DESC, forumId
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      forum.id AS "forumId", 
      forum.title AS "forumTitle", 
      person.id AS "moderatorId", 
      count(DISTINCT post.id) AS "messageCount"
FROM ldbc.Place AS city
INNER JOIN ldbc.Place_isPartOf_Place AS t3 ON t3.Place1Id = city.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t4 ON t4.CityId = city.id
INNER JOIN ldbc.Forum_hasModerator_Person AS t5 ON t4.PersonId = t5.PersonId
INNER JOIN ldbc.Place AS country ON country.id = t3.Place2Id
INNER JOIN ldbc.Person AS person ON person.id = t4.PersonId
INNER JOIN ldbc.Forum AS forum ON forum.id = t5.ForumId
INNER JOIN ldbc.Person_isLocatedIn_Place AS t4 ON t3.Place1Id = t4.CityId
INNER JOIN ldbc.Forum_containerOf_Post AS t6 ON t5.ForumId = t6.ForumId
INNER JOIN ldbc.Forum_containerOf_Post AS t6 ON t6.ForumId = forum.id
INNER JOIN ldbc.Forum_hasModerator_Person AS t5 ON t5.PersonId = person.id
INNER JOIN ldbc.Post_hasTag_Tag AS t7 ON t6.PostId = t7.PostId
INNER JOIN ldbc.Post AS post ON post.id = t6.PostId
INNER JOIN ldbc.Tag_hasType_TagClass AS t8 ON t7.TagId = t8.TagId
INNER JOIN ldbc.Post_hasTag_Tag AS t7 ON t7.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t7.TagId
INNER JOIN ldbc.TagClass AS tagClass ON tagClass.id = t8.TagClassId
INNER JOIN ldbc.Tag_hasType_TagClass AS t8 ON t8.TagId = tag.id
WHERE (((tagClass.name = 'MusicalArtist' AND country.name = 'China') AND city.type = 'City') AND country.type = 'Country')
GROUP BY forum.id, forum.title, person.id
ORDER BY messageCount DESC, forumId ASC
LIMIT  20
