-- LDBC Query: interactive-complex-3
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.820464
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
-- WHERE person.id <> friend.id
-- WITH friend
-- MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:IS_LOCATED_IN]->(country:Country)
-- WHERE post.creationDate >= $startDate 
--   AND post.creationDate < $endDate
--   AND country.name IN [$countryXName, $countryYName]
-- RETURN 
--     friend.id AS friendId,
--     friend.firstName AS friendFirstName,
--     friend.lastName AS friendLastName,
--     count(post) AS messageCount
-- ORDER BY messageCount DESC, friend.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte7 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE start_node.id = 933
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte7 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
), 
with_friend_cte_1 AS (SELECT 
      friend.birthday AS "friend_birthday", 
      friend.browserUsed AS "friend_browserUsed", 
      friend.creationDate AS "friend_creationDate", 
      friend.firstName AS "friend_firstName", 
      friend.gender AS "friend_gender", 
      friend.id AS "friend_id", 
      friend.lastName AS "friend_lastName", 
      friend.locationIP AS "friend_locationIP"
FROM (
SELECT 
      start_node.gender AS "person.gender", 
      start_node.locationIP AS "person.locationIP", 
      start_node.birthday AS "person.birthday", 
      start_node.lastName AS "person.lastName", 
      start_node.id AS "person.id", 
      start_node.browserUsed AS "person.browserUsed", 
      start_node.firstName AS "person.firstName", 
      start_node.creationDate AS "person.creationDate", 
      end_node.gender AS "friend.gender", 
      end_node.locationIP AS "friend.locationIP", 
      end_node.birthday AS "friend.birthday", 
      end_node.lastName AS "friend.lastName", 
      end_node.id AS "friend.id", 
      end_node.browserUsed AS "friend.browserUsed", 
      end_node.firstName AS "friend.firstName", 
      end_node.creationDate AS "friend.creationDate"
FROM vlp_cte7 AS vlp7
JOIN ldbc.Person AS person ON vlp7.start_id = person.id
JOIN ldbc.Person AS friend ON vlp7.end_id = friend.id
UNION ALL 
SELECT 
      start_node.gender AS "friend.gender", 
      start_node.locationIP AS "friend.locationIP", 
      start_node.birthday AS "friend.birthday", 
      start_node.lastName AS "friend.lastName", 
      start_node.id AS "friend.id", 
      start_node.browserUsed AS "friend.browserUsed", 
      start_node.firstName AS "friend.firstName", 
      start_node.creationDate AS "friend.creationDate", 
      end_node.gender AS "person.gender", 
      end_node.locationIP AS "person.locationIP", 
      end_node.birthday AS "person.birthday", 
      end_node.lastName AS "person.lastName", 
      end_node.id AS "person.id", 
      end_node.browserUsed AS "person.browserUsed", 
      end_node.firstName AS "person.firstName", 
      end_node.creationDate AS "person.creationDate"
FROM vlp_cte8 AS vlp8
JOIN ldbc.Person AS friend ON vlp8.start_id = friend.id
JOIN ldbc.Person AS person ON vlp8.end_id = person.id
WHERE end_node.id = 933
) AS __union
), 
vlp_cte8 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte8 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE end_node.id = 933
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte8 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = 933
)
    SELECT * FROM vlp_cte8
  )
)
SELECT 
      friend.id AS "friendId", 
      friend.firstName AS "friendFirstName", 
      friend.lastName AS "friendLastName", 
      count(post.id) AS "messageCount"
FROM ldbc.Post AS post
INNER JOIN ldbc.Post_hasCreator_Person AS t102 ON t102.PostId = post.id
INNER JOIN ldbc.Post_isLocatedIn_Country AS t103 ON t102.PostId = t103.PostId
INNER JOIN with_friend_cte_1 AS friend ON friend.id = t102.PersonId
INNER JOIN ldbc.Place AS country ON country.id = t103.CountryId
INNER JOIN ldbc.Post_isLocatedIn_Country AS t103 ON t103.PostId = post.id
WHERE ((country.name IN ['India', 'China'] AND (post.creationDate >= '2011-01-01' AND post.creationDate < '2012-12-31')) AND country.type = 'Country')
GROUP BY friend.id, friend.firstName, friend.lastName
ORDER BY messageCount DESC, friend.id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

