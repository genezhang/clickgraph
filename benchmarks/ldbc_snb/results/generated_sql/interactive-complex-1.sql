-- LDBC Query: interactive-complex-1
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.815112
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p:Person {id: $personId})-[:KNOWS*1..3]-(friend:Person)
-- WHERE friend.firstName = $firstName AND friend.id <> $personId
-- WITH friend, count(*) AS cnt
-- RETURN 
--     friend.id AS friendId,
--     friend.firstName AS friendFirstName,
--     friend.lastName AS friendLastName
-- ORDER BY friend.lastName ASC, friend.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte2 AS (
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
    WHERE start_node.id = 933 AND (end_node.firstName = 'Chau' AND end_node.id != 933)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte2 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND (end_node.firstName = 'Chau' AND end_node.id != 933)
), 
with_cnt_friend_cte_1 AS (SELECT 
      anyLast(friend.birthday) AS "friend_birthday", 
      anyLast(friend.browserUsed) AS "friend_browserUsed", 
      anyLast(friend.creationDate) AS "friend_creationDate", 
      anyLast(friend.firstName) AS "friend_firstName", 
      anyLast(friend.gender) AS "friend_gender", 
      friend.id AS "friend_id", 
      anyLast(friend.lastName) AS "friend_lastName", 
      anyLast(friend.locationIP) AS "friend_locationIP", 
      count(*) AS "cnt"
FROM (
SELECT 
      start_node.gender AS "p.gender", 
      start_node.locationIP AS "p.locationIP", 
      start_node.birthday AS "p.birthday", 
      start_node.lastName AS "p.lastName", 
      start_node.id AS "p.id", 
      start_node.browserUsed AS "p.browserUsed", 
      start_node.firstName AS "p.firstName", 
      start_node.creationDate AS "p.creationDate", 
      end_node.gender AS "friend.gender", 
      end_node.locationIP AS "friend.locationIP", 
      end_node.birthday AS "friend.birthday", 
      end_node.lastName AS "friend.lastName", 
      end_node.id AS "friend.id", 
      end_node.browserUsed AS "friend.browserUsed", 
      end_node.firstName AS "friend.firstName", 
      end_node.creationDate AS "friend.creationDate"
FROM vlp_cte2 AS vlp2
JOIN ldbc.Person AS p ON vlp2.start_id = p.id
JOIN ldbc.Person AS friend ON vlp2.end_id = friend.id
WHERE (end_node.firstName = 'Chau' AND end_node.id <> 933)
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
      end_node.gender AS "p.gender", 
      end_node.locationIP AS "p.locationIP", 
      end_node.birthday AS "p.birthday", 
      end_node.lastName AS "p.lastName", 
      end_node.id AS "p.id", 
      end_node.browserUsed AS "p.browserUsed", 
      end_node.firstName AS "p.firstName", 
      end_node.creationDate AS "p.creationDate"
FROM vlp_cte3 AS vlp3
JOIN ldbc.Person AS friend ON vlp3.start_id = friend.id
JOIN ldbc.Person AS p ON vlp3.end_id = p.id
WHERE end_node.id = 933
) AS __union
GROUP BY friend.id
), 
vlp_cte3 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte3 AS (
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
    WHERE (start_node.firstName = 'Chau' AND start_node.id != 933) AND end_node.id = 933
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte3 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = 933
)
    SELECT * FROM vlp_cte3
  )
)
SELECT 
      cnt_friend.friend_id AS "friendId", 
      cnt_friend.friend_firstName AS "friendFirstName", 
      cnt_friend.friend_lastName AS "friendLastName"
FROM with_cnt_friend_cte_1 AS cnt_friend
ORDER BY cnt_friend.friend_lastName ASC, cnt_friend.friend_id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

