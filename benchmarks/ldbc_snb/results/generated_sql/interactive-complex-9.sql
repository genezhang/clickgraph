-- LDBC Query: interactive-complex-9
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.821694
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (root:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
-- WHERE friend.id <> $personId
-- WITH DISTINCT friend
-- MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
-- WHERE post.creationDate < $maxDate
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     post.id AS postId,
--     post.content AS postContent,
--     post.creationDate AS postCreationDate
-- ORDER BY postCreationDate DESC, post.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte4 AS (
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
    FROM vlp_cte4 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
), 
with_friend_cte_1 AS (SELECT DISTINCT 
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
      start_node.gender AS "root.gender", 
      start_node.locationIP AS "root.locationIP", 
      start_node.birthday AS "root.birthday", 
      start_node.lastName AS "root.lastName", 
      start_node.id AS "root.id", 
      start_node.browserUsed AS "root.browserUsed", 
      start_node.firstName AS "root.firstName", 
      start_node.creationDate AS "root.creationDate", 
      end_node.gender AS "friend.gender", 
      end_node.locationIP AS "friend.locationIP", 
      end_node.birthday AS "friend.birthday", 
      end_node.lastName AS "friend.lastName", 
      end_node.id AS "friend.id", 
      end_node.browserUsed AS "friend.browserUsed", 
      end_node.firstName AS "friend.firstName", 
      end_node.creationDate AS "friend.creationDate"
FROM vlp_cte4 AS vlp4
JOIN ldbc.Person AS root ON vlp4.start_id = root.id
JOIN ldbc.Person AS friend ON vlp4.end_id = friend.id
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
      end_node.gender AS "root.gender", 
      end_node.locationIP AS "root.locationIP", 
      end_node.birthday AS "root.birthday", 
      end_node.lastName AS "root.lastName", 
      end_node.id AS "root.id", 
      end_node.browserUsed AS "root.browserUsed", 
      end_node.firstName AS "root.firstName", 
      end_node.creationDate AS "root.creationDate"
FROM vlp_cte5 AS vlp5
JOIN ldbc.Person AS friend ON vlp5.start_id = friend.id
JOIN ldbc.Person AS root ON vlp5.end_id = root.id
WHERE end_node.id = 933
) AS __union
), 
vlp_cte5 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte5 AS (
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
    FROM vlp_cte5 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = 933
)
    SELECT * FROM vlp_cte5
  )
)
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      post.id AS "postId", 
      post.content AS "postContent", 
      post.creationDate AS "postCreationDate"
FROM ldbc.Post AS post
WHERE post.creationDate < '2012-12-31'
ORDER BY postCreationDate DESC, post.id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

