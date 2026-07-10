WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        start_node.age as start_age,
        start_node.name as start_name,
        end_node.name as end_name,
        end_node.age as end_age
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        vp.start_age as start_age,
        vp.start_name as start_name,
        end_node.name as end_name,
        end_node.age as end_age
    FROM vlp_a_b vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_nodes, end_node.user_id)
), 
vlp_b_a AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        start_node.name as start_name,
        end_node.age as end_age,
        end_node.name as end_name,
        start_node.age as start_age
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        vp.start_name as start_name,
        end_node.age as end_age,
        end_node.name as end_name,
        vp.start_age as start_age
    FROM vlp_b_a vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_nodes, end_node.user_id)
), 
with_b_distance_cte_0 AS (SELECT anyLast(end_name) AS "p1_b_name", min(length(`path`)) AS "distance" FROM (
SELECT 
      path AS "path"
FROM vlp_a_b AS t
UNION ALL 
SELECT 
      path AS "path"
FROM vlp_b_a AS t
) AS __union
GROUP BY b.user_id
)
SELECT 
      b_distance.p1_b_name AS "b.name", 
      b_distance.distance AS "distance"
FROM with_b_distance_cte_0 AS b_distance
LIMIT 5