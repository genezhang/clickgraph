WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges,
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
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges,
        vp.start_age as start_age,
        vp.start_name as start_name,
        end_node.name as end_name,
        end_node.age as end_age
    FROM vlp_a_b vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
), 
vlp_b_a AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges,
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
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges,
        vp.start_name as start_name,
        end_node.age as end_age,
        end_node.name as end_name,
        vp.start_age as start_age
    FROM vlp_b_a vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
), 
with_b_distance_cte_0 AS (SELECT any_value(end_name) AS `p1_b_name`, min(length(`path`)) AS `distance` FROM (
SELECT 
      b.user_id AS `b.user_id`,
      path AS `path`
FROM vlp_a_b AS t
UNION ALL 
SELECT 
      b.user_id AS `b.user_id`,
      path AS `path`
FROM vlp_b_a AS t
) AS __union
GROUP BY `b.user_id`
)
SELECT 
      b_distance.p1_b_name AS `b.name`, 
      b_distance.distance AS `distance`
FROM with_b_distance_cte_0 AS b_distance
LIMIT 5