WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v2.user_id AS `p2_v3_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
vlp_v4_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      w1.v3__user_id AS `id`
FROM with_w1 AS w1))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v4_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
with_w3 AS (
SELECT 
      w1.v3__user_id AS `v3__user_id`, 
      w1.p2_v3_user_id AS `p2_v3_user_id`, 
      v5.user_id AS `v5__user_id`
FROM vlp_v4_path AS v4
JOIN with_w1 AS w1 ON v4.start_id = w1.v3__user_id
JOIN test_integration.users_test AS v5 ON v4.end_id = v5.user_id
), 
optional_d4 AS (
SELECT DISTINCT 
      w3.v5__user_id AS `v5__user_id`
FROM with_w3 AS w3
), 
vlp_v6_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d4.v5__user_id AS `id`
FROM optional_d4 AS d4))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v6_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
optional_o5 AS (
SELECT 
      d4.v5__user_id AS `v5__user_id`, 
      v7.user_id AS `v7__user_id`, 
      v7.user_id AS `p2_v7_user_id`
FROM vlp_v6_path AS v6
JOIN optional_d4 AS d4 ON v6.start_id = d4.v5__user_id
JOIN test_integration.users_test AS v7 ON v6.end_id = v7.user_id
)
SELECT 
      w3.p2_v3_user_id AS `c.user_id`, 
      o5.p2_v7_user_id AS `e.user_id`
FROM with_w3 AS w3
LEFT JOIN optional_o5 AS o5 ON w3.v5__user_id = o5.v5__user_id
