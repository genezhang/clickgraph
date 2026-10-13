WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v0.full_name AS `p2_v0_name`
FROM test_integration.users_test AS v0
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__user_id AS `v0__user_id`
FROM with_w1 AS w1
), 
vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d2.v0__user_id AS `id`
FROM optional_d2 AS d2))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
optional_o3 AS (
SELECT 
      d2.v0__user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.full_name AS `p2_v2_name`
FROM vlp_v1_path AS v1
JOIN optional_d2 AS d2 ON v1.start_id = d2.v0__user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
)
SELECT 
      w1.p2_v0_name AS `a.name`, 
      o3.p2_v2_name AS `b.name`
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v0__user_id = o3.v0__user_id
ORDER BY w1.p2_v0_name ASC NULLS LAST, o3.p2_v2_name ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]